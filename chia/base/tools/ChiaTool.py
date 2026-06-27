from __future__ import annotations

from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict, Any

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
import ray
import logging
import sys
from chia.base.tools.util import make_router_lifespan

@dataclass
class ToolInfo:
    name: str
    port: int
    node_id: str

class ChiaTool:
    """Base class for MCP tool servers deployed onto Ray workers.

    Subclasses define a setup() method, which calls::

        ``self.mcp.add_tool(self.method, name=...)``  (one or more times)

    to register functions as tools with instances of this ChiaTool.
        
    Subclass can shut down the tool server with::

        ``self.stop()``
        - Tells the actor to shut down uvicorn, then kills the actor.
        - Because start and stop run in the same actor process, the
            uvicorn server reference is always reachable.

    The resulting MCP endpoint is at::

        http://{self.hostname}:{self.port}/{self.name}/mcp

    Example subclass::

        class BashTool(ChiaTool):
            def setup(self):
                self.mcp.add_tool(self.run_command, name=f"{name}_run_command")

            def run_command(self, command: str) -> str:
                ...

    Alternatively, instead of a setup method a subclass can define an
    __init__ method which must do the following::
        
        def __init__(self, name, task_options):
            super().__init__(name, task_options=task_options)
            # Registers fns with self.mcp.add_tool
            super().__post_init__()
    """

    # Stores {"name": str, "port": int, "node_id": str}
    _tool_registry: List[ToolInfo] = []

    def __init__(self, name: str, task_options: Optional[Dict] = None, logging_level = logging.DEBUG):
        """Initializes ChiaTool with a name and optional resource requirements.
        """
        self.name = name
        self.logging_name = name
        self.mcp = FastMCP(
            name,
            stateless_http=True,
            transport_security=TransportSecuritySettings(
                enable_dns_rebinding_protection=False,
            ),
        )
        self.hostname = None  # Will be set when the tool starts up and finds its IP address.
        self.port = 8000      # Will be set to the actual port by start_tool.
        self.task_options = task_options
        self.logger = logging.getLogger(self.logging_name)
        self.logger.setLevel(logging_level)
        self.node_id = None  # Will be set to the actual node_id by __post_init__.
        self.tool_info = None
        self._server_actor = None  # Ray actor handle, set by __post_init__

    def __post_init__(self):
        # Idempotency guard: deploying twice would spin up a second actor and
        # orphan the first. No-op if the server is already up — this makes the
        # setup()-hook construction style (see __init_subclass__) safe even when
        # a subclass also calls super().__post_init__() from a hand-written
        # __init__.
        if self._server_actor is not None:
            return
        if self.task_options is not None:
            self._server_actor = _ToolServerActor.options(**self.task_options).remote()
        else:
            self._server_actor = _ToolServerActor.remote()

        self.hostname, self.port, self.node_id = ray.get(
            self._server_actor.start.remote(self)
        )
        self.logger.info(f"{self.name} started at {self.hostname}:{self.port} on node {self.node_id}")
        self.tool_info = ToolInfo(
            name=self.name,
            port=self.port,
            node_id=self.node_id
        )
        ChiaTool._tool_registry.append(self.tool_info)

    def setup(self, *args, **kwargs):
        """Hook for the auto-constructed style — override to register
        tools and set instance state, *instead of* writing ``__init__``.

        A subclass that defines ``setup`` and no ``__init__`` is given an
        ``__init__`` automatically (see :meth:`__init_subclass__`) that runs
        ``ChiaTool.__init__`` before and ``__post_init__`` after ``setup``, so
        the contract with the subclass can't be written incorrectly. Inside ``setup``
        the base ``__init__`` has already run, so ``self.name`` / ``self.mcp``
        are available::

            class BashTool(ChiaTool):
                def setup(self, work_dir="/"):
                    self.work_dir = work_dir
                    self.mcp.add_tool(self.run_command, name=f"{self.name}_run_command")

        Positional/keyword args from the constructor (other than ``name``,
        ``task_options``, and ``logging_level``, which the base consumes) are
        forwarded here. Multi-level subclasses override ``setup`` and call
        ``super().setup(...)`` to chain.
        """
        raise NotImplementedError(
            f"{type(self).__name__} defines neither __init__ nor setup(); "
            "implement one of them."
        )

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Opt-in convenience: a subclass that defines setup() but no __init__
        # gets an __init__ that brackets setup() with ChiaTool.__init__ (before)
        # and __post_init__ (after) automatically. Subclasses that write their
        # own __init__ are left untouched and keep using the explicit
        # super().__init__() / super().__post_init__() pattern.
        if "setup" in cls.__dict__ and "__init__" not in cls.__dict__:
            def _auto_init(self, name, *args,
                           task_options=None, logging_level=logging.DEBUG,
                           **kwargs):
                # Call ChiaTool.__init__ explicitly (not super()/self.__init__):
                # self.__init__ is *this* function, so that would recurse.
                ChiaTool.__init__(self, name, task_options=task_options,
                                  logging_level=logging_level)
                self.setup(*args, **kwargs)
                self.__post_init__()

            _auto_init.__name__ = "__init__"
            _auto_init.__qualname__ = f"{cls.__qualname__}.__init__"
            cls.__init__ = _auto_init

    def stop(self):
        """Stop the tool's MCP server and clean up resources."""
        if self.tool_info in ChiaTool._tool_registry:
            ChiaTool._tool_registry.remove(self.tool_info)
        if self._server_actor is not None:
            try:
                ray.get(self._server_actor.stop.remote())
            except Exception as e:
                self.logger.warning(f"Error stopping tool {self.name}: {e}")
            ray.kill(self._server_actor, no_restart=True)
            self._server_actor = None

    def dict_entry(self):
        return self.mcp

    def __getstate__(self):
        """Exclude actor handle when serialized by Ray (e.g. passed to remote tasks)."""
        state = self.__dict__.copy()
        state['_server_actor'] = None
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)


def resolve_tool_url(url: str) -> str:
    """Rewrite a tool URL so it is routable from the current node.

    On tunnelled EC2 workers ``CHIA_TOOL_ADVERTISE_HOST`` and
    ``CHIA_TOOL_RELAY_HOST`` are set.  Tool URLs advertise the head's
    real IP (``CHIA_TOOL_ADVERTISE_HOST``) which is only directly
    reachable from the local network.  EC2 workers must connect via a
    reverse-tunnel relay instead, so this function replaces the host
    portion of the URL with the relay loopback (``CHIA_TOOL_RELAY_HOST``).

    On non-tunnelled nodes (or when the env vars are absent) the URL
    is returned unchanged.
    """
    import os
    advertise = os.environ.get("CHIA_TOOL_ADVERTISE_HOST")
    relay = os.environ.get("CHIA_TOOL_RELAY_HOST")
    if advertise and relay and advertise in url:
        return url.replace(advertise, relay, 1)
    return url


def makeMCPDeploymentClass(
        name: str,
        fastapi_app,
        autoscaling_config: dict = {
            "min_replicas": 1,
            "max_replicas": 20,
            "target_ongoing_requests": 5
        },
        ray_actor_options: dict = {"num_cpus": 0.2}
):
    from ray import serve

    @serve.deployment(
        autoscaling_config=autoscaling_config,
        ray_actor_options=ray_actor_options,
        name = name
    )
    @serve.ingress(fastapi_app)
    class _MCPDeployment:
        def __init__(self):
            pass

    return _MCPDeployment

def make_lifespan(mcpInst: FastMCP[Any], name=""):
    @asynccontextmanager
    async def lifespan(app):
        app.mount(f"/", mcpInst.streamable_http_app())
        async with mcpInst.session_manager.run():
            yield
    return lifespan


class _PortRegistry:
    """Tracks which ports are taken per IP, so concurrent start_router calls
    in the same process don't race on the same port."""

    _taken: Dict[str, set] = {}  # ip -> set of ports

    @classmethod
    def reserve(cls, ip: str, port: int):
        cls._taken.setdefault(ip, set()).add(port)

    @classmethod
    def is_taken(cls, ip: str, port: int) -> bool:
        return port in cls._taken.get(ip, set())

    @classmethod
    def release(cls, ip: str, port: int) -> bool:
        """
        Releases port from reservation for ip
        Returns false if port was not already reserved
        """
        try:
            cls._taken[ip].remove(port)
        except KeyError:
            return False
        return True


# Worker-side registry: lives in the actor process so start_router and
# stop_router always see the same dict.
import threading as _threading
_active_servers: Dict[str, Tuple["uvicorn.Server", _threading.Thread, str, int]] = {}
"""tool_name -> (server, thread, ip, port)."""


@ray.remote(num_cpus=0)
class _ToolServerActor:
    """Persistent Ray actor that manages a uvicorn server for one MCP tool.

    Because it is an actor, start() and stop() always execute in the same
    process, so the uvicorn.Server reference (in _active_servers) is never lost.
    """

    def __init__(self):
        self._name = None

    def start(self, tool: "ChiaTool") -> Tuple[str, int, str]:
        """Start the MCP server. Returns (advertised_ip, port, node_id).

        Reads CHIA_TOOL_BASE_PORT / CHIA_TOOL_MAX_PORT from the **worker**
        environment so that tunnelled nodes bind to the SSH-forwarded port
        range instead of the default (8000).

        When CHIA_TOOL_ADVERTISE_HOST is set (tunnelled workers), the
        returned IP is the head's resolved IP rather than the tunnel
        loopback.  Uvicorn still binds to the real node IP so the SSH
        forward tunnel can reach it.
        """
        import os
        base_port = int(os.environ.get("CHIA_TOOL_BASE_PORT", "8000"))
        max_port = int(os.environ.get("CHIA_TOOL_MAX_PORT", "0"))
        max_tries = (max_port - base_port + 1) if max_port else 100

        bind_ip = ray.util.get_node_ip_address()
        port = start_router(tool, bind_ip, base_port=base_port, max_tries=max_tries)
        node_id = ray.get_runtime_context().get_node_id()
        self._name = tool.name
        advertise_ip = os.environ.get("CHIA_TOOL_ADVERTISE_HOST", bind_ip)
        return advertise_ip, port, node_id

    def stop(self) -> bool:
        """Stop the uvicorn server. Returns True if it was running."""
        if self._name:
            return stop_router(self._name)
        return False


def start_router(tool: ChiaTool, ip_address: str, base_port: int = 8000, max_tries: int = 100) -> int:
    """Start MCP tool servers using a local uvicorn instance.

    Uses a plain uvicorn server (background thread) instead of Ray Serve so
    that multiple nodes can each host their own independent tool servers
    without route-prefix conflicts.

    Tries ports starting from *base_port*, skipping any that are already in
    use (e.g. host-networked Docker containers sharing the same port space).

    Returns the port that was successfully bound.
    """
    import threading
    import time
    import uvicorn
    from fastapi import FastAPI

    # Try ports sequentially, starting uvicorn on each until one succeeds.
    for port in range(base_port, base_port + max_tries):
        if _PortRegistry.is_taken(ip_address, port):
            continue

        # Create a fresh app each attempt — MCP's StreamableHTTPSessionManager
        # can only .run() once per instance.
        tool.mcp._session_manager = None
        app = FastAPI(lifespan=make_router_lifespan([tool.mcp]))
        app.mount(f"/{tool.name}", tool.mcp.streamable_http_app())

        config = uvicorn.Config(app, host=ip_address, port=port, log_level="info")
        server = uvicorn.Server(config)
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()

        # Wait for uvicorn to confirm it bound the port.
        for _ in range(50):  # up to 5 seconds
            time.sleep(0.1)
            if server.started:
                _PortRegistry.reserve(ip_address, port)
                _active_servers[tool.name] = (server, thread, ip_address, port)
                return port

        # Uvicorn didn't start — shut it down and try next port.
        server.should_exit = True
        thread.join(timeout=2)
        continue

    raise RuntimeError(
        f"Could not find available port in range {base_port}-{base_port + max_tries - 1}"
    )


def stop_router(tool_name: str) -> bool:
    """Stop a running uvicorn server by tool name.

    Looks up the server in the worker-side _active_servers registry,
    signals it to exit, waits for the thread, and releases the port.
    Returns True if the server was found and stopped.
    """
    entry = _active_servers.pop(tool_name, None)
    if entry is None:
        print(f"Warning: stop_router called for '{tool_name}' but no active server found")
        return False

    server, thread, ip, port = entry
    server.should_exit = True
    thread.join(timeout=5)
    if thread.is_alive():
        print(f"Warning: uvicorn thread for '{tool_name}' still alive after 5s")
    _PortRegistry.release(ip, port)
    return True
