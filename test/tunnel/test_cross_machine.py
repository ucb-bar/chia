"""
Provider-agnostic cross-machine execution tests for a tunnelled cluster.

Every remote dispatch here goes through chia's own ``@ChiaFunction`` /
``chia_remote`` path (NOT raw ``ray.remote``), so the head-relayed
``DispatchProxy`` (chia/base/dispatch_proxy.py) is exercised exactly as in
production.  That proxy is *why* a tunnelled worker can drive work on a LAN
node: when ``chia_remote`` runs on a reverse-tunnelled worker, the dispatch is
relayed through a head-pinned actor that *owns* the inner task, so every RPC
leg rides a link that already exists (EC2/GCP <-> head, head <-> LAN).

Topology is auto-detected by node identity, so this runs unchanged on the AWS
or GCP cluster:

  * **head**   — the node the driver/pytest runs on.
  * **local**  — a non-tunnelled LAN worker
  * **remote** — tunnelled workers, by their ``127.0.0.x`` tunnel node-ip
                 (or a ``gcp``/``ec2`` marker resource as a fallback).

Two things are covered:

  1. **Remote-function matrix** — a ChiaFunction pinned to an *origin* node
     dispatches (via ``chia_remote``) a nested ChiaFunction onto a *target*
     node, returning both node ids.  This proves a function started on any
     machine can drive work on any other.

     All directions work, including **local -> remote**: ``should_proxy``
     detects that a LAN worker can't reach a tunnelled target directly and
     relays the dispatch through the head DispatchProxy, which owns the inner
     task. (An *unconstrained* LAN dispatch — no resource or hard affinity —
     is relayed conservatively, since its landing node can't be proven.)

  2. **MCP tool over the tunnel** — a BashTool is deployed on one node and
     called (from a ChiaFunction pinned to another node) in every direction.

Run (against whichever cluster is up):
  chia up test/tunnel/test_gcp_cluster.yaml      # or test_ec2_cluster.yaml
  python -m pytest test/tunnel/test_cross_machine.py -v -s
"""

import asyncio
import os
import socket
import unittest
import uuid

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from chia.base.ChiaFunction import ChiaFunction, get
from chia.base.tools.BashTool import BashTool
from chia.base.tools.ChiaTool import ChiaTool

_MARKERS = ("gcp", "ec2")          # remote-only marker resources (fallback)
_TIMEOUT = float(os.environ.get("CHIA_XMACHINE_TIMEOUT", "180"))


# ---------------------------------------------------------------------------
# Chia functions (dispatched via chia_remote so the DispatchProxy engages)
# ---------------------------------------------------------------------------

@ChiaFunction(num_cpus=0)
def _cf_whoami():
    """Return (node_id, hostname) of wherever this ran."""
    return ray.get_runtime_context().get_node_id(), socket.gethostname()


@ChiaFunction(num_cpus=0)
def _cf_origin_then_call(target_node_id):
    """Runs on the origin node (pinned by the caller) and, from there,
    ``chia_remote``-dispatches ``_cf_whoami`` onto *target_node_id*.

    Returns ``(origin_id, origin_host, target_id, target_host)``.  When this
    runs on a tunnelled worker, the inner ``chia_remote`` is relayed through
    the head DispatchProxy — the mechanism that makes remote->{head,local,
    remote} work.
    """
    tid, thost = get(_cf_whoami.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(target_node_id, soft=False)
    ).chia_remote())
    return (ray.get_runtime_context().get_node_id(),
            socket.gethostname(), tid, thost)


@ChiaFunction()
def _cf_tool_call(tool_url, tool_method, args):
    """Run an MCP tool call from wherever this is pinned; return (result, host)."""
    import asyncio as _aio
    import socket as _sock
    from mcp import ClientSession as _CS
    from mcp.client.streamable_http import streamable_http_client as _http
    from chia.base.tools.ChiaTool import resolve_tool_url

    async def _do():
        resolved = resolve_tool_url(tool_url)
        async with _http(resolved) as (r, w, _):
            async with _CS(r, w) as sess:
                await sess.initialize()
                res = await sess.call_tool(tool_method, arguments=args)
                return res.content[0].text

    return _aio.run(_do()), _sock.gethostname()


# ---------------------------------------------------------------------------
# MCP helpers (host-side, for deploying/checking the BashTool)
# ---------------------------------------------------------------------------

def _uid(prefix="bash"):
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _url(tool):
    return f"http://{tool.hostname}:{tool.port}/{tool.name}/mcp"


def _tmethod(tool):
    return f"{tool.name}_run_command"


async def _wait_ready(url, retries=40, delay=0.5):
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client
    for _ in range(retries):
        try:
            async with streamable_http_client(url) as (r, w, _):
                async with ClientSession(r, w) as sess:
                    await sess.initialize()
                    return
        except Exception:
            await asyncio.sleep(delay)
    raise RuntimeError(f"MCP server at {url} not ready after {retries} attempts")


async def _call(url, method, args):
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client
    async with streamable_http_client(url) as (r, w, _):
        async with ClientSession(r, w) as sess:
            await sess.initialize()
            res = await sess.call_tool(method, arguments=args)
            return res.content[0].text


# ---------------------------------------------------------------------------

class TestCrossMachine(unittest.TestCase):

    _ray_started = False

    @classmethod
    def setUpClass(cls):
        if not ray.is_initialized():
            ray.init(
                address="auto",
                runtime_env={"working_dir": os.path.dirname(
                    os.path.dirname(os.path.dirname(__file__)))},
            )
            cls._ray_started = True

        cls.head_id = ray.get_runtime_context().get_node_id()
        alive = [n for n in ray.nodes() if n.get("Alive")]
        cls.name_by_id = {n["NodeID"]: n.get("NodeName", "") for n in alive}

        def _res(n, name):
            return n.get("Resources", {}).get(name, 0) > 0

        cls.local_ids = [n["NodeID"] for n in alive if _res(n, "local")]

        def _is_remote(n):
            if n["NodeID"] == cls.head_id or _res(n, "local"):
                return False
            if str(n.get("NodeName", "")).startswith("127."):
                return True
            return any(_res(n, m) for m in _MARKERS)

        cls.remote_ids = [n["NodeID"] for n in alive if _is_remote(n)]

        # node_id -> hostname (probe each node once, via chia_remote from head).
        cls.host_by_id = {}
        for nid in {cls.head_id, *cls.local_ids, *cls.remote_ids}:
            try:
                cls.host_by_id[nid] = get(_cf_whoami.options(
                    scheduling_strategy=NodeAffinitySchedulingStrategy(nid, soft=False)
                ).chia_remote(), timeout=_TIMEOUT)[1]
            except Exception:
                cls.host_by_id[nid] = "?"

    @classmethod
    def tearDownClass(cls):
        if cls._ray_started:
            ChiaTool._serve_started = False
            ray.shutdown()

    # -- role pinning ----------------------------------------------------

    def _pin_resource(self, node_id):
        """Resource dict that pins a BashTool (Serve) deployment to *node_id*."""
        if node_id == self.head_id:
            return {"node:__internal_head__": 0.001}
        return {f"node:{self.name_by_id[node_id]}": 0.001}

    def _need(self, role):
        if role == "remote" and not self.remote_ids:
            self.skipTest("no tunnelled remote worker in cluster")
        if role == "remote2" and len(self.remote_ids) < 2:
            self.skipTest("need >= 2 tunnelled remote workers")
        if role == "local" and not self.local_ids:
            self.skipTest("no local worker in cluster")

    # ===================================================================
    # 1. Remote-function matrix (chia_remote; remote-origin uses the proxy)
    # ===================================================================

    def _cross(self, origin_id, target_id, timeout=_TIMEOUT):
        out = get(_cf_origin_then_call.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(origin_id, soft=False)
        ).chia_remote(target_id), timeout=timeout)
        oid, ohost, tid, thost = out
        self.assertEqual(oid, origin_id,
                         f"outer ran on {ohost}, expected origin "
                         f"{self.host_by_id.get(origin_id)}")
        self.assertEqual(tid, target_id,
                         f"inner ran on {thost}, expected target "
                         f"{self.host_by_id.get(target_id)}")
        print(f"  {self.host_by_id.get(origin_id)} -> "
              f"{self.host_by_id.get(target_id)} OK")
        return out

    def test_head_to_local(self):
        self._need("local")
        self._cross(self.head_id, self.local_ids[0])

    def test_head_to_remote(self):
        self._need("remote")
        self._cross(self.head_id, self.remote_ids[0])

    def test_local_to_head(self):
        self._need("local")
        self._cross(self.local_ids[0], self.head_id)

    def test_local_to_local(self):
        """LAN worker drives a function back on the local node.

        With a second local node it's lan->lan; otherwise a self-dispatch.
        Either way the target is directly reachable, so it never relays."""
        self._need("local")
        target = self.local_ids[1] if len(self.local_ids) > 1 else self.local_ids[0]
        self._cross(self.local_ids[0], target)

    def test_local_to_remote(self):
        """LAN worker drives a function on a tunnelled remote node.

        Works via the head DispatchProxy: ``should_proxy`` sees the hard
        NodeAffinity target is a node the LAN worker can't reach directly and
        relays the dispatch through the head, which owns the inner task
        (see chia/base/dispatch_proxy.py)."""
        self._need("local")
        self._need("remote")
        self._cross(self.local_ids[0], self.remote_ids[0])

    def test_remote_to_head(self):
        """Tunnelled worker drives a function on the head (via head proxy)."""
        self._need("remote")
        self._cross(self.remote_ids[0], self.head_id)

    def test_remote_to_local(self):
        """Tunnelled worker drives a function on the LAN node (via head proxy)."""
        self._need("remote")
        self._need("local")
        self._cross(self.remote_ids[0], self.local_ids[0])

    def test_remote_to_remote(self):
        """One tunnelled worker drives a function on another (via head proxy)."""
        self._need("remote2")
        self._cross(self.remote_ids[0], self.remote_ids[1])

    # ===================================================================
    # 2. MCP tool over the tunnel, in every cross-machine direction
    # ===================================================================

    def _tool_on(self, node_id):
        return BashTool(name=_uid(),
                        task_options={"resources": self._pin_resource(node_id)})

    def _expect_tool_host(self, url, method, node_id):
        host = asyncio.run(_call(url, method, {"command": "hostname"})).strip()
        self.assertEqual(host, self.host_by_id.get(node_id),
                         f"tool reported {host}, expected {self.host_by_id.get(node_id)}")
        return host

    def _worker_calls_tool(self, worker_node_id, url, method, command="hostname",
                           timeout=_TIMEOUT):
        result, worker_host = get(_cf_tool_call.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(worker_node_id, soft=False)
        ).chia_remote(url, method, {"command": command}), timeout=timeout)
        self.assertEqual(worker_host, self.host_by_id.get(worker_node_id),
                         f"caller ran on {worker_host}, expected "
                         f"{self.host_by_id.get(worker_node_id)}")
        return result.strip()

    def test_tool_host_to_remote(self):
        """Driver (head, no tunnel) calls a BashTool on a tunnelled remote node."""
        self._need("remote")
        tool = self._tool_on(self.remote_ids[0])
        url = _url(tool)
        asyncio.run(_wait_ready(url))
        self._expect_tool_host(url, _tmethod(tool), self.remote_ids[0])
        self.assertFalse(tool.hostname.startswith("127."),
                         f"tunnelled tool should advertise head IP, got {tool.hostname}")
        tool.stop()

    def test_tool_remote_to_head(self):
        """ChiaFunction on a tunnelled remote node calls a BashTool on the head."""
        self._need("remote")
        tool = self._tool_on(self.head_id)
        url, method = _url(tool), _tmethod(tool)
        asyncio.run(_wait_ready(url))
        self._expect_tool_host(url, method, self.head_id)
        result = self._worker_calls_tool(self.remote_ids[0], url, method)
        self.assertEqual(result, self.host_by_id.get(self.head_id))
        tool.stop()

    def test_tool_head_worker_to_remote(self):
        """ChiaFunction on the head calls a BashTool on a tunnelled remote node."""
        self._need("remote")
        tool = self._tool_on(self.remote_ids[0])
        url, method = _url(tool), _tmethod(tool)
        asyncio.run(_wait_ready(url))
        result = self._worker_calls_tool(self.head_id, url, method)
        self.assertEqual(result, self.host_by_id.get(self.remote_ids[0]))
        tool.stop()

    def test_tool_local_to_remote(self):
        """ChiaFunction on the local node calls a tool on a remote node."""
        self._need("local")
        self._need("remote")
        tool = self._tool_on(self.remote_ids[0])
        url, method = _url(tool), _tmethod(tool)
        asyncio.run(_wait_ready(url))
        result = self._worker_calls_tool(self.local_ids[0], url, method)
        self.assertEqual(result, self.host_by_id.get(self.remote_ids[0]))
        tool.stop()

    def test_tool_remote_to_remote(self):
        """ChiaFunction on remote A calls a BashTool deployed on remote B."""
        self._need("remote2")
        tool_b = self._tool_on(self.remote_ids[1])
        url_b, method_b = _url(tool_b), _tmethod(tool_b)
        asyncio.run(_wait_ready(url_b))
        self._expect_tool_host(url_b, method_b, self.remote_ids[1])
        result = self._worker_calls_tool(self.remote_ids[0], url_b, method_b)
        self.assertEqual(result, self.host_by_id.get(self.remote_ids[1]))
        tool_b.stop()

    @unittest.expectedFailure
    def test_tool_remote_to_local(self):
        """ChiaFunction on a tunnelled remote node calls a BashTool on the LAN
        node

        Unsupported, unlike remote->local *Ray dispatch* (which works via the
        head DispatchProxy). Tool calls are plain HTTP routed by
        ``resolve_tool_url``, which only relays tools that advertise the head's
        IP (head-resident or tunnelled-worker tools). A tool on a non-head LAN
        node advertises it's own address, has no relay/tunnel reachable from a
        remote, so the call can't connect -> expected failure. (Supported
        pattern: put the tool on the head, see test_tool_remote_to_head.)"""
        self._need("remote")
        self._need("local")
        tool = self._tool_on(self.local_ids[0])
        try:
            url, method = _url(tool), _tmethod(tool)
            asyncio.run(_wait_ready(url))                      
            self._expect_tool_host(url, method, self.local_ids[0])
            result = self._worker_calls_tool(self.remote_ids[0], url, method,
                                              timeout=60)
            self.assertEqual(result, self.host_by_id.get(self.local_ids[0]))
        finally:
            tool.stop()

    def test_tool_echo_roundtrip(self):
        """Data integrity through the tunnel (not just hostname)."""
        self._need("remote")
        tool = self._tool_on(self.remote_ids[0])
        url = _url(tool)
        asyncio.run(_wait_ready(url))
        payload = f"tunnel-{uuid.uuid4().hex}"
        result = asyncio.run(_call(url, _tmethod(tool),
                                   {"command": f"echo {payload}"})).strip()
        self.assertEqual(result, payload)
        tool.stop()


if __name__ == "__main__":
    unittest.main()
