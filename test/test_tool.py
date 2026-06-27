"""
Integration tests for Chia BashTool infrastructure.

Uses a single cluster config with dedicated node types for each scenario.
Tests auto-skip when required resources are absent from the cluster.

Node types:
  Local (same machine as test host):
    node_a  — bare-metal   (resource: node_a)
    node_b  — bare-metal   (resource: node_b)
    node_c  — Docker       (resource: node_c)
    node_d  — Docker       (resource: node_d)
  Remote (different machine from test host):
    node_e  — bare-metal   (resource: node_e)
    node_g  — Docker       (resource: node_g)

Test case -> node mapping:
   1. Host -> node_a            (host calls local bare-metal)
   2. Host -> node_c            (host calls local Docker)
   3. Host -> node_e            (host calls remote bare-metal)
   4. Host -> node_g            (host calls remote Docker)
   5. node_a <-> node_b         (local bare <-> local bare)
   6. node_a <-> node_c         (local bare <-> local Docker)
   7. node_c <-> node_c         (local Docker <-> same Docker, two tools)
   8. node_c <-> node_d         (local Docker <-> local Docker, different containers)
   9. node_a <-> node_e         (local bare <-> remote bare)
  10. node_c <-> node_e         (local Docker <-> remote bare)
  11. node_c <-> node_g         (local Docker <-> remote Docker)

Run:
  chia up test/test_tool_cluster.yaml
  python -m pytest test/test_tool.py -v

Or run with python:
  python test/test_tool.py TestHostToNode
  python test/test_tool.py TestHostToNode.test_local_bare
"""

import asyncio
import os
import socket
import unittest
import uuid


import ray
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from ray import serve

from chia.base.tools.BashTool import BashTool
from chia.base.tools.ChiaTool import ChiaTool

HOST_HOSTNAME = socket.gethostname()

# Expected hostnames per resource (Docker hostnames from --hostname in cluster YAML).
EXPECTED_HOSTNAMES = {
    "node_a": HOST_HOSTNAME,
    "node_b": HOST_HOSTNAME,
    "node_c": "chia-test-node-c",
    "node_d": "chia-test-node-d",
    # node_e: remote bare-metal — discovered at runtime in setUpClass
    "node_g": "chia-test-node-g",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _uid(prefix: str) -> str:
    """Unique tool name to avoid port/route collisions between tests."""
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _url(tool: BashTool) -> str:
    return f"http://{tool.hostname}:{tool.port}/{tool.name}/mcp"


def _method(tool: BashTool) -> str:
    return f"{tool.name}_run_command"


async def _wait_ready(url: str, retries: int = 30, delay: float = 0.5):
    """Poll an MCP endpoint until it responds to ``initialize``."""
    for _ in range(retries):
        try:
            async with streamable_http_client(url) as (r, w, _):
                async with ClientSession(r, w) as sess:
                    await sess.initialize()
                    return
        except Exception:
            await asyncio.sleep(delay)
    raise RuntimeError(f"MCP server at {url} not ready after {retries} attempts")


async def _call(url: str, method: str, args: dict) -> str:
    """Call an MCP tool and return the text of the first content block."""
    async with streamable_http_client(url) as (r, w, _):
        async with ClientSession(r, w) as sess:
            await sess.initialize()
            result = await sess.call_tool(method, arguments=args)
            return result.content[0].text


def hostname_via_tool(tool: BashTool) -> str:
    """Call ``hostname`` through *tool* from the test-host process."""
    url = _url(tool)
    asyncio.run(_wait_ready(url))
    return asyncio.run(_call(url, _method(tool), {"command": "hostname"})).strip()


@ray.remote
def _remote_hostname(url: str, method: str) -> str:
    """Execute an MCP ``hostname`` call inside a Ray worker."""
    import asyncio as _aio
    from mcp import ClientSession as _CS
    from mcp.client.streamable_http import streamable_http_client as _http

    async def _do():
        async with _http(url) as (r, w, _):
            async with _CS(r, w) as sess:
                await sess.initialize()
                res = await sess.call_tool(method, arguments={"command": "hostname"})
                return res.content[0].text

    return _aio.run(_do())


def hostname_via_remote(tool: BashTool, caller_resource: str) -> str:
    """Call ``hostname`` on *tool* from a Ray worker placed on *caller_resource*'s node."""
    ref = (
        _remote_hostname
        .options(resources={caller_resource: 0.1})
        .remote(_url(tool), _method(tool))
    )
    return ray.get(ref).strip()


def _has_resource(name: str) -> bool:
    return ray.cluster_resources().get(name, 0) > 0


def _require(*resources: str):
    """Skip the current test if any listed resources are missing."""
    missing = [r for r in resources if not _has_resource(r)]
    if missing:
        raise unittest.SkipTest(f"cluster missing resources: {', '.join(missing)}")


def _make_tool(resource: str) -> BashTool:
    return BashTool(name=_uid("bash"), task_options={'resources': {resource: 0.5}})


@ray.remote
def _get_hostname_on_resource(resource: str) -> str:
    """Run ``hostname`` on the node that owns *resource*."""
    return socket.gethostname()


def _discover_remote_hostnames():
    """Discover bare-metal hostnames for remote nodes that aren't known statically."""
    if _has_resource("node_e") and "node_e" not in EXPECTED_HOSTNAMES:
        ref = _get_hostname_on_resource.options(resources={"node_e": 0.1}).remote("node_e")
        EXPECTED_HOSTNAMES["node_e"] = ray.get(ref)


# ---------------------------------------------------------------------------
# Base test class
# ---------------------------------------------------------------------------


class _Base(unittest.TestCase):
    _ray_started = False

    @classmethod
    def setUpClass(cls):
        if not ray.is_initialized():
            ray.init(address="auto", log_to_driver=True, runtime_env={"working_dir": os.path.dirname(os.path.dirname(__file__))})
            cls._ray_started = True
        _discover_remote_hostnames()

    @classmethod
    def tearDownClass(cls):
        if cls._ray_started:
            ChiaTool._serve_started = False
            ray.shutdown()


def _expected(resource: str) -> str:
    return EXPECTED_HOSTNAMES[resource]


# ===================================================================
# Host -> Node
# ===================================================================


class TestHostToNode(_Base):
    """Test host process calling a BashTool on various node types."""

    def test_local_bare(self):
        """Case 1: Host -> local bare-metal node."""
        _require("node_a")
        tool = _make_tool("node_a")
        result = hostname_via_tool(tool)
        self.assertEqual(result, _expected("node_a"))

    def test_local_docker(self):
        """Case 2: Host -> local Docker node."""
        _require("node_c")
        tool = _make_tool("node_c")
        result = hostname_via_tool(tool)
        self.assertEqual(result, _expected("node_c"))

    def test_remote_bare(self):
        """Case 3: Host -> remote bare-metal node."""
        _require("node_e")
        tool = _make_tool("node_e")
        result = hostname_via_tool(tool)
        self.assertEqual(result, _expected("node_e"))

    def test_remote_docker(self):
        """Case 4: Host -> remote Docker node."""
        _require("node_g")
        tool = _make_tool("node_g")
        result = hostname_via_tool(tool)
        self.assertEqual(result, _expected("node_g"))


# ===================================================================
# Cross-node calls
# ===================================================================


class TestCrossNode(_Base):
    """Two nodes call each other's BashTool via MCP."""

    def _cross_call(self, res_x, res_y):
        """Create tools on two nodes, get baselines, then cross-call.

        Returns (host_x, host_y, x_calls_y, y_calls_x).
        """
        tool_x = _make_tool(res_x)
        tool_y = _make_tool(res_y)

        host_x = hostname_via_tool(tool_x)
        host_y = hostname_via_tool(tool_y)

        x_calls_y = hostname_via_remote(tool_y, caller_resource=res_x)
        y_calls_x = hostname_via_remote(tool_x, caller_resource=res_y)

        return host_x, host_y, x_calls_y, y_calls_x

    # -- Same machine ---------------------------------------------------

    def test_local_bare_bare(self):
        """Case 5: local bare-metal <-> local bare-metal."""
        _require("node_a", "node_b")
        host_a, host_b, a2b, b2a = self._cross_call("node_a", "node_b")
        self.assertEqual(host_a, _expected("node_a"))
        self.assertEqual(host_b, _expected("node_b"))
        self.assertEqual(a2b, host_b)
        self.assertEqual(b2a, host_a)

    def test_local_bare_docker(self):
        """Case 6: local bare-metal <-> local Docker."""
        _require("node_a", "node_c")
        host_a, host_c, a2c, c2a = self._cross_call("node_a", "node_c")
        self.assertEqual(host_a, _expected("node_a"))
        self.assertEqual(host_c, _expected("node_c"))
        self.assertEqual(a2c, host_c)
        self.assertEqual(c2a, host_a)

    def test_local_same_docker(self):
        """Case 7: local Docker <-> local Docker."""
        _require("node_c", "node_c")
        host_c, host_d, c2d, d2c = self._cross_call("node_c", "node_c")
        self.assertEqual(host_c, _expected("node_c"))
        self.assertEqual(host_d, _expected("node_c"))
        self.assertEqual(c2d, host_d)
        self.assertEqual(d2c, host_c)
    
    def test_local_docker_docker(self):
        """Case 7: local Docker <-> local Docker."""
        _require("node_c", "node_d")
        host_c, host_d, c2d, d2c = self._cross_call("node_c", "node_d")
        self.assertEqual(host_c, _expected("node_c"))
        self.assertEqual(host_d, _expected("node_d"))
        self.assertEqual(c2d, host_d)
        self.assertEqual(d2c, host_c)

    # -- Different machines ---------------------------------------------

    def test_diff_bare_bare(self):
        """Case 8: local bare-metal <-> remote bare-metal."""
        _require("node_a", "node_e")
        host_a, host_e, a2e, e2a = self._cross_call("node_a", "node_e")
        self.assertEqual(host_a, _expected("node_a"))
        self.assertEqual(host_e, _expected("node_e"))
        self.assertEqual(a2e, host_e)
        self.assertEqual(e2a, host_a)

    def test_diff_docker_bare(self):
        """Case 9: local Docker <-> remote bare-metal."""
        _require("node_c", "node_e")
        host_c, host_e, c2e, e2c = self._cross_call("node_c", "node_e")
        self.assertEqual(host_c, _expected("node_c"))
        self.assertEqual(host_e, _expected("node_e"))
        self.assertEqual(c2e, host_e)
        self.assertEqual(e2c, host_c)

    def test_diff_docker_docker(self):
        """Case 10: local Docker <-> remote Docker."""
        _require("node_c", "node_g")
        host_c, host_g, c2g, g2c = self._cross_call("node_c", "node_g")
        self.assertEqual(host_c, _expected("node_c"))
        self.assertEqual(host_g, _expected("node_g"))
        self.assertNotEqual(host_c, host_g)
        self.assertEqual(c2g, host_g)
        self.assertEqual(g2c, host_c)


# ===================================================================
# Tool lifecycle (start / stop / port reuse)
# ===================================================================


class TestToolLifecycle(_Base):
    """Verify that stop() actually tears down the uvicorn server."""

    def test_stop_makes_server_unreachable(self):
        """After stop(), the MCP endpoint should be unreachable."""
        _require("node_c")
        tool = _make_tool("node_c")
        url = _url(tool)

        # Verify server is running
        asyncio.run(_wait_ready(url))
        result = asyncio.run(_call(url, _method(tool), {"command": "echo alive"}))
        self.assertIn("alive", result)

        # Stop the tool
        tool.stop()

        # Verify server is no longer reachable
        import time
        time.sleep(1)
        with self.assertRaises(Exception):
            asyncio.run(_call(url, _method(tool), {"command": "echo dead"}))

    def test_port_reuse_after_stop(self):
        """After stop(), a new tool should be able to bind the freed port."""
        _require("node_c")
        tool1 = _make_tool("node_c")
        port1 = tool1.port
        hostname1 = tool1.hostname
        tool1.stop()

        import time
        time.sleep(1)

        # Create a second tool — it should reuse the freed port.
        tool2 = _make_tool("node_c")
        self.assertEqual(tool2.port, port1,
                         f"Expected port {port1} to be reused, got {tool2.port}")
        asyncio.run(_wait_ready(_url(tool2)))
        result = asyncio.run(_call(_url(tool2), _method(tool2), {"command": "echo ok"}))
        self.assertIn("ok", result)
        tool2.stop()

    def test_serialization_excludes_actor(self):
        """When a tool is serialized by Ray, _server_actor should be None."""
        _require("node_c")
        tool = _make_tool("node_c")

        @ray.remote
        def check_tool(t):
            return t.name, t.hostname, t.port, t._server_actor

        name, host, port, actor = ray.get(check_tool.remote(tool))
        self.assertEqual(name, tool.name)
        self.assertEqual(host, tool.hostname)
        self.assertEqual(port, tool.port)
        self.assertIsNone(actor)
        tool.stop()

    def test_double_stop_is_safe(self):
        """Calling stop() twice should not raise."""
        _require("node_c")
        tool = _make_tool("node_c")
        asyncio.run(_wait_ready(_url(tool)))
        tool.stop()
        tool.stop()  # should be a no-op


if __name__ == "__main__":
    unittest.main()
