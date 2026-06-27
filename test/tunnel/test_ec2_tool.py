"""
Integration test for MCP tool calls across SSH tunnels.

Verifies that MCP tool calls work in both directions between
tunnelled EC2 nodes and the local head node.

Test cases:
  1. Host -> EC2 tool          (host process calls tool on tunnelled EC2 node)
  2. Head worker -> EC2 tool   (Ray worker on head calls tool on EC2 node)
  3. EC2 worker -> local tool  (Ray worker on EC2 calls tool on head node)
  4. Bidirectional cross-call  (tools on both sides, each calls the other)
  5. EC2 -> EC2 tool           (one EC2 node calls tool on a different EC2 node)
  6. Echo round-trip           (data integrity through tunnel)
  7. Local worker -> EC2 tool  (Ray worker calls tool on EC2 node)

Cluster setup:
  chia up test/tunnel/test_ec2_cluster.yaml
  python -m pytest test/tunnel/test_ec2_tool.py -v
"""

import asyncio
import os
import socket
import unittest
import uuid

import ray
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from chia.base.tools.BashTool import BashTool
from chia.base.tools.ChiaTool import ChiaTool

HOST_HOSTNAME = socket.gethostname()


def _uid(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _url(tool: BashTool) -> str:
    return f"http://{tool.hostname}:{tool.port}/{tool.name}/mcp"


def _method(tool: BashTool) -> str:
    return f"{tool.name}_run_command"


async def _wait_ready(url: str, retries: int = 30, delay: float = 0.5):
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
    async with streamable_http_client(url) as (r, w, _):
        async with ClientSession(r, w) as sess:
            await sess.initialize()
            result = await sess.call_tool(method, arguments=args)
            return result.content[0].text


def _has_resource(name: str) -> bool:
    return ray.cluster_resources().get(name, 0) > 0


@ray.remote
def _remote_call(tool_url: str, tool_method: str, args: dict) -> tuple[str, str]:
    """Execute an MCP tool call inside a Ray worker.

    Returns ``(tool_result, worker_hostname)`` so callers can verify
    both the tool output and that the worker ran on the expected node.
    """
    import asyncio as _aio
    import socket as _sock
    from mcp import ClientSession as _CS
    from mcp.client.streamable_http import streamable_http_client as _http
    from chia.base.tools.ChiaTool import resolve_tool_url

    worker_hostname = _sock.gethostname()

    async def _do():
        resolved = resolve_tool_url(tool_url)
        async with _http(resolved) as (r, w, _):
            async with _CS(r, w) as sess:
                await sess.initialize()
                res = await sess.call_tool(tool_method, arguments=args)
                return res.content[0].text

    return _aio.run(_do()), worker_hostname


class TestEC2ToolCalls(unittest.TestCase):
    """Test MCP tool calls between non-tunnelled and tunnelled nodes."""

    _ray_started = False

    @classmethod
    def setUpClass(cls):
        if not ray.is_initialized():
            ray.init(
                address="auto",
                runtime_env={
                    "working_dir": os.path.dirname(
                        os.path.dirname(os.path.dirname(__file__))
                    )
                },
            )
            cls._ray_started = True

    @classmethod
    def tearDownClass(cls):
        if cls._ray_started:
            ChiaTool._serve_started = False
            ray.shutdown()

    def _require_ec2(self):
        if not _has_resource("ec2"):
            self.skipTest("cluster missing resource: ec2")

    def test_host_to_ec2_tool(self):
        """Host process (head node, no tunnel) calls BashTool on tunnelled EC2 node."""
        self._require_ec2()

        tool = BashTool(
            name=_uid("bash"),
            task_options={"resources": {"ec2": 0.5}},
        )
        url = _url(tool)
        asyncio.run(_wait_ready(url))

        # Call hostname — should return the EC2 instance hostname
        result = asyncio.run(
            _call(url, _method(tool), {"command": "hostname"})
        ).strip()
        print(f"EC2 tool hostname: {result}")
        self.assertTrue(
            result.startswith("ip-"),
            f"Expected EC2 hostname starting with 'ip-', got: {result}",
        )

        # With head-as-hub routing, tunnelled tools advertise the head's
        # resolved IP (not a tunnel loopback).
        self.assertFalse(
            tool.hostname.startswith("127."),
            f"Tunneled tool should advertise head IP, not loopback. Got: {tool.hostname}",
        )

        tool.stop()

    def test_head_worker_to_ec2_tool(self):
        """Ray worker on the head node (no tunnel) calls BashTool on tunnelled EC2 node."""
        self._require_ec2()

        tool = BashTool(
            name=_uid("bash"),
            task_options={"resources": {"ec2": 0.5}},
        )
        url = _url(tool)
        method = _method(tool)
        asyncio.run(_wait_ready(url))

        # Run the MCP call from a Ray worker pinned to the head node.
        ref = (
            _remote_call
            .options(resources={"node:__internal_head__": 0.001})
            .remote(url, method, {"command": "hostname"})
        )
        result, worker_host = ray.get(ref)
        result = result.strip()
        print(f"EC2 tool hostname (via head worker): {result}")
        self.assertEqual(
            worker_host, HOST_HOSTNAME,
            f"Worker should be on head node ({HOST_HOSTNAME}), ran on: {worker_host}",
        )
        self.assertTrue(
            result.startswith("ip-"),
            f"Expected EC2 hostname starting with 'ip-', got: {result}",
        )

        tool.stop()

    def test_ec2_to_local_tool(self):
        """Ray worker on tunnelled EC2 node calls BashTool on the head node (no tunnel)."""
        self._require_ec2()

        # Pin the tool to the head node so it doesn't land on an EC2 worker.
        local_tool = BashTool(
            name=_uid("bash"),
            task_options={"resources": {"node:__internal_head__": 0.001}},
        )
        local_url = _url(local_tool)
        local_method = _method(local_tool)
        asyncio.run(_wait_ready(local_url))

        # Verify the tool is actually on the head node.
        local_hostname = asyncio.run(
            _call(local_url, local_method, {"command": "hostname"})
        ).strip()
        self.assertEqual(
            local_hostname, HOST_HOSTNAME,
            f"Local tool should be on head node ({HOST_HOSTNAME}), got: {local_hostname}",
        )

        # Now call that local tool from a Ray worker on the EC2 node.
        ref = (
            _remote_call
            .options(resources={"ec2": 0.1})
            .remote(local_url, local_method, {"command": "hostname"})
        )
        result, worker_host = ray.get(ref)
        result = result.strip()
        print(f"Local tool hostname (called from EC2): {result}")
        self.assertTrue(
            worker_host.startswith("ip-"),
            f"Worker should be on EC2 (hostname starting with 'ip-'), ran on: {worker_host}",
        )
        self.assertEqual(
            result, HOST_HOSTNAME,
            f"EC2 worker calling local tool should get head hostname ({HOST_HOSTNAME}), got: {result}",
        )

        local_tool.stop()

    def test_bidirectional_cross_call(self):
        """Tools on both sides, each node calls the other's tool."""
        self._require_ec2()

        # Tool on EC2 (tunnelled).
        ec2_tool = BashTool(
            name=_uid("bash"),
            task_options={"resources": {"ec2": 0.5}},
        )
        # Tool on head (no tunnel), pinned to head node.
        local_tool = BashTool(
            name=_uid("bash"),
            task_options={"resources": {"node:__internal_head__": 0.001}},
        )

        ec2_url = _url(ec2_tool)
        ec2_method = _method(ec2_tool)
        local_url = _url(local_tool)
        local_method = _method(local_tool)
        asyncio.run(_wait_ready(ec2_url))
        asyncio.run(_wait_ready(local_url))

        # Baseline: verify each tool reports the expected hostname.
        ec2_hostname = asyncio.run(
            _call(ec2_url, ec2_method, {"command": "hostname"})
        ).strip()
        local_hostname = asyncio.run(
            _call(local_url, local_method, {"command": "hostname"})
        ).strip()
        self.assertTrue(ec2_hostname.startswith("ip-"))
        self.assertEqual(local_hostname, HOST_HOSTNAME)

        # Head worker calls EC2 tool.
        head_result, head_worker = ray.get(
            _remote_call
            .options(resources={"node:__internal_head__": 0.001})
            .remote(ec2_url, ec2_method, {"command": "hostname"})
        )
        self.assertEqual(head_worker, HOST_HOSTNAME,
                         f"Worker should be on head, ran on: {head_worker}")
        self.assertEqual(head_result.strip(), ec2_hostname)

        # EC2 worker calls local tool.
        ec2_result, ec2_worker = ray.get(
            _remote_call
            .options(resources={"ec2": 0.1})
            .remote(local_url, local_method, {"command": "hostname"})
        )
        self.assertTrue(ec2_worker.startswith("ip-"),
                        f"Worker should be on EC2, ran on: {ec2_worker}")
        self.assertEqual(ec2_result.strip(), local_hostname)

        ec2_tool.stop()
        local_tool.stop()

    def test_ec2_to_ec2_tool(self):
        """Tool on one EC2 node called from a Ray worker on a different EC2 node."""
        self._require_ec2()

        # Find two distinct EC2 node IPs.
        ec2_ips = []
        for n in ray.nodes():
            if n.get("Alive") and n.get("Resources", {}).get("ec2", 0) > 0:
                ec2_ips.append(n["NodeName"])
        if len(ec2_ips) < 2:
            self.skipTest(f"need >= 2 EC2 nodes, found {len(ec2_ips)}")

        ip_a, ip_b = ec2_ips[0], ec2_ips[1]

        # Create a tool pinned to each EC2 node.
        tool_a = BashTool(
            name=_uid("bash"),
            task_options={"resources": {f"node:{ip_a}": 0.001, "ec2": 0.1}},
        )
        tool_b = BashTool(
            name=_uid("bash"),
            task_options={"resources": {f"node:{ip_b}": 0.001, "ec2": 0.1}},
        )
        url_a, method_a = _url(tool_a), _method(tool_a)
        url_b, method_b = _url(tool_b), _method(tool_b)
        asyncio.run(_wait_ready(url_a))
        asyncio.run(_wait_ready(url_b))

        # Baseline: each tool is on a different EC2 host.
        host_a = asyncio.run(_call(url_a, method_a, {"command": "hostname"})).strip()
        host_b = asyncio.run(_call(url_b, method_b, {"command": "hostname"})).strip()
        self.assertNotEqual(host_a, host_b, "Tools should be on different EC2 nodes")

        # EC2 node A calls tool on EC2 node B.
        a_result, a_worker = ray.get(
            _remote_call
            .options(resources={f"node:{ip_a}": 0.001})
            .remote(url_b, method_b, {"command": "hostname"})
        )
        self.assertEqual(a_worker, host_a,
                         f"Worker should be on EC2 node A ({host_a}), ran on: {a_worker}")
        self.assertEqual(a_result.strip(), host_b)

        # EC2 node B calls tool on EC2 node A.
        b_result, b_worker = ray.get(
            _remote_call
            .options(resources={f"node:{ip_b}": 0.001})
            .remote(url_a, method_a, {"command": "hostname"})
        )
        self.assertEqual(b_worker, host_b,
                         f"Worker should be on EC2 node B ({host_b}), ran on: {b_worker}")
        self.assertEqual(b_result.strip(), host_a)

        tool_a.stop()
        tool_b.stop()

    def test_ec2_tool_echo_roundtrip(self):
        """Verify data makes a clean round-trip through the tunnel (not just hostname)."""
        self._require_ec2()

        tool = BashTool(
            name=_uid("bash"),
            task_options={"resources": {"ec2": 0.5}},
        )
        url = _url(tool)
        asyncio.run(_wait_ready(url))

        payload = f"tunnel-test-{uuid.uuid4().hex}"
        result = asyncio.run(
            _call(url, _method(tool), {"command": f"echo {payload}"})
        ).strip()
        self.assertEqual(result, payload)

        tool.stop()

    def test_local_worker_to_ec2_tool(self):
        """Ray worker on local node (non-head) calls BashTool on tunnelled EC2 node."""
        self._require_ec2()
        if not _has_resource("local"):
            self.skipTest("cluster missing resource: local (local_worker)")

        # Deploy a BashTool on an EC2 node.
        ec2_tool = BashTool(
            name=_uid("bash"),
            task_options={"resources": {"ec2": 0.5}},
        )
        url = _url(ec2_tool)
        method = _method(ec2_tool)
        asyncio.run(_wait_ready(url))

        # Call from a Ray worker pinned via the local resource.
        ref = (
            _remote_call
            .options(resources={"local": 0.1})
            .remote(url, method, {"command": "hostname"})
        )
        result, worker_host = ray.get(ref)
        result = result.strip()
        print(f"EC2 tool hostname (called from local worker): {result}")
        self.assertNotEqual(
            worker_host, HOST_HOSTNAME,
            f"Worker should be on local but not head (not head {HOST_HOSTNAME})",
        )
        self.assertFalse(
            worker_host.startswith("ip-"),
            f"Worker should be on local, not EC2. Ran on: {worker_host}",
        )
        self.assertTrue(
            result.startswith("ip-"),
            f"Expected EC2 hostname starting with 'ip-', got: {result}",
        )

        ec2_tool.stop()


if __name__ == "__main__":
    unittest.main()
