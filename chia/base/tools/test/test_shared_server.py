import asyncio
import importlib
import socket
from types import SimpleNamespace

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
import ray.cloudpickle

from chia.base.tools.BashTool import BashTool

module = importlib.import_module("chia.base.tools.ChiaTool")


def _port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_shared_server_keeps_workspaces_and_shutdown_independent(tmp_path, monkeypatch):
    actor = module._ToolServerActor.__ray_metadata__.modified_class()
    killed = []
    monkeypatch.setattr(module.ray, "get", lambda result: result)
    monkeypatch.setattr(module.ray, "kill", lambda handle, **kw: killed.append(handle))
    monkeypatch.setattr(module.ray.util, "get_node_ip_address", lambda: "127.0.0.1")
    monkeypatch.setattr(module.ray, "get_runtime_context", lambda: SimpleNamespace(get_node_id=lambda: "local"))
    handle = SimpleNamespace(
        start=SimpleNamespace(remote=lambda tool: actor.start(ray.cloudpickle.loads(ray.cloudpickle.dumps(tool)))),
        stop=SimpleNamespace(remote=actor.stop),
    )
    deployed = []

    async def read_marker(tool):
        async with streamable_http_client(f"http://{tool.hostname}:{tool.port}/{tool.name}/mcp") as (r, w, _):
            async with ClientSession(r, w) as session:
                await session.initialize()
                result = await session.call_tool(f"{tool.name}_run_command", {"command": "cat marker"})
                assert not result.isError
                return result.content[0].text

    try:
        for name in ("first", "second"):
            directory = tmp_path / name
            directory.mkdir()
            (directory / "marker").write_text(name)
            port = _port()
            tool = BashTool(name, work_dir=str(directory), server_actor=handle, task_options={
                "runtime_env": {"env_vars": {"CHIA_TOOL_BASE_PORT": str(port), "CHIA_TOOL_MAX_PORT": str(port)}},
            })
            deployed.append(tool)
            assert tool.port == port
            assert asyncio.run(read_marker(tool)) == name
        deployed[0].stop()
        assert not killed
        assert asyncio.run(read_marker(deployed[1])) == "second"
    finally:
        module.ChiaTool.stop_server_actor(handle)
    assert killed == [handle]
    assert not actor._names
    for tool in deployed:
        with socket.socket() as client:
            assert client.connect_ex((tool.hostname, tool.port)) != 0
