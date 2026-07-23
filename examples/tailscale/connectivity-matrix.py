"""Full connectivity matrix for a tailnet cluster.

Exercises every machine-to-machine path in the cluster, in both of
CHIA's communication styles:

1. **ChiaFunctions** — FROM each machine, dispatch a ChiaFunction pinned
   TO each machine (including itself) and collect the result: N x N runs
   exercising nested scheduling, argument shipping, and result return
   across every ordered pair of machines.
2. **ChiaTools** — host a BashTool on each machine, then FROM each
   machine call every tool over its MCP HTTP endpoint: N x N real tool
   invocations across the relay mesh.

Machines are identified by the custom Ray resources the example configs
advertise: ``head`` (the head's `ray start`), ``tailscale_worker``,
``ec2_worker``, and ``ec2_docker`` (a second, dockerized logical worker
sharing the EC2 host). Only the tags present in the running cluster are
swept, so this works on any of the example clusters. Two tags on one
physical machine (``ec2_worker`` + ``ec2_docker``) still get their own
row/column — the sweep verifies each logical worker independently.

Run from the head machine (after ``chia up`` of any example config):

    export RAY_ADDRESS=127.200.0.1:6379
    python examples/tailscale/connectivity-matrix.py
"""
import asyncio
import os
import socket
import sys

import ray

from chia.base.ChiaFunction import ChiaFunction, get
from chia.base.tools.BashTool import BashTool

MACHINE_TAGS = ["head", "tailscale_worker", "ec2_worker", "ec2_docker"]
# Tiny resource slices so a dispatcher pinned to a machine can nest a
# probe pinned to the same machine without exhausting its resource tag.
FRACTION = 0.05


@ChiaFunction()
def probe() -> str:
    return socket.gethostname()


@ChiaFunction()
def dispatch_probes(to_tags: list) -> dict:
    """Runs ON one machine; dispatches a pinned probe TO every machine."""
    row = {}
    for to in to_tags:
        row[to] = get(probe.options(resources={to: FRACTION}).chia_remote())
    return {"from_host": socket.gethostname(), "row": row}


async def _call_tool(url: str, tool_name: str, args: dict) -> str:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client
    async with streamablehttp_client(url) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(tool_name, args)
            return result.content[0].text


@ChiaFunction()
def call_tools(tools: dict) -> dict:
    """Runs ON one machine; invokes every machine's BashTool over MCP."""
    row = {}
    for to, (url, call_name) in tools.items():
        out = asyncio.run(_call_tool(url, call_name, {"command": "hostname"}))
        row[to] = out.strip().splitlines()[-1]
    return {"from_host": socket.gethostname(), "row": row}


def _print_matrix(title: str, tags: list, rows: dict):
    width = max(len(t) for t in tags) + 2
    print(f"\n=== {title} ===")
    print(" " * 18 + "".join(f"TO {t:<{width}}" for t in tags))
    for frm in tags:
        cells = "".join(f"{rows[frm]['row'][t]:<{width + 3}}" for t in tags)
        print(f"FROM {frm:<12} {cells}   (ran on {rows[frm]['from_host']})")


def main():
    ray.init(address=os.environ.get("RAY_ADDRESS", "auto"),
             ignore_reinit_error=True)
    tags = [t for t in MACHINE_TAGS if ray.cluster_resources().get(t, 0) > 0]
    if len(tags) < 2:
        print(f"Need >=2 machine resource tags, found: {tags}. "
              f"Are the example configs' --resources in place?")
        sys.exit(1)
    print(f"Machines present: {tags} ({len(tags)}x{len(tags)} matrix)")

    # --- Part 1: ChiaFunctions from each machine to each machine ---
    fn_rows = {}
    for frm in tags:
        fn_rows[frm] = get(dispatch_probes.options(
            resources={frm: FRACTION}).chia_remote(tags))
    _print_matrix("ChiaFunction matrix (cell = probe's hostname)", tags, fn_rows)

    # --- Part 2: a BashTool hosted on each machine, called from each ---
    tools_by_tag = {}
    handles = []
    for tag in tags:
        tool = BashTool(f"conn_{tag}", work_dir="/tmp", timeout_seconds=60,
                        task_options={"resources": {tag: FRACTION},
                                      "num_cpus": 0})
        handles.append(tool)
        tools_by_tag[tag] = (
            f"http://{tool.hostname}:{tool.port}/{tool.name}/mcp",
            f"{tool.name}_run_command",
        )
        print(f"tool conn_{tag} hosted at {tools_by_tag[tag][0]}")

    tool_rows = {}
    for frm in tags:
        tool_rows[frm] = get(call_tools.options(
            resources={frm: FRACTION}).chia_remote(tools_by_tag))
    _print_matrix("ChiaTool matrix (cell = `hostname` via each tool)",
                  tags, tool_rows)

    for tool in handles:
        tool.stop()

    # --- Verdict ---
    hosts = {t: fn_rows[t]["from_host"] for t in tags}
    ok = True
    for frm in tags:
        for to in tags:
            for rows in (fn_rows, tool_rows):
                if rows[frm]["row"][to] != hosts[to]:
                    print(f"MISMATCH {frm}->{to}: {rows[frm]['row'][to]!r} "
                          f"!= {hosts[to]!r}")
                    ok = False
    total = 2 * len(tags) * len(tags)
    if ok:
        print(f"\nCONNECTIVITY MATRIX PASSED: {total} cross-machine "
              f"interactions verified across {len(tags)} machines")
    else:
        print("\nCONNECTIVITY MATRIX FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
