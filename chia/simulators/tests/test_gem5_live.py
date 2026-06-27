"""Live tests for chia.simulators.gem5 — Gem5Node + Gem5ToolServer.

Runs against a real Ray cluster: every Gem5Node function is exercised through
real ``chia_remote`` dispatch, and every Gem5ToolServer tool through a real MCP
round-trip to the tool actor.  To avoid contending for the scarce ``gem5``
resource (and a ~30-min real gem5 build), Tier-1 stages a SYNTHETIC gem5
environment on the bundle's worker — a fake ``gem5.opt`` shell script that emits
a canned ``stats.txt`` plus a throwaway git repo — so it validates *our* wrapper
logic end-to-end without a real simulator.

Configuration (env vars):
  GEM5_TEST_RAY_ADDRESS   Ray head address
  GEM5_TEST_RESOURCE      bundle/run resource label  (default "gem5";
                          set "CPU" to run Tier 1 on any free node right now)
  GEM5_TEST_PG_TIMEOUT    seconds to wait for a free bundle (default 60)
  GEM5_TEST_REAL          "1" to run the gated real-gem5 smoke tier (Tier 2)
  GEM5_TEST_GEM5_ROOT     real gem5 checkout    (Tier 2; e.g. /home/ray/gem5)
  GEM5_TEST_CONFIG        real gem5 config .py  (Tier 2)
  GEM5_TEST_WORKLOAD_DIR  dir of real workload ELFs (Tier 2)

Run:
  GEM5_TEST_RESOURCE=CPU pytest chia/chia/simulators/tests/test_gem5_live.py -v
"""

from __future__ import annotations

import asyncio
import gzip
import os

import pytest
import ray
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

from chia.base.ChiaFunction import get
from chia.simulators.gem5 import (
    Gem5Node, Gem5ToolServer, Gem5BuildArtifact, Gem5RunResult, Gem5SourceState,
)

RES = os.environ.get("GEM5_TEST_RESOURCE", "gem5")
RAY_ADDR = os.environ.get("GEM5_TEST_RAY_ADDRESS")
PG_TIMEOUT = float(os.environ.get("GEM5_TEST_PG_TIMEOUT", "60"))

# Ship the local chia repo to workers so chia.simulators is importable even when
# the worker image's baked-in chia predates this module (chia is a namespace
# package, so the uploaded copy's `simulators` merges onto the installed one).
# Set GEM5_TEST_WORKING_DIR="" to disable (cluster already has this module).
import chia as _chia
_DEFAULT_WORKING_DIR = os.path.dirname(list(_chia.__path__)[0])  # repo root above chia/
WORKING_DIR = os.environ.get("GEM5_TEST_WORKING_DIR", _DEFAULT_WORKING_DIR)


# ===========================================================================
# Tier 0 — pure functions (no cluster)
# ===========================================================================

_TWO_BLOCK_STATS = (
    "---------- Begin Simulation Statistics ----------\n"
    "simInsts                  5000\n"
    "system.cpu.numCycles    150000\n"
    "simSeconds            0.000123\n"
    "hostSeconds               2.50\n"
    "---------- End Simulation Statistics   ----------\n"
    "---------- Begin Simulation Statistics ----------\n"
    "simInsts                    10\n"
    "system.cpu.numCycles        42\n"
    "---------- End Simulation Statistics   ----------\n"
)


def test_parse_stats_block_selection():
    assert Gem5Node.parse_gem5_stats(_TWO_BLOCK_STATS, stats_block="first") == {
        "cycles": 150000.0, "insts": 5000.0}
    assert Gem5Node.parse_gem5_stats(_TWO_BLOCK_STATS, stats_block="last") == {
        "cycles": 42.0, "insts": 10.0}
    # negative index == last
    assert Gem5Node.parse_gem5_stats(_TWO_BLOCK_STATS, stats_block=-1)["cycles"] == 42.0


def test_parse_stats_custom_keys_and_misses():
    out = Gem5Node.parse_gem5_stats(
        _TWO_BLOCK_STATS, {"sec": ["simSeconds"], "host": ["hostSeconds"],
                           "absent": ["does.not.exist"]}, stats_block="first")
    assert out == {"sec": 0.000123, "host": 2.5}  # missing key omitted, not None


def test_parse_stats_empty_and_missing_file(tmp_path):
    assert Gem5Node.parse_gem5_stats("no blocks here") == {}
    assert Gem5Node.parse_gem5_stats_file(str(tmp_path / "nope.txt")) == {}


def test_truncate_and_summarize_pipeview(tmp_path):
    lines = []
    for sn in range(1, 6):
        f = sn * 7000
        lines.append(f"O3PipeView:fetch:{f}:0x{sn:x}:0:{sn}:addi x1, x2, {sn}")
        for i, st in enumerate(["decode", "rename", "dispatch", "issue", "complete", "retire"], 1):
            lines.append(f"O3PipeView:{st}:{f + i * 1000}")
    p = str(tmp_path / "pipe_trace.gz")
    with gzip.open(p, "wt") as fh:
        fh.write("\n".join(lines) + "\n")

    summ = Gem5Node.summarize_o3_pipeview(p)
    assert "Instructions traced: 5" in summ
    assert "fetch->decode" in summ and "issue->complete" in summ

    retained, trunc = Gem5Node.truncate_gz_trace(p, max_decompressed_bytes=60)
    assert trunc is True and 0 < retained <= 60
    # still valid gzip after truncation
    with gzip.open(p, "rt") as fh:
        assert fh.read().count("O3PipeView:fetch") >= 1


def test_summarize_missing_trace():
    assert "no trace" in Gem5Node.summarize_o3_pipeview("/no/such/trace.gz").lower()


def test_render_build_strings():
    def art(ok, rc=0, dur=12.0, err=""):
        return Gem5BuildArtifact("b", "RISCV", "opt", "/r", "", ok, rc, dur, "", err)
    assert Gem5ToolServer._render_build(art(True)) == "OK (12s)"
    fail = Gem5ToolServer._render_build(art(False, 2, 5, "x.cc:9: error: bad"))
    assert fail.startswith("FAIL (5s, rc=2):") and "error:" in fail
    to = Gem5ToolServer._render_build(art(False, -1, 3600, "TIMEOUT after 3600s (limit 3600s)"))
    assert to.startswith("TIMEOUT") and "inconclusive" in to


# ===========================================================================
# Cluster fixtures + worker-side synthetic environment
# ===========================================================================

@pytest.fixture(scope="session")
def ray_cx():
    runtime_env = None
    if WORKING_DIR:
        runtime_env = {"working_dir": WORKING_DIR,
                       "excludes": [".git", "**/__pycache__", "**/*.pyc"]}
    try:
        ray.init(address=RAY_ADDR, namespace="gem5_live_test",
                 runtime_env=runtime_env, log_to_driver=False)
    except Exception as e:
        pytest.skip(f"cannot connect to Ray at {RAY_ADDR}: {e}")
    yield
    ray.shutdown()


@pytest.fixture(scope="session")
def bundle(ray_cx):
    shape = {"CPU": 1} if RES == "CPU" else {RES: 1.0, "CPU": 1}
    pg = placement_group([shape], strategy="STRICT_PACK")
    try:
        ray.get(pg.ready(), timeout=PG_TIMEOUT)
    except Exception:
        remove_placement_group(pg)
        pytest.skip(f"no free '{RES}' bundle within {PG_TIMEOUT}s (cluster busy?)")
    yield pg
    remove_placement_group(pg)


def _sched(pg):
    return {"scheduling_strategy": PlacementGroupSchedulingStrategy(
        placement_group=pg, placement_group_bundle_index=0)}


@ray.remote(num_cpus=0)
def _stage_env() -> dict:
    """Build a synthetic gem5 env in a worker-local tempdir and return paths.

    Co-located with the node/tool under test (same bundle), so the files this
    writes are visible to their subprocesses.
    """
    import os
    import shutil
    import stat
    import subprocess
    import tempfile

    root = tempfile.mkdtemp(prefix="gem5_live_")
    gem5_root = os.path.join(root, "gem5")
    os.makedirs(os.path.join(gem5_root, "src"))
    # throwaway git repo (for capture/restore + build base_rev)
    subprocess.run(["git", "init", "-q"], cwd=gem5_root, check=True)
    subprocess.run(["git", "config", "user.email", "t@t"], cwd=gem5_root, check=True)
    subprocess.run(["git", "config", "user.name", "t"], cwd=gem5_root, check=True)
    open(os.path.join(gem5_root, "src", "foo.txt"), "w").write("base\n")
    subprocess.run(["git", "add", "-A"], cwd=gem5_root, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "base"], cwd=gem5_root, check=True)

    # SConstruct that "builds" build/RISCV/gem5.opt (used by build-success test)
    open(os.path.join(gem5_root, "SConstruct"), "w").write(
        "env = Environment()\n"
        "env.Command('build/RISCV/gem5.opt', [],\n"
        "    'mkdir -p build/RISCV && echo binary > $TARGET && chmod +x $TARGET')\n"
    )
    # slow SConstruct variant for the timeout test
    slow_root = os.path.join(root, "gem5_slow")
    os.makedirs(slow_root)
    open(os.path.join(slow_root, "SConstruct"), "w").write(
        "env = Environment()\n"
        "env.Command('build/RISCV/gem5.opt', [], 'sleep 30 && touch $TARGET')\n"
    )

    def _script(name, body):
        p = os.path.join(root, name)
        open(p, "w").write("#!/usr/bin/env bash\n" + body)
        os.chmod(p, os.stat(p).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        return p

    outdir_extract = (
        'outdir=""; dbg=""\n'
        'for a in "$@"; do case "$a" in '
        '--outdir=*) outdir="${a#--outdir=}";; '
        '--debug-file=*) dbg="${a#--debug-file=}";; esac; done\n'
        'mkdir -p "$outdir"\n'
    )
    stats_body = (
        'cat > "$outdir/stats.txt" <<EOF\n'
        "---------- Begin Simulation Statistics ----------\n"
        "simInsts                  1234\n"
        "system.cpu.numCycles      5678\n"
        "simSeconds            0.000005\n"
        "hostSeconds               0.42\n"
        "---------- End Simulation Statistics   ----------\n"
        "EOF\n"
    )
    gem5_ok = _script("gem5_ok", outdir_extract + stats_body +
                      '[ -n "$dbg" ] && echo "O3PipeView:fetch:0" > "$outdir/$dbg"\n'
                      'echo "fake gem5 ok"\n')
    gem5_fail = _script("gem5_fail", outdir_extract + 'echo "boom" >&2\nexit 1\n')
    gem5_timeout = _script("gem5_timeout", outdir_extract + 'sleep 30\n')
    gem5_noparse = _script("gem5_noparse", outdir_extract +
                           'printf -- "---------- Begin Simulation Statistics ----------\\n'
                           'someOtherStat 1\\n'
                           '---------- End Simulation Statistics   ----------\\n" > "$outdir/stats.txt"\n')

    workloads = os.path.join(root, "workloads")
    os.makedirs(workloads)
    for w in ("a", "b"):
        open(os.path.join(workloads, f"{w}.elf"), "wb").write(b"\x7fELF")
    config = os.path.join(root, "config.py")
    open(config, "w").write("# dummy gem5 config\n")

    return {
        "root": root, "gem5_root": gem5_root, "slow_root": slow_root,
        "gem5_ok": gem5_ok, "gem5_fail": gem5_fail,
        "gem5_timeout": gem5_timeout, "gem5_noparse": gem5_noparse,
        "workloads": workloads, "config": config,
        "has_scons": shutil.which("scons") is not None,
    }


@ray.remote(num_cpus=0)
def _mutate_repo(gem5_root: str) -> bool:
    open(os.path.join(gem5_root, "src", "foo.txt"), "w").write("EDITED\n")
    open(os.path.join(gem5_root, "src", "new.txt"), "w").write("brand new\n")
    return True


@ray.remote(num_cpus=0)
def _read_repo(gem5_root: str) -> dict:
    foo = os.path.join(gem5_root, "src", "foo.txt")
    new = os.path.join(gem5_root, "src", "new.txt")
    return {
        "foo": open(foo).read() if os.path.exists(foo) else None,
        "new_exists": os.path.exists(new),
    }


@pytest.fixture(scope="session")
def env(bundle):
    return ray.get(_stage_env.options(**_sched(bundle)).remote())


@pytest.fixture(scope="session")
def node(bundle):
    n = Gem5Node(placement_group=bundle, bundle_index=0)
    yield n
    n.close()


def _remote(node, fn_name, **kwargs):
    """Dispatch a node member via chia_remote, dropping the gem5 resource
    requirement when running on a non-gem5 (CPU) test bundle."""
    fn = getattr(node, fn_name)
    if RES == "CPU":
        return get(fn.options(num_cpus=1, resources={}).chia_remote(**kwargs))
    return get(fn.chia_remote(**kwargs))


# ===========================================================================
# Tier 1a — placement / lifecycle
# ===========================================================================

def test_provided_pg_not_owned(node, bundle):
    assert node.placement_group is bundle
    assert node.owns_placement_group is False
    assert "scheduling_strategy" in node.task_options
    for name in Gem5Node._MEMBER_FNS:
        assert hasattr(getattr(node, name), "chia_remote")


def test_require_colocated_false_no_pin():
    n = Gem5Node(require_colocated=False)
    assert n.task_options == {}
    with pytest.raises(RuntimeError, match="placement group"):
        n.spawn_tool("x", gem5_root="/r", config_script="/c", workloads={})
    n.close()


def test_self_reserved_pg_lifecycle(ray_cx):
    if RES != "gem5":
        pytest.skip("self-reservation needs a free 'gem5' bundle (RES != gem5)")
    try:
        n = Gem5Node(require_colocated=True, pg_ready_timeout_s=PG_TIMEOUT)
    except Exception:
        pytest.skip("no free gem5 bundle to self-reserve")
    assert n.owns_placement_group is True
    n.close()
    assert n.placement_group is None  # released


# ===========================================================================
# Tier 1b — Gem5Node remote functions (real chia_remote dispatch)
# ===========================================================================

def test_build_failure_no_sconstruct(node, env):
    # a git repo with no SConstruct -> scons fails (or is absent) -> not success
    bare = os.path.join(env["root"], "bare_repo")
    ray.get(_stage_bare_repo.options(**_sched(node.placement_group)).remote(bare, committed=True))
    art = _remote(node, "build_gem5", gem5_root=bare, timeout_s=120)
    assert isinstance(art, Gem5BuildArtifact)
    assert art.success is False and art.returncode != 0
    assert art.base_rev  # git repo -> rev recorded


def test_build_success(node, env):
    if not env["has_scons"]:
        pytest.skip("scons not on the worker")
    art = _remote(node, "build_gem5", gem5_root=env["gem5_root"], timeout_s=300)
    assert art.success is True and art.returncode == 0
    assert art.binary_path.endswith("build/RISCV/gem5.opt")
    assert art.base_rev and art.build_duration_s >= 0


def test_build_timeout(node, env):
    if not env["has_scons"]:
        pytest.skip("scons not on the worker")
    art = _remote(node, "build_gem5", gem5_root=env["slow_root"], timeout_s=2)
    assert art.success is False
    assert art.stderr_tail.startswith("TIMEOUT")


def test_run_ok(node, env):
    outdir = os.path.join(env["root"], "out_ok")
    res = _remote(node, "run_gem5", gem5_bin=env["gem5_ok"],
                  config_script=env["config"], outdir=outdir,
                  config_args=["--kernel", os.path.join(env["workloads"], "a.elf")],
                  capture_stats=True)
    assert isinstance(res, Gem5RunResult)
    assert res.status == "ok"
    assert res.num_cycles == 5678 and res.sim_insts == 1234
    assert res.sim_seconds == 0.000005 and res.host_seconds == 0.42
    assert res.wall_s is not None and res.workload_name == "a.elf"
    assert res.stats_content and "numCycles" in res.stats_content


def test_run_debug_trace_capture(node, env):
    outdir = os.path.join(env["root"], "out_dbg")
    res = _remote(node, "run_gem5", gem5_bin=env["gem5_ok"],
                  config_script=env["config"], outdir=outdir,
                  config_args=["--kernel", os.path.join(env["workloads"], "a.elf")],
                  debug_flags="O3PipeView", debug_file="trace.out",
                  capture_debug_trace=True)
    assert res.status == "ok"
    assert res.debug_trace is not None and b"O3PipeView" in res.debug_trace


def test_run_failed(node, env):
    res = _remote(node, "run_gem5", gem5_bin=env["gem5_fail"],
                  config_script=env["config"], outdir=os.path.join(env["root"], "out_fail"),
                  config_args=["--kernel", os.path.join(env["workloads"], "a.elf")])
    assert res.status == "run_failed_1" and res.returncode == 1


def test_run_timeout(node, env):
    res = _remote(node, "run_gem5", gem5_bin=env["gem5_timeout"],
                  config_script=env["config"], outdir=os.path.join(env["root"], "out_to"),
                  config_args=["--kernel", os.path.join(env["workloads"], "a.elf")],
                  timeout_s=2)
    assert res.status == "timeout"


def test_run_parse_failed(node, env):
    res = _remote(node, "run_gem5", gem5_bin=env["gem5_noparse"],
                  config_script=env["config"], outdir=os.path.join(env["root"], "out_np"),
                  config_args=["--kernel", os.path.join(env["workloads"], "a.elf")])
    assert res.status == "parse_failed" and res.num_cycles is None


def test_capture_and_restore(node, env):
    root = os.path.join(env["root"], "cr_repo")
    ray.get(_stage_bare_repo.options(**_sched(node.placement_group)).remote(root, committed=True))

    clean = _remote(node, "capture_gem5_source_state", gem5_root=root)
    assert isinstance(clean, Gem5SourceState) and clean.base_rev
    assert clean.source_diff.strip() == ""  # nothing changed yet

    ray.get(_mutate_repo.options(**_sched(node.placement_group)).remote(root))
    dirty = _remote(node, "capture_gem5_source_state", gem5_root=root)
    assert "foo.txt" in dirty.source_diff and "new.txt" in dirty.source_diff  # incl untracked

    ok, msg = _remote(node, "restore_gem5_source_state", gem5_root=root, state=clean)
    assert ok, msg
    state = ray.get(_read_repo.options(**_sched(node.placement_group)).remote(root))
    assert state["foo"] == "base\n" and state["new_exists"] is False

    # restoring the dirty state reproduces the edits
    ok, msg = _remote(node, "restore_gem5_source_state", gem5_root=root, state=dirty)
    assert ok, msg
    state = ray.get(_read_repo.options(**_sched(node.placement_group)).remote(root))
    assert state["foo"] == "EDITED\n" and state["new_exists"] is True


@ray.remote(num_cpus=0)
def _stage_bare_repo(path: str, committed: bool = False) -> bool:
    import subprocess
    os.makedirs(os.path.join(path, "src"), exist_ok=True)
    subprocess.run(["git", "init", "-q"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.email", "t@t"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.name", "t"], cwd=path, check=True)
    open(os.path.join(path, "src", "foo.txt"), "w").write("base\n")
    if committed:
        subprocess.run(["git", "add", "-A"], cwd=path, check=True)
        subprocess.run(["git", "commit", "-q", "-m", "base"], cwd=path, check=True)
    return True


# ===========================================================================
# Tier 1c — Gem5ToolServer tool calls over a real MCP round-trip
# ===========================================================================

async def _call_tool(url: str, tool_name: str, args: dict, sse_read_timeout_s: float = 300) -> str:
    from datetime import timedelta
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client
    # sse_read_timeout bounds how long we wait between server events; the tool's
    # keepalive heartbeats (every 30s) reset it, so a long real sim stays alive.
    async with streamablehttp_client(
        url, sse_read_timeout=timedelta(seconds=sse_read_timeout_s)
    ) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(tool_name, args)
            return "".join(
                getattr(c, "text", "") for c in result.content
            )


@pytest.fixture(scope="session")
def tool(node, env):
    try:
        t = node.spawn_tool(
            "g5live", gem5_root=env["gem5_root"], config_script=env["config"],
            workloads=env["workloads"], gem5_bin=env["gem5_ok"],
        )
    except Exception as e:
        # The bundle's node must host a FastMCP server; some worker images have an
        # incompatible pydantic stack. Skip the MCP round-trip there (it is
        # exercised on a properly-provisioned gem5 worker / RES=gem5).
        pytest.skip(f"tool actor could not start on the bundle's node: {e}")
    yield t
    # node.close() (session teardown) also stops it; explicit stop is harmless.


@pytest.fixture(scope="session")
def tool_url(tool):
    return f"http://{tool.hostname}:{tool.port}/{tool.name}/mcp"


def test_tool_list_workloads(tool_url):
    out = asyncio.run(_call_tool(tool_url, "g5live_list_workloads", {}))
    assert "a" in out and "b" in out


def test_tool_run(tool_url):
    out = asyncio.run(_call_tool(tool_url, "g5live_run",
                                 {"workloads": ["a"], "skip_build": True}))
    assert "| a |" in out and "5678" in out and "1234" in out and "ok" in out


def test_tool_stats_followup(tool_url):
    asyncio.run(_call_tool(tool_url, "g5live_run", {"workloads": ["a"], "skip_build": True}))
    out = asyncio.run(_call_tool(tool_url, "g5live_stats",
                                 {"workload": "a", "pattern": "numCycles"}))
    assert "numCycles" in out and "5678" in out


def test_tool_run_unknown_workload(tool_url):
    out = asyncio.run(_call_tool(tool_url, "g5live_run", {"workloads": ["nope"]}))
    assert "unknown workload" in out.lower()


def test_tool_build(tool_url, env):
    if not env["has_scons"]:
        pytest.skip("scons not on the worker")
    out = asyncio.run(_call_tool(tool_url, "g5live_build", {}))
    assert out.startswith("OK (")


# ===========================================================================
# Tier 2 — gated real-gem5 smoke test
# ===========================================================================

@ray.remote(num_cpus=0)
def _list_elfs(wdir: str) -> list:
    return sorted(f for f in os.listdir(wdir) if f.endswith((".elf", ".riscv")))


@pytest.mark.skipif(os.environ.get("GEM5_TEST_REAL") != "1",
                    reason="set GEM5_TEST_REAL=1 (+ a free gem5 worker) to run")
def test_real_gem5_build_and_run(node, env):
    gem5_root = os.environ["GEM5_TEST_GEM5_ROOT"]
    config = os.environ["GEM5_TEST_CONFIG"]
    wdir = os.environ["GEM5_TEST_WORKLOAD_DIR"]
    # The workload dir lives on the WORKER, so list it there (not on the driver).
    chosen = os.environ.get("GEM5_TEST_WORKLOAD")
    if not chosen:
        elfs = ray.get(_list_elfs.options(**_sched(node.placement_group)).remote(wdir))
        assert elfs, f"no .elf/.riscv workloads in {wdir} on the worker"
        chosen = elfs[0]

    art = _remote(node, "build_gem5", gem5_root=gem5_root, timeout_s=3600)
    assert art.success, art.stderr_tail
    assert art.base_rev  # real git checkout -> rev recorded

    res = _remote(node, "run_gem5", gem5_bin=art.binary_path, config_script=config,
                  outdir=os.path.join(env["root"], "real_out"),
                  config_args=["--kernel", os.path.join(wdir, chosen)],
                  stats_block="first", timeout_s=1800)
    assert res.status == "ok", res.error_messages
    assert res.num_cycles and res.sim_insts
    print(f"\nREAL gem5 run: {chosen} -> {res.num_cycles} cycles, "
          f"{res.sim_insts} insts, {res.wall_s:.1f}s wall")


@ray.remote(num_cpus=0)
def _git_status_src(root: str) -> str:
    import subprocess
    return subprocess.run(["git", "status", "--porcelain", "--", "src/"],
                          cwd=root, capture_output=True, text=True).stdout


@ray.remote(num_cpus=0)
def _touch(path: str, content: str = "chia restore probe\n") -> bool:
    open(path, "w").write(content)
    return True


@ray.remote(num_cpus=0)
def _exists(path: str) -> bool:
    return os.path.exists(path)


@pytest.mark.skipif(os.environ.get("GEM5_TEST_REAL") != "1",
                    reason="set GEM5_TEST_REAL=1 (+ a free gem5 worker) to run")
def test_real_gem5_tool_build_run_stats(node):
    """Drive Gem5ToolServer over a real MCP round-trip against the real gem5.opt,
    running an actual simulation through the tool."""
    gem5_root = os.environ["GEM5_TEST_GEM5_ROOT"]
    config = os.environ["GEM5_TEST_CONFIG"]
    wdir = os.environ["GEM5_TEST_WORKLOAD_DIR"]
    wl = (os.environ.get("GEM5_TEST_WORKLOAD") or "em_crc32.gem5.elf").split(".")[0]

    tool = node.spawn_tool(
        "g5real", gem5_root=gem5_root, config_script=config, workloads=wdir,
        gem5_bin=os.path.join(gem5_root, "build/RISCV/gem5.opt"),
        workload_glob="*.gem5.elf", stats_block="first",
        run_timeout_per_workload_s=1800,
    )
    url = f"http://{tool.hostname}:{tool.port}/{tool.name}/mcp"

    listed = asyncio.run(_call_tool(url, "g5real_list_workloads", {}))
    assert wl in listed, listed

    # real simulation driven entirely through the MCP tool (skip_build: reuse gem5.opt)
    run_out = asyncio.run(_call_tool(
        url, "g5real_run", {"workloads": [wl], "skip_build": True},
        sse_read_timeout_s=1800))
    assert f"| {wl} |" in run_out and "ok" in run_out, run_out

    stats_out = asyncio.run(_call_tool(
        url, "g5real_stats", {"workload": wl, "pattern": "numCycles|committedInsts"}))
    assert "numCycles" in stats_out, stats_out
    print(f"\nREAL tool run output:\n{run_out}\n--- stats ---\n{stats_out}")

    build_out = asyncio.run(_call_tool(url, "g5real_build", {}, sse_read_timeout_s=1800))
    assert build_out.startswith("OK ("), build_out


@ray.remote(num_cpus=0)
def _mk_worktree(root: str, wt: str, rev: str) -> tuple:
    import subprocess
    r = subprocess.run(["git", "worktree", "add", "--detach", wt, rev],
                       cwd=root, capture_output=True, text=True)
    return r.returncode, r.stderr[-300:]


@ray.remote(num_cpus=0)
def _rm_worktree(root: str, wt: str) -> bool:
    import shutil
    import subprocess
    subprocess.run(["git", "worktree", "remove", "--force", wt],
                   cwd=root, capture_output=True, text=True)
    shutil.rmtree(wt, ignore_errors=True)
    return True


@pytest.mark.skipif(os.environ.get("GEM5_TEST_REAL") != "1",
                    reason="set GEM5_TEST_REAL=1 (+ a free gem5 worker) to run")
def test_real_gem5_capture_restore(node):
    """Full capture/restore round-trip against the REAL gem5 repo.

    Read-only capture runs against the live checkout; the mutating restore
    (git checkout + clean) runs in an isolated ``git worktree`` at the base rev,
    so the live src/ (which may hold the alignment loop's in-progress edits) is
    never touched.
    """
    root = os.environ["GEM5_TEST_GEM5_ROOT"]
    sched = _sched(node.placement_group)

    # 1) read-only capture of the real checkout (handles the real tree as-is)
    live = _remote(node, "capture_gem5_source_state", gem5_root=root)
    assert isinstance(live, Gem5SourceState) and live.base_rev

    # 2) mutating round-trip in an isolated worktree at the real base rev
    wt = "/tmp/g5_probe_wt"
    ray.get(_rm_worktree.options(**sched).remote(root, wt))  # clean any leftover
    rc, err = ray.get(_mk_worktree.options(**sched).remote(root, wt, live.base_rev))
    assert rc == 0, err
    try:
        clean = _remote(node, "capture_gem5_source_state", gem5_root=wt)
        assert clean.base_rev and clean.source_diff.strip() == ""  # fresh worktree is clean

        marker = os.path.join(wt, "src", ".chia_restore_probe")
        ray.get(_touch.options(**sched).remote(marker))
        dirty = _remote(node, "capture_gem5_source_state", gem5_root=wt)
        assert ".chia_restore_probe" in dirty.source_diff  # untracked file captured

        ok, msg = _remote(node, "restore_gem5_source_state", gem5_root=wt, state=clean)
        assert ok, msg
        assert ray.get(_exists.options(**sched).remote(marker)) is False  # reverted

        ok, msg = _remote(node, "restore_gem5_source_state", gem5_root=wt, state=dirty)
        assert ok, msg
        assert ray.get(_exists.options(**sched).remote(marker)) is True  # reproduced
    finally:
        ray.get(_rm_worktree.options(**sched).remote(root, wt))
