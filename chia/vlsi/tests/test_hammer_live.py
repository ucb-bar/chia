"""Tests for chia.vlsi.hammer — HammerNode.run / HammerNode.collect.

Tier 0 (no cluster): run/collect logic against a stub hammer binary —
command assembly, config staging, output-JSON parsing, the obj_dir listing,
timeout kill, and collect's glob/dedup/size-cap semantics.  These call the
members' ``_chia_original`` (the undecorated function) so no Ray connection
is ever made.

Tier 1 (live Ray): real ``chia_remote`` dispatch through a placement group.
The key test runs ``run`` and then ``collect`` through the same pinned node
and asserts collect sees the file run wrote — proving both landed on the same
worker, which is HammerNode's whole reason to exist.

Tier 2 (real hammer-vlsi, no cluster): runs the actual ``hammer-vlsi`` CLI
using hammer's own license-free test stack (``hammer.technology.nop`` +
``hammer.synthesis.mocksynth``), validating the command line we assemble and
the syn -> syn-to-par -> par chaining through ``HammerResult.output``.  Skips
unless ``hammer-vlsi`` is on PATH (``pip install -e <hammer repo>``).

Configuration (env vars):
  HAMMER_TEST_RAY_ADDRESS  Ray head address          (default "auto")
  HAMMER_TEST_RESOURCE     bundle/run resource label (default "hammer";
                           set "CPU" to run Tier 1 on any free node today)
  HAMMER_TEST_PG_TIMEOUT   seconds to wait for a free bundle (default 60)

Run:
  pytest chia/chia/vlsi/tests/test_hammer_live.py -v               # Tiers 0+2
  HAMMER_TEST_RESOURCE=CPU pytest chia/chia/vlsi/tests/test_hammer_live.py -v
"""

from __future__ import annotations

import json
import logging
import os
import shutil

import pytest
import ray
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

from chia.base.ChiaFunction import get
from chia.base.colocated import PinnedChiaFn
from chia.vlsi.hammer import (
    HammerNode, HammerResult, HammerCollectResult, HammerCollectFsResult,
    HammerMatchResult,
)

RES = os.environ.get("HAMMER_TEST_RESOURCE", "hammer")
RAY_ADDR = os.environ.get("HAMMER_TEST_RAY_ADDRESS", "auto")
PG_TIMEOUT = float(os.environ.get("HAMMER_TEST_PG_TIMEOUT", "60"))

COLOC_LOGGER = "chia.base.colocated"

# Ship the local chia repo to workers so the chia.vlsi.hammer on the worker
# matches this checkout even when the worker image's baked-in chia predates it
# (chia is a namespace package, so the uploaded copy merges onto the installed
# one).  Set HAMMER_TEST_WORKING_DIR="" to disable.
import chia as _chia
_DEFAULT_WORKING_DIR = os.path.dirname(list(_chia.__path__)[0])  # repo root
WORKING_DIR = os.environ.get("HAMMER_TEST_WORKING_DIR", _DEFAULT_WORKING_DIR)

# Stub hammer-vlsi: echoes its argv, fabricates a report tree under --obj_dir,
# and writes the -o output config — enough to validate everything run() does
# around the subprocess without a real EDA tool.
STUB_HAMMER = """#!/bin/bash
args=("$@")
for ((i=0; i<${#args[@]}; i++)); do
  if [ "${args[$i]}" == "-o" ]; then out="${args[$((i+1))]}"; fi
  if [ "${args[$i]}" == "--obj_dir" ]; then objdir="${args[$((i+1))]}"; fi
done
echo "stub hammer got: $@"
mkdir -p "$objdir/syn-rundir/reports"
echo "area report contents" > "$objdir/syn-rundir/reports/final_area.rpt"
echo '{"synthesis.outputs.output_files": ["netlist.v"]}' > "$out"
"""


def _local(member, *args, **kwargs):
    """Invoke a @ChiaFunction member's undecorated body in-process.

    Bypasses the ChiaFunction wrapper (whose profiler hook would auto-connect
    to Ray), keeping Tier 0 runnable with no cluster anywhere near it.
    """
    return member._chia_original(*args, **kwargs)


@pytest.fixture()
def stub_bin(tmp_path):
    path = tmp_path / "stub_hammer.sh"
    path.write_text(STUB_HAMMER)
    path.chmod(0o755)
    return str(path)


# ===========================================================================
# Tier 0 — run() against the stub binary (no cluster)
# ===========================================================================

def test_run_success(stub_bin, tmp_path):
    obj_dir = str(tmp_path / "build")
    r = _local(HammerNode.run, "syn",
               configs=["/cfg/tech.yml", "/cfg/tools.yml"],
               config_contents={"design.yml": "synthesis.inputs.top_module: Foo"},
               obj_dir=obj_dir,
               hammer_bin=stub_bin)
    assert isinstance(r, HammerResult)
    assert r.success and r.returncode == 0 and r.action == "syn"
    # -p order: path configs first (in order), then staged config_contents,
    # then --obj_dir / -o, with the action last.
    argv = r.stdout.splitlines()[0]
    staged = os.path.join(obj_dir, "configs", "design.yml")
    assert argv.index("/cfg/tech.yml") < argv.index("/cfg/tools.yml") < argv.index(staged)
    assert argv.rstrip().endswith("syn")
    # config_contents materialized on disk
    assert open(staged).read() == "synthesis.inputs.top_module: Foo"
    # -o output config parsed into .output
    assert r.output == {"synthesis.outputs.output_files": ["netlist.v"]}
    # listing manifests everything the run produced
    assert r.listing["syn-rundir/reports/final_area.rpt"] > 0
    assert "configs/design.yml" in r.listing and "syn-output.json" in r.listing


def test_run_failure(tmp_path):
    r = _local(HammerNode.run, "syn", obj_dir=str(tmp_path / "b"),
               hammer_bin="/bin/false")
    assert not r.success and r.returncode == 1
    assert r.output == {} and r.listing == {}


def test_run_timeout_kills_process_group(tmp_path):
    slow = tmp_path / "slow.sh"
    slow.write_text("#!/bin/bash\nsleep 30\n")
    slow.chmod(0o755)
    r = _local(HammerNode.run, "syn", obj_dir=str(tmp_path / "b"),
               hammer_bin=str(slow), timeout_seconds=1)
    assert not r.success and r.returncode != 0
    assert "timed out after 1s" in r.stderr


# ===========================================================================
# Tier 0 — collect() (no cluster)
# ===========================================================================

@pytest.fixture()
def obj_tree(tmp_path):
    d = tmp_path / "build"
    (d / "syn-rundir" / "reports").mkdir(parents=True)
    (d / "syn-rundir" / "reports" / "final_area.rpt").write_text("area\n")
    (d / "syn-rundir" / "genus.log").write_text("log line\n")
    (d / "syn-rundir" / "big_netlist.v").write_text("x" * 200_000)
    (d / "syn-output.json").write_text("{}")
    return str(d)


def test_collect_globs_dedup_and_size_cap(obj_tree):
    c = _local(HammerNode.collect, obj_tree,
               # overlapping patterns: each file must appear exactly once
               patterns=["syn-rundir/reports/**", "syn-rundir/**", "*.json"],
               max_bytes_per_file=100_000)
    assert isinstance(c, HammerCollectResult)
    assert sorted(c.files) == ["syn-output.json",
                               "syn-rundir/genus.log",
                               "syn-rundir/reports/final_area.rpt"]
    assert c.files["syn-rundir/reports/final_area.rpt"] == "area\n"
    # oversized file is reported, not shipped — but stays in the listing
    assert c.skipped == {"syn-rundir/big_netlist.v": 200_000}
    assert "syn-rundir/big_netlist.v" in c.listing
    assert len(c.listing) == 4


def test_collect_uncapped_by_default(obj_tree):
    # max_bytes_per_file defaults to None: everything matched is shipped,
    # including files an explicit cap would have skipped.
    c = _local(HammerNode.collect, obj_tree, patterns=["syn-rundir/**"])
    assert c.skipped == {}
    assert len(c.files["syn-rundir/big_netlist.v"]) == 200_000
    # 0 is the falsy edge and also means "no cap", not "skip everything"
    c0 = _local(HammerNode.collect, obj_tree, patterns=["syn-rundir/**"],
                max_bytes_per_file=0)
    assert c0.skipped == {} and "syn-rundir/big_netlist.v" in c0.files


def test_collect_no_match_and_missing_dir(obj_tree, tmp_path):
    c = _local(HammerNode.collect, obj_tree, patterns=["nothing/matches/**"])
    assert c.files == {} and c.skipped == {} and len(c.listing) == 4
    c2 = _local(HammerNode.collect, str(tmp_path / "does_not_exist"),
                patterns=["**"])
    assert c2.files == {} and c2.skipped == {} and c2.listing == {}


# ===========================================================================
# Tier 0 — collect_fs primitives: list_matches / read_chunk (no cluster)
# ===========================================================================

def test_list_matches_manifest(obj_tree):
    m = _local(HammerNode.list_matches, obj_tree,
               patterns=["syn-rundir/reports/**", "syn-rundir/**", "*.json"],
               max_bytes_per_file=100_000)
    assert isinstance(m, HammerMatchResult)
    # (relpath, size) within the cap — same selection as collect, no contents
    assert dict(m.matches) == {
        "syn-output.json": 2,
        "syn-rundir/genus.log": len("log line\n"),
        "syn-rundir/reports/final_area.rpt": len("area\n"),
    }
    assert m.skipped == {"syn-rundir/big_netlist.v": 200_000}


def test_read_chunk_reads_slices_and_eof(obj_tree):
    rel = "syn-rundir/reports/final_area.rpt"  # contents "area\n"
    assert _local(HammerNode.read_chunk, obj_tree, rel, 0, 3) == b"are"
    assert _local(HammerNode.read_chunk, obj_tree, rel, 3, 100) == b"a\n"
    assert _local(HammerNode.read_chunk, obj_tree, rel, 999, 10) == b""  # past EOF


def test_read_chunk_rejects_escape(obj_tree):
    with pytest.raises(ValueError, match="escapes obj_dir"):
        _local(HammerNode.read_chunk, obj_tree, "../../etc/passwd", 0, 10)


class _LocalFn:
    """Stands in for a node's pinned member: runs the @ChiaFunction body
    in-process and returns the value directly (no Ray dispatch)."""
    def __init__(self, member):
        self._member = member

    def chia_remote(self, *args, **kwargs):
        return self._member._chia_original(*args, **kwargs)


def test_collect_fs_loop_streams_locally(obj_tree, tmp_path, monkeypatch):
    """Exercise the collect_fs orchestration body (manifest -> per-file chunk
    loop -> incremental write) without a cluster: stub the two pinned members
    to run locally and make get() an identity over their direct returns."""
    monkeypatch.setattr("chia.base.ChiaFunction.get", lambda ref: ref)

    # a placement group is required by collect_fs; a fake satisfies the guard
    class _FakePG:
        bundle_specs = [{"CPU": 1, "hammer": 1}]

    node = HammerNode(placement_group=_FakePG())
    node.list_matches = _LocalFn(HammerNode.list_matches)
    node.read_chunk = _LocalFn(HammerNode.read_chunk)

    dest = str(tmp_path / "pulled")
    cfs = node.collect_fs(
        obj_tree, ["syn-rundir/reports/**", "syn-rundir/**", "*.json"], dest,
        max_bytes_per_file=100_000,
        chunk_bytes=2,   # tiny -> the chunk loop runs many iterations per file
    )
    assert isinstance(cfs, HammerCollectFsResult)
    # streamed onto the caller's disk, tree preserved, bytes exact despite 2B chunks
    assert open(os.path.join(dest, "syn-rundir", "reports", "final_area.rpt")).read() == "area\n"
    assert open(os.path.join(dest, "syn-rundir", "genus.log")).read() == "log line\n"
    assert cfs.copied == {
        "syn-output.json": 2,
        "syn-rundir/genus.log": len("log line\n"),
        "syn-rundir/reports/final_area.rpt": len("area\n"),
    }
    # oversized file skipped, never written
    assert cfs.skipped == {"syn-rundir/big_netlist.v": 200_000}
    assert not os.path.exists(os.path.join(dest, "syn-rundir", "big_netlist.v"))


# ===========================================================================
# Tier 0 — node construction (no cluster)
# ===========================================================================

def test_decorator_demands_intact():
    for name in HammerNode._MEMBER_FNS:
        assert getattr(HammerNode, name)._chia_options["resources"] == {"hammer": 1}
    node = HammerNode(require_colocated=False)
    assert node._member_demands() == {
        "run": {"CPU": 1, "hammer": 1},
        "collect": {"CPU": 1, "hammer": 1},
        "list_matches": {"CPU": 1, "hammer": 1},
        "read_chunk": {"CPU": 1, "hammer": 1},
    }
    # collect_fs is a caller-side method, not a pinned ChiaFunction member
    assert "collect_fs" not in HammerNode._MEMBER_FNS
    assert not hasattr(HammerNode.collect_fs, "_chia_options")


def test_no_pg_binding_and_close():
    with HammerNode(require_colocated=False) as node:
        assert node.placement_group is None and node.task_options == {}
        assert isinstance(node.run, PinnedChiaFn)
        assert node.run.chia_remote is HammerNode.run.chia_remote
    node.close()  # idempotent


def test_collect_fs_requires_pg():
    # without a placement group, list_matches and read_chunk could land on
    # different workers — collect_fs refuses rather than corrupt the stream.
    node = HammerNode(require_colocated=False)
    with pytest.raises(RuntimeError, match="placement group"):
        node.collect_fs("/some/obj_dir", ["**"], "/tmp/dest")
    node.close()


def test_partial_pg_warns(caplog):
    class _FakePG:
        bundle_specs = [{"CPU": 0.5}]  # satisfies no member

    with caplog.at_level(logging.WARNING, logger=COLOC_LOGGER):
        HammerNode(placement_group=_FakePG())
    # one warning per pinned member (run/collect/list_matches/read_chunk)
    assert len(caplog.records) == len(HammerNode._MEMBER_FNS)
    assert all("'hammer': 1" in r.message for r in caplog.records)


# ===========================================================================
# Tier 2 — real hammer-vlsi via nop tech + mocksynth (no cluster, no EDA tools)
# ===========================================================================

requires_hammer = pytest.mark.skipif(
    shutil.which("hammer-vlsi") is None,
    reason="hammer-vlsi not on PATH (pip install -e <hammer repo>)")


def _nop_config(obj_dir: str) -> str:
    """hammer's own minimal test config (tests/test_cli_driver.py), as YAML:
    nop technology, mocksynth synthesis, nop par — no EDA tools involved."""
    return f"""
vlsi.core.technology: "hammer.technology.nop"
vlsi.core.synthesis_tool: "hammer.synthesis.mocksynth"
vlsi.core.par_tool: "hammer.par.nop"
vlsi.inputs.hierarchical.config_source: "none"
vlsi.technology.extra_macro_sizes: []
synthesis.inputs.top_module: "dummy"
synthesis.inputs.input_files: ["/dev/null"]
synthesis.mocksynth.temp_folder: "{obj_dir}"
"""


@requires_hammer
def test_real_hammer_syn(tmp_path):
    obj_dir = str(tmp_path / "build")
    r = _local(HammerNode.run, "syn",
               config_contents={"design.yml": _nop_config(obj_dir)},
               obj_dir=obj_dir)
    assert r.success, r.stderr[-2000:]
    # the -o output config is real hammer output, parsed. is_complete=False is
    # hammer's (counterintuitive) marker for a well-formed output-only config.
    assert r.output["vlsi.builtins.is_complete"] is False
    assert "synthesis.outputs.output_files" in r.output
    # mocksynth's four steps ran for real
    for step in ("step1", "step2", "step3", "step4"):
        assert open(os.path.join(obj_dir, f"{step}.txt")).read() == step
    # hammer's own output config landed in the standard rundir location
    assert "syn-rundir/syn-output-full.json" in r.listing


@requires_hammer
def test_real_hammer_syn_to_par_chain(tmp_path):
    """The documented chaining flow: each action's HammerResult.output feeds
    the next action via config_contents — syn -> syn-to-par -> par."""
    obj_dir = str(tmp_path / "build")
    cfg = _nop_config(obj_dir)

    def run(action, extra):
        return _local(HammerNode.run, action,
                      config_contents={"design.yml": cfg, **extra},
                      obj_dir=obj_dir)

    syn = run("syn", {})
    assert syn.success, syn.stderr[-2000:]

    bridge = run("syn-to-par", {"syn-out.json": json.dumps(syn.output)})
    assert bridge.success, bridge.stderr[-2000:]
    assert bridge.output["par.inputs.top_module"] == "dummy"

    par = run("par", {"par-in.json": json.dumps(bridge.output)})
    assert par.success, par.stderr[-2000:]
    assert "par.outputs.output_gds" in par.output


# ===========================================================================
# Tier 1 — live cluster
# ===========================================================================

@pytest.fixture(scope="session")
def ray_cx():
    if ray.is_initialized():
        # another test module in this pytest session already connected; reuse
        # its connection (losing our working_dir runtime_env — workers then
        # need a current chia install) and leave shutdown to it
        yield
        return
    runtime_env = None
    if WORKING_DIR:
        runtime_env = {"working_dir": WORKING_DIR,
                       "excludes": [".git", "**/__pycache__", "**/*.pyc"]}
    try:
        ray.init(address=RAY_ADDR, namespace="hammer_live_test",
                 runtime_env=runtime_env, log_to_driver=False)
    except Exception as e:
        pytest.skip(f"cannot connect to Ray at {RAY_ADDR}: {e}")
    yield
    ray.shutdown()


@pytest.fixture(scope="session")
def bundle(ray_cx):
    shape = {"CPU": 1} if RES == "CPU" else {RES: 1, "CPU": 1}
    pg = placement_group([shape], strategy="STRICT_PACK")
    try:
        ray.get(pg.ready(), timeout=PG_TIMEOUT)
    except Exception:
        remove_placement_group(pg)
        pytest.skip(f"no free '{RES}' bundle within {PG_TIMEOUT}s (cluster busy?)")
    yield pg
    remove_placement_group(pg)


@pytest.fixture(scope="session")
def node(bundle):
    n = HammerNode(placement_group=bundle, bundle_index=0)
    yield n
    n.close()


def _sched(pg):
    return {"scheduling_strategy": PlacementGroupSchedulingStrategy(
        placement_group=pg, placement_group_bundle_index=0)}


@pytest.fixture(scope="session")
def staged(bundle):
    """Write the stub hammer binary on the bundle's worker; return its paths.

    The remote fn is defined locally so cloudpickle ships it by value.
    """
    @ray.remote(num_cpus=0)
    def _stage() -> dict:
        import os as _os
        import tempfile as _tempfile
        root = _tempfile.mkdtemp(prefix="hammer_live_")
        stub = _os.path.join(root, "stub_hammer.sh")
        with open(stub, "w") as f:
            f.write(STUB_HAMMER)
        _os.chmod(stub, 0o755)
        return {"root": root, "stub": stub}

    return ray.get(_stage.options(**_sched(bundle)).remote())


def _remote(node, fn_name, **kwargs):
    """Dispatch a node member via chia_remote, dropping the hammer resource
    requirement when running on a non-hammer (CPU) test bundle."""
    fn = getattr(node, fn_name)
    if RES == "CPU":
        return get(fn.options(num_cpus=1, resources={}).chia_remote(**kwargs))
    return get(fn.chia_remote(**kwargs))


def test_provided_pg_not_owned(node, bundle):
    assert node.placement_group is bundle
    assert node.owns_placement_group is False
    assert "scheduling_strategy" in node.task_options


def test_run_then_collect_colocated(node, staged):
    """The reason HammerNode exists: collect sees the files run wrote, because
    both were pinned to the same bundle and thus the same worker filesystem."""
    obj_dir = os.path.join(staged["root"], "build")
    r = _remote(node, "run", action="syn",
                config_contents={"design.yml": "synthesis.inputs.top_module: Foo"},
                obj_dir=obj_dir, hammer_bin=staged["stub"])
    assert r.success and r.output
    assert "syn-rundir/reports/final_area.rpt" in r.listing

    c = _remote(node, "collect", obj_dir=r.obj_dir,
                patterns=["syn-rundir/reports/**"])
    assert c.files["syn-rundir/reports/final_area.rpt"] == "area report contents\n"


def test_collect_fs_streams_to_caller_disk(node, staged, tmp_path):
    """collect_fs pulls files from the obj_dir worker and writes them onto the
    CALLER's filesystem (this test process), streaming chunk-by-chunk — the
    contents never live in one object-store payload.

    Skipped in CPU mode: collect_fs drives the hammer-resourced list_matches /
    read_chunk members, which only schedule onto a real hammer bundle."""
    if RES == "CPU":
        pytest.skip("collect_fs needs a real 'hammer' bundle (internal members "
                    "demand the hammer resource)")
    # produce a report tree on the worker, plus an oversized file to skip
    obj_dir = os.path.join(staged["root"], "build")
    r = get(node.run.chia_remote(
        action="syn", obj_dir=obj_dir, hammer_bin=staged["stub"]))
    assert r.success

    @ray.remote(num_cpus=0)
    def _make_big(d: str) -> int:
        import os as _os
        p = _os.path.join(d, "syn-rundir", "big.v")
        with open(p, "w") as f:
            f.write("x" * 200_000)
        return 200_000
    ray.get(_make_big.options(**_sched(node.placement_group)).remote(r.obj_dir))

    dest = str(tmp_path / "pulled")   # on the caller (this test process)
    cfs = node.collect_fs(
        r.obj_dir, ["syn-rundir/**"], dest,
        max_bytes_per_file=100_000,
        chunk_bytes=4096,             # tiny -> forces multi-chunk streaming
    )
    assert isinstance(cfs, HammerCollectFsResult)
    # the report streamed onto the caller's disk, tree preserved, bytes intact
    rpt = os.path.join(dest, "syn-rundir", "reports", "final_area.rpt")
    assert open(rpt).read() == "area report contents\n"
    assert cfs.copied["syn-rundir/reports/final_area.rpt"] == len("area report contents\n")
    # oversized file skipped, never written
    assert cfs.skipped == {"syn-rundir/big.v": 200_000}
    assert not os.path.exists(os.path.join(dest, "syn-rundir", "big.v"))


def test_unsatisfiable_pg_dispatch_raises(ray_cx, caplog):
    if RES != "CPU":
        # with a real hammer bundle this scenario is covered by the CPU run
        pytest.skip("undersized-PG dispatch test runs in CPU mode")
    pg = placement_group([{"CPU": 0.5}], strategy="STRICT_PACK")
    try:
        ray.get(pg.ready(), timeout=PG_TIMEOUT)
        with caplog.at_level(logging.WARNING, logger=COLOC_LOGGER):
            n = HammerNode(placement_group=pg)
        assert len(caplog.records) == len(HammerNode._MEMBER_FNS)
        with pytest.raises(ValueError, match="hammer"):
            n.run.chia_remote("syn")
    finally:
        remove_placement_group(pg)


def test_self_reserved_pg_released_on_exit(ray_cx):
    try:
        # CPU-only reserve works on any cluster; the node warns (members
        # demand hammer) but reserves, owns, and must release the PG.
        n = HammerNode(reserve_bundle={"CPU": 0.5},
                       pg_ready_timeout_s=PG_TIMEOUT)
    except Exception:
        pytest.skip("no free CPU bundle to self-reserve")
    with n:
        assert n.owns_placement_group is True
        pg = n.placement_group
    assert n.placement_group is None
    assert ray.util.placement_group_table(pg)["state"] == "REMOVED"
