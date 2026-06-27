"""Tests for chia.chipyard.chipyard_hammer — ChipyardHammerNode.make / .collect.

Tier 0 (no cluster): make-target runner logic against a stub chipyard tree
(a fake ``vlsi/Makefile``) — variable passing/override order, OBJ_DIR
handling, env layering, failure/timeout paths, and collect's glob/dedup/cap
semantics.  These call the members' ``_chia_original`` (the undecorated
function) so no Ray connection is ever made.

Tier 1 (live Ray): real ``chia_remote`` dispatch through a placement group;
``make`` then ``collect`` through the same pinned node proves both landed on
the same worker filesystem.

No tier runs a real chipyard build — that needs a chipyard worker image and
hours; this validates everything the node does around ``make``.

Configuration (env vars):
  CHIPYARD_TEST_RAY_ADDRESS  Ray head address          (default "auto")
  CHIPYARD_TEST_RESOURCE     bundle/run resource label (default "chipyard";
                             set "CPU" to run Tier 1 on any free node)
  CHIPYARD_TEST_PG_TIMEOUT   seconds to wait for a free bundle (default 60)

Run:
  pytest chia/chia/chipyard/test/test_chipyard_hammer_live.py -v      # Tier 0
  CHIPYARD_TEST_RESOURCE=CPU pytest chia/chia/chipyard/test/test_chipyard_hammer_live.py -v
"""

from __future__ import annotations

import logging
import os

import pytest
import ray
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

from chia.base.ChiaFunction import get
from chia.base.colocated import PinnedChiaFn
from chia.chipyard.chipyard_hammer import (
    ChipyardHammerNode, ChipyardHammerResult, ChipyardHammerCollectResult,
)

RES = os.environ.get("CHIPYARD_TEST_RESOURCE", "chipyard")
RAY_ADDR = os.environ.get("CHIPYARD_TEST_RAY_ADDRESS", "auto")
PG_TIMEOUT = float(os.environ.get("CHIPYARD_TEST_PG_TIMEOUT", "60"))

COLOC_LOGGER = "chia.base.colocated"

# Ship the local chia repo to workers so chia.chipyard.chipyard_hammer on the
# worker matches this checkout (chia is a namespace package, so the uploaded
# copy merges onto the installed one).  Set CHIPYARD_TEST_WORKING_DIR="" to
# disable.
import chia as _chia
_DEFAULT_WORKING_DIR = os.path.dirname(list(_chia.__path__)[0])  # repo root
WORKING_DIR = os.environ.get("CHIPYARD_TEST_WORKING_DIR", _DEFAULT_WORKING_DIR)

# Stub vlsi/Makefile: echoes the variables the node passes, fabricates a
# report tree under OBJ_DIR — enough to validate everything make() does
# around the subprocess without a real chipyard checkout.  (Recipe lines
# must be tab-indented.)
STUB_MAKEFILE = (
    ".PHONY: buildfile syn noop envcheck fail slow\n"
    "buildfile syn:\n"
    "\t@echo \"ran target=$@ CONFIG=$(CONFIG) tech_name=$(tech_name)\"\n"
    "\t@mkdir -p $(OBJ_DIR)/syn-rundir/reports\n"
    "\t@echo \"area report contents\" > $(OBJ_DIR)/syn-rundir/reports/final_area.rpt\n"
    "noop:\n"
    "\t@echo \"ran target=$@\"\n"
    "envcheck:\n"
    "\t@echo \"MYVAR=$$MYVAR\"\n"
    "fail:\n"
    "\t@exit 1\n"
    "slow:\n"
    "\t@sleep 30\n"
)


def _local(member, *args, **kwargs):
    """Invoke a @ChiaFunction member's undecorated body in-process.

    Bypasses the ChiaFunction wrapper (whose profiler hook would auto-connect
    to Ray), keeping Tier 0 runnable with no cluster anywhere near it.
    """
    return member._chia_original(*args, **kwargs)


@pytest.fixture()
def stub_chipyard(tmp_path):
    """A fake chipyard checkout: just <root>/vlsi/Makefile."""
    vlsi = tmp_path / "chipyard" / "vlsi"
    vlsi.mkdir(parents=True)
    (vlsi / "Makefile").write_text(STUB_MAKEFILE)
    return str(tmp_path / "chipyard")


# ===========================================================================
# Tier 0 — make() against the stub Makefile (no cluster)
# ===========================================================================

def test_make_success(stub_chipyard, tmp_path):
    obj_dir = str(tmp_path / "obj")
    r = _local(ChipyardHammerNode.make, stub_chipyard, "syn",
               config="RocketConfig", obj_dir=obj_dir,
               make_vars={"tech_name": "sky130"})
    assert isinstance(r, ChipyardHammerResult)
    assert r.success and r.returncode == 0 and r.target == "syn"
    assert r.vlsi_dir == os.path.join(stub_chipyard, "vlsi")
    # variables reached make
    assert "ran target=syn CONFIG=RocketConfig tech_name=sky130" in r.stdout
    # OBJ_DIR honored and manifested
    assert r.obj_dir == obj_dir
    assert r.listing["syn-rundir/reports/final_area.rpt"] > 0


def test_make_vars_win_over_convenience_args(stub_chipyard, tmp_path):
    # make_vars are appended last; make takes the last assignment.
    r = _local(ChipyardHammerNode.make, stub_chipyard, "syn",
               config="A", obj_dir=str(tmp_path / "obj"),
               make_vars={"CONFIG": "B"})
    assert "CONFIG=B" in r.stdout


def test_make_without_obj_dir(stub_chipyard):
    r = _local(ChipyardHammerNode.make, stub_chipyard, "noop")
    assert r.success and "ran target=noop" in r.stdout
    assert r.obj_dir is None and r.listing == {}


def test_make_env_layered(stub_chipyard):
    r = _local(ChipyardHammerNode.make, stub_chipyard, "envcheck",
               env={"MYVAR": "hello"})
    assert "MYVAR=hello" in r.stdout


def test_make_failure(stub_chipyard):
    r = _local(ChipyardHammerNode.make, stub_chipyard, "fail")
    assert not r.success and r.returncode != 0
    assert r.listing == {}


def test_make_timeout_kills_process_group(stub_chipyard):
    r = _local(ChipyardHammerNode.make, stub_chipyard, "slow",
               timeout_seconds=1)
    assert not r.success and r.returncode != 0
    assert "timed out after 1s" in r.stderr


# ===========================================================================
# Tier 0 — collect() (no cluster)
# ===========================================================================

@pytest.fixture()
def obj_tree(tmp_path):
    d = tmp_path / "obj"
    (d / "syn-rundir" / "reports").mkdir(parents=True)
    (d / "syn-rundir" / "reports" / "final_area.rpt").write_text("area\n")
    (d / "syn-rundir" / "genus.log").write_text("log line\n")
    (d / "syn-rundir" / "big_netlist.v").write_text("x" * 200_000)
    (d / "hammer.d").write_text("# generated\n")
    return str(d)


def test_collect_globs_dedup_and_size_cap(obj_tree):
    c = _local(ChipyardHammerNode.collect, obj_tree,
               patterns=["syn-rundir/reports/**", "syn-rundir/**", "*.d"],
               max_bytes_per_file=100_000)
    assert isinstance(c, ChipyardHammerCollectResult)
    assert sorted(c.files) == ["hammer.d",
                               "syn-rundir/genus.log",
                               "syn-rundir/reports/final_area.rpt"]
    assert c.skipped == {"syn-rundir/big_netlist.v": 200_000}
    assert "syn-rundir/big_netlist.v" in c.listing and len(c.listing) == 4


def test_collect_uncapped_by_default(obj_tree):
    c = _local(ChipyardHammerNode.collect, obj_tree, patterns=["syn-rundir/**"])
    assert c.skipped == {}
    assert len(c.files["syn-rundir/big_netlist.v"]) == 200_000


def test_collect_no_match_and_missing_dir(obj_tree, tmp_path):
    c = _local(ChipyardHammerNode.collect, obj_tree, patterns=["nope/**"])
    assert c.files == {} and c.skipped == {} and len(c.listing) == 4
    c2 = _local(ChipyardHammerNode.collect, str(tmp_path / "missing"),
                patterns=["**"])
    assert c2.files == {} and c2.skipped == {} and c2.listing == {}


# ===========================================================================
# Tier 0 — node construction (no cluster)
# ===========================================================================

def test_decorator_demands_intact():
    assert ChipyardHammerNode.make._chia_options["resources"] == {"chipyard": 1}
    assert ChipyardHammerNode.collect._chia_options["resources"] == {"chipyard": 1}
    node = ChipyardHammerNode(require_colocated=False)
    assert node._member_demands() == {
        "make": {"CPU": 1, "chipyard": 1},
        "collect": {"CPU": 1, "chipyard": 1},
    }


def test_no_pg_binding_and_close():
    with ChipyardHammerNode(require_colocated=False) as node:
        assert node.placement_group is None and node.task_options == {}
        assert isinstance(node.make, PinnedChiaFn)
        assert node.make.chia_remote is ChipyardHammerNode.make.chia_remote
    node.close()  # idempotent


def test_partial_pg_warns(caplog):
    class _FakePG:
        bundle_specs = [{"CPU": 0.5}]  # satisfies neither member

    with caplog.at_level(logging.WARNING, logger=COLOC_LOGGER):
        ChipyardHammerNode(placement_group=_FakePG())
    assert len(caplog.records) == 2
    assert all("'chipyard': 1" in r.message for r in caplog.records)


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
        ray.init(address=RAY_ADDR, namespace="chipyard_hammer_live_test",
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
    n = ChipyardHammerNode(placement_group=bundle, bundle_index=0)
    yield n
    n.close()


def _sched(pg):
    return {"scheduling_strategy": PlacementGroupSchedulingStrategy(
        placement_group=pg, placement_group_bundle_index=0)}


@pytest.fixture(scope="session")
def staged(bundle):
    """Write the stub chipyard tree on the bundle's worker; return its paths.

    The remote fn is defined locally so cloudpickle ships it by value.
    """
    @ray.remote(num_cpus=0)
    def _stage() -> dict:
        import os as _os
        import tempfile as _tempfile
        root = _tempfile.mkdtemp(prefix="chipyard_hammer_live_")
        vlsi = _os.path.join(root, "chipyard", "vlsi")
        _os.makedirs(vlsi)
        with open(_os.path.join(vlsi, "Makefile"), "w") as f:
            f.write(STUB_MAKEFILE)
        return {"root": root,
                "chipyard": _os.path.join(root, "chipyard"),
                "obj_dir": _os.path.join(root, "obj")}

    return ray.get(_stage.options(**_sched(bundle)).remote())


def _remote(node, fn_name, **kwargs):
    """Dispatch a node member via chia_remote, dropping the chipyard resource
    requirement when running on a non-chipyard (CPU) test bundle."""
    fn = getattr(node, fn_name)
    if RES == "CPU":
        return get(fn.options(num_cpus=1, resources={}).chia_remote(**kwargs))
    return get(fn.chia_remote(**kwargs))


def test_provided_pg_not_owned(node, bundle):
    assert node.placement_group is bundle
    assert node.owns_placement_group is False
    assert "scheduling_strategy" in node.task_options


def test_make_then_collect_colocated(node, staged):
    """The reason ChipyardHammerNode exists: collect sees the files make
    wrote, because both were pinned to the same bundle and thus the same
    worker filesystem."""
    r = _remote(node, "make", chipyard_path=staged["chipyard"], target="syn",
                config="RocketConfig", obj_dir=staged["obj_dir"])
    assert r.success
    assert "syn-rundir/reports/final_area.rpt" in r.listing

    c = _remote(node, "collect", base_dir=staged["obj_dir"],
                patterns=["syn-rundir/reports/**"])
    assert c.files["syn-rundir/reports/final_area.rpt"] == "area report contents\n"
