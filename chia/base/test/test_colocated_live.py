"""Tests for chia.base.colocated — PinnedChiaFn + ColocatedNode.

Tier 0 (no cluster): construction, member re-binding, demand derivation, and
the partial-PG advisory warning (driven through a fake placement group object,
so no Ray connection is needed).

Tier 1 (live Ray): real placement-group pinning and colocation, dispatch-time
ValueError for unsatisfiable members, and PG ownership/lifecycle.  The dummy
member functions are defined *locally* (inside factories/fixtures) so
cloudpickle ships them by value — the cluster's workers do not need this
module installed.

Configuration (env vars):
  COLOCATED_TEST_RAY_ADDRESS  Ray address (default "auto"; tier 1 skips if
                              unreachable)
  COLOCATED_TEST_PG_TIMEOUT   seconds to wait for bundles (default 60)

Run:
  pytest chia/chia/base/test/test_colocated_live.py -v
"""

from __future__ import annotations

import logging
import os

import pytest
import ray
from ray.util.placement_group import placement_group, remove_placement_group

from chia.base.ChiaFunction import ChiaFunction, get
from chia.base.colocated import ColocatedNode, PinnedChiaFn

RAY_ADDR = os.environ.get("COLOCATED_TEST_RAY_ADDRESS", "auto")
PG_TIMEOUT = float(os.environ.get("COLOCATED_TEST_PG_TIMEOUT", "60"))

COLOC_LOGGER = "chia.base.colocated"


# ---------------------------------------------------------------------------
# Local dummy nodes. Functions are defined inside the factory so cloudpickle
# serializes them by value (no import of this test module on workers).
# ---------------------------------------------------------------------------

def _make_cpu_node_cls():
    """A ColocatedNode whose two members each demand the default CPU: 1."""

    @ChiaFunction()
    def _where_a() -> str:
        import ray as _ray
        return _ray.get_runtime_context().get_node_id()

    @ChiaFunction()
    def _where_b() -> str:
        import ray as _ray
        return _ray.get_runtime_context().get_node_id()

    # NB: the RHS names must differ from the class attribute names — a name
    # assigned in a class body is not resolved from the enclosing function.
    class CpuNode(ColocatedNode):
        _MEMBER_FNS = ("where_a", "where_b")
        _DEFAULT_BUNDLE = {"CPU": 1}
        where_a = staticmethod(_where_a)
        where_b = staticmethod(_where_b)

    return CpuNode


def _make_mixed_node_cls():
    """A ColocatedNode whose members have heterogeneous resource demands."""

    @ChiaFunction()
    def _plain() -> str:
        return "plain"

    @ChiaFunction(num_cpus=0.1, resources={"widget": 2})
    def _fancy() -> str:
        return "fancy"

    class MixedNode(ColocatedNode):
        _MEMBER_FNS = ("plain", "fancy")
        _DEFAULT_BUNDLE = {"CPU": 1, "widget": 2}
        plain = staticmethod(_plain)
        fancy = staticmethod(_fancy)

    return MixedNode


class _FakePG:
    """Stands in for a Ray PlacementGroup in no-cluster tests: ColocatedNode
    only reads ``bundle_specs`` (for the advisory warning) and stores the
    object inside a PlacementGroupSchedulingStrategy, which does not validate."""

    def __init__(self, bundle_specs):
        self.bundle_specs = bundle_specs


# ===========================================================================
# Tier 0 — no cluster
# ===========================================================================

def test_member_demands_derivation():
    node = _make_mixed_node_cls()(require_colocated=False)
    assert node._member_demands() == {
        "plain": {"CPU": 1},                  # Ray's task default num_cpus=1
        "fancy": {"CPU": 0.1, "widget": 2},   # explicit num_cpus + resources
    }


def test_demands_never_overwritten_by_pinning():
    cls = _make_mixed_node_cls()
    node = cls(placement_group=_FakePG([{"CPU": 1, "widget": 2}]))
    # The decorator-level demands are intact on the class attribute, and
    # PinnedChiaFn carries only scheduling opts — never resources.
    assert cls.fancy._chia_options["resources"] == {"widget": 2}
    assert "resources" not in node._sched_opts
    assert list(node.task_options) == ["scheduling_strategy"]


def test_no_pg_members_rebound_unpinned():
    cls = _make_cpu_node_cls()
    node = cls(require_colocated=False)
    assert node.placement_group is None
    assert node.task_options == {}
    for name in cls._MEMBER_FNS:
        member = getattr(node, name)
        assert isinstance(member, PinnedChiaFn)
        # With no scheduling opts the pinned wrapper delegates to the raw fn.
        assert member.chia_remote is getattr(cls, name).chia_remote
    node.close()  # no-op
    node.close()  # idempotent


def test_context_manager_no_pg():
    with _make_cpu_node_cls()(require_colocated=False) as node:
        assert node.owns_placement_group is False


def test_given_pg_not_owned():
    node = _make_cpu_node_cls()(placement_group=_FakePG([{"CPU": 1}]))
    assert node.owns_placement_group is False
    node.close()  # must not try to remove a PG it did not create
    assert node.placement_group is not None


def test_partial_pg_warns_per_member(caplog):
    with caplog.at_level(logging.WARNING, logger=COLOC_LOGGER):
        _make_cpu_node_cls()(placement_group=_FakePG([{"CPU": 0.5}]))
    warnings = [r.message for r in caplog.records]
    assert len(warnings) == 2  # both members miss the bundle
    assert any("where_a" in w and "{'CPU': 1}" in w for w in warnings)
    assert all("ValueError at submission" in w for w in warnings)


def test_satisfiable_pg_no_warning(caplog):
    with caplog.at_level(logging.WARNING, logger=COLOC_LOGGER):
        _make_mixed_node_cls()(placement_group=_FakePG([{"CPU": 1, "widget": 2}]))
    assert caplog.records == []


def test_bundle_index_any_fits_some_bundle(caplog):
    # bundle_index=-1: a member fits if ANY bundle satisfies it.
    specs = [{"CPU": 0.5}, {"CPU": 1, "widget": 2}]
    with caplog.at_level(logging.WARNING, logger=COLOC_LOGGER):
        _make_mixed_node_cls()(placement_group=_FakePG(specs), bundle_index=-1)
    assert caplog.records == []
    # Pinned to the small bundle only, both members warn.
    with caplog.at_level(logging.WARNING, logger=COLOC_LOGGER):
        _make_mixed_node_cls()(placement_group=_FakePG(specs), bundle_index=0)
    assert len(caplog.records) == 2


def test_pg_without_metadata_skips_check(caplog):
    class _OpaquePG:
        @property
        def bundle_specs(self):
            raise RuntimeError("metadata unavailable")

    with caplog.at_level(logging.WARNING, logger=COLOC_LOGGER):
        node = _make_cpu_node_cls()(placement_group=_OpaquePG())
    assert caplog.records == []  # advisory check skipped, construction fine
    assert "scheduling_strategy" in node.task_options


# ===========================================================================
# Tier 1 — live Ray cluster
# ===========================================================================

@pytest.fixture(scope="session")
def ray_cx():
    if ray.is_initialized():
        # another test module in this pytest session already connected;
        # reuse its connection and leave shutdown to it
        yield
        return
    try:
        ray.init(address=RAY_ADDR, namespace="colocated_live_test",
                 log_to_driver=False)
    except Exception as e:
        pytest.skip(f"cannot connect to Ray at {RAY_ADDR}: {e}")
    yield
    ray.shutdown()


@pytest.fixture()
def cpu_pg(ray_cx):
    pg = placement_group([{"CPU": 1}], strategy="STRICT_PACK")
    try:
        ray.get(pg.ready(), timeout=PG_TIMEOUT)
    except Exception:
        remove_placement_group(pg)
        pytest.skip(f"no free CPU bundle within {PG_TIMEOUT}s (cluster busy?)")
    yield pg
    remove_placement_group(pg)


def test_pinned_members_colocate(cpu_pg):
    node = _make_cpu_node_cls()(placement_group=cpu_pg)
    node_ids = [
        get(node.where_a.chia_remote()),
        get(node.where_b.chia_remote()),
        get(node.where_a.chia_remote()),
    ]
    assert len(set(node_ids)) == 1  # every member ran on the bundle's worker
    node.close()


def test_local_call_passthrough(cpu_pg):
    node = _make_cpu_node_cls()(placement_group=cpu_pg)
    # PinnedChiaFn.__call__ runs the underlying fn in-process (no dispatch).
    assert node.where_a() == ray.get_runtime_context().get_node_id()
    node.close()


def test_unsatisfiable_dispatch_raises(ray_cx, caplog):
    pg = placement_group([{"CPU": 0.5}], strategy="STRICT_PACK")
    try:
        try:
            ray.get(pg.ready(), timeout=PG_TIMEOUT)
        except Exception:
            pytest.skip(f"no free CPU bundle within {PG_TIMEOUT}s (cluster busy?)")
        with caplog.at_level(logging.WARNING, logger=COLOC_LOGGER):
            node = _make_cpu_node_cls()(placement_group=pg)
        assert len(caplog.records) == 2  # advisory fired at construction
        # Ray rejects the dispatch at submission with the demand preserved.
        with pytest.raises(ValueError, match="CPU"):
            node.where_a.chia_remote()
    finally:
        remove_placement_group(pg)


def test_self_reserved_pg_released_on_exit(ray_cx):
    with _make_cpu_node_cls()(pg_ready_timeout_s=PG_TIMEOUT) as node:
        assert node.owns_placement_group is True
        pg = node.placement_group
        assert ray.util.placement_group_table(pg)["state"] in ("CREATED", "PENDING")
    assert node.placement_group is None
    assert ray.util.placement_group_table(pg)["state"] == "REMOVED"


def test_given_pg_survives_close(cpu_pg):
    node = _make_cpu_node_cls()(placement_group=cpu_pg)
    node.close()
    assert ray.util.placement_group_table(cpu_pg)["state"] != "REMOVED"
