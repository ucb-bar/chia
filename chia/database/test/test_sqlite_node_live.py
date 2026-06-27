"""Tests for chia.database.sqlite_node — SQLiteNode + SQLiteQueryTool.

Tier 0 (no cluster): the connection helper's PRAGMAs, every member function
called locally (PinnedChiaFn passthrough), transaction atomicity and
params-type dispatch, constructor validation, the db_path-injecting
_DbBoundChiaFn binding (driven through a fake function object), and the
query tool's formatting/caps (driven through an unstarted tool instance).

Tier 1 (live Ray): real colocation through a placement group, concurrent
writers, NodeAffinity pinning via pin_to_current_node, spawn_query_tool
co-location, and tool/PG lifecycle on close().  Tier-1 data roundtrips go
exclusively through the node's members (one bundle = one machine), so they
hold on multi-node clusters too; only the pin_to_current_node tests inspect
the DB file directly (it is on the driver's node by construction).

Configuration (env vars):
  SQLITE_NODE_TEST_RAY_ADDRESS  Ray address (default "auto"; tier 1 skips if
                                unreachable)
  SQLITE_NODE_TEST_PG_TIMEOUT   seconds to wait for bundles (default 60)

Run:
  pytest chia/chia/database/test/test_sqlite_node_live.py -v
"""

from __future__ import annotations

import os
import sqlite3

import pytest
import ray
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from chia.base.ChiaFunction import get
from chia.database.sqlite_node import (
    SQLiteExecResult,
    SQLiteNode,
    SQLiteQueryTool,
    _connect,
    _DbBoundChiaFn,
)

RAY_ADDR = os.environ.get("SQLITE_NODE_TEST_RAY_ADDRESS", "auto")
PG_TIMEOUT = float(os.environ.get("SQLITE_NODE_TEST_PG_TIMEOUT", "60"))


@pytest.fixture(scope="session", autouse=True)
def _disabled_profiler():
    """Pre-build the chia profiler singleton as disabled.

    A *local* @ChiaFunction call runs get_profiler(), whose first
    construction looks up the collector actor via ray.get_actor — and Ray's
    auto-init then tries to join whatever cluster address is lying around.
    Tier 0 must not touch Ray, so stub the collector lookup while the
    singleton is built (same spirit as test_classify_error.py's profiler
    mock)."""
    import chia.trace.profiler as profiler_mod
    orig_get_collector = profiler_mod.get_collector
    profiler_mod.get_collector = lambda namespace=None: None
    try:
        profiler_mod.reset_profiler()
        profiler_mod.get_profiler()
    finally:
        profiler_mod.get_collector = orig_get_collector
    yield
    profiler_mod.reset_profiler()

SCHEMA = """
CREATE TABLE IF NOT EXISTS t (
    id INTEGER PRIMARY KEY,
    v  TEXT
);
"""


@pytest.fixture()
def db(tmp_path) -> str:
    """Absolute path of a fresh, schema-initialized DB."""
    path = str(tmp_path / "test.db")
    SQLiteNode.init_schema(path, SCHEMA)
    return path


# ===========================================================================
# Tier 0 — no cluster
# ===========================================================================

# -- _connect ---------------------------------------------------------------

def test_connect_default_pragmas(db):
    conn = _connect(db)
    try:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 30000
        assert conn.execute("PRAGMA query_only").fetchone()[0] == 0
    finally:
        conn.close()


def test_connect_read_only_and_overrides(tmp_path):
    path = str(tmp_path / "plain.db")
    SQLiteNode.init_schema(path, SCHEMA, connect_opts={"wal": False})
    conn = _connect(path, read_only=True,
                    connect_opts={"wal": False, "busy_timeout_s": 1.0,
                                  "foreign_keys": False})
    try:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
        assert conn.execute("PRAGMA query_only").fetchone()[0] == 1
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 1000
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 0
    finally:
        conn.close()


def test_connect_rejects_bad_synchronous(db):
    with pytest.raises(ValueError, match="synchronous"):
        _connect(db, connect_opts={"synchronous": "NORMAL; DROP TABLE t"})


# -- DDL / schema -----------------------------------------------------------

def test_init_schema_creates_parents_and_tables(tmp_path):
    path = str(tmp_path / "a" / "b" / "deep.db")
    SQLiteNode.init_schema(path, SCHEMA)
    assert os.path.exists(path)
    assert "CREATE TABLE t" in SQLiteNode.schema(path)


def test_schema_empty_db(tmp_path):
    path = str(tmp_path / "empty.db")
    SQLiteNode.init_schema(path, "")
    assert SQLiteNode.schema(path) == ""


def test_executescript(db):
    SQLiteNode.executescript(db, "CREATE TABLE u (x); INSERT INTO u VALUES (7);")
    assert SQLiteNode.query_value(db, "SELECT x FROM u") == 7


def test_add_column_if_missing(db):
    assert SQLiteNode.add_column_if_missing(db, "t", "extra", "TEXT") is True
    assert SQLiteNode.add_column_if_missing(db, "t", "extra", "TEXT") is False
    SQLiteNode.execute(db, "INSERT INTO t (v, extra) VALUES (?, ?)", ("a", "b"))
    assert SQLiteNode.query_one(db, "SELECT extra FROM t")["extra"] == "b"


@pytest.mark.parametrize("table,column,type_decl", [
    ("t; DROP TABLE t", "c", "TEXT"),
    ("t", "c d", "TEXT"),
    ("t", "c", "TEXT; DROP TABLE t"),
])
def test_add_column_rejects_bad_identifiers(db, table, column, type_decl):
    with pytest.raises(ValueError):
        SQLiteNode.add_column_if_missing(db, table, column, type_decl)


# -- writes -----------------------------------------------------------------

def test_execute_result_shape(db):
    res = SQLiteNode.execute(db, "INSERT INTO t (v) VALUES (?)", ("hello",))
    assert isinstance(res, SQLiteExecResult)
    assert res.rowcount == 1
    assert res.lastrowid == 1
    res = SQLiteNode.execute(db, "UPDATE t SET v = ?", ("bye",))
    assert res.rowcount == 1


def test_executemany(db):
    res = SQLiteNode.executemany(
        db, "INSERT INTO t (v) VALUES (?)", [("a",), ("b",), ("c",)])
    assert res.rowcount == 3
    assert res.lastrowid is None
    assert SQLiteNode.query_value(db, "SELECT COUNT(*) FROM t") == 3


def test_blob_roundtrip(db):
    blob = b"\x00\xff" + os.urandom(64)
    SQLiteNode.execute(db, "INSERT INTO t (v) VALUES (?)", (blob,))
    assert SQLiteNode.query_value(db, "SELECT v FROM t") == blob


# -- transaction ------------------------------------------------------------

def test_transaction_params_dispatch(db):
    results = SQLiteNode.transaction(db, [
        ("INSERT INTO t (v) VALUES ('bare')", None),                 # bare
        ("INSERT INTO t (v) VALUES (?)", ("tuple",)),                # execute
        ("INSERT INTO t (v) VALUES (:v)", {"v": "dict"}),            # execute
        ("INSERT INTO t (v) VALUES (?)", [("m1",), ("m2",)]),        # many
    ])
    assert len(results) == 4
    assert [r.rowcount for r in results] == [1, 1, 1, 2]
    assert results[3].lastrowid is None
    vals = [r["v"] for r in SQLiteNode.query(db, "SELECT v FROM t ORDER BY id")]
    assert vals == ["bare", "tuple", "dict", "m1", "m2"]


def test_transaction_atomic_rollback(db):
    SQLiteNode.execute(db, "INSERT INTO t (id, v) VALUES (1, 'pre')")
    with pytest.raises(sqlite3.IntegrityError):
        SQLiteNode.transaction(db, [
            ("INSERT INTO t (id, v) VALUES (2, 'ok')", None),
            ("INSERT INTO t (id, v) VALUES (1, 'dup')", None),  # PK collision
            ("INSERT INTO t (id, v) VALUES (3, 'never')", None),
        ])
    # The whole batch rolled back — only the pre-existing row remains.
    assert SQLiteNode.query_value(db, "SELECT COUNT(*) FROM t") == 1


def test_transaction_bad_op_shapes(db):
    with pytest.raises(TypeError, match=r"ops\[0\]"):
        SQLiteNode.transaction(db, ["INSERT INTO t (v) VALUES ('x')"])
    with pytest.raises(TypeError, match="params must be"):
        SQLiteNode.transaction(db, [("INSERT INTO t (v) VALUES (?)", "str")])


# -- reads ------------------------------------------------------------------

def test_query_shapes_and_limit(db):
    SQLiteNode.executemany(db, "INSERT INTO t (v) VALUES (?)",
                           [(f"v{i}",) for i in range(5)])
    rows = SQLiteNode.query(db, "SELECT * FROM t ORDER BY id")
    assert len(rows) == 5 and rows[0] == {"id": 1, "v": "v0"}
    assert len(SQLiteNode.query(db, "SELECT * FROM t", limit=2)) == 2
    assert SQLiteNode.query(db, "SELECT * FROM t WHERE 0") == []


def test_query_one_and_value(db):
    assert SQLiteNode.query_one(db, "SELECT * FROM t") is None
    assert SQLiteNode.query_value(db, "SELECT v FROM t", default="dflt") == "dflt"
    # Aggregate over an empty table yields a NULL row -> None, not default.
    assert SQLiteNode.query_value(db, "SELECT MAX(id) FROM t", default=-1) is None
    SQLiteNode.execute(db, "INSERT INTO t (v) VALUES ('x')")
    assert SQLiteNode.query_one(db, "SELECT v FROM t") == {"v": "x"}
    assert SQLiteNode.query_value(db, "SELECT MAX(id) FROM t", default=-1) == 1


def test_query_rejects_writes(db):
    with pytest.raises(sqlite3.OperationalError):
        SQLiteNode.query(db, "UPDATE t SET v = 'oops'")


def test_wal_checkpoint(db):
    SQLiteNode.execute(db, "INSERT INTO t (v) VALUES ('x')")
    out = SQLiteNode.wal_checkpoint(db)
    assert set(out) == {"busy", "log", "checkpointed"}
    assert out["busy"] == 0
    with pytest.raises(ValueError, match="checkpoint mode"):
        SQLiteNode.wal_checkpoint(db, mode="TRUNCATE); DROP TABLE t; --")


# -- constructor validation ---------------------------------------------------

def test_relative_db_path_rejected():
    with pytest.raises(ValueError, match="absolute"):
        SQLiteNode("relative/path.db", require_colocated=False)


def test_conflicting_placement_args_rejected(tmp_path):
    path = str(tmp_path / "x.db")
    with pytest.raises(ValueError, match="mutually exclusive"):
        SQLiteNode(path, placement_group=object(), node_id="abc")
    with pytest.raises(ValueError, match="not both"):
        SQLiteNode(path, node_id="abc", pin_to_current_node=True)


def test_unpinned_node_local_roundtrip(tmp_path):
    node = SQLiteNode(str(tmp_path / "n.db"), require_colocated=False)
    assert node.task_options == {}
    for name in SQLiteNode._MEMBER_FNS:
        assert isinstance(getattr(node, name), _DbBoundChiaFn)
    # Local calls inject db_path automatically.
    node.init_schema(SCHEMA)
    node.execute("INSERT INTO t (v) VALUES (?)", ("local",))
    assert node.query("SELECT v FROM t") == [{"v": "local"}]
    node.close()
    node.close()  # idempotent


def test_node_level_connect_opts_default(tmp_path):
    path = str(tmp_path / "nowal.db")
    node = SQLiteNode(path, require_colocated=False,
                      connect_opts={"wal": False})
    node.init_schema(SCHEMA)
    node.execute("INSERT INTO t (v) VALUES ('x')")
    conn = sqlite3.connect(path)
    try:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    finally:
        conn.close()


def test_spawn_query_tool_requires_placement(tmp_path):
    node = SQLiteNode(str(tmp_path / "x.db"), require_colocated=False)
    with pytest.raises(RuntimeError, match="placement"):
        node.spawn_query_tool("nope")


# -- _DbBoundChiaFn binding ---------------------------------------------------

class _FakeFn:
    """Mimics a ChiaWrapped function; records every invocation path."""

    def __init__(self):
        self.log = []

    def __call__(self, *a, **kw):
        self.log.append(("local", a, kw))
        return "val"

    def chia_remote(self, *a, **kw):
        self.log.append(("remote", a, kw))
        return "ref"

    def options(self, **opts):
        self.log.append(("options", opts))
        outer = self

        class _H:
            def chia_remote(self, *a, **kw):
                outer.log.append(("opt_remote", a, kw))
                return "ref2"

        return _H()


def test_db_bound_injects_on_all_paths():
    fake = _FakeFn()
    bound = _DbBoundChiaFn(fake, {}, ("DB",), {"connect_opts": {"wal": False}})

    assert bound("SQL", (1,)) == "val"
    assert fake.log[-1] == ("local", ("DB", "SQL", (1,)),
                            {"connect_opts": {"wal": False}})

    assert bound.chia_remote("SQL") == "ref"
    assert fake.log[-1] == ("remote", ("DB", "SQL"),
                            {"connect_opts": {"wal": False}})

    # Per-call kwargs override the bound defaults.
    bound.chia_remote("SQL", connect_opts={"wal": True})
    assert fake.log[-1][2] == {"connect_opts": {"wal": True}}

    bound.options(num_cpus=1).chia_remote("SQL")
    assert ("options", {"num_cpus": 1}) in fake.log
    assert fake.log[-1] == ("opt_remote", ("DB", "SQL"),
                            {"connect_opts": {"wal": False}})


def test_db_bound_with_scheduling_opts():
    fake = _FakeFn()
    bound = _DbBoundChiaFn(fake, {"scheduling_strategy": "SS"}, ("DB",))
    # The base class routed chia_remote through fn.options(<sched>).
    assert fake.log[0] == ("options", {"scheduling_strategy": "SS"})
    bound.chia_remote("SQL")
    assert fake.log[-1] == ("opt_remote", ("DB", "SQL"), {})
    # .options() merges the pin with the overrides.
    bound.options(num_cpus=2).chia_remote("SQL")
    assert ("options", {"scheduling_strategy": "SS", "num_cpus": 2}) in fake.log


# -- SQLiteQueryTool formatting (unstarted instance; no Ray) ------------------

def _unstarted_tool(db_path: str, **caps) -> SQLiteQueryTool:
    """Bypass __init__ (which starts a Ray actor); set just the attrs the
    query/schema/execute methods read."""
    t = SQLiteQueryTool.__new__(SQLiteQueryTool)
    t.db_path = db_path
    t.row_limit = caps.get("row_limit", 100)
    t.cell_char_limit = caps.get("cell_char_limit", 4096)
    t.total_char_limit = caps.get("total_char_limit", 32768)
    return t


def test_tool_query_markdown(db):
    SQLiteNode.execute(db, "INSERT INTO t (v) VALUES ('hello|world')")
    out = _unstarted_tool(db).query("SELECT id, v FROM t")
    lines = out.splitlines()
    assert lines[0] == "id | v"
    assert lines[1] == "--- | ---"
    assert lines[2] == "1 | hello\\|world"


def test_tool_query_caps(db):
    SQLiteNode.executemany(db, "INSERT INTO t (v) VALUES (?)",
                           [("x" * 50,) for _ in range(5)])
    out = _unstarted_tool(db, row_limit=2, cell_char_limit=10).query(
        "SELECT v FROM t")
    assert "NOTE:" in out
    assert "row limit of 2 hit" in out
    assert "truncated to 10 chars" in out


def test_tool_query_blocks_writes(db):
    out = _unstarted_tool(db).query("INSERT INTO t (v) VALUES ('nope')")
    assert out.startswith("SQL error:")
    assert SQLiteNode.query_value(db, "SELECT COUNT(*) FROM t") == 0


def test_tool_schema_and_execute(db):
    tool = _unstarted_tool(db)
    assert "CREATE TABLE t" in tool.schema()
    out = tool.execute("INSERT INTO t (v) VALUES ('rw')")
    assert out == "OK: rowcount=1 lastrowid=1"
    assert tool.execute("BOGUS SQL").startswith("SQL error:")
    assert SQLiteNode.query_value(db, "SELECT v FROM t") == "rw"


# ===========================================================================
# Tier 1 — live Ray cluster
# ===========================================================================

@pytest.fixture(scope="session")
def ray_cx():
    if ray.is_initialized():
        yield
        return
    try:
        ray.init(address=RAY_ADDR, namespace="sqlite_node_live_test",
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


def test_remote_roundtrip_through_pg(cpu_pg, tmp_path):
    node = SQLiteNode(str(tmp_path / "remote.db"), placement_group=cpu_pg)
    get(node.init_schema.chia_remote(SCHEMA))
    res = get(node.execute.chia_remote(
        "INSERT INTO t (v) VALUES (?)", ("remote",)))
    assert res.rowcount == 1
    assert get(node.query.chia_remote("SELECT v FROM t")) == [{"v": "remote"}]
    assert "CREATE TABLE t" in get(node.schema.chia_remote())
    node.close()


def test_concurrent_writers(cpu_pg, tmp_path):
    node = SQLiteNode(str(tmp_path / "conc.db"), placement_group=cpu_pg)
    get(node.init_schema.chia_remote(SCHEMA))
    refs = [node.execute.chia_remote("INSERT INTO t (v) VALUES (?)", (str(i),))
            for i in range(20)]
    for ref in refs:
        assert get(ref).rowcount == 1
    assert get(node.query_value.chia_remote("SELECT COUNT(*) FROM t")) == 20
    node.close()


def test_pin_to_current_node(ray_cx, tmp_path):
    path = str(tmp_path / "head.db")
    node = SQLiteNode(path, pin_to_current_node=True)
    strat = node.task_options["scheduling_strategy"]
    assert isinstance(strat, NodeAffinitySchedulingStrategy)
    assert strat.node_id == ray.get_runtime_context().get_node_id()
    assert strat.soft is False
    get(node.init_schema.chia_remote(SCHEMA))
    get(node.execute.chia_remote("INSERT INTO t (v) VALUES ('head')"))
    # Members ran on this node, so the file is on the driver's filesystem.
    assert os.path.exists(path)
    conn = sqlite3.connect(path)
    try:
        assert conn.execute("SELECT v FROM t").fetchone()[0] == "head"
    finally:
        conn.close()
    node.close()


def test_spawn_query_tool_colocates(cpu_pg, tmp_path):
    node = SQLiteNode(str(tmp_path / "tooled.db"), placement_group=cpu_pg)
    get(node.init_schema.chia_remote(SCHEMA))
    get(node.execute.chia_remote("INSERT INTO t (v) VALUES ('seen')"))
    tool = node.spawn_query_tool("sqlite_test_tool")
    try:
        assert tool.node_id is not None
        assert tool.hostname is not None
    finally:
        node.close()
    # close() stopped the spawned tool.
    assert tool._server_actor is None


def test_owned_pg_and_tools_released_on_close(ray_cx, tmp_path):
    node = SQLiteNode(str(tmp_path / "owned.db"),
                      pg_ready_timeout_s=PG_TIMEOUT)
    assert node.owns_placement_group is True
    pg = node.placement_group
    tool = node.spawn_query_tool("sqlite_owned_tool")
    node.close()
    assert tool._server_actor is None
    assert node.placement_group is None
    assert ray.util.placement_group_table(pg)["state"] == "REMOVED"


def test_given_pg_survives_close(cpu_pg, tmp_path):
    node = SQLiteNode(str(tmp_path / "given.db"), placement_group=cpu_pg)
    node.close()
    assert ray.util.placement_group_table(cpu_pg)["state"] != "REMOVED"
