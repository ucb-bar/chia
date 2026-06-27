"""Tests for chia.database.postgres_node — PostgresNode + PostgresQueryTool.

Tier 0a (no psycopg, no server — runs everywhere): construction from a str
DSN, the flipped require_colocated default, placement mutual-exclusion,
DatabaseNode abstractness (polymorphism contract), identifier validation
(which precedes the lazy psycopg import), and the query tool's
_format_table rendering via an unstarted instance.

Tier 0b (psycopg installed, no server): dict->conninfo normalization and
clean connection-refused errors.

Tier 1 (live server): every member against a uniquely-named schema (dropped
CASCADE on teardown), the read-only guard, and a Ray sub-tier (unpinned
remote roundtrip, spawn_query_tool without placement, tool stopped by
close()).  The ``pg_dsn`` fixture finds a server two ways, in order:

1. ``POSTGRES_NODE_TEST_DSN`` — an existing server, e.g.::

     docker run -d --name chia-pg -p 5432:5432 \
         -e POSTGRES_PASSWORD=chia postgres:16
     export POSTGRES_NODE_TEST_DSN="postgresql://postgres:chia@127.0.0.1:5432/postgres"

2. **Self-hosted**: if postgres server binaries are discoverable
   (``CHIA_PG_BINDIR`` env var, else ``initdb`` on PATH — e.g. from
   ``conda install -c conda-forge postgresql``), the fixture itself runs
   initdb into a pytest tmp dir, launches ``postgres`` on a free port,
   and tears it down at session end.  The fixture body doubles as the
   documented recipe for standing up a scratch server by hand.

Without either, tier 1 skips.

Configuration (env vars):
  POSTGRES_NODE_TEST_DSN          conninfo of an existing scratch database
  CHIA_PG_BINDIR                  dir containing initdb/postgres for the
                                  self-hosted path
  POSTGRES_NODE_TEST_RAY_ADDRESS  Ray address for the Ray sub-tier
                                  (default "auto"; skips if unreachable)

Run:
  pytest chia/chia/database/test/test_postgres_node_live.py -v
"""

from __future__ import annotations

import importlib.util
import os
import shutil
import signal
import socket
import subprocess
import time
import uuid

import pytest
import ray

from chia.base.ChiaFunction import get
from chia.database.base import DatabaseNode, ExecResult, _DbBoundChiaFn
from chia.database.postgres_node import (
    PostgresNode,
    PostgresQueryTool,
    _normalize_dsn,
)

PG_DSN = os.environ.get("POSTGRES_NODE_TEST_DSN")
RAY_ADDR = os.environ.get("POSTGRES_NODE_TEST_RAY_ADDRESS", "auto")

_PSYCOPG_INSTALLED = importlib.util.find_spec("psycopg") is not None


@pytest.fixture(scope="session", autouse=True)
def _disabled_profiler():
    """Pre-build the chia profiler singleton as disabled.

    A *local* @ChiaFunction call runs get_profiler(), whose first
    construction looks up the collector actor via ray.get_actor — and Ray's
    auto-init then tries to join whatever cluster address is lying around.
    Tier 0 must not touch Ray, so stub the collector lookup while the
    singleton is built."""
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


# ===========================================================================
# Tier 0a — no psycopg, no server
# ===========================================================================

def test_str_dsn_construction_needs_no_driver_and_no_ray():
    node = PostgresNode("postgresql://user@host:5432/db")
    # Flipped default: clients can run anywhere -> unpinned members.
    assert node.task_options == {}
    assert node.placement_group is None
    assert node.dsn == node.locator == "postgresql://user@host:5432/db"
    assert node.paramstyle == "format"
    for name in PostgresNode._MEMBER_FNS:
        assert isinstance(getattr(node, name), _DbBoundChiaFn)
    node.close()
    node.close()  # idempotent


def test_empty_or_bad_dsn_rejected():
    with pytest.raises(ValueError, match="non-empty"):
        PostgresNode("   ")
    with pytest.raises(TypeError, match="conninfo"):
        PostgresNode(123)


def test_conflicting_placement_args_rejected():
    dsn = "postgresql://u@h/db"
    with pytest.raises(ValueError, match="mutually exclusive"):
        PostgresNode(dsn, placement_group=object(), node_id="abc")
    with pytest.raises(ValueError, match="not both"):
        PostgresNode(dsn, node_id="abc", pin_to_current_node=True)


# -- DatabaseNode abstractness (polymorphism contract) ------------------------

def test_database_node_is_abstract():
    with pytest.raises(TypeError, match="abstract"):
        DatabaseNode("anything")


def test_subclass_missing_member_fails_at_construction():
    # Borrow every PostgresNode member except query_value: the subclass must
    # be rejected at instantiation (fail-fast), not at first dispatch.
    attrs = {
        name: staticmethod(getattr(PostgresNode, name))
        for name in DatabaseNode._MEMBER_FNS if name != "query_value"
    }
    attrs["spawn_query_tool"] = PostgresNode.spawn_query_tool
    attrs["paramstyle"] = "format"
    Incomplete = type("Incomplete", (DatabaseNode,), attrs)
    with pytest.raises(TypeError, match="query_value"):
        Incomplete("postgresql://u@h/db", require_colocated=False)


def test_paramstyle_required():
    attrs = {
        name: staticmethod(getattr(PostgresNode, name))
        for name in DatabaseNode._MEMBER_FNS
    }
    attrs["spawn_query_tool"] = PostgresNode.spawn_query_tool
    NoStyle = type("NoStyle", (DatabaseNode,), attrs)  # paramstyle unset
    with pytest.raises(TypeError, match="paramstyle"):
        NoStyle("postgresql://u@h/db", require_colocated=False)


def test_engine_blind_code_can_type_against_base(tmp_path):
    # Both engines satisfy the same isinstance contract.
    from chia.database.sqlite_node import SQLiteNode
    sq = SQLiteNode(str(tmp_path / "x.db"), require_colocated=False)
    pg = PostgresNode("postgresql://u@h/db")
    assert isinstance(sq, DatabaseNode) and isinstance(pg, DatabaseNode)
    assert {sq.paramstyle, pg.paramstyle} == {"qmark", "format"}
    sq.close()
    pg.close()


# -- validation precedes the lazy driver import --------------------------------

@pytest.mark.parametrize("table,column,type_decl", [
    ("t; DROP TABLE t", "c", "TEXT"),
    ("sch.t.extra", "c", "TEXT"),
    ("t", "c d", "TEXT"),
    ("t", "c", "TEXT; DROP TABLE t"),
])
def test_add_column_rejects_bad_identifiers(table, column, type_decl):
    # Raises ValueError without a server and without psycopg.
    with pytest.raises(ValueError):
        PostgresNode.add_column_if_missing(
            "postgresql://u@h/db", table, column, type_decl)


@pytest.mark.skipif(_PSYCOPG_INSTALLED, reason="psycopg installed")
def test_dict_dsn_without_psycopg_names_the_extra():
    with pytest.raises(ImportError, match=r"chia\[postgres\]"):
        PostgresNode({"host": "h", "dbname": "db"})


# -- tool rendering (unstarted instance; no Ray, no server) --------------------

def _unstarted_tool(**caps) -> PostgresQueryTool:
    """Bypass __init__ (which starts a Ray actor); set just the attrs the
    rendering/query methods read."""
    t = PostgresQueryTool.__new__(PostgresQueryTool)
    t.dsn = "postgresql://unused@nowhere/none"
    t.row_limit = caps.get("row_limit", 100)
    t.cell_char_limit = caps.get("cell_char_limit", 4096)
    t.total_char_limit = caps.get("total_char_limit", 32768)
    t.schemas = ("public",)
    return t


def test_format_table_markdown_and_escaping():
    out = _unstarted_tool()._format_table(
        ["id", "v"], [(1, "hello|world"), (2, None)])
    lines = out.splitlines()
    assert lines[0] == "id | v"
    assert lines[1] == "--- | ---"
    assert lines[2] == "1 | hello\\|world"
    assert lines[3] == "2 | NULL"


def test_format_table_caps():
    rows = [("x" * 50,) for _ in range(3)]  # row_limit+1 signals clipping
    out = _unstarted_tool(row_limit=2, cell_char_limit=10)._format_table(
        ["v"], rows)
    assert "NOTE:" in out
    assert "row limit of 2 hit" in out
    assert "truncated to 10 chars" in out


def test_format_table_no_columns():
    assert _unstarted_tool()._format_table([], []) == \
        "(query returned no columns)"


# ===========================================================================
# Tier 0b — psycopg installed, no server
# ===========================================================================

@pytest.mark.skipif(not _PSYCOPG_INSTALLED, reason="psycopg not installed")
def test_dict_dsn_normalization():
    dsn = _normalize_dsn({"host": "h", "port": 5499, "dbname": "db",
                          "user": "u"})
    assert "host=h" in dsn and "port=5499" in dsn and "dbname=db" in dsn


@pytest.mark.skipif(not _PSYCOPG_INSTALLED, reason="psycopg not installed")
def test_connection_refused_surfaces_cleanly():
    import psycopg
    with pytest.raises(psycopg.OperationalError):
        PostgresNode.query(
            "postgresql://postgres@127.0.0.1:1/postgres",
            "SELECT 1",
            connect_opts={"connect_timeout_s": 2},
        )


# ===========================================================================
# Tier 1 — live server (POSTGRES_NODE_TEST_DSN)
# ===========================================================================

def _find_pg_bindir() -> str | None:
    """Directory holding working ``initdb``/``postgres`` binaries, or None.

    Looks at ``CHIA_PG_BINDIR`` first, then PATH.  Easiest way to get
    binaries without docker::

        conda create -n pg -c conda-forge postgresql
        export CHIA_PG_BINDIR=$HOME/miniconda3/envs/pg/bin

    A candidate is verified by running both ``initdb --version`` AND
    ``postgres --version`` — vendor trees (e.g. Calibre's) can ship an
    initdb that runs while the postgres binary itself is broken by missing
    shared libraries.
    """
    candidates = []
    env_dir = os.environ.get("CHIA_PG_BINDIR")
    if env_dir:
        candidates.append(env_dir)
    which = shutil.which("initdb")
    if which:
        candidates.append(os.path.dirname(which))
    for bindir in candidates:
        try:
            for exe in ("initdb", "postgres"):
                subprocess.run([os.path.join(bindir, exe), "--version"],
                               capture_output=True, check=True, timeout=10)
            return bindir
        except (OSError, subprocess.SubprocessError):
            continue
    return None


@pytest.fixture(scope="session")
def pg_dsn(tmp_path_factory) -> str:
    """A live postgres DSN: POSTGRES_NODE_TEST_DSN if set, else a scratch
    server this fixture hosts itself.

    The self-hosting branch below IS the recipe for standing up a throwaway
    postgres with nothing but server binaries: initdb a data dir, launch
    ``postgres`` on a free port, connect with the superuser DSN, and SIGINT
    (fast shutdown) when done.
    """
    psycopg = pytest.importorskip("psycopg")
    if PG_DSN:
        yield PG_DSN
        return

    bindir = _find_pg_bindir()
    if not bindir:
        pytest.skip(
            "no live postgres: either set POSTGRES_NODE_TEST_DSN to an "
            "existing server, or make server binaries discoverable for the "
            "self-hosted path (CHIA_PG_BINDIR=<dir with initdb>, or initdb "
            "on PATH — e.g. conda install -c conda-forge postgresql)")

    # 1. Init a cluster in a temp data dir (superuser + password auth).
    data_dir = tmp_path_factory.mktemp("pg_data")
    sock_dir = tmp_path_factory.mktemp("pg_sock")
    pwfile = tmp_path_factory.mktemp("pg_pw") / "pw"
    pwfile.write_text("chia\n")
    try:
        subprocess.run(
            [os.path.join(bindir, "initdb"), "-D", str(data_dir),
             "-U", "postgres", "-A", "scram-sha-256", f"--pwfile={pwfile}"],
            check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        pytest.fail(f"initdb from {bindir} failed:\n"
                    f"{e.stderr.decode(errors='replace')[-2000:]}")

    # 2. Pick a free port and launch postgres as a direct child process.
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    log_path = data_dir / "server.log"
    proc = subprocess.Popen(
        [os.path.join(bindir, "postgres"), "-D", str(data_dir),
         "-p", str(port),
         "-c", "listen_addresses=127.0.0.1",
         "-c", f"unix_socket_directories={sock_dir}"],
        stdout=open(log_path, "w"), stderr=subprocess.STDOUT)

    # 3. Wait until it accepts connections.
    dsn = f"postgresql://postgres:chia@127.0.0.1:{port}/postgres"
    deadline = time.time() + 60
    while True:
        try:
            psycopg.connect(dsn, connect_timeout=2).close()
            break
        except psycopg.Error:
            if proc.poll() is not None or time.time() > deadline:
                proc.kill()
                tail = log_path.read_text()[-2000:] if log_path.exists() else ""
                pytest.fail(f"scratch postgres failed to start:\n{tail}")
            time.sleep(0.3)

    yield dsn

    # 4. Fast shutdown (SIGINT); escalate if it hangs.
    proc.send_signal(signal.SIGINT)
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


@pytest.fixture()
def pg_schema(pg_dsn):
    """A uniquely named schema holding one test table; yields (dsn, schema).
    Dropped CASCADE on teardown so reruns never collide."""
    sch = f"chia_test_{uuid.uuid4().hex[:8]}"
    PostgresNode.init_schema(
        pg_dsn,
        f"CREATE SCHEMA {sch}; "
        f"CREATE TABLE {sch}.t (id SERIAL PRIMARY KEY, v TEXT);",
    )
    yield pg_dsn, sch
    PostgresNode.executescript(pg_dsn, f"DROP SCHEMA {sch} CASCADE")


def test_execute_returning_rows(pg_schema):
    dsn, sch = pg_schema
    res = PostgresNode.execute(
        dsn, f"INSERT INTO {sch}.t (v) VALUES (%s) RETURNING id", ("x",))
    assert isinstance(res, ExecResult)
    assert res.rowcount == 1
    assert res.lastrowid is None          # postgres: use RETURNING instead
    assert res.rows == [{"id": 1}]
    res = PostgresNode.execute(dsn, f"UPDATE {sch}.t SET v = %s", ("y",))
    assert res.rowcount == 1 and res.rows is None


def test_executemany(pg_schema):
    dsn, sch = pg_schema
    res = PostgresNode.executemany(
        dsn, f"INSERT INTO {sch}.t (v) VALUES (%s)",
        [("a",), ("b",), ("c",)])
    assert res.rowcount == 3 and res.lastrowid is None
    assert PostgresNode.query_value(
        dsn, f"SELECT COUNT(*) FROM {sch}.t") == 3


def test_transaction_params_dispatch(pg_schema):
    dsn, sch = pg_schema
    results = PostgresNode.transaction(dsn, [
        (f"INSERT INTO {sch}.t (v) VALUES ('bare')", None),
        (f"INSERT INTO {sch}.t (v) VALUES (%s)", ("tuple",)),
        (f"INSERT INTO {sch}.t (v) VALUES (%(v)s)", {"v": "dict"}),
        (f"INSERT INTO {sch}.t (v) VALUES (%s)", [("m1",), ("m2",)]),
    ])
    assert [r.rowcount for r in results] == [1, 1, 1, 2]
    vals = [r["v"] for r in PostgresNode.query(
        dsn, f"SELECT v FROM {sch}.t ORDER BY id")]
    assert vals == ["bare", "tuple", "dict", "m1", "m2"]


def test_transaction_atomic_rollback(pg_schema):
    dsn, sch = pg_schema
    import psycopg
    PostgresNode.execute(dsn, f"INSERT INTO {sch}.t (id, v) VALUES (1, 'pre')")
    with pytest.raises(psycopg.errors.UniqueViolation):
        PostgresNode.transaction(dsn, [
            (f"INSERT INTO {sch}.t (id, v) VALUES (2, 'ok')", None),
            (f"INSERT INTO {sch}.t (id, v) VALUES (1, 'dup')", None),
            (f"INSERT INTO {sch}.t (id, v) VALUES (3, 'never')", None),
        ])
    assert PostgresNode.query_value(
        dsn, f"SELECT COUNT(*) FROM {sch}.t") == 1


def test_transaction_bad_op_shapes(pg_schema):
    dsn, sch = pg_schema
    with pytest.raises(TypeError, match=r"ops\[0\]"):
        PostgresNode.transaction(dsn, [f"INSERT INTO {sch}.t (v) VALUES ('x')"])
    with pytest.raises(TypeError, match="params must be"):
        PostgresNode.transaction(
            dsn, [(f"INSERT INTO {sch}.t (v) VALUES (%s)", "str")])


def test_query_shapes_and_limit(pg_schema):
    dsn, sch = pg_schema
    PostgresNode.executemany(dsn, f"INSERT INTO {sch}.t (v) VALUES (%s)",
                             [(f"v{i}",) for i in range(5)])
    rows = PostgresNode.query(dsn, f"SELECT * FROM {sch}.t ORDER BY id")
    assert len(rows) == 5 and rows[0] == {"id": 1, "v": "v0"}
    assert len(PostgresNode.query(dsn, f"SELECT * FROM {sch}.t", limit=2)) == 2
    assert PostgresNode.query(dsn, f"SELECT * FROM {sch}.t WHERE false") == []


def test_query_one_and_value(pg_schema):
    dsn, sch = pg_schema
    assert PostgresNode.query_one(dsn, f"SELECT * FROM {sch}.t") is None
    assert PostgresNode.query_value(
        dsn, f"SELECT v FROM {sch}.t", default="dflt") == "dflt"
    # Aggregate over an empty table yields a NULL row -> None, not default.
    assert PostgresNode.query_value(
        dsn, f"SELECT MAX(id) FROM {sch}.t", default=-1) is None
    PostgresNode.execute(dsn, f"INSERT INTO {sch}.t (v) VALUES ('x')")
    assert PostgresNode.query_one(
        dsn, f"SELECT v FROM {sch}.t") == {"v": "x"}
    assert PostgresNode.query_value(
        dsn, f"SELECT MAX(id) FROM {sch}.t", default=-1) == 1


def test_query_rejects_writes(pg_schema):
    dsn, sch = pg_schema
    import psycopg
    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        PostgresNode.query(dsn, f"INSERT INTO {sch}.t (v) VALUES ('w')")
    assert PostgresNode.query_value(
        dsn, f"SELECT COUNT(*) FROM {sch}.t") == 0


def test_add_column_if_missing(pg_schema):
    dsn, sch = pg_schema
    assert PostgresNode.add_column_if_missing(
        dsn, f"{sch}.t", "extra", "TEXT") is True
    assert PostgresNode.add_column_if_missing(
        dsn, f"{sch}.t", "extra", "TEXT") is False
    PostgresNode.execute(
        dsn, f"INSERT INTO {sch}.t (v, extra) VALUES (%s, %s)", ("a", "b"))
    assert PostgresNode.query_one(
        dsn, f"SELECT extra FROM {sch}.t")["extra"] == "b"


def test_schema_rendering(pg_schema):
    dsn, sch = pg_schema
    out = PostgresNode.schema(dsn, schemas=(sch,))
    assert f"TABLE {sch}.t" in out
    assert "id integer NOT NULL" in out
    assert "t_pkey" in out  # the SERIAL PK's index


# -- Ray sub-tier --------------------------------------------------------------

@pytest.fixture(scope="session")
def ray_cx(pg_dsn):
    if ray.is_initialized():
        yield
        return
    try:
        ray.init(address=RAY_ADDR, namespace="postgres_node_live_test",
                 log_to_driver=False)
    except Exception as e:
        pytest.skip(f"cannot connect to Ray at {RAY_ADDR}: {e}")
    yield
    ray.shutdown()


def test_unpinned_remote_roundtrip(ray_cx, pg_schema):
    dsn, sch = pg_schema
    node = PostgresNode(dsn)            # unpinned: members run on any worker
    res = get(node.execute.chia_remote(
        f"INSERT INTO {sch}.t (v) VALUES (%s) RETURNING id", ("remote",)))
    assert res.rows == [{"id": 1}]
    assert get(node.query.chia_remote(
        f"SELECT v FROM {sch}.t")) == [{"v": "remote"}]
    node.close()


def test_spawn_query_tool_without_placement(ray_cx, pg_schema):
    dsn, sch = pg_schema
    node = PostgresNode(dsn)
    get(node.execute.chia_remote(
        f"INSERT INTO {sch}.t (v) VALUES ('seen')"))
    tool = node.spawn_query_tool("pg_test_tool", schemas=(sch,))
    try:
        assert tool.node_id is not None
        out = tool.query(f"SELECT v FROM {sch}.t")
        assert "seen" in out and out.splitlines()[0] == "v"
        blocked = tool.query(f"INSERT INTO {sch}.t (v) VALUES ('nope')")
        assert blocked.startswith("SQL error:")
        assert f"TABLE {sch}.t" in tool.schema()
    finally:
        node.close()
    assert tool._server_actor is None   # close() stopped the spawned tool
