"""chia.database.sqlite_node — generic colocated SQLite store for chia loops.

:class:`SQLiteNode` is a :class:`~chia.database.base.DatabaseNode` (and thus
a :class:`~chia.base.colocated.ColocatedNode`) whose member
``@ChiaFunction``\\ s all operate on one SQLite database file.  Because
every member is pinned to the same machine, they all see the same file —
that colocation guarantee is what makes a plain on-disk SQLite database a
safe shared store for a distributed chia loop.  SQL placeholders are
sqlite's ``paramstyle = "qmark"`` (``?`` positional, ``:name`` named); see
:class:`~chia.database.postgres_node.PostgresNode` for the client-server
sibling.

Placement modes (decided once at construction):

  * default / ``placement_group=...`` — standard ColocatedNode pinning; a
    fresh DB lands wherever the bundle does and all members co-locate there.
  * ``node_id=...`` / ``pin_to_current_node=True`` — hard NodeAffinity pin,
    for a DB file that already lives on a known machine (e.g. the head
    node's local disk, as in gem5align's alignment.db).

Concurrency model: each ``chia_remote`` call may run in a *different worker
process* on the pinned machine, so every member opens a fresh connection.
Defaults: WAL journal mode (readers never block the writer), 30 s busy
timeout, ``BEGIN IMMEDIATE`` write transactions (concurrent writers queue on
the lock instead of failing mid-transaction), ``synchronous=NORMAL``,
``foreign_keys=ON``.  Override per node or per call via ``connect_opts``
(keys: ``busy_timeout_s``, ``wal``, ``synchronous``, ``foreign_keys``).

.. warning::
   Never point ``db_path`` at NFS or other shared/network storage.  WAL's
   shared-memory coordination only works between processes on one machine
   with a local filesystem — which is exactly what the colocation guarantee
   provides.  On network filesystems WAL can corrupt the database.

Data guidance: rows travel through the Ray object store as ``list[dict]``
(BLOB columns round-trip as ``bytes``).  Store large artifacts as files and
put *paths* in the DB; multi-MB blobs inflate the object store and every
``get()``.

Write semantics: write members are declared with ``max_retries=0`` — Ray's
task replay after a worker death would double-apply a committed-but-
unreturned non-idempotent write.  Callers that know a write is idempotent
can opt back in via ``node.execute.options(max_retries=...)``.

Example — gem5align's AlignmentDB expressed on this node::

    db = SQLiteNode("/abs/path/alignment.db", pin_to_current_node=True)
    get(db.init_schema.chia_remote(ITERATIONS_SCHEMA + BENCH_SCHEMA))
    get(db.add_column_if_missing.chia_remote("iterations", "base_rev", "TEXT"))
    tool = db.spawn_query_tool("align_db")        # read-only SQL for LLMs

    # insert_iteration: INSERT + DELETE + bulk INSERT, atomically
    get(db.transaction.chia_remote([
        ("INSERT OR REPLACE INTO iterations (...) VALUES (?, ...)", iter_row),
        ("DELETE FROM benchmark_results WHERE entry_id = ?", (entry_id,)),
        ("INSERT INTO benchmark_results (...) VALUES (?, ...)", bench_rows),
    ]))

    max_iter = get(db.query_value.chia_remote(
        "SELECT MAX(iteration) FROM iterations", default=-1))

Composite reads with Python-side logic (an AlignmentDB ``load_entry`` /
``best_per_benchmark``) belong in a domain subclass — append them to
``_MEMBER_FNS`` and the binding machinery picks them up::

    class AlignmentDBNode(SQLiteNode):
        _MEMBER_FNS = SQLiteNode._MEMBER_FNS + ("load_entry",)

        @staticmethod
        @ChiaFunction(num_cpus=0.1)
        def load_entry(db_path: str, entry_id: str, *,
                       connect_opts: dict | None = None) -> dict | None:
            conn = _connect(db_path, read_only=True, connect_opts=connect_opts)
            ...
"""

from __future__ import annotations

import contextlib
import os
import re
import sqlite3
from typing import Any

from chia.base.ChiaFunction import ChiaFunction
from chia.base.tools.ChiaTool import ChiaTool
from chia.database.base import (
    DatabaseNode,
    ExecResult,
    _DbBoundChiaFn,  # noqa: F401  (re-exported: part of this module's API)
    _IDENT_RE,
    _TYPE_DECL_RE,
)

#: Back-compat alias — the original name for the shared result dataclass.
SQLiteExecResult = ExecResult


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------

_CONNECT_DEFAULTS = {
    "busy_timeout_s": 30.0,
    "wal": True,
    "synchronous": "NORMAL",
    "foreign_keys": True,
}

_SYNC_RE = re.compile(r"[A-Za-z0-9]+")


def _connect(
    db_path: str,
    *,
    read_only: bool = False,
    connect_opts: dict | None = None,
) -> sqlite3.Connection:
    """Open a fresh connection with the node's PRAGMA defaults applied.

    ``read_only`` sets ``PRAGMA query_only=ON`` on a normal read-write open
    (not a ``mode=ro`` URI: a read-only open of a WAL database fails when the
    ``-shm``/``-wal`` sidecars need creation or recovery).  ``query_only`` is
    a guard against accidental writes, not an adversarial boundary — the
    LLM-facing :class:`SQLiteQueryTool` keeps the hard ``mode=ro`` open.

    ``isolation_level=None`` puts the connection in autocommit; write members
    manage transactions explicitly with ``BEGIN IMMEDIATE`` / ``COMMIT``.
    """
    o = {**_CONNECT_DEFAULTS, **(connect_opts or {})}
    if not _SYNC_RE.fullmatch(str(o["synchronous"])):
        raise ValueError(f"invalid synchronous level: {o['synchronous']!r}")
    conn = sqlite3.connect(db_path, timeout=float(o["busy_timeout_s"]),
                           isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={int(float(o['busy_timeout_s']) * 1000)}")
    conn.execute(f"PRAGMA foreign_keys={'ON' if o['foreign_keys'] else 'OFF'}")
    if o["wal"]:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(f"PRAGMA synchronous={o['synchronous']}")
    if read_only:
        conn.execute("PRAGMA query_only=ON")
    return conn


@contextlib.contextmanager
def _immediate_txn(conn: sqlite3.Connection):
    """``BEGIN IMMEDIATE`` ... ``COMMIT``, rolling back on any error.

    IMMEDIATE takes the write lock up front so a concurrent writer waits on
    ``busy_timeout`` at BEGIN rather than hitting a mid-transaction lock
    upgrade failure.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass  # keep the original exception
        raise
    conn.execute("COMMIT")


# ---------------------------------------------------------------------------
# The node
# ---------------------------------------------------------------------------

class SQLiteNode(DatabaseNode):
    """Colocated node exposing generic SQLite operations over one DB file.

    All members are ``@staticmethod @ChiaFunction`` taking ``db_path`` as the
    explicit first argument; ``__init__`` re-binds them so instance calls
    inject ``self.db_path`` (and the node's ``connect_opts``) automatically::

        with SQLiteNode("/abs/path/state.db") as db:        # reserves a PG
            get(db.init_schema.chia_remote("CREATE TABLE IF NOT EXISTS t ..."))
            get(db.execute.chia_remote("INSERT INTO t VALUES (?)", (1,)))
            rows = get(db.query.chia_remote("SELECT * FROM t"))

    See the module docstring for placement modes, concurrency model, and the
    AlignmentDB mapping example.
    """

    _MEMBER_FNS = DatabaseNode._MEMBER_FNS + ("wal_checkpoint",)
    # Plain CPU bundle: sqlite3 is stdlib, present on every worker.  To steer
    # placement onto a labeled DB host, pass e.g.
    # reserve_bundle={"CPU": 1, "database": 0.1}.
    _DEFAULT_BUNDLE = {"CPU": 1}
    paramstyle = "qmark"  # ? positional, :name named

    def __init__(
        self,
        db_path: str | os.PathLike,
        placement_group=None,
        require_colocated: bool = True,
        *,
        node_id: str | None = None,
        pin_to_current_node: bool = False,
        bundle_index: int = 0,
        reserve_bundle: dict | None = None,
        pg_strategy: str = "STRICT_PACK",
        wait_for_pg: bool = True,
        pg_ready_timeout_s: float | None = None,
        connect_opts: dict | None = None,
    ):
        """Validate ``db_path`` and defer to :class:`DatabaseNode`.

        Args:
            db_path: absolute path of the database file *on the target
                machine* (a relative path would resolve against a Ray
                worker's cwd).  Created on first write if absent.
            connect_opts: connection defaults injected into every member
                call (keys: ``busy_timeout_s``, ``wal``, ``synchronous``,
                ``foreign_keys``); per-call ``connect_opts=`` overrides.

        Remaining args are as in :class:`DatabaseNode`.
        """
        db_path = str(db_path)
        if not os.path.isabs(db_path):
            raise ValueError(
                f"db_path must be absolute (it is resolved on the target "
                f"machine's filesystem); got {db_path!r}"
            )
        super().__init__(
            db_path,
            placement_group,
            require_colocated,
            node_id=node_id,
            pin_to_current_node=pin_to_current_node,
            bundle_index=bundle_index,
            reserve_bundle=reserve_bundle,
            pg_strategy=pg_strategy,
            wait_for_pg=wait_for_pg,
            pg_ready_timeout_s=pg_ready_timeout_s,
            connect_opts=connect_opts,
        )
        self.db_path = self.locator

    # -- tool spawning --------------------------------------------------------

    def spawn_query_tool(self, name: str, *, read_write: bool = False,
                         **tool_kwargs) -> "SQLiteQueryTool":
        """Create a :class:`SQLiteQueryTool` co-located with this node.

        Exposes read-only SQL (and, with ``read_write=True``, single-statement
        writes) over MCP, pinned to the same machine as the members so the
        tool reads the same DB file.  The returned tool is tracked and stopped
        by :meth:`close`.  ``tool_kwargs`` forward to ``SQLiteQueryTool``
        (e.g. ``row_limit``, ``cell_char_limit``, ``total_char_limit``).
        """
        if not self._sched_opts:
            raise RuntimeError(
                "spawn_query_tool needs placement to co-locate against; "
                "construct with require_colocated=True, placement_group=..., "
                "node_id=..., or pin_to_current_node=True"
            )
        tool = SQLiteQueryTool(
            name,
            db_path=self.db_path,
            task_options=self.task_options,
            read_write=read_write,
            **tool_kwargs,
        )
        self._tools.append(tool)
        return tool

    # -- DDL / schema ---------------------------------------------------------

    @staticmethod
    @ChiaFunction(num_cpus=0.1, max_retries=0)
    def init_schema(db_path: str, script: str, *,
                    connect_opts: dict | None = None) -> None:
        """Create parent dirs and the DB file, then run ``script`` via
        ``executescript``.  Idempotent ``CREATE TABLE IF NOT EXISTS``-style
        scripts are encouraged; ``executescript`` semantics apply (the script
        may contain its own BEGIN/COMMIT)."""
        parent = os.path.dirname(db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        conn = _connect(db_path, connect_opts=connect_opts)
        try:
            conn.executescript(script)
        finally:
            conn.close()

    @staticmethod
    @ChiaFunction(num_cpus=0.1, max_retries=0)
    def executescript(db_path: str, script: str, *,
                      connect_opts: dict | None = None) -> None:
        """Run a multi-statement SQL script (no params — sqlite3 limitation).
        Like :meth:`init_schema` without the create-dirs step."""
        conn = _connect(db_path, connect_opts=connect_opts)
        try:
            conn.executescript(script)
        finally:
            conn.close()

    @staticmethod
    @ChiaFunction(num_cpus=0.1, max_retries=0)
    def add_column_if_missing(db_path: str, table: str, column: str,
                              type_decl: str, *,
                              connect_opts: dict | None = None) -> bool:
        """``ALTER TABLE ADD COLUMN`` iff the column is absent; True iff added.

        ``table``/``column`` must be plain identifiers and ``type_decl`` a
        benign type expression — they are interpolated into the SQL
        (identifiers cannot be parameters)."""
        for ident in (table, column):
            if not _IDENT_RE.fullmatch(ident):
                raise ValueError(f"invalid SQL identifier: {ident!r}")
        if not _TYPE_DECL_RE.fullmatch(type_decl):
            raise ValueError(f"invalid column type declaration: {type_decl!r}")
        conn = _connect(db_path, connect_opts=connect_opts)
        try:
            with _immediate_txn(conn):
                cols = {r["name"] for r in
                        conn.execute(f"PRAGMA table_info({table})")}
                if column in cols:
                    return False
                conn.execute(
                    f"ALTER TABLE {table} ADD COLUMN {column} {type_decl}")
                return True
        finally:
            conn.close()

    @staticmethod
    @ChiaFunction(num_cpus=0.1)
    def schema(db_path: str, *, connect_opts: dict | None = None) -> str:
        """CREATE TABLE / CREATE INDEX statements from ``sqlite_master``."""
        conn = _connect(db_path, read_only=True, connect_opts=connect_opts)
        try:
            rows = conn.execute(
                "SELECT sql FROM sqlite_master "
                "WHERE type IN ('table','index') "
                "AND sql IS NOT NULL "
                "AND name NOT LIKE 'sqlite_%' "
                "ORDER BY type, name"
            ).fetchall()
        finally:
            conn.close()
        return ";\n\n".join(r[0] for r in rows) + (";" if rows else "")

    # -- writes ---------------------------------------------------------------

    @staticmethod
    @ChiaFunction(num_cpus=0.1, max_retries=0)
    def execute(db_path: str, sql: str, params: tuple | dict = (), *,
                connect_opts: dict | None = None) -> SQLiteExecResult:
        """Run one statement in its own committed transaction."""
        conn = _connect(db_path, connect_opts=connect_opts)
        try:
            with _immediate_txn(conn):
                cur = conn.execute(sql, params)
                return SQLiteExecResult(rowcount=cur.rowcount,
                                        lastrowid=cur.lastrowid)
        finally:
            conn.close()

    @staticmethod
    @ChiaFunction(num_cpus=0.1, max_retries=0)
    def executemany(db_path: str, sql: str, seq_of_params: list, *,
                    connect_opts: dict | None = None) -> SQLiteExecResult:
        """``executemany`` in one committed transaction; ``rowcount`` is the
        total across all parameter sets."""
        conn = _connect(db_path, connect_opts=connect_opts)
        try:
            with _immediate_txn(conn):
                cur = conn.executemany(sql, seq_of_params)
                return SQLiteExecResult(rowcount=cur.rowcount, lastrowid=None)
        finally:
            conn.close()

    @staticmethod
    @ChiaFunction(num_cpus=0.1, max_retries=0)
    def transaction(db_path: str, ops: list, *,
                    connect_opts: dict | None = None) -> list[SQLiteExecResult]:
        """Run ``ops`` atomically: BEGIN IMMEDIATE; each op in order; COMMIT.

        Each op is ``(sql, params)`` where the params TYPE selects the form:

        * ``tuple`` or ``dict`` -> ``execute(sql, params)``  (single statement)
        * ``list``              -> ``executemany(sql, params)``  (bulk rows)
        * ``None``              -> ``execute(sql)``  (no params)

        NOTE: single-statement params MUST be a tuple/dict — a list means
        executemany.  Any sqlite error rolls the whole batch back and
        re-raises (surfacing as a Ray task error at ``get()``).  Explicit
        BEGIN/COMMIT inside op SQL is unsupported.  Returns one
        :class:`SQLiteExecResult` per op."""
        conn = _connect(db_path, connect_opts=connect_opts)
        try:
            results: list[SQLiteExecResult] = []
            with _immediate_txn(conn):
                for i, op in enumerate(ops):
                    try:
                        sql, params = op
                    except (TypeError, ValueError):
                        raise TypeError(
                            f"ops[{i}] must be a (sql, params) pair; got {op!r}"
                        ) from None
                    if params is None:
                        cur = conn.execute(sql)
                        results.append(SQLiteExecResult(cur.rowcount,
                                                        cur.lastrowid))
                    elif isinstance(params, list):
                        cur = conn.executemany(sql, params)
                        results.append(SQLiteExecResult(cur.rowcount, None))
                    elif isinstance(params, (tuple, dict)):
                        cur = conn.execute(sql, params)
                        results.append(SQLiteExecResult(cur.rowcount,
                                                        cur.lastrowid))
                    else:
                        raise TypeError(
                            f"ops[{i}] params must be tuple/dict (execute), "
                            f"list (executemany), or None; got "
                            f"{type(params).__name__}"
                        )
            return results
        finally:
            conn.close()

    # -- reads ----------------------------------------------------------------

    @staticmethod
    @ChiaFunction(num_cpus=0.1)
    def query(db_path: str, sql: str, params: tuple | dict = (), *,
              limit: int | None = None,
              connect_opts: dict | None = None) -> list[dict]:
        """Rows as ``list[dict]``.  ``limit=N`` caps the fetch (protects the
        object store from unbounded SELECTs); default None fetches all.  The
        connection is opened with ``PRAGMA query_only=ON``, so an accidental
        write raises ``OperationalError``."""
        conn = _connect(db_path, read_only=True, connect_opts=connect_opts)
        try:
            cur = conn.execute(sql, params)
            rows = cur.fetchmany(limit) if limit is not None else cur.fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    @staticmethod
    @ChiaFunction(num_cpus=0.1)
    def query_one(db_path: str, sql: str, params: tuple | dict = (), *,
                  connect_opts: dict | None = None) -> dict | None:
        """First row as a dict, or None."""
        conn = _connect(db_path, read_only=True, connect_opts=connect_opts)
        try:
            row = conn.execute(sql, params).fetchone()
            return dict(row) if row is not None else None
        finally:
            conn.close()

    @staticmethod
    @ChiaFunction(num_cpus=0.1)
    def query_value(db_path: str, sql: str, params: tuple | dict = (), *,
                    default: Any = None,
                    connect_opts: dict | None = None) -> Any:
        """First column of the first row, or ``default`` when there is no row
        (e.g. ``query_value("SELECT MAX(iteration) FROM t", default=-1)`` —
        note an aggregate over an empty table yields a NULL row, returned as
        None, not ``default``)."""
        conn = _connect(db_path, read_only=True, connect_opts=connect_opts)
        try:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row is not None else default
        finally:
            conn.close()

    # -- maintenance ----------------------------------------------------------

    @staticmethod
    @ChiaFunction(num_cpus=0.1, max_retries=0)
    def wal_checkpoint(db_path: str, mode: str = "TRUNCATE", *,
                       connect_opts: dict | None = None) -> dict:
        """``PRAGMA wal_checkpoint(mode)`` — fold the ``-wal`` sidecar back
        into the main file so the DB is a single copyable file (snapshot /
        backup story).  Returns ``{"busy", "log", "checkpointed"}``."""
        mode_u = mode.upper()
        if mode_u not in ("PASSIVE", "FULL", "RESTART", "TRUNCATE"):
            raise ValueError(f"invalid checkpoint mode: {mode!r}")
        conn = _connect(db_path, connect_opts=connect_opts)
        try:
            row = conn.execute(f"PRAGMA wal_checkpoint({mode_u})").fetchone()
            return {"busy": row[0], "log": row[1], "checkpointed": row[2]}
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# MCP tool
# ---------------------------------------------------------------------------

class SQLiteQueryTool(ChiaTool):
    """MCP tool exposing SQL over a SQLite DB file (generalized from
    gem5align's ``AlignmentDbQueryTool``).

    ``query``/``schema`` open ``sqlite3.connect(..., uri=True, mode=ro)`` —
    the hard safety boundary against LLM-authored SQL: any INSERT / UPDATE /
    DELETE / DDL raises ``OperationalError``, which is caught and returned as
    text.  Row and byte caps keep the tool result bounded even when the LLM
    writes an unfiltered ``SELECT *``.  With ``read_write=True`` an
    ``execute`` tool is additionally registered for single-statement writes.

    WAL caveat: a ``mode=ro`` open can fail if the ``-shm`` sidecar needs
    recovery; the error string is returned to the LLM.  A co-resident writer
    (the owning :class:`SQLiteNode`) normally keeps the sidecars live.

    This class is deliberately self-contained (stdlib ``sqlite3`` only, no
    module-level helpers) so it can be copied verbatim into a driver's
    ``__main__`` as a by-value-pickling escape hatch for worker images whose
    chia install predates this module — see the module docstring.
    """

    def __init__(
        self,
        name: str,
        db_path: str,
        task_options: dict | None = None,
        row_limit: int = 100,
        cell_char_limit: int = 4096,
        total_char_limit: int = 32768,
        read_write: bool = False,
    ):
        super().__init__(name, task_options=task_options)
        self.db_path = db_path
        self.row_limit = row_limit
        self.cell_char_limit = cell_char_limit
        self.total_char_limit = total_char_limit
        self.read_write = read_write
        self.mcp.add_tool(self.query, name=f"{name}_query")
        self.mcp.add_tool(self.schema, name=f"{name}_schema")
        if read_write:
            self.mcp.add_tool(self.execute, name=f"{name}_execute")
        super().__post_init__()

    def query(self, sql: str) -> str:
        """Execute ``sql`` read-only against the database.

        Returns a pipe-delimited markdown-ish table.  Rows capped at
        ``self.row_limit``; cells truncated to ``self.cell_char_limit`` chars
        with ``…`` marker; total result capped at ``self.total_char_limit``
        bytes.  A trailing ``NOTE:`` line flags any cap that fired so the
        caller can narrow the query.
        """
        import sqlite3
        try:
            conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        except sqlite3.Error as e:
            return f"SQL open error: {type(e).__name__}: {e}"

        try:
            cur = conn.execute(sql)
            rows = cur.fetchmany(self.row_limit + 1)
            cols = [d[0] for d in cur.description] if cur.description else []
        except sqlite3.Error as e:
            return f"SQL error: {type(e).__name__}: {e}"
        finally:
            try:
                conn.close()
            except sqlite3.Error:
                pass

        clipped_rows = len(rows) > self.row_limit
        rows = rows[: self.row_limit]

        if not cols:
            return "(query returned no columns)"

        truncated_cells = 0
        body_lines: list[str] = []
        for r in rows:
            cells: list[str] = []
            for v in r:
                s = "NULL" if v is None else str(v)
                if len(s) > self.cell_char_limit:
                    s = s[: self.cell_char_limit].rstrip() + "…"
                    truncated_cells += 1
                s = s.replace("|", "\\|").replace("\n", " ").replace("\r", " ")
                cells.append(s)
            body_lines.append(" | ".join(cells))

        header = " | ".join(cols)
        sep = " | ".join(["---"] * len(cols))
        text = "\n".join([header, sep] + body_lines) if body_lines else (
            header + "\n" + sep + "\n(no rows)"
        )

        byte_truncated = False
        if len(text) > self.total_char_limit:
            text = text[: self.total_char_limit].rstrip() + "…"
            byte_truncated = True

        notes: list[str] = []
        if clipped_rows:
            notes.append(
                f"row limit of {self.row_limit} hit; add LIMIT / WHERE to narrow"
            )
        if truncated_cells:
            notes.append(
                f"{truncated_cells} cell(s) truncated to "
                f"{self.cell_char_limit} chars"
            )
        if byte_truncated:
            notes.append(
                f"result truncated to {self.total_char_limit}-byte total cap"
            )
        if notes:
            text += "\n\nNOTE: " + "; ".join(notes)
        return text

    def schema(self) -> str:
        """Return CREATE TABLE / CREATE INDEX statements from the live DB.

        Pulls from ``sqlite_master`` so newly-migrated columns appear
        automatically without updating the prompt.
        """
        import sqlite3
        try:
            conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        except sqlite3.Error as e:
            return f"SQL open error: {type(e).__name__}: {e}"
        try:
            rows = conn.execute(
                "SELECT sql FROM sqlite_master "
                "WHERE type IN ('table','index') "
                "AND sql IS NOT NULL "
                "AND name NOT LIKE 'sqlite_%' "
                "ORDER BY type, name"
            ).fetchall()
        except sqlite3.Error as e:
            return f"SQL error: {type(e).__name__}: {e}"
        finally:
            try:
                conn.close()
            except sqlite3.Error:
                pass
        return ";\n\n".join(r[0] for r in rows) + (";" if rows else "")

    def execute(self, sql: str) -> str:
        """Execute ONE write statement in its own committed transaction
        (registered only when ``read_write=True``).  Returns
        ``OK: rowcount=N lastrowid=M`` or an error string.
        """
        import sqlite3
        try:
            conn = sqlite3.connect(self.db_path, timeout=30.0,
                                   isolation_level=None)
        except sqlite3.Error as e:
            return f"SQL open error: {type(e).__name__}: {e}"
        try:
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("BEGIN IMMEDIATE")
            try:
                cur = conn.execute(sql)
                conn.execute("COMMIT")
            except sqlite3.Error:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            return f"OK: rowcount={cur.rowcount} lastrowid={cur.lastrowid}"
        except sqlite3.Error as e:
            return f"SQL error: {type(e).__name__}: {e}"
        finally:
            try:
                conn.close()
            except sqlite3.Error:
                pass
