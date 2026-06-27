"""chia.database.postgres_node — generic PostgreSQL client node for chia flows.

SQL placeholders are psycopg's
``paramstyle = "format"`` (``%s`` positional, ``%(name)s`` named) — SQL
written for SQLiteNode (``?`` / ``:name``) is NOT portable verbatim.

Placement: Colocation is NOT required for correctness — any
worker can reach the server over TCP — so ``require_colocated`` defaults to
``False`` (members dispatch unpinned).  The placement modes are still
available for latency or topology reasons (``placement_group=``,
``node_id=``, ``pin_to_current_node=True``).

Server: this node does not manage one.  The easy path::

    docker run -d --name chia-pg -p 5432:5432 \\
        -e POSTGRES_PASSWORD=chia postgres:16
    db = PostgresNode("postgresql://postgres:chia@<host>:5432/postgres")

A managed ``PostgresServerNode(ColocatedNode)`` (initdb / pg_ctl with the
data dir on one worker's local disk — where colocation genuinely matters
for postgres) is deliberately deferred: it is blocked on distributing
postgres binaries to worker images, and running the official docker image
on the target host is usually the better answer.

Driver: ``psycopg`` (v3) via the optional extra ``pip install
'chia[postgres]'``.  All psycopg imports are lazy (inside function bodies),
so importing this module — and constructing a node from a string DSN —
works without the driver; only executing members (locally or on a worker)
needs it.

Concurrency model: each ``chia_remote`` call opens a fresh connection. Defaults:
``autocommit=True`` with explicit ``conn.transaction()`` blocks for writes, 
``lock_timeout = 30s``, 30 s connect timeout.  Override per
node or per call via ``connect_opts`` (keys: ``connect_timeout_s``,
``lock_timeout_ms``, ``statement_timeout_ms``, ``application_name``).
Read members run with ``default_transaction_read_only = on`` — a guard
against accidental writes, not an adversarial boundary.

Write semantics: write members are declared ``max_retries=0`` (Ray task
replay would double-apply a committed-but-unreturned non-idempotent write).
``lastrowid`` has no meaning on postgres — use ``INSERT ... RETURNING id``
and read :attr:`ExecResult.rows`::

    res = get(db.execute.chia_remote(
        "INSERT INTO t (v) VALUES (%s) RETURNING id", ("x",)))
    new_id = res.rows[0]["id"]

LLM tool read-only layering: :meth:`PostgresNode.spawn_query_tool` connects
with ``default_transaction_read_only=on`` by default - fine for cooperative
LLMs.  The real boundary is credentials: pass a ``GRANT SELECT``-only
role's DSN via ``spawn_query_tool(..., dsn=readonly_dsn)``.
"""

from __future__ import annotations

from typing import Any

from chia.base.ChiaFunction import ChiaFunction
from chia.base.tools.ChiaTool import ChiaTool
from chia.database.base import (
    DatabaseNode,
    ExecResult,
    _IDENT_RE,
    _TYPE_DECL_RE,
)


# ---------------------------------------------------------------------------
# Driver / connection helpers
# ---------------------------------------------------------------------------

_CONNECT_DEFAULTS = {
    "connect_timeout_s": 30.0,
    "application_name": "chia.database.postgres_node",
    "lock_timeout_ms": 30_000,        # analog of sqlite busy_timeout
    "statement_timeout_ms": None,     # optional runaway-query guard
}


def _import_psycopg():
    try:
        import psycopg
    except ImportError as e:
        raise ImportError(
            "PostgresNode requires the optional psycopg driver; install it "
            "with: pip install 'chia[postgres]'  (or: pip install "
            "'psycopg[binary]>=3.2')"
        ) from e
    return psycopg


def _normalize_dsn(dsn: str | dict) -> str:
    """Return a libpq conninfo string.

    A string passes through untouched (no psycopg needed — a driver process
    can construct the node and dispatch members to workers without the
    extra installed).  A dict of conninfo parts (host, port, dbname, user,
    password, ...) is rendered via ``psycopg.conninfo.make_conninfo``,
    which requires psycopg locally.
    """
    if isinstance(dsn, str):
        if not dsn.strip():
            raise ValueError("dsn must be a non-empty conninfo string")
        return dsn
    if isinstance(dsn, dict):
        psycopg = _import_psycopg()
        return psycopg.conninfo.make_conninfo(**dsn)
    raise TypeError(f"dsn must be a conninfo string or dict; got "
                    f"{type(dsn).__name__}")


def _connect(dsn: str, *, read_only: bool = False,
             connect_opts: dict | None = None):
    """Open a fresh psycopg connection with the node's session defaults.

    ``autocommit=True`` mirrors the sqlite node's ``isolation_level=None``:
    no implicit transactions; write members open explicit
    ``conn.transaction()`` blocks.  ``read_only`` sets
    ``default_transaction_read_only = on`` for the session — an
    accidental-write guard, not an adversarial boundary.
    """
    psycopg = _import_psycopg()
    from psycopg.rows import dict_row

    o = {**_CONNECT_DEFAULTS, **(connect_opts or {})}
    conn = psycopg.connect(
        dsn,
        autocommit=True,
        row_factory=dict_row,
        connect_timeout=int(float(o["connect_timeout_s"])),
        application_name=str(o["application_name"]),
    )
    if o["lock_timeout_ms"]:
        conn.execute(f"SET lock_timeout = {int(o['lock_timeout_ms'])}")
    if o["statement_timeout_ms"]:
        conn.execute(f"SET statement_timeout = {int(o['statement_timeout_ms'])}")
    if read_only:
        conn.execute("SET default_transaction_read_only = on")
    return conn


def _split_qualified_table(table: str) -> tuple[str, str]:
    """Validate a (possibly schema-qualified) table name and split it into
    ``(schema, table)``; the schema defaults to ``public``."""
    parts = table.split(".")
    if len(parts) > 2 or not all(_IDENT_RE.fullmatch(p) for p in parts):
        raise ValueError(f"invalid SQL table identifier: {table!r}")
    return (parts[0], parts[1]) if len(parts) == 2 else ("public", parts[0])


# ---------------------------------------------------------------------------
# The node
# ---------------------------------------------------------------------------

class PostgresNode(DatabaseNode):
    """Generic PostgreSQL client node over one server/database (by DSN).

    All members are ``@staticmethod @ChiaFunction`` taking ``dsn`` as the
    explicit first argument; ``__init__`` re-binds them so instance calls
    inject ``self.dsn`` (and the node's ``connect_opts``) automatically::

        db = PostgresNode("postgresql://postgres:chia@dbhost:5432/postgres")
        get(db.init_schema.chia_remote("CREATE TABLE IF NOT EXISTS t ..."))
        get(db.execute.chia_remote("INSERT INTO t (v) VALUES (%s)", ("x",)))
        rows = get(db.query.chia_remote("SELECT * FROM t"))

    See the module docstring for the server story, paramstyle, and the
    read-only layering of the LLM tool.
    """

    _MEMBER_FNS = DatabaseNode._MEMBER_FNS
    _DEFAULT_BUNDLE = {"CPU": 1}
    paramstyle = "format"  # %s positional, %(name)s named

    def __init__(
        self,
        dsn: str | dict,
        placement_group=None,
        require_colocated: bool = False,  # clients can run from any worker
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
        """Normalize ``dsn`` and defer to :class:`DatabaseNode`.

        Args:
            dsn: libpq conninfo string (``postgresql://user:pw@host/db`` or
                ``host=... dbname=...``) or a dict of conninfo parts (dict
                form requires psycopg locally).
            require_colocated: defaults to ``False`` — postgres clients
                reach the server from any worker, so members dispatch
                unpinned unless a placement mode is requested.
            connect_opts: connection defaults injected into every member
                call (keys: ``connect_timeout_s``, ``lock_timeout_ms``,
                ``statement_timeout_ms``, ``application_name``); per-call
                ``connect_opts=`` overrides.
            (remaining args as in :class:`DatabaseNode`.)
        """
        super().__init__(
            _normalize_dsn(dsn),
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
        self.dsn = self.locator

    # -- tool spawning --------------------------------------------------------

    def spawn_query_tool(self, name: str, *, read_write: bool = False,
                         dsn: str | None = None,
                         **tool_kwargs) -> "PostgresQueryTool":
        """Create a :class:`PostgresQueryTool` for this node's database.

        No placement is required — any worker can
        reach the server — so this works on an unpinned node (the tool
        actor is then scheduled wherever Ray likes).  Pass ``dsn=`` to give
        the tool different credentials (e.g. a ``GRANT SELECT``-only role —
        the real read-only boundary).  The returned tool is tracked and
        stopped by :meth:`close`.  ``tool_kwargs`` forward to
        ``PostgresQueryTool`` (e.g. ``row_limit``, ``cell_char_limit``,
        ``total_char_limit``, ``schemas``).
        """
        tool = PostgresQueryTool(
            name,
            dsn=dsn or self.dsn,
            task_options=self.task_options or None,
            read_write=read_write,
            **tool_kwargs,
        )
        self._tools.append(tool)
        return tool

    # -- DDL / schema ---------------------------------------------------------

    @staticmethod
    @ChiaFunction(num_cpus=0.1, max_retries=0)
    def init_schema(dsn: str, script: str, *,
                    connect_opts: dict | None = None) -> None:
        """Run a multi-statement DDL ``script`` in one transaction.
        Idempotent ``CREATE ... IF NOT EXISTS`` scripts are encouraged.
        (psycopg runs multi-statement strings only when no parameters are
        passed — which is the case here.)"""
        conn = _connect(dsn, connect_opts=connect_opts)
        try:
            with conn.transaction():
                conn.execute(script)
        finally:
            conn.close()

    @staticmethod
    @ChiaFunction(num_cpus=0.1, max_retries=0)
    def executescript(dsn: str, script: str, *,
                      connect_opts: dict | None = None) -> None:
        """Alias-level sibling of :meth:`init_schema` (postgres has no
        file/dirs to create, so the two are identical here; both exist to
        satisfy the :class:`DatabaseNode` contract)."""
        conn = _connect(dsn, connect_opts=connect_opts)
        try:
            with conn.transaction():
                conn.execute(script)
        finally:
            conn.close()

    @staticmethod
    @ChiaFunction(num_cpus=0.1, max_retries=0)
    def add_column_if_missing(dsn: str, table: str, column: str,
                              type_decl: str, *,
                              connect_opts: dict | None = None) -> bool:
        """``ALTER TABLE ... ADD COLUMN IF NOT EXISTS`` ; True iff added.

        ``table`` may be schema-qualified (``myschema.t``; default schema
        ``public``).  Identifiers and ``type_decl`` are regex-validated
        before any SQL interpolation (and before the psycopg import, so
        validation errors don't depend on the driver)."""
        sch, tbl = _split_qualified_table(table)
        if not _IDENT_RE.fullmatch(column):
            raise ValueError(f"invalid SQL identifier: {column!r}")
        if not _TYPE_DECL_RE.fullmatch(type_decl):
            raise ValueError(f"invalid column type declaration: {type_decl!r}")
        conn = _connect(dsn, connect_opts=connect_opts)
        try:
            with conn.transaction():
                row = conn.execute(
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s "
                    "AND column_name = %s",
                    (sch, tbl, column),
                ).fetchone()
                if row is not None:
                    return False
                conn.execute(
                    f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
                    f"{column} {type_decl}")
                return True
        finally:
            conn.close()

    @staticmethod
    @ChiaFunction(num_cpus=0.1)
    def schema(dsn: str, *, schemas: tuple = ("public",),
               connect_opts: dict | None = None) -> str:
        """Human-readable table/index listing from ``information_schema``
        and ``pg_indexes`` for the given schemas.  A simple rendering, not
        pg_dump fidelity."""
        conn = _connect(dsn, read_only=True, connect_opts=connect_opts)
        try:
            cols = conn.execute(
                "SELECT table_schema, table_name, column_name, data_type, "
                "       is_nullable, column_default "
                "FROM information_schema.columns "
                "WHERE table_schema = ANY(%s) "
                "ORDER BY table_schema, table_name, ordinal_position",
                (list(schemas),),
            ).fetchall()
            idx = conn.execute(
                "SELECT indexdef FROM pg_indexes "
                "WHERE schemaname = ANY(%s) "
                "ORDER BY schemaname, tablename, indexname",
                (list(schemas),),
            ).fetchall()
        finally:
            conn.close()

        out: list[str] = []
        cur_table = None
        lines: list[str] = []
        for c in cols:
            t = f"{c['table_schema']}.{c['table_name']}"
            if t != cur_table:
                if cur_table is not None:
                    out.append(f"TABLE {cur_table} (\n    "
                               + ",\n    ".join(lines) + "\n);")
                cur_table, lines = t, []
            decl = f"{c['column_name']} {c['data_type']}"
            if c["is_nullable"] == "NO":
                decl += " NOT NULL"
            if c["column_default"] is not None:
                decl += f" DEFAULT {c['column_default']}"
            lines.append(decl)
        if cur_table is not None:
            out.append(f"TABLE {cur_table} (\n    "
                       + ",\n    ".join(lines) + "\n);")
        out.extend(f"{r['indexdef']};" for r in idx)
        return "\n\n".join(out)

    # -- writes ---------------------------------------------------------------

    @staticmethod
    @ChiaFunction(num_cpus=0.1, max_retries=0)
    def execute(dsn: str, sql: str, params: tuple | dict = (), *,
                connect_opts: dict | None = None) -> ExecResult:
        """Run one statement in its own committed transaction.  When the
        statement returns rows (``RETURNING``, or a SELECT), they are
        attached as :attr:`ExecResult.rows`"""
        conn = _connect(dsn, connect_opts=connect_opts)
        try:
            with conn.transaction():
                cur = conn.execute(sql, params or None)
                rows = cur.fetchall() if cur.description else None
            return ExecResult(rowcount=cur.rowcount, lastrowid=None,
                              rows=rows)
        finally:
            conn.close()

    @staticmethod
    @ChiaFunction(num_cpus=0.1, max_retries=0)
    def executemany(dsn: str, sql: str, seq_of_params: list, *,
                    connect_opts: dict | None = None) -> ExecResult:
        """``executemany`` in one committed transaction; ``rowcount`` is the
        total across all parameter sets."""
        conn = _connect(dsn, connect_opts=connect_opts)
        try:
            with conn.transaction():
                cur = conn.cursor()
                cur.executemany(sql, seq_of_params)
            return ExecResult(rowcount=cur.rowcount, lastrowid=None,
                              rows=None)
        finally:
            conn.close()

    @staticmethod
    @ChiaFunction(num_cpus=0.1, max_retries=0)
    def transaction(dsn: str, ops: list, *,
                    connect_opts: dict | None = None) -> list[ExecResult]:
        """Run ``ops`` atomically in one transaction.

        Each op is ``(sql, params)`` where the params TYPE selects the form:

        * ``tuple`` or ``dict`` -> ``execute(sql, params)``  (single statement)
        * ``list``              -> ``executemany(sql, params)``  (bulk rows)
        * ``None``              -> ``execute(sql)``  (no params)

        NOTE: single-statement params MUST be a tuple/dict — a list means
        executemany.  Any error rolls the whole batch back and re-raises
        (surfacing as a Ray task error at ``get()``).  Explicit
        BEGIN/COMMIT inside op SQL is unsupported.  Returns one
        :class:`ExecResult` per op (rows attached for ops that return
        them)."""
        conn = _connect(dsn, connect_opts=connect_opts)
        try:
            results: list[ExecResult] = []
            with conn.transaction():
                for i, op in enumerate(ops):
                    try:
                        sql, params = op
                    except (TypeError, ValueError):
                        raise TypeError(
                            f"ops[{i}] must be a (sql, params) pair; got {op!r}"
                        ) from None
                    if params is None:
                        cur = conn.execute(sql)
                        rows = cur.fetchall() if cur.description else None
                        results.append(ExecResult(cur.rowcount, None, rows))
                    elif isinstance(params, list):
                        cur = conn.cursor()
                        cur.executemany(sql, params)
                        results.append(ExecResult(cur.rowcount, None, None))
                    elif isinstance(params, (tuple, dict)):
                        cur = conn.execute(sql, params)
                        rows = cur.fetchall() if cur.description else None
                        results.append(ExecResult(cur.rowcount, None, rows))
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
    def query(dsn: str, sql: str, params: tuple | dict = (), *,
              limit: int | None = None,
              connect_opts: dict | None = None) -> list[dict]:
        """Rows as ``list[dict]``.  ``limit=N`` caps the fetch (protects the
        object store from unbounded SELECTs); default None fetches all.  The
        session runs with ``default_transaction_read_only = on``, so an
        accidental write raises ``ReadOnlySqlTransaction``."""
        conn = _connect(dsn, read_only=True, connect_opts=connect_opts)
        try:
            cur = conn.execute(sql, params or None)
            rows = cur.fetchmany(limit) if limit is not None else cur.fetchall()
            return list(rows)
        finally:
            conn.close()

    @staticmethod
    @ChiaFunction(num_cpus=0.1)
    def query_one(dsn: str, sql: str, params: tuple | dict = (), *,
                  connect_opts: dict | None = None) -> dict | None:
        """First row as a dict, or None."""
        conn = _connect(dsn, read_only=True, connect_opts=connect_opts)
        try:
            return conn.execute(sql, params or None).fetchone()
        finally:
            conn.close()

    @staticmethod
    @ChiaFunction(num_cpus=0.1)
    def query_value(dsn: str, sql: str, params: tuple | dict = (), *,
                    default: Any = None,
                    connect_opts: dict | None = None) -> Any:
        """First column of the first row, or ``default`` when there is no
        row.  (An aggregate over an empty table yields a NULL row, returned
        as None, not ``default``.)"""
        conn = _connect(dsn, read_only=True, connect_opts=connect_opts)
        try:
            row = conn.execute(sql, params or None).fetchone()
            return next(iter(row.values())) if row is not None else default
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# MCP tool
# ---------------------------------------------------------------------------

class PostgresQueryTool(ChiaTool):
    """MCP tool exposing SQL over a PostgreSQL database

    ``query``/``schema`` connect with ``default_transaction_read_only=on``
    in the session options — a weak guard against LLM-authored writes
    (SQL containing ``SET default_transaction_read_only = off`` flips it).  
    For an adversarial boundary, construct the tool with a ``GRANT SELECT``-only role's DSN.
    Errors are caught and returned as text; row and byte caps keep the tool
    result bounded even for an unfiltered ``SELECT *``.  With
    ``read_write=True`` an ``execute`` tool is additionally registered for
    single-statement writes.

    This class is deliberately self-contained (lazy ``psycopg`` imports, no
    module-level helpers) so it can be copied verbatim into a driver's
    ``__main__`` as a by-value-pickling escape hatch for worker images whose
    chia install predates this module.
    """

    def __init__(
        self,
        name: str,
        dsn: str,
        task_options: dict | None = None,
        row_limit: int = 100,
        cell_char_limit: int = 4096,
        total_char_limit: int = 32768,
        read_write: bool = False,
        schemas: tuple = ("public",),
    ):
        super().__init__(name, task_options=task_options)
        self.dsn = dsn
        self.row_limit = row_limit
        self.cell_char_limit = cell_char_limit
        self.total_char_limit = total_char_limit
        self.read_write = read_write
        self.schemas = tuple(schemas)
        self.mcp.add_tool(self.query, name=f"{name}_query")
        self.mcp.add_tool(self.schema, name=f"{name}_schema")
        if read_write:
            self.mcp.add_tool(self.execute, name=f"{name}_execute")
        super().__post_init__()

    # -- rendering (server-free; unit-testable on an unstarted instance) ------

    def _format_table(self, cols: list, rows: list) -> str:
        """Render rows (tuples) as a pipe-delimited markdown-ish table.

        ``rows`` may hold up to ``row_limit + 1`` entries — the extra row
        signals clipping.  Cells truncate to ``cell_char_limit`` chars with
        an ``…`` marker; the total result caps at ``total_char_limit``
        bytes.  A trailing ``NOTE:`` line flags any cap that fired.
        """
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

    # -- MCP methods -----------------------------------------------------------

    def query(self, sql: str) -> str:
        """Execute ``sql`` against the database in a read-only session.

        Returns a pipe-delimited markdown-ish table with row/cell/byte caps
        and a trailing ``NOTE:`` line when a cap fires.
        """
        try:
            import psycopg
        except ImportError as e:
            return f"psycopg not installed on this worker: {e}"
        try:
            conn = psycopg.connect(
                self.dsn, autocommit=True, connect_timeout=30,
                options="-c default_transaction_read_only=on")
        except psycopg.Error as e:
            return f"SQL open error: {type(e).__name__}: {e}"

        try:
            cur = conn.execute(sql)
            rows = cur.fetchmany(self.row_limit + 1)
            cols = [d.name for d in cur.description] if cur.description else []
        except psycopg.Error as e:
            return f"SQL error: {type(e).__name__}: {e}"
        finally:
            try:
                conn.close()
            except psycopg.Error:
                pass

        return self._format_table(cols, rows)

    def schema(self) -> str:
        """Table/index listing from ``information_schema`` / ``pg_indexes``
        for the tool's configured schemas — live, so migrated columns appear
        automatically."""
        try:
            import psycopg
        except ImportError as e:
            return f"psycopg not installed on this worker: {e}"
        try:
            conn = psycopg.connect(
                self.dsn, autocommit=True, connect_timeout=30,
                options="-c default_transaction_read_only=on")
        except psycopg.Error as e:
            return f"SQL open error: {type(e).__name__}: {e}"
        try:
            cols = conn.execute(
                "SELECT table_schema, table_name, column_name, data_type, "
                "       is_nullable "
                "FROM information_schema.columns "
                "WHERE table_schema = ANY(%s) "
                "ORDER BY table_schema, table_name, ordinal_position",
                (list(self.schemas),),
            ).fetchall()
            idx = conn.execute(
                "SELECT indexdef FROM pg_indexes "
                "WHERE schemaname = ANY(%s) "
                "ORDER BY schemaname, tablename, indexname",
                (list(self.schemas),),
            ).fetchall()
        except psycopg.Error as e:
            return f"SQL error: {type(e).__name__}: {e}"
        finally:
            try:
                conn.close()
            except psycopg.Error:
                pass

        out: list[str] = []
        cur_table = None
        lines: list[str] = []
        for sch, tbl, col, typ, nullable in cols:
            t = f"{sch}.{tbl}"
            if t != cur_table:
                if cur_table is not None:
                    out.append(f"TABLE {cur_table} (\n    "
                               + ",\n    ".join(lines) + "\n);")
                cur_table, lines = t, []
            lines.append(f"{col} {typ}"
                         + (" NOT NULL" if nullable == "NO" else ""))
        if cur_table is not None:
            out.append(f"TABLE {cur_table} (\n    "
                       + ",\n    ".join(lines) + "\n);")
        out.extend(f"{r[0]};" for r in idx)
        return "\n\n".join(out)

    def execute(self, sql: str) -> str:
        """Execute ONE write statement in its own committed transaction
        (registered only when ``read_write=True``).  Returns
        ``OK: rowcount=N`` (plus returned rows for ``RETURNING``) or an
        error string.
        """
        try:
            import psycopg
        except ImportError as e:
            return f"psycopg not installed on this worker: {e}"
        try:
            conn = psycopg.connect(self.dsn, autocommit=True,
                                   connect_timeout=30)
        except psycopg.Error as e:
            return f"SQL open error: {type(e).__name__}: {e}"
        try:
            with conn.transaction():
                cur = conn.execute(sql)
                rows = cur.fetchall() if cur.description else []
                cols = ([d.name for d in cur.description]
                        if cur.description else [])
            out = f"OK: rowcount={cur.rowcount}"
            if rows:
                out += "\n" + self._format_table(cols, rows)
            return out
        except psycopg.Error as e:
            return f"SQL error: {type(e).__name__}: {e}"
        finally:
            try:
                conn.close()
            except psycopg.Error:
                pass
