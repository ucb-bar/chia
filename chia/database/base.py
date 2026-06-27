"""chia.database.base — shared machinery and contract for database nodes.

:class:`DatabaseNode` is the abstract base for engine-specific database
nodes (:class:`~chia.database.sqlite_node.SQLiteNode`,
:class:`~chia.database.postgres_node.PostgresNode`).  It owns everything
that is engine-agnostic:

  * the three placement modes (placement group via
    :class:`~chia.base.colocated.ColocatedNode`, or a hard NodeAffinity pin
    via ``node_id=`` / ``pin_to_current_node=True``),
  * the :class:`_DbBoundChiaFn` re-binding loop that injects the node's
    ``locator`` (a file path, a DSN, ...) and default ``connect_opts`` into
    every member call,
  * spawned-tool tracking and the tools-then-placement-group ``close()``.

It also declares each common member as an abstract staticmethod, so the
family is polymorphic: ``DatabaseNode`` cannot be instantiated, an engine
subclass missing a member fails at construction (not at first dispatch),
and engine-blind code can be written against the base::

    def record(db: DatabaseNode, sql: str, params: tuple) -> None:
        get(db.execute.chia_remote(sql, params))

The subclasses override every stub with a real ``@ChiaFunction``; 
the stubs carry the canonical signatures and semantics.

Portability caveat: the members share names, signatures, result types
(:class:`ExecResult`, ``list[dict]``), and :meth:`DatabaseNode.transaction`'s
params-type dispatch — but SQL *text* is dialect-specific.  At minimum the
parameter placeholders differ (see each subclass's ``paramstyle``: sqlite is
``qmark`` — ``?`` / ``:name`` — and postgres is ``format`` — ``%s`` /
``%(name)s``), and DDL idioms often do too.  Engine-blind code must either
render SQL per ``db.paramstyle`` or stick to statements valid in both
dialects.

Contract notes the subclasses follow (asserted by their tests, not by the
base): write members are declared ``max_retries=0`` (Ray task replay would
double-apply a committed-but-unreturned non-idempotent write), and read
members open their connection with an engine-appropriate read-only guard.
"""

from __future__ import annotations

import abc
import re
from dataclasses import dataclass
from typing import Any

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from chia.base.ChiaFunction import get
from chia.base.colocated import ColocatedNode, PinnedChiaFn

# Identifiers and column type declarations are interpolated into DDL
# (identifiers cannot be SQL parameters), so constrain them to benign
# character sets.  Shared by all engine subclasses.
_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_TYPE_DECL_RE = re.compile(r"[A-Za-z0-9_() ,.'\"+-]+")


@dataclass
class ExecResult:
    """Outcome of one write statement, uniform across engines.

    ``rowcount`` is the DBAPI ``cursor.rowcount`` (the total for
    executemany).  ``lastrowid`` is sqlite-only (None for executemany and
    non-INSERT statements; always None on postgres — use ``RETURNING`` and
    read ``rows`` instead).  ``rows`` carries fetched result rows when the
    statement returned any (e.g. postgres ``INSERT ... RETURNING id``).
    """
    rowcount: int
    lastrowid: int | None = None
    rows: list[dict] | None = None


class _DbBoundChiaFn(PinnedChiaFn):
    """A :class:`PinnedChiaFn` that prepends fixed leading args and default
    kwargs (the node's locator + ``connect_opts``) on every dispatch,
    local call, and ``.options(...)`` handle — so instance calls read
    ``node.execute.chia_remote(sql, params)`` while the raw class attribute
    ``<NodeCls>.execute.chia_remote(locator, sql, ...)`` stays usable
    unpinned.  Per-call kwargs override the bound defaults.
    """

    def __init__(self, fn, scheduling_opts: dict, bound_args=(),
                 bound_kwargs: dict | None = None):
        super().__init__(fn, scheduling_opts)
        self._bound_args = tuple(bound_args)
        self._bound_kwargs = dict(bound_kwargs or {})
        inner_remote = self.chia_remote  # pinned (or raw) from the base class
        bound_a, bound_kw = self._bound_args, self._bound_kwargs

        def chia_remote(*args, **kwargs):
            return inner_remote(*bound_a, *args, **{**bound_kw, **kwargs})

        self.chia_remote = chia_remote

    def options(self, **overrides):
        handle = super().options(**overrides)
        bound_a, bound_kw = self._bound_args, self._bound_kwargs

        class _BoundHandle:
            def chia_remote(self, *args, **kwargs):
                return handle.chia_remote(*bound_a, *args,
                                          **{**bound_kw, **kwargs})

            def remote(self, *args, **kwargs):
                return self.chia_remote(*args, **kwargs)

            def chia_remote_blocking(self, *args, **kwargs):
                return get(self.chia_remote(*args, **kwargs))

        return _BoundHandle()

    def __call__(self, *args, **kwargs):
        return self._fn(*self._bound_args, *args,
                        **{**self._bound_kwargs, **kwargs})


class DatabaseNode(ColocatedNode, abc.ABC):
    """Abstract base for engine-specific database nodes.

    Subclasses must define:

      * every abstract member below as a ``@staticmethod @ChiaFunction``
        taking the engine's locator as the explicit first argument,
      * ``paramstyle`` — the PEP 249 placeholder style of the engine's SQL,
      * ``spawn_query_tool`` — the engine's LLM-facing MCP tool factory,

    and may extend ``_MEMBER_FNS`` with engine-specific members (e.g.
    ``SQLiteNode`` appends ``wal_checkpoint``); the binding loop picks the
    extras up automatically.
    """

    _MEMBER_FNS = (
        "init_schema", "execute", "executemany", "executescript",
        "transaction", "query", "query_one", "query_value",
        "schema", "add_column_if_missing",
    )
    _DEFAULT_BUNDLE = {"CPU": 1}

    #: PEP 249 paramstyle of the SQL this node's members expect
    #: (e.g. "qmark" for sqlite, "format" for postgres).
    paramstyle: str = ""

    def __init__(
        self,
        locator: str,
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
        """Set up placement and bind the member functions.

        Args:
            locator: the engine-specific database address (an absolute file
                path for sqlite, a DSN/conninfo string for postgres),
                already validated/normalized by the subclass.
            placement_group / require_colocated / bundle_index /
                reserve_bundle / pg_strategy / wait_for_pg /
                pg_ready_timeout_s: as in :class:`ColocatedNode`.
            node_id: pin every member to this Ray node via a hard
                ``NodeAffinitySchedulingStrategy`` (``soft=False``) instead
                of a placement group — for state that already lives on a
                known machine.  Mutually exclusive with ``placement_group``.
            pin_to_current_node: convenience for ``node_id=<caller's node>``.
                Requires an initialized Ray context.
            connect_opts: engine-specific connection defaults injected into
                every member call; per-call ``connect_opts=`` overrides.
        """
        if not type(self).paramstyle:
            raise TypeError(
                f"{type(self).__name__} must define a non-empty paramstyle"
            )
        if (node_id is not None or pin_to_current_node) and placement_group is not None:
            raise ValueError(
                "node_id / pin_to_current_node and placement_group are "
                "mutually exclusive"
            )
        if node_id is not None and pin_to_current_node:
            raise ValueError("pass either node_id or pin_to_current_node, not both")

        if node_id is not None or pin_to_current_node:
            # NodeAffinity mode: no PG; super() binds with empty sched opts,
            # then we overwrite them and re-bind below.
            super().__init__(require_colocated=False)
            nid = node_id or ray.get_runtime_context().get_node_id()
            self._sched_opts = {
                "scheduling_strategy": NodeAffinitySchedulingStrategy(
                    node_id=nid, soft=False,
                )
            }
        else:
            super().__init__(
                placement_group,
                require_colocated,
                bundle_index=bundle_index,
                reserve_bundle=reserve_bundle,
                pg_strategy=pg_strategy,
                wait_for_pg=wait_for_pg,
                pg_ready_timeout_s=pg_ready_timeout_s,
            )

        self.locator = str(locator)
        self._connect_opts = dict(connect_opts or {})
        self._tools: list = []

        # Re-bind every member with locator (+ default connect_opts) injected.
        bound_kwargs = (
            {"connect_opts": self._connect_opts} if self._connect_opts else {}
        )
        for name in self._MEMBER_FNS:
            setattr(self, name, _DbBoundChiaFn(
                getattr(type(self), name), self._sched_opts,
                (self.locator,), bound_kwargs,
            ))

    # -- lifecycle ------------------------------------------------------------

    def close(self) -> None:
        """Stop spawned tools, then release the PG iff this node reserved it.

        Tools are stopped BEFORE the PG is removed — a tool's server actor
        may be pinned to this PG, so tearing the PG down first would orphan
        it.  Idempotent.
        """
        for tool in self._tools:
            try:
                tool.stop()
            except Exception as e:
                name = getattr(tool, "name", "?")
                print(f"[{type(self).__name__}.close] warning: "
                      f"failed to stop tool {name}: {e}")
        self._tools.clear()
        super().close()

    def __enter__(self):
        return self

    # -- abstract member contract ----------------------------------------------
    #
    # Each stub below is overridden by a @staticmethod @ChiaFunction in the
    # engine subclass.  Signatures here are the canonical contract; ``locator``
    # is the subclass's first positional arg (db_path / dsn) — injected
    # automatically on instance calls by the binding loop above.

    @staticmethod
    @abc.abstractmethod
    def init_schema(locator: str, script: str, *,
                    connect_opts: dict | None = None) -> None:
        """Create the database (if creatable) and run a multi-statement DDL
        script.  Idempotent ``CREATE ... IF NOT EXISTS`` scripts encouraged."""

    @staticmethod
    @abc.abstractmethod
    def executescript(locator: str, script: str, *,
                      connect_opts: dict | None = None) -> None:
        """Run a multi-statement SQL script (no parameters)."""

    @staticmethod
    @abc.abstractmethod
    def add_column_if_missing(locator: str, table: str, column: str,
                              type_decl: str, *,
                              connect_opts: dict | None = None) -> bool:
        """``ALTER TABLE ... ADD COLUMN`` iff absent; True iff added.
        Identifiers are regex-validated before any SQL interpolation."""

    @staticmethod
    @abc.abstractmethod
    def schema(locator: str, *, connect_opts: dict | None = None) -> str:
        """Human-readable rendering of the current tables/indexes."""

    @staticmethod
    @abc.abstractmethod
    def execute(locator: str, sql: str, params: tuple | dict = (), *,
                connect_opts: dict | None = None) -> ExecResult:
        """Run one statement in its own committed transaction.
        Declared with ``max_retries=0`` (non-idempotent-replay guard)."""

    @staticmethod
    @abc.abstractmethod
    def executemany(locator: str, sql: str, seq_of_params: list, *,
                    connect_opts: dict | None = None) -> ExecResult:
        """DBAPI ``executemany`` in one committed transaction; ``rowcount``
        is the total across all parameter sets."""

    @staticmethod
    @abc.abstractmethod
    def transaction(locator: str, ops: list, *,
                    connect_opts: dict | None = None) -> list[ExecResult]:
        """Run ``ops`` atomically — all or nothing, in one transaction.

        Each op is ``(sql, params)`` where the params TYPE selects the form:
        ``tuple``/``dict`` -> execute, ``list`` -> executemany, ``None`` ->
        bare execute.  Any error rolls the whole batch back and re-raises."""

    @staticmethod
    @abc.abstractmethod
    def query(locator: str, sql: str, params: tuple | dict = (), *,
              limit: int | None = None,
              connect_opts: dict | None = None) -> list[dict]:
        """Rows as ``list[dict]``; ``limit=N`` caps the fetch.  Opened with
        the engine's read-only guard, so accidental writes raise."""

    @staticmethod
    @abc.abstractmethod
    def query_one(locator: str, sql: str, params: tuple | dict = (), *,
                  connect_opts: dict | None = None) -> dict | None:
        """First row as a dict, or None."""

    @staticmethod
    @abc.abstractmethod
    def query_value(locator: str, sql: str, params: tuple | dict = (), *,
                    default: Any = None,
                    connect_opts: dict | None = None) -> Any:
        """First column of the first row, or ``default`` when there is no
        row.  (An aggregate over an empty table yields a NULL row, returned
        as None, not ``default``.)"""

    @abc.abstractmethod
    def spawn_query_tool(self, name: str, *, read_write: bool = False,
                         **tool_kwargs):
        """Create this engine's LLM-facing MCP query tool (read-only SQL;
        opt-in single-statement writes with ``read_write=True``).  The
        returned tool is tracked and stopped by :meth:`close`."""
