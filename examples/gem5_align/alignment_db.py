"""SQLite-backed store for gem5 alignment loop iteration records.

Each entry records the config file, gem5 source diff, LLM summaries, build
info, and per-benchmark comparison results.  Entries are keyed by a UUID
(``entry_id``) and linked via ``parent_id`` so the DB represents the tree of
config states the loop has explored (baseline at the root, each iteration
chaining off the state it built on).  The integer ``iteration`` is a
display-friendly sequence number, not a key.

``AlignmentDB`` is a thin domain layer over the canonical
:class:`chia.database.SQLiteNode`.  The node owns everything generic — the
connection/PRAGMA handling, atomic transactions, the schema DDL, and the
read-only SQL MCP tool (:meth:`spawn_query_tool` -> ``SQLiteQueryTool``, the
canonical replacement for the old hand-rolled ``AlignmentDbQueryTool``).  This
class adds only the gem5-alignment-specific composite reads/writes
(``insert_iteration``, ``top_k_entries``, ``best_per_benchmark``, ``lineage``,
…), built by composing the node's ``query`` / ``query_one`` / ``query_value`` /
``transaction`` members.

Placement: construct with a placement so the spawned query tool can co-locate
with the DB file.  ``alignment.db`` lives on the head's local disk, so the loop
uses ``AlignmentDB(path, pin_to_current_node=True)`` (a hard NodeAffinity pin to
the driver's node — the canonical equivalent of the loop's old align_db pin).
The members are invoked in-process on that node, so the loop uses this object
synchronously just like a plain head-side store.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from chia.database import SQLiteNode


_ITERATIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS iterations (
    entry_id         TEXT PRIMARY KEY,
    parent_id        TEXT,
    iteration        INTEGER NOT NULL,
    timestamp        TEXT NOT NULL,
    avg_pct_diff     REAL,
    changes_summary  TEXT,
    source_changes   TEXT,
    config_contents  TEXT,
    gem5_source_diff TEXT,
    base_rev         TEXT,
    build_success    INTEGER,
    build_duration   REAL,
    llm_log_path     TEXT,
    FOREIGN KEY (parent_id) REFERENCES iterations(entry_id)
);
"""

_BENCHMARK_RESULTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS benchmark_results (
    entry_id             TEXT,
    benchmark            TEXT,
    percentage_diff      REAL,
    gem5_status          TEXT,
    verilator_status     TEXT,
    gem5_num_cycles      INTEGER,
    verilator_num_cycles INTEGER,
    gem5_ver_cycle_ratio REAL,
    error_messages       TEXT,
    stdout_tail          TEXT,
    pipe_trace_summary   TEXT,
    PRIMARY KEY (entry_id, benchmark),
    FOREIGN KEY (entry_id) REFERENCES iterations(entry_id)
);
"""


class AlignmentDB(SQLiteNode):
    """Alignment-iteration store backed by a colocated :class:`SQLiteNode`.

    ``__init__`` forwards placement args to :class:`SQLiteNode` (e.g.
    ``pin_to_current_node=True`` for the head's local ``alignment.db``), then
    creates the schema.  Reads/writes below compose the node's generic
    members; ``close()`` (inherited) stops the spawned query tool.
    """

    def __init__(self, db_path, *args, **kwargs):
        super().__init__(db_path, *args, **kwargs)
        # Create the schema, then add columns introduced after the original
        # schema so an older alignment.db from a prior run stays usable
        # (both are idempotent; a fresh DB already has these columns).
        self.init_schema(_ITERATIONS_SCHEMA + _BENCHMARK_RESULTS_SCHEMA)
        self.add_column_if_missing("benchmark_results", "pipe_trace_summary", "TEXT")
        self.add_column_if_missing("iterations", "base_rev", "TEXT")

    # --- writes ----------------------------------------------------------

    def insert_iteration(
        self,
        entry_id: str,
        parent_id: str | None,
        iteration: int,
        *,
        avg_pct_diff: float | None,
        changes: str,
        source_changes: str,
        config_contents: str,
        gem5_source_diff: str,
        build_success: bool,
        build_duration: float,
        llm_log_path: str,
        results: list[dict],
        base_rev: str | None = None,
    ) -> None:
        """Write one entry + its per-benchmark results atomically.

        ``entry_id`` must be a fresh UUID; ``parent_id`` is the entry this one
        branches from (``None`` for the baseline root).  The INSERT, the stale-
        row DELETE, and the bulk benchmark INSERT run in a single
        ``SQLiteNode.transaction`` (all-or-nothing).
        """
        ts = datetime.now().isoformat(timespec="seconds")
        avg = None if avg_pct_diff is None or avg_pct_diff == float("inf") else avg_pct_diff
        bench_rows = [
            (
                entry_id,
                r.get("benchmark"),
                r.get("percentage_diff"),
                r.get("gem5_status"),
                r.get("verilator_status"),
                r.get("gem5_numCycles"),
                r.get("verilator_numCycles"),
                r.get("gem5/ver_cycle_ratio"),
                r.get("error_messages"),
                r.get("stdout_tail"),
                r.get("pipe_trace_summary"),
            )
            for r in results
        ]
        # Op params TYPE selects the form: tuple -> execute, list -> executemany.
        self.transaction([
            (
                "INSERT OR REPLACE INTO iterations "
                "(entry_id, parent_id, iteration, timestamp, avg_pct_diff, "
                " changes_summary, source_changes, config_contents, "
                " gem5_source_diff, base_rev, build_success, build_duration, "
                " llm_log_path) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (entry_id, parent_id, iteration, ts, avg, changes,
                 source_changes, config_contents, gem5_source_diff, base_rev,
                 int(bool(build_success)), build_duration, llm_log_path),
            ),
            ("DELETE FROM benchmark_results WHERE entry_id = ?", (entry_id,)),
            (
                "INSERT INTO benchmark_results "
                "(entry_id, benchmark, percentage_diff, gem5_status, verilator_status, "
                " gem5_num_cycles, verilator_num_cycles, gem5_ver_cycle_ratio, "
                " error_messages, stdout_tail, pipe_trace_summary) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                bench_rows,
            ),
        ])

    # --- reads -----------------------------------------------------------

    def load_entry(self, entry_id: str) -> dict[str, Any] | None:
        """Return the row for ``entry_id`` with an attached ``results`` list."""
        row = self.query_one(
            "SELECT * FROM iterations WHERE entry_id = ?", (entry_id,))
        if row is None:
            return None
        row["results"] = self.query(
            "SELECT * FROM benchmark_results WHERE entry_id = ? ORDER BY benchmark",
            (entry_id,))
        return row

    def load_by_iteration(self, iter_num: int) -> dict[str, Any] | None:
        """Return the entry stored at ``iteration=iter_num`` (with results),
        or None if no such iteration exists."""
        row = self.query_one(
            "SELECT entry_id FROM iterations WHERE iteration = ?", (iter_num,))
        return self.load_entry(row["entry_id"]) if row else None

    def max_iteration(self) -> int:
        """Return the highest ``iteration`` sequence number in the DB, or -1."""
        m = self.query_value("SELECT MAX(iteration) FROM iterations")
        return m if m is not None else -1

    def best_entry(self) -> dict[str, Any] | None:
        """Return the entry with the lowest ``avg_pct_diff`` (most aligned).

        Prefers rows with non-null ``avg_pct_diff``; falls back to the most
        recent entry if every row is null.  Returns ``None`` when empty.
        """
        row = self.query_one(
            "SELECT entry_id FROM iterations "
            "WHERE avg_pct_diff IS NOT NULL "
            "ORDER BY avg_pct_diff ASC, timestamp ASC LIMIT 1")
        if row is None:
            row = self.query_one(
                "SELECT entry_id FROM iterations ORDER BY timestamp DESC LIMIT 1")
        return self.load_entry(row["entry_id"]) if row else None

    def top_k_entries(self, k: int) -> list[dict[str, Any]]:
        """Up to ``k`` entries ranked by ``avg_pct_diff`` (ascending).

        If fewer than ``k`` entries have a non-null ``avg_pct_diff`` (e.g.,
        immediately after baseline), pad with the newest unscored entries so
        callers always get *some* sample pool while the DB is non-empty.
        """
        ranked = self.query(
            "SELECT entry_id FROM iterations "
            "WHERE avg_pct_diff IS NOT NULL "
            "ORDER BY avg_pct_diff ASC, timestamp ASC LIMIT ?", (k,))
        entries = [self.load_entry(r["entry_id"]) for r in ranked]
        if len(entries) < k:
            extra = self.query(
                "SELECT entry_id FROM iterations "
                "WHERE avg_pct_diff IS NULL "
                "ORDER BY timestamp DESC LIMIT ?", (k - len(entries),))
            entries.extend(self.load_entry(r["entry_id"]) for r in extra)
        return entries

    def best_per_benchmark(self) -> list[dict[str, Any]]:
        """For each benchmark, the entry whose ``|signed_pct|`` is smallest
        across all ok-status results.

        Signed pct matches the in-loop ``per_bench`` dict: positive => gem5
        slower than verilator (``gem5_ver_cycle_ratio >= 1.0``), negative =>
        faster.  Ties by ``|signed_pct|`` break toward the lower ``iteration``.
        Returns ``{benchmark, entry_id, iteration, signed_pct}`` dicts sorted by
        ``|signed_pct|`` ascending; ``[]`` on an empty DB.
        """
        rows = self.query(
            "SELECT br.benchmark, br.entry_id, br.percentage_diff, "
            "       br.gem5_ver_cycle_ratio, i.iteration "
            "FROM benchmark_results br "
            "JOIN iterations i ON i.entry_id = br.entry_id "
            "WHERE br.gem5_status = 'ok' AND br.percentage_diff IS NOT NULL")

        best: dict[str, dict[str, Any]] = {}
        for r in rows:
            pct = r["percentage_diff"]
            ratio = r["gem5_ver_cycle_ratio"]
            signed = pct if (ratio is not None and ratio >= 1.0) else -pct
            cand = {
                "benchmark": r["benchmark"],
                "entry_id": r["entry_id"],
                "iteration": r["iteration"],
                "signed_pct": signed,
            }
            cur = best.get(r["benchmark"])
            if cur is None:
                best[r["benchmark"]] = cand
                continue
            if abs(signed) < abs(cur["signed_pct"]) or (
                abs(signed) == abs(cur["signed_pct"])
                and cand["iteration"] < cur["iteration"]
            ):
                best[r["benchmark"]] = cand
        return sorted(best.values(), key=lambda r: (abs(r["signed_pct"]), r["iteration"]))

    def lineage(self, entry_id: str) -> list[dict[str, Any]]:
        """Return the root-to-``entry_id`` chain (inclusive), oldest first.

        Walks ``parent_id`` pointers; detects cycles defensively.
        """
        chain: list[dict[str, Any]] = []
        seen: set[str] = set()
        cur: str | None = entry_id
        while cur and cur not in seen:
            seen.add(cur)
            entry = self.load_entry(cur)
            if entry is None:
                break
            chain.append(entry)
            cur = entry.get("parent_id")
        chain.reverse()
        return chain

    def all_entries(self) -> list[dict[str, Any]]:
        """Every entry in insertion/timestamp order (oldest first)."""
        rows = self.query(
            "SELECT entry_id FROM iterations ORDER BY timestamp, iteration")
        return [self.load_entry(r["entry_id"]) for r in rows]
