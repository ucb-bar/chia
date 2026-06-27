"""SQLite persistence for the CIRCT issue flow (head node only).

One row per issue attempt, pinned to the driver's Ray node
with ``pin_to_current_node=True``. The driver runs on the head (the bare
``ray start --head`` instance, a distinct Ray node from the worker containers),
so members execute there and the DB file lands on the head's local disk.

Because SQLiteNode dispatches through Ray (``.chia_remote()`` + ``get()``),
``init_db()`` must be called after ``ray.init()``. The driver writes here as each
run_issue_remote result resolves; triage reads ``attempted_numbers()`` to avoid
repeats.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from chia.base.ChiaFunction import get
from chia.database.sqlite_node import SQLiteNode

_SCHEMA = """
CREATE TABLE IF NOT EXISTS attempts (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_number  INTEGER NOT NULL,
    title         TEXT,
    url           TEXT,
    status        TEXT,        -- fixed | attempted | no_repro | unclear | not_a_bug | error
    reproduced    INTEGER,
    build_ok      INTEGER,
    fixed         INTEGER,
    lit_ok        INTEGER,
    lit_passed    INTEGER,
    lit_failed    INTEGER,
    lit_failures  TEXT,        -- JSON list
    diff_added    INTEGER,
    diff_removed  INTEGER,
    diff          TEXT,
    pr_writeup    TEXT,
    test_paths    TEXT,        -- JSON list
    notes         TEXT,
    llm_model     TEXT,
    artifact_dir  TEXT,
    created_at    TEXT
);
"""

# Column order for INSERT — keep in sync with the row tuple built in record().
_COLS = (
    "issue_number", "title", "url", "status", "reproduced", "build_ok", "fixed",
    "lit_ok", "lit_passed", "lit_failed", "lit_failures", "diff_added",
    "diff_removed", "diff", "pr_writeup", "test_paths", "notes", "llm_model",
    "artifact_dir", "created_at",
)
_INSERT = (f"INSERT INTO attempts ({', '.join(_COLS)}) "
           f"VALUES ({', '.join(['?'] * len(_COLS))})")

_node: SQLiteNode | None = None


def init_db(path: str) -> None:
    """Open/create the head-local DB as a SQLiteNode and ensure the schema.

    Call AFTER ray.init() — SQLiteNode pins to the current (driver/head) node and
    dispatches its members as Ray tasks.
    """
    global _node
    _node = SQLiteNode(path, pin_to_current_node=True)
    get(_node.init_schema.chia_remote(_SCHEMA))


def _db() -> SQLiteNode:
    if _node is None:
        raise RuntimeError("db.init_db(path) must be called first (after ray.init)")
    return _node


def close_db() -> None:
    """Release the node (idempotent; best-effort)."""
    global _node
    if _node is not None:
        try:
            _node.close()
        except Exception:
            pass
        _node = None


def _i(v) -> int | None:
    """None passthrough; bool/int -> int (int(True) == 1)."""
    return None if v is None else int(v)


def attempted_numbers() -> set[int]:
    """Issue numbers already attempted (any status) — triage skips these."""
    rows = get(_db().query.chia_remote("SELECT DISTINCT issue_number FROM attempts"))
    return {r["issue_number"] for r in rows}


def record(issue, res: dict, model: str, artifact_dir: str) -> int:
    """Insert one attempt row from a run_issue_remote result dict; return its id."""
    row = (
        issue.number, issue.title, issue.url, res.get("status"),
        _i(res.get("reproduced")), _i(res.get("build_ok")), _i(res.get("fixed")),
        _i(res.get("lit_ok")), _i(res.get("lit_passed")), _i(res.get("lit_failed")),
        json.dumps(res.get("lit_failures") or []),
        _i(res.get("added")), _i(res.get("removed")), res.get("diff"),
        res.get("writeup"), json.dumps(res.get("test_paths") or []),
        res.get("notes") or res.get("repro_tail"),
        model, artifact_dir, datetime.now(timezone.utc).isoformat(),
    )
    return get(_db().execute.chia_remote(_INSERT, row)).lastrowid


def summary() -> list[tuple]:
    """One compact row per attempt, newest first (for quick CLI inspection)."""
    rows = get(_db().query.chia_remote(
        "SELECT issue_number, status, fixed, lit_ok, diff_added, diff_removed "
        "FROM attempts ORDER BY id DESC"))
    return [(r["issue_number"], r["status"], r["fixed"], r["lit_ok"],
             r["diff_added"], r["diff_removed"]) for r in rows]
