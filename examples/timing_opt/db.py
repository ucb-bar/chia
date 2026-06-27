"""SQLite-backed store for the BoomTile timing-improvement flow.

Replaces the old directory-based "FlatDB". A single SQLite database
(``DB/timing.db``) indexes each flow run ("branch"): it stores queryable scalar
metrics (area, worst-slack, status, per-test perf counters) plus *pointers* to
on-disk artifact files. Every artifact is written as a real file under
``DB/files/<branch>/`` and referenced by a row in the ``files`` table — nothing
large is inlined as a blob, per the design.

Layout::

    DB/
      timing.db                  # this SQLite database (head-node only)
      files/<branch>/            # one directory per flow run
        diff.json
        generated_src/<relpath>  # each generated Verilog file, as a real file
        timing_report.rpt
        synthesis_reports/...     # _save_variant_results output
        area_estimates.json, synthesis_log.md, *reports.json
        syn_obj/                 # full Hammer/Genus working dir
        perf_results.md
        improve_timing_log.md
        logs/                    # timing.csv, prompt, LLM/debug transcripts, ...

``TimingDB`` is a thin domain layer over the canonical
:class:`chia.database.SQLiteNode` (the same pattern as gem5_align's
``AlignmentDB``). The node owns the generic machinery — connection/PRAGMA
handling (WAL, busy-timeout), atomic ``BEGIN IMMEDIATE`` transactions, and the
DDL — and this class adds the timing-flow-specific composite reads/writes plus
the on-disk artifact bookkeeping (the ``files``-table registrations and the real
files under ``files/<branch>/``).

Placement: the DB lives on the head node's local disk and is only touched on the
head (the driver and the head-pinned ``ExperimentLogger`` actor); workers never
open it. So ``TimingDB`` constructs the node with ``pin_to_current_node=True`` (a
hard NodeAffinity pin to the constructing node) and invokes its members
in-process, synchronously — like a plain head-side store. This requires an
initialized Ray context, so construct ``TimingDB`` *after* ``ray.init``. Paths
stored in the DB are absolute (the synthesis worker needs an absolute
``syn_obj`` path, and the head resolves the rest on its own disk).
"""

from __future__ import annotations

import json
import re
import shutil
from pathlib import Path

from chia.database import SQLiteNode


_SCHEMA = """
CREATE TABLE IF NOT EXISTS branches (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    name              TEXT UNIQUE NOT NULL,
    parent_id         INTEGER REFERENCES branches(id),
    is_seed           INTEGER NOT NULL DEFAULT 0,
    iteration         INTEGER,
    build_config      TEXT,
    status            TEXT,
    boom_tile_module  TEXT,
    area              REAL,
    worst_slack_ns    REAL,
    worst_slack_met   INTEGER,
    worst_slack_line  TEXT,
    synthesis_success INTEGER,
    verilator_passed  INTEGER,
    verilator_failed  INTEGER,
    files_dir         TEXT,
    created_at        TEXT DEFAULT (datetime('now')),
    updated_at        TEXT
);

CREATE TABLE IF NOT EXISTS files (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_id INTEGER NOT NULL REFERENCES branches(id) ON DELETE CASCADE,
    role      TEXT NOT NULL,
    path      TEXT NOT NULL,
    kind      TEXT NOT NULL DEFAULT 'file',
    label     TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(branch_id, role, path)
);

CREATE TABLE IF NOT EXISTS perf_results (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_id     INTEGER NOT NULL REFERENCES branches(id) ON DELETE CASCADE,
    test_name     TEXT NOT NULL,
    passed        INTEGER,
    counters_json TEXT
);

CREATE TABLE IF NOT EXISTS llm_experiments (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_id        INTEGER NOT NULL REFERENCES branches(id) ON DELETE CASCADE,
    exp_index        INTEGER,                  -- 1-based within the parent branch
    vlsi_top         TEXT NOT NULL,
    status           TEXT,
    area             REAL,
    worst_slack_ns   REAL,
    worst_slack_met  INTEGER,
    worst_slack_line TEXT,
    elapsed_seconds  REAL,
    files_dir        TEXT,                     -- DB/files/<parent>/experiments/<exp_id>
    created_at       TEXT DEFAULT (datetime('now'))
);
"""

# Columns set_metrics() is allowed to update (guards against SQL injection via
# arbitrary kwargs and documents the queryable surface).
_METRIC_COLUMNS = frozenset({
    "status", "boom_tile_module", "area", "worst_slack_ns", "worst_slack_met",
    "worst_slack_line", "synthesis_success", "verilator_passed",
    "verilator_failed", "iteration", "build_config",
})

# Columns set_experiment_metrics() is allowed to update.
_EXP_METRIC_COLUMNS = frozenset({
    "status", "area", "worst_slack_ns", "worst_slack_met", "worst_slack_line",
    "elapsed_seconds",
})


class TimingDB(SQLiteNode):
    """Timing-flow store: a domain layer over the canonical :class:`SQLiteNode`.

    Composes the node's generic members (``query`` / ``query_one`` /
    ``query_value`` / ``execute`` / ``transaction``, invoked in-process on the
    pinned node) and keeps the on-disk artifact bookkeeping. One instance lives
    on the head for a run; Ray workers never touch the database (they only write
    into the file directories they are handed, e.g. ``syn_obj``).
    """

    def __init__(self, db_dir, *args, pin_to_current_node: bool = True, **kwargs):
        """Resolve the DB dir/files tree, pin the node, and create the schema.

        ``db_dir`` is the directory holding ``timing.db`` + ``files/``; it is
        created if absent. Placement defaults to ``pin_to_current_node=True``
        (the head's local DB); other :class:`SQLiteNode` placement args may be
        passed through. Requires an initialized Ray context.
        """
        self.db_dir = Path(db_dir).resolve()
        self.files_dir = self.db_dir / "files"
        self.db_dir.mkdir(parents=True, exist_ok=True)
        self.files_dir.mkdir(parents=True, exist_ok=True)
        super().__init__(
            str(self.db_dir / "timing.db"), *args,
            pin_to_current_node=pin_to_current_node, **kwargs,
        )
        self.init_schema(_SCHEMA)

    # ------------------------------------------------------------------ #
    # Branch lookup / creation
    # ------------------------------------------------------------------ #
    def has_any_branch(self) -> bool:
        return self.query_value("SELECT 1 FROM branches LIMIT 1") is not None

    def branch_exists(self, name: str) -> bool:
        return self.query_value(
            "SELECT 1 FROM branches WHERE name = ?", (name,)) is not None

    def get_branch(self, name: str) -> dict | None:
        return self.query_one("SELECT * FROM branches WHERE name = ?", (name,))

    def _branch_id(self, name: str) -> int:
        v = self.query_value("SELECT id FROM branches WHERE name = ?", (name,))
        if v is None:
            raise KeyError(f"branch not in DB: {name}")
        return int(v)

    def create_branch(
        self,
        name: str,
        parent: str | None = None,
        is_seed: bool = False,
        iteration: int | None = None,
        build_config: str | None = None,
    ) -> int:
        """Insert a branch row (idempotent) and create its files directory.

        ``parent`` is a branch *name*; it must already exist (or be None for a
        seed). Returns the branch id. If the branch already exists its row is
        left intact (re-runs just reuse it) but the files dir is ensured.
        """
        parent_id = self._branch_id(parent) if parent is not None else None
        fdir = self.branch_files_dir(name)  # also mkdirs
        self.execute(
            "INSERT OR IGNORE INTO branches "
            "(name, parent_id, is_seed, iteration, build_config, files_dir) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (name, parent_id, 1 if is_seed else 0, iteration,
             build_config, str(fdir)),
        )
        return self._branch_id(name)

    def branch_files_dir(self, name: str) -> Path:
        """Absolute directory holding this branch's artifact files (created)."""
        d = self.files_dir / name
        d.mkdir(parents=True, exist_ok=True)
        return d

    # ------------------------------------------------------------------ #
    # File registration (DB points at on-disk files)
    # ------------------------------------------------------------------ #
    def _register(self, name: str, role: str, path, kind: str = "file",
                  label: str | None = None) -> None:
        bid = self._branch_id(name)
        self.execute(
            "INSERT OR REPLACE INTO files (branch_id, role, path, kind, label) "
            "VALUES (?, ?, ?, ?, ?)",
            (bid, role, str(Path(path).resolve()), kind, label),
        )

    def register_file(self, name: str, role: str, path, label: str | None = None) -> None:
        self._register(name, role, path, kind="file", label=label)

    def register_dir(self, name: str, role: str, path, label: str | None = None) -> None:
        self._register(name, role, path, kind="dir", label=label)

    def files_for(self, name: str, role: str | None = None) -> list[dict]:
        bid = self._branch_id(name)
        if role is None:
            return self.query("SELECT * FROM files WHERE branch_id = ?", (bid,))
        return self.query(
            "SELECT * FROM files WHERE branch_id = ? AND role = ?", (bid, role))

    # ------------------------------------------------------------------ #
    # Typed artifact save/load
    # ------------------------------------------------------------------ #
    def save_diff(self, name: str, diff_dict: dict) -> Path:
        p = self.branch_files_dir(name) / "diff.json"
        p.write_text(json.dumps(diff_dict))
        self.register_file(name, "diff", p)
        return p

    def load_diff(self, name: str) -> dict:
        p = self.branch_files_dir(name) / "diff.json"
        if not p.exists():
            raise FileNotFoundError(f"missing diff.json for branch {name}: {p}")
        return json.loads(p.read_text())

    def save_generated_src(self, name: str, items: list) -> Path:
        """Write each (relpath, contents) pair as a real file under generated_src/."""
        gdir = self.branch_files_dir(name) / "generated_src"
        if gdir.exists():
            shutil.rmtree(gdir)
        gdir.mkdir(parents=True)
        for relname, contents in items:
            fp = gdir / relname
            fp.parent.mkdir(parents=True, exist_ok=True)
            fp.write_text(contents)
        self.register_dir(name, "generated_src", gdir)
        return gdir

    def load_generated_src(self, name: str) -> list:
        gdir = self.branch_files_dir(name) / "generated_src"
        if not gdir.is_dir():
            raise FileNotFoundError(
                f"missing generated_src/ for branch {name}: {gdir}"
            )
        items = []
        for p in sorted(gdir.rglob("*")):
            if p.is_file():
                items.append((p.relative_to(gdir).as_posix(), p.read_text()))
        return items

    def save_timing_report(self, name: str, text: str) -> Path:
        """Save the produced timing report and populate worst-slack columns."""
        p = self.branch_files_dir(name) / "timing_report.rpt"
        p.write_text(text or "")
        self.register_file(name, "timing_report", p)
        ns, met, line = parse_worst_slack(text or "")
        self.set_metrics(
            name, worst_slack_ns=ns, worst_slack_met=met, worst_slack_line=line
        )
        return p

    def load_timing_report(self, name: str) -> str:
        p = self.branch_files_dir(name) / "timing_report.rpt"
        if not p.exists():
            raise FileNotFoundError(
                f"missing timing_report.rpt for branch {name}: {p}"
            )
        return p.read_text()

    def save_perf_results(self, name: str, results: list) -> Path:
        """Persist per-test TMA counters.

        ``results`` is a list of dicts ``{"test_name", "passed", "counters"}``.
        Writes a ``perf_results.md`` (same format the flow used) and replaces
        the branch's ``perf_results`` rows (DELETE + bulk INSERT) atomically so
        the counters are queryable.
        """
        bid = self._branch_id(name)
        md = [f"# perf_results for {name}\n\n"]
        rows = []
        for r in results:
            test_name = r["test_name"]
            passed = r.get("passed")
            counters = r.get("counters") or {}
            rows.append((bid, test_name, 1 if passed else 0, json.dumps(counters)))
            md.append(
                f"## {test_name} (passed={passed})\n"
                f"```\n{json.dumps(counters, indent=2)}\n```\n"
            )
        ops = [("DELETE FROM perf_results WHERE branch_id = ?", (bid,))]
        if rows:
            ops.append((
                "INSERT INTO perf_results (branch_id, test_name, passed, counters_json) "
                "VALUES (?, ?, ?, ?)",
                rows,  # list params -> executemany inside the transaction
            ))
        self.transaction(ops)
        p = self.branch_files_dir(name) / "perf_results.md"
        p.write_text("".join(md))
        self.register_file(name, "perf_results", p)
        return p

    def get_perf_results(self, name: str) -> list[dict]:
        bid = self._branch_id(name)
        rows = self.query(
            "SELECT test_name, passed, counters_json FROM perf_results "
            "WHERE branch_id = ?", (bid,))
        return [{
            "test_name": r["test_name"],
            "passed": bool(r["passed"]),
            "counters": json.loads(r["counters_json"] or "{}"),
        } for r in rows]

    # ------------------------------------------------------------------ #
    # Scalar metrics + composite loaders
    # ------------------------------------------------------------------ #
    def set_metrics(self, name: str, **cols) -> None:
        """Update queryable scalar columns on the branch row.

        Silently ignores kwargs whose value is None so callers can pass a wide
        net. Unknown column names raise (typo guard).
        """
        updates = {k: v for k, v in cols.items() if v is not None}
        bad = set(updates) - _METRIC_COLUMNS
        if bad:
            raise ValueError(f"unknown metric column(s): {sorted(bad)}")
        if not updates:
            return
        bid = self._branch_id(name)
        set_clause = ", ".join(f"{k} = ?" for k in updates)  # keys whitelisted above
        self.execute(
            f"UPDATE branches SET {set_clause}, updated_at = datetime('now') "
            f"WHERE id = ?",
            (*updates.values(), bid),
        )

    def load_inputs(self, name: str) -> dict:
        """DB equivalent of the flow's old ``_load_inputs``.

        Returns the parent branch's diff, generated source, timing report, and
        the BoomTile module name (read from the stored column). Raises if the
        branch or its required artifacts are missing.
        """
        branch = self.get_branch(name)
        if branch is None:
            raise FileNotFoundError(f"branch not found in DB: {name}")
        return {
            "branch_name": name,
            "diff_dict": self.load_diff(name),
            "generated_src": self.load_generated_src(name),
            "boom_tile": branch["boom_tile_module"],
            "timing_report": self.load_timing_report(name),
            "timing_report_path": self.branch_files_dir(name) / "timing_report.rpt",
        }

    # ------------------------------------------------------------------ #
    # LLM experiments — exploratory sub-block synth runs made by the LLM
    # inside the /improve_timing session, recorded under the parent branch
    # ------------------------------------------------------------------ #
    def create_experiment(self, parent_branch: str, vlsi_top: str) -> tuple[int, Path]:
        """Insert an llm_experiments row and create its files dir.

        Returns ``(exp_id, files_dir)`` where ``files_dir`` is the absolute path
        ``DB/files/<parent>/experiments/<exp_id>/`` (created). ``exp_index`` is
        the 1-based ordinal within the parent's experiments. Called serially by
        the single head-pinned ``ExperimentLogger`` actor, so the read-then-
        insert of ``exp_index`` doesn't race.
        """
        pid = self._branch_id(parent_branch)
        next_idx = int(self.query_value(
            "SELECT COALESCE(MAX(exp_index), 0) + 1 FROM llm_experiments "
            "WHERE branch_id = ?", (pid,), default=1))
        res = self.execute(
            "INSERT INTO llm_experiments (branch_id, exp_index, vlsi_top) "
            "VALUES (?, ?, ?)",
            (pid, next_idx, vlsi_top))
        exp_id = int(res.lastrowid)
        files_dir = (self.branch_files_dir(parent_branch)
                     / "experiments" / str(exp_id))
        files_dir.mkdir(parents=True, exist_ok=True)
        self.execute(
            "UPDATE llm_experiments SET files_dir = ? WHERE id = ?",
            (str(files_dir), exp_id))
        return exp_id, files_dir

    def get_experiment(self, exp_id: int) -> dict | None:
        return self.query_one(
            "SELECT * FROM llm_experiments WHERE id = ?", (exp_id,))

    def get_experiments_for(self, parent_branch: str) -> list[dict]:
        pid = self._branch_id(parent_branch)
        return self.query(
            "SELECT * FROM llm_experiments WHERE branch_id = ? "
            "ORDER BY exp_index", (pid,))

    def set_experiment_metrics(self, exp_id: int, **cols) -> None:
        updates = {k: v for k, v in cols.items() if v is not None}
        bad = set(updates) - _EXP_METRIC_COLUMNS
        if bad:
            raise ValueError(f"unknown experiment metric column(s): {sorted(bad)}")
        if not updates:
            return
        set_clause = ", ".join(f"{k} = ?" for k in updates)  # keys whitelisted
        self.execute(
            f"UPDATE llm_experiments SET {set_clause} WHERE id = ?",
            (*updates.values(), exp_id))

    def experiment_files_dir(self, exp_id: int) -> Path:
        row = self.get_experiment(exp_id)
        if row is None or not row["files_dir"]:
            raise KeyError(f"experiment not in DB or missing files_dir: {exp_id}")
        return Path(row["files_dir"])

    def save_experiment_timing_report(self, exp_id: int, text: str) -> Path:
        """Save an experiment's timing report and populate worst-slack columns."""
        p = self.experiment_files_dir(exp_id) / "timing_report.rpt"
        p.write_text(text or "")
        ns, met, line = parse_worst_slack(text or "")
        self.set_experiment_metrics(
            exp_id, worst_slack_ns=ns, worst_slack_met=met, worst_slack_line=line,
        )
        return p


# ---------------------------------------------------------------------------
# Worst-slack parsing (the "most important" queryable bit of a timing report)
# ---------------------------------------------------------------------------
_FLOAT_RE = re.compile(r"[-+]?\d+(?:\.\d+)?")
# Genus per-path header, e.g. "Path 1: VIOLATED (-12054 ps) Setup Check ..."
# Path 1 is the worst path by construction (Genus sorts paths by slack).
_PATH_HEAD_RE = re.compile(
    r"^Path\s+\d+:\s+(VIOLATED|MET)\s+\(\s*([-+]?\d+(?:\.\d+)?)\s*(ps|ns)\s*\)",
    re.IGNORECASE,
)


def parse_worst_slack(report_text: str) -> tuple[float | None, int | None, str | None]:
    """Best-effort extract (slack_ns, met, raw_line) from a Genus timing report.
    """
    for line in report_text.splitlines():
        m = _PATH_HEAD_RE.match(line.lstrip())
        if m:
            marker = m.group(1).upper()
            value = float(m.group(2))
            unit = m.group(3).lower()
            slack_ns = value / 1000.0 if unit == "ps" else value
            return slack_ns, (1 if marker == "MET" else 0), line.strip()
    for line in report_text.splitlines():
        low = line.lower()
        if "slack" in low and ("(violated)" in low or "(met)" in low):
            stripped = line.strip()
            met = 1 if "(met)" in low else 0
            m = _FLOAT_RE.search(stripped)
            ns = float(m.group()) if m else None
            return ns, met, stripped
    return None, None, None
