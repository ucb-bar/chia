"""Helpers for the MemCpy example: classification, output dumping, git diff.

Logic kept out of ``claude.py`` (which drives the LLM) and ``memcpy_loop.py``
(the orchestration). ``classify_run`` turns a verilator :class:`RunResult` into
an :class:`Outcome` the loop branches on; :class:`Dumper` / :func:`dump_llm`
write per-run collateral into ``OUT_DIR``; :func:`collect_diff` is a chipyard
worker node that captures the git diff of the generated Chisel.
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from constants import DATA_SIZE, DRAMSIM_INI_DIR
from chia.base.ChiaFunction import ChiaFunction
from chia.chipyard.state_def import RunResult

logger = logging.getLogger(__name__)


@dataclass
class Outcome:
    """Classification of one build+run attempt, with a human label."""
    passed: bool          # design built AND memcpy test fully correct
    kind: str             # "pass" | "build_failure" | "runtime" | "timeout" | "incorrect"
    detail: str           # short human-readable summary (num correct, rc, etc.)


def _parse_num_correct(text: str) -> int | None:
    """Pull ``MEMCPY Num Correct: N`` out of simulator output (None if absent)."""
    m = re.search(r"MEMCPY Num Correct:\s*(\d+)", text)
    return int(m.group(1)) if m else None


def classify_run(run: RunResult) -> Outcome:
    """Classify a verilator RunResult against the memcpy correctness contract.

    Reads the ``MEMCPY Num Correct: N`` line that the test program prints and
    requires ``N == DATA_SIZE``. A nonzero/none return code means the sim
    crashed or timed out.
    """
    combined = f"{run.log}\n{run.out}"
    if run.returncode is None or run.returncode < 0:
        return Outcome(False, "timeout", "simulation timed out (no clean exit)")
    if run.returncode != 0:
        return Outcome(False, "runtime", f"simulator exited with rc={run.returncode}")
    num_correct = _parse_num_correct(combined)
    if num_correct is None:
        return Outcome(False, "incorrect",
                       "no 'MEMCPY Num Correct' line in output (copy never completed?)")
    if num_correct == DATA_SIZE:
        return Outcome(True, "pass", f"{num_correct}/{DATA_SIZE} elements correct")
    return Outcome(False, "incorrect", f"{num_correct}/{DATA_SIZE} elements correct")


# ---------------------------------------------------------------------------
# Output dumping
# ---------------------------------------------------------------------------

class Dumper:
    """Writes intermediate results into OUT_DIR, prefixing each filename with
    the timestamp at write time (so files sort by when they were produced)."""

    def __init__(self, out_dir: Path):
        self.out_dir = out_dir
        self.out_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Dumping run artifacts to %s", self.out_dir)

    def _path(self, name: str) -> Path:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return self.out_dir / f"{ts}_{name}"

    def text(self, name: str, content: str) -> None:
        with open(self._path(name), "w", errors="replace") as f:
            f.write(content or "")

    def bytes(self, name: str, content: bytes) -> None:
        with open(self._path(name), "wb") as f:
            f.write(content or b"")

    def json(self, name: str, obj: dict) -> None:
        with open(self._path(name), "w") as f:
            json.dump(obj, f, indent=2)


def dump_llm(dump: Dumper, name: str, cli) -> None:
    """Persist an LLM call's final text + full stream transcript."""
    body = f"# {name}\n\nsuccess={getattr(cli, 'success', None)}\n\n"
    body += (getattr(cli, "result", "") or "")
    stream = getattr(cli, "stream_result", "") or ""
    if stream:
        body += "\n\n## Stream transcript\n\n" + stream
    dump.text(f"{name}.md", body)


# ---------------------------------------------------------------------------
# Chipyard worker nodes (git ops on the checkout)
# ---------------------------------------------------------------------------

@ChiaFunction(resources={"chipyard": 0.05})
def reset_chipyard(chipyard_path: str) -> str:
    """Reset the chipyard checkout to its committed baseline so each run's
    implement node starts from a clean tree (the container is reused across
    runs, so a previous run's accelerator + config edits would otherwise
    persist). Reverts tracked modifications (`git checkout -- .`) and removes
    new untracked, non-ignored files/dirs (`git clean -fd`); gitignored build
    artifacts are left in place. Operates on the root chipyard repo, where the
    accelerator, config wiring, and test live. Returns a short status string.
    """
    co = subprocess.run(
        ["git", "-C", chipyard_path, "checkout", "--", "."],
        capture_output=True, text=True,
    )
    cl = subprocess.run(
        ["git", "-C", chipyard_path, "clean", "-fd"],
        capture_output=True, text=True,
    )
    return (co.stdout + co.stderr + cl.stdout + cl.stderr).strip() or "clean"


def _untracked_diff(repo_path: str) -> str:
    """New-file diffs for untracked, non-ignored files in *repo_path*, read-only.

    `git diff` ignores untracked files, so brand-new sources (the freshly
    written MemCopyRoCC.scala / tests/memcpy.c) would be omitted. We surface
    them without touching the index: list them with `git ls-files --others
    --exclude-standard`, then diff each against /dev/null with
    `git diff --no-index` (which never reads or writes the index). ls-files does
    not descend into submodules, so a repo's scan won't pick up its submodules.
    """
    listed = subprocess.run(
        ["git", "-C", repo_path, "ls-files", "--others", "--exclude-standard"],
        capture_output=True, text=True,
    ).stdout.split()
    parts = []
    for f in listed:
        # --no-index exits 1 when files differ; we only consume stdout.
        r = subprocess.run(
            ["git", "-C", repo_path, "diff", "--no-index", "--", "/dev/null", f],
            capture_output=True, text=True,
        )
        if r.stdout:
            parts.append(r.stdout)
    return "".join(parts)


@ChiaFunction(resources={"chipyard": 0.05})
def collect_diff(
    chipyard_path: str,
    submodules: list[str] | None = None,
) -> tuple[int, dict[str, str]]:
    """Collect the chipyard git diff: tracked modifications AND new untracked
    files, for the root repo and each listed submodule.

    Read-only — nothing is staged, added, or committed (untracked files are
    surfaced via ``git diff --no-index``; see :func:`_untracked_diff`). Returns
    ``(error, diffs)`` where ``diffs`` maps repo path to diff text (key ``""``
    = root chipyard). ``error=1`` if a listed submodule is missing.
    """
    submodules = submodules or []
    for sm in submodules:
        if not os.path.isdir(os.path.join(chipyard_path, sm, ".git")):
            return (1, {})

    diffs: dict[str, str] = {}
    root = subprocess.run(
        ["git", "-C", chipyard_path, "diff", "--ignore-submodules=all"],
        capture_output=True, text=True,
    ).stdout
    diffs[""] = root + _untracked_diff(chipyard_path)
    for sm in submodules:
        sm_path = os.path.join(chipyard_path, sm)
        tracked = subprocess.run(
            ["git", "-C", sm_path, "diff"], capture_output=True, text=True,
        ).stdout
        diffs[sm] = tracked + _untracked_diff(sm_path)
    return (0, diffs)


# ---------------------------------------------------------------------------
# Collateral loading
# ---------------------------------------------------------------------------

def load_dramsim_ini() -> dict[str, bytes]:
    ini: dict[str, bytes] = {}
    d = DRAMSIM_INI_DIR
    if d.is_dir():
        for fpath in sorted(d.iterdir()):
            if fpath.is_file():
                ini[fpath.name] = fpath.read_bytes()
    return ini
