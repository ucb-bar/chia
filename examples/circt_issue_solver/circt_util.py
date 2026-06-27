"""Flow-specific CIRCT worker ops for the circt_issue_solver example.
"""

import logging
import os
import subprocess
import tempfile

from chia.base.ChiaFunction import ChiaFunction

logger = logging.getLogger(__name__)

# The general build/test primitives + the source-tree path constant live in
# chia.chipyard.circt. Re-export the ones the flow uses so this module stays the
# single `import circt_util` entry point for worker code.
from chia.chipyard.circt import (  # noqa: F401,E402  (re-exported)
    _CIRCT_SOURCE_TREE,
    circt_ninja_build,
    circt_run_lit,
    circt_warm_build,
)


def _tail(text: str, n: int = 120) -> str:
    """Last *n* lines of *text* (keeps task return payloads bounded)."""
    return "\n".join(text.splitlines()[-n:])


def _decode(s) -> str:
    """Coerce subprocess stdout/stderr (str or bytes or None) to str."""
    if s is None:
        return ""
    return s if isinstance(s, str) else s.decode(errors="replace")


@ChiaFunction(resources={"circt": 1})
def circt_trust_source(timeout_seconds: int = 30) -> dict:
    """Mark the CIRCT checkout as a git safe.directory in this container's global
    config. The image bakes /workspace/circt under a different uid than the
    ``--user`` the container runs as, so git otherwise refuses every command with
    "detected dubious ownership". Idempotent; also covers git run via the bash
    tool (shared container HOME). circt_git_reset / circt_capture_diff pass
    ``-c safe.directory`` too, so they work even if this can't write the config.
    """
    try:
        r = subprocess.run(
            ["git", "config", "--global", "--add", "safe.directory", _CIRCT_SOURCE_TREE],
            capture_output=True, text=True, timeout=timeout_seconds)
        return {"success": r.returncode == 0, "log": (r.stdout + r.stderr).strip()}
    except Exception as e:  # noqa: BLE001
        return {"success": False, "log": str(e)}


@ChiaFunction(resources={"circt": 1})
def circt_git_reset(ref: str = "HEAD", timeout_seconds: int = 300) -> dict:
    """Reset /workspace/circt to a clean *ref* between independent edit sessions.

    Runs ``git reset --hard <ref>`` then ``git clean -fd`` (NOT ``-x``, so the
    gitignored ``build/`` tree — the warm incremental build — survives). For the
    chia-circt image, ``HEAD`` is the firtool-1.148.0 tag the source is pinned to.

    Returns ``{success: bool, log: str}``.
    """
    if not os.path.isdir(_CIRCT_SOURCE_TREE):
        return {"success": False, "log": f"no CIRCT source tree at {_CIRCT_SOURCE_TREE}"}
    log: list[str] = []
    for args in (["reset", "--hard", ref], ["clean", "-fd"]):
        cmd = ["git", "-c", f"safe.directory={_CIRCT_SOURCE_TREE}", "-C", _CIRCT_SOURCE_TREE, *args]
        logger.info(f"[git] {' '.join(cmd)}")
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            return {"success": False, "log": "\n".join(log) + f"\n{' '.join(cmd)} timed out"}
        log.append(f"$ {' '.join(cmd)}\n{r.stdout}{r.stderr}".rstrip())
        if r.returncode != 0:
            return {"success": False, "log": "\n".join(log)}
    return {"success": True, "log": "\n".join(log)}


@ChiaFunction(resources={"circt": 1})
def circt_apply_diff(diff_text: str, timeout_seconds: int = 120) -> dict:
    """``git apply`` *diff_text* onto the CIRCT tree (cwd = source root).

    Used to REPLAY a saved fix.diff so a later phase (e.g. the regression-repair
    turn) can run without redoing repro+fix. Returns ``{success, log}``.
    """
    if not os.path.isdir(_CIRCT_SOURCE_TREE):
        return {"success": False, "log": f"no CIRCT source tree at {_CIRCT_SOURCE_TREE}"}
    with tempfile.NamedTemporaryFile("w", suffix=".diff", delete=False) as f:
        f.write(diff_text)
        path = f.name
    cmd = ["git", "-c", f"safe.directory={_CIRCT_SOURCE_TREE}", "-C", _CIRCT_SOURCE_TREE,
           "apply", "--whitespace=nowarn", path]
    logger.info(f"[git] {' '.join(cmd)}")
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        return {"success": False, "log": f"{' '.join(cmd)} timed out"}
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass
    return {"success": r.returncode == 0, "log": f"$ {' '.join(cmd)}\n{r.stdout}{r.stderr}".rstrip()}


@ChiaFunction(resources={"circt": 1})
def circt_write_files(files: dict, base_dir: str) -> dict:
    """Write ``{relpath: content}`` under *base_dir* (e.g. restore saved repro
    artifacts into /workspace/circt/.circtissues). ``.sh`` files are made
    executable. Returns ``{written: [relpaths]}``.
    """
    os.makedirs(base_dir, exist_ok=True)
    written = []
    for rel, content in (files or {}).items():
        dest = os.path.join(base_dir, rel)
        os.makedirs(os.path.dirname(dest) or base_dir, exist_ok=True)
        with open(dest, "w") as f:
            f.write(content)
        if dest.endswith(".sh"):
            os.chmod(dest, 0o755)
        written.append(rel)
    return {"written": written}


@ChiaFunction(resources={"circt": 1})
def circt_run_script(
    script_path: str,
    cwd: str = _CIRCT_SOURCE_TREE,
    timeout_seconds: int = 1200,
) -> dict:
    """Run a repro script (bash) and report its exit status.

    The issue flow's repro.sh contract: **exit 0 iff the bug is fixed** — a crash
    or assertion exits nonzero on its own; a miscompile wraps the expected-correct
    output in ``FileCheck`` so wrong output fails. So the same script gates both
    reproduction (nonzero on the clean tree) and the fix (zero after the patch).

    Returns ``{exit_code: int, log_tail: str, timed_out: bool}``.
    """
    if not os.path.isfile(script_path):
        return {"exit_code": 127, "log_tail": f"no such script: {script_path}", "timed_out": False}
    try:
        r = subprocess.run(["bash", script_path], cwd=cwd, capture_output=True,
                           text=True, timeout=timeout_seconds)
    except subprocess.TimeoutExpired as e:
        return {"exit_code": -1, "log_tail": _tail(_decode(e.stdout) + _decode(e.stderr)), "timed_out": True}
    return {"exit_code": r.returncode, "log_tail": _tail(r.stdout + r.stderr), "timed_out": False}


# Regression-gate scope: run the WHOLE lit suite minus the categories that are
# red on the unmodified tree in this SDK-only image (so a gate failure means a
# real regression, not a missing-binary artifact):
#   - test/CAPI/*               need circt-capi-*-test binaries (not built here)
#   - test/Tools/circt-tblgen/* need a circt-tblgen with RTG backends (absent)
# CAPI is dropped wholesale (whole dir is red); circt-tblgen is a subdir of Tools
# (whose other tests are green), so it's dropped via lit --filter-out instead.
_LIT_GATE_EXCLUDE_DIRS = ("CAPI",)
_LIT_GATE_FILTER_OUT = "circt-tblgen"


def circt_lit_gate_paths() -> list[str]:
    """Build-relative test dirs for the regression gate: every top-level
    test/<category> except the baseline-red ones in _LIT_GATE_EXCLUDE_DIRS.

    Pair with ``circt_run_lit(..., filter_out=_LIT_GATE_FILTER_OUT)`` to also drop
    the red Tools/circt-tblgen subdir. Falls back to () if the tree is missing.
    """
    base = os.path.join(_CIRCT_SOURCE_TREE, "test")
    if not os.path.isdir(base):
        return []
    excl = set(_LIT_GATE_EXCLUDE_DIRS)
    return [f"test/{name}" for name in sorted(os.listdir(base))
            if name not in excl and os.path.isdir(os.path.join(base, name))]


def circt_lit_failure_paths(failures: list[str]) -> list[str]:
    """Map ``circt_run_lit`` failure names back to build-relative test paths so a
    single failing test can be re-run on its own.

    A failure name looks like ``CIRCT :: Conversion/FIRRTLToHW/foo.mlir (216 of
    1075)``; we take the suite-relative path after ``::`` and drop the
    ``(N of M)`` progress suffix -> ``test/Conversion/FIRRTLToHW/foo.mlir``.
    """
    import re
    out = []
    for name in failures:
        rel = name.split("::", 1)[-1].strip()
        rel = re.sub(r"\s*\(\d+\s+of\s+\d+\)\s*$", "", rel).strip()
        if rel:
            out.append(rel if rel.startswith("test/") else f"test/{rel}")
    return out


@ChiaFunction(resources={"circt": 1})
def circt_capture_diff(ref: str = "HEAD", timeout_seconds: int = 120) -> dict:
    """Capture ``git diff <ref>`` for /workspace/circt + changed files and +/- counts.

    Returns ``{diff: str, files: list[str], added: int, removed: int}``.
    """
    def _git(args: list[str]) -> str:
        return subprocess.run(
            ["git", "-c", f"safe.directory={_CIRCT_SOURCE_TREE}", "-C", _CIRCT_SOURCE_TREE, *args],
            capture_output=True, text=True, timeout=timeout_seconds).stdout

    diff = _git(["diff", ref])
    files = [ln for ln in _git(["diff", "--name-only", ref]).splitlines() if ln]
    added = sum(1 for ln in diff.splitlines() if ln.startswith("+") and not ln.startswith("+++"))
    removed = sum(1 for ln in diff.splitlines() if ln.startswith("-") and not ln.startswith("---"))
    return {"diff": diff, "files": files, "added": added, "removed": removed}
