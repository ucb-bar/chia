"""chia.simulators.gem5 — gem5 build / run / source-state primitives.

Functions are PATH-BASED and co-located: a gem5 binary is far too large to ship
through the object store the way chipyard ships its Verilator binary as
bytes, so build / run / capture / restore all operate on a gem5 checkout on
the worker's filesystem and must land on the SAME worker.  :class:`Gem5Node`
enforces that via a placement group (see its docstring).  Portability across
workers is achieved by shipping the small source *diff*
(:meth:`Gem5Node.capture_gem5_source_state`) and rebuilding, not by moving
the binary.

Also defines :class:`Gem5ToolServer`, the LLM-facing MCP adapter over a
:class:`Gem5Node` (see :meth:`Gem5Node.spawn_tool`).  Because the tool subclasses
``ChiaTool``, importing this module pulls in the MCP/FastMCP stack.

"""

from __future__ import annotations

import os
import re
import shutil
import signal
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from enum import Enum

import ray
from ray.util.placement_group import (
    placement_group as _placement_group,
    remove_placement_group as _remove_placement_group,
)
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

from mcp.server.fastmcp import Context

from chia.base.ChiaFunction import ChiaFunction
from chia.base.tools.ChiaTool import ChiaTool


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class Gem5Isa(str, Enum):
    RISCV = "RISCV"
    ARM = "ARM"
    X86 = "X86"
    ALL = "ALL"


class Gem5Variant(str, Enum):
    OPT = "opt"      # optimized, asserts on (default)
    FAST = "fast"    # optimized, asserts off, no tracing
    DEBUG = "debug"  # unoptimized, full tracing


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Canonical stat-name candidates, tried in order; first present wins.  Override
# / extend per call via ``run_gem5(stats_keys=...)``.
DEFAULT_STATS_KEYS: dict[str, list[str]] = {
    "cycles": [
        "system.cpu.numCycles",
        "board.processor.cores.core.numCycles",
    ],
    "insts": [
        "simInsts",
        "system.cpu.committedInsts",
        "system.cpu.commit.committedInsts",
    ],
}

# O3PipeView trace summarization tunables (used by summarize_o3_pipeview).
_PIPE_VIEW_STAGES = ("fetch", "decode", "rename", "dispatch", "issue", "complete", "retire")
_PIPE_VIEW_TICK_PER_CYCLE = 1000      # ps/cycle at 1 GHz
_PIPE_VIEW_RESERVOIR_SIZE = 1000

# Number/identifier matcher for a stats.txt "<name> <value> # comment" line.
_STATS_NUMBER_RE = re.compile(
    r"^\s*([A-Za-z0-9_:\.\[\]\-]+)\s+([-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)\s*(?:#.*)?$"
)

# Build-diagnostic line matcher (keep error lines + one line of context above).
_BUILD_ERROR_RE = re.compile(
    r"(error[:\s]|undefined reference|fatal error|"
    r"in (?:static |member )?function|note:)",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class Gem5BuildArtifact:
    """Result of an scons build. Path-based: ``binary_path`` lives on the worker
    and is only reachable from a run pinned to the same node."""
    binary_path: str            # e.g. "/home/ray/gem5/build/RISCV/gem5.opt"
    isa: str                    # Gem5Isa value
    variant: str                # Gem5Variant value
    gem5_root: str
    base_rev: str               # git HEAD of gem5_root at build time ("" if not a repo)
    success: bool
    returncode: int
    build_duration_s: float
    stdout_tail: str            # last ~3 KB of build output
    stderr_tail: str            # filtered compiler/linker diagnostics on failure


@dataclass
class Gem5RunResult:
    """Result of a single gem5 invocation on one workload."""
    workload_name: str
    status: str                 # "ok" | "run_failed_<rc>" | "timeout" | "parse_failed"
    returncode: int
    outdir: str                 # --outdir (stats.txt, debug file live here)
    num_cycles: int | None
    sim_insts: int | None
    sim_seconds: float | None       # gem5-reported simulated time
    host_seconds: float | None      # gem5-reported host CPU time
    wall_s: float | None            # measured subprocess wall-clock
    stats: dict[str, float] = field(default_factory=dict)   # logical-name -> value
    stdout_tail: str = ""
    error_messages: str = ""
    stats_content: str | None = None     # full stats.txt, only if capture_stats=True
    debug_trace: bytes | None = None      # raw --debug-file bytes, only if capture_debug_trace=True


@dataclass
class Gem5SourceState:
    """Portable snapshot of edits to a gem5 checkout (ship this, not the binary)."""
    base_rev: str               # git rev the diff applies on top of
    source_diff: str            # unified diff (e.g. over src/)
    config_contents: str = ""   # optional snapshot of the gem5 config .py


# ---------------------------------------------------------------------------
# Worker-side helpers (module-level so they resolve by import on the worker)
# ---------------------------------------------------------------------------

def _run_logged(
    cmd: list[str] | str,
    cwd: str | None,
    timeout_s: int,
    env: dict[str, str] | None = None,
) -> tuple[int, str, str, bool, float]:
    """Run *cmd* in its own process group; return
    ``(returncode, stdout, stderr, timed_out, wall_s)``.

    ``start_new_session=True`` puts the whole subprocess tree in one process
    group so the timeout path can ``killpg`` every descendant — without it a
    SIGKILL to the shell leaves grandchildren (g++, gem5) holding the captured
    pipes open and the call stalls in cleanup.
    """
    t0 = time.time()
    full_env = {**os.environ, **env} if env else None
    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        shell=isinstance(cmd, str),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
        env=full_env,
    )
    try:
        stdout, stderr = proc.communicate(timeout=timeout_s)
        return proc.returncode, stdout, stderr, False, time.time() - t0
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            stdout, stderr = proc.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            stdout, stderr = "", ""
        return -1, stdout, stderr, True, time.time() - t0


def _git(args: list[str], cwd: str, timeout: int = 120) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, text=True, timeout=timeout,
    )


def _filter_build_diagnostics(stdout: str, stderr: str, max_bytes: int = 3000) -> str:
    """Keep compiler/linker error lines + one line of context above each."""
    lines = (stdout + "\n" + stderr).splitlines()
    flagged: list[str] = []
    prev_was_flag = False
    for i, line in enumerate(lines):
        if _BUILD_ERROR_RE.search(line):
            if i > 0 and not prev_was_flag and not _BUILD_ERROR_RE.search(lines[i - 1]):
                flagged.append(lines[i - 1])
            flagged.append(line)
            prev_was_flag = True
        else:
            prev_was_flag = False
    if not flagged:
        flagged = lines[-30:]
    body = "\n".join(flagged[-60:])
    return body[-max_bytes:]


def _parse_kv_stats_block(lines: list[str]) -> dict[str, float]:
    out: dict[str, float] = {}
    for line in lines:
        m = _STATS_NUMBER_RE.match(line)
        if not m:
            continue
        key, raw = m.group(1), m.group(2)
        out[key] = float(raw) if ("." in raw or "e" in raw.lower()) else float(int(raw))
    return out


def _split_stats_blocks(stats_text: str) -> list[dict[str, float]]:
    """Split stats.txt into one kv-dict per ``Begin/End Simulation Statistics``
    block (gem5 emits one per dump; ROI configs that reset at workbegin/workend
    produce several)."""
    begin, end = "Begin Simulation Statistics", "End Simulation Statistics"
    blocks: list[list[str]] = []
    cur: list[str] = []
    in_block = False
    for line in stats_text.splitlines():
        if begin in line:
            in_block, cur = True, []
            continue
        if end in line and in_block:
            in_block = False
            blocks.append(cur)
            cur = []
            continue
        if in_block:
            cur.append(line)
    return [_parse_kv_stats_block(b) for b in blocks if b]


def _pick_block(blocks: list[dict[str, float]], block_sel: int | str):
    if not blocks:
        return None
    if block_sel == "first":
        return blocks[0]
    if block_sel == "last":
        return blocks[-1]
    try:
        idx = int(block_sel)
    except (ValueError, TypeError):
        return blocks[0]
    if idx < 0:
        idx += len(blocks)
    return blocks[idx] if 0 <= idx < len(blocks) else None


# ---------------------------------------------------------------------------
# Per-instance binding wrapper
# ---------------------------------------------------------------------------

class _PinnedChiaFn:
    """Exposes a ``@ChiaFunction`` with ``.chia_remote`` pre-pinned to a
    placement-group bundle.  Resource requirements are carried over unchanged by
    ``.options()``; with no scheduling opts it delegates to the raw function so
    the caller's own placement applies."""

    def __init__(self, fn, scheduling_opts: dict):
        self._fn = fn
        self._opts = dict(scheduling_opts) if scheduling_opts else {}
        # The requested substitution: chia_remote == fn.options(<sched>).chia_remote
        self.chia_remote = (
            fn.options(**self._opts).chia_remote if self._opts else fn.chia_remote
        )

    def options(self, **overrides):
        """Layer extra Ray options on top of the node's pinning."""
        merged = {**self._opts, **overrides}
        return self._fn.options(**merged) if merged else self._fn

    def __call__(self, *args, **kwargs):
        """Local (non-Ray) invocation of the underlying function."""
        return self._fn(*args, **kwargs)


# ---------------------------------------------------------------------------
# Gem5Node
# ---------------------------------------------------------------------------

class Gem5Node:
    """gem5 build / run / source-state primitives sharing one placement.

    The four core operations are ``@staticmethod @ChiaFunction(resources=
    {"gem5": 1.0})`` members; ``__init__`` re-binds each into a per-instance
    :class:`_PinnedChiaFn` so ``node.<op>.chia_remote(...)`` lands on this node's
    bundle.  ``Gem5Node.<op>.chia_remote(...)`` (the class attribute) is the raw,
    unpinned form.

    Co-location: because gem5 binaries stay on the worker filesystem, every
    member must run on the same node.  Placement is decided once at construction:

      * ``placement_group`` given        -> pin members to ``bundle_index`` of it
        (the node will NOT release a PG it did not create).
      * none + ``require_colocated=True`` -> reserve a 1-bundle
        ``{"gem5": 1, "CPU": 1}`` PG (owned + released by this node).
      * none + ``require_colocated=False`` -> no pinning; the caller schedules
        each ``.chia_remote`` / ``.options(...)`` call itself.

    :meth:`spawn_tool` (opt-in) creates a ``Gem5ToolServer`` co-located on this
    node's bundle, exposing the same build/run logic to an LLM over MCP; spawned
    tools are stopped by :meth:`close`.

    Usable as a context manager so spawned tools are stopped and a self-reserved
    PG is released on exit.
    """

    # Names of the @ChiaFunction members re-bound per instance in __init__.
    _MEMBER_FNS = (
        "build_gem5",
        "run_gem5",
        "capture_gem5_source_state",
        "restore_gem5_source_state",
    )
    _DEFAULT_BUNDLE = {"CPU": 1, "gem5": 1.0}

    def __init__(
        self,
        placement_group=None,
        require_colocated: bool = True,
        *,
        bundle_index: int = 0,
        reserve_bundle: dict | None = None,
        pg_strategy: str = "STRICT_PACK",
        wait_for_pg: bool = True,
        pg_ready_timeout_s: float | None = None,
    ):
        """Set up placement and bind the member functions.

        Args:
            placement_group: an existing Ray ``PlacementGroup`` to schedule onto.
                If given, ``require_colocated`` is moot (placement is already
                fixed) and this node will not remove the PG on close.
            require_colocated: when no PG is given, reserve one so all members
                co-locate. When False, leave placement to the caller.
            bundle_index: which bundle of the (given or reserved) PG to pin to.
            reserve_bundle: resource shape of a self-reserved bundle
                (default ``{"CPU": 1, "gem5": 1.0}``); must provide ``gem5`` >=
                each member's requirement (1.0).
            pg_strategy: placement strategy for a self-reserved PG.
            wait_for_pg: block on ``pg.ready()`` for a self-reserved PG so the
                node is usable immediately.
            pg_ready_timeout_s: optional timeout for that wait.
        """
        self._owns_pg = False
        self._bundle_index = bundle_index

        if placement_group is not None:
            self._pg = placement_group
        elif require_colocated:
            bundle = reserve_bundle or dict(self._DEFAULT_BUNDLE)
            if bundle.get("gem5", 0) < 1.0:
                raise ValueError(
                    f"reserve_bundle must provide gem5>=1.0 for member tasks; "
                    f"got {bundle!r}"
                )
            self._pg = _placement_group([bundle], strategy=pg_strategy)
            self._owns_pg = True
            self._bundle_index = 0
            if wait_for_pg:
                ray.get(self._pg.ready(), timeout=pg_ready_timeout_s)
        else:
            self._pg = None

        if self._pg is not None:
            self._sched_opts = {
                "scheduling_strategy": PlacementGroupSchedulingStrategy(
                    placement_group=self._pg,
                    placement_group_bundle_index=self._bundle_index,
                )
            }
        else:
            self._sched_opts = {}

        # Tools spawned via spawn_tool(), stopped on close().
        self._tools: list = []

        # Re-bind each class-level @ChiaFunction into a pinned instance member:
        #   node.<fn>.chia_remote == <fn>.options(<sched>).chia_remote
        for name in self._MEMBER_FNS:
            setattr(self, name, _PinnedChiaFn(getattr(type(self), name), self._sched_opts))

    # -- placement-group lifecycle --------------------------------------------

    @property
    def placement_group(self):
        """The PG members are pinned to (None when the caller handles placement)."""
        return self._pg

    @property
    def owns_placement_group(self) -> bool:
        """True iff this node reserved its PG and will release it on close()."""
        return self._owns_pg

    @property
    def task_options(self) -> dict:
        """Scheduling opts to co-locate an actor (e.g. a ``ChiaTool``) with this
        node's bundle.  Empty when the node has no placement group
        (``require_colocated=False``) — there is no shared placement to inherit,
        so an actor given these opts would not be pinned."""
        return dict(self._sched_opts)

    def spawn_tool(
        self,
        name: str,
        *,
        gem5_root: str,
        config_script: str,
        workloads,
        **tool_kwargs,
    ):
        """Create a ``Gem5ToolServer`` co-located on this node's bundle.

        Exposes this node's gem5 build/run/stats to an LLM over MCP, pinned to
        the same worker so the tool and ``node.<op>.chia_remote(...)`` operate on
        the same on-disk gem5 checkout.  The returned tool is tracked and stopped
        by :meth:`close`.

        Requires a placement group — construct the node with
        ``require_colocated=True`` or pass ``placement_group=...``; otherwise
        there is no shared placement for the tool to join.

        ``tool_kwargs`` are forwarded to ``Gem5ToolServer`` (e.g. ``config_args``,
        ``isa``, ``variant``, ``stats_keys``, ``excluded_workloads``, timeouts).
        """
        if not self._sched_opts:
            raise RuntimeError(
                "spawn_tool needs a placement group to co-locate against; "
                "construct Gem5Node with require_colocated=True or pass "
                "placement_group=..."
            )
        tool = Gem5ToolServer(
            name,
            gem5_root=gem5_root,
            config_script=config_script,
            workloads=workloads,
            task_options=self.task_options,
            **tool_kwargs,
        )
        self._tools.append(tool)
        return tool

    def close(self) -> None:
        """Stop any spawned tools, then release the PG iff this node reserved it.

        Tools are stopped BEFORE the PG is removed: a tool's ``_ToolServerActor``
        is pinned to this PG, so tearing the PG down first would orphan it.
        Idempotent.
        """
        for tool in self._tools:
            try:
                tool.stop()
            except Exception as e:
                # Best-effort teardown; a failed tool stop shouldn't block PG release.
                self_name = getattr(tool, "name", "?")
                print(f"[Gem5Node.close] warning: failed to stop tool {self_name}: {e}")
        self._tools.clear()
        if self._owns_pg and self._pg is not None:
            _remove_placement_group(self._pg)
            self._pg = None
            self._owns_pg = False

    def __enter__(self) -> "Gem5Node":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -- nodes (gem5-resourced; pinned per-instance via __init__) -------------

    @staticmethod
    @ChiaFunction(resources={"gem5": 1.0})
    def build_gem5(
        gem5_root: str,
        isa: str = Gem5Isa.RISCV.value,
        variant: str = Gem5Variant.OPT.value,
        *,
        target: str | None = None,        # default: f"build/{isa}/gem5.{variant}"
        jobs: int | None = None,          # default: max(1, cpu_count() // 2)
        extra_scons_args: str = "",
        timeout_s: int = 3600,
    ) -> Gem5BuildArtifact:
        """Incremental scons-build a gem5 binary in ``gem5_root``.

        Returns a path-based artifact; on failure ``stderr_tail`` carries the
        filtered compiler/linker diagnostics (so this doubles as a fast compile
        check). Records ``base_rev`` (git HEAD) for later diffing.
        """
        tgt = target or f"build/{isa}/gem5.{variant}"
        njobs = jobs if jobs is not None else max(1, (os.cpu_count() or 4) // 2)
        cmd = f"scons {tgt} -j{njobs}"
        if extra_scons_args:
            cmd += f" {extra_scons_args}"

        rc, stdout, stderr, timed_out, wall = _run_logged(cmd, gem5_root, timeout_s)

        base_rev = ""
        rev = _git(["rev-parse", "HEAD"], gem5_root, timeout=30)
        if rev.returncode == 0:
            base_rev = rev.stdout.strip()

        success = (rc == 0) and not timed_out
        return Gem5BuildArtifact(
            binary_path=os.path.join(gem5_root, tgt),
            isa=isa,
            variant=variant,
            gem5_root=gem5_root,
            base_rev=base_rev,
            success=success,
            returncode=rc,
            build_duration_s=wall,
            stdout_tail=stdout[-3000:],
            stderr_tail=(
                f"TIMEOUT after {wall:.0f}s (limit {timeout_s}s)"
                if timed_out else
                ("" if success else _filter_build_diagnostics(stdout, stderr))
            ),
        )

    @staticmethod
    @ChiaFunction(resources={"gem5": 1.0})
    def run_gem5(
        gem5_bin: str,
        config_script: str,
        outdir: str,
        *,
        workload_name: str | None = None,
        config_args: list[str] | None = None,   # args AFTER the config script,
                                                 # e.g. ["--kernel", elf, "--memory-backend", "dramsim2"]
        gem5_args: list[str] | None = None,      # gem5 core args BEFORE the config script (rare)
        debug_flags: str | None = None,          # --debug-flags=... (e.g. "O3PipeView")
        debug_file: str | None = None,           # --debug-file=... (relative to outdir)
        stats_keys: dict[str, list[str]] | None = None,
        stats_block: int | str = "last",
        capture_stats: bool = False,
        capture_debug_trace: bool = False,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_s: int = 3600,
    ) -> Gem5RunResult:
        """Run a single gem5 invocation and parse cycle/instruction counters.

        Builds the command line as::

            gem5_bin [gem5_args] --outdir=<outdir> [--debug-flags=..] [--debug-file=..]
                     config_script [config_args]

        ``--kernel`` / ``--memory-backend`` etc. are config-script arguments, so
        pass them via ``config_args`` (keeps this node agnostic to any specific
        config .py). After the run, reads ``<outdir>/stats.txt`` and fills
        ``num_cycles`` / ``sim_insts`` / ``stats`` (logical names from
        ``stats_keys``, defaulting to :data:`DEFAULT_STATS_KEYS`).

        ``stats_block`` picks which dump to read ("last" = final cumulative
        dump; pass "first" for configs that reset stats at an ROI workbegin).
        """
        config_args = list(config_args or [])
        os.makedirs(outdir, exist_ok=True)

        if workload_name is None:
            if "--kernel" in config_args:
                workload_name = os.path.basename(
                    config_args[config_args.index("--kernel") + 1]
                )
            else:
                workload_name = os.path.splitext(os.path.basename(config_script))[0]

        cmd = [gem5_bin, *(gem5_args or []), f"--outdir={outdir}"]
        if debug_flags:
            cmd.append(f"--debug-flags={debug_flags}")
        if debug_file:
            cmd.append(f"--debug-file={debug_file}")
        cmd += [config_script, *config_args]

        rc, stdout, stderr, timed_out, wall = _run_logged(
            cmd, cwd=cwd, timeout_s=timeout_s, env=env,
        )

        # Parse stats.txt.
        keys = {**DEFAULT_STATS_KEYS, **(stats_keys or {})}
        parsed = Gem5Node.parse_gem5_stats_file(
            os.path.join(outdir, "stats.txt"),
            {**keys, "_sim_seconds": ["simSeconds"], "_host_seconds": ["hostSeconds"]},
            stats_block=stats_block,
        )
        num_cycles = parsed.get("cycles")
        sim_insts = parsed.get("insts")
        sim_seconds = parsed.pop("_sim_seconds", None)
        host_seconds = parsed.pop("_host_seconds", None)

        if timed_out:
            status, err = "timeout", f"timed out after {wall:.0f}s (limit {timeout_s}s)"
        elif rc != 0:
            status, err = f"run_failed_{rc}", f"gem5 rc={rc}"
        elif num_cycles is None:
            status, err = "parse_failed", "no cycle counter found in stats.txt"
        else:
            status, err = "ok", ""

        stats_content = None
        if capture_stats:
            try:
                stats_content = open(os.path.join(outdir, "stats.txt")).read()
            except OSError:
                pass

        debug_trace = None
        if capture_debug_trace and debug_file:
            try:
                debug_trace = open(os.path.join(outdir, debug_file), "rb").read()
            except OSError:
                pass

        return Gem5RunResult(
            workload_name=workload_name,
            status=status,
            returncode=rc,
            outdir=outdir,
            num_cycles=int(num_cycles) if num_cycles is not None else None,
            sim_insts=int(sim_insts) if sim_insts is not None else None,
            sim_seconds=sim_seconds,
            host_seconds=host_seconds,
            wall_s=wall,
            stats={k: v for k, v in parsed.items() if v is not None},
            stdout_tail=stdout[-3000:],
            error_messages=err,
            stats_content=stats_content,
            debug_trace=debug_trace,
        )

    @staticmethod
    @ChiaFunction(resources={"gem5": 1.0})
    def capture_gem5_source_state(
        gem5_root: str,
        *,
        base_rev: str | None = None,            # default: current HEAD
        diff_paths: list[str] | None = None,    # default: ["src/"]
        config_path: str | None = None,
    ) -> Gem5SourceState:
        """Capture a portable snapshot of the current edits in ``gem5_root``.

        Computes ``git diff <base_rev> -- <diff_paths>``, first marking untracked
        files under those paths as intent-to-add so brand-new files (e.g. a new
        SimObject's .py/.cc/.hh) are included, then un-staging them to leave the
        index clean. Optionally snapshots ``config_path``.
        """
        paths = diff_paths or ["src/"]

        if base_rev is None:
            rev = _git(["rev-parse", "HEAD"], gem5_root, timeout=30)
            base_rev = rev.stdout.strip() if rev.returncode == 0 else ""

        config_contents = ""
        if config_path:
            try:
                config_contents = open(config_path).read()
            except OSError as e:
                config_contents = f"(failed to read config: {e})"

        ls = _git(["ls-files", "--others", "--exclude-standard", "--", *paths], gem5_root)
        untracked = [p for p in ls.stdout.splitlines() if p]
        if untracked:
            _git(["add", "-N", "--", *untracked], gem5_root, timeout=60)
        try:
            diff_proc = _git(["diff", base_rev, "--", *paths], gem5_root, timeout=60)
            source_diff = diff_proc.stdout
        finally:
            if untracked:
                _git(["reset", "--quiet", "--", *untracked], gem5_root, timeout=60)

        return Gem5SourceState(
            base_rev=base_rev,
            source_diff=source_diff,
            config_contents=config_contents,
        )

    @staticmethod
    @ChiaFunction(resources={"gem5": 1.0})
    def restore_gem5_source_state(
        gem5_root: str,
        state: Gem5SourceState,
        *,
        restore_paths: list[str] | None = None,  # default: ["src/"]
        config_path: str | None = None,
    ) -> tuple[bool, str]:
        """Reset ``gem5_root`` to ``state.base_rev`` and apply ``state.source_diff``.

        ``git checkout <base_rev> -- <paths>`` + ``git clean -fd <paths>`` (to drop
        untracked leftovers from a prior apply that would collide with the next
        apply) + ``git apply``, then optionally write ``state.config_contents`` to
        ``config_path``. Caller is responsible for a subsequent ``build_gem5``.
        Returns ``(ok, message)``.
        """
        paths = restore_paths or ["src/"]

        if config_path and state.config_contents:
            try:
                open(config_path, "w").write(state.config_contents)
            except OSError as e:
                return False, f"failed to write config: {e}"

        if state.base_rev:
            co = _git(["checkout", state.base_rev, "--", *paths], gem5_root)
            if co.returncode != 0:
                return False, f"git checkout failed: {co.stderr[-500:]}"
        clean = _git(["clean", "-fd", *paths], gem5_root, timeout=60)
        if clean.returncode != 0:
            return False, f"git clean failed: {clean.stderr[-500:]}"

        if state.source_diff.strip():
            apply = subprocess.run(
                ["git", "apply", "-"],
                input=state.source_diff, cwd=gem5_root,
                capture_output=True, text=True, timeout=120,
            )
            if apply.returncode != 0:
                return False, f"git apply failed: {apply.stderr[-500:]}"

        n = len(state.source_diff.splitlines())
        return True, f"restored to {state.base_rev[:10] or 'HEAD'} + {n}-line diff"

    # -- pure helpers (no gem5 resource needed) -------------------------------

    @staticmethod
    def parse_gem5_stats(
        stats_text: str,
        stats_keys: dict[str, list[str]] | None = None,
        stats_block: int | str = "last",
    ) -> dict[str, float]:
        """Map logical name -> value from stats.txt *text*.

        ``stats_keys`` maps a logical name to ordered candidate gem5 stat names
        (first present wins); defaults to :data:`DEFAULT_STATS_KEYS`.
        ``stats_block`` selects which ``Begin/End Simulation Statistics`` dump to
        read ("first" | "last" | int index, negatives allowed).
        """
        keys = stats_keys or DEFAULT_STATS_KEYS
        block = _pick_block(_split_stats_blocks(stats_text), stats_block)
        if block is None:
            return {}
        out: dict[str, float] = {}
        for logical, candidates in keys.items():
            for cand in candidates:
                if cand in block:
                    out[logical] = block[cand]
                    break
        return out

    @staticmethod
    def parse_gem5_stats_file(
        stats_path: str,
        stats_keys: dict[str, list[str]] | None = None,
        stats_block: int | str = "last",
    ) -> dict[str, float]:
        """Like :meth:`parse_gem5_stats` but reads from a path; missing file -> {}."""
        try:
            text = open(stats_path).read()
        except OSError:
            return {}
        return Gem5Node.parse_gem5_stats(text, stats_keys, stats_block)

    @staticmethod
    def truncate_gz_trace(path: str, max_decompressed_bytes: int) -> tuple[int, bool]:
        """In-place head-truncate a gzipped trace to a decompressed-byte cap,
        ending on a line boundary; returns ``(retained_bytes, was_truncated)``.
        Stays valid gzip so downstream readers needn't know it was trimmed."""
        import gzip
        retained, truncated = 0, False
        tmp = path + ".trunc"
        try:
            with gzip.open(path, "rt", errors="replace") as fin, \
                 gzip.open(tmp, "wt") as fout:
                for line in fin:
                    if retained + len(line) > max_decompressed_bytes:
                        truncated = True
                        break
                    fout.write(line)
                    retained += len(line)
        except (OSError, EOFError):
            if os.path.exists(tmp):
                os.remove(tmp)
            return 0, False
        if truncated:
            os.replace(tmp, path)
        elif os.path.exists(tmp):
            os.remove(tmp)
        return retained, truncated

    @staticmethod
    def summarize_o3_pipeview(
        trace_path: str,
        max_first: int = 30,
        max_slowest: int = 10,
        tick_per_cycle: int = _PIPE_VIEW_TICK_PER_CYCLE,
        reservoir_size: int = _PIPE_VIEW_RESERVOIR_SIZE,
    ) -> str:
        """Stream an O3PipeView ``--debug-file`` trace into a compact markdown
        digest (per-stage wait-cycle stats, slowest instructions, first-N table).

        O3-CPU-specific. Memory stays bounded (a few tens of KB) regardless of
        trace length: percentiles come from a reservoir sample, mean/max/count
        are exact. The raw trace on disk remains the authoritative artifact.
        """
        import gzip
        import heapq
        import random

        if not os.path.exists(trace_path):
            return "(no trace produced — gem5 may have crashed before tracing)"

        open_fn = gzip.open if trace_path.endswith(".gz") else open
        pairs = list(zip(_PIPE_VIEW_STAGES, _PIPE_VIEW_STAGES[1:]))
        count = {p: 0 for p in pairs}
        total = {p: 0 for p in pairs}
        peak = {p: 0 for p in pairs}
        reservoir: dict[tuple[str, str], list[int]] = {p: [] for p in pairs}
        slowest: list = []
        first_records: list[dict] = []
        total_instructions = 0
        base_fetch: int | None = None

        def flush(r: dict) -> None:
            nonlocal base_fetch, total_instructions
            total_instructions += 1
            if base_fetch is None and "fetch" in r:
                base_fetch = r["fetch"]
            if len(first_records) < max_first:
                first_records.append(dict(r))
            for a, b in pairs:
                if a in r and b in r and r[b] >= r[a]:
                    d = (r[b] - r[a]) // tick_per_cycle
                    key = (a, b)
                    count[key] += 1
                    total[key] += d
                    peak[key] = max(peak[key], d)
                    n = count[key]
                    res = reservoir[key]
                    if n <= reservoir_size:
                        res.append(d)
                    else:
                        j = random.randint(0, n - 1)
                        if j < reservoir_size:
                            res[j] = d
            if "complete" in r and "issue" in r:
                exc = (r["complete"] - r["issue"]) // tick_per_cycle
                snap = {"sn": r.get("sn", ""), "pc": r.get("pc", ""),
                        "disasm": r.get("disasm", ""), "exec": exc}
                if len(slowest) < max_slowest:
                    heapq.heappush(slowest, (exc, total_instructions, snap))
                elif exc > slowest[0][0]:
                    heapq.heapreplace(slowest, (exc, total_instructions, snap))

        current: dict | None = None
        try:
            with open_fn(trace_path, "rt", errors="replace") as f:
                for line in f:
                    if not line.startswith("O3PipeView:"):
                        continue
                    parts = line.rstrip("\n").split(":", 3)
                    if len(parts) < 3:
                        continue
                    stage = parts[1]
                    try:
                        tick = int(parts[2])
                    except ValueError:
                        continue
                    if stage == "fetch":
                        if current is not None:
                            flush(current)
                        tail = parts[3] if len(parts) > 3 else ""
                        tp = tail.split(":", 3)
                        current = {
                            "sn": tp[2] if len(tp) > 2 else "",
                            "pc": tp[0] if tp else "",
                            "disasm": (tp[3] if len(tp) > 3 else "")[:40],
                            "fetch": tick,
                        }
                    elif current is not None and stage in _PIPE_VIEW_STAGES:
                        current[stage] = tick
                if current is not None:
                    flush(current)
        except (OSError, EOFError) as e:
            return f"(failed to parse {os.path.basename(trace_path)}: {type(e).__name__}: {e})"

        if total_instructions == 0:
            return f"(trace {os.path.basename(trace_path)} contained no O3PipeView:fetch lines)"

        def _stats(key: tuple[str, str]) -> str:
            n = count[key]
            if n == 0:
                return "—"
            sample = sorted(reservoir[key])
            m = len(sample)
            return (f"mean={total[key] / n:.2f} p50={sample[m // 2]} "
                    f"p90={sample[min(m - 1, int(m * 0.9))]} max={peak[key]} n={n}")

        lines = ["# Pipeline trace summary",
                 f"Instructions traced: {total_instructions}", "",
                 "## Per-stage wait cycles"]
        for a, b in pairs:
            lines.append(f"- **{a}->{b}**: {_stats((a, b))}")
        lines += ["", f"## Top {max_slowest} instructions by issue->complete cycles"]
        for _exc, _seq, rec in sorted(slowest, key=lambda t: t[0], reverse=True):
            lines.append(f"- sn={rec['sn']} pc={rec['pc']} exec={rec['exec']}c  {rec['disasm']}")
        lines += ["", f"## First {max_first} instructions (cycles relative to first fetch)",
                  "| sn | pc | F | D | R | Dp | I | C | Rt | disasm |",
                  "|----|----|---|---|---|----|---|---|----|--------|"]
        base = base_fetch if base_fetch is not None else 0

        def _cyc(r: dict, k: str) -> str:
            return "" if k not in r else str((r[k] - base) // tick_per_cycle)

        for r in first_records:
            lines.append("| " + " | ".join([
                r["sn"], r["pc"],
                _cyc(r, "fetch"), _cyc(r, "decode"), _cyc(r, "rename"),
                _cyc(r, "dispatch"), _cyc(r, "issue"), _cyc(r, "complete"),
                _cyc(r, "retire"), r["disasm"],
            ]) + " |")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# LLM-facing MCP adapter over Gem5Node
# ---------------------------------------------------------------------------

async def _run_with_keepalive(ctx: Context, fn, *args, interval: float = 30.0, **kwargs):
    """Run blocking *fn* in a thread, emitting periodic ``info`` notifications via
    *ctx* so the SSE response stream stays active.

    Without these heartbeats the claude CLI's idle timer (~5 min) abandons the
    stream and reconnects, and the late tool result lands on the abandoned
    stream and is silently dropped.  Constant text == a consistent keepalive
    marker for the model.
    """
    import asyncio

    async def _heartbeat():
        try:
            while True:
                await asyncio.sleep(interval)
                try:
                    await ctx.info("Tool still running")
                except Exception:
                    pass
        except asyncio.CancelledError:
            pass

    hb = asyncio.create_task(_heartbeat())
    try:
        return await asyncio.to_thread(fn, *args, **kwargs)
    finally:
        hb.cancel()
        try:
            await hb
        except asyncio.CancelledError:
            pass


class Gem5ToolServer(ChiaTool):
    """LLM-facing MCP tool server exposing :class:`Gem5Node`'s build/run/stats.

    A thin *adapter* over the node , not a reimplementation:

      * Each method calls the corresponding :class:`Gem5Node` function
        **locally** (a ``@ChiaFunction`` invoked directly runs in-process — no
        extra Ray hop), so one battle-tested build/run implementation serves both
        the programmatic node path and this agentic path.
      * Operational arguments (``gem5_root``, ``gem5_bin``, ``config_script``,
        timeouts, …) are bound at construction, NOT exposed to the LLM — the
        model only chooses *what to run*, never *where/how* the environment is
        wired.
      * Results are rendered to compact text (the dataclasses are for code, not
        for a model to read), and long-running calls emit SSE keepalives so the
        CLI's idle timer doesn't abandon the stream and drop the result.

    Co-location: the tool runs build/run against ``gem5_root`` on the worker's
    filesystem, so pin the actor to the same bundle as whatever bash tool the LLM
    edits source through, via ``task_options`` (exactly as the loop pins
    ``QuickRunTool``).  :meth:`Gem5Node.spawn_tool` wires this up automatically.

    LLM-facing methods:
      - ``{name}_build()`` — incrementally rebuild gem5 from the current on-disk
        source and report OK / compiler errors (a fast compile check).
      - ``{name}_run(workloads, skip_build=False)`` — build (unless skipping) and
        run 1..N workloads, returning a small cycles/insts/status table.
      - ``{name}_stats(workload, pattern)`` — grep that workload's most recent
        ``stats.txt`` for a regex (drill into gem5 counters).
      - ``{name}_list_workloads()`` — list the workloads available here.

    Pass ``expose=(...)`` to register only a subset of the four tools — e.g.
    ``expose=("build",)`` makes this server a pure compile-check tool.
    """

    def __init__(
        self,
        name: str,
        gem5_root: str,
        config_script: str,
        workloads: dict[str, str] | str,
        *,
        gem5_bin: str | None = None,
        isa: str = Gem5Isa.RISCV.value,
        variant: str = Gem5Variant.OPT.value,
        config_args: list[str] | None = None,     # appended to every run (e.g. ["--memory-backend","dramsim2"])
        kernel_flag: str = "--kernel",            # how this config takes the workload ELF
        stats_keys: dict[str, list[str]] | None = None,
        stats_block: int | str = "last",
        excluded_workloads: set[str] | None = None,
        workload_glob: str = "*.elf",             # used only when `workloads` is a directory
        max_workloads_per_call: int = 5,
        build_timeout_s: int = 3600,
        run_timeout_per_workload_s: int = 3600,
        expose: tuple[str, ...] | None = None,
        task_options: dict | None = None,
    ):
        super().__init__(name, task_options=task_options)

        self.gem5_root = gem5_root
        self.config_script = config_script
        self.isa = isa
        self.variant = variant
        self.gem5_bin = gem5_bin or os.path.join(gem5_root, f"build/{isa}/gem5.{variant}")
        self.config_args = list(config_args or [])
        self.kernel_flag = kernel_flag
        self.stats_keys = stats_keys
        self.stats_block = stats_block
        self.excluded_workloads = excluded_workloads or set()
        self._workloads_spec = workloads
        self._workload_glob = workload_glob
        self.max_workloads_per_call = max_workloads_per_call
        self.build_timeout_s = build_timeout_s
        self.run_timeout_per_workload_s = run_timeout_per_workload_s

        # Per-tool scratch dir so callers can re-query stats between calls.
        self._scratch_root = tempfile.mkdtemp(prefix=f"gem5tool_{name}_")

        # Which MCP tools to register.  Default: all four.  Pass a subset (e.g.
        # ``expose=("build",)``) to use this server as just a compile-checker
        # without surfacing run/stats/list_workloads to the model.
        _registry = {
            "build": self.build,
            "run": self.run,
            "stats": self.stats,
            "list_workloads": self.list_workloads,
        }
        selected = tuple(_registry) if expose is None else tuple(expose)
        unknown = [t for t in selected if t not in _registry]
        if unknown:
            raise ValueError(
                f"Gem5ToolServer expose={expose!r}: unknown tool(s) {unknown}; "
                f"valid options are {sorted(_registry)}"
            )
        for t in selected:
            self.mcp.add_tool(_registry[t], name=f"{name}_{t}")
        super().__post_init__()

    # -- workload resolution --------------------------------------------------

    def _available(self) -> dict[str, str]:
        """Map workload name -> ELF path, honoring exclusions."""
        if isinstance(self._workloads_spec, dict):
            items = self._workloads_spec.items()
        else:
            import glob
            items = (
                (os.path.basename(p).split(".")[0], p)
                for p in sorted(glob.glob(os.path.join(self._workloads_spec, self._workload_glob)))
            )
        return {n: p for n, p in items if n not in self.excluded_workloads}

    # -- MCP methods ----------------------------------------------------------

    async def build(self, ctx: Context) -> str:
        """Incrementally rebuild gem5 from the current on-disk source and report
        the result.

        Use after a meaningful source edit to catch compile errors *before*
        spending a run on a binary that won't build.  Returns ``OK (<secs>)`` on
        a clean build, or ``FAIL ...`` followed by the relevant compiler/linker
        diagnostics.  A ``TIMEOUT`` is inconclusive — the build did not finish in
        time, NOT proof your code is broken; narrow your edit or try again.
        """
        art = await _run_with_keepalive(
            ctx, Gem5Node.build_gem5, self.gem5_root,
            isa=self.isa, variant=self.variant, timeout_s=self.build_timeout_s,
        )
        return self._render_build(art)

    async def run(
        self,
        workloads: list[str],
        ctx: Context,
        skip_build: bool = False,
    ) -> str:
        """Build (unless ``skip_build``) and run 1..N workloads against the current
        source/config, returning a markdown table of cycles + instructions +
        status.

        Set ``skip_build=True`` to reuse the existing binary when you've only
        changed the gem5 *config*, not source.  After this returns, call
        ``{name}_stats("<workload>", "<regex>")`` to drill into any workload's
        gem5 counters.  Run at most ``max_workloads_per_call`` per call.
        """
        if not workloads:
            return "ERROR: no workloads provided"
        if len(workloads) > self.max_workloads_per_call:
            return (f"ERROR: {len(workloads)} requested but "
                    f"max_workloads_per_call={self.max_workloads_per_call}; narrow the list")
        available = self._available()
        unknown = [w for w in workloads if w not in available]
        if unknown:
            return (f"ERROR: unknown workload(s): {unknown}. "
                    f"Use {self.name}_list_workloads to see the full list.")

        return await _run_with_keepalive(
            ctx, self._build_and_run_sync, workloads, available, skip_build,
        )

    def stats(self, workload: str, pattern: str, max_lines: int = 40) -> str:
        """Grep the most recent ``run`` of *workload* for stats.txt lines matching
        the regex *pattern* (capped at *max_lines*).

        Use to read gem5 internal counters (e.g. ``IntDiv|FuncUnit``,
        ``iqFullEvents|blockedCycles``, ``dcache.demand``, ``IPC|CPI``).
        """
        stats_path = os.path.join(self._scratch_root, workload, "stats.txt")
        if not os.path.isfile(stats_path):
            return (f"ERROR: no stats.txt for {workload!r}; run "
                    f"`{self.name}_run([\"{workload}\"])` first, then re-query.")
        try:
            rx = re.compile(pattern)
        except re.error as e:
            return f"ERROR: bad regex {pattern!r}: {e}"
        out: list[str] = []
        with open(stats_path) as f:
            for line in f:
                if rx.search(line):
                    out.append(line.rstrip())
                    if len(out) >= max_lines:
                        break
        return "\n".join(out) if out else f"(no lines in {workload}/stats.txt match {pattern!r})"

    def list_workloads(self) -> str:
        """List the workloads available on this worker (excluded ones hidden)."""
        names = sorted(self._available())
        if not names:
            return "(no workloads staged on this worker)"
        return f"{len(names)} available:\n" + "\n".join(f"  {n}" for n in names)

    # -- sync workers (run inside the keepalive thread) -----------------------

    def _build_and_run_sync(
        self, workloads: list[str], available: dict[str, str], skip_build: bool,
    ) -> str:
        log: list[str] = []
        if not skip_build:
            art = Gem5Node.build_gem5(
                self.gem5_root, isa=self.isa, variant=self.variant,
                timeout_s=self.build_timeout_s,
            )
            log.append(f"build: {art.build_duration_s:.0f}s rc={art.returncode}")
            if not art.success:
                return f"BUILD FAIL ({art.build_duration_s:.0f}s):\n{art.stderr_tail}\n(no workloads were run)"

        rows = ["| workload | cycles | insts | sim_s | status |",
                "|----------|--------|-------|-------|--------|"]
        for w in workloads:
            outdir = os.path.join(self._scratch_root, w)
            if os.path.isdir(outdir):
                shutil.rmtree(outdir)
            res = Gem5Node.run_gem5(
                self.gem5_bin, self.config_script, outdir,
                workload_name=w,
                config_args=[self.kernel_flag, available[w], *self.config_args],
                stats_keys=self.stats_keys,
                stats_block=self.stats_block,
                timeout_s=self.run_timeout_per_workload_s,
            )
            log.append(f"run {w}: {res.wall_s:.0f}s status={res.status}")
            sim_s = f"{res.sim_seconds:.6f}" if res.sim_seconds is not None else "?"
            rows.append(
                f"| {w} | {res.num_cycles if res.num_cycles is not None else '?'} "
                f"| {res.sim_insts if res.sim_insts is not None else '?'} "
                f"| {sim_s} | {res.status} |"
            )
        footer = (
            f"\n\nFollow-up: call `{self.name}_stats(\"<workload>\", \"<regex>\")` "
            "to read gem5 stats.txt for any workload above."
        )
        return (f"### gem5 run ({len(workloads)} workload(s))\n\n"
                + "\n".join(log) + "\n\n" + "\n".join(rows) + footer)

    # -- rendering ------------------------------------------------------------

    @staticmethod
    def _render_build(art: "Gem5BuildArtifact") -> str:
        if art.success:
            return f"OK ({art.build_duration_s:.0f}s)"
        if art.stderr_tail.startswith("TIMEOUT"):
            return (f"TIMEOUT after {art.build_duration_s:.0f}s — build did not "
                    f"complete; treat as inconclusive, not a compile failure.")
        return f"FAIL ({art.build_duration_s:.0f}s, rc={art.returncode}):\n{art.stderr_tail}"
