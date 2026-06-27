"""gem5-to-BOOM alignment loop (config + source).

Iteratively runs gem5 microbenchmarks against cached verilator results, uses
LLMs to analyze mismatches, and tunes the gem5 configuration file **and/or
source code** to match the target BOOM config (selected via ``BUILD_CONFIG``).

Parallel Execution Model
------------------------

The head runs ``N`` iterations concurrently — one per physical gem5 node.
A single placement group with ``N`` ``STRICT_SPREAD`` bundles pins each
bundle to a distinct node; a dedicated ``gem5_src_bash`` actor lives on
each bundle.  A ``ThreadPoolExecutor(max_workers=N)`` on the head drives
one iteration per bundle.  When an iteration finishes, that bundle is
freed and the main thread dispatches another iteration to it, picking a
fresh parent from the top-2 entries in the alignment DB (uniform random).

Per-Iteration Flow (one bundle)
-------------------------------

  HEAD THREAD (one per bundle)
    |
    +-- sample parent uniformly from DB.top_k_entries(2)
    +-- restore_gem5_state(parent.config, parent.diff)  [gem5 bundle]
    +-- rebuild_gem5()                                   [gem5 bundle]
    |
    +-- align_node(..., session_id=uuid4(), ...)         [llm worker]
    |     (analyzes parent's results, reads BOOM source, edits config/source)
    |
    +-- rebuild_gem5()                                   [gem5 bundle]
    +-- run_gem5_comparison()                            [gem5 bundle]
    |     (debug_node loop on failures; debug reuses align's session_id)
    |
    +-- return IterationResult; head thread persists to DB + logs, then
        dispatches next iteration onto the freed bundle.

Nodes & Resources
-----------------
  [align_node / debug_node]   dispatch ClaudeCodeLLM.prompt (chia.models.claude)
                              with resources={"llm": 1.0} onto the llm workers;
                              Ray schedules by the "llm" resource.
  gem5_src_bash_{i} (actor)   one per gem5 bundle; HTTP MCP server for LLM
                              bash access to /home/ray/gem5/src and the
                              in-container config.
  chipyard_bash (actor)       read-only, shared across bundles.

gem5 build / source-state / trace primitives are the canonical
``chia.simulators.gem5.Gem5Node`` (shipped to every worker via Ray ``py_modules``
so the version is the head's checkout, not whatever the image baked in); see
config.py for all tunable paths/knobs and README.md for setup.

Usage
-----
  cd <repo>/chia
  export GEM5_ALIGN_BENCH_ROOT=/path/to/microbench/checkout   # see README.md
  python -m chia.cli.main up examples/gem5_align/cluster.yaml
  ray job submit -- python examples/gem5_align/gem5_align_loop.py
  python -m chia.cli.main down examples/gem5_align/cluster.yaml
"""

from __future__ import annotations

import concurrent.futures
import csv
import json
import os
import random
import re
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from uuid import uuid4

# Ensure local modules (alignment_db, config) are importable regardless of cwd
# when this file is run as a script via `ray job submit`.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import ray
from ray.util.placement_group import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

from chia.base.ChiaFunction import ChiaFunction, get
from chia.models.claude import ClaudeCodeLLM
from chia.base.tools.BashTool import BashTool
from chia.base.tools.ChiaTool import ChiaTool
from mcp.server.fastmcp import Context
from chia.trace import MetricsLogger
from chia.trace.profiler import start_collector
from chia.chipyard.chisel_build_node import ChiselBuildNode
from chia.chipyard.verilator_run_node import VerilatorRunNode
from chia.chipyard.state_def import BuildArtifact, BuildTarget, RunResult
from chia.simulators.gem5 import Gem5Node, Gem5SourceState, Gem5ToolServer
from chia.database import SQLiteQueryTool

from alignment_db import AlignmentDB
from config import (
    BUILD_CONFIG, CONFIG_SLUG,
    GEM5_CONFIG, MICROBENCH, UBENCH_BUILD, VERILATOR_CACHE, RUN_COMPARE,
    LOG_DIR, EXCLUDED_BENCHMARKS, DEBUG_MAX_RETRIES, MAX_PARALLEL_ITERATIONS,
    METRICS_CONFIG, PIPE_TRACE_MAX_DECOMPRESSED_BYTES,
    GEM5_ISA, GEM5_VARIANT, GEM5_BIN,
    GEM5_CONTAINER_SRC, GEM5_CONTAINER_ROOT,
    WORKER_BENCH_ROOT, WORKER_RUN_COMPARE, WORKER_CONFIG,
    WORKER_GEM5_BASE_REV, WORKER_PARENT_TRACES, PIPE_TRACE_FILENAME,
    CHIPYARD_PATH, BOOM_SRC, CHIPYARD_GENERATORS,
)

# Ship the head's chia package to workers via py_modules so every task/tool
# imports THIS chia (incl. chia.simulators.gem5) regardless of what each worker
# image baked in.  Resolve it from the importable chia package — NOT __file__ —
# so this still works when the loop runs from a `ray job submit` working_dir
# snapshot (where __file__ lives under runtime_resources/, not the repo).
import chia
# chia is a PEP 660 editable / namespace package (chia.__file__ is None), so take
# the package directory from __path__.  Depending on the setuptools version,
# __path__ can list BOTH the repo root and the package dir (and a finder-hook
# entry); pick the one that actually IS the chia package — the dir containing the
# `base` subpackage — never the repo root or the finder hook.
_CHIA_PKG = next(
    Path(p).resolve() for p in chia.__path__
    if Path(p).is_dir() and (Path(p) / "base").is_dir()
)
_RUNTIME_ENV = {
    "py_modules": [str(_CHIA_PKG)],
    # excludes applies to runtime-env uploads (py_modules + working_dir).
    "excludes": ["**/__pycache__", "**/*.pyc"],
}


# ---------------------------------------------------------------------------
# Write-restricted BashTool (allowlist-based)
# ---------------------------------------------------------------------------

class RestrictedBashTool(BashTool):
    """BashTool that unconditionally blocks ``git commit`` and optionally
    runs in read-only mode (all write-looking commands rejected).

    Source edits live in the diff captured by the alignment loop, not in
    repo history, so ``git commit`` is always blocked.  Read-only mode is
    used for the chipyard channel where the LLM should inspect BOOM source
    but not mutate it.  The gem5 channel leaves ``read_only`` off so the
    LLM can freely edit anywhere in the gem5 tree.
    """

    # Shell noise that looks like a redirect but never writes to a real file.
    # Stripped before running the write-marker heuristic so idioms like
    # ``find ... 2>/dev/null`` aren't mistaken for a file write.
    _DISCARD_REDIRECT_RE = re.compile(
        r"""(?:
            \d?>\s*/dev/null        # `>/dev/null`, `2>/dev/null`, `1>/dev/null`
          | &>\s*/dev/null          # `&>/dev/null`
          | 2>&1                    # merge stderr into stdout
          | >&\d                    # `>&2` etc.
        )""",
        re.VERBOSE,
    )

    # Real write markers, checked after discards are stripped.  ``>`` on its
    # own catches any remaining file-targeted redirect.
    _WRITE_MARKERS = (
        ">", ">>", "tee ", "sed -i", "mv ", "cp ", "rm ",
        "chmod ", "touch ", "mkdir ",
    )

    def __init__(self, name: str, read_only: bool = False, **kwargs):
        self._read_only = read_only
        super().__init__(name=name, **kwargs)

    def _is_read_only(self, command: str) -> tuple[bool, str]:
        """Return ``(is_read_only, trigger)`` where ``trigger`` names the
        marker that flagged the command as a write (empty if read-only)."""
        cleaned = self._DISCARD_REDIRECT_RE.sub("", command)
        for marker in self._WRITE_MARKERS:
            if marker in cleaned:
                return False, marker
        return True, ""

    def run_command(self, command: str) -> str:
        # Always block git commits — the loop captures your diff automatically.
        if "git commit" in command:
            return ("BLOCKED: git commit is not allowed. Edit files directly; "
                    "the alignment loop captures your diff automatically.")

        if self._read_only:
            read_only, trigger = self._is_read_only(command)
            if not read_only:
                return (
                    f"BLOCKED: this bash tool is read-only (flagged on "
                    f"{trigger!r}).  stderr-silencing redirects "
                    f"(`2>/dev/null`, `>/dev/null`, `2>&1`) are fine in "
                    f"pure-read commands."
                )
        return super().run_command(command)


# ---------------------------------------------------------------------------
# Read-only SQL tool over the alignment DB
# ---------------------------------------------------------------------------
# The LLM's read-only SQL access to alignment.db is the canonical
# chia.database.SQLiteQueryTool, spawned co-located with the DB by
# AlignmentDB.spawn_query_tool("align_db") in run_alignment_loop (AlignmentDB is
# a chia.database.SQLiteNode subclass — see alignment_db.py).  It reaches the
# LLM worker via the chia py_modules runtime env, so it no longer needs to live
# inline here for cloudpickle-by-value (the bash/db tools that DO — see
# RestrictedBashTool — stay in __main__).


async def _run_with_keepalive(ctx, fn, *args, interval: float = 30.0, **kwargs):
    """Run blocking *fn* in a thread, emitting periodic ``Tool still running``
    notifications via *ctx* so the SSE response stream stays active.

    Without these heartbeats the claude CLI's idle timer (~5 min) abandons the
    response stream and reconnects, but the late tool result then lands on the
    abandoned stream and is silently dropped.  An ``info`` notification every
    ``interval`` seconds resets that timer.  The notification text is
    intentionally constant so the LLM sees a consistent keepalive marker.
    """
    import asyncio

    async def _heartbeat():
        try:
            while True:
                await asyncio.sleep(interval)
                try:
                    await ctx.info("Tool still running")
                except Exception:
                    # Notification failures shouldn't kill the tool call.
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


# The LLM's incremental compile-check tool is the canonical
# ``chia.simulators.gem5.Gem5ToolServer`` exposing ONLY its ``build`` tool
# (``expose=("build",)``) — a keepalive-wrapped ``Gem5Node.build_gem5`` that
# renders OK / FAIL / TIMEOUT exactly like the old bespoke CompileCheckTool, but
# canonical and shipped via py_modules.  It is spawned per bundle via
# ``Gem5Node.spawn_tool`` in ``run_alignment_loop`` (run/stats/list_workloads are
# intentionally NOT exposed — the %diff-bearing QuickRunTool owns runs).


class QuickRunTool(ChiaTool):
    """Standalone gem5 run on a benchmark subset, against the LLM's
    in-progress source state.

    The full alignment-loop iteration runs *all* benchmarks (~25-40 min on
    a slow worker) and only fires once per dispatch; this tool lets the
    aligning LLM run **1-5 chosen benchmarks** in 1-3 minutes against the
    current on-disk source so it can:

      - verify a structural change actually moves the targeted benchmark
        before committing a full iteration to it,
      - empirically check operand assumptions (e.g. "what divisor does
        this bench actually use?") via gem5 stats,
      - cross-check gem5's per-benchmark counters against verilator's
        TMA counters (in `/home/ray/bench_workspace/verilator/<B>.out`)
        before forming a hypothesis.

    Each LLM iteration (placement-group bundle) deploys its own
    QuickRunTool actor, pinned to that bundle's gem5 resources via
    ``task_options``.  The bundle owns ``gem5: 1.0`` total; the
    align/debug LLM phase doesn't otherwise consume gem5 capacity, so
    QuickRunTool's runs are non-blocking with the rest of the loop.

    Workflow:
      1. ``run(benchmarks=["MCS"])`` — does ``scons`` (incremental) +
         ``run_compare.py --benchmarks MCS`` and returns a small markdown
         table with cycles + ratio + status.  Pass ``skip_build=True``
         to reuse the binary already on disk (e.g. when only changing
         the gem5 *config* file, not source).
      2. ``stats(benchmark, pattern)`` — grep the just-produced
         ``stats.txt`` for matching lines (e.g. ``"FuncUnit|IntDiv|
         iqFullEvents|blockedCycles"``).  Use this to verify a
         hypothesis with gem5's internal counters.
      3. ``list_benchmarks()`` — list available benchmarks on this
         worker.

    Defined inline in __main__ so it cloudpickles by value to LLM
    workers (see RestrictedBashTool for the same rationale).
    """

    def __init__(
        self,
        name: str,
        gem5_root: str,
        gem5_bin: str,
        bench_workspace: str,
        run_compare_path: str,
        gem5_config: str,
        excluded_benchmarks: set[str] | None = None,
        max_benchmarks_per_call: int = 5,
        default_timeout_per_bench_s: int = 3600,
        build_timeout_s: int = 3600,
        task_options: dict | None = None,
    ):
        super().__init__(name, task_options=task_options)
        self.gem5_root = gem5_root
        self.gem5_bin = gem5_bin
        self.bench_workspace = bench_workspace
        self.run_compare_path = run_compare_path
        self.gem5_config = gem5_config
        self.excluded_benchmarks = excluded_benchmarks or set()
        self.max_benchmarks_per_call = max_benchmarks_per_call
        self.default_timeout_per_bench_s = default_timeout_per_bench_s
        self.build_timeout_s = build_timeout_s
        # Per-tool scratch dir for results so callers can re-query stats
        # between calls without re-running.
        import tempfile
        self._scratch_root = tempfile.mkdtemp(prefix=f"quickrun_{name}_")
        self.mcp.add_tool(self.run, name=f"{name}_run")
        self.mcp.add_tool(self.stats, name=f"{name}_stats")
        self.mcp.add_tool(self.list_benchmarks,
                          name=f"{name}_list_benchmarks")
        super().__post_init__()

    # ----- helpers ---------------------------------------------------------

    def _ubench_dir(self) -> str:
        import os
        return os.path.join(self.bench_workspace, "build", "ubench")

    def _verilator_dir(self) -> str:
        import os
        return os.path.join(self.bench_workspace, "verilator")

    def _available_benches(self) -> list[str]:
        import os
        ubench = self._ubench_dir()
        out: list[str] = []
        if os.path.isdir(ubench):
            for fn in os.listdir(ubench):
                if fn.endswith(".gem5.elf"):
                    name = fn[:-len(".gem5.elf")]
                    if name not in self.excluded_benchmarks:
                        out.append(name)
        return sorted(out)

    def _build(self, log_lines: list[str]) -> tuple[bool, str]:
        """Incremental scons via the canonical Gem5Node.build_gem5.

        Returns ``(ok, diagnostic_tail)``; on failure the tail is build_gem5's
        filtered compiler/linker diagnostics (or its TIMEOUT message).
        """
        art = Gem5Node.build_gem5(
            self.gem5_root, isa=GEM5_ISA, variant=GEM5_VARIANT,
            extra_scons_args="--keep-going", timeout_s=self.build_timeout_s,
        )
        log_lines.append(f"build: {art.build_duration_s:.1f}s rc={art.returncode}")
        if art.success:
            return True, ""
        return False, art.stderr_tail

    # ----- MCP-exposed methods --------------------------------------------

    def list_benchmarks(self) -> str:
        """List the benchmarks available on this worker (excluded ones
        are hidden).  Use this if you want to confirm a benchmark name
        before calling ``run``.
        """
        names = self._available_benches()
        if not names:
            return "(no benchmarks staged on this worker)"
        return f"{len(names)} available:\n" + "\n".join(f"  {n}" for n in names)

    def _invoke_run_compare(self, cmd: list[str], wall_timeout: int):
        """Run ``run_compare.py`` in its own process group; return
        ``(stdout, stderr, returncode)`` or raise ``TimeoutError`` on timeout.
        Process-group kill prevents grandchildren (gem5 procs spawned by
        run_compare) from holding the captured pipes open.
        """
        import os, signal, subprocess
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        try:
            stdout, stderr = proc.communicate(timeout=wall_timeout)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                proc.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                pass
            raise TimeoutError(wall_timeout)
        return stdout, stderr, proc.returncode

    async def run(
        self,
        benchmarks: list[str],
        ctx: Context,
        skip_build: bool = False,
        timeout_per_bench_s: int | None = None,
    ) -> str:
        """Run a small subset of benchmarks (max ``max_benchmarks_per_call``)
        on this worker against the current source/config state and return a
        markdown table of cycles + ratio + status.

        ``skip_build`` reuses the existing gem5.opt — set this when
        you've only changed the config file and not gem5 source, to
        avoid unnecessary scons time.

        After this returns, follow up with ``stats(benchmark, pattern)``
        on any benchmark in your list to query gem5's internal counters
        (``stats.txt``) for that run.

        Per-bench timeout is ``timeout_per_bench_s`` (default
        ``default_timeout_per_bench_s``); total wall time is bounded by
        the tool's own timeout window.
        """
        import json
        import os
        import shutil
        import time

        log: list[str] = []

        # Validate benchmark list.
        if not benchmarks:
            return "ERROR: no benchmarks provided"
        if len(benchmarks) > self.max_benchmarks_per_call:
            return (f"ERROR: {len(benchmarks)} benchmarks requested but "
                    f"max_benchmarks_per_call={self.max_benchmarks_per_call}; "
                    f"narrow the list and re-run")
        available = set(self._available_benches())
        unknown = [b for b in benchmarks if b not in available]
        if unknown:
            return (f"ERROR: benchmark(s) not available on this worker: "
                    f"{unknown}.  Use {self.name}_list_benchmarks to see "
                    f"the full list.")

        # 1. Optional incremental build.
        if not skip_build:
            t0 = time.time()
            ok, tail = await _run_with_keepalive(ctx, self._build, log)
            if not ok:
                return (f"BUILD FAIL ({time.time() - t0:.1f}s):\n{tail}\n"
                        f"(no benchmarks were run)")

        # 2. Set up a fresh results dir under the per-tool scratch root and
        #    copy verilator goldens into <results_dir>/verilator/ so
        #    run_compare.py --gem5-only can compute pct_diff.
        results_dir = os.path.join(self._scratch_root, "results")
        if os.path.isdir(results_dir):
            shutil.rmtree(results_dir)
        os.makedirs(results_dir, exist_ok=True)
        ver_target = os.path.join(results_dir, "verilator")
        os.makedirs(ver_target, exist_ok=True)
        ver_src = self._verilator_dir()
        for b in benchmarks:
            src = os.path.join(ver_src, f"{b}.log")
            if os.path.isfile(src):
                shutil.copy2(src, os.path.join(ver_target, f"{b}.log"))

        # 3. Invoke run_compare.py --gem5-only on the subset.
        per_bench = timeout_per_bench_s or self.default_timeout_per_bench_s
        # Run sequentially (jobs=1) to avoid one bench's gem5 starving
        # another when the bundle has only 1 CPU.  For LLM diagnostic
        # runs of 1-5 benches that's cheap.
        cmd = [
            "python3", self.run_compare_path,
            "--gem5-bin", self.gem5_bin,
            "--gem5-config", self.gem5_config,
            "--baremetal-dir", self.bench_workspace,
            "--build-ubench-dir", self._ubench_dir(),
            "--results-dir", results_dir,
            "--gem5-only", "--skip-build",
            "--memory-backend", "dramsim2", "--jobs", "1",
            "--benchmarks", *benchmarks,
        ]
        # Per-bench timeout * count + 60s padding for run_compare overhead.
        wall_timeout = per_bench * len(benchmarks) + 60
        t0 = time.time()
        try:
            stdout, stderr, rc = await _run_with_keepalive(
                ctx, self._invoke_run_compare, cmd, wall_timeout,
            )
        except TimeoutError:
            return (f"TIMEOUT after {wall_timeout}s for "
                    f"{len(benchmarks)} bench(es); narrow the list or "
                    f"reduce timeout_per_bench_s")
        dur = time.time() - t0
        log.append(f"run: {dur:.1f}s rc={rc} "
                   f"benches={len(benchmarks)}")

        json_path = os.path.join(results_dir, "compare_results.json")
        if not os.path.isfile(json_path):
            tail = (stdout + "\n" + stderr)[-1500:]
            return (f"FAIL ({dur:.1f}s): no compare_results.json produced.\n"
                    f"--- run_compare tail ---\n{tail}")
        try:
            results = json.loads(open(json_path).read())
        except Exception as e:
            return f"FAIL: could not parse compare_results.json: {e}"

        # 4. Render compact markdown table.
        rows = ["| benchmark | gem5_cy | ver_cy | ratio | %diff | status |",
                "|-----------|---------|--------|-------|-------|--------|"]
        for r in results:
            pct = r.get("percentage_diff")
            ratio = r.get("gem5/ver_cycle_ratio")
            rows.append(
                f"| {r.get('benchmark', '?')} "
                f"| {r.get('gem5_numCycles', '?')} "
                f"| {r.get('verilator_numCycles', '?')} "
                f"| {f'{ratio:.3f}' if isinstance(ratio, (int, float)) else '?'} "
                f"| {f'{pct:.2f}%' if isinstance(pct, (int, float)) else '?'} "
                f"| {r.get('gem5_status', '?')} |"
            )
        # Average |%diff| over ok rows for quick orientation.
        diffs = [abs(r["percentage_diff"]) for r in results
                 if r.get("percentage_diff") is not None
                 and r.get("gem5_status") == "ok"]
        avg = sum(diffs) / len(diffs) if diffs else float("nan")
        header = (
            f"### QuickRun result ({len(results)} bench(es), "
            f"avg|%|={avg:.2f}%)\n\n" + "\n".join(log) + "\n\n"
        )
        # Stats hint so the LLM knows how to drill in.
        footer = (
            "\n\nFollow-up: call `{tool}_stats(\"<bench>\", \"<regex>\")` "
            "to query gem5 stats.txt for any of the above benches.  "
            "Useful patterns: `IntDiv|FuncUnit`, `iqFullEvents|"
            "blockedCycles`, `dcache.demand`, `l2.demand`, `IPC|CPI`."
        ).replace("{tool}", self.name)
        return header + "\n".join(rows) + footer

    def stats(self, benchmark: str, pattern: str,
              max_lines: int = 40) -> str:
        """Grep ``stats.txt`` for the benchmark's most recent ``run`` call
        and return matching lines (capped at ``max_lines``).

        Use to query gem5 internal counters that map to BOOM's TMA
        counters (e.g. ``divider_active`` ↔ ``FuncUnit.*IntDiv``,
        ``stq_full`` ↔ ``rename.SQFullEvents``,
        ``int_iq_full`` ↔ ``iqFullEvents``,
        ``l1d_miss_pending`` ↔ ``dcache.blockedCycles::no_mshrs``).

        Pattern is a regex applied per-line.  Use ``|`` for alternation:
          ``"system\\.cpu\\.numCycles|simInsts|cpu\\.cpi|cpu\\.ipc"``
        """
        import os
        import re
        results_dir = os.path.join(self._scratch_root, "results")
        stats_path = os.path.join(results_dir, "gem5", benchmark, "stats.txt")
        if not os.path.isfile(stats_path):
            return (f"ERROR: no stats.txt for benchmark {benchmark!r} on "
                    f"this worker.  Run `{self.name}_run([\"{benchmark}\"])` "
                    f"first, then re-query.")
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
        if not out:
            return f"(no lines in {benchmark}/stats.txt match {pattern!r})"
        return "\n".join(out)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
# All tunable paths/knobs live in config.py (imported at the top of this file):
# BUILD_CONFIG / CONFIG_SLUG, the host-side artifact paths (microbench checkout,
# ubench build, verilator cache, vendored run_compare.py + baseline config),
# LOG_DIR, the in-container gem5/chipyard paths, and the gem5 build identity
# (GEM5_ISA / GEM5_VARIANT / GEM5_BIN).  Edit config.py to retarget; this module
# stays free of site-specific constants.


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

@dataclass
class IterState:
    iteration: int = 0
    entry_id: str = ""            # UUID of this entry in the alignment DB
    parent_id: str | None = None  # UUID of the entry this one branches from
    avg_pct_diff: float = float("inf")
    changes: str = ""
    per_bench: dict[str, float] = field(default_factory=dict)  # bench -> signed pct_diff
    source_changes: str = ""   # gem5 source files modified this iteration
    build_duration: float = 0.0  # scons rebuild time in seconds
    results: list[dict] = field(default_factory=list)  # full run_compare results for this iter


@dataclass
class IterationResult:
    """Everything one dispatched iteration produces, returned to the head
    thread for DB insertion and artifact writing.

    ``aborted=True`` means this dispatch made no meaningful state change
    worth recording (LLM refusal, build-debug exhausted, bench-debug
    exhausted); the head thread frees the bundle and re-dispatches with a
    fresh parent.
    """
    bundle_idx: int
    iteration: int
    entry_id: str
    parent_id: str
    aborted: bool = False
    abort_reason: str = ""
    # LLM outputs
    changes: str = ""
    source_changes: str = ""
    llm_log: str = ""                          # align LLM event-stream log (cli.stream_result)
    debug_logs: list[str] = field(default_factory=list)  # per-debug-attempt event-stream logs
    debug_build_failures: list[tuple[int, str]] = field(default_factory=list)
    build_failure_log: str = ""                # non-empty when initial post-align rebuild fell into debug
    # Post-iteration state of the gem5 tree on the worker
    config_contents: str = ""
    gem5_source_diff: str = ""
    base_rev: str = ""
    build_success: bool = False
    build_duration: float = 0.0
    # Benchmark outputs
    results: list[dict] = field(default_factory=list)
    avg_pct_diff: float = float("inf")
    per_bench: dict[str, float] = field(default_factory=dict)
    prev_avg_pct_diff: float = float("inf")    # parent's avg_pct_diff, for delta display
    # Pipeline traces: raw gzipped O3PipeView bytes (bench -> data) + per-bench
    # markdown summaries.  Persisted by _persist_iteration_result; re-staged
    # onto the worker that picks this entry as a parent in a later iteration.
    pipe_trace_bundles: dict[str, bytes] = field(default_factory=dict)
    pipe_trace_summaries: dict[str, str] = field(default_factory=dict)
    # Timing
    align_duration: float = 0.0
    gem5_duration: float = 0.0
    iter_duration: float = 0.0


# Serialize concurrent print()s so interleaved bundle output stays on
# line boundaries (instead of mid-line splicing).
_print_lock = threading.Lock()


def _wprint(bundle_idx: int, iteration: int, msg: str) -> None:
    """Thread-safe prefixed print for per-bundle iteration output."""
    with _print_lock:
        for line in msg.splitlines() or [""]:
            print(f"[w{bundle_idx}/iter {iteration}] {line}")


# ---------------------------------------------------------------------------
# Chipyard / verilator nodes (for producing verilator golden results)
# ---------------------------------------------------------------------------

@ChiaFunction(resources={"chipyard": 1.0})
def build_megaboom() -> BuildArtifact:
    """Build the configured Chipyard config (``BUILD_CONFIG``) on the chipyard node."""
    build_node = ChiselBuildNode(
        chipyard_path=CHIPYARD_PATH,
        config=BUILD_CONFIG,
        config_package="chipyard",
        target=BuildTarget.VERILATOR,
    )
    return build_node.build()


@ChiaFunction(resources={"verilator_run": 1.0})
def run_verilator_test(
    artifact: BuildArtifact,
    test_binary_content: bytes,
    test_binary_name: str,
) -> RunResult:
    """Run a single verilator microbenchmark."""
    inner_loop_dir = os.path.dirname(os.path.abspath(__file__))
    dramsim_dir = os.path.join(inner_loop_dir, "dramsim_ini")
    ini: dict[str, bytes] = {}
    if os.path.isdir(dramsim_dir):
        for fname in os.listdir(dramsim_dir):
            fpath = os.path.join(dramsim_dir, fname)
            if os.path.isfile(fpath):
                with open(fpath, "rb") as f:
                    ini[fname] = f.read()

    run_node = VerilatorRunNode()
    return run_node.run(
        artifact=artifact,
        test_binary_content=test_binary_content,
        test_binary_name=test_binary_name,
        work_dir="/home/ray/verilator_gem5_align/",
        plusargs={"+loadmem": test_binary_name, "+dump-tma-counters": None},
        dramsim_ini_files=ini,
        timeout_seconds=3000,
    )


def ensure_verilator_cache() -> bool:
    """If verilator results are not cached, build ``BUILD_CONFIG`` and run all benchmarks."""
    benchmarks = sorted(p.stem.replace(".verilator", "") for p in UBENCH_BUILD.glob("*.verilator.riscv"))
    considered = [b for b in benchmarks if b not in EXCLUDED_BENCHMARKS]
    missing = [
        b for b in considered
        if not (VERILATOR_CACHE / f"{b}.log").exists()
        or not (VERILATOR_CACHE / f"{b}.out").exists()
    ]

    if not missing:
        print(f"[verilator cache] All {len(considered)} benchmarks cached. Skipping build.")
        return True

    print(f"\n[verilator cache] {len(missing)}/{len(considered)} benchmarks missing.")

    # Build
    print(f"[verilator cache] Building {BUILD_CONFIG}...")
    t0 = time.time()
    artifact = get(build_megaboom.chia_remote())
    if artifact.returncode != 0:
        print(f"[verilator cache] Build FAILED [{_elapsed(t0)}]")
        print(f"  {artifact.stderr[-500:]}")
        return False
    print(f"[verilator cache] Build succeeded [{_elapsed(t0)}]")

    # Run all missing in parallel
    print(f"[verilator cache] Dispatching {len(missing)} verilator tests...")
    refs = []
    for b in missing:
        path = UBENCH_BUILD / f"{b}.verilator.riscv"
        content = path.read_bytes()
        refs.append((b, run_verilator_test.chia_remote(artifact, content, path.name)))
        print(f"  Dispatched: {b}")

    VERILATOR_CACHE.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    passed = 0
    for i, (bench_name, ref) in enumerate(refs):
        result = get(ref)
        (VERILATOR_CACHE / f"{bench_name}.log").write_text(result.log)
        (VERILATOR_CACHE / f"{bench_name}.out").write_text(result.out)
        status = "OK" if result.success else "FAIL"
        if result.success:
            passed += 1
        print(f"  [{i+1}/{len(refs)}] {bench_name}: {status}")

    print(f"[verilator cache] Done: {passed}/{len(refs)} passed [{_elapsed(t0)}]")
    return True


# ---------------------------------------------------------------------------
# Head-node helpers (local, no Ray scheduling)
# ---------------------------------------------------------------------------

def load_bench_descriptions() -> str:
    """Load all benchmark desc.txt files into a formatted string."""
    lines = []
    for d in sorted(MICROBENCH.iterdir()):
        desc = d / "desc.txt"
        if desc.exists():
            lines.append(f"- **{d.name}**: {' '.join(desc.read_text().strip().split())}")
    return "\n".join(lines)


def _iter_state_from_db_row(row: dict) -> IterState:
    """Rehydrate an ``IterState`` from a DB row produced by
    ``AlignmentDB.load_iteration``.  Used to rebuild in-memory history when
    resuming the loop.  Translates DB column names back to the field names
    produced by ``run_compare.py`` so downstream helpers (e.g.
    ``format_comparison_table``) work uniformly on fresh and rehydrated
    results.
    """
    per_bench: dict[str, float] = {}
    results_out: list[dict] = []
    for r in row.get("results", []):
        mapped = {
            "benchmark": r.get("benchmark"),
            "percentage_diff": r.get("percentage_diff"),
            "gem5_status": r.get("gem5_status"),
            "verilator_status": r.get("verilator_status"),
            "gem5_numCycles": r.get("gem5_num_cycles"),
            "verilator_numCycles": r.get("verilator_num_cycles"),
            "gem5/ver_cycle_ratio": r.get("gem5_ver_cycle_ratio"),
            "error_messages": r.get("error_messages"),
            "stdout_tail": r.get("stdout_tail"),
            "pipe_trace_summary": r.get("pipe_trace_summary"),
        }
        results_out.append(mapped)
        if mapped["gem5_status"] != "ok":
            continue
        pct = mapped["percentage_diff"]
        ratio = mapped["gem5/ver_cycle_ratio"]
        if pct is None:
            continue
        signed = pct if (ratio is not None and ratio >= 1.0) else -pct
        per_bench[mapped["benchmark"]] = signed
    avg = row.get("avg_pct_diff")
    return IterState(
        iteration=row["iteration"],
        entry_id=row.get("entry_id") or "",
        parent_id=row.get("parent_id"),
        avg_pct_diff=avg if avg is not None else float("inf"),
        changes=row.get("changes_summary") or "",
        per_bench=per_bench,
        source_changes=row.get("source_changes") or "",
        build_duration=row.get("build_duration") or 0.0,
        results=results_out,
    )


def _collect_init_payload() -> dict:
    """Read benchmark binaries, sources, run_compare, and config from the head
    filesystem and package them as a dict of bytes for shipping to the gem5
    worker over Ray's object store.
    """
    # MICROBENCH is the ubench checkout, which also contains build/ (the
    # compiled images, shipped separately as gem5_binaries) — skip it so we
    # ship only the benchmark sources the LLM reads, not the binaries/dumps.
    microbench_sources = {}
    for p in MICROBENCH.rglob("*"):
        if not p.is_file():
            continue
        rel = p.relative_to(MICROBENCH)
        if rel.parts and rel.parts[0] == "build":
            continue
        microbench_sources[str(rel)] = p.read_bytes()

    return {
        "gem5_binaries": {
            p.name: p.read_bytes() for p in UBENCH_BUILD.glob("*.gem5.elf")
        },
        "microbench_sources": microbench_sources,
        "verilator_artifacts": {
            p.name: p.read_bytes()
            for p in VERILATOR_CACHE.glob("*")
            if p.suffix in (".log", ".out")
        },
        "run_compare": RUN_COMPARE.read_bytes(),
        "config": GEM5_CONFIG.read_bytes(),
    }


@ChiaFunction(resources={"gem5": 0.5})
def init_gem5_worker(payload: dict) -> tuple[bool, str]:
    """One-shot setup on a pinned gem5 worker: materialize binaries, sources,
    run_compare.py, and the initial config into ``/home/ray/bench_workspace/``.
    Also stashes the current gem5 source git HEAD so future diffs are computed
    against this baseline.
    """
    import shutil
    root = Path(WORKER_BENCH_ROOT)
    if root.exists():
        shutil.rmtree(root)
    (root / "build" / "ubench").mkdir(parents=True)
    (root / "microbench").mkdir(parents=True)
    (root / "verilator").mkdir(parents=True)

    for name, content in payload["gem5_binaries"].items():
        (root / "build" / "ubench" / name).write_bytes(content)
    for rel, content in payload["microbench_sources"].items():
        dst = root / "microbench" / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(content)
    for name, content in payload["verilator_artifacts"].items():
        (root / "verilator" / name).write_bytes(content)
    (root / "run_compare.py").write_bytes(payload["run_compare"])
    Path(WORKER_CONFIG).write_bytes(payload["config"])

    rev = subprocess.run(
        f"cd {GEM5_CONTAINER_ROOT} && git rev-parse HEAD",
        shell=True, capture_output=True, text=True, timeout=30,
    )
    if rev.returncode != 0:
        return False, f"git rev-parse failed: {rev.stderr[-500:]}"
    Path(WORKER_GEM5_BASE_REV).write_text(rev.stdout.strip())

    # The image ships the gem5 source tree but not a prebuilt binary.  Do a
    # one-time build here (via the canonical Gem5Node.build_gem5) if the binary
    # is missing so the baseline run has something to execute.  On subsequent
    # starts the binary is already in place and scons is a no-op.
    build_msg = ""
    if not GEM5_BIN.exists():
        art = Gem5Node.build_gem5(
            GEM5_CONTAINER_ROOT, isa=GEM5_ISA, variant=GEM5_VARIANT, timeout_s=7200,
        )
        if not art.success:
            return False, (f"initial gem5 build failed after "
                           f"{art.build_duration_s:.0f}s: {art.stderr_tail}")
        build_msg = f", built gem5.opt in {art.build_duration_s:.0f}s"

    n_bins = len(list((root / "build" / "ubench").glob("*.gem5.elf")))
    n_ver = len(list((root / "verilator").glob("*.log")))
    return True, (
        f"initialized with {n_bins} gem5 binaries, {n_ver} verilator logs, "
        f"base rev {rev.stdout.strip()[:10]}{build_msg}"
    )


def _capture_config_and_diff() -> tuple[str, str, str]:
    """Capture ``(config_contents, gem5_source_diff, base_rev)`` on the gem5 worker.

    Delegates to the canonical ``Gem5Node.capture_gem5_source_state`` so the loop
    and the chia simulators package share one capture implementation (untracked
    files in ``src/`` are marked intent-to-add so brand-new files land in the
    diff, then un-staged to leave the index clean).  Diffs against the gem5 base
    rev recorded at worker-init time (``WORKER_GEM5_BASE_REV``) rather than
    current HEAD, so later evaluations can reset ``src/`` to the exact commit the
    diff was captured against.  Used by both ``run_gem5_comparison`` and
    ``rebuild_gem5``.
    """
    try:
        base_rev = Path(WORKER_GEM5_BASE_REV).read_text().strip()
    except Exception:
        base_rev = None  # capture falls back to current HEAD

    state = Gem5Node.capture_gem5_source_state(
        GEM5_CONTAINER_ROOT,
        base_rev=base_rev,
        diff_paths=["src/"],
        config_path=WORKER_CONFIG,
    )
    return state.config_contents, state.source_diff, state.base_rev


# ---------------------------------------------------------------------------
# O3PipeView trace summarization (runs worker-side inside run_gem5_comparison)
# ---------------------------------------------------------------------------
# Both helpers delegate to the canonical chia.simulators.gem5.Gem5Node so the
# loop and the simulators package share one implementation.  The decompressed
# size cap (PIPE_TRACE_MAX_DECOMPRESSED_BYTES) lives in config.py.


def _truncate_pipe_trace(path: "Path", max_decompressed_bytes: int) -> tuple[int, bool]:
    """Head-truncate a gzipped O3PipeView trace to a decompressed-byte cap,
    ending on a line boundary; returns ``(retained_bytes, truncated)``.

    Thin wrapper over the canonical ``Gem5Node.truncate_gz_trace``.
    """
    return Gem5Node.truncate_gz_trace(str(path), max_decompressed_bytes)


def _summarize_pipeview_trace(
    trace_path: "Path",
    max_first: int = 30,
    max_slowest: int = 10,
) -> str:
    """Stream an O3PipeView trace into a compact markdown digest.

    Thin wrapper over the canonical ``Gem5Node.summarize_o3_pipeview`` (same
    streaming algorithm + output format), so trace summarization lives in one
    place.  The raw trace on disk stays the authoritative artifact.
    """
    return Gem5Node.summarize_o3_pipeview(
        str(trace_path), max_first=max_first, max_slowest=max_slowest)


@ChiaFunction(resources={"gem5": 0.5})
def run_gem5_comparison(iteration: int) -> dict:
    """Run run_compare.py on the pinned gem5 worker.

    Verilator golden logs are materialized once at worker-init time under
    ``{WORKER_BENCH_ROOT}/verilator/``; this call copies them into the
    per-run ``results_dir/verilator/`` where ``run_compare.py --gem5-only``
    expects them.  gem5 is invoked with ``--debug-flags=O3PipeView
    --debug-file=pipe_trace.gz`` so every bench's --outdir ends up with a
    gzipped instruction-pipeline trace; those bytes + per-bench summaries
    are shipped back to the head alongside the normal comparison results.
    """
    import socket
    import tempfile
    import shutil

    t_fn_start = time.time()
    host = socket.gethostname()

    def _log(msg: str) -> None:
        print(f"[run_gem5_comparison iter={iteration} host={host} "
              f"t+{time.time() - t_fn_start:6.1f}s] {msg}", flush=True)

    _log("ENTER")

    root = Path(WORKER_BENCH_ROOT)
    build_ubench = root / "build" / "ubench"
    persistent_ver_dir = root / "verilator"

    with tempfile.TemporaryDirectory() as tmpdir:
        results_dir = Path(tmpdir) / "results"
        results_dir.mkdir(parents=True, exist_ok=True)

        t0 = time.time()
        ver_dir = results_dir / "verilator"
        ver_dir.mkdir()
        ver_copied = 0
        for log_path in persistent_ver_dir.glob("*.log"):
            shutil.copy2(log_path, ver_dir / log_path.name)
            ver_copied += 1
        _log(f"copied {ver_copied} verilator logs to tmpdir in {time.time() - t0:.1f}s")

        all_benches = sorted(
            p.stem.replace(".gem5", "")
            for p in build_ubench.glob("*.gem5.elf")
        )
        benches = [b for b in all_benches if b not in EXCLUDED_BENCHMARKS]
        _log(f"dispatching {len(benches)} benches (excluded: "
             f"{sorted(EXCLUDED_BENCHMARKS)}) via run_compare.py --jobs 4 "
             f"with O3PipeView tracing — this is the slow phase, expect "
             f"~1-10 min per bench with tracing enabled")

        cmd = (
            f"python3 {WORKER_RUN_COMPARE} "
            f"--gem5-bin {GEM5_BIN} "
            f"--gem5-config {WORKER_CONFIG} "
            f"--baremetal-dir {root} "
            f"--build-ubench-dir {build_ubench} "
            f"--results-dir {results_dir} "
            f"--gem5-only --skip-build "
            f"--memory-backend dramsim2 --jobs 4 "
            f"--gem5-debug-flags O3PipeView "
            f"--gem5-debug-file {PIPE_TRACE_FILENAME} "
            f"--benchmarks {' '.join(benches)}"
        )
        t0 = time.time()
        rc = subprocess.run(cmd, shell=True, timeout=3600)
        gem5_elapsed = time.time() - t0
        _log(f"gem5 subprocess returned rc={rc.returncode} after "
             f"{gem5_elapsed:.1f}s ({gem5_elapsed / 60:.1f} min)")

        json_path = results_dir / "compare_results.json"
        results: list[dict] = []
        gem5_logs_dir = results_dir / "gem5"
        pipe_trace_bundles: dict[str, bytes] = {}
        pipe_trace_summaries: dict[str, str] = {}

        if not json_path.exists():
            _log(f"WARNING: compare_results.json not found at {json_path}; "
                 f"returning empty results")
        else:
            t_trace_phase = time.time()
            results = json.loads(json_path.read_text())
            _log(f"parsed compare_results.json ({len(results)} entries); "
                 f"starting per-bench trace truncation + summarization "
                 f"(truncation cap = {PIPE_TRACE_MAX_DECOMPRESSED_BYTES // (1024*1024)} MB decompressed)")

            total_raw_size = 0
            total_trunc_size = 0
            n_trunc = 0
            n_traced = 0

            for r in results:
                bench = r["benchmark"]
                bench_dir = gem5_logs_dir / bench

                # Embed stdout log tails for failed benchmarks so the head can
                # report failures without access to the worker filesystem.
                if r.get("gem5_status") != "ok":
                    log_path = bench_dir / "stdout.log"
                    if log_path.exists():
                        r["stdout_tail"] = "\n".join(
                            log_path.read_text().splitlines()[-80:]
                        )

                trace_path = bench_dir / PIPE_TRACE_FILENAME
                if trace_path.exists():
                    t_b = time.time()
                    raw_size = trace_path.stat().st_size
                    total_raw_size += raw_size
                    retained, truncated = _truncate_pipe_trace(
                        trace_path, PIPE_TRACE_MAX_DECOMPRESSED_BYTES)
                    post_size = trace_path.stat().st_size
                    total_trunc_size += post_size
                    if truncated:
                        n_trunc += 1
                    pipe_trace_bundles[bench] = trace_path.read_bytes()
                    pipe_trace_summaries[bench] = _summarize_pipeview_trace(trace_path)
                    r["pipe_trace_summary"] = pipe_trace_summaries[bench]
                    n_traced += 1
                    _log(f"  {bench:<12} status={r.get('gem5_status'):<10} "
                         f"raw_gz={raw_size / (1024*1024):6.2f}MB "
                         f"post={post_size / (1024*1024):6.2f}MB "
                         f"truncated={truncated} "
                         f"summ={len(pipe_trace_summaries[bench]) / 1024:5.1f}KB "
                         f"[{time.time() - t_b:.2f}s]")

            _log(f"trace phase done in {time.time() - t_trace_phase:.1f}s: "
                 f"{n_traced} traces read, {n_trunc} truncated, raw total "
                 f"{total_raw_size / (1024*1024):.1f}MB -> post "
                 f"{total_trunc_size / (1024*1024):.1f}MB shipped via Ray")

    t0 = time.time()
    config_contents, gem5_source_diff, base_rev = _capture_config_and_diff()
    _log(f"captured config+diff in {time.time() - t0:.1f}s "
         f"(config {len(config_contents)}B, diff {len(gem5_source_diff)}B, "
         f"base_rev={base_rev[:10]})")
    _log(f"EXIT total_wall={time.time() - t_fn_start:.1f}s")
    return {
        "results": results,
        "config_contents": config_contents,
        "gem5_source_diff": gem5_source_diff,
        "base_rev": base_rev,
        "pipe_trace_bundles": pipe_trace_bundles,
        "pipe_trace_summaries": pipe_trace_summaries,
    }


@ChiaFunction(resources={"gem5": 0.5})
def install_parent_traces(
    pipe_trace_bundles: dict[str, bytes],
    pipe_trace_summaries: dict[str, str],
    parent_iteration: int,
) -> tuple[bool, str]:
    """Stage the parent iteration's pipeline traces on this bundle.

    Writes ``<WORKER_PARENT_TRACES>/<bench>/pipe_trace.gz`` and
    ``summary.md`` for every bench, plus a top-level ``INDEX.md`` listing
    what's available.  The aligning LLM reaches these via
    ``gem5_src_bash_run_command`` (zcat / grep / cat).  Called at the start
    of every iteration; previous contents are wiped so the LLM never sees
    a stale parent's traces.
    """
    import shutil

    root = Path(WORKER_PARENT_TRACES)
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)

    index_lines = [
        f"# Parent iteration {parent_iteration} pipeline traces",
        "",
        f"Benches with traces: {len(pipe_trace_bundles)}",
        "",
    ]
    for bench in sorted(pipe_trace_bundles):
        bench_dir = root / bench
        bench_dir.mkdir(parents=True, exist_ok=True)
        (bench_dir / PIPE_TRACE_FILENAME).write_bytes(pipe_trace_bundles[bench])
        summary = pipe_trace_summaries.get(bench, "(no summary)")
        (bench_dir / "summary.md").write_text(summary)
        index_lines.append(f"- `{bench}/` — pipe_trace.gz + summary.md")
    (root / "INDEX.md").write_text("\n".join(index_lines) + "\n")
    return True, f"installed {len(pipe_trace_bundles)} parent traces at {root}"


@ChiaFunction(resources={"gem5": 0.5})
def rebuild_gem5() -> tuple[bool, str, float, str, str, str]:
    """Incremental scons rebuild of gem5.

    Returns ``(success, log_tail, duration_s, config_contents, gem5_source_diff,
    base_rev)`` so the head can persist the post-build config + diff + the
    gem5 commit they were captured against, alongside results.

    Must be dispatched with ``.options(scheduling_strategy=...)`` pinned to the
    same placement group where gem5 source was edited.

    The scons build is delegated to the canonical ``Gem5Node.build_gem5``; on
    failure ``log_tail`` carries its filtered compiler/linker diagnostics (which
    the debug LLM reads), on success the tail of build stdout.
    """
    art = Gem5Node.build_gem5(
        GEM5_CONTAINER_ROOT, isa=GEM5_ISA, variant=GEM5_VARIANT, timeout_s=3600,
    )
    log_tail = art.stderr_tail if not art.success else art.stdout_tail
    config_contents, gem5_source_diff, base_rev = _capture_config_and_diff()
    return art.success, log_tail, art.build_duration_s, config_contents, gem5_source_diff, base_rev


@ChiaFunction(resources={"gem5": 0.5})
def restore_gem5_state(config_contents: str, gem5_source_diff: str) -> tuple[bool, str]:
    """Restore a known gem5 state inside the worker container.

    Resets the source tree to the recorded base rev, applies the stored patch,
    and writes the config file.  Used when resuming from a DB entry at startup
    and at the start of every iteration to re-establish the sampled parent's
    state.  The caller is responsible for rebuilding gem5 after this returns.

    Delegates to the canonical ``Gem5Node.restore_gem5_source_state`` (config
    write -> ``git checkout <base_rev> -- src/`` -> ``git clean -fd src/`` ->
    ``git apply``), reconstructing a ``Gem5SourceState`` from the stored config +
    diff and the gem5 base rev recorded at worker-init time.
    """
    try:
        base_rev = Path(WORKER_GEM5_BASE_REV).read_text().strip()
    except Exception as e:
        return False, f"failed to read base rev: {e}"

    state = Gem5SourceState(
        base_rev=base_rev,
        source_diff=gem5_source_diff or "",
        config_contents=config_contents,
    )
    return Gem5Node.restore_gem5_source_state(
        GEM5_CONTAINER_ROOT, state, restore_paths=["src/"], config_path=WORKER_CONFIG,
    )


def compute_avg_pct_diff(results: list[dict]) -> float:
    diffs = [abs(r["percentage_diff"]) for r in results
             if r.get("percentage_diff") is not None and r.get("gem5_status") == "ok"]
    return sum(diffs) / len(diffs) if diffs else float("inf")


def format_comparison_table(results: list[dict]) -> str:
    lines = ["| Benchmark | % Diff | gem5/ver Ratio | Description |",
             "|-----------|--------|---------------|-------------|"]
    for r in results:
        pct = r.get("percentage_diff")
        ratio = r.get("gem5/ver_cycle_ratio")
        lines.append(
            f"| {r['benchmark']} "
            f"| {f'{pct:.2f}%' if pct is not None else 'N/A'} "
            f"| {f'{ratio:.4f}' if ratio is not None else 'N/A'} "
            f"| {r.get('benchmark_notes', '')} |"
        )
    return "\n".join(lines)


def build_history_report(
    lineage: list[IterState],
    siblings: list[IterState] | None = None,
) -> str:
    """Build the history block handed to the aligning LLM.

    ``lineage`` is the ordered root→parent chain of this dispatch's parent
    (oldest first, parent last).  ``siblings`` is an optional short list of
    promising branches outside the lineage — typically the best K entries by
    ``avg_pct_diff`` that aren't already in ``lineage``.  Bounding the
    report to these keeps prompt length sublinear in the size of the DB
    tree, so parallel exploration doesn't drown the LLM in sibling noise.
    """
    if not lineage:
        return "No previous iterations."

    parts: list[str] = ["### Your lineage (root → parent)"]
    for idx, h in enumerate(lineage):
        delta = ""
        if idx > 0:
            prev = lineage[idx - 1]
            if prev.avg_pct_diff != float("inf") and h.avg_pct_diff != float("inf"):
                diff = h.avg_pct_diff - prev.avg_pct_diff
                delta = f" ({'improved' if diff < 0 else 'regressed'} {abs(diff):.2f}pp)"
        parts.append(
            f"- **Iter {h.iteration}**: avg_diff={h.avg_pct_diff:.2f}%{delta}\n"
            f"  Changes: {h.changes[:300]}"
        )
        if h.source_changes and h.source_changes.lower() != "none":
            parts.append(f"  Source changes: {h.source_changes[:300]}")

        # Per-benchmark movement between this lineage step and the next.
        if idx + 1 < len(lineage):
            nxt = lineage[idx + 1]
            moved: list[str] = []
            for bench in sorted(h.per_bench):
                before = h.per_bench.get(bench)
                after = nxt.per_bench.get(bench)
                if before is not None and after is not None and abs(abs(after) - abs(before)) > 0.5:
                    verdict = "IMPROVED" if abs(after) < abs(before) else "REGRESSED"
                    moved.append(f"    {bench}: {before:+.2f}% -> {after:+.2f}% ({verdict})")
            if moved:
                parts.append("  **Benchmarks that moved (>0.5pp):**")
                parts.extend(moved)

    # Per-benchmark trend table across the lineage.
    all_benches = sorted({b for h in lineage for b in h.per_bench})
    if all_benches and len(lineage) >= 2:
        parts.append("\n### Benchmark trend across your lineage")
        header = "| Benchmark | " + " | ".join(f"Iter {h.iteration}" for h in lineage) + " | Trend |"
        sep = "|---|" + "|".join("---" for _ in lineage) + "|---|"
        parts.append(header)
        parts.append(sep)
        for bench in all_benches:
            vals = [h.per_bench.get(bench) for h in lineage]
            cells = [f"{v:.2f}%" if v is not None else "—" for v in vals]
            real = [v for v in vals if v is not None]
            if len(real) >= 2:
                diff = abs(real[-1]) - abs(real[-2])
                trend = "improving" if diff < -1.0 else "REGRESSED" if diff > 1.0 else "stable"
            else:
                trend = "—"
            parts.append(f"| {bench} | " + " | ".join(cells) + f" | {trend} |")

    # Off-lineage branches worth knowing about.
    lineage_ids = {h.entry_id for h in lineage if h.entry_id}
    other = [s for s in (siblings or []) if s.entry_id not in lineage_ids]
    if other:
        parts.append("\n### Other promising branches (top entries not in your lineage)")
        for s in other:
            parts.append(
                f"- Iter {s.iteration} (entry {(s.entry_id or '?')[:8]}): "
                f"avg_diff={s.avg_pct_diff:.2f}% — {s.changes[:200]}"
            )

    return "\n".join(parts)


def build_best_per_bench_table(best_rows: list[dict]) -> str:
    """Render the per-benchmark best-so-far table.

    One row per benchmark listing the iteration whose signed pct is
    closest to zero.  Input is ``AlignmentDB.best_per_benchmark()``'s
    output (already sorted ``|signed_pct|`` asc, iteration asc).  The
    column set mirrors the IterState fields already surfaced elsewhere
    so the LLM can cross-reference a row against the sibling list.
    """
    if not best_rows:
        return "### Per-benchmark best-so-far\n\n(no benchmarks scored yet)"

    parts = [
        "### Per-benchmark best-so-far",
        "",
        "Each row is the iteration whose |signed_pct| is smallest for that",
        "benchmark. The entries listed here are the sibling branches in the",
        "history section below — look at their lineage / changes to learn",
        "why that iteration leads on that bench.",
        "",
        "| Benchmark | Best iter | Signed % | Entry |",
        "|-----------|-----------|----------|-------|",
    ]
    for r in best_rows:
        parts.append(
            f"| {r['benchmark']} | {r['iteration']} | "
            f"{r['signed_pct']:+.2f}% | {r['entry_id'][:8]} |"
        )
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# LLM node (dispatched to llm worker via Ray)
# ---------------------------------------------------------------------------

ALIGN_PROMPT_PATH = Path(__file__).parent / "prompts" / "align_node_prompt.md"

def align_node(
    prompt_template: str,
    comparison_table: str,
    bench_descriptions: str,
    history_report: str,
    parent_block: str,
    iteration: int,
    gem5_src_bash: BashTool,
    chipyard_bash: BashTool,
    align_db: "SQLiteQueryTool",
    compile_check: "Gem5ToolServer",
    quick_run: "QuickRunTool",
    session_id: str | None = None,
) -> tuple[str, str, str, bytes, bool]:
    """Run the aligning LLM: analyze mismatches, edit config and/or source.

    Runs on the head thread and dispatches ``ClaudeCodeLLM.prompt``
    (``chia.models.claude``) onto an ``llm`` worker via
    ``resources={"llm": 1.0}``; Ray schedules by the ``llm`` resource. The CLI
    session transcript rides back on the result so a later ``debug_node``
    resuming the same ``session_id`` continues the conversation even if it
    lands on a different worker.

    Returns ``(changes_summary, source_changes, llm_log, session_transcript,
    success)``.  ``llm_log`` is the LLM event-stream log (``cli.stream_result``)
    written to ``iter_N/llm/`` on the head; ``session_transcript`` is the
    captured ``<id>.jsonl`` bytes threaded into subsequent debug-node calls.
    """
    llm = ClaudeCodeLLM(
        model="claude-opus-4-6",
        timeout_seconds=3600,
        logging_name="align",
        resume_session=session_id is not None,
        projects_cwd=None,
        extra_cli_args=["--effort", "max"],
    )
    if session_id is not None:
        llm._session_id = session_id

    prompt = prompt_template.format(
        comparison_table=comparison_table,
        bench_descriptions=bench_descriptions,
        history_report=history_report,
        parent_block=parent_block,
        iteration=iteration,
        BOOM_SRC=BOOM_SRC,
        BUILD_CONFIG=BUILD_CONFIG,
        GEM5_CONFIG=WORKER_CONFIG,
        GEM5_SRC=GEM5_CONTAINER_SRC,
        CHIPYARD_GENERATORS=CHIPYARD_GENERATORS,
        PARENT_TRACES=WORKER_PARENT_TRACES,
    )

    cli = get(
        llm.prompt.options(resources={"llm": 1.0}).chia_remote(
            llm, prompt,
            [gem5_src_bash, chipyard_bash, align_db, compile_check, quick_run],
        )
    )
    result, success = cli.result, cli.success
    llm_log = cli.stream_result
    out_transcript = cli.session_transcript or b""

    summary = ""
    source_changes = ""
    if success:
        # Rare ==MARKER== delimiters to avoid collision with English prose.
        m = re.search(
            r"#{1,4}\s*==ALIGNMENT_OUTPUT==\s*\n(.*?)(?=\n#{1,4}\s*==SOURCE_PATCH==|$)",
            result, re.DOTALL,
        )
        if m:
            summary = m.group(1).strip()
        m = re.search(
            r"#{1,4}\s*==SOURCE_PATCH==\s*\n(.*?)(?=$)", result, re.DOTALL,
        )
        if m:
            source_changes = m.group(1).strip()

    return summary, source_changes, llm_log, out_transcript, success


def debug_node(
    prompt: str,
    iteration: int,
    attempt: int,
    gem5_src_bash: BashTool,
    compile_check: "Gem5ToolServer",
    quick_run: "QuickRunTool",
    debug_session_id: str,
    session_transcript: bytes | None = None,
    is_followup: bool = False,
) -> tuple[bool, str, bytes]:
    """Resume the iteration's LLM session to diagnose and fix a failure.

    Runs on the head thread and dispatches ``ClaudeCodeLLM.prompt``
    (``chia.models.claude``) onto an ``llm`` worker. On the first
    call (``is_followup=False``) a session with ``debug_session_id`` is
    created; on follow-ups it is resumed so the LLM retains full context of
    prior fix attempts. The carried ``session_transcript`` bytes travel with
    the LLM instance and are pasted onto the chosen worker before the CLI runs
    (``_restore_transcript``), so the conversation continues even across
    workers.

    Returns ``(success, debug_log, session_transcript)`` — ``debug_log`` is the
    event-stream log (``cli.stream_result``); the updated transcript can be
    threaded into the next debug call of the same iteration.
    """
    llm = ClaudeCodeLLM(
        model="claude-opus-4-6",
        timeout_seconds=3600,
        logging_name="debug",
        resume_session=is_followup,
        projects_cwd=None,
        extra_cli_args=["--effort", "max"],
    )
    llm._session_id = debug_session_id
    if session_transcript:
        llm._session_transcript = session_transcript

    cli = get(
        llm.prompt.options(resources={"llm": 1.0}).chia_remote(
            llm, prompt, [gem5_src_bash, compile_check, quick_run],
        )
    )
    # _session_tracked syncs cli.session_transcript onto llm on get(); fall back
    # to the carried transcript if the CLI captured nothing this call.
    out_transcript = cli.session_transcript or session_transcript or b""
    return cli.success, cli.stream_result, out_transcript


# ---------------------------------------------------------------------------
# Timing & changelog helpers
# ---------------------------------------------------------------------------

def _init_timing_log() -> str:
    os.makedirs(LOG_DIR, exist_ok=True)
    path = str(LOG_DIR / "timing.csv")
    if not os.path.isfile(path):
        with open(path, "w", newline="") as f:
            csv.writer(f).writerow(["iteration", "step", "duration_s"])
    return path


def _log_timing(path: str, iteration: int, step: str, dur: float):
    with open(path, "a", newline="") as f:
        csv.writer(f).writerow([iteration, step, f"{dur:.1f}"])


def _log_changelog(iteration: int, state: IterState, prev_avg: float, results: list[dict]):
    changelog = LOG_DIR / "changelog.md"
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    short_entry = (state.entry_id or "?")[:8]
    short_parent = (state.parent_id or "")[:8] or "(root)"
    with open(changelog, "a") as f:
        f.write(f"\n## Iteration {iteration} — entry {short_entry} — parent {short_parent} — {ts}\n")
        f.write(f"**Avg % diff**: {prev_avg:.2f}% -> {state.avg_pct_diff:.2f}%\n")
        f.write(f"**Changes**: {state.changes[:500]}\n")
        if state.source_changes and state.source_changes.lower() != "none":
            f.write(f"**Source changes**: {state.source_changes[:500]}\n")
        if state.build_duration > 0:
            f.write(f"**Build time**: {state.build_duration:.1f}s\n")
        f.write("\n")


def _collect_failed_output(results: list[dict]) -> str:
    """Collect error messages and stdout.log tails for all failed benchmarks.

    Reads ``stdout_tail`` and ``error_messages`` from the result dicts
    (embedded by ``run_gem5_comparison`` on the worker) so no filesystem
    access to the worker node is needed.
    """
    parts: list[str] = []
    for r in results:
        if r.get("gem5_status") == "ok":
            continue
        bench = r["benchmark"]
        parts.append(f"### {bench}  (status: {r.get('gem5_status', '?')})")
        err = r.get("error_messages", "")
        if err:
            parts.append(f"Error notes: {err}")
        tail = r.get("stdout_tail", "")
        if tail:
            parts.append(f"```\n{tail}\n```")
        parts.append("")
    return "\n".join(parts)


def _write_debug_failure_report(
    iteration: int, history: list[IterState],
    results: list[dict], last_changes: str,
) -> Path:
    """Write a report when the debug node exhausts all retries."""
    report_path = LOG_DIR / "debug_failure_report.md"
    failed = [r for r in results if r.get("gem5_status") != "ok"]
    lines = [
        f"# Debug Failure Report",
        f"**Generated**: {datetime.now():%Y-%m-%d %H:%M:%S}",
        f"**Iteration**: {iteration}  |  **Retries exhausted**: {DEBUG_MAX_RETRIES}",
        f"\n## Failed benchmarks ({len(failed)}/{len(results)})",
        *[f"- **{r['benchmark']}**: {r.get('gem5_status','?')} — {r.get('error_messages','')}" for r in failed],
        f"\n## Last config modification (probable cause)",
        f"```\n{last_changes or '(none recorded)'}\n```",
        f"\n## Iteration history",
        *[f"- Iter {h.iteration}: avg_diff={h.avg_pct_diff:.2f}%  {h.changes[:200]}" for h in history],
        f"\n## Failed benchmark logs",
        _collect_failed_output(results),
    ]
    report_path.write_text("\n".join(lines))
    return report_path


def _elapsed(t0: float) -> str:
    s = time.time() - t0
    return f"{s:.1f}s" if s < 60 else f"{s / 60:.1f}m"


def _now() -> str:
    return datetime.now().strftime("%H:%M:%S")


# ---------------------------------------------------------------------------
# Per-iteration driver (runs in a head thread, pinned to one gem5 bundle)
# ---------------------------------------------------------------------------

def _load_parent_traces_from_disk(
    parent_iteration: int,
) -> tuple[dict[str, bytes], dict[str, str]]:
    """Read a persisted iteration's pipe_trace.gz + summary.md files back
    into in-memory dicts keyed by benchmark.

    Returns ``({}, {})`` if the parent predates trace collection (old DB
    entries, or the traces subtree was deleted) — the LLM just sees an
    empty ``parent_traces/`` dir on the worker.
    """
    traces_dir = LOG_DIR / f"iter_{parent_iteration}" / "traces"
    bundles: dict[str, bytes] = {}
    summaries: dict[str, str] = {}
    if not traces_dir.is_dir():
        return bundles, summaries
    for bench_dir in sorted(traces_dir.iterdir()):
        if not bench_dir.is_dir():
            continue
        trace = bench_dir / PIPE_TRACE_FILENAME
        summary = bench_dir / "summary.md"
        if trace.exists():
            bundles[bench_dir.name] = trace.read_bytes()
        if summary.exists():
            summaries[bench_dir.name] = summary.read_text()
    return bundles, summaries


def _build_parent_block(parent: dict) -> str:
    """Render the 'Parent Entry' section fed into the align prompt.

    The aligning LLM sees the global history report plus this compact
    pointer at who its direct parent is — useful now that parallel
    dispatch means any of several siblings may live side-by-side in the
    history table.
    """
    parent_id = parent.get("entry_id") or ""
    gp_id = parent.get("parent_id") or ""
    avg = parent.get("avg_pct_diff")
    avg_str = f"{avg:.2f}%" if isinstance(avg, (int, float)) else "unscored"
    changes = (parent.get("changes_summary") or "").strip()
    src = (parent.get("source_changes") or "").strip()
    lines = [
        f"- iter {parent['iteration']}  "
        f"entry {parent_id[:8] or '?'}  "
        f"grandparent {(gp_id[:8] or '(root)')}",
        f"- avg_pct_diff: {avg_str}",
        f"- parent's most recent change:\n  {changes[:600] or '(none recorded)'}",
    ]
    if src and src.lower() != "none":
        lines.append(f"- parent's source changes:\n  {src[:300]}")
    return "\n".join(lines)


def _run_iteration(
    bundle_idx: int,
    iteration_number: int,
    parent: dict,
    lineage: list[IterState],
    siblings: list[IterState],
    best_table_text: str,
    bundle_opts: dict,
    gem5_src_bash: "RestrictedBashTool",
    chipyard_bash: "BashTool",
    align_db: "SQLiteQueryTool",
    compile_check: "Gem5ToolServer",
    quick_run: "QuickRunTool",
    bench_descs: str,
    prompt_template: str,
) -> IterationResult:
    """Run one full align-build-test iteration on the given gem5 bundle.

    ``bundle_opts`` is the scheduling-strategy dict (from the bundle's
    ``Gem5Node.task_options``) that pins every gem5 op in this iteration to the
    same physical node.

    Returns an ``IterationResult``.  Sets ``aborted=True`` for dispatches
    that made no meaningful state change worth recording (LLM refusal,
    debug exhaustion).  The caller frees the bundle and redispatches.
    """
    def log(msg: str) -> None:
        _wprint(bundle_idx, iteration_number, msg)

    iter_t0 = time.time()
    result = IterationResult(
        bundle_idx=bundle_idx, iteration=iteration_number,
        entry_id=str(uuid4()), parent_id=parent["entry_id"],
        prev_avg_pct_diff=(parent.get("avg_pct_diff") or float("inf")),
    )

    # --- Restore parent state on this bundle ---
    parent_avg = parent.get("avg_pct_diff")
    parent_avg_str = f"{parent_avg:.2f}%" if parent_avg is not None else "unscored"
    log(f"parent iter {parent['iteration']} (entry {parent['entry_id'][:8]}, "
        f"avg={parent_avg_str})")
    t0 = time.time()
    ok, msg = get(restore_gem5_state.options(**bundle_opts).chia_remote(
        parent["config_contents"], parent.get("gem5_source_diff") or ""))
    if not ok:
        log(f"Restore FAILED: {msg}")
        result.aborted = True
        result.abort_reason = f"restore_failed: {msg}"
        result.iter_duration = time.time() - iter_t0
        return result
    log(f"Restored parent state [{_elapsed(t0)}]: {msg}")

    # --- Stage parent traces for the LLM to inspect via bash ---
    parent_bundles, parent_summaries = _load_parent_traces_from_disk(parent["iteration"])
    if parent_bundles:
        t0 = time.time()
        ok, msg = get(install_parent_traces.options(**bundle_opts).chia_remote(
            parent_bundles, parent_summaries, parent["iteration"]))
        log(f"Installed {len(parent_bundles)} parent traces [{_elapsed(t0)}]: {msg}")
    else:
        # Still wipe any stale traces from an earlier dispatch on this bundle.
        get(install_parent_traces.options(**bundle_opts).chia_remote(
            {}, {}, parent["iteration"]))
        log("No parent traces on disk — installed empty parent_traces/ dir")

    # --- Rebuild against parent state ---
    log("Rebuilding gem5 against parent state...")
    t0 = time.time()
    build_ok, build_log, build_dur, _pc, _pd, _pr = get(
        rebuild_gem5.options(**bundle_opts).chia_remote())
    if not build_ok:
        log(f"Parent-restore rebuild FAILED [{build_dur:.1f}s] — aborting")
        result.aborted = True
        result.abort_reason = "parent_rebuild_failed"
        result.build_failure_log = build_log
        result.build_duration = build_dur
        result.iter_duration = time.time() - iter_t0
        return result
    log(f"Parent rebuild OK [{build_dur:.1f}s]")

    # --- Step 1: LLM alignment (one fresh session per iteration) ---
    iteration_session_id = str(uuid4())
    log(f"[Step 1/4] Alignment LLM (session {iteration_session_id[:8]})...")
    t0 = time.time()
    parent_block = _build_parent_block(parent)
    history_report = (
        best_table_text + "\n\n---\n\n" +
        build_history_report(lineage, siblings)
    )
    changes, source_changes, llm_log, session_transcript, llm_ok = align_node(
        prompt_template,
        format_comparison_table(parent.get("results", [])),
        bench_descs, history_report, parent_block,
        iteration_number, gem5_src_bash, chipyard_bash, align_db,
        compile_check, quick_run, iteration_session_id,
    )
    result.align_duration = time.time() - t0
    result.changes = changes
    result.source_changes = source_changes
    result.llm_log = llm_log

    if not llm_ok:
        log(f"Alignment FAILED [{_elapsed(t0)}] — aborting dispatch")
        result.aborted = True
        result.abort_reason = "align_failed"
        result.iter_duration = time.time() - iter_t0
        return result

    log(f"Alignment complete [{_elapsed(t0)}]")
    for line in changes.split("\n")[:5]:
        if line.strip():
            log(f"    {line.strip()}")
    if source_changes and source_changes.lower() != "none":
        log("Source changes:")
        for line in source_changes.split("\n")[:5]:
            if line.strip():
                log(f"    {line.strip()}")

    # --- Step 2: Rebuild gem5 (incremental scons) ---
    log("[Step 2/4] Rebuilding gem5 (scons incremental)...")
    t0 = time.time()
    build_ok, build_log, build_dur, post_config, post_diff, post_rev = get(
        rebuild_gem5.options(**bundle_opts).chia_remote())
    config_contents = post_config
    gem5_source_diff = post_diff
    base_rev = post_rev

    if not build_ok:
        log(f"Build FAILED [{build_dur:.1f}s]")
        result.build_failure_log = build_log
        for attempt in range(1, DEBUG_MAX_RETRIES + 1):
            log(f"Build debug attempt {attempt}/{DEBUG_MAX_RETRIES}")
            if attempt == 1:
                debug_prompt = (
                    f"## Build Failure: gem5 scons build failed after source modification\n\n"
                    f"Config: `{WORKER_CONFIG}`\n"
                    f"gem5 source: `{GEM5_CONTAINER_SRC}`\n\n"
                    f"### Build log (last 3000 chars)\n```\n{build_log}\n```\n\n"
                    f"Read the source files you modified, identify the compilation "
                    f"error, and fix it. Do NOT revert the change — fix the error "
                    f"so the intended modification compiles correctly.\n"
                )
            else:
                debug_prompt = (
                    f"## Build retry {attempt}/{DEBUG_MAX_RETRIES}: still failing\n\n"
                    f"### Build log\n```\n{build_log}\n```\n\n"
                    f"Read the failing file(s) again and try a different fix.\n"
                )
            debug_ok, debug_log, session_transcript = debug_node(
                debug_prompt, iteration_number, attempt, gem5_src_bash,
                compile_check, quick_run, iteration_session_id,
                session_transcript, True)
            if debug_log:
                result.debug_logs.append(debug_log)
            if not debug_ok:
                log(f"Debug LLM call failed on attempt {attempt}")
                continue
            log("Re-building...")
            build_ok, build_log, build_dur, post_config, post_diff, post_rev = get(
                rebuild_gem5.options(**bundle_opts).chia_remote())
            if build_ok:
                log(f"Build succeeded on retry {attempt} [{build_dur:.1f}s]")
                config_contents = post_config
                gem5_source_diff = post_diff
                base_rev = post_rev
                break
        if not build_ok:
            log(f"Build failed after {DEBUG_MAX_RETRIES} attempts — aborting dispatch")
            result.aborted = True
            result.abort_reason = "build_debug_exhausted"
            result.build_duration = build_dur
            result.iter_duration = time.time() - iter_t0
            return result
    else:
        log(f"Build succeeded [{build_dur:.1f}s]")

    result.build_success = build_ok
    result.build_duration = build_dur

    # --- Step 3: Run gem5 comparison ---
    log(f"[Step 3/4] Running gem5 microbenchmarks...")
    t0 = time.time()
    response = get(run_gem5_comparison.options(**bundle_opts).chia_remote(
        iteration_number))
    result.gem5_duration = time.time() - t0
    results = response["results"]
    config_contents = response["config_contents"]
    gem5_source_diff = response["gem5_source_diff"]
    base_rev = response.get("base_rev", "") or base_rev
    pipe_trace_bundles = response.get("pipe_trace_bundles", {}) or {}
    pipe_trace_summaries = response.get("pipe_trace_summaries", {}) or {}

    ok_count = sum(1 for r in results if r.get("gem5_status") == "ok")
    fail_count = len(results) - ok_count
    log(f"gem5 finished: {ok_count} passed, {fail_count} failed [{_elapsed(t0)}]")

    # --- Bench-failure debug loop (debug reuses align session) ---
    if fail_count > 0 and len(results) > 0:
        for attempt in range(1, DEBUG_MAX_RETRIES + 1):
            failed_output = _collect_failed_output(results)
            log(f"{fail_count} failing — debug attempt {attempt}/{DEBUG_MAX_RETRIES}")
            if attempt == 1:
                src_line = (
                    f"Source: {source_changes}\n"
                    if source_changes and source_changes.lower() != "none"
                    else ""
                )
                prompt = (
                    f"## Debug: {fail_count}/{len(results)} benchmarks failed\n\n"
                    f"Config: `{WORKER_CONFIG}`\n"
                    f"gem5 source: `{GEM5_CONTAINER_SRC}`\n\n"
                    f"### Modification made this iteration\n\n"
                    f"{changes or '(none recorded)'}\n"
                    f"{src_line}\n"
                    f"### Failed benchmark output\n\n{failed_output}\n\n"
                    f"Read the config (`cat {WORKER_CONFIG}`) and/or modified source, "
                    f"identify what in this iteration's modification caused the failures, "
                    f"and fix it. You may edit config or source; scons will rebuild "
                    f"automatically if you touch source. "
                    f"Do NOT change working microarchitectural parameters.\n"
                )
            else:
                prompt = (
                    f"## Debug retry {attempt}/{DEBUG_MAX_RETRIES}: "
                    f"still {fail_count}/{len(results)} failing\n\n"
                    f"{failed_output}\n\n"
                    f"Read the config again (`cat {WORKER_CONFIG}`) and try a different fix.\n"
                )
            debug_ok, debug_log, session_transcript = debug_node(
                prompt, iteration_number, attempt, gem5_src_bash,
                compile_check, quick_run, iteration_session_id,
                session_transcript, True)
            if debug_log:
                result.debug_logs.append(debug_log)
            if not debug_ok:
                log(f"Debug LLM call failed on attempt {attempt}")
                continue

            log("Rebuilding after debug fix...")
            rebuild_ok, rebuild_log, rebuild_dur, post_config, post_diff, post_rev = get(
                rebuild_gem5.options(**bundle_opts).chia_remote())
            if not rebuild_ok:
                log(f"Rebuild FAILED after debug [{rebuild_dur:.1f}s] — "
                    f"counting attempt {attempt} as failed")
                result.debug_build_failures.append((attempt, rebuild_log))
                continue
            result.build_success = True
            result.build_duration = rebuild_dur
            config_contents = post_config
            gem5_source_diff = post_diff
            base_rev = post_rev

            log("Re-running benchmarks...")
            response = get(run_gem5_comparison.options(**bundle_opts).chia_remote(
                iteration_number))
            results = response["results"]
            config_contents = response["config_contents"]
            gem5_source_diff = response["gem5_source_diff"]
            base_rev = response.get("base_rev", "") or base_rev
            pipe_trace_bundles = response.get("pipe_trace_bundles", {}) or {}
            pipe_trace_summaries = response.get("pipe_trace_summaries", {}) or {}
            ok_count = sum(1 for r in results if r.get("gem5_status") == "ok")
            fail_count = len(results) - ok_count
            log(f"{ok_count} passed, {fail_count} failed")
            if fail_count == 0:
                break

        if fail_count > 0:
            log(f"Debug exhausted after {DEBUG_MAX_RETRIES} attempts — aborting dispatch")
            result.aborted = True
            result.abort_reason = "bench_debug_exhausted"
            result.results = results
            result.config_contents = config_contents
            result.gem5_source_diff = gem5_source_diff
            result.base_rev = base_rev
            result.iter_duration = time.time() - iter_t0
            return result

    # --- Pack result ---
    result.results = results
    result.config_contents = config_contents
    result.gem5_source_diff = gem5_source_diff
    result.base_rev = base_rev
    result.pipe_trace_bundles = pipe_trace_bundles
    result.pipe_trace_summaries = pipe_trace_summaries
    result.avg_pct_diff = compute_avg_pct_diff(results)

    per_bench: dict[str, float] = {}
    for r in results:
        pct = r.get("percentage_diff")
        ratio = r.get("gem5/ver_cycle_ratio")
        if pct is not None and r.get("gem5_status") == "ok":
            signed_pct = pct if (ratio is not None and ratio >= 1.0) else -pct
            per_bench[r["benchmark"]] = signed_pct
    result.per_bench = per_bench
    result.iter_duration = time.time() - iter_t0
    log(f"done [{_elapsed(iter_t0)}], avg_pct_diff={result.avg_pct_diff:.2f}%")
    return result


def _persist_iteration_result(
    r: IterationResult,
    db: AlignmentDB,
    history: list[IterState],
    metrics: MetricsLogger,
    timing_path: str,
) -> None:
    """Write iter_N/ artifacts, update history, insert the DB row.

    Runs on the main head thread so all side effects — file writes, DB
    writes, history appends, metrics — are serialized across bundles.
    Aborted dispatches get diagnostics written but no DB row or history
    entry (the dispatcher will redispatch a fresh parent).
    """
    iter_dir = LOG_DIR / f"iter_{r.iteration}"
    iter_dir.mkdir(parents=True, exist_ok=True)

    if r.llm_log:
        llm_log_dir = iter_dir / "llm"
        llm_log_dir.mkdir(parents=True, exist_ok=True)
        (llm_log_dir / "align.log").write_text(r.llm_log)
    if r.changes:
        (iter_dir / "changes.md").write_text(r.changes)
    if r.source_changes:
        (iter_dir / "source_changes.md").write_text(r.source_changes)
    for i, debug_log in enumerate(r.debug_logs, 1):
        if debug_log:
            debug_dir = iter_dir / "debug"
            debug_dir.mkdir(parents=True, exist_ok=True)
            (debug_dir / f"attempt_{i}.log").write_text(debug_log)
    for attempt, log_text in r.debug_build_failures:
        (iter_dir / f"debug_build_failure_attempt{attempt}.log").write_text(log_text)
    if r.build_failure_log:
        (iter_dir / "build_failure.log").write_text(r.build_failure_log)

    if r.align_duration > 0:
        _log_timing(timing_path, r.iteration, "align_node", r.align_duration)
    if r.gem5_duration > 0:
        _log_timing(timing_path, r.iteration, "gem5_comparison", r.gem5_duration)
    if r.build_duration > 0:
        _log_timing(timing_path, r.iteration, "gem5_rebuild", r.build_duration)
    _log_timing(timing_path, r.iteration, "total", r.iter_duration)

    if r.aborted:
        (iter_dir / "aborted.txt").write_text(r.abort_reason + "\n")
        _wprint(r.bundle_idx, r.iteration,
                f"ABORTED ({r.abort_reason}) — no DB entry")
        return

    results_save_dir = iter_dir / "results"
    results_save_dir.mkdir(parents=True, exist_ok=True)
    (results_save_dir / "compare_results.json").write_text(
        json.dumps(r.results, indent=2))

    # Persist raw O3PipeView traces + per-bench summaries so the next
    # dispatch that picks this entry as its parent can re-stage them on
    # whichever bundle it lands on.
    if r.pipe_trace_bundles:
        traces_dir = iter_dir / "traces"
        traces_dir.mkdir(parents=True, exist_ok=True)
        for bench, data in r.pipe_trace_bundles.items():
            bench_dir = traces_dir / bench
            bench_dir.mkdir(parents=True, exist_ok=True)
            (bench_dir / PIPE_TRACE_FILENAME).write_bytes(data)
            summary = r.pipe_trace_summaries.get(bench)
            if summary is not None:
                (bench_dir / "summary.md").write_text(summary)

    under5 = under1 = 0
    worst = sorted(r.per_bench.items(), key=lambda x: abs(x[1]), reverse=True)
    for bench, pct in worst:
        metrics.log_scalar(f"pct_diff/{bench}", abs(pct), step=r.iteration)
        if abs(pct) < 1.0:
            under1 += 1
            under5 += 1
        elif abs(pct) < 5.0:
            under5 += 1
    metrics.log_scalar("avg_pct_diff", r.avg_pct_diff, step=r.iteration)
    metrics.log_scalar("num_under_5pct", under5, step=r.iteration)
    metrics.log_scalar("num_under_1pct", under1, step=r.iteration)
    metrics.log_scalar("iteration_duration_s", r.iter_duration, step=r.iteration)

    state = IterState(
        iteration=r.iteration,
        entry_id=r.entry_id,
        parent_id=r.parent_id,
        avg_pct_diff=r.avg_pct_diff,
        changes=r.changes,
        per_bench=r.per_bench,
        source_changes=r.source_changes,
        build_duration=r.build_duration,
        results=r.results,
    )
    history.append(state)
    _log_changelog(r.iteration, state, r.prev_avg_pct_diff, r.results)

    db.insert_iteration(
        entry_id=r.entry_id,
        parent_id=r.parent_id,
        iteration=r.iteration,
        avg_pct_diff=r.avg_pct_diff,
        changes=r.changes,
        source_changes=r.source_changes,
        config_contents=r.config_contents,
        gem5_source_diff=r.gem5_source_diff,
        base_rev=r.base_rev,
        build_success=r.build_success,
        build_duration=r.build_duration,
        llm_log_path=str(iter_dir / "llm"),
        results=r.results,
    )

    total = len(r.per_bench)
    _wprint(r.bundle_idx, r.iteration,
            f"DB inserted (entry {r.entry_id[:8]}, parent {r.parent_id[:8]}), "
            f"avg_pct_diff={r.avg_pct_diff:.2f}% (parent {r.prev_avg_pct_diff:.2f}%), "
            f"{under1}/{total} within 1%, {under5}/{total} within 5%")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run_alignment_loop() -> list[IterState]:
    history: list[IterState] = []
    timing_path = _init_timing_log()
    metrics = MetricsLogger.from_config(METRICS_CONFIG)
    bench_descs = load_bench_descriptions()

    # Ensure verilator golden results are cached (build + run if needed)
    if not ensure_verilator_cache():
        print("FATAL: Could not produce verilator cache. Exiting.")
        return []

    # --- Placement group: one STRICT_SPREAD bundle per physical gem5 node ---
    gem5_nodes = [
        n for n in ray.nodes()
        if n.get("Resources", {}).get("gem5", 0) >= 1.0 and n.get("Alive")
    ]
    total_nodes = len(gem5_nodes)
    if total_nodes == 0:
        print("FATAL: no alive gem5 nodes in the cluster.")
        metrics.close()
        return []
    N = min(total_nodes, MAX_PARALLEL_ITERATIONS)
    if N < total_nodes:
        print(f"Capping parallelism to {N} of {total_nodes} available gem5 nodes "
              f"(MAX_PARALLEL_ITERATIONS={MAX_PARALLEL_ITERATIONS}).")
    print(f"Allocating {N} gem5 bundles (STRICT_SPREAD — one per physical node)...")
    gem5_pg = placement_group(
        [{"CPU": 1, "gem5": 1}] * N, strategy="STRICT_SPREAD")
    ray.get(gem5_pg.ready())

    # One canonical Gem5Node per bundle, pinned to a fixed bundle of the shared
    # placement group.  We use each node's ``task_options`` (a
    # PlacementGroupSchedulingStrategy) to co-locate the bundle's gem5 ops and
    # ChiaTool actors on the same physical node; the loop's @ChiaFunction
    # wrappers stay at ``gem5: 0.5`` so a gem5 op and its tool actors share the
    # bundle (the ChiaTool actor itself reserves only 0.2 CPU).
    bundle_nodes = [
        Gem5Node(placement_group=gem5_pg, bundle_index=i) for i in range(N)
    ]

    def bundle_opts(i: int) -> dict:
        return bundle_nodes[i].task_options

    # --- One gem5_src_bash actor per bundle (unique global names) ---
    print(f"Deploying {N} gem5_src_bash actors (one per bundle)...")
    gem5_src_bashes: list[RestrictedBashTool] = [
        RestrictedBashTool(
            name=f"gem5_src_bash_{i}",
            work_dir=GEM5_CONTAINER_ROOT,
            timeout_seconds=900,
            task_options=bundle_opts(i),
        )
        for i in range(N)
    ]

    # --- One compile-check tool per bundle (paired with gem5_src_bash) ---
    # Canonical Gem5ToolServer exposing ONLY its `build` tool: an incremental
    # scons against the in-progress source state, so syntax / typing / privacy
    # errors surface during the iteration rather than after the loop's final
    # rebuild.  Spawned via Gem5Node.spawn_tool so it co-locates on (and is torn
    # down with) the same bundle.  run/stats/list_workloads are intentionally NOT
    # exposed — QuickRunTool owns runs (it carries the gem5-vs-verilator %diff).
    print(f"Deploying {N} compile-check tools (Gem5ToolServer build-only, one per bundle)...")
    compile_checks: list[Gem5ToolServer] = [
        bundle_nodes[i].spawn_tool(
            f"gem5_compile_check_{i}",
            gem5_root=GEM5_CONTAINER_ROOT,
            config_script=WORKER_CONFIG,
            workloads=f"{WORKER_BENCH_ROOT}/build/ubench",
            isa=GEM5_ISA,
            variant=GEM5_VARIANT,
            build_timeout_s=3600,
            expose=("build",),
        )
        for i in range(N)
    ]

    # --- One quick_run actor per bundle ---
    # Lets the aligning LLM run a *subset* of benchmarks (1-5) against
    # the in-progress source/config state in 1-3 minutes, instead of
    # paying the full ~25-40 min iteration build+run.  Pinned to the
    # same bundle as gem5_src_bash + compile_check so it edits/builds/
    # runs the same gem5 tree.  Each LLM iteration gets its own
    # QuickRunTool actor with its own dedicated gem5 resources.
    print(f"Deploying {N} quick_run actors (one per bundle)...")
    quick_runs: list[QuickRunTool] = [
        QuickRunTool(
            name=f"gem5_quick_run_{i}",
            gem5_root=GEM5_CONTAINER_ROOT,
            gem5_bin=str(GEM5_BIN),
            bench_workspace=WORKER_BENCH_ROOT,
            run_compare_path=WORKER_RUN_COMPARE,
            gem5_config=WORKER_CONFIG,
            excluded_benchmarks=EXCLUDED_BENCHMARKS,
            max_benchmarks_per_call=5,
            default_timeout_per_bench_s=3600,
            build_timeout_s=3600,
            task_options=bundle_opts(i),
        )
        for i in range(N)
    ]

    # --- Shared read-only chipyard_bash (single instance) ---
    chipyard_pg = placement_group([{"CPU": 1, "chipyard": 1}], strategy="STRICT_PACK")
    ray.get(chipyard_pg.ready())
    print("Deploying chipyard_bash (read-only, shared across bundles)...")
    chipyard_bash = RestrictedBashTool(
        name="chipyard_bash",
        read_only=True,
        work_dir=CHIPYARD_PATH,
        timeout_seconds=600,
        task_options={"scheduling_strategy": PlacementGroupSchedulingStrategy(
            placement_group=chipyard_pg, placement_group_bundle_index=0,
        )},
    )

    # --- Alignment DB (chia.database.SQLiteNode) + its read-only SQL tool ---
    # alignment.db lives on the head's local disk, so the SQLiteNode pins to
    # this node (pin_to_current_node) and spawn_query_tool puts the LLM-facing
    # read-only SQLiteQueryTool on the same node — the canonical replacement
    # for the old NodeAffinity-pinned AlignmentDbQueryTool.  AlignmentDB.__init__
    # creates the schema; the DB is reached in-process from the head thread.
    print("Initializing alignment DB + read-only SQL query tool (pinned to head)...")
    db = AlignmentDB(str(LOG_DIR / "alignment.db"), pin_to_current_node=True)
    align_db_tool = db.spawn_query_tool("align_db")

    # --- Parallel init across all bundles ---
    print(f"Initializing all {N} gem5 bundles in parallel "
          f"(each node builds gem5.opt on first boot; ~30 min one-time)...")
    payload = _collect_init_payload()
    print(f"  {len(payload['gem5_binaries'])} gem5 binaries, "
          f"{len(payload['microbench_sources'])} source files per bundle")
    init_refs = [
        init_gem5_worker.options(**bundle_opts(i)).chia_remote(payload)
        for i in range(N)
    ]
    for i, ref in enumerate(init_refs):
        ok, msg = get(ref)
        if not ok:
            print(f"FATAL: bundle {i} init failed: {msg}")
            db.close()  # stops the spawned align_db query tool too
            metrics.close()
            return []
        print(f"  bundle {i}: {msg}")


    print(f"\n{'=' * 60}")
    print(f"  gem5-to-BOOM Alignment Loop (parallel, {N} workers)")
    print(f"{'=' * 60}")
    print(f"  gem5 source: {GEM5_CONTAINER_SRC}  (via gem5_src_bash_<i>)")
    print(f"  BOOM source: {BOOM_SRC}  (via chipyard_bash)")
    print(f"  gem5 binary: {GEM5_BIN}  (in gem5 container)")
    print(f"  Log dir:     {LOG_DIR}")
    print(f"{'=' * 60}\n")

    # --- Baseline or rehydrate history ---
    max_iter_in_db = db.max_iteration()
    if max_iter_in_db < 0:
        print(f"\n{'=' * 60}")
        print(f"  BASELINE  [{_now()}]  (DB empty — running unmodified gem5 on bundle 0)")
        print(f"{'=' * 60}")
        baseline_iter_dir = LOG_DIR / "iter_0"
        baseline_iter_dir.mkdir(parents=True, exist_ok=True)
        t0 = time.time()
        response = get(run_gem5_comparison.options(**bundle_opts(0)).chia_remote(
            0))
        baseline_results = response["results"]
        baseline_config = response["config_contents"]
        baseline_diff = response["gem5_source_diff"]
        baseline_rev = response.get("base_rev", "") or ""
        baseline_avg = compute_avg_pct_diff(baseline_results)
        _log_timing(timing_path, 0, "baseline_comparison", time.time() - t0)

        (baseline_iter_dir / "results").mkdir(exist_ok=True)
        (baseline_iter_dir / "results" / "compare_results.json").write_text(
            json.dumps(baseline_results, indent=2))

        baseline_trace_bundles = response.get("pipe_trace_bundles", {}) or {}
        baseline_trace_summaries = response.get("pipe_trace_summaries", {}) or {}
        if baseline_trace_bundles:
            traces_dir = baseline_iter_dir / "traces"
            traces_dir.mkdir(parents=True, exist_ok=True)
            for bench, data in baseline_trace_bundles.items():
                bench_dir = traces_dir / bench
                bench_dir.mkdir(parents=True, exist_ok=True)
                (bench_dir / PIPE_TRACE_FILENAME).write_bytes(data)
                summary = baseline_trace_summaries.get(bench)
                if summary is not None:
                    (bench_dir / "summary.md").write_text(summary)

        ok_count = sum(1 for r in baseline_results if r.get("gem5_status") == "ok")
        print(f"  baseline: {ok_count}/{len(baseline_results)} ok, "
              f"avg_pct_diff={baseline_avg:.2f}%")

        if ok_count == 0:
            print("FATAL: baseline produced zero ok results. Aborting.")
            db.close()  # stops the spawned align_db query tool too
            metrics.close()
            return []

        baseline_entry_id = str(uuid4())
        db.insert_iteration(
            entry_id=baseline_entry_id, parent_id=None, iteration=0,
            avg_pct_diff=baseline_avg,
            changes="BASELINE (unmodified gem5)",
            source_changes="",
            config_contents=baseline_config,
            gem5_source_diff=baseline_diff,
            base_rev=baseline_rev,
            build_success=True, build_duration=0.0,
            llm_log_path="",
            results=baseline_results,
        )
        history.append(_iter_state_from_db_row(db.load_entry(baseline_entry_id)))
        metrics.log_scalar("avg_pct_diff", baseline_avg, step=0)
        print(f"  baseline entry_id: {baseline_entry_id[:8]}")
    else:
        print(f"\n{'=' * 60}")
        print(f"  RESUMING  [{_now()}]  (DB has {max_iter_in_db + 1} entries — "
              f"each dispatch samples its own parent from top-2)")
        print(f"{'=' * 60}")
        for row in db.all_entries():
            history.append(_iter_state_from_db_row(row))

    # --- Parallel dispatch loop ---

    iteration_counter = db.max_iteration() + 1
    dispatch_counter = 0

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=N)
    inflight: dict[concurrent.futures.Future, int] = {}

    def dispatch_to_bundle(bundle_idx: int) -> None:
        nonlocal iteration_counter, dispatch_counter
        # TEMP: parent pool is just the top-2 entries by avg_pct_diff. The
        # A/B alternation and threshold-bench logic is bypassed for now.
        top2_by_avg = db.top_k_entries(2)
        if not top2_by_avg:
            _wprint(bundle_idx, iteration_counter, "No DB entries — cannot dispatch.")
            return
        best_rows = db.best_per_benchmark()

        pool = list(top2_by_avg)
        pool_name = "top-2"
        parent = random.choice(pool)

        pool_iters = [e["iteration"] for e in pool]
        _wprint(bundle_idx, iteration_counter,
                f"pool {pool_name} (dispatch #{dispatch_counter}, "
                f"{len(pool)} entries, with multiplicity): iters={pool_iters}")
        dispatch_counter += 1

        # Root-to-parent lineage feeds the LLM context; siblings are the
        # per-benchmark best-so-far entries (deduped, lineage excluded),
        # giving the LLM a pointer to whichever iteration already leads
        # on each benchmark instead of only the globally top-avg ones.
        lineage_rows = db.lineage(parent["entry_id"])
        lineage_states = [_iter_state_from_db_row(r) for r in lineage_rows]
        lineage_ids = {r["entry_id"] for r in lineage_rows}
        sibling_entry_ids = {r["entry_id"] for r in best_rows} - lineage_ids
        sibling_entries = [db.load_entry(eid) for eid in sibling_entry_ids]
        sibling_states = [
            _iter_state_from_db_row(r) for r in sibling_entries if r
        ]
        best_table_text = build_best_per_bench_table(best_rows)
        # Re-read the prompt from disk for every dispatch so edits take
        # effect without restarting the job.
        prompt_template = ALIGN_PROMPT_PATH.read_text()
        fut = executor.submit(
            _run_iteration,
            bundle_idx, iteration_counter, parent,
            lineage_states, sibling_states, best_table_text,
            bundle_opts(bundle_idx),
            gem5_src_bashes[bundle_idx], chipyard_bash, align_db_tool,
            compile_checks[bundle_idx], quick_runs[bundle_idx],
            bench_descs, prompt_template,
        )
        inflight[fut] = bundle_idx
        iteration_counter += 1

    for i in range(N):
        dispatch_to_bundle(i)

    print(f"\n{N} dispatches in flight; draining as they complete.\n")

    try:
        while inflight:
            done_set, _ = concurrent.futures.wait(
                list(inflight.keys()),
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            for fut in done_set:
                bundle_idx = inflight.pop(fut)
                try:
                    result = fut.result()
                except Exception as e:
                    print(f"[w{bundle_idx}] iteration raised {type(e).__name__}: {e}; redispatching")
                    dispatch_to_bundle(bundle_idx)
                    continue
                _persist_iteration_result(result, db, history, metrics, timing_path)
                dispatch_to_bundle(bundle_idx)
    except KeyboardInterrupt:
        print("\nInterrupted — waiting for in-flight iterations to finish...")
        concurrent.futures.wait(list(inflight.keys()))
        # Persist anything that made it back.
        for fut in list(inflight.keys()):
            if fut.done() and not fut.cancelled() and fut.exception() is None:
                _persist_iteration_result(
                    fut.result(), db, history, metrics, timing_path)

    executor.shutdown(wait=False)
    db.close()  # stops the spawned align_db query tool too
    metrics.close()
    print(f"\nAlignment loop exited after {len(history)} recorded iterations.")
    return history




# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # address: honor RAY_ADDRESS so a `ray job submit` lands on its intended
    # cluster.  Plain "auto" scans local Ray sessions and picks the latest,
    # which mis-connects when several clusters are up on these hosts.
    ray.init(address=os.environ.get("RAY_ADDRESS", "auto"), runtime_env=_RUNTIME_ENV)
    start_collector(log_dir=str(LOG_DIR))

    try:
        history = run_alignment_loop()
    except KeyboardInterrupt:
        print("\nInterrupted")
        history = []

    print(f"\n{'=' * 60}")
    print(f"Summary — {len(history)} iterations")
    print(f"{'=' * 60}")
    for h in history:
        print(f"  Iter {h.iteration}: avg_pct_diff={h.avg_pct_diff:.2f}%")
