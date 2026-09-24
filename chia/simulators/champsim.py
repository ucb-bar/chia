"""chia.simulators.champsim — ChampSim build / run / source-state primitives.

Mirrors ``chia.simulators.gem5`` (Gem5Node), with three deliberate differences:

  1. **Binary-in-result (D-02):** The compiled ChampSim binary is embedded as
     ``bytes`` in :class:`ChampSimBuildResult` (not a path reference).  ChampSim
     static executables are small (~5 MB), so they travel through the Ray object
     store.

  2. **Split build/run scheduling (D-03):** Build and run do NOT require
     co-location.  The binary travels through the object store; the trace path
     is absolute or URI-resolved.  Only capture/restore need the git repo on
     the worker filesystem.

  3. **JSON stats (D-04):** Run results are parsed from ChampSim's ``--json``
     file output (not a text-based ``stats.txt``), with per-cache nested
     structures and derived prefetch quality metrics.

Deployment note: like ``chia.simulators.gem5``, this module must be importable
on the ChampSim worker image — the ``@ChiaFunction`` bodies reference
module-level helpers that resolve by import on the worker (they are NOT
cloudpickled by value).  Do NOT import from ``chia.simulators.gem5``; helpers
are duplicated so that this module is importable on images without gem5
installed.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
import re
import shlex
import signal
import stat
import subprocess
import tempfile
import time
from dataclasses import dataclass, field

# Ray + ChiaFunction are optional: when not installed, the pure helpers and
# dataclasses remain importable (Tier 0 tests run without Ray).  ChampSimNode
# is only available when ray is present.
try:
    import ray
    from ray.util.placement_group import (
        placement_group as _placement_group,
        remove_placement_group as _remove_placement_group,
    )
    from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

    from chia.base.ChiaFunction import ChiaFunction

    _HAS_RAY = True
except ImportError:
    _HAS_RAY = False

# ---------------------------------------------------------------------------
# Module logger
# ---------------------------------------------------------------------------

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Build-diagnostic line matcher (keep error lines + one line of context above).
_BUILD_ERROR_RE = re.compile(
    r"(error[:\s]|undefined reference|fatal error|"
    r"in (?:static |member )?function|note:)",
    re.IGNORECASE,
)

# Custom stat line matcher for prefetcher_final_stats() stdout output.
# Matches lines like "my_counter : 42" or "another_stat = 3.14".
_CUSTOM_STAT_RE = re.compile(r"^\s*(.+?)\s*[:=]\s*([\d.eE+\-]+)\s*$")

# Known ChampSim plain_printer stat prefixes.  Lines starting with
# these tokens come from ChampSim's built-in printer, not from custom
# prefetcher_final_stats() output, and must be excluded to avoid false
# positives in _extract_custom_prefetch_stats().
_PLAIN_PRINTER_PREFIXES = (
    "cpu", "CPU", "IPC", "L1D", "L1I", "L2C", "LLC", "ITLB", "DTLB",
    "STLB", "DRAM", "TOTAL", "Region of Interest",
)

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class CachePrefetchStats:
    """Per-cache prefetch counters and derived quality metrics.

    Raw counters (``requested``, ``issued``, ``useful``, ``useless``) are
    populated directly from ChampSim's ``--json`` output.  Derived metrics
    are computed by :func:`_compute_derived_metrics` after parsing and are
    ``None`` when the denominator is zero.

    Attributes:
        requested: Total prefetch requests made by the prefetcher module.
        issued: Prefetches actually issued (may be less than requested due
            to MSHR/queue limits).
        useful: Prefetched lines that were later accessed by a demand request.
        useless: Prefetched lines evicted without any demand access.
        accuracy: ``useful / issued``.  None when ``issued == 0``.
        coverage: ``useful / (useful + demand_misses)``, where demand_misses
            is ``sum(load_miss) + sum(rfo_miss)``.  None when the denominator
            is zero.
        mpki: Demand misses per kilo-instruction.  None when
            ``total_instructions == 0``.
    """
    requested: int = 0
    issued: int = 0
    useful: int = 0
    useless: int = 0
    accuracy: float | None = None
    coverage: float | None = None
    mpki: float | None = None


@dataclass
class CacheStats:
    """Per-cache statistics from a single ChampSim run.

    Each access type (LOAD, RFO, PREFETCH, WRITE, TRANSLATION) has per-level
    hit/miss arrays matching ChampSim's JSON output.  The ``prefetch`` field
    aggregates prefetch-specific counters and derived quality metrics.

    Attributes:
        name: Normalized cache name (e.g. ``"L1D"``, ``"L2C"``, ``"LLC"``).
            The ``cpu{N}_`` prefix from ChampSim JSON keys is stripped.
        prefetch: Aggregated prefetch counters and derived metrics.
        miss_latency: Average miss latency reported by ChampSim for this cache.
        load_hit: Per-level LOAD hit counts.
        load_miss: Per-level LOAD miss counts.
        rfo_hit: Per-level RFO (read-for-ownership / store) hit counts.
        rfo_miss: Per-level RFO miss counts.
        prefetch_hit: Per-level PREFETCH hit counts.
        prefetch_miss: Per-level PREFETCH miss counts.
        write_hit: Per-level WRITE hit counts.
        write_miss: Per-level WRITE miss counts.
        translation_hit: Per-level TRANSLATION hit counts.
        translation_miss: Per-level TRANSLATION miss counts.
    """
    name: str
    prefetch: CachePrefetchStats
    miss_latency: float = 0.0
    load_hit: list[int] = field(default_factory=list)
    load_miss: list[int] = field(default_factory=list)
    rfo_hit: list[int] = field(default_factory=list)
    rfo_miss: list[int] = field(default_factory=list)
    prefetch_hit: list[int] = field(default_factory=list)
    prefetch_miss: list[int] = field(default_factory=list)
    write_hit: list[int] = field(default_factory=list)
    write_miss: list[int] = field(default_factory=list)
    translation_hit: list[int] = field(default_factory=list)
    translation_miss: list[int] = field(default_factory=list)


@dataclass
class ChampSimBuildResult:
    """Result of a ChampSim build.

    The compiled binary is embedded as raw bytes (typically ~5 MB for a static
    ChampSim executable) so it can travel through the Ray object store without
    requiring filesystem co-location between builder and runner.

    Attributes:
        binary: Raw executable bytes of the compiled ChampSim binary.
            Empty on build failure.
        module_name: Name of the prefetcher module that was compiled.
        champsim_root: Filesystem path to the ChampSim checkout used for the
            build (on the worker that ran it).
        base_rev: Git HEAD of the ChampSim repo at build time, or ``""`` if
            the checkout is not a git repository.
        success: True if the build completed without error or timeout.
        returncode: Process exit code from ``make``.
        build_duration_s: Wall-clock seconds for the full build.
        stdout_tail: Last ~3 KB of combined build output (for diagnostics).
        build_diagnostics: Filtered compiler/linker error lines on failure;
            empty on success.
    """
    binary: bytes
    module_name: str
    champsim_root: str
    base_rev: str
    success: bool
    returncode: int
    build_duration_s: float
    stdout_tail: str
    build_diagnostics: str


@dataclass
class ChampSimRunResult:
    """Result of a single ChampSim trace simulation.

    Attributes:
        ipc: Instructions per cycle from the simulation phase.
        instructions: Total instructions retired in the simulation phase.
        cycles: Total cycles in the simulation phase.
        cache_stats: Per-cache statistics keyed by normalized cache name
            (e.g. ``"L1D"``, ``"L2C"``, ``"LLC"``).
        branch_mispredictions: Per-type branch misprediction counts
            (e.g. ``"BRANCH_CONDITIONAL": 5``).
        dram_stats: DRAM-level statistics (row buffer hits/misses,
            congestion cycles, etc.).
        custom_prefetch_stats: Key-value pairs extracted from custom
            ``prefetcher_final_stats()`` stdout output.  Values are strings
            to preserve the original formatting.
        success: True if the run completed and JSON stats were parseable.
        returncode: Process exit code from the ChampSim binary.
        wall_s: Wall-clock seconds for the simulation.
        stdout_tail: Last ~3 KB of simulation output (for diagnostics).
        timed_out: True if the simulation was killed due to timeout.
        raw_stats: The parsed ``--json`` output as ChampSim wrote it (every
            phase, every cache, every DRAM channel, MSHR merges), for callers
            that need what the typed fields above leave out.
    """
    ipc: float
    instructions: int
    cycles: int
    cache_stats: dict[str, CacheStats] = field(default_factory=dict)
    branch_mispredictions: dict[str, int] = field(default_factory=dict)
    dram_stats: dict[str, float] = field(default_factory=dict)
    custom_prefetch_stats: dict[str, str] = field(default_factory=dict)
    success: bool = False
    returncode: int = -1
    wall_s: float = 0.0
    stdout_tail: str = ""
    timed_out: bool = False
    raw_stats: list = field(default_factory=list)


@dataclass
class ChampSimSourceState:
    """Portable snapshot of edits to a ChampSim checkout.

    Used to ship prefetcher source changes between workers without transferring
    the full git repo.  Apply with :meth:`ChampSimNode.restore_champsim_source_state`.

    Attributes:
        base_rev: Git commit SHA the diff was taken against.
        source_diff: Unified diff (``git diff`` output) scoped to the
            prefetcher directory.
    """
    base_rev: str
    source_diff: str


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
    SIGKILL to the shell leaves grandchildren (g++, champsim) holding the
    captured pipes open and the call stalls in cleanup.
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


# ---------------------------------------------------------------------------
# Stats parsing functions
# ---------------------------------------------------------------------------

def _parse_champsim_json(
    json_data: list[dict],
    total_instructions: int | None = None,
) -> tuple[dict[str, CacheStats], dict]:
    """Parse ChampSim ``--json`` output into typed cache stats and core info.

    Parameters
    ----------
    json_data
        The parsed JSON list (one dict per phase: Warmup, Simulation, ...).
    total_instructions
        Override for instruction count used in derived metrics.  If *None*,
        the value is read from ``roi.cores[0].instructions``.

    Returns
    -------
    (cache_stats, core_info)
        *cache_stats* maps normalized cache name (``L1D``, ``L2C``, ``LLC``)
        to a :class:`CacheStats`.  *core_info* carries ``instructions``,
        ``cycles``, ``ipc``, ``branch_mispredictions``, and ``dram_stats``.
    """
    # Find the Simulation phase; fall back to last phase.
    phase = None
    for entry in json_data:
        if entry.get("name") == "Simulation":
            phase = entry
            break
    if phase is None and json_data:
        phase = json_data[-1]
    if phase is None:
        return {}, {}

    roi = phase.get("roi", {})

    # --- Core stats ---
    cores = roi.get("cores", [{}])
    core0 = cores[0] if cores else {}
    instructions = core0.get("instructions", 0)
    cycles = core0.get("cycles", 0)
    ipc = instructions / cycles if cycles > 0 else 0.0

    if total_instructions is None:
        total_instructions = instructions

    # Branch mispredictions
    branch_mispredictions: dict[str, int] = {}
    mispredict = core0.get("mispredict", {})
    for k, v in mispredict.items():
        branch_mispredictions[k] = int(v)

    # DRAM stats
    dram_stats: dict[str, float] = {}
    dram_list = roi.get("DRAM", [])
    if dram_list:
        dram0 = dram_list[0] if isinstance(dram_list, list) else dram_list
        for k, v in dram0.items():
            try:
                dram_stats[k] = float(v)
            except (ValueError, TypeError):
                pass

    # --- Per-cache stats ---
    cache_stats: dict[str, CacheStats] = {}
    for key, val in roi.items():
        if not isinstance(val, dict):
            continue
        # Identify cache entries by presence of "prefetch requested" field.
        if "prefetch requested" not in val:
            continue

        # Normalize name: strip cpu{N}_ prefix (Pitfall 8).
        normalized_name = re.sub(r"^cpu\d+_", "", key)

        pf = CachePrefetchStats(
            requested=int(val.get("prefetch requested", 0)),
            issued=int(val.get("prefetch issued", 0)),
            useful=int(val.get("useful prefetch", 0)),
            useless=int(val.get("useless prefetch", 0)),
        )

        cs = CacheStats(
            name=normalized_name,
            prefetch=pf,
            miss_latency=float(val.get("miss latency") or 0.0),
        )

        # Parse per-access-type hit/miss/mshr_merge arrays.
        for access_type, field_prefix in [
            ("LOAD", "load"),
            ("RFO", "rfo"),
            ("PREFETCH", "prefetch"),
            ("WRITE", "write"),
            ("TRANSLATION", "translation"),
        ]:
            at_data = val.get(access_type, {})
            setattr(cs, f"{field_prefix}_hit", list(at_data.get("hit", [])))
            setattr(cs, f"{field_prefix}_miss", list(at_data.get("miss", [])))

        # Compute derived metrics.
        _compute_derived_metrics(cs, total_instructions)

        cache_stats[normalized_name] = cs

    core_info = {
        "instructions": instructions,
        "cycles": cycles,
        "ipc": ipc,
        "branch_mispredictions": branch_mispredictions,
        "dram_stats": dram_stats,
    }

    return cache_stats, core_info


def _compute_derived_metrics(
    cache_stats: CacheStats,
    total_instructions: int,
) -> None:
    """Mutate *cache_stats*.prefetch with derived accuracy/coverage/MPKI.

    Per D-05 and Pitfall 7:
    - accuracy = useful / issued  (None if issued == 0)
    - demand_misses = sum(load_miss) + sum(rfo_miss)
    - coverage = useful / (useful + demand_misses)  (None if denominator == 0)
    - mpki = demand_misses * 1000 / total_instructions  (None if instructions == 0)
    """
    pf = cache_stats.prefetch

    # Accuracy: what fraction of issued prefetches were useful.
    if pf.issued > 0:
        pf.accuracy = pf.useful / pf.issued

    # Demand misses = LOAD misses + RFO misses (NOT prefetch or write misses).
    demand_misses = sum(cache_stats.load_miss) + sum(cache_stats.rfo_miss)

    # Coverage: what fraction of would-be misses were served by prefetch.
    if pf.useful + demand_misses > 0:
        pf.coverage = pf.useful / (pf.useful + demand_misses)

    # MPKI: demand misses per kilo-instruction.
    if total_instructions > 0:
        pf.mpki = demand_misses * 1000 / total_instructions


def _extract_custom_prefetch_stats(stdout: str) -> dict[str, str]:
    """Extract custom counters from ``prefetcher_final_stats()`` stdout output.

    In DPC4-ChampSim ``main.cc``, the call order is:

      1. ``plain_printer`` -> structured text stats to stdout
      2. ``impl_prefetcher_final_stats()`` -> custom stdout
      3. ``impl_replacement_final_stats()`` -> replacement stdout
      4. ``json_printer`` -> JSON to file or stdout

    Custom stats appear after the "ChampSim completed all CPUs" marker and
    before JSON output.  Pattern is implementation-defined by each prefetcher
    module.
    """
    stats: dict[str, str] = {}
    in_custom = False
    for line in stdout.splitlines():
        if "ChampSim completed all CPUs" in line:
            in_custom = True
            continue
        if in_custom and line.strip():
            if line.strip().startswith("[") or line.strip().startswith("{"):
                break
            # Skip known ChampSim plain_printer output lines that
            # appear between the "completed" marker and custom stats.
            stripped = line.strip()
            if any(stripped.startswith(p) for p in _PLAIN_PRINTER_PREFIXES):
                continue
            m = _CUSTOM_STAT_RE.match(line)
            if m:
                stats[m.group(1).strip()] = m.group(2).strip()
    return stats


def _resolve_trace(trace: str) -> str:
    """Resolve a trace path or URI to a local filesystem path.

    Supports local paths and ``s3://`` URIs.  ``gs://`` URIs raise
    :class:`NotImplementedError` (full TraceSource abstraction is future
    scope).

    Raises
    ------
    FileNotFoundError
        If the trace is a local path that does not exist.
    NotImplementedError
        If the trace uses a ``gs://`` URI scheme.
    """
    if os.path.isfile(trace):
        return trace

    if trace.startswith("s3://"):
        import boto3  # noqa: lazy import for optional dependency
        from urllib.parse import urlparse
        parsed = urlparse(trace)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        # Include a hash of the full key to avoid basename collisions
        # across different S3 prefixes.
        key_hash = hashlib.sha256(key.encode()).hexdigest()[:12]
        basename = os.path.basename(key)
        local = os.path.join(
            tempfile.gettempdir(), f"{key_hash}_{basename}",
        )
        if not os.path.isfile(local):
            s3 = boto3.client("s3")
            s3.download_file(bucket, key, local)
        return local

    if trace.startswith("gs://"):
        raise NotImplementedError(
            f"GCS trace URIs not yet supported: {trace}"
        )

    raise FileNotFoundError(f"Trace not found: {trace}")


# ---------------------------------------------------------------------------
# Per-instance binding wrapper (duplicated from gem5.py — do NOT import)
# ---------------------------------------------------------------------------
# Everything below requires Ray + ChiaFunction.  When ray is not installed
# (e.g. running Tier 0 tests that only exercise pure functions), these
# classes are simply not defined.  Callers should import ChampSimNode via
# chia.simulators.__init__ which already guards with try/except.

if _HAS_RAY:

    class _PinnedChiaFn:
        """Exposes a ``@ChiaFunction`` with ``.chia_remote`` pre-pinned to a
        placement-group bundle.  Resource requirements are carried over unchanged
        by ``.options()``; with no scheduling opts it delegates to the raw
        function so the caller's own placement applies."""

        def __init__(self, fn, scheduling_opts: dict):
            self._fn = fn
            self._opts = dict(scheduling_opts) if scheduling_opts else {}
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

    # -----------------------------------------------------------------------
    # ChampSimNode
    # -----------------------------------------------------------------------

    class ChampSimNode:
        """ChampSim build / run / source-state primitives sharing one placement.

        The four core operations are ``@staticmethod @ChiaFunction(resources=
        {"champsim": 1.0})`` members; ``__init__`` re-binds each into a
        per-instance :class:`_PinnedChiaFn` so
        ``node.<op>.chia_remote(...)`` lands on this node's bundle.
        ``ChampSimNode.<op>.chia_remote(...)`` (the class attribute) is the raw,
        unpinned form.

        Unlike :class:`Gem5Node`, the compiled binary is small (~5 MB) and
        embedded as bytes in :class:`ChampSimBuildResult` (D-02), so build and
        run do NOT require co-location (D-03).  Placement groups still
        coordinate capture/restore (which need the git repo on the worker
        filesystem) and provide a consistent scheduling interface for all four
        operations.

        Co-location behaviour mirrors Gem5Node:

          * ``placement_group`` given        -> pin members to ``bundle_index``
            of it (the node will NOT release a PG it did not create).
          * none + ``require_colocated=True`` -> reserve a 1-bundle
            ``{"champsim": 1, "CPU": 1}`` PG (owned + released by this node).
          * none + ``require_colocated=False`` -> no pinning; the caller
            schedules each ``.chia_remote`` / ``.options(...)`` call itself.

        Usable as a context manager so a self-reserved PG is released on exit.
        """

        # Names of the @ChiaFunction members re-bound per instance in __init__.
        _MEMBER_FNS = (
            "build_champsim",
            "run_champsim",
            "capture_champsim_source_state",
            "restore_champsim_source_state",
        )
        _DEFAULT_BUNDLE = {"CPU": 1, "champsim": 1.0}

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
                placement_group: an existing Ray ``PlacementGroup`` to schedule
                    onto.  If given, ``require_colocated`` is moot (placement is
                    already fixed) and this node will not remove the PG on close.
                require_colocated: when no PG is given, reserve one so all
                    members co-locate. When False, leave placement to the caller.
                bundle_index: which bundle of the (given or reserved) PG to pin
                    to.
                reserve_bundle: resource shape of a self-reserved bundle
                    (default ``{"CPU": 1, "champsim": 1.0}``); must provide
                    ``champsim`` >= each member's requirement (1.0).
                pg_strategy: placement strategy for a self-reserved PG.
                wait_for_pg: block on ``pg.ready()`` for a self-reserved PG so
                    the node is usable immediately.
                pg_ready_timeout_s: optional timeout for that wait.
            """
            self._owns_pg = False
            self._bundle_index = bundle_index

            if placement_group is not None:
                self._pg = placement_group
            elif require_colocated:
                bundle = reserve_bundle or dict(self._DEFAULT_BUNDLE)
                if bundle.get("champsim", 0) < 1.0:
                    raise ValueError(
                        f"reserve_bundle must provide champsim>=1.0 for member "
                        f"tasks; got {bundle!r}"
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

            # Re-bind each class-level @ChiaFunction into a pinned instance
            # member: node.<fn>.chia_remote == <fn>.options(<sched>).chia_remote
            for name in self._MEMBER_FNS:
                setattr(
                    self, name,
                    _PinnedChiaFn(getattr(type(self), name), self._sched_opts),
                )

        # -- placement-group lifecycle ----------------------------------------

        @property
        def placement_group(self):
            """The PG members are pinned to (None when the caller handles
            placement)."""
            return self._pg

        @property
        def owns_placement_group(self) -> bool:
            """True iff this node reserved its PG and will release it on
            close()."""
            return self._owns_pg

        @property
        def task_options(self) -> dict:
            """Scheduling opts to co-locate an actor (e.g. a ``ChiaTool``) with
            this node's bundle.  Empty when the node has no placement group
            (``require_colocated=False``)."""
            return dict(self._sched_opts)

        def close(self) -> None:
            """Release the PG iff this node reserved it.  Idempotent."""
            if self._owns_pg and self._pg is not None:
                _remove_placement_group(self._pg)
                self._pg = None
                self._owns_pg = False

        def __enter__(self) -> "ChampSimNode":
            return self

        def __exit__(self, *exc) -> None:
            self.close()

        # -- nodes (champsim-resourced; pinned per-instance via __init__) -----

        @staticmethod
        @ChiaFunction(resources={"champsim": 1.0})
        def build_champsim(
            champsim_root: str,
            prefetcher_src: str,
            module_name: str,
            *,
            cache_level: str = "L2C",
            timeout_s: int = 600,
            incremental: bool = False,
        ) -> ChampSimBuildResult:
            """Build ChampSim with a custom prefetcher module.

            Writes ``prefetcher_src`` to
            ``prefetcher/{module_name}/{module_name}.h``, generates a config
            JSON targeting ``cache_level``, then runs
            ``make clean && config.sh && make``.  The compiled binary is read
            as bytes and embedded in the result so it can be shipped to any
            worker for simulation.

            Args:
                champsim_root: Path to the DPC4-ChampSim checkout on the
                    worker filesystem.
                prefetcher_src: Complete C++ header source for the prefetcher
                    module (written as ``{module_name}.h``).
                module_name: Identifier for the prefetcher (must match
                    ``[a-zA-Z_][a-zA-Z0-9_]*``).
                cache_level: Which cache level to attach the prefetcher to.
                    One of ``L1D``, ``L1I``, ``L2C``, ``LLC``, ``ITLB``,
                    ``DTLB``, ``STLB``.  Defaults to ``"L2C"``.
                timeout_s: Maximum wall-clock seconds for the build before
                    it is killed.
                incremental: Skip ``make clean`` and ``config.sh``, relying
                    on make's dependency tracking.  Safe when only the
                    prefetcher header changes between builds and the module
                    name / cache level stay constant.  Much faster (~30s vs
                    ~10min) on single-CPU workers.

            Returns:
                A :class:`ChampSimBuildResult` with the compiled binary bytes
                (on success) or diagnostic output (on failure).

            Raises:
                ValueError: If ``module_name`` fails validation or the
                    compiled binary exceeds 50 MB.
            """
            # T-02-04: validate module_name to prevent path traversal.
            if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", module_name):
                raise ValueError(
                    f"Invalid module_name {module_name!r}: must match "
                    f"[a-zA-Z_][a-zA-Z0-9_]*"
                )

            # Validate cache_level against known ChampSim cache names.
            _VALID_CACHE_LEVELS = {
                "L1D", "L1I", "L2C", "LLC", "ITLB", "DTLB", "STLB",
            }
            if cache_level not in _VALID_CACHE_LEVELS:
                raise ValueError(
                    f"Invalid cache_level {cache_level!r}: must be one of "
                    f"{sorted(_VALID_CACHE_LEVELS)}"
                )

            # Capture git HEAD before any changes.
            base_rev = ""
            rev = _git(["rev-parse", "HEAD"], champsim_root, timeout=30)
            if rev.returncode == 0:
                base_rev = rev.stdout.strip()

            # Write prefetcher source as a header-only module.
            # DPC4-ChampSim's generated_environment.cc #includes the .h
            # to get the full struct definition; all method bodies are
            # inline so no separate .cc is needed.
            module_dir = os.path.join(
                champsim_root, "prefetcher", module_name,
            )
            os.makedirs(module_dir, exist_ok=True)
            header_path = os.path.join(module_dir, f"{module_name}.h")
            with open(header_path, "w") as f:
                f.write(prefetcher_src)

            # Generate config JSON with the specified cache level (Pitfall 4).
            config = {
                "executable_name": "champsim",
                cache_level: {"prefetcher": module_name},
            }
            config_fd, config_path = tempfile.mkstemp(
                suffix=".json", prefix="champsim_config_",
            )
            try:
                with os.fdopen(config_fd, "w") as cf:
                    json.dump(config, cf)

                # D-07: always run config.sh + make together.
                # Full clean prevents stale object files (Pitfall 5) but is
                # expensive on single-CPU workers.  Incremental mode skips
                # clean — safe when only the prefetcher source changes,
                # because config.sh regenerates core_inst.cc.inc which make's
                # dependency tracking picks up.
                # Quote config_path to handle spaces in TMPDIR.
                if incremental:
                    # Skip config.sh — it generates a new build hash each
                    # run, forcing main.cc to recompile (~10 min).  The
                    # Docker image already ran config.sh with the correct
                    # module/cache config; only the prefetcher header
                    # changes between iterations, and make's dependency
                    # tracking handles that.
                    cmd = f"make -j$(nproc)"
                else:
                    cmd = (
                        f"make clean && python3 ./config.sh {shlex.quote(config_path)} "
                        f"&& make -j$(nproc)"
                    )
                rc, stdout, stderr, timed_out, wall = _run_logged(
                    cmd, champsim_root, timeout_s,
                )
            finally:
                try:
                    os.unlink(config_path)
                except OSError:
                    pass

            success = (rc == 0) and not timed_out

            # Read the compiled binary (D-02).
            binary = b""
            if success:
                bin_path = os.path.join(champsim_root, "bin", "champsim")
                try:
                    with open(bin_path, "rb") as bf:
                        binary = bf.read()
                except OSError as e:
                    success = False
                    stderr += f"\nFailed to read binary: {e}"

            # T-02-06: reject oversized binaries (only check on successful read).
            if success and len(binary) > 50_000_000:
                raise ValueError(
                    f"Compiled binary too large "
                    f"({len(binary)} bytes > 50 MB limit); "
                    f"possible build misconfiguration"
                )

            build_diagnostics = ""
            if not success:
                if timed_out:
                    build_diagnostics = (
                        f"TIMEOUT after {wall:.0f}s (limit {timeout_s}s)"
                    )
                else:
                    build_diagnostics = _filter_build_diagnostics(
                        stdout, stderr,
                    )

            return ChampSimBuildResult(
                binary=binary,
                module_name=module_name,
                champsim_root=champsim_root,
                base_rev=base_rev,
                success=success,
                returncode=rc,
                build_duration_s=wall,
                stdout_tail=stdout[-3000:],
                build_diagnostics=build_diagnostics,
            )

        @staticmethod
        @ChiaFunction(resources={"champsim": 1.0})
        def run_champsim(
            binary: bytes,
            trace: str,
            *,
            warmup_instructions: int = 5_000_000,
            simulation_instructions: int = 25_000_000,
            timeout_s: int = 300,
        ) -> ChampSimRunResult:
            """Run a ChampSim binary against a trace and parse structured stats.

            Writes *binary* to a content-hashed temp path (idempotent across
            concurrent runs), resolves *trace* (local path or S3 URI), runs
            the simulation with ``--json`` file output, and parses results
            into per-cache nested stats with derived prefetch quality metrics.

            Args:
                binary: Raw executable bytes from a prior
                    :meth:`build_champsim` call.
                trace: Path to a ChampSim trace file.  Accepts local
                    filesystem paths and ``s3://`` URIs (downloaded
                    automatically).
                warmup_instructions: Number of instructions for the warmup
                    phase (not included in reported stats).
                simulation_instructions: Number of instructions for the
                    measured simulation phase.
                timeout_s: Maximum wall-clock seconds before the simulation
                    is killed.

            Returns:
                A :class:`ChampSimRunResult` with IPC, per-cache stats,
                branch mispredictions, DRAM stats, and any custom prefetcher
                counters.
            """
            # Write binary to content-hashed temp path
            # (Pitfall 3: chmod +x after write).
            content_hash = hashlib.sha256(binary).hexdigest()[:12]
            binary_path = os.path.join(
                tempfile.gettempdir(), f"champsim_{content_hash}",
            )

            # Idempotent write with flock (verilator_run_node pattern).
            lock_path = binary_path + ".lock"
            lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR)
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            try:
                os.lseek(lock_fd, 0, os.SEEK_SET)
                stored = os.read(lock_fd, 64).decode().strip()

                if stored != content_hash:
                    with open(binary_path, "wb") as bf:
                        bf.write(binary)
                    os.chmod(
                        binary_path,
                        os.stat(binary_path).st_mode
                        | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH,
                    )
                    os.ftruncate(lock_fd, 0)
                    os.lseek(lock_fd, 0, os.SEEK_SET)
                    os.write(lock_fd, content_hash.encode())
            finally:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                os.close(lock_fd)

            # Resolve trace path (local / S3 / GCS).
            resolved_trace = _resolve_trace(trace)

            # Create temp file for JSON stats output.
            stats_fd, stats_path = tempfile.mkstemp(
                suffix=".json", prefix="champsim_stats_",
            )
            os.close(stats_fd)

            try:
                # Build command as a list (T-02-08: no shell injection).
                cmd = [
                    binary_path,
                    "--warmup-instructions", str(warmup_instructions),
                    "--simulation-instructions", str(simulation_instructions),
                    "--json", stats_path,
                    resolved_trace,
                ]
                rc, stdout, stderr, timed_out, wall = _run_logged(
                    cmd, cwd=None, timeout_s=timeout_s,
                )

                success = (rc == 0) and not timed_out

                if success:
                    try:
                        with open(stats_path) as sf:
                            json_data = json.load(sf)
                    except (OSError, json.JSONDecodeError) as e:
                        _logger.warning(
                            "Failed to parse ChampSim JSON stats: %s", e,
                        )
                        json_data = []
                        success = False

                if success:
                    cache_stats, core_info = _parse_champsim_json(json_data)
                    custom = _extract_custom_prefetch_stats(stdout)

                    return ChampSimRunResult(
                        ipc=core_info.get("ipc", 0.0),
                        instructions=core_info.get("instructions", 0),
                        cycles=core_info.get("cycles", 0),
                        cache_stats=cache_stats,
                        branch_mispredictions=core_info.get(
                            "branch_mispredictions", {},
                        ),
                        dram_stats=core_info.get("dram_stats", {}),
                        custom_prefetch_stats=custom,
                        success=True,
                        returncode=rc,
                        wall_s=wall,
                        stdout_tail=stdout[-3000:],
                        timed_out=False,
                        raw_stats=json_data,
                    )
                else:
                    return ChampSimRunResult(
                        ipc=0.0,
                        instructions=0,
                        cycles=0,
                        success=False,
                        returncode=rc,
                        wall_s=wall,
                        stdout_tail=(stdout + "\n" + stderr)[-3000:],
                        timed_out=timed_out,
                    )
            finally:
                try:
                    os.unlink(stats_path)
                except OSError:
                    pass

        @staticmethod
        @ChiaFunction(resources={"champsim": 1.0})
        def capture_champsim_source_state(
            champsim_root: str,
            *,
            base_rev: str | None = None,
            diff_paths: list[str] | None = None,
        ) -> ChampSimSourceState:
            """Capture a portable snapshot of prefetcher edits.

            Computes ``git diff <base_rev> -- <diff_paths>``, first marking
            untracked files under those paths as intent-to-add so brand-new
            files (e.g. a new prefetcher module's ``.h``) are included,
            then un-staging them to leave the index clean.

            Args:
                champsim_root: Path to the DPC4-ChampSim checkout.
                base_rev: Git commit to diff against.  Defaults to HEAD.
                diff_paths: Paths to include in the diff (default:
                    ``["prefetcher/"]``).

            Returns:
                A :class:`ChampSimSourceState` that can be shipped to another
                worker and applied with :meth:`restore_champsim_source_state`.
            """
            paths = diff_paths or ["prefetcher/"]

            if base_rev is None:
                rev = _git(
                    ["rev-parse", "HEAD"], champsim_root, timeout=30,
                )
                base_rev = rev.stdout.strip() if rev.returncode == 0 else ""

            ls = _git(
                ["ls-files", "--others", "--exclude-standard", "--", *paths],
                champsim_root,
            )
            untracked = [p for p in ls.stdout.splitlines() if p]
            if untracked:
                _git(
                    ["add", "-N", "--", *untracked],
                    champsim_root,
                    timeout=60,
                )
            try:
                diff_proc = _git(
                    ["diff", base_rev, "--", *paths],
                    champsim_root,
                    timeout=60,
                )
                source_diff = diff_proc.stdout
            finally:
                if untracked:
                    _git(
                        ["reset", "--quiet", "--", *untracked],
                        champsim_root,
                        timeout=60,
                    )

            return ChampSimSourceState(
                base_rev=base_rev,
                source_diff=source_diff,
            )

        @staticmethod
        @ChiaFunction(resources={"champsim": 1.0})
        def restore_champsim_source_state(
            champsim_root: str,
            state: ChampSimSourceState,
            *,
            restore_paths: list[str] | None = None,
        ) -> tuple[bool, str]:
            """Restore a previously captured source state onto a ChampSim checkout.

            Resets the specified paths to ``state.base_rev``, cleans untracked
            files, then applies the captured diff.  Call :meth:`build_champsim`
            afterward to compile the restored source.

            Args:
                champsim_root: Path to the DPC4-ChampSim checkout.
                state: A :class:`ChampSimSourceState` from a prior
                    :meth:`capture_champsim_source_state` call.
                restore_paths: Paths to reset and apply the diff to
                    (default: ``["prefetcher/"]``).

            Returns:
                A ``(ok, message)`` tuple.  ``ok`` is True on success.
            """
            paths = restore_paths or ["prefetcher/"]

            if state.base_rev:
                co = _git(
                    ["checkout", state.base_rev, "--", *paths],
                    champsim_root,
                )
                if co.returncode != 0:
                    return False, f"git checkout failed: {co.stderr[-500:]}"
            clean = _git(
                ["clean", "-fd", "--", *paths],
                champsim_root,
                timeout=60,
            )
            if clean.returncode != 0:
                return False, f"git clean failed: {clean.stderr[-500:]}"

            if state.source_diff.strip():
                apply = subprocess.run(
                    ["git", "apply", "-"],
                    input=state.source_diff,
                    cwd=champsim_root,
                    capture_output=True,
                    text=True,
                    timeout=120,
                )
                if apply.returncode != 0:
                    return (
                        False,
                        f"git apply failed: {apply.stderr[-500:]}",
                    )

            n = len(state.source_diff.splitlines())
            return (
                True,
                f"restored to {state.base_rev[:10] or 'HEAD'} "
                f"+ {n}-line diff",
            )
