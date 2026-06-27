"""Configuration for the gem5-to-BOOM alignment loop example.

All tunable paths and knobs live here so ``gem5_align_loop.py`` stays free of
site-specific constants.  What a new user must set:

  * ``GEM5_ALIGN_VERILATOR_CACHE`` (REQUIRED — no default) — directory holding the
    verilator golden cache (``<bench>.log`` + ``<bench>.out`` per benchmark).
    Point it at pre-computed goldens to reuse them, or at a fresh writable dir to
    have the first run generate them (see ``ensure_verilator_cache`` in the loop).

  * ``GEM5_ALIGN_BENCH_ROOT`` (optional) — filesystem path to the microbenchmark
    checkout.  Defaults to ``examples/benchmarks/ubench`` (the vendored submodule).
    It holds the sources (``<bench>/bench.c`` + ``<bench>/desc.txt``) and, after
    ``make`` there, the compiled images under ``build/`` (``<bench>.gem5.elf`` +
    ``<bench>.verilator.riscv``).

  * ``GEM5_ALIGN_LOG_DIR`` (optional) — where iteration logs + ``alignment.db``
    are written on the head node.  Defaults to ``./align_loop_logs-{CONFIG_SLUG}``.

``run_compare.py`` and ``baseline_megaboom_conf.py`` are vendored alongside this
file, so they resolve relative to it regardless of cwd.
"""

from __future__ import annotations

import os
from pathlib import Path

import chia

# Resolve repo-relative paths from the importable (editable) chia package rather
# than __file__, so they stay correct even when the loop runs from a
# `ray job submit` working_dir snapshot (where __file__ lives under a temp
# runtime_resources/ dir, not the repo).
_HERE = Path(__file__).resolve().parent
# chia is a PEP 660 editable / namespace package (chia.__file__ is None), so take
# the package directory from __path__.  Depending on the setuptools version,
# __path__ can list BOTH the repo root and the package dir (and a finder-hook
# entry); pick the one that actually IS the chia package — the dir containing the
# `base` subpackage — never the repo root or the finder hook.
_CHIA_PKG_DIR = next(
    Path(p).resolve() for p in chia.__path__
    if Path(p).is_dir() and (Path(p) / "base").is_dir()
)
_REPO = _CHIA_PKG_DIR.parent                          # <repo> (has chia/ + examples/)
_EXAMPLE = _REPO / "examples" / "gem5_align"

# --- Config identity (EDIT THESE TOGETHER WHEN SWITCHING CONFIGS) ----------
# BUILD_CONFIG is the full Chipyard config class name used for verilator builds
# and surfaced to the LLM via the {BUILD_CONFIG} prompt placeholder.
# CONFIG_SLUG is a short kebab-case tag that suffixes on-disk locations so
# multiple configs can coexist without clobbering each other's artifacts.
# BUILD_CONFIG = "MediumBoomV3HumanCommitLogTMAConfig"
# CONFIG_SLUG  = "mediumboom"
BUILD_CONFIG = "SmallBoomV3HumanCommitLogTMAConfig"
CONFIG_SLUG  = "smallboom"
# --------------------------------------------------------------------------

# --- Benchmark checkout (the `benchmarks` submodule's ubench/) -------------
# BENCH_ROOT is the ubench directory: it holds the microbenchmark sources
# (<bench>/bench.c + desc.txt) and, after `make` there, the compiled images
# under build/ (<bench>.gem5.elf + <bench>.verilator.riscv).  Defaults to the
# vendored submodule at <repo>/examples/benchmarks/ubench; override with
# GEM5_ALIGN_BENCH_ROOT.
BENCH_ROOT       = Path(os.environ.get("GEM5_ALIGN_BENCH_ROOT",
                                       _REPO / "examples" / "benchmarks" / "ubench"))
MICROBENCH       = BENCH_ROOT                       # <bench>/bench.c + desc.txt
UBENCH_BUILD     = BENCH_ROOT / "build"             # <bench>.{gem5.elf,verilator.riscv}
# Verilator golden cache (REQUIRED — no default; set GEM5_ALIGN_VERILATOR_CACHE).
# Holds <bench>.log + <bench>.out per benchmark.  Point at pre-computed goldens to
# reuse them (skips the chisel build + per-bench verilator runs), or at a fresh
# writable dir to (re)generate the cache on first run (see ensure_verilator_cache).
VERILATOR_CACHE  = Path(os.environ.get(
    "GEM5_ALIGN_VERILATOR_CACHE"))

# Vendored scripts/config — resolved against the real example dir (not a ray
# working_dir snapshot) so the head reads the committed copies.
RUN_COMPARE      = _EXAMPLE / "run_compare.py"
GEM5_CONFIG      = _EXAMPLE / "baseline_megaboom_conf.py"

# Optional DRAMsim INI dir shipped to verilator runs.  Absent by default (the
# loop then passes empty INI files, matching the original behavior).
DRAMSIM_INI_DIR  = _EXAMPLE / "dramsim_ini"

# Head-node output: iteration logs + alignment.db.  Resolved against the real
# example dir so it persists (not into an ephemeral ray working_dir snapshot).
# Override with GEM5_ALIGN_LOG_DIR.
LOG_DIR = Path(os.environ.get("GEM5_ALIGN_LOG_DIR", _EXAMPLE / f"align_loop_logs-{CONFIG_SLUG}"))

# Benchmarks excluded from the gem5<->verilator comparison.
#   MM, MM_st  — ISA-level instruction count differences; excluded before we
#                began our alignment flow, and we did not want to introduce them
#                into the flow later on.
#   CRd, M_Dyn — could not be run in Verilator on the BOOM core before we began
#                our alignment flow, and we did not want to introduce them into
#                the flow later on.
EXCLUDED_BENCHMARKS = {"MM", "MM_st", "CRd", "M_Dyn"}

# --- Loop tuning -----------------------------------------------------------
DEBUG_MAX_RETRIES = 5

# Upper bound on concurrent iterations (one in-flight per gem5 bundle).  The
# cluster may advertise more gem5 nodes than this; the rest stay idle.  Lower =
# less simultaneous LLM + gem5-build + trace-shipping pressure; higher = more
# exploration per wall-clock hour.
MAX_PARALLEL_ITERATIONS = 3

METRICS_CONFIG = {
    "backend": "tensorboard",
    "log_dir": str(LOG_DIR / "metrics"),
}

# Max decompressed size of an O3PipeView trace shipped back from a worker.
PIPE_TRACE_MAX_DECOMPRESSED_BYTES = 20 * 1024 * 1024

# --- gem5 build identity (canonical chia.simulators.gem5) ------------------
GEM5_ISA     = "RISCV"
GEM5_VARIANT = "opt"

# --- In-container paths (gem5 worker: ghcr.io/ucb-bar/chia-gem5) -----------
# Paths the LLM is allowed to write to (everything else is read-only).
GEM5_CONTAINER_SRC  = "/home/ray/gem5/src"
GEM5_CONTAINER_ROOT = "/home/ray/gem5"

# The gem5 binary is built into the image root on first boot; the loop reaches
# it via the in-container path below (== build/{ISA}/gem5.{VARIANT}).
GEM5_BIN = Path(f"{GEM5_CONTAINER_ROOT}/build/{GEM5_ISA}/gem5.{GEM5_VARIANT}")

# In-container workspace on the gem5 worker.  Binaries, sources, run_compare,
# and the gem5 config are materialized here by ``init_gem5_worker``; the LLM
# edits the config in place via ``gem5_src_bash``.
WORKER_BENCH_ROOT    = "/home/ray/bench_workspace"
WORKER_RUN_COMPARE   = f"{WORKER_BENCH_ROOT}/run_compare.py"
WORKER_CONFIG        = f"{WORKER_BENCH_ROOT}/baseline_megaboom_conf.py"
WORKER_GEM5_BASE_REV = f"{WORKER_BENCH_ROOT}/.gem5_base_rev"
# Where each bundle stores the *parent* iteration's pipeline traces so the
# aligning LLM can inspect them via gem5_src_bash.  Rewritten at the start of
# every iteration to match whichever parent was sampled.
WORKER_PARENT_TRACES = f"{WORKER_BENCH_ROOT}/parent_traces"

# gem5 O3PipeView emits one debug line per stage per instruction.  Filename is
# relative to each benchmark's --outdir, so every bench gets its own trace.
PIPE_TRACE_FILENAME = "pipe_trace.gz"

# --- In-container paths (chipyard worker: ghcr.io/ucb-bar/chia-chisel-build)-
CHIPYARD_PATH       = "/home/ray/chipyard/"
BOOM_SRC            = "/home/ray/chipyard/generators/boom/src/main/scala/v3/"
CHIPYARD_GENERATORS = "/home/ray/chipyard/generators"
