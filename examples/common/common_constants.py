"""Constants for the single-opt pipeline.

Shared constants (``CHIPYARD_PATH``, ``FLATDB_PATH``, ``OPT_NUMBER``,
``WARM_START_THRESHOLD_SIZE``, ``METRICS_CONFIG``, ``UBENCH_DIR``,
``BOOM_REPO_PATH``, ``TOP_N_STALE_DEAD_END``, ``LOG_DIR``) are duplicated
from the original constants module so the single-opt pipeline can be tuned
independently of the multi-backend pipeline. Keep values in sync with
the original constants module unless you intentionally want them to diverge.
"""

from pathlib import Path


# --- Shared constants (duplicated from the original constants module) --

CHIPYARD_PATH = "/home/ray/chipyard/"
BOOM_REPO_PATH = str(Path(CHIPYARD_PATH) / "generators/boom/src/main/scala/v3/")

# set to 0 to disable warm start and enqueue all existing branches
WARM_START_THRESHOLD_SIZE = 40

# number of stale "dead-end" branches to enqueue to baseline warm start
# dead-end is a stale branch at the frontier of the stale tree
TOP_N_STALE_DEAD_END = 0

# Number of parallel optimization paths per iteration. Controls:
#   - How many candidates deep_research produces (N parameter to the command)
#   - How many chipyard_bash tools are deployed
#   - How many implementation+build+test pipelines run in parallel
#   - Cluster config should have this many chipyard nodes
OPT_NUMBER = 4

# Root of the chipyard tests directory; benchmarks loaded from verilatorbins/ subdirs.
UBENCH_DIR = Path("/path/to/")

LOG_DIR = "/path/to/chia-logging/four_wide_log"
FLATDB_PATH = "/path/to/chia-logging/databases/flatdb"

# Aliases / sibling log root for gather_past_db_attempts. The single-opt
# pipeline writes its own logs under LOG_DIR (== FOUR_WIDE_LOG_DIR), but
# past attempts may also live in the multi-backend pipeline's log root —
# both are passed to /gather_past_db_attempts so it can scan both trees.
FOUR_WIDE_LOG_DIR = LOG_DIR



METRICS_CONFIG = {
    "backend": "tensorboard",
    "log_dir": f"{LOG_DIR}/metrics",
}


# --- CLI defaults ----------------------------------------------------------

# Default total number of sequential iterations to run.
DEFAULT_MAX_ITERATIONS = 4

# Default max build/test debug retry attempts per opt pipeline.
DEFAULT_MAX_DEBUG_RETRIES = 3


# Top-level placement-group pool size. Set to accommodate the expected max
# num_parallel * OPT_NUMBER. Default sized for up to 4 parallel iterations at
# OPT_NUMBER=4.
POOL_SIZE = DEFAULT_NUM_PARALLEL * OPT_NUMBER


# --- FlatDB: sibling 6-wide perf file --------------------------------------
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# WARNING: 6-wide perf-data filename convention is HARDCODED HERE.
#
# The single-opt pipeline runs BUILD_CONFIG = MegaBoomChiaBigCacheConfig (a
# 4-wide core). The bottleneck_analysis LLM is instead fed 6-wide TMA data
# *when available* so it identifies bottlenecks relevant to the wider core
# we are cherry-picking optimizations toward. Everything else in the loop
# (baseline IPC, per-opt delta measurement, warm-start sorting) continues to
# use 4-wide data from ``perf_results.md``, which is what the Verilator runs
# in the backends actually produce.
#
# The 6-wide data is ASSUMED to live at
# ``<flatdb>/<branch>/perf_results_6wide.md`` in the same format as
# ``perf_results.md`` (one TMA block per test, separated by ``---``). This
# filename convention is a contract with whatever upstream pipeline writes
# the 6-wide data — it is NOT produced by any code in the single-opt loop.
# If that upstream pipeline changes its storage layout (e.g. moves to a
# subdirectory, or tags filenames by config name), update this constant AND
# coordinate with the writer.
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
SIX_WIDE_PERF_FILENAME = "perf_results_6_wide.md"


# --- S3 upload (pipeline-specific subdir) ----------------------------------

# Branch-relative subfolder under S3_BUCKET/{branch_name}/ for 4-wide waves.
# S3_BUCKET, AWS_CREDS_DIR, AWS_REGION live in common_constants.
S3_WAVES_SUBDIR = "waves_4/"


# --- Performance-optimization loop -----------------------------------------

# After the initial implement+test succeeds, _run_opt_pipeline runs up to this
# many perf_opt+test iterations to drive IPC higher. Each iteration uploads
# its waves to <S3_WAVES_SUBDIR>_o<i>/ for offline comparison; at end-of-loop
# the best-IPC version's waves are consolidated into S3_WAVES_SUBDIR and the
# per-iter subdirs are deleted.
NUM_PERF_OPT_ITERS = 2
