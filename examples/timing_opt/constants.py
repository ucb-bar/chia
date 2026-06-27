"""Configuration for the BoomTile timing-improvement example.

Only the knobs the timing flow actually reads live here; the shared
build/verilator/synthesis helpers it calls (in ``common``) take their own
defaults as keyword arguments. Ported from the original ``constants.py`` /
``common_constants.py`` of the standalone timing project, trimmed to
the timing pipeline's needs.
"""

import os
from pathlib import Path


# Path to the chipyard checkout INSIDE the chisel-build / LLM docker images.
CHIPYARD_PATH = os.environ.get("TIMING_OPT_CHIPYARD_PATH", "/home/ray/chipyard/")
BOOM_REPO_PATH = str(Path(CHIPYARD_PATH) / "generators/boom/src/main/scala/v3/")
BUILD_CONFIG = "MegaBoomChiaBigCacheConfig"
BUILD_CONFIG_PACKAGE = "chipyard"

# LLM working directory inside the Docker container (not the host) — the CWD
# the Claude Code process runs in. Created empty by the cluster setup; the
# flow ships all prompts in this package under prompts/ and sends them inline
# (the /improve_timing prompt is pre-expanded on the head node, see
# IMPROVE_TIMING_PROMPT_PATH), so nothing needs to live in this dir.
LLM_ENV = os.environ.get("TIMING_OPT_LLM_ENV", "/home/ray/llm_env")

# Pre-expanded on the head node and piped to `claude --print -p -`; print mode
# from piped stdin does not expand project slash commands itself, so we
# substitute $ARGUMENTS / $1 / $TIMING_REPORT_PATH driver-side. Resolved
# relative to this package so the example stays self-contained wherever the
# repo is checked out.
IMPROVE_TIMING_PROMPT_PATH = str(
    Path(__file__).resolve().parent / "prompts" / "improve_timing.md"
)

# Root of the test-binary tree; benchmarks loaded from verilatorbins/ subdirs.
# Points at this package so we pick up the verilatorbins/ git submodule that
# ships with the working_dir upload.
UBENCH_DIR = Path(__file__).resolve().parent

# --- Workload thread / timeout maps (verilator) ----------------------------

ASMTESTS_WORKLOADS_THREAD_MAP: dict[str, int] = {}

EMBENCH_WORKLOADS_THREAD_MAP = {
    "edn"           : 1,
    "slre"          : 1,
    "statemate"     : 1,
    "matmult-int"   : 1,
    "md5sum"        : 4,
    "sglib-combined": 4,
    "aha-mont64"    : 4,
    "tarfind"       : 4,
    "depthconv"     : 4,
    "ud"            : 4,
    "nettle-sha256" : 4,
    "qrduino"       : 4,
    "picojpeg"      : 4,
    "nsichneu"      : 4,
    "nettle-aes"    : 4,
    "wikisort"      : 4,
    "huffbench"     : 4,
    "crc32"         : 4,
    "xgboost"       : 4,
}
EMBENCH_TIMEOUT_CYCLES = {
    "default":         20_000_000,
    "edn":           2*1_822_000,
    "slre":          2*1_831_000,
    "statemate":     2*2_402_000,
    "matmult-int":   2*1_524_000,
    "md5sum":        2*1_788_000,
    "sglib-combined":2*4_187_000,
    "aha-mont64":    2*1_928_000,
    "tarfind":       2*1_481_000,
    "depthconv":     2*2_245_000,
    "ud":            2*4_700_000,
    "nettle-sha256": 2*2_568_000,
    "qrduino":       2*5_114_000,
    "picojpeg":      2*2_615_000,
    "nsichneu":      2*4_312_000,
    "nettle-aes":    2*2_585_000,
    "wikisort":      2*1_913_000,
    "huffbench":     2*3_416_000,
    "crc32":         2*4_905_000,
    "xgboost":       2*7_089_000,
}

# --- Timing reports --------------------------------------------------------

# Candidate timing-report relpaths under a synthesis_reports/<top>_child/
# directory, in preferred order. final_constrained.rpt (Genus's
# report_timing -summary -lint output) is tighter / more curated than the
# raw setup_view dump; falling back to the corner-specific setup view for
# older runs that don't emit it.
TIMING_REPORT_RELPATHS = (
    "path/to/timing/report",
    "path/to/timing/report",
)

# Where the parent branch's timing report is staged for the LLM on the
# chipyard worker — chipyard_bash reads/greps this through the BashTool.
TIMING_REPORT_LLM_PATH = "/tmp/improve_timing/timing_report.rpt"

# Where TimingExperimentTool stages per-experiment reports for the LLM. Lives
# on the same chipyard worker as chipyard_bash (same PG bundle), so Claude can
# grep <EXPERIMENT_REPORTS_LLM_DIR>/<exp_id>/... without crossing nodes.
EXPERIMENT_REPORTS_LLM_DIR = "/tmp/improve_timing/experiments"

# --- Storage ---------------------------------------------------------------

# SQLite-backed store for the improve_timing flow. The DB is only touched on
# the head node, so an absolute head path is used everywhere it is read —
# NOT Path(__file__), which under `chia job submit --working-dir .`'s
# upload resolves to /tmp/ray/.../_ray_pkg_*/ (the uploaded, empty copy),
# making has_any_branch() lie and re-fire seed_flow. Set TIMING_OPT_DB_DIR to a
# stable absolute head path; the default lives under the head user's home (also
# stable, and independent of the ray-upload working dir).
DB_DIR = os.environ.get(
    "TIMING_OPT_DB_DIR",
    os.path.expanduser("~/timing_opt_DB"),
)

# Worker-local scratch directory where the remote synthesis task writes its
# Hammer/Genus obj_dir. /scratch is local to each machine (not shared across
# the cluster), so the synth task tars its obj_dir at the end and returns the
# bytes; the head node then extracts them into DB/files/<branch>/syn_obj.
SYN_OBJ_SCRATCH_DIR = os.environ.get(
    "TIMING_OPT_SYN_OBJ_SCRATCH_DIR", "/path/to/chia-logging/timing_syn_obj")

# --- Synthesis collateral --------------------------------------------------

# Sky130 standard-cell / macro collateral and the CACTI binary, on the synthesis
# (genus) workers. Extracted/installed by the cluster setup (see cluster.yaml).
SKY130_COL_PATH = os.environ.get("TIMING_OPT_SKY130_COL_PATH", "/path/to/sky130_col")
CACTI_PATH = os.environ.get("TIMING_OPT_CACTI_PATH", "/path/to/cacti/cacti")
