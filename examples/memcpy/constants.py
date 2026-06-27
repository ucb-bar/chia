"""Tunable parameters for the MemCpy RoCC agentic example.

Every knob the loop reads lives here. Two kinds of constant:

  * Repo-relative paths (this example's own collateral) are derived from
    ``Path(__file__)`` so the example works wherever the repo is checked out
    and however it is uploaded (``ray job submit --working-dir .`` resolves
    ``__file__`` to the uploaded copy, which is exactly what we want for the
    bundled ``memcpy.c`` / ``dramsim_ini`` / ``out``).

  * Container-side paths and cluster knobs are read from ``MEMCPY_*`` env vars
    with defaults matching the reference cluster (the ``chia-chisel-build`` /
    ``chia-verilator-run`` docker images on the a14/a13 nodes). Override the
    env var if your image layout differs — nothing here is hardcoded into the
    loop logic.
"""

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Repo-relative paths (this example's bundled files)
# ---------------------------------------------------------------------------

EXAMPLE_DIR = Path(__file__).resolve().parent

# Where every intermediate node result and piece of file collateral is dumped.
# Each file is written with a run timestamp at the START of its name (see
# memcpy_loop._Dumper), so multiple runs interleave cleanly and sort by time.
OUT_DIR = EXAMPLE_DIR / "out"

# The C test program exercised by the accelerator (two RoCC custom instructions
# + a correctness check). Cross-compiled to memcpy.riscv by the test-build node.
MEMCPY_C_PATH = EXAMPLE_DIR / "memcpy.c"

# DRAMSim2 ini files shipped to the verilator run node (MegaBoom uses a DRAM
# model). Loaded as bytes per run and written into the sim task dir. Shared with
# the other examples via examples/common/dramsim_ini.
DRAMSIM_INI_DIR = EXAMPLE_DIR.parent / "common" / "dramsim_ini"

# Runtime env shipped to every worker:
#   * working_dir = this example dir, so its flat modules (test_build, helpers,
#     constants, claude) are importable top-level on workers — the dispatched
#     ChiaFunctions (build_test, collect_diff) deserialize by reference there,
#     no register_pickle_by_value needed.
#   * py_modules = the head's (current) chia package, overriding the workers'
#     baked chia which predates chia.models (the implement/debug LLM dispatch).
_REPO_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_ENV = {
    "working_dir": str(EXAMPLE_DIR),
    "py_modules": [str(_REPO_ROOT / "chia")],
    "excludes": ["out/", "__pycache__", ".mypy_cache"],
}


# ---------------------------------------------------------------------------
# Chipyard container paths (env-overridable)
# ---------------------------------------------------------------------------

# Chipyard checkout inside the chisel-build (chipyard) container. The tests
# folder, RISC-V toolchain, and CMake all live in this image.
CHIPYARD_PATH = os.environ.get("MEMCPY_CHIPYARD_PATH", "/home/ray/chipyard")

# The bare-metal C test suite inside chipyard. The test-build node copies
# memcpy.c here, registers a CMake target, and builds build/memcpy.{riscv,dump}.
CHIPYARD_TESTS_DIR = os.environ.get(
    "MEMCPY_CHIPYARD_TESTS_DIR", os.path.join(CHIPYARD_PATH, "tests")
)

# Scratch dir on the verilator container where the sim binary + test ELF are
# staged and run.
VERILATOR_WORK_DIR = os.environ.get("MEMCPY_VERILATOR_WORK_DIR", "/home/ray/")

# ---------------------------------------------------------------------------
# Chisel build target
# ---------------------------------------------------------------------------

# The design the implement node must wire its RoCC accelerator into, and the
# config the chisel-build node compiles. MegaBoom V3 with the human-readable
# commit-log harness (so a failing run gives us a readable instruction trace
# to feed back to the debugger).
BUILD_CONFIG = os.environ.get("MEMCPY_BUILD_CONFIG", "MegaBoomV3HumanCommitLogConfig")
BUILD_CONFIG_PACKAGE = os.environ.get("MEMCPY_BUILD_CONFIG_PACKAGE", "chipyard")

# Where the BOOM / chipyard Scala sources live in the container — handed to the
# implement and debug nodes as a starting point for exploration (they use the
# chipyard_bash tool to find the exact files).
CHIPYARD_SRC_PATH = os.environ.get(
    "MEMCPY_CHIPYARD_SRC_PATH", os.path.join(CHIPYARD_PATH, "generators")
)

# ---------------------------------------------------------------------------
# Test program / correctness
# ---------------------------------------------------------------------------

# Logical name of the test (drives the .c / .riscv / .dump / CMake target name).
TEST_NAME = "memcpy"

# Number of 64-bit elements memcpy.c copies and checks. MUST match DATA_SIZE in
# memcpy.c — the run passes iff the program reports this many correct elements.
DATA_SIZE = 100

# ---------------------------------------------------------------------------
# LLM (implement + debug nodes)
# ---------------------------------------------------------------------------

LLM_MODEL = os.environ.get("MEMCPY_LLM_MODEL", "claude-opus-4-6")
LLM_SYSTEM_MESSAGE = (
    "You are an expert Chisel / RISC-V engineer specializing in RoCC "
    "accelerators for the Chipyard / BOOM ecosystem."
)
LLM_TIMEOUT_SECONDS = int(os.environ.get("MEMCPY_LLM_TIMEOUT_SECONDS", "1800"))
# Forwarded to the claude CLI; "max" effort matches the other examples' debugger.
LLM_EXTRA_CLI_ARGS = ["--effort", "max"]

# ---------------------------------------------------------------------------
# Loop control
# ---------------------------------------------------------------------------

# Reserved for a future performance-optimization phase (once correctness
# passes, iterate to reduce cycle count). The correctness + debug flow does not
# use it yet; it is defined here so the knob exists in one place.
NUM_PERF_OPT_ITERS = int(os.environ.get("MEMCPY_NUM_PERF_OPT_ITERS", "1"))

# Max debug-and-retry attempts after the FIRST build/sim failure. attempt 0 is
# the initial build+run; each failure (up to this many) triggers a debug node
# call followed by a rebuild + rerun.
NUM_DEBUG_ATTEMPTS = int(os.environ.get("MEMCPY_NUM_DEBUG_ATTEMPTS", "3"))

# ---------------------------------------------------------------------------
# Timeouts
# ---------------------------------------------------------------------------

TEST_BUILD_TIMEOUT_SECONDS = int(os.environ.get("MEMCPY_TEST_BUILD_TIMEOUT_SECONDS", "900"))
CHISEL_BUILD_TIMEOUT_SECONDS = int(os.environ.get("MEMCPY_CHISEL_BUILD_TIMEOUT_SECONDS", "60000"))
CHISEL_BUILD_MAKE_JOBS = int(os.environ.get("MEMCPY_CHISEL_BUILD_MAKE_JOBS", "16"))
VERILATOR_TIMEOUT_SECONDS = int(os.environ.get("MEMCPY_VERILATOR_TIMEOUT_SECONDS", "1800"))
# memcpy is tiny; cap simulated cycles so a hung/incorrect design times out fast
# instead of running to the sim's internal limit.
VERILATOR_TIMEOUT_CYCLES = int(os.environ.get("MEMCPY_VERILATOR_TIMEOUT_CYCLES", "2000000"))

# ---------------------------------------------------------------------------
# Debug feedback shaping
# ---------------------------------------------------------------------------

# Last N lines of memcpy.dump (disassembly) and of the commit log handed to the
# debugger on a verilator failure, per the example spec.
DUMP_TAIL_LINES = int(os.environ.get("MEMCPY_DUMP_TAIL_LINES", "50"))
COMMIT_LOG_TAIL_LINES = int(os.environ.get("MEMCPY_COMMIT_LOG_TAIL_LINES", "50"))
# Hard cap on any single chunk of build/sim output pasted into a prompt.
MAX_OUTPUT_CHARS = int(os.environ.get("MEMCPY_MAX_OUTPUT_CHARS", "100000"))

# ---------------------------------------------------------------------------
# Ray resource tokens
# ---------------------------------------------------------------------------
#
# The chisel-build node advertises a single "chipyard": 1 token (see
# cluster.yaml). A BashTool deploys a Ray actor that HOLDS its token for the
# whole loop, so every chipyard task's token must leave room for the bash tool
# to coexist. These are scheduling tokens only — real build parallelism is
# governed by CHISEL_BUILD_MAKE_JOBS, not by the token size.
CHIPYARD_BASH_RESOURCE = float(os.environ.get("MEMCPY_CHIPYARD_BASH_RESOURCE", "0.01"))
TEST_BUILD_RESOURCE = float(os.environ.get("MEMCPY_TEST_BUILD_RESOURCE", "0.05"))
CHISEL_BUILD_RESOURCE = float(os.environ.get("MEMCPY_CHISEL_BUILD_RESOURCE", "0.9"))
VERILATOR_RUN_RESOURCE = float(os.environ.get("MEMCPY_VERILATOR_RUN_RESOURCE", "1.0"))

# The implement + debug LLM calls are dispatched onto the dedicated claude
# ("llm") node, which advertises {"llm": 1} (see cluster.yaml). The claude CLI
# + credentials live in that node's image; the head only orchestrates.
LLM_RESOURCE = float(os.environ.get("MEMCPY_LLM_RESOURCE", "1.0"))

# ---------------------------------------------------------------------------
# Chisel diff capture
# ---------------------------------------------------------------------------

# Submodules collect_diff inspects in addition to the root chipyard repo. The
# accelerator + config edits land in generators/chipyard (the root repo, always
# captured under the "" key); these are included so a debugger edit that strays
# into a submodule (e.g. BOOM) is still recorded.
CHIPYARD_DIFF_SUBMODULES = [
    "generators/boom",
    "generators/rocket-chip",
    "generators/rocket-chip-inclusive-cache",
    "generators/rocket-chip-blocks",
    "generators/bar-fetchers",
]
