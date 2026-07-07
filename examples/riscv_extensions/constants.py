"""Cross-node constants and the extension registry for the RISC-V extension loop.

Paths tagged "(in <image>)" are inside the docker image the cluster launches for
that role, not on the head filesystem. DB-host paths live in db_node.py.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from chia.chipyard.riscv_dv_gen_node import GenSpec


# --- LLM -------------------------------------------------------------------
LLM_MODEL = "claude-opus-4-7"
LLM_EXTRA_ARGS = ["--effort", "max"]

# --- Chisel build (in chia-chisel-build) -----------------------------------
CHIPYARD_PATH = "/home/ray/chipyard"
BOOM_SRC_REL = "generators/boom/src/main/scala/v3"          # what the LLM edits
BOOM_REPO_REL = "generators/boom"                            # git submodule root
# Cosim config: the synth config plus the cospike/trace/commit-log harness. The
# LLM writes it per run (prompts/system.md); one binary serves directed tests
# (cospike off) and the stress test (cospike on).
COSIM_CONFIG = "MegaBoomChiaBigCacheCosimConfig"
# Synthesis (PPA) config: harness-free. Baseline and implemented design both build
# from it, so the area/slack delta is purely the extension's RTL.
SYNTH_CONFIG = "MegaBoomChiaBigCacheConfig"
CONFIG_PACKAGE = "chipyard"
BUILD_MAKE_JOBS = 16
BUILD_TIMEOUT_S = 60 * 60                                    # cold elaborate + verilate
VERILATOR_THREADS = 8
# RANDOM=0 zeroes the Verilog register-init blocks so unspecified reset state
# matches spike's zeros (required for cosim-able random CSR stimulus). Appended
# via chipyard's EXTRA_SIM_PREPROC_DEFINES.
SIM_ZERO_INIT_DEFINES = "+define+RANDOM=0"

# Prebuilt self-checking riscv-tests ELFs in the chipyard image; staged into the
# DB (DB_ROOT/tests/<ext>/) and read via db_node.fetch_tests.
RISCV_TESTS_ISA_DIR = (
    f"{CHIPYARD_PATH}/.conda-env/riscv-tools/riscv64-unknown-elf/share/riscv-tests/isa"
)

# Base-ISA asm riscv-test suite, staged into DB_ROOT/tests/asm/; the S2 gate runs
# it to catch base-ISA regressions before the stress_test.
ASM_TESTS = "asm"

# --- Simulation ------------------------------------------------------------
SIM_TIMEOUT_CYCLES = 10_000_000

# --- Driver scratch --------------------------------------------------------
VEXT_LOG_ROOT = "/tmp/vext"                                 # process-wide chdir target
MAX_ITERS = 60                                              # implement-loop iteration cap
DEBUG_MAX_ITERS = 5                                         # per stress_test-divergence debug round

# --- Stress test (S3): riscv-dv gen -> test pool -> cospike lockstep --------
# Generators fill a pool on the database node; cosims stream from it and mark
# pending/passed. A divergence resets the pool so the whole batch re-runs after
# the fix. instr_cnt 175k is riscv-dv's practical ceiling (the UVM generator is
# superlinear in program size).
STRESS_TEST_HOURS = 24                                            # hard deadline for the batch
COSIM_VRUN = 0.9                                           # verilator_run each cosim reserves
STRESS_TEST_VRUN_FRACTION = 0.5                                   # cosim-slot fraction per pipeline
                                                          # (0.5 runs two extensions in parallel;
                                                          # 1.0 for a single-extension run)
STRESS_TEST_MAX_CYCLES = 10_000_000                              # per-test cycle budget
STRESS_TEST_CYCLES_PER_INSTR = 20                                # budget scale for oversized tests
# Two named testlist shapes (uniform + ALU/bitmanip-weighted); STRESS_TEST_MIX picks them
# by name with per-run count + gen_timeout overrides.
STRESS_TEST_MIX = [
    (150, GenSpec(test="riscv_rand_instr_test", instr_cnt=175_000, gen_timeout=900)),
    (150, GenSpec(test="riscv_rand_balu_test",  instr_cnt=175_000, gen_timeout=900)),
]

# --- Synthesis (PPA): sky130/Genus area + timing, baseline vs implemented ----
# SRAMs are CACTI-characterized and remapped to macros, same flow as the timing
# loop (single_loop._synth). The baseline-vs-implemented delta is the metric. Set
# the real synth-image paths via the env vars below (defaults are placeholders).
SKY130_COL_PATH = os.environ.get("VEXT_SKY130_COL_PATH", "/path/to/sky130_col")  # sky130 PDK dir in the synth image
CACTI_PATH = os.environ.get("VEXT_CACTI_PATH", "/path/to/cacti/cacti")           # cacti binary in the synth image
SYNTH_OBJ_ROOT = "/tmp/vext-synth"                        # Genus obj dirs (in-container scratch)
SYNTH_TIMEOUT_S = 86400                                   # 24h — full BoomTile synth is slow


def sky130_vlsi_runtime_env() -> dict:
    """runtime_env shipping examples/{riscv_extensions,sky130_vlsi,common,timing_opt}.

    These packages live outside the installed chia package, so worker images don't
    carry them. Shipping the dirs as py_modules makes them importable top-level on
    every worker, so the pickled-by-reference ChiaFunctions resolve to the same
    module on driver and worker. The driver also puts examples/ on sys.path.
    """
    import chia.vlsi.hammer as _anchor   # concrete file: chia is a namespace pkg
    examples = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(_anchor.__file__)))), "examples")
    return {"py_modules": [os.path.join(examples, "riscv_extensions"),
                           os.path.join(examples, "sky130_vlsi"),
                           os.path.join(examples, "common"),
                           os.path.join(examples, "timing_opt")],
            "env_vars": {"VEXT_SKY130_COL_PATH": SKY130_COL_PATH}}


@dataclass(frozen=True)
class Extension:
    """One ISA-extension target — the unit of work for a single pipeline."""

    name: str                          # short id, e.g. "bitmanip"
    description: str                   # one-liner for prompts / summaries
    isa_suffix: str                    # ISA groups appended to isaDTS (spike) and
                                       # rv64gc (riscv-dv gen), e.g. "_zba_zbb_zbc_zbs"
    dv_target: str = "riscv_dv_target" # riscv-dv custom-target dir; override for an
                                       # extension needing custom-instruction gen


# Every extension is verified the same way: per-instruction directed tests from
# specs/<ext>/instructions.json, run differentially against Spike via cospike.
# Adding one: generate instructions.json (riscv-opcodes), stage its spec.md, set
# isa_suffix.
EXTENSIONS: dict[str, Extension] = {
    "bitmanip": Extension(
        name="bitmanip",
        description="RISC-V Bit-Manipulation extension (Zba, Zbb, Zbc, Zbs).",
        isa_suffix="_zba_zbb_zbc_zbs",
    ),
    "crypto": Extension(
        name="crypto",
        description="RISC-V scalar cryptography, NIST suite "
                    "(Zbkb, Zbkc, Zbkx, Zknd, Zkne, Zknh).",
        isa_suffix="_zbkb_zbkc_zbkx_zknd_zkne_zknh",
        dv_target="riscv_dv_target_crypto",   # crypto custom-instr stress_test coverage (riscv-dv has none natively)
    ),
    "zicond": Extension(
        name="zicond",
        description="RISC-V Integer Conditional Operations (Zicond): czero.eqz, czero.nez.",
        isa_suffix="_zicond",
        dv_target="riscv_dv_target_zicond",   # czero.* custom-instr stress_test coverage
    ),
}
