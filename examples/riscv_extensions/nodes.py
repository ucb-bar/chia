"""@ChiaFunction workers for the VEXT loop — one wrapper per cluster role.

Always dispatched with `.chia_remote(...)` / `.options(...).chia_remote(...)`
and awaited with `get(...)` (field guide §5). The chisel_build workers
(`reset_chipyard`, `collect_diff`, `build_megaboom`) are dispatched into the
pipeline's `chipyard` placement group so the LLM's editor BashTool and the
build share one container exclusively.
"""

import hashlib
import os
import subprocess
import uuid

from chia.base.ChiaFunction import ChiaFunction
from chia.trace.profiler import get_profiler
from chia.chipyard.chisel_build_node import ChiselBuildNode
from chia.chipyard.cosim_node import CosimNode
from chia.chipyard.riscv_build_node import RiscvBuildNode
from chia.chipyard.riscv_dv_gen_node import GenSpec, RiscvDvGenNode
from chia.chipyard.verilator_run_node import VerilatorRunNode
from chia.chipyard.state_def import (
    BuildArtifact,
    BuildTarget,
    CosimResult,
    RunResult,
)

from riscv_extensions.constants import (
    BOOM_REPO_REL,
    BUILD_MAKE_JOBS,
    BUILD_TIMEOUT_S,
    CHIPYARD_PATH,
    CONFIG_PACKAGE,
    COSIM_VRUN,
    COSIM_CONFIG,
    SIM_TIMEOUT_CYCLES,
    SIM_ZERO_INIT_DEFINES,
    SOAK_CYCLES_PER_INSTR,
    SOAK_MAX_CYCLES,
    VERILATOR_THREADS,
)


# --- chisel_build node (pin to the pipeline's placement group) -------------

@ChiaFunction(resources={"chipyard": 0.9})
def reset_chipyard(chipyard_path: str = CHIPYARD_PATH, extension: str = "") -> str:
    """Reset the chipyard repo AND the BOOM submodule to a pristine baseline so
    every experiment starts clean: reset + clean chipyard, then the submodule
    (to the commit chipyard pins)."""
    if extension: get_profiler().add_info({"extension": extension})
    subprocess.run(["git", "reset", "--hard", "HEAD"], cwd=chipyard_path, check=False)
    subprocess.run(["git", "clean", "-fd"], cwd=chipyard_path, check=False)
    sm_path = os.path.join(chipyard_path, BOOM_REPO_REL)
    r = subprocess.run(["git", "ls-tree", "HEAD", BOOM_REPO_REL],
                       cwd=chipyard_path, capture_output=True, text=True)
    target = r.stdout.split()[2] if (r.returncode == 0 and r.stdout.strip()) else "HEAD"
    subprocess.run(["git", "reset", "--hard", target], cwd=sm_path, check=False)
    subprocess.run(["git", "clean", "-fd"], cwd=sm_path, check=False)
    return f"Reset chipyard + {BOOM_REPO_REL}@{target[:10]}"


@ChiaFunction(resources={"chipyard": 0.9})
def apply_diff(diff: str, chipyard_path: str = CHIPYARD_PATH, extension: str = "") -> str:
    """Seed the (freshly reset) tree with a prior run's probe diff, so a
    pipeline can resume from a known implementation instead of re-deriving it.
    Probe diffs are chipyard-rooted (see collect_diff); legacy BOOM-rooted
    seeds still apply via --directory.
    TODO: this should be offered with bypassing."""
    if extension: get_profiler().add_info({"extension": extension})
    for extra in ((), ("--directory", BOOM_REPO_REL)):
        p = subprocess.run(["git", "apply", "--whitespace=nowarn", *extra],
                           input=diff, cwd=chipyard_path, capture_output=True, text=True)
        if p.returncode == 0:
            return "applied" + (" (legacy boom-rooted)" if extra else "")
    return f"FAILED: {p.stderr[-500:]}"


@ChiaFunction(resources={"chipyard": 0.9})
def collect_diff(chipyard_path: str = CHIPYARD_PATH, extension: str = "") -> str:
    """The LLM's accumulated edits as ONE chipyard-rooted unified diff:
    chipyard-level changes (the cosim config, the IOBinders ISA line) plus the
    BOOM submodule's, its paths prefixed so a single `git apply` reseeds both.
    Intent-to-add untracked files first so new sources appear. For probes."""
    if extension: get_profiler().add_info({"extension": extension})
    def _diff(cwd: str, prefix: str = "") -> str:
        subprocess.run(["git", "add", "-N", "."], cwd=cwd, check=False)
        args = ["git", "diff", "--ignore-submodules"]
        if prefix:
            args += [f"--src-prefix=a/{prefix}/", f"--dst-prefix=b/{prefix}/"]
        d = subprocess.run(args, cwd=cwd, capture_output=True, text=True).stdout
        subprocess.run(["git", "reset"], cwd=cwd, check=False)
        return d
    return (_diff(chipyard_path)
            + _diff(os.path.join(chipyard_path, BOOM_REPO_REL), BOOM_REPO_REL))


@ChiaFunction(resources={"chipyard": 0.9})
def build_megaboom(config: str = COSIM_CONFIG, extension: str = "") -> BuildArtifact:
    """Elaborate + verilate the current BOOM source into a Verilator sim with
    spike compiled in (cospike). The run image carries the matching spike libs
    (copied from the chipyard base image — lockstep by construction). `config`
    defaults to the cosim config; the baseline PPA synth passes a stock config
    that builds in a fresh tree (the cosim config is the LLM's to write)."""
    if extension: get_profiler().add_info({"extension": extension})
    node = ChiselBuildNode(
        chipyard_path=CHIPYARD_PATH,
        config=config,
        config_package=CONFIG_PACKAGE,
        target=BuildTarget.VERILATOR,
        make_jobs=BUILD_MAKE_JOBS,
        timeout_seconds=BUILD_TIMEOUT_S,
        extra_make_args={"VERILATOR_THREADS": str(VERILATOR_THREADS),
                         "EXTRA_SIM_PREPROC_DEFINES": SIM_ZERO_INIT_DEFINES},
        clean_sim=True,
        collect_generated_src=True,   # RTL for the sky130 PPA synth (single_loop)
    )
    return node.build()


# --- riscv_build node: compile emitted differential directed tests ----------
# Extensions without self-checking riscv-tests (crypto) verify S1 against Spike:
# riscv_extensions.isa_tests emits one tiny program per instruction; we cross-compile each
# here (htif_nano harness) and cosim it. Built once per run — the test programs
# don't change across implement iterations.

ISA_CACHE_DIR = "/tmp/vext_isacache"   # build-node-local (long-lived container)


@ChiaFunction(resources={"riscv_build": 1}, num_cpus=2)
def build_isa_test(asm: str, program: str, march: str, work_dir: str,
                   extension: str = "") -> bytes | None:
    """Cross-compile one emitted per-instruction test (.S) into a baremetal ELF
    via the riscv-harness. `march` carries the extension groups so the assembler
    accepts the instruction. Returns the ELF bytes, or None on build failure.
    Cached by content (the .S + march fully determine the ELF), so re-runs and
    resumes — including going back to S1 after a soak divergence — reuse the same
    binaries instead of recompiling."""
    if extension: get_profiler().add_info({"extension": extension})
    cached = os.path.join(ISA_CACHE_DIR, hashlib.sha1(f"{march}\0{asm}".encode()).hexdigest())
    if os.path.exists(cached):
        with open(cached, "rb") as f:
            return f.read()
    art = RiscvBuildNode().build(asm.encode(), program, work_dir, target="verilator",
                                 lang="asm", extra_cflags=f"-march={march}")
    if not art.success:
        return None
    os.makedirs(ISA_CACHE_DIR, exist_ok=True)
    with open(cached, "wb") as f:
        f.write(art.binary_content)
    return art.binary_content


# --- verilator_run node (the DUT) ------------------------------------------
# Directed test ELFs come from the durable DB (riscv_extensions.db_node.fetch_tests) and are
# SELF-CHECKING: the DUT run alone is the verdict. Spike appears only inside the
# random-soak cosim (CosimNode), where tests have no self-check.

@ChiaFunction(resources={"verilator_run": COSIM_VRUN}, num_cpus=VERILATOR_THREADS)
def verilator_run_remote(
    artifact: BuildArtifact,
    elf_content: bytes,
    elf_name: str,
    work_dir: str,
    extension: str = "",
) -> RunResult:
    """Run one ELF on the Verilator simulation of the freshly-built core."""
    if extension: get_profiler().add_info({"extension": extension})
    return VerilatorRunNode().run(
        artifact=artifact,
        test_binary_content=elf_content,
        test_binary_name=elf_name,
        work_dir=work_dir,
        plusargs={"+loadmem": elf_name},
        timeout_cycles=SIM_TIMEOUT_CYCLES,
        verbose=False,
    )


# --- random soak (S3): gen on dv+xcelium, cosim on a cosim node ------------

# The vext custom riscv-dv targets ship in the working-dir, so their path on any
# worker is relative to this package. Each extension picks its target by name
# (Extension.dv_target): the base (rv64gc + ratified Zb*) or an extension-specific
# one that enables extra custom instructions (e.g. riscv_dv_target_zicond -> czero).
_VEXT_BASE = os.path.dirname(os.path.abspath(__file__))
_DV_TARGET = os.path.join(_VEXT_BASE, "riscv_dv_target")


@ChiaFunction(resources={"dv": 1, "xcelium": 1}, num_cpus=4)
def gen_to_pool(spec: GenSpec, isa: str, pool_dir: str, work_dir: str,
                target_name: str = "riscv_dv_target", extension: str = "") -> int:
    """Generate one random test (`spec`) with riscv-dv's SV flow (Xcelium, the vext
    custom target `target_name`, resolved worker-side under this package) and
    register it in the test pool under a unique name. The program shape lives in
    the named testlist entry; `spec` overrides per run. Returns the number of
    tests added."""
    if extension: get_profiler().add_info({"extension": extension})
    from chia.base.ChiaFunction import get
    import riscv_extensions.db_node as db_node
    target_dir = os.path.join(_VEXT_BASE, target_name)
    # Our overlay: a target's custom instrs (e.g. zicond czero.*) ship in its isa/custom/;
    # hand that dir to the node's custom_instr_dir so it installs them into riscv-dv's
    # src/. None for the base target, which has no custom instrs.
    instr_dir = os.path.join(target_dir, "isa", "custom")
    tests = RiscvDvGenNode(simulator="xlm", isa=isa, mabi="lp64d", custom_target=target_dir,
                           custom_instr_dir=instr_dir if os.path.isdir(instr_dir) else None,
                           timeout_seconds=2 * (spec.gen_timeout or 900)).generate(spec, work_dir)
    for name, elf, asm in tests:
        unique = f"{os.path.splitext(name)[0]}_{uuid.uuid4().hex[:8]}"
        get(db_node.pool_add.chia_remote(pool_dir, unique, elf, asm, spec.instr_cnt or 0, extension=extension))
    return len(tests)


@ChiaFunction(resources={"verilator_run": COSIM_VRUN}, num_cpus=VERILATOR_THREADS)
def cosim_run(artifact: BuildArtifact, elf_content: bytes, elf_name: str,
              instr: int, work_dir: str, extension: str = "") -> CosimResult:
    """Co-simulate one ELF in lockstep: spike rides inside the sim (cospike)
    and the run aborts at the first divergence. Budget is SOAK_MAX_CYCLES, but
    scales up for tests too large to execute within it."""
    if extension: get_profiler().add_info({"extension": extension})
    budget = max(SOAK_MAX_CYCLES, instr * SOAK_CYCLES_PER_INSTR)
    return CosimNode().run(artifact, elf_content, elf_name, work_dir,
                           timeout_cycles=budget)
