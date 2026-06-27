"""End-to-end integration test: ChiselBuildNode + RiscvBuildNode (parallel) -> VerilatorRunNode.

Builds a MegaBoom V3 verilator simulator and a hello-world RISC-V ELF in parallel,
then runs the ELF on the simulator and asserts the run succeeds.

Data flow
---------

    ┌──────────────────────────┐         ┌──────────────────────────┐
    │ ChiselBuildNode          │         │ RiscvBuildNode           │
    │  (resource: chipyard)    │         │  (resource: riscv_build) │
    │  config=MegaBoomV3Config │         │  source=hello_world.c    │
    │  make_jobs=64            │         │  target=verilator        │
    └────────────┬─────────────┘         └────────────┬─────────────┘
                 │ BuildArtifact                      │ RiscvBuildArtifact
                 │   .simulator_binary_content        │   .binary_content
                 │   .simulator_binary_name           │   .binary_name
                 │   (~MB of verilated MegaBoom)      │   (~KB hello.riscv ELF)
                 │                                    │
                 └──────────────┬─────────────────────┘
                                │  ray.wait gates here:
                                │  both must succeed before next step
                                ▼
                  ┌──────────────────────────────┐
                  │ VerilatorRunNode             │
                  │  (resource: verilator_run)   │
                  │  loads sim binary,           │
                  │  feeds .riscv as test ELF    │
                  │   via `+permissive-off`      │
                  └──────────────┬───────────────┘
                                 │ RunResult
                                 ▼
                  asserts success and "*** PASSED ***" in log

The two builds are submitted with `.remote()` before any `ray.get()`, so Ray
schedules them on their respective workers concurrently. `_await_parallel`
polls with `ray.wait` and prints a per-build status line as each completes.
VerilatorRunNode is only dispatched once both artifacts are in hand.

Cluster: test/test_chisel_riscv_verilator_cluster.yaml

Usage:
    chia up test/test_chisel_riscv_verilator_cluster.yaml -y
    ray job submit --address IP:6379 --working-dir . \\
        -- python test/test_chisel_riscv_verilator_e2e.py
    chia down test/test_chisel_riscv_verilator_cluster.yaml -y

Or run all of the above via test/run_chisel_riscv_verilator_tests.sh.
"""

import os
import sys
import time

import ray

from chia.chipyard.chisel_build_node import ChiselBuildNode
from chia.chipyard.riscv_build_node import RiscvBuildNode
from chia.chipyard.verilator_run_node import VerilatorRunNode
from chia.chipyard.state_def import BuildArtifact, RiscvBuildArtifact, RunResult


# Inputs to the test ---------------------------------------------------------

CHIPYARD_PATH = "/home/ray/chipyard"
MEGABOOM_CONFIG = "MegaBoomV3Config"
MAKE_JOBS = 64           # crank for speed on a multi-core host
BUILD_TIMEOUT_SECONDS = 7200

PROGRAM_NAME = "hello_world"
HELLO_C = b"""\
#include <stdio.h>

int main(void) {
    printf("hello from RiscvBuildNode\\n");
    printf("*** PASSED ***\\n");
    return 0;
}
"""

VERILATOR_WORK_DIR = "/tmp/test_chisel_riscv_verilator_e2e"
RISCV_WORK_DIR     = "/tmp/test_chisel_riscv_verilator_e2e_riscv"

TEST_DIR = os.path.dirname(os.path.abspath(__file__))


def load_dramsim_ini() -> dict[str, bytes]:
    """Reuse the same DRAMSim INI bundle test_verilator_run.py ships with."""
    dramsim_dir = os.path.join(TEST_DIR, "dramsim_ini")
    ini: dict[str, bytes] = {}
    for fname in os.listdir(dramsim_dir):
        fpath = os.path.join(dramsim_dir, fname)
        if os.path.isfile(fpath):
            with open(fpath, "rb") as f:
                ini[fname] = f.read()
    return ini


# Ray remote wrappers --------------------------------------------------------

@ray.remote(resources={"chipyard": 0.9})
def chisel_build_remote(chipyard_path: str, config: str,
                        make_jobs: int, timeout_seconds: int) -> BuildArtifact:
    node = ChiselBuildNode(
        chipyard_path=chipyard_path,
        config=config,
        make_jobs=make_jobs,
        timeout_seconds=timeout_seconds,
    )
    return node.build()


@ray.remote(resources={"riscv_build": 0.9})
def riscv_build_remote(source: bytes, program_name: str,
                       work_dir: str) -> RiscvBuildArtifact:
    return RiscvBuildNode().build(
        source_content=source,
        program_name=program_name,
        work_dir=work_dir,
        target="verilator",
    )


@ray.remote(resources={"verilator_run": 0.9})
def verilator_run_remote(build_artifact: BuildArtifact,
                         test_binary_content: bytes,
                         test_binary_name: str,
                         work_dir: str,
                         dramsim_ini: dict) -> RunResult:
    return VerilatorRunNode().run(
        build_artifact, test_binary_content, test_binary_name,
        work_dir=work_dir,
        dramsim_ini_files=dramsim_ini,
    )


# Orchestration --------------------------------------------------------------

def _await_parallel(pending: dict) -> dict:
    """Wait for every future in `pending` (object_ref -> label), printing
    each as it completes. Returns {label: result}."""
    results: dict = {}
    t0 = time.time()
    refs = list(pending.keys())
    while refs:
        done, refs = ray.wait(refs, num_returns=1, timeout=30.0)
        if not done:
            still = ", ".join(pending[r] for r in refs)
            print(f"  [{int(time.time() - t0)}s] still building: {still}")
            continue
        ref = done[0]
        label = pending[ref]
        artifact = ray.get(ref)
        results[label] = artifact
        ok = getattr(artifact, "success", False)
        print(f"  [{int(time.time() - t0)}s] {label} finished: success={ok}")
    return results


def main() -> int:
    ray.init(address="auto")

    print("=== Kicking off chisel_build and riscv_build in parallel ===")
    print(f"  chisel: {MEGABOOM_CONFIG}, make_jobs={MAKE_JOBS}")
    print(f"  riscv:  {PROGRAM_NAME}.c (target=verilator, {len(HELLO_C)} bytes)")

    pending = {
        chisel_build_remote.remote(
            CHIPYARD_PATH, MEGABOOM_CONFIG, MAKE_JOBS, BUILD_TIMEOUT_SECONDS
        ): "chisel_build",
        riscv_build_remote.remote(
            HELLO_C, PROGRAM_NAME, RISCV_WORK_DIR
        ): "riscv_build",
    }
    results = _await_parallel(pending)

    chisel: BuildArtifact = results["chisel_build"]
    riscv:  RiscvBuildArtifact = results["riscv_build"]

    if not chisel.success:
        print(f"FAIL: chisel build failed (rc={chisel.returncode})")
        print(chisel.stderr[-2000:])
        return 1
    if not riscv.success:
        print(f"FAIL: riscv build failed (rc={riscv.returncode})")
        print(riscv.stderr[-2000:])
        return 1

    print(f"\nBoth builds OK:")
    print(f"  chisel simulator: {chisel.simulator_binary_name} "
          f"({len(chisel.simulator_binary_content)} bytes)")
    print(f"  riscv  ELF:       {riscv.binary_name} "
          f"({len(riscv.binary_content)} bytes)")

    print("\n=== Running verilator simulation ===")
    run_result: RunResult = ray.get(
        verilator_run_remote.remote(
            chisel,
            riscv.binary_content,
            riscv.binary_name,
            VERILATOR_WORK_DIR,
            load_dramsim_ini(),
        )
    )

    print(f"  success={run_result.success}  returncode={run_result.returncode}")
    print(f"  log: {len(run_result.log.splitlines())} lines  "
          f"out: {len(run_result.out.splitlines())} lines")
    print(f"  log preview: {run_result.log[:500]!r}")

    if not run_result.success:
        print("FAIL: simulator exited non-zero")
        return 1
    if "*** PASSED ***" not in run_result.log:
        print("FAIL: simulator output missing '*** PASSED ***' marker")
        return 1

    print("\nAll stages passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
