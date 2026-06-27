"""Smoke test for VerilatorRunNode.

Runs the hello.riscv binary on the FastRTLSimRocketConfig simulator
as a Ray remote task on a verilator_run resource node.

Usage:
    ray job submit --address IP:6379 --working-dir /scratch/acui/chia \
        -- python test/test_verilator_run.py
"""

import os
import sys
import asyncio
import ray

from chia.chipyard.state_def import BuildArtifact, BuildTarget
from chia.chipyard.verilator_run_node import VerilatorRunNode

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
SIMULATOR_NAME = "simulator-chipyard.harness-FastRTLSimRocketConfig"
TEST_BINARY_NAME = "hello.riscv"


def load_dramsim_ini() -> dict[str, bytes]:
    dramsim_dir = os.path.join(TEST_DIR, "dramsim_ini")
    ini: dict[str, bytes] = {}
    for fname in os.listdir(dramsim_dir):
        fpath = os.path.join(dramsim_dir, fname)
        if os.path.isfile(fpath):
            with open(fpath, "rb") as f:
                ini[fname] = f.read()
    return ini


def load_build_artifact() -> BuildArtifact:
    sim_path = os.path.join(TEST_DIR, SIMULATOR_NAME)
    with open(sim_path, "rb") as f:
        sim_content = f.read()
    return BuildArtifact(
        simulator_binary_content=sim_content,
        simulator_binary_name=SIMULATOR_NAME,
        config="MegaBoomV3Config",
        config_package="chipyard",
        target=BuildTarget.VERILATOR,
        success=True,
        stdout="",
        stderr="",
        returncode=0,
    )


def load_test_binary() -> bytes:
    with open(os.path.join(TEST_DIR, TEST_BINARY_NAME), "rb") as f:
        return f.read()


@ray.remote(resources={"verilator_run": 0.9})
def run_simulation(
    build_artifact: BuildArtifact,
    test_binary_content: bytes,
    test_binary_name: str,
    dramsim_ini: dict,
    verbose: bool,
) -> dict:
    node = VerilatorRunNode()
    result = node.run(
        build_artifact, test_binary_content, test_binary_name,
        work_dir="/tmp/test_verilator_run",
        dramsim_ini_files=dramsim_ini,
        verbose=verbose,
    )
    return {
        "success": result.success,
        "returncode": result.returncode,
        "log_lines": len(result.log.splitlines()),
        "out_lines": len(result.out.splitlines()),
        "log_preview": result.log[:500],
        "out_preview": result.out[:500],
    }


def main():
    ray.init(address="auto")

    build_artifact = load_build_artifact()
    test_binary = load_test_binary()
    dramsim_ini = load_dramsim_ini()

    print(f"Simulator: {SIMULATOR_NAME} ({len(build_artifact.simulator_binary_content)} bytes)")
    print(f"Test binary: {TEST_BINARY_NAME} ({len(test_binary)} bytes)")
    print(f"DRAMSim INI files: {list(dramsim_ini.keys())}")

    # --- Test 1: verbose=True ---
    print("\n=== Test 1: verbose=True ===")
    result = ray.get(
        run_simulation.remote(build_artifact, test_binary, TEST_BINARY_NAME, dramsim_ini, True)
    )
    print(f"  success={result['success']}  returncode={result['returncode']}")
    print(f"  log: {result['log_lines']} lines  out: {result['out_lines']} lines")
    print(f"  log preview: {result['log_preview'][:200]}")
    print(f"  out preview: {result['out_preview'][:200]}")

    if result["out_lines"] == 0:
        print("  FAIL: verbose=True but out is empty (spike-dasm produced no output)")
        sys.exit(1)
    print("  PASS")

    asyncio.sleep(10)  # small delay to ensure logs are flushed before next test

    # --- Test 2: verbose=False ---
    print("\n=== Test 2: verbose=False ===")
    result = ray.get(
        run_simulation.remote(build_artifact, test_binary, TEST_BINARY_NAME, dramsim_ini, False)
    )
    print(f"  success={result['success']}  returncode={result['returncode']}")
    print(f"  log: {result['log_lines']} lines  out: {result['out_lines']} lines")
    print(f"  log preview: {result['log_preview'][:200]}")
    print(f"  out preview: {result['out_preview'][:200]}")

    # Without +verbose, stderr only has DRAMSim init messages (a handful of lines),
    # not the full instruction trace (thousands of lines).
    if result["out_lines"] > 20:
        print(f"  FAIL: verbose=False but out has {result['out_lines']} lines (expected only DRAMSim init noise)")
        sys.exit(1)
    print("  PASS")

    print("\nAll tests passed.")


if __name__ == "__main__":
    main()
