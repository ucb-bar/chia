"""Smoke test for FireSim metasim verilator binaries.

Runs hello.riscv on the FireSimRocketConfig metasim verilator binary
(VFireSim) on a verilator_run resource node to verify the Docker
environment has the correct runtime dependencies.

cd chipyard
source env.sh && source sims/firesim/env.sh && cd sims/firesim/sim && make TARGET_PROJECT=firesim \
  TARGET_PROJECT_MAKEFRAG=/scratch/acui/chipyard/generators/firechip/chip/src/main/makefrag/firesim verilator

This builds the target config FireSimRocketConfig

class FireSimRocketConfig extends Config(
  new WithDefaultFireSimBridges ++
  new WithFireSimConfigTweaks ++
  new chipyard.RocketConfig)

class RocketConfig extends Config(
  new freechips.rocketchip.rocket.WithNHugeCores(1) ++         // single rocket-core
  new chipyard.config.AbstractConfig)

Usage:
    ray job submit --address <head>:6379 --working-dir /scratch/acui/chia \
        -- python test/test_firesim_metasim_run.py
"""

import os
import sys

import ray

from chia.chipyard.state_def import BuildArtifact, BuildTarget
from chia.chipyard.verilator_run_node import VerilatorRunNode

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
METASIM_BINARY_NAME = "VFireSim"
TEST_BINARY_NAME = "hello.riscv"


def load_build_artifact() -> BuildArtifact:
    sim_path = os.path.join(TEST_DIR, METASIM_BINARY_NAME)
    with open(sim_path, "rb") as f:
        sim_content = f.read()
    return BuildArtifact(
        simulator_binary_content=sim_content,
        simulator_binary_name=METASIM_BINARY_NAME,
        config="FireSimRocketConfig",
        config_package="firechip.chip",
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
) -> dict:
    node = VerilatorRunNode()
    result = node.run_metasim(
        build_artifact, test_binary_content, test_binary_name,
        work_dir="/tmp/test_firesim_metasim",
        verbose=False,
        timeout_cycles=100_000_000,
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
    print(f"Metasim binary: {METASIM_BINARY_NAME} ({len(build_artifact.simulator_binary_content)} bytes)")
    print(f"Test binary: {TEST_BINARY_NAME} ({len(test_binary)} bytes)")

    print("\n=== Test: FireSim metasim run (hello.riscv) ===")
    result = ray.get(
        run_simulation.remote(build_artifact, test_binary, TEST_BINARY_NAME)
    )
    print(f"  success={result['success']}  returncode={result['returncode']}")
    print(f"  log: {result['log_lines']} lines  out: {result['out_lines']} lines")
    print(f"  log preview: {result['log_preview'][:200]}")
    print(f"  out preview: {result['out_preview'][:200]}")

    if not result["success"]:
        print(f"  FAIL: metasim exited with returncode {result['returncode']}")
        sys.exit(1)
    print("  PASS")

    print("\nAll tests passed.")


if __name__ == "__main__":
    main()
