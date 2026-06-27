"""Smoke test for building FireSim metasim verilator binaries.

Builds the FireSimRocketConfig metasim verilator binary (VFireSim) on a
chipyard resource node to verify the Docker environment can compile
FireSim metasim targets via Golden Gate + Verilator.

The build runs:
    cd chipyard/sims/firesim/sim
    make TARGET_PROJECT=firesim \
        TARGET_PROJECT_MAKEFRAG=<chipyard>/generators/firechip/chip/src/main/makefrag/firesim \
        verilator

This builds the target config FireSimRocketConfig (default from config.mk):

class FireSimRocketConfig extends Config(
  new WithDefaultFireSimBridges ++
  new WithFireSimConfigTweaks ++
  new chipyard.RocketConfig)

Output binary: sims/firesim/sim/generated-src/f1/f1-firesim-FireSim-FireSimRocketConfig-BaseF1Config/VFireSim

Usage:
    ray job submit --address <head>:6379 --working-dir /scratch/acui/chia \
        -- python test/test_firesim_metasim_build.py
"""

import sys

import ray

from chia.chipyard.state_def import BuildTarget
from chia.chipyard.chisel_build_node import ChiselBuildNode

@ray.remote(resources={"chipyard": 0.9})
def build_firesim_metasim(
    chipyard_path: str = "/home/ray/chipyard",
    config: str = "FireSimRocketConfig",
    config_package: str = "firechip.chip",
    make_jobs: int = 32,
    timeout_seconds: int = 7200,
) -> dict:
    node = ChiselBuildNode(
        chipyard_path=chipyard_path,
        config=config,
        config_package=config_package,
        target=BuildTarget.FIRESIM_METASIM_VERILATOR,
        make_jobs=make_jobs,
        timeout_seconds=timeout_seconds,
    )
    result = node.build()
    return {
        "success": result.success,
        "returncode": result.returncode,
        "binary_name": result.simulator_binary_name,
        "binary_size": len(result.simulator_binary_content),
        "stdout_preview": result.stdout[-500:],
        "stderr_preview": result.stderr[-500:],
    }


def main():
    ray.init(address="auto")

    print("=== Test: FireSim metasim verilator build (FireSimRocketConfig) ===")
    result = ray.get(build_firesim_metasim.remote())

    print(f"  success={result['success']}  returncode={result['returncode']}")
    print(f"  binary: {result['binary_name']} ({result['binary_size']} bytes)")
    print(f"  stdout preview: {result['stdout_preview'][:200]}")
    print(f"  stderr preview: {result['stderr_preview'][:200]}")

    if not result["success"]:
        print(f"  FAIL: build exited with returncode {result['returncode']}")
        sys.exit(1)
    if result["binary_size"] == 0:
        print("  FAIL: binary is empty")
        sys.exit(1)
    print("  PASS")

    print("\nAll tests passed.")


if __name__ == "__main__":
    main()
