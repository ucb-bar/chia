"""Example: lockstep co-simulation of one prebuilt cospike sim + ELF, via the
reusable CosimNode. The sim must be built from a cosim config (WithCospike +
WithTraceIO): spike rides inside it, checks every committed instruction, and
the run aborts at the first divergence.

    COSIM_SIM=/path/to/simulator-...  COSIM_BINARY=/path/to/test.elf \
      ray job submit --address IP:6379 --working-dir . -- \
      python chia/chipyard/test/spike_cosim_e2e_driver.py

Env: COSIM_SIM (required), COSIM_BINARY (required),
     COSIM_TIMEOUT_CYCLES (10_000_000),
     COSIM_NUM_CPUS (8, = the sim's build-time verilator threads),
     COSIM_OUT_ROOT (/tmp/spike_cosim).
"""
from __future__ import annotations

import os
import sys

import ray

TIMEOUT_CYCLES = int(os.environ.get("COSIM_TIMEOUT_CYCLES", "10000000"))
NUM_CPUS = int(os.environ.get("COSIM_NUM_CPUS", "8"))
OUT_ROOT = os.environ.get("COSIM_OUT_ROOT", "/tmp/spike_cosim")


def main() -> int:
    sim, binary = os.environ.get("COSIM_SIM"), os.environ.get("COSIM_BINARY")
    if not (sim and os.path.isfile(sim)) or not (binary and os.path.isfile(binary)):
        print("COSIM_SIM and COSIM_BINARY must point to existing files", file=sys.stderr)
        return 2

    chia_root = os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))))
    if chia_root not in sys.path:
        sys.path.insert(0, chia_root)
    ray.init(address="auto")

    from chia.base.ChiaFunction import get
    from chia.chipyard.cosim_node import CosimNode
    from chia.chipyard.state_def import BuildArtifact, BuildTarget
    with open(sim, "rb") as f:
        sim_bytes = f.read()
    with open(binary, "rb") as f:
        elf_bytes = f.read()
    artifact = BuildArtifact(
        name="cosim", simulator_binary_content=sim_bytes,
        simulator_binary_name=os.path.basename(sim), config="prebuilt",
        config_package="chipyard", target=BuildTarget.VERILATOR,
        success=True, stdout="", stderr="", returncode=0)

    elf_name = os.path.basename(binary)
    node = CosimNode()
    r = get(node.run.options(num_cpus=NUM_CPUS, max_retries=0).chia_remote(
        node, artifact, elf_bytes, elf_name, "/tmp/cosim", timeout_cycles=TIMEOUT_CYCLES))

    print(f"[cosim] {elf_name}: matched={r.matched} completed={r.completed} "
          f"cycles={r.sim_cycles} -> {'MATCH' if r.match else 'DIVERGENCE'}")
    if not r.match:
        print(f"  first divergence: {r.first_divergence}")
        if r.failing_trace_gz:
            os.makedirs(OUT_ROOT, exist_ok=True)
            path = os.path.join(OUT_ROOT, elf_name + ".faildiff.gz")
            with open(path, "wb") as f:
                f.write(r.failing_trace_gz)
            print(f"  failing-trace window: {path}")
    return 0 if r.match else 1


if __name__ == "__main__":
    sys.exit(main())
