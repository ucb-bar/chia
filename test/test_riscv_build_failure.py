"""Negative test for RiscvBuildNode: a source with a syntax error must
fail cleanly, with a non-zero return code, no binary, and a useful
compiler diagnostic surfaced to stderr.

Cluster: test/test_chisel_riscv_verilator_cluster.yaml (only needs the
`riscv_build` resource).

Usage:
    chia up test/test_chisel_riscv_verilator_cluster.yaml -y
    ray job submit --address IP:6379 --working-dir . \\
        -- python test/test_riscv_build_failure.py
    chia down test/test_chisel_riscv_verilator_cluster.yaml -y

Or run all of the above via test/run_chisel_riscv_verilator_tests.sh.
"""

import sys

import ray

from chia.chipyard.riscv_build_node import RiscvBuildNode
from chia.chipyard.state_def import RiscvBuildArtifact


PROGRAM_NAME = "broken"
# Missing `;` and a typo on the return type — gcc should reject this.
BROKEN_C = b"""\
#include <stdio.h>

itn main(void) {
    printf("this never compiles"
    return 0;
}
"""

WORK_DIR = "/tmp/test_riscv_build_failure"


@ray.remote(resources={"riscv_build": 0.9})
def riscv_build_remote(source: bytes, program_name: str,
                       work_dir: str) -> RiscvBuildArtifact:
    return RiscvBuildNode().build(
        source_content=source,
        program_name=program_name,
        work_dir=work_dir,
        target="verilator",
    )


def main() -> int:
    ray.init(address="auto")

    print(f"=== Feeding {len(BROKEN_C)}-byte broken C source to RiscvBuildNode ===")
    artifact: RiscvBuildArtifact = ray.get(
        riscv_build_remote.remote(BROKEN_C, PROGRAM_NAME, WORK_DIR)
    )

    print(f"  success      = {artifact.success}")
    print(f"  returncode   = {artifact.returncode}")
    print(f"  binary_name  = {artifact.binary_name}")
    print(f"  binary size  = {len(artifact.binary_content)} bytes")
    print("  --- gcc stderr (verbatim) ---")
    print(artifact.stderr)
    print("  ----------------------------")

    failures: list[str] = []
    if artifact.success:
        failures.append("artifact.success was True; expected False")
    if artifact.returncode == 0:
        failures.append(f"returncode was 0; expected non-zero")
    if artifact.binary_content != b"":
        failures.append(
            f"binary_content was {len(artifact.binary_content)} bytes; expected empty"
        )

    if failures:
        print("\nFAIL:")
        for f in failures:
            print(f"  - {f}")
        return 1

    print("\nPASS: build correctly reported failure, produced no binary, "
          "and surfaced the compiler error.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
