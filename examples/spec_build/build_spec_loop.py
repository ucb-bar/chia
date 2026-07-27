"""Build SPEC CPU2006 CINT2006 (test) via RiscvBuildNode.build_program.

Copies the vendored collateral (build-cint.sh + Makefile) into the task dir as
input files; build-cint.sh clones speckle (pinned) and builds the CINT2006 suite
against the SPEC install at $SPEC_DIR (from the worker's -e in cluster.yaml). The
overlay binaries land at speckle/build/overlay/... and are collected back as bytes.

Run (after `chia up cluster.yaml -y`):
    chia job submit --no-wait --submission-id spec-cint-test --working-dir . -- python build_spec_loop.py
"""

import sys
from pathlib import Path

import ray

from chia.base.ChiaFunction import get
from chia.chipyard.riscv_build_node import RiscvBuildNode
from chia.chipyard.state_def import ProgramBuildArtifact

WORK_DIR = "/tmp/spec_build"
BUILD_TIMEOUT_S = 4 * 60 * 60

COLLATERAL = Path(__file__).parent / "collateral"


def main() -> int:
    ray.init(address="auto")
    node = RiscvBuildNode(timeout_seconds=BUILD_TIMEOUT_S)

    input_files = {
        "build-cint.sh": (COLLATERAL / "build-cint.sh").read_bytes(),
        "Makefile": (COLLATERAL / "Makefile").read_bytes(),
    }

    art: ProgramBuildArtifact = get(node.build_program.chia_remote(
        node,
        input_files=input_files,
        command=["bash", "build-cint.sh", "test"],
        work_dir=WORK_DIR,
        outputs=["speckle/build/overlay"],
    ))

    print(f"success={art.success} returncode={art.returncode} files={len(art.files)}")
    for name in sorted(art.files)[:40]:
        print(f"  {name} ({len(art.files[name])} bytes)")
    if not art.success or not art.files:
        print("--- stderr tail ---")
        print(art.stderr[-3000:])
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
