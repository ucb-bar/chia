"""Build a SPEC CPU2006 CINT FireSim workload: compile SPEC and a FireMarshal base
in parallel, compose SPEC into the base rootfs, and package it for the run nodes.

    build_program (SPEC via speckle) ─┐
    build_base    (br-base)          ─┴─> compose(overlay=SPEC, base=br-base,
                                              jobs=one per benchmark)
                                          └─> workload package (sparse-tar bytes)
                                              to hand to a FireSim run node later

The base build runs concurrently with the (longer) SPEC compile — both are
dispatched before either is awaited.

Run (after `chia up cluster_sw.yaml -y`):
    chia job submit --working-dir . -- python spec_sw_build_loop.py
"""

import sys
from pathlib import Path

import ray

from chia.base.ChiaFunction import get
from chia.chipyard.riscv_build_node import RiscvBuildNode
from chia.chipyard.firemarshal_node import FireMarshalNode

COLLATERAL = Path(__file__).parent / "collateral"
WORK_DIR = "/tmp/spec_build"
SUITE_DIR = "cint2006/test"   # speckle overlay subpath for CINT2006, test inputs
CINT = ["400.perlbench", "401.bzip2", "403.gcc", "429.mcf", "445.gobmk", "456.hmmer",
        "458.sjeng", "462.libquantum", "464.h264ref", "471.omnetpp", "473.astar", "483.xalancbmk"]


def main() -> int:
    ray.init(address="auto")
    rv = RiscvBuildNode(timeout_seconds=4 * 60 * 60)
    fm = FireMarshalNode(timeout_seconds=60 * 60)

    # Dispatch both before awaiting -> base builds while SPEC compiles.
    base_ref = fm.build_base.chia_remote(fm, name="br-base")
    spec_ref = rv.build_program.chia_remote(
        rv,
        input_files={"build-cint.sh": (COLLATERAL / "build-cint.sh").read_bytes(),
                     "Makefile": (COLLATERAL / "Makefile").read_bytes()},
        command=["bash", "build-cint.sh", "test"],
        work_dir=WORK_DIR,
        outputs=["speckle/build/overlay"])

    base = get(base_ref)
    spec = get(spec_ref)
    if not base.success:
        print("base build failed\n" + base.stderr[-2000:]); return 1
    if not spec.success:
        print("SPEC build failed\n" + spec.stderr[-2000:]); return 1

    # SPEC outputs (keyed by speckle path) -> rootfs overlay under /root/spec,
    # preserving the per-benchmark tree.
    overlay = {f"root/spec/{p.split('overlay/', 1)[1]}": b
               for p, b in spec.files.items() if "overlay/" in p}

    # One job per benchmark; the run command depends on speckle's run scripts
    # (here: the cint.sh driver shipped in the overlay) -- tune to your setup.
    jobs = [{"name": b, "command": f"cd /root/spec/{SUITE_DIR} && ./cint.sh {b}",
             "outputs": ["/output"]} for b in CINT]

    wl = get(fm.compose.chia_remote(
        fm, base_name="br-base", overlay_files=overlay, name="spec-cint",
        config={"jobs": jobs, "post_run_hook": "handle-results.py"}))
    if not wl.success:
        print("compose failed\n" + wl.stderr[-2000:]); return 1

    # wl.archive is the packaged workload (per-benchmark images + FireSim
    # descriptor). Ship it to a FireSim run node in a future step.
    print(f"OK: workload package {len(wl.archive)} bytes, descriptor {wl.json_name}, {len(CINT)} jobs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
