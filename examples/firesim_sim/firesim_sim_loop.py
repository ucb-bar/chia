"""Run a multi-job FireMarshal workload across a farm of F2 FPGAs.

    FireMarshal workload (N jobs) ─┐
                                   ├─> SimSplitter.launch(num_fpgas)
    FSBitstream (image + driver)  ─┘        └─> one FireSimManagerNode per FPGA
                                                 └─> results back to the splitter

SimSplitter brings the F2 instances up, joins them to this Ray cluster as
ordinary Chia workers, and submits one job per FPGA. With more jobs than FPGAs
Ray queues the rest, so nothing here has to schedule.

Run (after `chia up <cluster>.yaml -y`):
    chia job submit --working-dir . -- python firesim_sim_loop.py
"""

import sys

import ray

from chia.aws.config import AWSConfig
from chia.base.ChiaFunction import get
from chia.chipyard.firemarshal_node import FireMarshalNode
from chia.firesim.fs_bitstream import FSBitstream
from chia.firesim.sim_splitter import SimSplitter

S3_BUCKET = "firesim-chia-builds"
NUM_FPGAS = 2

# The bitstream and the driver built against it. Either half may instead be
# passed by value (`*_bytes`) straight out of a build node.
BITSTREAM = FSBitstream(
    quintuplet="f2-firesim-FireSim-FireSimRocketConfig-DefaultF2Config",
    agfi="agfi-0123456789abcdef0",
    driver_uri=f"s3://{S3_BUCKET}/builds/latest/driver-bundle.tar.gz",
)


def main() -> int:
    ray.init(address="auto")

    fm = FireMarshalNode()
    workload = get(fm.compose.chia_remote(
        fm, base_name="br-base", overlay_files={}, name="hello",
        config={"jobs": [{"name": f"job{i}", "command": "uname -a"}
                         for i in range(4)]}))
    if not workload.success:
        print("compose failed\n" + workload.stderr[-2000:])
        return 1

    splitter = SimSplitter(aws_config=AWSConfig(ssh_private_key="~/firesim.pem"),
                           ray_address=ray.get_runtime_context().gcs_address,
                           s3_bucket=S3_BUCKET)
    jobs = splitter.split_workload(workload)
    farm = splitter.launch(NUM_FPGAS)
    try:
        results = splitter.run(jobs, BITSTREAM)
    finally:
        splitter.teardown(farm)

    for name, result in sorted(results.items()):
        status = "PASS" if result.success else "FAIL"
        print(f"{name}: {status} ({result.duration_seconds:.1f}s)")
        print((result.uartlog or result.log)[-500:])

    return 0 if all(r.success for r in results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
