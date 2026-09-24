"""Run a multi-job FireMarshal workload across a farm of F2 FPGAs.

    SimSplitter.split_workload(workload) -> jobs
    SimSplitter.launch(num_fpgas)        -> F2 workers in the cluster
    FireSimManagerNode.run_workload(job, bitstream) -> runs on any free FPGA

SimSplitter only splits and brings up FPGAs. Running a job is the manager's,
and Ray places each one on whichever FPGA is free, so with more jobs than FPGAs
the rest just queue.

Run (after `chia up <cluster>.yaml -y`):
    chia job submit --working-dir . -- python firesim_sim_loop.py
"""

import sys

import ray

from chia.aws.config import AWSConfig
from chia.cluster.config import load_config
from chia.base.ChiaFunction import get
from chia.chipyard.firemarshal_node import FireMarshalNode
from chia.firesim.fs_bitstream import FSBitstream
from chia.firesim.manager_node import FireSimManagerNode
from chia.firesim.sim_splitter import SimSplitter

S3_BUCKET = "firesim-chia-builds"
CLUSTER_YAML = "cluster.yaml"   # the cluster the F2 workers join
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

    splitter = SimSplitter(cluster_config=load_config(CLUSTER_YAML),
                           aws_config=AWSConfig(ssh_private_key="~/firesim.pem"),
                           s3_bucket=S3_BUCKET)
    jobs = splitter.split_workload(workload)
    farm = splitter.launch(NUM_FPGAS)

    # Submit every job; Ray places each on whichever FPGA is free.
    manager = FireSimManagerNode()
    bitstream = BITSTREAM.publish(S3_BUCKET, "bitstreams/rocket")
    try:
        refs = [manager.run_workload.chia_remote(manager, job=job,
                                                 bitstream=bitstream)
                for job in jobs]
        results = [get(ref) for ref in refs]
    finally:
        splitter.teardown(farm)

    for result in results:
        status = "PASS" if result.success else "FAIL"
        print(f"{result.benchmark_name}: {status} ({result.duration_seconds:.1f}s)")
        print((result.uartlog or result.log)[-500:])

    return 0 if all(r.success for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
