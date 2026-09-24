"""Split a FireMarshal workload into one job per FPGA.

Bringing the FPGAs up is :class:`~chia.aws.manager.AWSManager`'s job; running a
job is :meth:`~chia.firesim.manager_node.FireSimManagerNode.run_workload`'s.

TODO: this is a pure function of a FireMarshalArtifact and needs neither AWS nor
a cluster, so it likely belongs on the artifact itself, in FireMarshal.
"""

from __future__ import annotations

import io
import json
import os
import tarfile
import tempfile

from chia.cluster.log import get_logger
from chia.aws.s3 import S3Node
from chia.chipyard.state_def import FireMarshalArtifact
from chia.firesim.state_def import SimJob

logger = get_logger("firesim.sim_splitter")

S3_PREFIX = "workloads"


class SimSplitter:
    """Splits a FireMarshal workload into one :class:`SimJob` per job."""

    def __init__(self, s3_bucket: str):
        """
        Args:
            s3_bucket: Bucket the workload images are staged to.
        """
        self.s3_bucket = s3_bucket

    def split_workload(self, artifact: FireMarshalArtifact) -> list[SimJob]:
        """Unpack a FireMarshal workload into one SimJob per job, staged to S3.

        Uploading here means the N managers pull in parallel, instead of the
        images being replicated through Ray.
        """
        if not artifact.success or not artifact.archive:
            raise ValueError("FireMarshalArtifact is empty or failed")

        s3 = S3Node(self.s3_bucket)
        with tempfile.TemporaryDirectory() as work:
            with tarfile.open(fileobj=io.BytesIO(artifact.archive), mode="r:gz") as tar:
                tar.extractall(work)
            with open(os.path.join(work, artifact.json_name)) as f:
                descriptor = json.load(f)

            suite = descriptor["benchmark_name"]
            entries = descriptor.get("workloads") or [{
                "name": suite,
                "bootbinary": descriptor["common_bootbinary"],
                "rootfs": descriptor["common_rootfs"],
            }]

            def upload(filename: str) -> str:
                key = f"{S3_PREFIX}/{suite}/{filename}"
                s3.upload_file(os.path.join(work, filename), key)
                return f"s3://{self.s3_bucket}/{key}"

            jobs = [
                SimJob(benchmark_name=entry["name"],
                       rootfs_uri=upload(entry["rootfs"]),
                       bootbinary_uri=upload(entry["bootbinary"]),
                       outputs=entry.get("outputs", descriptor.get("common_outputs", [])),
                       simulation_outputs=descriptor.get(
                           "common_simulation_outputs", ["uartlog"]))
                for entry in entries
            ]
        logger.info(f"Split '{suite}' into {len(jobs)} job(s)")
        return jobs
