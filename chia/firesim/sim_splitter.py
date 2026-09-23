"""Run a multi-job FireMarshal workload across a farm of F2 FPGAs.

One F2 instance per FPGA, each joined to this cluster as a Chia worker holding
one unit of ``firesim_fpga``. Jobs are submitted all at once and Ray admits one
per free FPGA, so nothing here schedules.

Runs on the head node, not as a Ray task, so it can wait on the jobs it submits.
"""

from __future__ import annotations

import io
import json
import os
import shlex
import tarfile
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

from chia.aws.config import AWSConfig, EC2InstanceConfig
from chia.aws.ec2 import launch_ec2_instances, terminate_ec2_instances, wait_for_instances
from chia.aws.host import EphemeralEC2Host
from chia.aws.s3 import S3Node
from chia.chipyard.state_def import FireMarshalArtifact
from chia.cluster.log import get_logger
from chia.firesim.fs_bitstream import FSBitstream
from chia.firesim.manager_node import FPGA_RESOURCE, FireSimManagerNode
from chia.firesim.state_def import SimFarm, SimJob, SimJobResult

logger = get_logger("firesim.sim_splitter")

CONTAINER_NAME = "chia-firesim"
S3_PREFIX = "workloads"

# The FPGA Developer AMI has the FPGA tools but not docker.
_USER_DATA = """#!/bin/bash
while fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1; do sleep 5; done
apt-get update -qq && apt-get install -y -qq docker.io > /dev/null
systemctl start docker
usermod -aG docker ubuntu
"""


class SimSplitter:
    """Owns a farm of F2 instances and runs one workload job per FPGA."""

    def __init__(
        self,
        aws_config: AWSConfig,
        s3_bucket: str,
        image: str = "ghcr.io/ucb-bar/chia-firesim:latest",
        instance_type: str = "f2.6xlarge",
    ):
        """
        Args:
            aws_config: Credentials, region, and networking for the F2 hosts.
            s3_bucket: Bucket the workload images and bitstreams are staged to.
            image: FireSim manager container image.
            instance_type: F2 instance type; one FPGA is used per instance.
        """
        self.aws_config = aws_config
        self.s3_bucket = s3_bucket
        self.image = image
        self.instance_type = instance_type
        self.chia_source_path = _chia_source()

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

    def launch(self, num_fpgas: int) -> SimFarm:
        """Bring up one F2 instance per FPGA and join them to this cluster."""
        config = EC2InstanceConfig(
            instance_type=self.instance_type,
            volume_size_gb=300,
            tags={"chia-op": "firesim-sim", "chia-cluster": "chia"},
            user_data=_USER_DATA,
        )
        logger.info(f"Launching {num_fpgas}x {self.instance_type}")
        instances = launch_ec2_instances(
            self.aws_config, config, count=num_fpgas, instance_name="chia-firesim-sim")
        farm = SimFarm(instance_ids=[i.instance_id for i in instances],
                       region=self.aws_config.region)
        try:
            # Measure from what the cluster already has, so a second farm does
            # not see the first one's FPGAs and return before its own join.
            baseline = _fpga_count()
            ready = wait_for_instances(farm.instance_ids, region=farm.region)
            with ThreadPoolExecutor(max_workers=len(ready)) as pool:
                joined = sum(pool.map(self._setup_host, ready))
            if not joined:
                raise RuntimeError("Every F2 host failed setup")
            self._wait_for_workers(baseline + joined)
        except Exception:
            self.teardown(farm)
            raise
        return farm

    def run(self, jobs: list[SimJob],
            bitstream: FSBitstream) -> dict[str, SimJobResult]:
        """Run every job on the farm and collect the results."""
        from chia.base.ChiaFunction import get

        published = bitstream.publish(self.s3_bucket, f"bitstreams/{bitstream.quintuplet}")
        node = FireSimManagerNode()
        # Ray is the queue: submit everything, the scheduler admits one job per
        # free firesim_fpga unit, so there is no window to keep here.
        refs = {job.benchmark_name: node.run_job.chia_remote(
                    node, job=job, bitstream=published)
                for job in jobs}
        logger.info(f"Submitted {len(refs)} job(s) to the farm")

        results = {}
        for name, ref in refs.items():
            try:
                results[name] = get(ref)
            except Exception as e:
                logger.error(f"Job {name} raised: {e}")
                results[name] = SimJobResult(name, success=False, log=repr(e))
            logger.info(f"Job {name}: "
                        f"{'SUCCESS' if results[name].success else 'FAILED'}")
        return results

    def teardown(self, farm: SimFarm) -> None:
        """Terminate every instance in the farm."""
        if farm.instance_ids:
            logger.info(f"Terminating {len(farm.instance_ids)} F2 instance(s)")
            terminate_ec2_instances(farm.instance_ids, region=farm.region)

    def _setup_host(self, instance) -> bool:
        """Start the FireSim container and join it to the cluster as a worker."""
        host = EphemeralEC2Host(instance, self.aws_config)
        src = self.chia_source_path
        try:
            host.wait_ready(timeout=600)
            host.run("sudo cloud-init status --wait", timeout=900, check=False)

            token = _github_token()
            if token:
                host.run(f"echo {shlex.quote(token)} | sudo docker login ghcr.io "
                         f"-u chia --password-stdin", timeout=60, check=False)
            host.run(f"sudo docker pull {shlex.quote(self.image)}", timeout=1800)

            host.run(f"mkdir -p {src}", timeout=10)
            host.rsync_up(f"{src}/", f"{src}/", exclude=[".git", "__pycache__", "*.pyc"])

            # --net=host makes the manager's "localhost" run farm host this
            # machine; --privileged + /dev let it reach the FPGA.
            host.run(
                f"sudo docker run -d --name {CONTAINER_NAME} "
                f"--net=host --privileged -v /dev:/dev -v {src}:{src}:ro "
                f"{shlex.quote(self.image)} sleep infinity", timeout=120)

            # So the manager can ssh to "localhost" as the AMI's ubuntu user.
            pubkey = host.run(
                f"sudo docker exec {CONTAINER_NAME} cat /home/ray/firesim.pem.pub",
                timeout=30).stdout.strip()
            host.run(
                f"mkdir -p ~/.ssh && chmod 700 ~/.ssh && "
                f"grep -qxF {shlex.quote(pubkey)} ~/.ssh/authorized_keys 2>/dev/null || "
                f"echo {shlex.quote(pubkey)} >> ~/.ssh/authorized_keys", timeout=30)

            host.run(
                f"sudo docker exec {CONTAINER_NAME} bash -lc " + shlex.quote(
                    f"export PYTHONPATH={src}:$PYTHONPATH && "
                    f"ray start --address={_head_address()} "
                    f"--resources='{json.dumps({FPGA_RESOURCE: 1})}'"), timeout=300)
            logger.info(f"[{instance.instance_id}] Worker joined")
            return True
        except Exception as e:
            logger.error(f"[{instance.instance_id}] Setup failed: {e}")
            return False

    def _wait_for_workers(self, expected: int, timeout: int = 600) -> None:
        """Block until the cluster advertises `expected` FPGAs in total."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if _fpga_count() >= expected:
                logger.info(f"{expected} FPGA worker(s) ready")
                return
            time.sleep(5)
        raise RuntimeError(f"Only {_fpga_count():.0f} of {expected} FPGA workers "
                           f"registered within {timeout}s")


def _head_address() -> str:
    """The head this splitter is connected to, which the F2 workers join."""
    import ray

    return ray.get_runtime_context().gcs_address


def _fpga_count() -> float:
    import ray

    return ray.cluster_resources().get(FPGA_RESOURCE, 0)


def _github_token() -> str:
    if os.environ.get("GITHUB_TOKEN"):
        return os.environ["GITHUB_TOKEN"].strip()
    path = os.path.expanduser("~/.config/chia/github-token")
    return open(path).read().strip() if os.path.isfile(path) else ""


def _chia_source() -> str:
    """The chia checkout to rsync onto each host and mount in the container.

    ``chia`` is a namespace package, so it has ``__path__`` but no ``__file__``.
    """
    import chia

    return os.path.dirname(os.path.abspath(list(chia.__path__)[0]))
