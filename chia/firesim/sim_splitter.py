"""Fan a multi-job FireMarshal workload out over a farm of F2 FPGAs.

Brings up one F2 instance per FPGA, joins each to the Ray cluster as an
ordinary Chia worker advertising one unit of ``firesim_fpga``, then submits one
:meth:`~chia.firesim.manager_node.FireSimManagerNode.run_job` task per job. Ray
is the queue: with more jobs than FPGAs the scheduler runs as many as fit and
holds the rest.

Service-pattern node (head-node only, not a Ray task), so it can submit and
wait on the per-job tasks without nesting inside one.
"""

from __future__ import annotations

import io
import json
import logging
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

# Docker and the FPGA management tools are on the FPGA Developer AMI, but
# docker.io is not — install it before SSH is used.
_USER_DATA = """#!/bin/bash
while fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1; do sleep 5; done
apt-get update -qq && apt-get install -y -qq docker.io > /dev/null
systemctl start docker
usermod -aG docker ubuntu
"""


class SimSplitter:
    """Owns a farm of F2 instances and runs one workload job per FPGA."""

    logging_name = "SimSplitter"

    def __init__(
        self,
        aws_config: AWSConfig,
        ray_address: str,
        s3_bucket: str,
        image: str = "ghcr.io/ucb-bar/chia-firesim:latest",
        instance_type: str = "f2.6xlarge",
        chia_source_path: str | None = None,
        logging_level: int = logging.DEBUG,
    ):
        """
        Args:
            aws_config: Credentials, region, and networking for the F2 hosts.
            ray_address: Head GCS address (``host:port``) the workers join.
            s3_bucket: Bucket that workload images and bitstreams are staged to.
            image: FireSim manager container image.
            instance_type: F2 instance type; one FPGA is used per instance.
            chia_source_path: Chia checkout rsynced to each host and mounted
                into the container. Defaults to this installation's path.
            logging_level: Logging level for this node's logger.
        """
        self.aws_config = aws_config
        self.ray_address = ray_address
        self.s3_bucket = s3_bucket
        self.image = image
        self.instance_type = instance_type
        self.chia_source_path = chia_source_path or _detect_chia_source()
        self.logger = logger
        self.logger.setLevel(logging_level)

    # ---- workload splitting -------------------------------------------------

    def split_workload(self, artifact: FireMarshalArtifact,
                       prefix: str = "workloads") -> list[SimJob]:
        """Unpack a FireMarshal workload and stage one :class:`SimJob` per job.

        Images are uploaded to S3 once here so that N managers pull them in
        parallel, rather than the bytes being replicated through Ray.

        Args:
            artifact: Output of ``FireMarshalNode.compose``.
            prefix: S3 key prefix for the staged images.

        Returns:
            One :class:`SimJob` per job in the workload descriptor.

        Raises:
            ValueError: If the artifact failed or carries no descriptor.
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
            sim_outputs = descriptor.get("common_simulation_outputs", ["uartlog"])
            entries = descriptor.get("workloads") or [{
                "name": suite,
                "bootbinary": descriptor["common_bootbinary"],
                "rootfs": descriptor["common_rootfs"],
                "outputs": descriptor.get("common_outputs", []),
            }]

            jobs = []
            for entry in entries:
                name = entry["name"]
                key_prefix = f"{prefix.strip('/')}/{suite}"
                rootfs = _upload(s3, self.s3_bucket, work, key_prefix, entry["rootfs"])
                bootbin = _upload(s3, self.s3_bucket, work, key_prefix, entry["bootbinary"])
                jobs.append(SimJob(
                    benchmark_name=name,
                    rootfs_uri=rootfs,
                    bootbinary_uri=bootbin,
                    outputs=entry.get("outputs", descriptor.get("common_outputs", [])),
                    simulation_outputs=sim_outputs,
                ))
        self.logger.info(f"Split '{suite}' into {len(jobs)} job(s)")
        return jobs

    # ---- farm lifecycle -----------------------------------------------------

    def launch(self, num_fpgas: int, market: str = "ondemand") -> SimFarm:
        """Bring up ``num_fpgas`` F2 instances and join them to the cluster.

        Args:
            num_fpgas: Number of FPGAs, one instance each.
            market: EC2 market for the hosts (``"ondemand"`` or ``"spot"``).

        Returns:
            The :class:`SimFarm` handle, for :meth:`run` and :meth:`teardown`.

        Raises:
            RuntimeError: If no host finished setup, or the workers did not
                register within the timeout.
        """
        config = EC2InstanceConfig(
            instance_type=self.instance_type,
            volume_size_gb=300,
            market=market,
            tags={"chia-op": "firesim-sim", "chia-cluster": "chia"},
            user_data=_USER_DATA,
        )
        self.logger.info(f"Launching {num_fpgas}x {self.instance_type}")
        instances = launch_ec2_instances(
            self.aws_config, config, count=num_fpgas, instance_name="chia-firesim-sim")
        instance_ids = [i.instance_id for i in instances]
        farm = SimFarm(instance_ids=instance_ids,
                       region=self.aws_config.region,
                       resource=FPGA_RESOURCE)
        try:
            # Count from what the cluster already has, so a second farm does not
            # see the first one's FPGAs and return before its own workers join.
            baseline = _fpga_resources()
            ready = wait_for_instances(instance_ids, region=self.aws_config.region)
            with ThreadPoolExecutor(max_workers=len(ready)) as pool:
                results = list(pool.map(self._setup_host, ready))
            if not any(results):
                raise RuntimeError("Every F2 host failed setup")
            self._wait_for_workers(baseline + sum(results))
        except Exception:
            self.teardown(farm)
            raise
        return farm

    def run(self, jobs: list[SimJob], bitstream: FSBitstream,
            plusarg_passthrough: str = "") -> dict[str, SimJobResult]:
        """Run every job on the farm and collect the results.

        All jobs are submitted at once; Ray schedules as many as there are free
        FPGAs and queues the rest.

        Args:
            jobs: Jobs from :meth:`split_workload`.
            bitstream: Image + driver to run them against.
            plusarg_passthrough: Extra ``+plusargs`` for the simulator.

        Returns:
            ``benchmark_name -> SimJobResult`` for every job.
        """
        from chia.base.ChiaFunction import get

        published = bitstream.publish(self.s3_bucket, f"bitstreams/{bitstream.quintuplet}")
        node = FireSimManagerNode()
        refs = {
            job.benchmark_name: node.run_job.chia_remote(
                node, job=job, bitstream=published,
                plusarg_passthrough=plusarg_passthrough)
            for job in jobs
        }
        self.logger.info(f"Submitted {len(refs)} job(s) to the farm")

        results: dict[str, SimJobResult] = {}
        for name, ref in refs.items():
            try:
                results[name] = get(ref)
            except Exception as e:
                self.logger.error(f"Job {name} raised: {e}")
                results[name] = SimJobResult(
                    benchmark_name=name, success=False, log=repr(e))
            status = "SUCCESS" if results[name].success else "FAILED"
            self.logger.info(f"Job {name}: {status}")
        return results

    def teardown(self, farm: SimFarm) -> None:
        """Terminate every instance in ``farm``.

        Args:
            farm: Handle returned by :meth:`launch`.
        """
        if not farm.instance_ids:
            return
        self.logger.info(f"Terminating {len(farm.instance_ids)} F2 instance(s)")
        terminate_ec2_instances(farm.instance_ids, region=farm.region)

    # ---- host setup ---------------------------------------------------------

    def _setup_host(self, instance) -> bool:
        """Start the FireSim container and join it to the cluster as a worker."""
        host = EphemeralEC2Host(instance, self.aws_config)
        try:
            host.wait_ready(timeout=600)
            host.run("sudo cloud-init status --wait", timeout=900, check=False)

            token = _github_token()
            if token:
                host.run(f"echo {shlex.quote(token)} | sudo docker login ghcr.io "
                         f"-u chia --password-stdin", timeout=60, check=False)
            host.run(f"sudo docker pull {shlex.quote(self.image)}", timeout=1800)

            host.run(f"mkdir -p {self.chia_source_path}", timeout=10)
            host.rsync_up(f"{self.chia_source_path}/", f"{self.chia_source_path}/",
                          exclude=[".git", "__pycache__", "*.pyc"])

            # --net=host so the manager's "localhost" run farm host is this
            # machine; --privileged + /dev so it can reach the FPGA.
            host.run(
                f"sudo docker run -d --name {CONTAINER_NAME} "
                f"--net=host --privileged -v /dev:/dev "
                f"-v {self.chia_source_path}:{self.chia_source_path}:ro "
                f"{shlex.quote(self.image)} sleep infinity",
                timeout=120)
            # Under --net=host the manager's "localhost" run farm host is this
            # machine, so its key has to be authorized for the AMI's ubuntu user.
            pubkey = host.run(
                f"sudo docker exec {CONTAINER_NAME} cat /home/ray/firesim.pem.pub",
                timeout=30).stdout.strip()
            host.run(
                f"mkdir -p ~/.ssh && chmod 700 ~/.ssh && "
                f"grep -qxF {shlex.quote(pubkey)} ~/.ssh/authorized_keys 2>/dev/null || "
                f"echo {shlex.quote(pubkey)} >> ~/.ssh/authorized_keys",
                timeout=30)

            host.run(
                f"sudo docker exec {CONTAINER_NAME} bash -lc "
                + shlex.quote(
                    f"export PYTHONPATH={self.chia_source_path}:$PYTHONPATH && "
                    f"ray start --address={self.ray_address} "
                    f"--resources='{json.dumps({FPGA_RESOURCE: 1})}'"),
                timeout=300)
            self.logger.info(f"[{instance.instance_id}] Worker joined")
            return True
        except Exception as e:
            self.logger.error(f"[{instance.instance_id}] Setup failed: {e}")
            return False

    def _wait_for_workers(self, expected: int, timeout: int = 600) -> None:
        """Block until the cluster advertises ``expected`` FPGAs in total."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            available = _fpga_resources()
            if available >= expected:
                self.logger.info(f"{available:.0f} FPGA worker(s) ready")
                return
            time.sleep(5)
        raise RuntimeError(
            f"Only {_fpga_resources():.0f} of {expected} FPGA workers "
            f"registered within {timeout}s")


def _fpga_resources() -> float:
    """FPGAs currently advertised by the whole Ray cluster."""
    import ray

    return ray.cluster_resources().get(FPGA_RESOURCE, 0)


def _upload(s3: S3Node, bucket: str, work: str, prefix: str, filename: str) -> str:
    key = f"{prefix}/{filename}"
    s3.upload_file(os.path.join(work, filename), key)
    return f"s3://{bucket}/{key}"


def _github_token() -> str:
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if token:
        return token
    path = os.path.expanduser("~/.config/chia/github-token")
    if os.path.isfile(path):
        with open(path) as f:
            return f.read().strip()
    return ""


def _detect_chia_source() -> str:
    import chia

    return os.path.dirname(os.path.dirname(os.path.abspath(chia.__file__)))
