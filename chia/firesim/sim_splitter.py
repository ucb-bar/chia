"""Run a multi-job FireMarshal workload across a farm of F2 FPGAs.

Only the F2 bring-up is ours. Once the instances are running, their IPs go
through :func:`~chia.cluster.node_setup.setup_worker_node`, the same call
``chia up`` uses, so the F2s join exactly like any other Chia worker — one unit
of ``firesim_fpga`` each. Jobs are then submitted all at once and Ray admits one
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
from chia.cluster.config import ClusterConfig, DockerConfig, NodeAssignment, NodeTypeConfig
from chia.cluster.log import get_logger
from chia.cluster.node_setup import setup_worker_node
from chia.cluster.ssh import SSHClient
from chia.aws.s3 import S3Node
from chia.chipyard.state_def import FireMarshalArtifact
from chia.firesim.fs_bitstream import FSBitstream
from chia.firesim.manager_node import FPGA_RESOURCE, FireSimManagerNode
from chia.firesim.state_def import SimFarm, SimJob, SimJobResult

logger = get_logger("firesim.sim_splitter")

NODE_TYPE = "firesim"
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
        cluster_config: ClusterConfig,
        aws_config: AWSConfig,
        s3_bucket: str,
        image: str = "ghcr.io/ucb-bar/chia-firesim:latest",
        instance_type: str = "f2.6xlarge",
    ):
        """
        Args:
            cluster_config: The cluster the F2 workers join.
            aws_config: Credentials, region, and networking for the F2 hosts.
            s3_bucket: Bucket the workload images and bitstreams are staged to.
            image: FireSim manager container image.
            instance_type: F2 instance type; one FPGA is used per instance.
        """
        self.cluster_config = cluster_config
        self.aws_config = aws_config
        self.s3_bucket = s3_bucket
        self.instance_type = instance_type
        self.node_type = NodeTypeConfig(
            name=NODE_TYPE,
            resources={FPGA_RESOURCE: 1},
            # --net=host is added by DockerManager; --privileged and /dev are
            # what let the manager reach the FPGA on this host.
            docker=DockerConfig(image=image, container_name="chia-firesim",
                                run_options=["--privileged", "-v", "/dev:/dev"]),
        )

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
        """Bring up one F2 instance per FPGA and join them to the cluster."""
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
            ready = wait_for_instances(farm.instance_ids, region=farm.region)
            ips = [i.public_ip if self.aws_config.use_public_ip else i.private_ip
                   for i in ready]
            with ThreadPoolExecutor(max_workers=len(ips)) as pool:
                joined = [ip for ip, ok in zip(ips, pool.map(self._join, ips)) if ok]
            if not joined:
                raise RuntimeError("No F2 host joined the cluster")
            logger.info(f"{len(joined)} of {len(ips)} F2 worker(s) joined")
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

    def _join(self, ip: str) -> bool:
        """Join one F2 host to the cluster the way every other worker joins."""
        try:
            setup_worker_node(self.cluster_config, NodeAssignment(
                ip=ip, node_type=self.node_type,
                resources=dict(self.node_type.resources)))
            self._authorize_manager_key(ip)
            logger.info(f"[{ip}] Worker joined")
            return True
        except Exception as e:
            logger.error(f"[{ip}] Join failed: {e}")
            return False

    def _authorize_manager_key(self, ip: str) -> None:
        """Let the manager ssh to "localhost", which under --net=host is here."""
        auth = self.cluster_config.get_ssh_auth(ip)
        ssh = SSHClient(ip, auth.ssh_user, auth.ssh_private_key)
        container = self.node_type.docker.container_name
        pubkey = ssh.run(
            f"sudo docker exec {container}-0 cat /home/ray/firesim.pem.pub",
            timeout=30).stdout.strip()
        ssh.run(f"mkdir -p ~/.ssh && chmod 700 ~/.ssh && "
                f"grep -qxF {shlex.quote(pubkey)} ~/.ssh/authorized_keys 2>/dev/null || "
                f"echo {shlex.quote(pubkey)} >> ~/.ssh/authorized_keys", timeout=30)
