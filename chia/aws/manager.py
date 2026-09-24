"""Bring EC2 instances up as Chia workers, and take them down.

The launch is ours; the join is not. Once an instance is running, its IP goes
through :func:`~chia.cluster.node_setup.setup_worker_node`, the same call
``chia up`` uses, so these workers are ordinary Chia workers in every respect.

One instance per worker: a worker advertises the resources of the machine it
has to itself (an FPGA, a Vivado install), which is the case this is for.

    farm = AWSManager(cluster_config, aws_config).launch(F2_SIM, count=4)
    ...                                     # tasks land on the new workers
    AWSManager(cluster_config, aws_config).teardown(farm)
"""

from __future__ import annotations

import os
import shlex
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from chia.aws.config import AWSConfig, EC2InstanceConfig
from chia.aws.ec2 import launch_ec2_instances, terminate_ec2_instances, wait_for_instances
from chia.cluster.config import (ClusterConfig, DockerConfig, NodeAssignment,
                                 NodeTypeConfig, SSHAuthConfig)
from chia.cluster.log import get_logger
from chia.cluster.node_setup import setup_worker_node
from chia.cluster.ssh import SSHClient

logger = get_logger("aws.manager")

# Docker is what the worker runs in, and the AMIs we use do not ship it.
_USER_DATA = """#!/bin/bash
while fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1; do sleep 5; done
apt-get update -qq && apt-get install -y -qq docker.io > /dev/null
systemctl start docker
usermod -aG docker ubuntu
"""


@dataclass
class AWSWorkerSpec:
    """One kind of EC2 worker: what machine, what container, what it advertises.

    Attributes:
        name: Node type name, used for the container name and EC2 tags.
        instance_type: EC2 instance type, one worker per instance.
        resources: Ray resources the worker advertises, e.g.
            ``{"firesim_fpga": 1}``. A ``@ChiaFunction`` asking for these lands
            on one of these workers.
        image: Container image the worker's Ray process runs in.
        ami_id: AMI to launch; ``None`` uses the FPGA Developer AMI, which is
            what carries Vivado and the AWS FPGA tooling.
        volume_size_gb: Root EBS volume size.
        run_options: Extra ``docker run`` flags. ``--net=host`` is already
            added by :class:`~chia.cluster.docker.DockerManager`.
        push_aws_creds: Copy the head's ``~/.aws`` to the instance and mount it
            into the container. Needed by workers that call AWS themselves —
            ``create-fpga-image`` for one. Instances with an IAM role do not
            need it.
        host_ssh_key: Path, inside the container, of an ssh key to authorize on
            the host. Set it when the worker must reach tooling that lives on
            the instance rather than in its container — FireSim's manager
            ssh'ing to "localhost" for the FPGA, or a build node invoking the
            AMI's Vivado. The key is generated if absent. ``None`` skips this.
        user_data: Extra boot script lines, run as root after docker is installed.
    """
    name: str
    instance_type: str
    resources: dict[str, float]
    image: str
    ami_id: str | None = None
    volume_size_gb: int = 300
    run_options: list[str] = field(default_factory=list)
    push_aws_creds: bool = False
    host_ssh_key: str | None = None
    user_data: str = ""

    def node_type(self) -> NodeTypeConfig:
        """The cluster's view of this worker, for ``setup_worker_node``."""
        run_options = list(self.run_options)
        if self.push_aws_creds:
            run_options += ["-v", "/home/ubuntu/.aws:/home/ray/.aws:ro"]
        return NodeTypeConfig(
            name=self.name,
            resources=dict(self.resources),
            docker=DockerConfig(image=self.image,
                                container_name=f"chia-{self.name}",
                                run_options=run_options),
        )


@dataclass
class Farm:
    """The instances one :meth:`AWSManager.launch` brought up.

    Attributes:
        spec_name: Name of the :class:`AWSWorkerSpec` they were launched from.
        instance_ids: EC2 instance ids.
        region: AWS region they live in.
        joined_ips: IPs that reached the cluster; the rest failed setup.
    """
    spec_name: str
    instance_ids: list[str]
    region: str
    joined_ips: list[str] = field(default_factory=list)


class AWSManager:
    """Launches EC2 instances as Chia workers and terminates them."""

    def __init__(self, cluster_config: ClusterConfig, aws_config: AWSConfig):
        """
        Args:
            cluster_config: The cluster the workers join.
            aws_config: Credentials, region, and networking for the instances.
        """
        self.cluster_config = cluster_config
        self.aws_config = aws_config

    def launch(self, spec: AWSWorkerSpec, count: int = 1) -> Farm:
        """Bring up ``count`` instances of ``spec`` and join them to the cluster.

        Returns once at least one worker has joined; the rest catch up, and Ray
        queues tasks against whatever is registered.

        Raises:
            RuntimeError: If no instance finished setup.
        """
        config = EC2InstanceConfig(
            instance_type=spec.instance_type,
            volume_size_gb=spec.volume_size_gb,
            ami_id=spec.ami_id,
            tags={"chia-op": spec.name,
                  "chia-cluster": self.cluster_config.cluster_name},
            user_data=_USER_DATA + spec.user_data,
        )
        logger.info(f"Launching {count}x {spec.instance_type} for '{spec.name}'")
        instances = launch_ec2_instances(
            self.aws_config, config, count=count,
            instance_name=f"chia-{spec.name}")
        farm = Farm(spec_name=spec.name,
                    instance_ids=[i.instance_id for i in instances],
                    region=self.aws_config.region)
        try:
            ready = wait_for_instances(farm.instance_ids, region=farm.region)
            ips = [i.public_ip if self.aws_config.use_public_ip else i.private_ip
                   for i in ready]
            # These instances answer to the EC2 key pair, not to whatever key
            # the cluster's own nodes use, so register that before any ssh.
            for ip in ips:
                self.cluster_config.auth_overrides[ip] = SSHAuthConfig(
                    ssh_user=self.aws_config.ssh_user,
                    ssh_private_key=self.aws_config.ssh_private_key)
            with ThreadPoolExecutor(max_workers=len(ips)) as pool:
                ok = pool.map(lambda ip: self._join(spec, ip), ips)
                farm.joined_ips = [ip for ip, joined in zip(ips, ok) if joined]
            if not farm.joined_ips:
                raise RuntimeError(f"No '{spec.name}' host joined the cluster")
            logger.info(f"{len(farm.joined_ips)} of {count} '{spec.name}' "
                        f"worker(s) joined")
        except Exception:
            self.teardown(farm)
            raise
        return farm

    def teardown(self, farm: Farm) -> None:
        """Terminate every instance in the farm."""
        if farm.instance_ids:
            logger.info(f"Terminating {len(farm.instance_ids)} "
                        f"'{farm.spec_name}' instance(s)")
            terminate_ec2_instances(farm.instance_ids, region=farm.region)

    def _join(self, spec: AWSWorkerSpec, ip: str) -> bool:
        try:
            if spec.push_aws_creds:
                # Before the container exists, so it can be mounted into it.
                self._push_aws_creds(ip)
            setup_worker_node(self.cluster_config, NodeAssignment(
                ip=ip, node_type=spec.node_type(),
                resources=dict(spec.resources)))
            if spec.host_ssh_key:
                self._authorize_container_key(spec, ip)
            logger.info(f"[{ip}] Joined as '{spec.name}'")
            return True
        except Exception as e:
            logger.error(f"[{ip}] Join failed: {e}")
            return False

    def _push_aws_creds(self, ip: str) -> None:
        """Copy the head's AWS credentials onto the instance."""
        creds = os.path.expanduser(self.aws_config.aws_creds_dir or "~/.aws")
        if not os.path.isdir(creds):
            raise RuntimeError(f"push_aws_creds is set but {creds} does not exist")
        auth = self.cluster_config.get_ssh_auth(ip)
        ssh = SSHClient(ip, auth.ssh_user, auth.ssh_private_key)
        ssh.run("mkdir -p ~/.aws", timeout=30)
        ssh.rsync_up(f"{creds}/", "/home/ubuntu/.aws/")

    def _authorize_container_key(self, spec: AWSWorkerSpec, ip: str) -> None:
        """Let the worker's container ssh into the instance hosting it."""
        auth = self.cluster_config.get_ssh_auth(ip)
        ssh = SSHClient(ip, auth.ssh_user, auth.ssh_private_key)
        container = f"{spec.node_type().docker.container_name}-0"
        key = spec.host_ssh_key
        pubkey = ssh.run(
            f"sudo docker exec {container} bash -lc "
            + shlex.quote(f"test -f {key} || ssh-keygen -q -t rsa -b 2048 -N '' -f {key}; "
                          f"cat {key}.pub"),
            timeout=60).stdout.strip().splitlines()[-1]
        ssh.run(f"mkdir -p ~/.ssh && chmod 700 ~/.ssh && "
                f"grep -qxF {shlex.quote(pubkey)} ~/.ssh/authorized_keys 2>/dev/null || "
                f"echo {shlex.quote(pubkey)} >> ~/.ssh/authorized_keys", timeout=30)
