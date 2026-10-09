"""Bring a given EC2 node up as a Chia worker at runtime, and take it down.

The node is given, not discovered: a ``(NodeTypeConfig, AWSNodeConfig)`` pair,
the same classes a cluster file's node type and ``aws_nodes:`` entry parse
into. Each step is the code ``chia up`` runs for AWS nodes:

    provision_aws_nodes     launch the instances
    run_aws_setup           install docker (and chia's other defaults), synchronously
    add_nodes_to_cluster    ssh tunnel, container, ray start

The tunnel carries every Ray connection between the worker and the head, so
the head needs no inbound ports and may sit anywhere. Its ports are read from
the running cluster rather than from the cluster file.

AWSManager runs as a Ray actor on the head (:func:`start_aws_manager`): the
tunnels are processes on the head, and the head raylet's ports are read from
its ``/proc``.
"""

from __future__ import annotations

import copy
import os
from dataclasses import dataclass, replace

import ray

from chia.aws.config import AWSConfig
from chia.aws.ec2 import get_default_ami, terminate_ec2_instances
from chia.base.ChiaFunction import chia_actor
from chia.cluster.aws_nodes import AWSNodeConfig, provision_aws_nodes, run_aws_setup
from chia.cluster.config import (AWSClusterConfig, ClusterConfig, NodeTypeConfig,
                                 SSHAuthConfig, TunnelConfig, assign_nodes)
from chia.cluster.log import get_logger, setup_logging
from chia.cluster.node_setup import add_nodes_to_cluster

logger = get_logger("aws.manager")

AWSWorker = tuple[NodeTypeConfig, AWSNodeConfig]


@dataclass
class Farm:
    """The instances one :meth:`AWSManager.launch` brought up, and that manager."""
    name: str
    region: str
    ips: list[str]
    manager: "ray.actor.ActorHandle | None" = None

    def teardown(self) -> None:
        """Close the farm's tunnels and terminate its instances, through its manager."""
        ray.get(self.manager.teardown.remote(self))


class AWSManager:
    """Adds a given EC2 node to a running cluster, and removes it."""

    def __init__(self, cluster_config: ClusterConfig, aws_config: AWSConfig):
        """
        Args:
            cluster_config: The running cluster the node joins.
            aws_config: Account values the node definition leaves out: region,
                EC2 key pair, and the ssh user and key for it.
        """
        setup_logging()        # an actor process has no log handler; chia's CLIs set one up
        self.cluster_config = cluster_config
        self.aws_config = aws_config
        self._tunnels = {}     # farm ips -> the TunnelManager carrying their Ray traffic
        aws_cluster = cluster_config.aws_config or AWSClusterConfig()
        self._vpc_id = aws_cluster.vpc_id
        # The head's private IP with aws.connection "vpc"; None means SSH tunnels.
        self._vpc_head_ip = cluster_config.head_ip if aws_cluster.connection == "vpc" else None

    def launch(self, worker: AWSWorker, count: int = 1) -> Farm:
        """Bring up ``count`` instances of ``worker`` and join them to the cluster."""
        node_type, machine = self._configured(worker)
        aws = self.aws_config
        machine = replace(machine, count=count, KeyName=aws.key_name,
                          ssh_user=aws.ssh_user, ssh_private_key=aws.ssh_private_key,
                          ImageId=machine.ImageId or get_default_ami(aws.region))
        nodes = {node_type.name: machine}

        ips = provision_aws_nodes(self.cluster_config.cluster_name, nodes, aws.region,
                                  self._vpc_id, self._vpc_head_ip)[node_type.name]
        farm = Farm(node_type.name, aws.region, ips, ray.get_runtime_context().current_actor)
        try:
            # A copy: this launch's machines stay out of the manager's config.
            config = copy.deepcopy(self.cluster_config)
            tunnel = None if self._vpc_head_ip else self._head_tunnel_config()
            for ip in ips:
                config.auth_overrides[ip] = SSHAuthConfig(
                    ssh_user=aws.ssh_user, ssh_private_key=aws.ssh_private_key,
                    tunnel=tunnel)
            run_aws_setup(nodes, {node_type.name: ips}, config.get_ssh_auth)

            # add_nodes_to_cluster allocates tunnels across the whole config, keyed
            # by assign_nodes' worker indexes, so the node joins the copy first
            # and its assignments come from assign_nodes.
            config.worker_ips = config.worker_ips + ips
            # The container's AWS calls (F2_ECAD's aws_create_afi) need a region.
            docker = node_type.docker and replace(node_type.docker, run_options=[
                *node_type.docker.run_options, "-e", f"AWS_DEFAULT_REGION={aws.region}"])
            config.node_types[node_type.name] = replace(
                node_type, num_workers=count, compatible_ips=ips, docker=docker)
            self._tunnels[tuple(ips)] = add_nodes_to_cluster(
                config, [a for a in assign_nodes(config) if a.ip in ips])
        except Exception:
            self.teardown(farm)
            raise
        logger.info(f"{len(ips)} '{node_type.name}' worker(s) joined")
        return farm

    def _configured(self, worker: AWSWorker) -> AWSWorker:
        """``worker`` with the changes of its entry in the cluster file's ``aws: workers:``."""
        node_type, machine = worker
        aws = self.cluster_config.aws_config
        changes = dict(aws.workers.get(node_type.name, {})) if aws else {}
        if "image" in changes:
            node_type = replace(node_type, docker=replace(node_type.docker, image=changes.pop("image")))
        return node_type, replace(machine, **changes)

    def teardown(self, farm: Farm) -> None:
        """Close the farm's tunnels and terminate its instances."""
        # TODO: tunnels are kept per launch, so a farm with part of a launch's machines
        # (one FPGA taken down early) closes none; each exits when its machine goes.
        tunnels = self._tunnels.pop(tuple(farm.ips), None)
        if tunnels is not None:
            tunnels.stop_all()
        if farm.ips:
            import boto3

            # With aws.connection "vpc", the farm's IPs are private ones.
            address = "private-ip-address" if self._vpc_head_ip else "ip-address"
            reservations = boto3.client("ec2", region_name=farm.region).describe_instances(
                Filters=[{"Name": address, "Values": farm.ips}])["Reservations"]
            ids = [i["InstanceId"] for r in reservations for i in r["Instances"]]
            if ids:
                logger.info(f"Terminating {len(ids)} '{farm.name}' instance(s)")
                terminate_ec2_instances(ids, region=farm.region)

    def _head_tunnel_config(self) -> TunnelConfig:
        """Tunnel ports for the running head, all read from the running cluster:
        GCS and raylet ports as Ray reports them, and the worker-port range from
        the head raylet's arguments (this runs on the head)."""
        gcs_port = int(ray.get_runtime_context().gcs_address.rsplit(":", 1)[1])
        head = next(n for n in ray.nodes() if "node:__internal_head__" in n["Resources"])
        # Sorted: the workers' iptables rule takes the two as one low:high range.
        low, high = sorted((head["NodeManagerPort"], head["ObjectManagerPort"]))
        args = next(a for a in (_cmdline(p) for p in os.listdir("/proc") if p.isdigit())
                    if f"--node_id={head['NodeID']}" in a)
        worker = {k: int(next(a.split("=", 1)[1] for a in args
                              if a.startswith(f"--{k}_worker_port=")))
                  for k in ("min", "max")}
        return TunnelConfig(gcs_tunnel_port=gcs_port,
                            head_node_manager_port=low, head_object_manager_port=high,
                            head_worker_port_min=worker["min"],
                            head_worker_port_max=worker["max"])


def start_aws_manager(cluster_config: ClusterConfig, aws_config: AWSConfig):
    """Start an :class:`AWSManager` actor on the head and return its handle."""
    # TODO: read the cluster file that `chia up` used, so loops need not pass it.
    actor = ray.remote(AWSManager).options(
        num_cpus=0, resources={"node:__internal_head__": 0.001})
    return chia_actor(actor.remote(cluster_config, aws_config))


def _cmdline(pid: str) -> list[str]:
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            return f.read().decode().split("\0")
    except OSError:
        return []
