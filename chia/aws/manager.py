"""Bring a given EC2 node up as a Chia worker at runtime, and take it down.

The node is given, not discovered: a ``(NodeTypeConfig, AWSNodeConfig)`` pair,
the same classes a cluster file's node type and ``aws_nodes:`` entry parse
into. Each step is the code ``chia up`` runs for AWS nodes:

    provision_aws_nodes     launch the instances
    run_aws_setup           install docker (and chia's other defaults), synchronously
    add_nodes_to_cluster    ssh tunnel, container, ray start

The tunnel carries every Ray connection between the worker and the head, so
the head needs no inbound ports and may sit anywhere. Its ports are read from
the running head rather than fixed in advance.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace

from chia.aws.config import AWSConfig
from chia.aws.ec2 import get_default_ami, terminate_ec2_instances
from chia.cluster.aws_nodes import AWSNodeConfig, provision_aws_nodes, run_aws_setup
from chia.cluster.config import (ClusterConfig, NodeAssignment, NodeTypeConfig,
                                 SSHAuthConfig, TunnelConfig)
from chia.cluster.log import get_logger
from chia.cluster.node_setup import add_nodes_to_cluster

logger = get_logger("aws.manager")

AWSWorker = tuple[NodeTypeConfig, AWSNodeConfig]


@dataclass
class Farm:
    """The instances one :meth:`AWSManager.launch` brought up."""
    name: str
    region: str
    ips: list[str]
    tunnels: object = None     # the TunnelManager carrying their Ray traffic


class AWSManager:
    """Adds a given EC2 node to a running cluster, and removes it."""

    def __init__(self, cluster_config: ClusterConfig, aws_config: AWSConfig):
        """
        Args:
            cluster_config: The running cluster the node joins.
            aws_config: Account values the node definition leaves out: region,
                EC2 key pair, and the ssh user and key for it.
        """
        self.cluster_config = cluster_config
        self.aws_config = aws_config

    def launch(self, worker: AWSWorker, count: int = 1) -> Farm:
        """Bring up ``count`` instances of ``worker`` and join them to the cluster."""
        node_type, machine = worker
        aws = self.aws_config
        machine = replace(machine, count=count, KeyName=aws.key_name,
                          ssh_user=aws.ssh_user, ssh_private_key=aws.ssh_private_key,
                          ImageId=machine.ImageId or get_default_ami(aws.region))
        nodes = {node_type.name: machine}

        ips = provision_aws_nodes(self.cluster_config.cluster_name, nodes,
                                  aws.region)[node_type.name]
        farm = Farm(node_type.name, aws.region, ips)
        try:
            config = self.cluster_config
            tunnel = self._head_tunnel_config()
            for ip in ips:
                config.auth_overrides[ip] = SSHAuthConfig(
                    ssh_user=aws.ssh_user, ssh_private_key=aws.ssh_private_key,
                    tunnel=tunnel)
            run_aws_setup(nodes, {node_type.name: ips}, config.get_ssh_auth)

            # add_nodes_to_cluster allocates tunnels across the whole config, so
            # the node joins it first.
            config.worker_ips = config.worker_ips + ips
            config.node_types[node_type.name] = replace(
                node_type, num_workers=count, compatible_ips=ips)
            farm.tunnels = add_nodes_to_cluster(config, [
                NodeAssignment(ip=ip, node_type=config.node_types[node_type.name],
                               resources=dict(node_type.resources))
                for ip in ips])
        except Exception:
            self.teardown(farm)
            raise
        logger.info(f"{len(ips)} '{node_type.name}' worker(s) joined")
        return farm

    def teardown(self, farm: Farm) -> None:
        """Close the farm's tunnels and terminate its instances."""
        if farm.tunnels is not None:
            farm.tunnels.stop_all()
        if farm.ips:
            import boto3

            reservations = boto3.client("ec2", region_name=farm.region).describe_instances(
                Filters=[{"Name": "ip-address", "Values": farm.ips}])["Reservations"]
            ids = [i["InstanceId"] for r in reservations for i in r["Instances"]]
            if ids:
                logger.info(f"Terminating {len(ids)} '{farm.name}' instance(s)")
                terminate_ec2_instances(ids, region=farm.region)

    def _head_tunnel_config(self) -> TunnelConfig:
        """Tunnel ports for the running head: its raylet ports as Ray reports
        them, and its worker-port range from its start command."""
        import ray

        head = next(n for n in ray.nodes()
                    if n["Alive"] and "node:__internal_head__" in n["Resources"])
        low, high = sorted((head["NodeManagerPort"], head["ObjectManagerPort"]))
        command = " ".join(self.cluster_config.head_start_ray_commands)
        ports = {k: re.search(rf"--{k}-worker-port[= ](\d+)", command)
                 for k in ("min", "max")}
        if not all(ports.values()):
            raise RuntimeError("The head's start command must set --min-worker-port "
                               "and --max-worker-port: each port is tunneled")
        return TunnelConfig(head_node_manager_port=low, head_object_manager_port=high,
                            head_worker_port_min=int(ports["min"].group(1)),
                            head_worker_port_max=int(ports["max"].group(1)))
