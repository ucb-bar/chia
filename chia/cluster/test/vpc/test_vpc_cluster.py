"""Live test of how chia connects its AWS machines: the cluster of
test_vpc_cluster.yaml, and one more machine that AWSManager launches into it.

With connection "vpc" (the default), the machines join with their private IPs and no
SSH tunnel; with "tunnel", through SSH tunnels from the head. Either way, every node
must reach every other node, the head included.

Run on the head after `chia up` (see test_vpc_cluster.yaml):
    python -m pytest chia/cluster/test/vpc/test_vpc_cluster.py -s
"""

from __future__ import annotations

import os
import subprocess
import sys
import unittest

import boto3
import ray
from ray import cloudpickle
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from chia.aws.config import AWSConfig
from chia.aws.manager import start_aws_manager
from chia.base.ChiaFunction import ChiaFunction, get
from chia.cluster.config import NodeTypeConfig, load_config, load_raw_config, parse_aws_nodes

# The workers cannot import this test module, so its functions travel by value.
cloudpickle.register_pickle_by_value(sys.modules[__name__])

CLUSTER_YAML = os.environ.get("CHIA_TEST_CLUSTER",
                              os.path.join(os.path.dirname(__file__), "test_vpc_cluster.yaml"))
PAYLOAD = 4 << 20      # bytes that each call sends back, so data crosses each link
TIMEOUT = 600


def _on(node_id: str) -> NodeAffinitySchedulingStrategy:
    return NodeAffinitySchedulingStrategy(node_id, soft=False)


@ChiaFunction(num_cpus=0)
def _node_and_payload() -> tuple[str, bytes]:
    return ray.get_runtime_context().get_node_id(), os.urandom(PAYLOAD)


# Runs on the node it is pinned to, and calls the target node from there.
@ChiaFunction(num_cpus=0)
def _call(target: str) -> tuple[str, str, int]:
    node, payload = get(_node_and_payload.options(scheduling_strategy=_on(target)).chia_remote())
    return ray.get_runtime_context().get_node_id(), node, len(payload)


class TestAWSConnection(unittest.TestCase):

    def test_nodes_reach_each_other(self):
        if not all(os.environ.get(v) for v in ("HEAD_IP", "HEAD_SSH_KEY", "EC2_KEYNAME", "EC2_SSH_KEY", "UBUNTU_AMI")):
            self.skipTest("needs the cluster of test_vpc_cluster.yaml, see that file")
        cluster = load_config(CLUSTER_YAML)
        aws = cluster.aws_config
        vpc = cluster.connection == "vpc"
        ec2 = boto3.client("ec2", region_name=aws.region)
        ray.init(address=f"{cluster.head_ip}:{cluster.head_gcs_port}")
        try:
            manager = start_aws_manager(cluster, AWSConfig(
                region=aws.region, key_name=aws.key_name,
                ssh_user=aws.ssh_user, ssh_private_key=aws.ssh_private_key))
            # The cluster's vpc_worker machine, as a node type of its own.
            machine = parse_aws_nodes(load_raw_config(CLUSTER_YAML))[0]["vpc_worker"]
            node_type = NodeTypeConfig(name="vpc_extra", resources={"vpc_extra": 1},
                                       docker=cluster.node_types["vpc_worker"].docker)
            farm = get(manager.launch.chia_remote((node_type, machine), count=1))
            ids = [i["InstanceId"] for r in ec2.describe_instances(Filters=[
                {"Name": "private-ip-address" if vpc else "ip-address", "Values": farm.ips},
            ])["Reservations"] for i in r["Instances"]]
            try:
                self._check(cluster, ec2, vpc, farm.ips)
            finally:
                farm.teardown()
            states = [i["State"]["Name"] for r in ec2.describe_instances(InstanceIds=ids)["Reservations"]
                      for i in r["Instances"]]
            self.assertTrue(ids and all(s in ("shutting-down", "terminated") for s in states), states)
        finally:
            ray.shutdown()

    def _check(self, cluster, ec2, vpc: bool, farm_ips: list[str]) -> None:
        machines = {i["PrivateIpAddress"]: i.get("PublicIpAddress") for r in ec2.describe_instances(Filters=[
            {"Name": "tag:chia-cluster", "Values": [cluster.cluster_name]},
            {"Name": "instance-state-name", "Values": ["running"]},
        ])["Reservations"] for i in r["Instances"]}
        self.assertEqual(len(machines), 3)
        # AWSManager gives private IPs with vpc, and public ones with tunnel.
        self.assertLessEqual(set(farm_ips), set(machines) if vpc else set(machines.values()))

        head = ray.get_runtime_context().get_node_id()
        workers = [n for n in ray.nodes()
                   if n["Alive"] and {"vpc_worker", "vpc_extra"} & n["Resources"].keys()]
        self.assertEqual(len(workers), 3)
        for n in workers:
            # A machine joins with its private IP, or with its tunnel's address on the head.
            if vpc:
                self.assertIn(n["NodeManagerAddress"], machines)
            else:
                self.assertTrue(n["NodeManagerAddress"].startswith("127."), n["NodeManagerAddress"])

        # chia's tunnels are "ssh -N ... user@<machine>" processes on the head.
        procs = subprocess.run(["ps", "-ww", "-eo", "args"], capture_output=True, text=True).stdout.splitlines()
        tunnels = [p for p in procs if p.startswith("ssh -N ")]
        for private, public in machines.items():
            tunneled = any(p.endswith((f"@{private}", f"@{public}")) for p in tunnels)
            self.assertEqual(tunneled, not vpc, f"tunnel to {private} ({public}); tunnels go to "
                             f"{[p.rsplit(' ', 1)[-1] for p in tunnels]}")

        # The cluster's security group: with vpc, all traffic from its members, the head
        # included; with tunnel, only SSH from given addresses, and the head is not a member.
        nic = ec2.describe_network_interfaces(Filters=[
            {"Name": "addresses.private-ip-address", "Values": [cluster.head_ip]},
        ])["NetworkInterfaces"][0]
        group = ec2.describe_security_groups(Filters=[
            {"Name": "group-name", "Values": [f"chia-{cluster.cluster_name}-ssh"]},
            {"Name": "vpc-id", "Values": [nic["VpcId"]]},
        ])["SecurityGroups"][0]
        rules = [(r["IpProtocol"], r.get("FromPort"), [p["GroupId"] for p in r["UserIdGroupPairs"]],
                  bool(r["IpRanges"])) for r in group["IpPermissions"]]
        head_member = group["GroupId"] in [g["GroupId"] for g in nic["Groups"]]
        if vpc:
            self.assertEqual(rules, [("-1", None, [group["GroupId"]], False)])
            self.assertTrue(head_member)
        else:
            self.assertTrue(rules and all(r == ("tcp", 22, [], True) for r in rules), rules)
            self.assertFalse(head_member)

        # Every node calls every other node and gets a payload back.
        nodes = [head] + [n["NodeID"] for n in workers]
        calls = {(a, b): _call.options(scheduling_strategy=_on(a)).chia_remote(b)
                 for a in nodes for b in nodes if a != b}
        for (a, b), ref in calls.items():
            self.assertEqual(get(ref, timeout=TIMEOUT), (a, b, PAYLOAD))


if __name__ == "__main__":
    unittest.main()
