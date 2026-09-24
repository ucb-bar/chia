"""Thin wrapper around AWS EC2 operations for launching/terminating instances.

Imports FireSim's awstools functions when available, falling back to direct
boto3 calls otherwise.
"""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from typing import Any

import boto3
from botocore.exceptions import ClientError, WaiterError

from chia.aws.config import AWSConfig, EC2InstanceConfig
from chia.cluster.log import get_logger

logger = get_logger("aws.ec2")


@dataclass
class EC2Instance:
    """Serializable representation of a launched EC2 instance."""
    instance_id: str
    private_ip: str
    public_ip: str | None
    instance_type: str


def _get_local_subnet_id() -> str | None:
    """Auto-detect the subnet of this EC2 instance via IMDS."""
    import urllib.request
    try:
        token_req = urllib.request.Request(
            "http://169.254.169.254/latest/api/token",
            method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "30"},
        )
        with urllib.request.urlopen(token_req, timeout=2) as resp:
            token = resp.read().decode()
        mac_req = urllib.request.Request(
            "http://169.254.169.254/latest/meta-data/network/interfaces/macs/",
            headers={"X-aws-ec2-metadata-token": token},
        )
        with urllib.request.urlopen(mac_req, timeout=2) as resp:
            mac = resp.read().decode().strip().rstrip("/").split("\n")[0].rstrip("/")
        subnet_req = urllib.request.Request(
            f"http://169.254.169.254/latest/meta-data/network/interfaces/macs/{mac}/subnet-id",
            headers={"X-aws-ec2-metadata-token": token},
        )
        with urllib.request.urlopen(subnet_req, timeout=2) as resp:
            return resp.read().decode().strip()
    except Exception:
        return None


def _get_vpc_and_security_group(
    ec2_resource: Any, client: Any, aws_config: AWSConfig
) -> tuple[str, str]:
    """Look up the VPC subnet and security group by name tags.

    Subnet selection priority:
      1. aws_config.subnet_id (explicit)
      2. Auto-detected from this instance's IMDS (same subnet as head node)
      3. First subnet in the VPC (fallback)
    """
    vpc_filter = [{"Name": "tag:Name", "Values": [aws_config.vpc_name]}]
    vpcs = list(ec2_resource.vpcs.filter(Filters=vpc_filter))
    if not vpcs:
        raise RuntimeError(f"No VPC found with tag Name={aws_config.vpc_name}")

    if aws_config.subnet_id:
        subnet_id = aws_config.subnet_id
        logger.info(f"Using configured subnet: {subnet_id}")
    else:
        # Try auto-detecting from IMDS (pins to head node's subnet)
        local_subnet = _get_local_subnet_id()
        if local_subnet:
            # Verify it belongs to our VPC
            vpc_subnets = {s.subnet_id for s in vpcs[0].subnets.filter()}
            if local_subnet in vpc_subnets:
                subnet_id = local_subnet
                logger.info(f"Auto-detected head node subnet: {subnet_id}")
            else:
                subnets = list(vpcs[0].subnets.filter())
                subnet_id = subnets[0].subnet_id
                logger.warning(
                    f"Local subnet {local_subnet} not in VPC {aws_config.vpc_name}, "
                    f"falling back to {subnet_id}"
                )
        else:
            subnets = list(vpcs[0].subnets.filter())
            if not subnets:
                raise RuntimeError(f"No subnets found in VPC {aws_config.vpc_name}")
            subnet_id = subnets[0].subnet_id
            logger.info(f"Using first VPC subnet: {subnet_id}")

    sg_filter = {"Filters": [
        {"Name": "group-name", "Values": [aws_config.security_group_name]},
        {"Name": "vpc-id", "Values": [vpcs[0].vpc_id]},
    ]}
    sgs = client.describe_security_groups(**sg_filter)["SecurityGroups"]
    if not sgs:
        raise RuntimeError(
            f"No security group {aws_config.security_group_name!r} in VPC {vpcs[0].vpc_id}"
        )
    sg_id = sgs[0]["GroupId"]

    return subnet_id, sg_id


def _construct_market_options(config: EC2InstanceConfig) -> dict[str, Any]:
    """Build InstanceMarketOptions dict for spot or on-demand."""
    if config.market == "spot":
        spot_opts: dict[str, Any] = {}
        if config.spot_max_price != "ondemand":
            spot_opts["MaxPrice"] = config.spot_max_price
        if config.spot_interruption_behavior != "terminate":
            spot_opts["InstanceInterruptionBehavior"] = config.spot_interruption_behavior
            spot_opts["SpotInstanceType"] = "persistent"
        return {"MarketType": "spot", "SpotOptions": spot_opts}
    return {}


def get_default_ami(region: str | None = None) -> str:
    """Look up the F2/FPGA Developer AMI (Ubuntu) in the current (or given) region.

    Uses the same AMI name pattern as FireSim's awstools.get_f2_ami_name(),
    with fallback attempts for hotfix version bumps.
    """
    client = boto3.client("ec2", region_name=region) if region else boto3.client("ec2")

    # Try the exact FireSim AMI name first, then increment patch version
    base_name = "FPGA Developer AMI (Ubuntu) - 1.17"
    response = client.describe_images(
        Filters=[{"Name": "name", "Values": [f"{base_name}*"]}],
    )
    images = response.get("Images", [])
    if not images:
        # Broader fallback
        response = client.describe_images(
            Filters=[{"Name": "name", "Values": ["*FPGA Developer AMI*Ubuntu*"]}],
        )
        images = response.get("Images", [])

    if not images:
        raise RuntimeError("No F2 Developer AMI found in this region")
    images.sort(key=lambda i: i.get("CreationDate", ""), reverse=True)
    ami_id = images[0]["ImageId"]
    logger.info(f"Auto-detected F2 AMI: {ami_id} ({images[0].get('Name', '?')})")
    return ami_id


def launch_ec2_instances(
    aws_config: AWSConfig,
    instance_config: EC2InstanceConfig,
    count: int = 1,
    instance_name: str = "chia-firesim",
) -> list[EC2Instance]:
    """Launch EC2 instances and return serializable EC2Instance objects.

    Args:
        aws_config: AWS/VPC configuration.
        instance_config: Instance type and market configuration.
        count: Number of instances to launch.

    Returns:
        List of EC2Instance dataclasses (safe for Ray serialization).
    """
    ec2 = boto3.resource("ec2", region_name=aws_config.region)
    client = boto3.client("ec2", region_name=aws_config.region)

    subnet_id, sg_id = _get_vpc_and_security_group(ec2, client, aws_config)

    ami_id = instance_config.ami_id or get_default_ami(aws_config.region)
    market_options = _construct_market_options(instance_config)

    block_devices = [
        {
            "DeviceName": "/dev/sda1",
            "Ebs": {
                "VolumeSize": instance_config.volume_size_gb,
                "VolumeType": "gp2",
            },
        },
    ]

    tags = {
        **instance_config.tags,
        "Name": instance_name,
    }
    tag_specs = [
        {
            "ResourceType": "instance",
            "Tags": [{"Key": k, "Value": v} for k, v in tags.items()],
        }
    ]

    create_args: dict[str, Any] = {
        "ImageId": ami_id,
        "InstanceType": instance_config.instance_type,
        "MinCount": count,
        "MaxCount": count,
        "KeyName": aws_config.key_name,
        "EbsOptimized": True,
        "BlockDeviceMappings": block_devices,
        "NetworkInterfaces": [
            {
                "SubnetId": subnet_id,
                "DeviceIndex": 0,
                "AssociatePublicIpAddress": True,
                "Groups": [sg_id],
            }
        ],
        "TagSpecifications": tag_specs,
    }
    if instance_config.user_data:
        create_args["UserData"] = instance_config.user_data
    if market_options:
        create_args["InstanceMarketOptions"] = market_options

    logger.info(f"Launching {count}x {instance_config.instance_type} instances")
    max_attempts = 3
    retry_sleep = 300  # 5 minutes
    boto_instances = None
    for attempt in range(1, max_attempts + 1):
        try:
            boto_instances = ec2.create_instances(**create_args)
            break
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            # retry because spot quota takes some time to reflect after instance termination
            if code == "MaxSpotInstanceCountExceeded" and attempt < max_attempts:
                logger.warning(
                    f"Spot quota hit (attempt {attempt}/{max_attempts}): {e}. "
                    f"Sleeping {retry_sleep}s before retry."
                )
                time.sleep(retry_sleep)
                continue
            raise

    results = []
    for inst in boto_instances:
        results.append(EC2Instance(
            instance_id=inst.id,
            private_ip="",
            public_ip=None,
            instance_type=instance_config.instance_type,
        ))
    return results


def wait_for_instances(
    instance_ids: list[str],
    region: str = "us-east-1",
    timeout: float = 600,
) -> list[EC2Instance]:
    """Wait for instances to reach 'running' state and populate IP addresses.

    Args:
        instance_ids: List of EC2 instance IDs to wait on.
        region: AWS region.
        timeout: Maximum seconds to wait.

    Returns:
        Updated list of EC2Instance with IP addresses filled in.
    """
    ec2 = boto3.resource("ec2", region_name=region)
    deadline = time.monotonic() + timeout

    results = []
    for iid in instance_ids:
        inst = ec2.Instance(iid)
        logger.info(f"Waiting for {iid} to reach running state...")
        inst.wait_until_running()
        inst.reload()

        if time.monotonic() > deadline:
            raise TimeoutError(f"Timed out waiting for instance {iid}")

        results.append(EC2Instance(
            instance_id=iid,
            private_ip=inst.private_ip_address or "",
            public_ip=inst.public_ip_address,
            instance_type=inst.instance_type,
        ))
        logger.info(f"  {iid}: private_ip={results[-1].private_ip}, public_ip={results[-1].public_ip}")

    return results


def terminate_ec2_instances(
    instance_ids: list[str],
    region: str = "us-east-1",
    dryrun: bool = False,
    wait: bool = True,
    wait_timeout: int = 600,
) -> None:
    """Terminate EC2 instances by ID.

    Args:
        instance_ids: Instance IDs to terminate.
        region: AWS region.
        dryrun: If True, validate the request without actually terminating.
        wait: If True, block until every instance reaches the `terminated`
            state. Ensures the caller can safely assume capacity/quota has
            been released before returning.
        wait_timeout: Max seconds to wait for termination (when wait=True).
    """
    if not instance_ids:
        return
    client = boto3.client("ec2", region_name=region)
    logger.info(f"Terminating instances: {instance_ids} (dryrun={dryrun})")
    client.terminate_instances(InstanceIds=instance_ids, DryRun=dryrun)
    if wait and not dryrun:
        delay = 15
        waiter = client.get_waiter("instance_terminated")
        try:
            waiter.wait(
                InstanceIds=instance_ids,
                WaiterConfig={"Delay": delay, "MaxAttempts": max(1, wait_timeout // delay)},
            )
            logger.info(f"Instances terminated: {instance_ids}")
        except WaiterError as e:
            # Termination request was accepted; AWS just hasn't flipped state
            # within wait_timeout. Don't propagate — doing so masks the caller's
            # real result via Python's `finally` semantics.
            logger.warning(
                f"Terminate waiter timed out for {instance_ids} after {wait_timeout}s: {e}. "
                f"Instances should still terminate asynchronously."
            )
