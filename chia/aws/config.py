from __future__ import annotations

from dataclasses import dataclass, field

import boto3

from chia.cluster.log import get_logger

_logger = get_logger("aws.config")


def _get_default_s3_bucket() -> str:
    """Generate a per-user bucket name from AWS account ID and region,
    following FireSim's convention. Auto-creates the bucket if it doesn't exist."""
    session = boto3.session.Session()
    sts = session.client("sts")
    account_id = sts.get_caller_identity()["Account"]
    region = session.region_name or "us-east-1"
    bucket_name = f"firesim-{account_id}-{region}"

    s3 = session.client("s3")
    try:
        s3.head_bucket(Bucket=bucket_name)
    except s3.exceptions.ClientError as exc:
        if "Not Found" in repr(exc) or "404" in repr(exc):
            _logger.info(f"Creating S3 bucket: {bucket_name}")
            if region == "us-east-1":
                s3.create_bucket(Bucket=bucket_name)
            else:
                s3.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": region},
                )
            s3.put_object(Bucket=bucket_name, Body=b"", Key="dcp/")
            s3.put_object(Bucket=bucket_name, Body=b"", Key="logs/")
        elif "Forbidden" in repr(exc):
            _logger.warning(f"Bucket {bucket_name} exists but is not accessible. Using name anyway.")
        else:
            raise
    return bucket_name


@dataclass
class AWSConfig:
    """AWS infrastructure configuration for FireSim operations."""
    # AWS region to launch instances in (e.g. "us-east-1", "us-west-2").
    region: str = "us-east-1"
    # Name of the EC2 key pair used for SSH access to launched instances.
    key_name: str = "firesim"
    # Name tag of the VPC to launch instances in. Used to look up the VPC ID
    # and its subnets.
    vpc_name: str = "firesim"
    # Name of the EC2 security group to attach to launched instances.
    security_group_name: str = "for-farms-only-firesim"
    # Username for SSH connections to launched instances (OS-dependent).
    ssh_user: str = "ubuntu"
    # Path to the SSH private key file corresponding to key_name.
    # None means SSH operations requiring a key will fail.
    ssh_private_key: str | None = None
    # Whether to use public IPs for SSH. Set True when the head node is
    # outside the VPC (e.g. on-prem or a different account).
    use_public_ip: bool = False
    # S3 bucket for build artifacts, workload manifests, and results.
    s3_bucket: str = "firesim-chia-builds"
    # Explicit VPC subnet ID to launch instances in. If None, auto-detected
    # from the head node's IMDS or falls back to the first subnet in the VPC.
    subnet_id: str | None = None
    # Path to a directory containing AWS credential files (config,
    # credentials). Mounted into Docker containers for S3/EC2 access.
    # If None, defaults to ~/.aws.
    aws_creds_dir: str | None = None


@dataclass
class EC2InstanceConfig:
    """Configuration for a single EC2 instance type to launch."""
    # EC2 instance type (e.g. "z1d.6xlarge", "f2.6xlarge", "f2.12xlarge").
    instance_type: str
    # Root EBS volume size in GiB.
    volume_size_gb: int = 200
    # Instance purchasing model. Values: "ondemand", "spot".
    market: str = "ondemand"
    # Maximum hourly price for spot instances. "ondemand" (the default) means
    # bid up to the on-demand price. Set a dollar amount (e.g. "1.50") to cap
    # the bid. Only used when market is "spot".
    spot_max_price: str = "ondemand"
    # Behavior when a spot instance is interrupted by AWS.
    # Values: "terminate" (default, one-time spot), "stop", "hibernate".
    # Non-"terminate" values also set SpotInstanceType to "persistent".
    spot_interruption_behavior: str = "terminate"
    # AMI ID to launch. If None, the default FireSim AMI for the region is
    # used (see get_default_ami).
    ami_id: str | None = None
    # EC2 tags applied to the launched instance (e.g. {"chia-op": "build"}).
    tags: dict[str, str] = field(default_factory=dict)
    # Cloud-init user-data script executed as root on first boot.
    # If None, no user-data is passed.
    user_data: str | None = None
