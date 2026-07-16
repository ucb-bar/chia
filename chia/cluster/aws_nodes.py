"""AWS EC2 node provisioning and teardown for chia clusters.

Manages the lifecycle of EC2 instances declared in the ``aws_nodes`` YAML
section: launching instances during ``chia up``, discovering running
instances for ``chia down``, and terminating them on teardown.
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from chia.cluster.log import get_logger

logger = get_logger("aws_nodes")


def _boto3():
    """Lazy import of boto3 so the module can be imported without it installed."""
    import boto3
    return boto3

DEFAULT_IMAGE_ID = "ami-0a914de4dc1f18727"
DEFAULT_REGION = "us-west-2"

DEFAULT_AWS_SETUP_COMMANDS = [
    # Install git (before conda so system PATH is checked)
    "command -v git >/dev/null 2>&1 || (command -v apt-get >/dev/null 2>&1 && sudo apt-get update && sudo apt-get install -y git || sudo yum install -y git)",
    # Remove Ubuntu's non-interactive guard from bashrc so conda init
    # works in non-interactive bash --login sessions (SSH scripts).
    "sed -i '/^# If not running interactively/,/^esac$/d' ~/.bashrc",
    # Install miniconda
    "wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh",
    "bash /tmp/miniconda.sh -b -p $HOME/miniconda3",
    "$HOME/miniconda3/bin/conda init bash",
    "source $HOME/miniconda3/etc/profile.d/conda.sh",
    "conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main",
    "conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r",
    # Clone repo (before conda env so system git + SSH agent are used).
    # mkdir -p ~/.ssh first: OS Login logins serve keys from the OS Login
    # profile via AuthorizedKeysCommand and never create ~/.ssh (unlike the
    # guest agent's metadata-key path, which writes ~/.ssh/authorized_keys),
    # so the known_hosts append would otherwise fail on a fresh box.
    "mkdir -p ~/.ssh && chmod 700 ~/.ssh",
    "ssh-keyscan -t ed25519,rsa github.com >> ~/.ssh/known_hosts 2>/dev/null",
    "git clone git@github.com:ucb-bar/chia.git",
    # Create conda env and install chia
    "conda create -y -n chia_env python=3.10.19",
    "conda activate chia_env",
    "pip install -e chia",
    # Install docker (distro-agnostic)
    "command -v docker >/dev/null 2>&1 || (command -v apt-get >/dev/null 2>&1 && (curl -fsSL https://get.docker.com | sudo sh) || (sudo yum install -y docker && sudo systemctl start docker && sudo systemctl enable docker))",
    "sudo usermod -aG docker $USER",
]


@dataclass
class AWSNodeConfig:
    """Configuration for a single AWS node type to launch."""
    KeyName: str
    InstanceType: str
    count: int
    ImageId: str = DEFAULT_IMAGE_ID
    ssh_user: str | None = None
    ssh_private_key: str | None = None
    extra_args: dict[str, Any] = field(default_factory=dict)
    skip_default_setup: bool = False
    setup_commands: list[str] = field(default_factory=list)
    setup_timeout: int = 1800
    ssh_timeout: int = 120

    @property
    def effective_setup_commands(self) -> list[str]:
        """Default setup commands + user setup commands, unless defaults are skipped."""
        if self.skip_default_setup:
            return self.setup_commands
        return list(DEFAULT_AWS_SETUP_COMMANDS) + self.setup_commands


# ---------------------------------------------------------------------------
# Security group helpers
# ---------------------------------------------------------------------------

def _get_default_vpc(client: Any, region: str) -> tuple[str, dict[str, str]]:
    """Return (vpc_id, {availability_zone: subnet_id}) for the account's default VPC.

    Returns every default-VPC subnet keyed by AZ so callers can pick one
    whose AZ actually offers the desired instance type.
    """
    vpcs = client.describe_vpcs(
        Filters=[{"Name": "isDefault", "Values": ["true"]}],
    )["Vpcs"]
    if not vpcs:
        raise RuntimeError(
            f"No default VPC found in region {region}. "
            "Create one with: aws ec2 create-default-vpc"
        )
    vpc_id = vpcs[0]["VpcId"]

    subnets = client.describe_subnets(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}],
    )["Subnets"]
    if not subnets:
        raise RuntimeError(f"No subnets found in default VPC {vpc_id}")
    subnets_by_az: dict[str, str] = {}
    for s in subnets:
        subnets_by_az.setdefault(s["AvailabilityZone"], s["SubnetId"])

    logger.info(
        f"Using default VPC {vpc_id}, {len(subnets_by_az)} subnets across AZs: "
        f"{sorted(subnets_by_az)}"
    )
    return vpc_id, subnets_by_az


def _pick_subnet_for_instance_type(
    client: Any,
    subnets_by_az: dict[str, str],
    instance_type: str,
) -> str:
    """Return a subnet ID in an AZ that offers ``instance_type``."""
    resp = client.describe_instance_type_offerings(
        LocationType="availability-zone",
        Filters=[{"Name": "instance-type", "Values": [instance_type]}],
    )
    supported_azs = {o["Location"] for o in resp.get("InstanceTypeOfferings", [])}
    for az, subnet_id in subnets_by_az.items():
        if az in supported_azs:
            return subnet_id
    raise RuntimeError(
        f"No subnet in default VPC is in an AZ offering {instance_type}. "
        f"AZs with subnets: {sorted(subnets_by_az)}; "
        f"AZs offering {instance_type}: {sorted(supported_azs)}"
    )


def _resolve_head_ssh_cidrs() -> list[tuple[str, str]]:
    """Return ``[(cidr, description), ...]`` allowed to SSH into EC2 workers.

    Only the head (this machine, where ``chia up`` runs) ever initiates SSH to
    the EC2 workers — all Ray/tool traffic rides inside that tunnel — so the
    sole allowed source is the head's public egress IP.

    Overridable via ``CHIA_SSH_ALLOWED_CIDRS`` (comma-separated CIDRs) for
    static IPs, NAT quirks, or adding extra control hosts.  We deliberately do
    NOT fall back to ``0.0.0.0/0``: if the head IP can't be determined we raise,
    rather than silently leaving SSH open to the internet.
    """
    import os

    override = os.environ.get("CHIA_SSH_ALLOWED_CIDRS", "").strip()
    if override:
        return [(c.strip(), "chia allowed (env)") for c in override.split(",") if c.strip()]

    import urllib.request
    for url in ("https://checkip.amazonaws.com", "https://api.ipify.org"):
        try:
            ip = urllib.request.urlopen(url, timeout=5).read().decode().strip()
            if ip:
                return [(f"{ip}/32", "chia head")]
        except Exception:
            continue

    raise RuntimeError(
        "Could not determine the head's public IP to lock down the SSH "
        "security group. Set CHIA_SSH_ALLOWED_CIDRS (comma-separated CIDRs) "
        "to the control host(s) explicitly."
    )


def _reconcile_ssh_ingress(client, sg_id: str, desired: list[tuple[str, str]]) -> None:
    """Make the SG's port-22 ingress allow exactly *desired* (plus revoke world-open).

    Idempotent: authorizes any desired CIDR not already present and revokes a
    leftover ``0.0.0.0/0`` SSH rule (e.g. from an SG created before this change).
    Other manually-added specific CIDRs are left untouched.
    """
    sg = client.describe_security_groups(GroupIds=[sg_id])["SecurityGroups"][0]
    present: set[str] = set()
    has_world = False
    for perm in sg.get("IpPermissions", []):
        if perm.get("IpProtocol") != "tcp" or perm.get("FromPort") != 22 or perm.get("ToPort") != 22:
            continue
        for r in perm.get("IpRanges", []):
            cidr = r.get("CidrIp")
            present.add(cidr)
            if cidr == "0.0.0.0/0":
                has_world = True

    if has_world:
        client.revoke_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[{
                "IpProtocol": "tcp", "FromPort": 22, "ToPort": 22,
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
            }],
        )
        logger.info(f"Revoked world-open (0.0.0.0/0) SSH rule on {sg_id}")

    to_add = [(c, d) for c, d in desired if c not in present]
    if to_add:
        client.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[{
                "IpProtocol": "tcp", "FromPort": 22, "ToPort": 22,
                "IpRanges": [{"CidrIp": c, "Description": d} for c, d in to_add],
            }],
        )
        logger.info(f"Authorized SSH ingress on {sg_id} for {[c for c, _ in to_add]}")


def _vpc_cidrs(client, vpc_id: str) -> list[str]:
    """Return the VPC's associated IPv4 CIDR block(s)."""
    vpc = client.describe_vpcs(VpcIds=[vpc_id])["Vpcs"][0]
    cidrs = [
        a["CidrBlock"]
        for a in vpc.get("CidrBlockAssociationSet", [])
        if a.get("CidrBlockState", {}).get("State") == "associated"
    ]
    if not cidrs and vpc.get("CidrBlock"):
        cidrs = [vpc["CidrBlock"]]
    return cidrs


def _intra_vpc_enabled(default: bool) -> bool:
    """Resolve the intra-VPC switch, letting ``CHIA_ALLOW_INTRA_VPC`` override."""
    import os

    env = os.environ.get("CHIA_ALLOW_INTRA_VPC")
    if env is not None:
        return env.strip().lower() not in ("0", "false", "no", "off", "")
    return default


def _reconcile_intra_vpc_ingress(client, sg_id: str, vpc_cidrs: list[str]) -> None:
    """Allow all traffic from the VPC's own CIDR(s) so same-VPC machines can connect.

    Idempotent: only authorizes CIDRs not already present as an all-protocol
    (``-1``) ingress rule.
    """
    sg = client.describe_security_groups(GroupIds=[sg_id])["SecurityGroups"][0]
    present: set[str] = set()
    for perm in sg.get("IpPermissions", []):
        if perm.get("IpProtocol") == "-1":
            for r in perm.get("IpRanges", []):
                present.add(r.get("CidrIp"))

    to_add = [c for c in vpc_cidrs if c not in present]
    if to_add:
        client.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[{
                "IpProtocol": "-1",
                "IpRanges": [{"CidrIp": c, "Description": "chia intra-VPC"} for c in to_add],
            }],
        )
        logger.info(f"Authorized intra-VPC ingress on {sg_id} for {to_add}")


def ensure_ssh_security_group(
    cluster_name: str,
    region: str,
    allow_intra_vpc: bool = True,
) -> tuple[dict[str, str], str]:
    """Create or find the SSH security group in the default VPC.

    Inbound SSH (22) is locked to the head's public IP (see
    :func:`_resolve_head_ssh_cidrs`); egress is left at the AWS default.

    When *allow_intra_vpc* is True (the default; overridable via the
    ``CHIA_ALLOW_INTRA_VPC`` env var), an all-traffic ingress rule from the
    VPC's own CIDR is added so other machines in the same VPC can reach the
    nodes on any port.

    Returns ({availability_zone: subnet_id}, sg_id).
    """
    client = _boto3().client("ec2", region_name=region)
    vpc_id, subnets_by_az = _get_default_vpc(client, region)

    sg_name = f"chia-{cluster_name}-ssh"
    desired_cidrs = _resolve_head_ssh_cidrs()
    intra_vpc = _intra_vpc_enabled(allow_intra_vpc)
    vpc_cidrs = _vpc_cidrs(client, vpc_id) if intra_vpc else []

    # Check if SG already exists
    existing = client.describe_security_groups(
        Filters=[
            {"Name": "group-name", "Values": [sg_name]},
            {"Name": "vpc-id", "Values": [vpc_id]},
        ],
    )["SecurityGroups"]

    if existing:
        sg_id = existing[0]["GroupId"]
        logger.info(f"Security group {sg_name} already exists: {sg_id}")
        # Reconcile in case the head IP changed or the SG predates the lockdown.
        _reconcile_ssh_ingress(client, sg_id, desired_cidrs)
        if intra_vpc:
            _reconcile_intra_vpc_ingress(client, sg_id, vpc_cidrs)
        return subnets_by_az, sg_id

    # Create new SG
    resp = client.create_security_group(
        GroupName=sg_name,
        Description=f"SSH access for chia cluster {cluster_name}",
        VpcId=vpc_id,
    )
    sg_id = resp["GroupId"]

    client.authorize_security_group_ingress(
        GroupId=sg_id,
        IpPermissions=[
            {
                "IpProtocol": "tcp",
                "FromPort": 22,
                "ToPort": 22,
                "IpRanges": [{"CidrIp": c, "Description": d} for c, d in desired_cidrs],
            },
        ],
    )
    if intra_vpc:
        _reconcile_intra_vpc_ingress(client, sg_id, vpc_cidrs)
    logger.info(
        f"Created security group {sg_name}: {sg_id} "
        f"(SSH allowed from {[c for c, _ in desired_cidrs]}"
        f"{f'; intra-VPC from {vpc_cidrs}' if intra_vpc else ''})"
    )
    return subnets_by_az, sg_id


def cleanup_security_group(cluster_name: str, region: str) -> None:
    """Delete the SSH security group if it exists."""
    client = _boto3().client("ec2", region_name=region)
    sg_name = f"chia-{cluster_name}-ssh"

    existing = client.describe_security_groups(
        Filters=[{"Name": "group-name", "Values": [sg_name]}],
    )["SecurityGroups"]

    if not existing:
        return

    sg_id = existing[0]["GroupId"]
    try:
        client.delete_security_group(GroupId=sg_id)
        logger.info(f"Deleted security group {sg_name} ({sg_id})")
    except client.exceptions.ClientError as exc:
        # SG may still be referenced by recently-terminated instances
        logger.warning(f"Could not delete security group {sg_name}: {exc}")


# ---------------------------------------------------------------------------
# Provisioning
# ---------------------------------------------------------------------------

def _launch_one_instance(
    ec2_resource: Any,
    cluster_name: str,
    name: str,
    node_cfg: AWSNodeConfig,
    index: int,
    subnet_id: str,
    sg_id: str,
) -> str:
    """Launch a single tagged EC2 instance and return its instance ID."""
    tags = {
        "chia-cluster": cluster_name,
        "chia-node-type": name,
        "chia-node-index": str(index),
        "Name": f"chia-{cluster_name}-{name}-{index}",
    }
    tag_specs = [
        {
            "ResourceType": "instance",
            "Tags": [{"Key": k, "Value": v} for k, v in tags.items()],
        }
    ]

    create_args: dict[str, Any] = {
        "ImageId": node_cfg.ImageId,
        "InstanceType": node_cfg.InstanceType,
        "KeyName": node_cfg.KeyName,
        "MinCount": 1,
        "MaxCount": 1,
        # Enforce IMDSv2: require a session token for instance-metadata access
        # (blocks SSRF-style credential theft) and cap the response hop limit
        # at 1 so bridged Docker containers can't reach the metadata endpoint.
        # Placed before **extra_args so a per-node YAML override still wins.
        "MetadataOptions": {
            "HttpTokens": "required",
            "HttpPutResponseHopLimit": 1,
            "HttpEndpoint": "enabled",
        },
        "NetworkInterfaces": [
            {
                "SubnetId": subnet_id,
                "DeviceIndex": 0,
                "AssociatePublicIpAddress": True,
                "Groups": [sg_id],
            },
        ],
        "TagSpecifications": tag_specs,
        **node_cfg.extra_args,
    }

    instances = ec2_resource.create_instances(**create_args)
    return instances[0].id


def provision_aws_nodes(
    cluster_name: str,
    aws_nodes: dict[str, AWSNodeConfig],
    region: str,
) -> dict[str, list[str]]:
    """Launch EC2 instances for each node type.

    Returns ``{node_name: [public_ip_0, public_ip_1, ...]}``, ordered by
    index.  Instances are tagged for discovery by :func:`discover_aws_nodes`.
    """
    ec2_client = _boto3().client("ec2", region_name=region)
    ec2_resource = _boto3().resource("ec2", region_name=region)
    subnets_by_az, sg_id = ensure_ssh_security_group(cluster_name, region)

    # node_name -> list of instance IDs (ordered by index)
    ids_by_node: dict[str, list[str]] = {}
    launched_ids: list[str] = []  # for rollback on partial failure

    try:
        for name, node_cfg in aws_nodes.items():
            subnet_id = _pick_subnet_for_instance_type(
                ec2_client, subnets_by_az, node_cfg.InstanceType,
            )
            logger.info(
                f"Launching {node_cfg.count}x {node_cfg.InstanceType} "
                f"for node type '{name}' in subnet {subnet_id}"
            )
            node_ids: list[str] = []
            for i in range(node_cfg.count):
                iid = _launch_one_instance(
                    ec2_resource, cluster_name, name, node_cfg, i, subnet_id, sg_id,
                )
                node_ids.append(iid)
                launched_ids.append(iid)
                logger.info(f"  Launched {iid} (index {i})")
            ids_by_node[name] = node_ids

    except Exception:
        # Rollback: terminate anything we managed to launch
        if launched_ids:
            logger.error("Provisioning failed — terminating launched instances")
            _terminate_instances(launched_ids, region)
        raise

    # Wait for all instances to be running and collect IPs
    return _wait_and_collect_ips(ids_by_node, region)


def provision_missing_aws_nodes(
    cluster_name: str,
    aws_nodes: dict[str, AWSNodeConfig],
    region: str,
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Launch only the EC2 instances missing to reach the desired count per node type.

    Discovers instances already tagged for this cluster and launches just
    the difference, filling the lowest unused ``chia-node-index`` values
    so the combined map stays contiguous.

    Returns ``(full_ip_map, new_ip_map)``:

      - ``full_ip_map``: existing + newly launched IPs, sorted by index.
      - ``new_ip_map``: only newly launched IPs (for running setup).

    Both maps use the same ``{node_name: [ip, ...]}`` shape as
    :func:`provision_aws_nodes`.
    """
    existing = _discover_aws_nodes_with_indexes(cluster_name, region)

    # For each node type, compute indexes we need to launch.
    indexes_to_launch: dict[str, list[int]] = {}
    for name, cfg in aws_nodes.items():
        used = {idx for idx, _ in existing.get(name, [])}
        needed = cfg.count - len(used)
        if needed <= 0:
            continue
        picks: list[int] = []
        idx = 0
        while len(picks) < needed:
            if idx not in used:
                picks.append(idx)
            idx += 1
        indexes_to_launch[name] = picks

    new_ids_by_node: dict[str, list[str]] = {}
    if indexes_to_launch:
        ec2_client = _boto3().client("ec2", region_name=region)
        ec2_resource = _boto3().resource("ec2", region_name=region)
        subnets_by_az, sg_id = ensure_ssh_security_group(cluster_name, region)
        launched_ids: list[str] = []
        try:
            for name, indexes in indexes_to_launch.items():
                cfg = aws_nodes[name]
                subnet_id = _pick_subnet_for_instance_type(
                    ec2_client, subnets_by_az, cfg.InstanceType,
                )
                logger.info(
                    f"Launching {len(indexes)}x {cfg.InstanceType} "
                    f"for node type '{name}' (indexes {indexes}) in subnet {subnet_id}"
                )
                node_ids: list[str] = []
                for i in indexes:
                    iid = _launch_one_instance(
                        ec2_resource, cluster_name, name, cfg, i, subnet_id, sg_id,
                    )
                    node_ids.append(iid)
                    launched_ids.append(iid)
                    logger.info(f"  Launched {iid} (index {i})")
                new_ids_by_node[name] = node_ids
        except Exception:
            if launched_ids:
                logger.error("Provisioning failed — terminating launched instances")
                _terminate_instances(launched_ids, region)
            raise

    new_ip_map = (
        _wait_and_collect_ips(new_ids_by_node, region) if new_ids_by_node else {}
    )

    # Merge existing (index, ip) with newly launched ips.
    merged: dict[str, list[tuple[int, str]]] = {
        name: list(entries) for name, entries in existing.items()
    }
    for name, indexes in indexes_to_launch.items():
        new_ips = new_ip_map.get(name, [])
        merged.setdefault(name, [])
        for idx, ip in zip(indexes, new_ips):
            merged[name].append((idx, ip))

    full_ip_map: dict[str, list[str]] = {}
    for name, entries in merged.items():
        entries.sort(key=lambda e: e[0])
        full_ip_map[name] = [ip for _, ip in entries]

    return full_ip_map, new_ip_map


def _wait_and_collect_ips(
    ids_by_node: dict[str, list[str]],
    region: str,
    timeout: float = 600,
) -> dict[str, list[str]]:
    """Wait for instances to reach running state, return public IPs."""
    ec2_resource = _boto3().resource("ec2", region_name=region)
    deadline = time.monotonic() + timeout

    ip_map: dict[str, list[str]] = {}
    for name, instance_ids in ids_by_node.items():
        ips: list[str] = []
        for iid in instance_ids:
            inst = ec2_resource.Instance(iid)
            logger.info(f"Waiting for {iid} ({name}) to reach running state...")
            inst.wait_until_running()
            inst.reload()

            if time.monotonic() > deadline:
                raise TimeoutError(f"Timed out waiting for instance {iid}")

            public_ip = inst.public_ip_address
            if not public_ip:
                raise RuntimeError(
                    f"Instance {iid} has no public IP. "
                    "Ensure the subnet has auto-assign public IP enabled."
                )
            ips.append(public_ip)
            logger.info(f"  {iid}: public_ip={public_ip}")

        ip_map[name] = ips

    return ip_map


# ---------------------------------------------------------------------------
# Setup (post-provision, pre-cluster)
# ---------------------------------------------------------------------------

def run_aws_setup(
    aws_nodes: dict[str, AWSNodeConfig],
    ip_map: dict[str, list[str]],
    get_ssh_auth,
) -> None:
    """Run setup commands on freshly provisioned AWS instances.

    Executes each node type's ``setup_commands`` via SSH on all its
    instances.  Runs instances in parallel across different IPs.

    Args:
        aws_nodes: Node type configs (with setup_commands and setup_timeout).
        ip_map: ``{node_name: [ip, ...]}`` from :func:`provision_aws_nodes`.
        get_ssh_auth: Callable ``(ip) -> SSHAuthConfig`` for looking up
            per-IP SSH credentials (typically ``config.get_ssh_auth``).
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from chia.cluster.ssh import SSHClient
    from chia.cluster.log import log_phase

    tasks: list[tuple[str, str, AWSNodeConfig]] = []
    for name, cfg in aws_nodes.items():
        if not cfg.effective_setup_commands:
            continue
        for ip in ip_map.get(name, []):
            tasks.append((name, ip, cfg))

    if not tasks:
        logger.info("No AWS setup commands to run")
        return

    def _run_setup(name: str, ip: str, cfg: AWSNodeConfig):
        cmds = cfg.effective_setup_commands
        auth = get_ssh_auth(ip)
        # AWS instances accept only their dedicated key pair; use it exclusively
        # so a full ssh-agent can't exhaust sshd MaxAuthTries before it's tried.
        ssh = SSHClient(ip, auth.ssh_user, auth.ssh_private_key, identities_only=True)
        with log_phase(logger, f"Waiting for SSH to {ip} ({name})"):
            ssh.wait_for_ssh(timeout=cfg.ssh_timeout)
        with log_phase(logger, f"Running setup on {ip} ({name}) [{len(cmds)} commands, timeout={cfg.setup_timeout}s]"):
            ssh.run_script(cmds, timeout=cfg.setup_timeout)

    failed = []
    with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        futures = {
            pool.submit(_run_setup, name, ip, cfg): (name, ip)
            for name, ip, cfg in tasks
        }
        for fut in as_completed(futures):
            name, ip = futures[fut]
            try:
                fut.result()
                logger.info(f"Setup complete on {ip} ({name})")
            except Exception as e:
                logger.error(f"Setup FAILED on {ip} ({name}): {e}")
                failed.append((name, ip))

    if failed:
        raise RuntimeError(
            f"AWS setup failed on {len(failed)} instance(s): "
            f"{', '.join(f'{ip} ({name})' for name, ip in failed)}"
        )


# ---------------------------------------------------------------------------
# Discovery (for chia down)
# ---------------------------------------------------------------------------

def _discover_aws_nodes_with_indexes(
    cluster_name: str,
    region: str,
) -> dict[str, list[tuple[int, str]]]:
    """Find running EC2 instances and return ``{name: [(index, ip), ...]}``.

    Like :func:`discover_aws_nodes` but preserves the ``chia-node-index``
    tag so callers can pick unused indexes when provisioning additional
    instances.
    """
    client = _boto3().client("ec2", region_name=region)

    resp = client.describe_instances(
        Filters=[
            {"Name": "tag:chia-cluster", "Values": [cluster_name]},
            {"Name": "instance-state-name", "Values": ["running"]},
        ],
    )

    nodes: dict[str, list[tuple[int, str]]] = defaultdict(list)
    for reservation in resp["Reservations"]:
        for inst in reservation["Instances"]:
            tags = {t["Key"]: t["Value"] for t in inst.get("Tags", [])}
            node_type = tags.get("chia-node-type")
            node_index = tags.get("chia-node-index")
            public_ip = inst.get("PublicIpAddress")

            if node_type and node_index is not None and public_ip:
                nodes[node_type].append((int(node_index), public_ip))

    return {name: sorted(entries, key=lambda e: e[0]) for name, entries in nodes.items()}


def discover_aws_nodes(
    cluster_name: str,
    region: str,
) -> dict[str, list[str]]:
    """Find running EC2 instances for this cluster and return their public IPs.

    Returns ``{node_name: [public_ip_0, ...]}``, same format as
    :func:`provision_aws_nodes`.
    """
    entries_by_node = _discover_aws_nodes_with_indexes(cluster_name, region)
    ip_map: dict[str, list[str]] = {
        name: [ip for _, ip in entries] for name, entries in entries_by_node.items()
    }

    if ip_map:
        logger.info(f"Discovered AWS nodes for cluster '{cluster_name}': "
                     f"{', '.join(f'{k}({len(v)})' for k, v in ip_map.items())}")
    else:
        logger.info(f"No running AWS nodes found for cluster '{cluster_name}'")

    return ip_map


# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------

def _terminate_instances(instance_ids: list[str], region: str) -> None:
    """Terminate instances by ID."""
    if not instance_ids:
        return
    client = _boto3().client("ec2", region_name=region)
    client.terminate_instances(InstanceIds=instance_ids)
    logger.info(f"Terminated instances: {instance_ids}")


def teardown_aws_nodes(
    cluster_name: str,
    region: str,
) -> list[str]:
    """Terminate all EC2 instances tagged with this cluster.

    Returns list of terminated instance IDs.  Idempotent: returns empty
    list if no instances found.
    """
    client = _boto3().client("ec2", region_name=region)

    resp = client.describe_instances(
        Filters=[
            {"Name": "tag:chia-cluster", "Values": [cluster_name]},
            {"Name": "instance-state-name", "Values": ["running", "stopped", "pending"]},
        ],
    )

    instance_ids = []
    for reservation in resp["Reservations"]:
        for inst in reservation["Instances"]:
            instance_ids.append(inst["InstanceId"])

    if not instance_ids:
        logger.info(f"No AWS instances to terminate for cluster '{cluster_name}'")
        return []

    logger.info(f"Terminating {len(instance_ids)} instance(s) for cluster '{cluster_name}'")
    _terminate_instances(instance_ids, region)
    _wait_for_terminated(instance_ids, region)
    return instance_ids


def _wait_for_terminated(
    instance_ids: list[str],
    region: str,
    timeout: float = 300,
) -> None:
    """Wait for instances to reach the 'terminated' state."""
    if not instance_ids:
        return
    ec2_resource = _boto3().resource("ec2", region_name=region)
    logger.info(f"Waiting for {len(instance_ids)} instance(s) to terminate...")
    deadline = time.monotonic() + timeout
    for iid in instance_ids:
        inst = ec2_resource.Instance(iid)
        try:
            inst.wait_until_terminated(
                WaiterConfig={"Delay": 5, "MaxAttempts": int(timeout // 5)},
            )
        except Exception as e:
            if time.monotonic() > deadline:
                logger.warning(f"Timed out waiting for {iid} to terminate: {e}")
                return
            logger.warning(f"Error waiting for {iid} to terminate: {e}")
    logger.info("All instances terminated")
