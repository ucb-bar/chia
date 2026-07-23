"""GCP Compute Engine node provisioning and teardown for chia clusters.

The GCP analog of :mod:`chia.cluster.aws_nodes`.  Manages the lifecycle of
Compute Engine instances declared in the ``gcp_nodes`` YAML section: launching
instances during ``chia up``, discovering running instances for ``chia down``,
and terminating them on teardown.

This module is a deliberate **structural mirror** of ``aws_nodes.py`` — same
public function names and return shapes — so that a future ``CloudProvisioner``
abstraction can wrap both with no signature churn.  Everything downstream of
"get me the IPs" (placeholder expansion, tunnel injection, SSH setup, the
tunnels themselves) is already provider-agnostic, so the only GCP-specific
surface is here: provision / discover / teardown / firewall.

Key differences from AWS, all contained in this module:

* **Zonal model.**  Instances are zonal, subnets regional, firewall rules
  network-global.  Config carries ``project`` + a default ``zone`` with a
  per-node-type ``zone`` override.
* **SSH keys (no ``KeyName``).**  By default we inject ``<user>:<public-key>``
  via instance metadata; the GCP guest agent provisions the user and
  ``authorized_keys``.  The SSH *client* reuses ``ssh_private_key`` unchanged.
  ``use_os_login`` flips to OS Login (registration left as a documented stub).
* **Firewall rules vs security groups.**  Rules live on the VPC network and
  target instances by network *tag*; see :func:`ensure_ssh_firewall`.
* **Labels.**  Discovery tags are GCP labels (lowercase ``[a-z0-9_-]``).
* **``extra_args`` is structured.**  boto3 splats flat kwargs; compute_v1 takes
  an ``Instance`` proto, so ``extra_args`` is deep-merged onto the built
  instance (keys = ``compute_v1.Instance`` snake_case fields).

Credentials come from Application Default Credentials (``gcloud auth
application-default login`` or ``GOOGLE_APPLICATION_CREDENTIALS``); ``project``
comes from config.
"""

from __future__ import annotations

import os
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from chia.cluster.log import get_logger
# The default bootstrap commands are cloud-agnostic (install git/conda/docker,
# clone the repo); reuse the single source of truth from aws_nodes.  Importing
# aws_nodes is cheap — its boto3 import is lazy, so this does not require boto3.
from chia.cluster.aws_nodes import (
    DEFAULT_AWS_SETUP_COMMANDS as DEFAULT_SETUP_COMMANDS,
    _intra_vpc_enabled,
    _resolve_head_ssh_cidrs,
    run_aws_setup as run_gcp_setup,  # provider-agnostic; re-exported for symmetry
)

logger = get_logger("gcp_nodes")

# Public re-export so callers can use a GCP-named symbol (see module docstring).
__all__ = [
    "GCPNodeConfig", "DEFAULT_IMAGE", "DEFAULT_ZONE",
    "provision_gcp_nodes", "provision_missing_gcp_nodes",
    "discover_gcp_nodes", "teardown_gcp_nodes",
    "ensure_ssh_firewall", "cleanup_firewall", "run_gcp_setup",
]


def _compute():
    """Lazy import of compute_v1 so the module imports without the GCP lib."""
    from google.cloud import compute_v1
    return compute_v1


DEFAULT_IMAGE = "projects/debian-cloud/global/images/family/debian-12"
DEFAULT_ZONE = "us-central1-a"
DEFAULT_NETWORK = "default"

# How long to wait on a single insert/delete extended operation.
_OP_TIMEOUT = 600.0


@dataclass
class GCPNodeConfig:
    """Configuration for a single GCP node type to launch.

    Mirrors :class:`~chia.cluster.aws_nodes.AWSNodeConfig`'s *interface*
    (``ssh_user`` / ``ssh_private_key`` / ``effective_setup_commands`` /
    ``setup_timeout`` / ``ssh_timeout`` / ``extra_args``) so the shared setup
    runner and tunnel-injection helpers work by duck typing.
    """
    machine_type: str            # e.g. "n1-standard-4"  (≈ AWS InstanceType)
    count: int
    image: str = DEFAULT_IMAGE   # source image/family   (≈ AWS ImageId)
    zone: str | None = None      # per-type zone override (GCP is zonal)
    disk_size_gb: int | None = None
    spot: bool = False           # → scheduling.provisioning_model = SPOT
    ssh_user: str | None = None
    ssh_private_key: str | None = None
    ssh_public_key: str | None = None   # else derive <ssh_private_key>.pub
    use_os_login: bool = False          # metadata-key path is the default
    extra_args: dict[str, Any] = field(default_factory=dict)
    skip_default_setup: bool = False
    setup_commands: list[str] = field(default_factory=list)
    setup_timeout: int = 1800
    ssh_timeout: int = 120
    # Join the cluster over the tailnet instead of reverse SSH tunnels.
    # None (default) resolves to True when the cluster config has a
    # top-level ``tailnet:`` section, else False. Set explicitly to
    # override either way.
    join_tailnet: bool | None = None

    @property
    def effective_setup_commands(self) -> list[str]:
        """Default setup commands + user setup commands, unless defaults are skipped."""
        if self.skip_default_setup:
            return self.setup_commands
        return list(DEFAULT_SETUP_COMMANDS) + self.setup_commands


# ---------------------------------------------------------------------------
# Naming / sanitization helpers
# ---------------------------------------------------------------------------

def _sanitize_name(s: str) -> str:
    """Sanitize *s* into a valid GCP resource name / network tag.

    GCP instance names and network tags must match
    ``[a-z]([-a-z0-9]*[a-z0-9])?`` and be <= 63 chars (no underscores).
    """
    out = re.sub(r"[^a-z0-9-]", "-", s.lower()).strip("-")
    if not out or not out[0].isalpha():
        out = "c" + out
    return out[:63].rstrip("-")


def _sanitize_label(s: str) -> str:
    """Sanitize *s* into a valid GCP label value (lowercase ``[a-z0-9_-]``, <=63)."""
    return re.sub(r"[^a-z0-9_-]", "-", s.lower())[:63]


def _network_tag(cluster_name: str) -> str:
    """Network tag applied to a cluster's instances (firewall target)."""
    return _sanitize_name(f"chia-{cluster_name}")


def _network_url(network: str | None) -> str:
    """Normalize a network name/path to a partial URL compute_v1 accepts."""
    net = network or DEFAULT_NETWORK
    if "/" in net:
        return net
    return f"global/networks/{net}"


def _region_of(zone: str) -> str:
    """``us-central1-a`` -> ``us-central1``."""
    return zone.rsplit("-", 1)[0]


def _machine_type_url(zone: str, machine_type: str) -> str:
    if "/" in machine_type:
        return machine_type
    return f"zones/{zone}/machineTypes/{machine_type}"


def _deep_merge(base: dict, overlay: dict) -> dict:
    """Recursively merge *overlay* into *base* (overlay wins). Returns *base*."""
    for k, v in overlay.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v
    return base


# ---------------------------------------------------------------------------
# SSH key metadata (the GCP analog of AWS KeyName)
# ---------------------------------------------------------------------------

def _read_public_key(cfg: GCPNodeConfig, default_private_key: str | None) -> str:
    """Resolve and read the SSH public key to inject via metadata.

    Order: explicit ``ssh_public_key``, else ``<ssh_private_key>.pub``, else
    ``<default_private_key>.pub``.
    """
    pub_path = cfg.ssh_public_key
    if not pub_path:
        priv = cfg.ssh_private_key or default_private_key
        if priv:
            pub_path = priv + ".pub"
    if not pub_path:
        raise RuntimeError(
            "GCP metadata SSH-key auth needs a public key: set 'ssh_public_key' "
            "(or 'ssh_private_key' so <key>.pub can be derived) on the node type "
            "or in the global auth block, or set use_os_login: true."
        )
    pub_path = os.path.expanduser(pub_path)
    with open(pub_path) as f:
        return f.read().strip()


def _gcp_ssh_metadata(
    cfg: GCPNodeConfig,
    ssh_user: str | None,
    default_private_key: str | None,
) -> list[tuple[str, str]]:
    """Return instance-metadata ``(key, value)`` pairs for SSH access.

    Default (metadata keys): ``ssh-keys: "<user>:<public-key>"`` and
    ``enable-oslogin: FALSE`` so a project-default OS Login setting does not
    shadow the injected key (an *org-enforced* OS Login policy can still
    override this — see the module docstring).

    With ``use_os_login`` we instead emit ``enable-oslogin: TRUE``.  Registering
    the key against the user's OS Login profile (the OS Login API call) is left
    as a documented TODO — this is the structural hook for "support both".
    """
    if cfg.use_os_login:
        # TODO(os-login): register the key via OS Login API
        # (oslogin_v1.OsLoginServiceClient().import_ssh_public_key) and resolve
        # the derived posix username for the SSH client.
        return [("enable-oslogin", "TRUE")]

    if not ssh_user:
        raise RuntimeError(
            "GCP metadata SSH-key auth needs a username: set 'ssh_user' on the "
            "node type or 'auth.ssh_user' globally."
        )
    pubkey = _read_public_key(cfg, default_private_key)
    return [
        ("ssh-keys", f"{ssh_user}:{pubkey}"),
        ("enable-oslogin", "FALSE"),
    ]


# ---------------------------------------------------------------------------
# Firewall (the GCP analog of the SSH security group)
# ---------------------------------------------------------------------------

def _subnet_cidrs(project: str, network: str | None) -> list[str]:
    """Return the IPv4 CIDR ranges of all subnets in *network* (intra-VPC source)."""
    cv = _compute()
    client = cv.SubnetworksClient()
    net_url = _network_url(network)
    net_name = net_url.rsplit("/", 1)[-1]
    cidrs: list[str] = []
    for _scope, scoped in client.aggregated_list(
        request=cv.AggregatedListSubnetworksRequest(project=project)
    ):
        for sn in (scoped.subnetworks or []):
            if sn.network.rsplit("/", 1)[-1] == net_name and sn.ip_cidr_range:
                cidrs.append(sn.ip_cidr_range)
    return cidrs


def _ensure_firewall_rule(
    client,
    project: str,
    name: str,
    network: str,
    allowed,                       # list[compute_v1.Allowed]
    source_ranges: list[str],
    target_tags: list[str],
    description: str,
) -> None:
    """Idempotently create-or-update a firewall rule to the desired spec."""
    cv = _compute()
    fw = cv.Firewall(
        name=name,
        network=network,
        direction="INGRESS",
        allowed=allowed,
        source_ranges=source_ranges,
        target_tags=target_tags,
        description=description,
    )
    from google.api_core.exceptions import NotFound
    try:
        client.get(project=project, firewall=name)
        op = client.patch(project=project, firewall=name, firewall_resource=fw)
        _wait_global_op(op)
        logger.info(f"Updated firewall rule {name} (sources={source_ranges})")
    except NotFound:
        op = client.insert(project=project, firewall_resource=fw)
        _wait_global_op(op)
        logger.info(f"Created firewall rule {name} (sources={source_ranges})")


def ensure_ssh_firewall(
    cluster_name: str,
    project: str,
    network: str | None = None,
    allow_intra_vpc: bool = True,
) -> str:
    """Create/update the cluster's firewall rules and return its network tag.

    Mirrors :func:`chia.cluster.aws_nodes.ensure_ssh_security_group`:

    * Ingress SSH (22) is locked to the head's public IP
      (:func:`_resolve_head_ssh_cidrs`, reused from ``aws_nodes``), targeting
      only this cluster's instances via the network tag.
    * When *allow_intra_vpc* is True (overridable via ``CHIA_ALLOW_INTRA_VPC``),
      an all-traffic rule from the network's own subnet CIDRs is added.

    Note: GCP's default network ships a broad ``default-allow-ssh`` (tcp:22 from
    ``0.0.0.0/0``, all targets) that our targeted rule does not remove.  Set
    ``CHIA_GCP_LOCKDOWN_DEFAULT_SSH=1`` to delete that world-open rule (the
    analog of the AWS 0.0.0.0/0 revoke); otherwise we log a warning.
    """
    cv = _compute()
    client = cv.FirewallsClient()
    net_url = _network_url(network)
    tag = _network_tag(cluster_name)
    base = _sanitize_name(f"chia-{cluster_name}")

    desired_cidrs = _resolve_head_ssh_cidrs()
    _ensure_firewall_rule(
        client, project, f"{base}-ssh", net_url,
        allowed=[cv.Allowed(I_p_protocol="tcp", ports=["22"])],
        source_ranges=[c for c, _ in desired_cidrs],
        target_tags=[tag],
        description=f"chia SSH access for cluster {cluster_name}",
    )

    if _intra_vpc_enabled(allow_intra_vpc):
        cidrs = _subnet_cidrs(project, network)
        if cidrs:
            _ensure_firewall_rule(
                client, project, f"{base}-internal", net_url,
                allowed=[
                    cv.Allowed(I_p_protocol="tcp", ports=["0-65535"]),
                    cv.Allowed(I_p_protocol="udp", ports=["0-65535"]),
                    cv.Allowed(I_p_protocol="icmp"),
                ],
                source_ranges=cidrs,
                target_tags=[tag],
                description=f"chia intra-VPC access for cluster {cluster_name}",
            )
        else:
            logger.warning(
                f"No subnet CIDRs found for network {net_url}; "
                "skipping intra-VPC firewall rule"
            )

    _maybe_lockdown_default_ssh(client, project)
    return tag


def _maybe_lockdown_default_ssh(client, project: str) -> None:
    """Delete the default network's world-open ``default-allow-ssh`` rule.

    Gated by ``CHIA_GCP_LOCKDOWN_DEFAULT_SSH`` (the analog of revoking the AWS
    0.0.0.0/0 SSH rule).  When unset, only warns if such a rule is present.
    """
    from google.api_core.exceptions import NotFound
    try:
        rule = client.get(project=project, firewall="default-allow-ssh")
    except NotFound:
        return

    if "0.0.0.0/0" not in list(rule.source_ranges):
        return

    enabled = os.environ.get("CHIA_GCP_LOCKDOWN_DEFAULT_SSH", "").strip().lower()
    if enabled in ("1", "true", "yes", "on"):
        op = client.delete(project=project, firewall="default-allow-ssh")
        _wait_global_op(op)
        logger.info("Deleted world-open default-allow-ssh firewall rule")
    else:
        logger.warning(
            "Network has a world-open 'default-allow-ssh' rule (tcp:22 from "
            "0.0.0.0/0, all instances). chia's targeted rule does not remove it. "
            "Set CHIA_GCP_LOCKDOWN_DEFAULT_SSH=1 to delete it."
        )


def cleanup_firewall(cluster_name: str, project: str) -> None:
    """Delete the cluster's firewall rules if they exist (idempotent)."""
    cv = _compute()
    client = cv.FirewallsClient()
    base = _sanitize_name(f"chia-{cluster_name}")
    from google.api_core.exceptions import NotFound
    for name in (f"{base}-ssh", f"{base}-internal"):
        try:
            op = client.delete(project=project, firewall=name)
            _wait_global_op(op)
            logger.info(f"Deleted firewall rule {name}")
        except NotFound:
            continue
        except Exception as exc:
            logger.warning(f"Could not delete firewall rule {name}: {exc}")


# ---------------------------------------------------------------------------
# Operation waiters
# ---------------------------------------------------------------------------

def _wait_op(op) -> None:
    """Block on a zonal/regional extended operation."""
    op.result(timeout=_OP_TIMEOUT)


def _wait_global_op(op) -> None:
    """Block on a global extended operation (firewall ops)."""
    op.result(timeout=_OP_TIMEOUT)


# ---------------------------------------------------------------------------
# Instance construction
# ---------------------------------------------------------------------------

def _build_instance(
    cluster_name: str,
    name: str,
    cfg: GCPNodeConfig,
    index: int,
    zone: str,
    network: str | None,
    subnetwork: str | None,
    ssh_user: str | None,
    default_private_key: str | None,
):
    """Build a fully-specified ``compute_v1.Instance`` for one node."""
    cv = _compute()

    instance_name = _sanitize_name(f"chia-{cluster_name}-{name}-{index}")

    disk = cv.AttachedDisk(
        boot=True,
        auto_delete=True,
        initialize_params=cv.AttachedDiskInitializeParams(source_image=cfg.image),
    )
    if cfg.disk_size_gb:
        disk.initialize_params.disk_size_gb = cfg.disk_size_gb

    nic = cv.NetworkInterface(network=_network_url(network))
    if subnetwork:
        nic.subnetwork = subnetwork
    nic.access_configs = [
        cv.AccessConfig(name="External NAT", type_="ONE_TO_ONE_NAT"),
    ]

    metadata_items = _gcp_ssh_metadata(cfg, ssh_user, default_private_key)

    instance = cv.Instance(
        name=instance_name,
        machine_type=_machine_type_url(zone, cfg.machine_type),
        disks=[disk],
        network_interfaces=[nic],
        labels={
            "chia-cluster": _sanitize_label(cluster_name),
            "chia-node-type": _sanitize_label(name),
            "chia-node-index": str(index),
        },
        tags=cv.Tags(items=[_network_tag(cluster_name)]),
        metadata=cv.Metadata(
            items=[cv.Items(key=k, value=v) for k, v in metadata_items]
        ),
    )

    if cfg.spot:
        instance.scheduling = cv.Scheduling(
            provisioning_model="SPOT",
            instance_termination_action="DELETE",
            automatic_restart=False,
        )

    if cfg.extra_args:
        # compute_v1 takes a structured Instance proto (unlike boto3's flat
        # kwargs), so deep-merge user extra_args (snake_case Instance fields)
        # onto the built instance via a dict round-trip.
        base = cv.Instance.to_dict(instance, preserving_proto_field_name=True)
        merged = _deep_merge(base, cfg.extra_args)
        instance = cv.Instance(merged)

    return instance


# ---------------------------------------------------------------------------
# Provisioning
# ---------------------------------------------------------------------------

def provision_gcp_nodes(
    cluster_name: str,
    gcp_nodes: dict[str, GCPNodeConfig],
    project: str,
    zone: str,
    network: str | None = None,
    subnetwork: str | None = None,
    default_ssh_user: str | None = None,
    default_ssh_private_key: str | None = None,
) -> dict[str, list[str]]:
    """Launch Compute Engine instances for each node type.

    Returns ``{node_name: [external_ip_0, external_ip_1, ...]}``, ordered by
    index.  Instances are labeled for discovery by :func:`discover_gcp_nodes`.
    """
    cv = _compute()
    client = cv.InstancesClient()
    ensure_ssh_firewall(cluster_name, project, network=network)

    # node_name -> [(index, zone, instance_name)]
    entries_by_node: dict[str, list[tuple[int, str, str]]] = {}
    launched: list[tuple[str, str]] = []  # (zone, name) for rollback
    pending: list[tuple[Any, str, str, int, str]] = []  # (op, node, zone, idx, inst_name)

    try:
        for name, cfg in gcp_nodes.items():
            z = cfg.zone or zone
            ssh_user = cfg.ssh_user or default_ssh_user
            logger.info(
                f"Launching {cfg.count}x {cfg.machine_type} "
                f"for node type '{name}' in zone {z}"
            )
            entries_by_node[name] = []
            for i in range(cfg.count):
                inst = _build_instance(
                    cluster_name, name, cfg, i, z, network, subnetwork,
                    ssh_user, default_ssh_private_key,
                )
                op = client.insert(project=project, zone=z, instance_resource=inst)
                pending.append((op, name, z, i, inst.name))
                launched.append((z, inst.name))
                logger.info(f"  Insert issued for {inst.name} (index {i})")

        # Wait for all inserts to complete.
        for op, name, z, i, inst_name in pending:
            _wait_op(op)
            entries_by_node[name].append((i, z, inst_name))

    except Exception:
        if launched:
            logger.error("Provisioning failed — terminating launched instances")
            _delete_instances(client, project, launched)
        raise

    return _collect_ips(client, project, entries_by_node)


def provision_missing_gcp_nodes(
    cluster_name: str,
    gcp_nodes: dict[str, GCPNodeConfig],
    project: str,
    zone: str,
    network: str | None = None,
    subnetwork: str | None = None,
    default_ssh_user: str | None = None,
    default_ssh_private_key: str | None = None,
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Launch only the instances missing to reach each node type's count.

    Mirrors :func:`chia.cluster.aws_nodes.provision_missing_aws_nodes`.
    Returns ``(full_ip_map, new_ip_map)``.
    """
    cv = _compute()
    client = cv.InstancesClient()
    existing = _discover_gcp_nodes_with_indexes(cluster_name, project)

    indexes_to_launch: dict[str, list[int]] = {}
    for name, cfg in gcp_nodes.items():
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

    new_entries_by_node: dict[str, list[tuple[int, str, str]]] = {}
    if indexes_to_launch:
        ensure_ssh_firewall(cluster_name, project, network=network)
        launched: list[tuple[str, str]] = []
        pending: list[tuple[Any, str, str, int, str]] = []
        try:
            for name, indexes in indexes_to_launch.items():
                cfg = gcp_nodes[name]
                z = cfg.zone or zone
                ssh_user = cfg.ssh_user or default_ssh_user
                logger.info(
                    f"Launching {len(indexes)}x {cfg.machine_type} for node "
                    f"type '{name}' (indexes {indexes}) in zone {z}"
                )
                new_entries_by_node[name] = []
                for i in indexes:
                    inst = _build_instance(
                        cluster_name, name, cfg, i, z, network, subnetwork,
                        ssh_user, default_ssh_private_key,
                    )
                    op = client.insert(project=project, zone=z, instance_resource=inst)
                    pending.append((op, name, z, i, inst.name))
                    launched.append((z, inst.name))
                    logger.info(f"  Insert issued for {inst.name} (index {i})")
            for op, name, z, i, inst_name in pending:
                _wait_op(op)
                new_entries_by_node[name].append((i, z, inst_name))
        except Exception:
            if launched:
                logger.error("Provisioning failed — terminating launched instances")
                _delete_instances(client, project, launched)
            raise

    new_ip_map = (
        _collect_ips(client, project, new_entries_by_node)
        if new_entries_by_node else {}
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


def _external_ip(instance) -> str | None:
    for nic in (instance.network_interfaces or []):
        for ac in (nic.access_configs or []):
            if ac.nat_i_p:
                return ac.nat_i_p
    return None


def _collect_ips(
    client,
    project: str,
    entries_by_node: dict[str, list[tuple[int, str, str]]],
) -> dict[str, list[str]]:
    """Fetch each instance and return ``{node_name: [external_ip, ...]}``."""
    ip_map: dict[str, list[str]] = {}
    for name, entries in entries_by_node.items():
        ips: list[str] = []
        for (_idx, zone, inst_name) in sorted(entries, key=lambda e: e[0]):
            inst = client.get(project=project, zone=zone, instance=inst_name)
            ip = _external_ip(inst)
            if not ip:
                raise RuntimeError(
                    f"Instance {inst_name} has no external IP. Ensure the node "
                    "has an access config (ONE_TO_ONE_NAT)."
                )
            ips.append(ip)
            logger.info(f"  {inst_name}: external_ip={ip}")
        ip_map[name] = ips
    return ip_map


# ---------------------------------------------------------------------------
# Discovery (for chia down / --add)
# ---------------------------------------------------------------------------

def _discover_gcp_nodes_with_indexes(
    cluster_name: str,
    project: str,
) -> dict[str, list[tuple[int, str]]]:
    """Find running instances and return ``{name: [(index, ip), ...]}``."""
    cv = _compute()
    client = cv.InstancesClient()
    label = _sanitize_label(cluster_name)

    nodes: dict[str, list[tuple[int, str]]] = defaultdict(list)
    req = cv.AggregatedListInstancesRequest(
        project=project, filter=f"labels.chia-cluster={label}"
    )
    for _scope, scoped in client.aggregated_list(request=req):
        for inst in (scoped.instances or []):
            if inst.status != "RUNNING":
                continue
            labels = dict(inst.labels)
            node_type = labels.get("chia-node-type")
            node_index = labels.get("chia-node-index")
            ip = _external_ip(inst)
            if node_type and node_index is not None and ip:
                nodes[node_type].append((int(node_index), ip))

    return {name: sorted(entries, key=lambda e: e[0]) for name, entries in nodes.items()}


def discover_gcp_nodes(
    cluster_name: str,
    project: str,
) -> dict[str, list[str]]:
    """Find running instances for this cluster and return their external IPs.

    Returns ``{node_name: [external_ip_0, ...]}``, same format as
    :func:`provision_gcp_nodes`.

    Caveat: ``{node_name}`` is recovered from the ``chia-node-type`` *label*, so
    node-type names must be valid label values (enforced in ``parse_gcp_nodes``).
    """
    entries_by_node = _discover_gcp_nodes_with_indexes(cluster_name, project)
    ip_map: dict[str, list[str]] = {
        name: [ip for _, ip in entries] for name, entries in entries_by_node.items()
    }

    if ip_map:
        logger.info(f"Discovered GCP nodes for cluster '{cluster_name}': "
                     f"{', '.join(f'{k}({len(v)})' for k, v in ip_map.items())}")
    else:
        logger.info(f"No running GCP nodes found for cluster '{cluster_name}'")

    return ip_map


# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------

def _delete_instances(client, project: str, targets: list[tuple[str, str]]) -> None:
    """Delete instances given ``[(zone, name), ...]`` and wait for completion."""
    if not targets:
        return
    ops = []
    for zone, name in targets:
        ops.append(client.delete(project=project, zone=zone, instance=name))
        logger.info(f"Delete issued for {name} (zone {zone})")
    for op in ops:
        try:
            _wait_op(op)
        except Exception as exc:
            logger.warning(f"Error waiting for instance delete: {exc}")


def teardown_gcp_nodes(
    cluster_name: str,
    project: str,
) -> list[str]:
    """Delete all Compute Engine instances labeled with this cluster.

    Returns the list of deleted instance names.  Idempotent.
    """
    cv = _compute()
    client = cv.InstancesClient()
    label = _sanitize_label(cluster_name)

    targets: list[tuple[str, str]] = []
    names: list[str] = []
    req = cv.AggregatedListInstancesRequest(
        project=project, filter=f"labels.chia-cluster={label}"
    )
    for scope, scoped in client.aggregated_list(request=req):
        for inst in (scoped.instances or []):
            zone = scope.split("/")[-1] if "/" in scope else scope.replace("zones/", "")
            targets.append((zone, inst.name))
            names.append(inst.name)

    if not targets:
        logger.info(f"No GCP instances to terminate for cluster '{cluster_name}'")
        return []

    logger.info(f"Deleting {len(targets)} instance(s) for cluster '{cluster_name}'")
    _delete_instances(client, project, targets)
    return names
