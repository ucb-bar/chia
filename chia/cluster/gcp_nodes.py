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
  target instances by network *tag*; see :func:`ensure_firewall`.
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
    _resolve_head_ssh_cidrs,
    run_aws_setup as run_gcp_setup,  # provider-agnostic; re-exported for symmetry
)

logger = get_logger("gcp_nodes")

# Public re-export so callers can use a GCP-named symbol (see module docstring).
__all__ = [
    "GCPNodeConfig", "DEFAULT_IMAGE", "DEFAULT_ZONE",
    "provision_gcp_nodes", "provision_missing_gcp_nodes",
    "discover_gcp_nodes", "teardown_gcp_nodes",
    "ensure_firewall", "cleanup_firewall", "check_names", "run_gcp_setup",
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

    GCP instance names, firewall rule names and network tags must match
    ``[a-z]([-a-z0-9]*[a-z0-9])?`` and be <= 63 chars (no underscores). A longer
    name raises instead of being cut, because a cut name could clash with another
    cluster's.
    """
    out = re.sub(r"[^a-z0-9-]", "-", s.lower()).strip("-")
    if not out or not out[0].isalpha():
        out = "c" + out
    if len(out) > 63:
        raise ValueError(f"the GCP name {out!r} is longer than 63 characters; "
                         f"use a shorter cluster_name or node type name")
    return out


def _sanitize_label(s: str) -> str:
    """Sanitize *s* into a valid GCP label value (lowercase ``[a-z0-9_-]``, <=63)."""
    return re.sub(r"[^a-z0-9_-]", "-", s.lower())[:63]


def _network_tag(cluster_name: str) -> str:
    """Network tag of a cluster's workers (firewall target)."""
    return _sanitize_name(f"chia-{cluster_name}")


def _head_tag(cluster_name: str) -> str:
    """Network tag that ``connection: vpc`` adds to the cluster's head VM."""
    return _sanitize_name(f"chia-{cluster_name}-head")


def _instance_name(cluster_name: str, name: str, index: int) -> str:
    return _sanitize_name(f"chia-{cluster_name}-{name}-{index}")


# The cluster's firewall rules, by the suffix of their names (see ensure_firewall).
_RULES = ("members", "ssh", "deny")


def _rule_name(cluster_name: str, rule: str) -> str:
    return _sanitize_name(f"chia-{cluster_name}-{rule}")


def check_names(cluster_name: str, gcp_nodes: dict[str, GCPNodeConfig]) -> None:
    """Raise ValueError if a network tag, firewall rule or instance name of the
    cluster would be longer than GCP allows."""
    for rule in _RULES:
        _rule_name(cluster_name, rule)
    _head_tag(cluster_name)
    for name, cfg in gcp_nodes.items():
        _instance_name(cluster_name, name, max(cfg.count - 1, 0))


def _network_url(network: str | None) -> str:
    """Normalize a network name/path to a partial URL compute_v1 accepts."""
    net = network or DEFAULT_NETWORK
    if "/" in net:
        return net
    return f"global/networks/{net}"


def _network_path(network: str | None, project: str) -> str:
    """*network* (a name, a path or a URL) as ``projects/<project>/global/networks/<name>``."""
    net = network or DEFAULT_NETWORK
    if "projects/" in net:
        return net[net.index("projects/"):]
    return f"projects/{project}/global/networks/{net.rsplit('/', 1)[-1]}"


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
# Firewall (the GCP analog of the cluster's security group)
# ---------------------------------------------------------------------------

def _ensure_firewall_rule(client, project: str, fw) -> None:
    """Create the firewall rule *fw*, or update the rule of that name to match it."""
    from google.api_core.exceptions import NotFound
    try:
        client.get(project=project, firewall=fw.name)
        _wait_global_op(client.patch(project=project, firewall=fw.name, firewall_resource=fw))
        logger.info(f"Updated firewall rule {fw.name}")
    except NotFound:
        _wait_global_op(client.insert(project=project, firewall_resource=fw))
        logger.info(f"Created firewall rule {fw.name}")


def ensure_firewall(cluster_name: str, project: str, network: str, ssh_from_head: bool) -> None:
    """Create or update the cluster's firewall rules on *network*.

    * ``members``: all traffic between the cluster's workers (tag ``chia-<cluster>``)
      and its head (tag ``chia-<cluster>-head``, which ``connection: vpc`` adds to the
      head VM).
    * ``ssh``, with *ssh_from_head*: tcp 22 to the workers from the head's public IP,
      for a head that reaches them over the internet (``tunnel``, ``ts_user``).
    * ``deny``: all other incoming traffic to the workers. It also overrides the open
      rules of GCP's ``default`` network, such as ``default-allow-internal``.

    GCP applies the matching rule with the lowest priority number: the members and ssh
    rules (1000) win over the deny rule (65000), which wins over the ``default``
    network's rules (65534).
    """
    cv = _compute()
    workers, head = _network_tag(cluster_name), _head_tag(cluster_name)
    rules = [cv.Firewall(
        name=_rule_name(cluster_name, "members"), network=network, direction="INGRESS",
        priority=1000, allowed=[cv.Allowed(I_p_protocol="all")],
        source_tags=[workers, head], target_tags=[workers, head],
        description=f"chia: all traffic between the machines of cluster {cluster_name}",
    )]
    if ssh_from_head:
        rules.append(cv.Firewall(
            name=_rule_name(cluster_name, "ssh"), network=network, direction="INGRESS",
            priority=1000, allowed=[cv.Allowed(I_p_protocol="tcp", ports=["22"])],
            source_ranges=[c for c, _ in _resolve_head_ssh_cidrs()], target_tags=[workers],
            description=f"chia: SSH from the head to the workers of cluster {cluster_name}",
        ))
    rules.append(cv.Firewall(
        name=_rule_name(cluster_name, "deny"), network=network, direction="INGRESS",
        priority=65000, denied=[cv.Denied(I_p_protocol="all")],
        source_ranges=["0.0.0.0/0"], target_tags=[workers],
        description=f"chia: no other incoming traffic to the workers of cluster {cluster_name}",
    ))
    client = cv.FirewallsClient()
    for fw in rules:
        _ensure_firewall_rule(client, project, fw)


def cleanup_firewall(cluster_name: str, project: str) -> None:
    """Delete the cluster's firewall rules, and remove its tag from the head VM (idempotent)."""
    cv = _compute()
    client = cv.FirewallsClient()
    from google.api_core.exceptions import NotFound
    for rule in _RULES:
        name = _rule_name(cluster_name, rule)
        try:
            _wait_global_op(client.delete(project=project, firewall=name))
            logger.info(f"Deleted firewall rule {name}")
        except NotFound:
            continue
        except Exception as exc:
            logger.warning(f"Could not delete firewall rule {name}: {exc}")
    tag = _head_tag(cluster_name)
    req = cv.AggregatedListInstancesRequest(project=project)
    for scope, scoped in cv.InstancesClient().aggregated_list(request=req):
        for inst in (scoped.instances or []):
            if tag in inst.tags.items:
                _set_tag(project, scope.rsplit("/", 1)[-1], inst, tag, add=False)


# ---------------------------------------------------------------------------
# The cluster's network (the head's, with connection "vpc")
# ---------------------------------------------------------------------------

def _find_head(project: str, head_ip: str, network: str | None):
    """``(zone, instance, network interface)`` of the head with ``connection: vpc``: the
    VM in *project* whose internal IP is *head_ip*, in *network* if it is set."""
    cv = _compute()
    matches = []
    req = cv.AggregatedListInstancesRequest(project=project)
    for scope, scoped in cv.InstancesClient().aggregated_list(request=req):
        for inst in (scoped.instances or []):
            for nic in inst.network_interfaces:
                if nic.network_i_p == head_ip:
                    matches.append((scope.rsplit("/", 1)[-1], inst, nic))
    if network:
        in_network = [m for m in matches if _network_path(m[2].network, project) == network]
        if matches and not in_network:
            _, inst, nic = matches[0]
            ips = [n.network_i_p for n in inst.network_interfaces
                   if _network_path(n.network, project) == network]
            raise RuntimeError(
                f"gcp_nodes.network {network} is not the network of the head's IP {head_ip} "
                f"({_network_path(nic.network, project)}); with connection: vpc the workers go "
                f"in the network of provider.head_ip"
                + (f", so set provider.head_ip to the head's IP in {network}, {ips[0]}" if ips else ""))
        matches = in_network
    if not matches:
        raise RuntimeError(
            f"connection: vpc, the default, needs the head on a GCP VM in project {project}, "
            f"and provider.head_ip ({head_ip}) set to its internal IP; for a head outside GCP, "
            f"set connection: tunnel"
        )
    if len(matches) > 1:
        raise RuntimeError(
            f"{len(matches)} VMs in project {project} have the internal IP {head_ip}; set "
            f"gcp_nodes.network to the head's network")
    return matches[0]


def _set_tag(project: str, zone: str, inst, tag: str, add: bool) -> None:
    """Add the network tag *tag* to the VM *inst*, or remove it; keep its other tags."""
    items = list(inst.tags.items)
    if (tag in items) == add:
        return
    items = items + [tag] if add else [t for t in items if t != tag]
    cv = _compute()
    _wait_op(cv.InstancesClient().set_tags(
        project=project, zone=zone, instance=inst.name,
        tags_resource=cv.Tags(items=items, fingerprint=inst.tags.fingerprint)))
    logger.info(f"{'Added' if add else 'Removed'} network tag {tag} on {inst.name}")


def _check_internet_route(project: str, network: str) -> None:
    """Raise if *network* has no route for 0.0.0.0/0, without which its machines cannot
    download images or packages."""
    cv = _compute()
    for route in cv.RoutesClient().list(project=project):
        if route.dest_range == "0.0.0.0/0" and _network_path(route.network, project) == network:
            return
    raise RuntimeError(
        f"network {network} has no route to the internet (0.0.0.0/0), so its machines "
        f"cannot download images or packages; add one, or use another network"
    )


def _pick_subnets(project: str, network: str, zones: set[str], subnetwork: str | None,
                  head_subnet: str | None) -> dict[str, str]:
    """The subnet for each zone's machines: *subnetwork* if set, else the head's subnet
    in the head's region, else the network's only subnet in the zone's region."""
    if subnetwork:
        return dict.fromkeys(zones, subnetwork)
    cv = _compute()
    client = cv.SubnetworksClient()
    picked: dict[str, str] = {}
    for z in sorted(zones):
        region = _region_of(z)
        if head_subnet and f"/regions/{region}/" in head_subnet:
            picked[z] = head_subnet
            continue
        # Only subnets that can hold VMs: not proxy-only, Private Service Connect, etc.
        subnets = [s for s in client.list(project=project, region=region)
                   if _network_path(s.network, project) == network
                   and s.purpose in ("", "PRIVATE", "PRIVATE_RFC_1918")]
        if not subnets:
            raise RuntimeError(
                f"network {network} has no subnet in region {region}, the region of "
                f"zone {z}; use a zone in a region where it has one")
        if len(subnets) > 1:
            raise RuntimeError(
                f"network {network} has {len(subnets)} subnets in region {region} "
                f"({', '.join(s.name for s in subnets)}); set gcp_nodes.subnetwork to one of them")
        picked[z] = subnets[0].self_link
    return picked


def _warn_open_head(project: str, network: str, head) -> None:
    """Warn about rules that let IP ranges reach every port of the head: Ray's ports on
    the head are then open to them, because the cluster's rules protect only its workers."""
    cv = _compute()
    tags = set(head.tags.items)
    accounts = {sa.email for sa in head.service_accounts}
    for fw in cv.FirewallsClient().list(project=project):
        if (fw.disabled or fw.direction != "INGRESS" or not fw.source_ranges
                or _network_path(fw.network, project) != network):
            continue
        targets_head = ((not fw.target_tags and not fw.target_service_accounts)
                        or tags & set(fw.target_tags)
                        or accounts & set(fw.target_service_accounts))
        # GCP's own rules write "every port" as 0-65535, others leave the ports out.
        if targets_head and any(a.I_p_protocol == "all"
                                or (a.I_p_protocol in ("tcp", "udp")
                                    and (not a.ports or "0-65535" in a.ports))
                                for a in fw.allowed):
            logger.warning(f"Firewall rule {fw.name} lets {', '.join(fw.source_ranges)} "
                           f"reach every port of the head, Ray's ports included")


def _prepare_network(cluster_name: str, gcp_nodes: dict[str, GCPNodeConfig], project: str,
                     zone: str, network: str | None, subnetwork: str | None,
                     vpc_head_ip: str | None) -> tuple[str, dict[str, str]]:
    """Check the cluster's network and create its firewall rules. Return the network and
    the subnet for each zone of the cluster's machines.

    With *vpc_head_ip* (``connection: vpc``), the network is the head's, and the head VM
    gets the cluster's head tag.
    """
    head_subnet = None
    if vpc_head_ip:
        head_zone, head, nic = _find_head(project, vpc_head_ip,
                                          _network_path(network, project) if network else None)
        network, head_subnet = _network_path(nic.network, project), nic.subnetwork
        if not network.startswith(f"projects/{project}/"):
            raise RuntimeError(
                f"the head's network {network} belongs to another project (a Shared VPC), "
                f"which connection: vpc does not support; set connection: tunnel")
    else:
        network = _network_path(network, project)
    _check_internet_route(project, network)
    subnets = _pick_subnets(project, network, {cfg.zone or zone for cfg in gcp_nodes.values()},
                            subnetwork, head_subnet)
    ensure_firewall(cluster_name, project, network, ssh_from_head=vpc_head_ip is None)
    if vpc_head_ip:
        _set_tag(project, head_zone, head, _head_tag(cluster_name), add=True)
        _warn_open_head(project, network, head)
    return network, subnets


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

    instance_name = _instance_name(cluster_name, name, index)

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
    vpc_head_ip: str | None = None,
) -> dict[str, list[str]]:
    """Launch Compute Engine instances for each node type.

    Returns ``{node_name: [ip_0, ip_1, ...]}``, ordered by index: external IPs, or
    internal IPs with *vpc_head_ip* (``connection: vpc``: the head's internal IP, and
    the instances go in the head's network).  Instances are labeled for discovery
    by :func:`discover_gcp_nodes`.
    """
    cv = _compute()
    client = cv.InstancesClient()
    network, subnets = _prepare_network(cluster_name, gcp_nodes, project, zone,
                                        network, subnetwork, vpc_head_ip)

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
                    cluster_name, name, cfg, i, z, network, subnets[z],
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

    return _collect_ips(client, project, entries_by_node, private=vpc_head_ip is not None)


def provision_missing_gcp_nodes(
    cluster_name: str,
    gcp_nodes: dict[str, GCPNodeConfig],
    project: str,
    zone: str,
    network: str | None = None,
    subnetwork: str | None = None,
    default_ssh_user: str | None = None,
    default_ssh_private_key: str | None = None,
    vpc_head_ip: str | None = None,
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Launch only the instances missing to reach each node type's count.

    Mirrors :func:`chia.cluster.aws_nodes.provision_missing_aws_nodes`.
    Returns ``(full_ip_map, new_ip_map)``, with internal IPs with *vpc_head_ip*
    (see :func:`provision_gcp_nodes`).
    """
    cv = _compute()
    client = cv.InstancesClient()
    private = vpc_head_ip is not None
    existing = _discover_gcp_nodes_with_indexes(cluster_name, project, private)

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
        network, subnets = _prepare_network(cluster_name, gcp_nodes, project, zone,
                                            network, subnetwork, vpc_head_ip)
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
                        cluster_name, name, cfg, i, z, network, subnets[z],
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
        _collect_ips(client, project, new_entries_by_node, private)
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


def _ip(instance, private: bool) -> str | None:
    """The instance's internal IP if *private*, else its external IP."""
    for nic in (instance.network_interfaces or []):
        if private:
            return nic.network_i_p or None
        for ac in (nic.access_configs or []):
            if ac.nat_i_p:
                return ac.nat_i_p
    return None


def _collect_ips(
    client,
    project: str,
    entries_by_node: dict[str, list[tuple[int, str, str]]],
    private: bool,
) -> dict[str, list[str]]:
    """Fetch each instance and return ``{node_name: [ip, ...]}``: internal IPs if
    *private*, else external IPs."""
    kind = "internal" if private else "external"
    ip_map: dict[str, list[str]] = {}
    for name, entries in entries_by_node.items():
        ips: list[str] = []
        for (_idx, zone, inst_name) in sorted(entries, key=lambda e: e[0]):
            inst = client.get(project=project, zone=zone, instance=inst_name)
            ip = _ip(inst, private)
            if not ip:
                raise RuntimeError(
                    f"Instance {inst_name} has no {kind} IP." + ("" if private else
                    " Ensure the node has an access config (ONE_TO_ONE_NAT).")
                )
            ips.append(ip)
            logger.info(f"  {inst_name}: {kind}_ip={ip}")
        ip_map[name] = ips
    return ip_map


# ---------------------------------------------------------------------------
# Discovery (for chia down / --add)
# ---------------------------------------------------------------------------

def _discover_gcp_nodes_with_indexes(
    cluster_name: str,
    project: str,
    private: bool = False,
) -> dict[str, list[tuple[int, str]]]:
    """Find running instances and return ``{name: [(index, ip), ...]}``, with
    internal IPs if *private*, else external IPs."""
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
            ip = _ip(inst, private)
            if node_type and node_index is not None and ip:
                nodes[node_type].append((int(node_index), ip))

    return {name: sorted(entries, key=lambda e: e[0]) for name, entries in nodes.items()}


def discover_gcp_nodes(
    cluster_name: str,
    project: str,
    private: bool = False,
) -> dict[str, list[str]]:
    """Find running instances for this cluster and return their IPs: internal IPs
    if *private* (``connection: vpc``), else external IPs.

    Returns ``{node_name: [ip_0, ...]}``, same format as
    :func:`provision_gcp_nodes`.

    Caveat: ``{node_name}`` is recovered from the ``chia-node-type`` *label*, so
    node-type names must be valid label values (enforced in ``parse_gcp_nodes``).
    """
    entries_by_node = _discover_gcp_nodes_with_indexes(cluster_name, project, private)
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
