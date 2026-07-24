from __future__ import annotations

import ipaddress
import os
import re
import yaml
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any

from chia.cluster.log import get_logger

logger = get_logger("config")


class ConfigError(Exception):
    pass


@dataclass
class TunnelConfig:
    """Tunnel and port-pinning config for a single tunneled IP.

    Loopback IP must NOT be 127.0.0.1 — Ray's resolve_ip_for_localhost()
    rewrites that to the real IP.  127.0.0.2 (any 127.x.x.x except .0.0.1) works.

    Leave ``tunnel_ip`` empty (the default) for automatic assignment.
    Each tunneled worker gets a unique ``127.0.0.N`` starting at N=2.
    """
    tunnel_ip: str = "" # set by chia, not exposed to user

    # used-exposed global configs
    gcs_tunnel_port: int = 16379
    kill_orphaned_tunnels: bool = True
    ray_node_manager_port: int = 16800
    ray_object_manager_port: int = 16801
    ray_worker_port_min: int = 30000
    ray_worker_port_max: int = 30024
    tool_port_min: int = 18000
    tool_port_max: int = 18010
    head_tool_port_min: int = 8000
    head_tool_port_max: int = 8010
    head_node_manager_port: int = 29800
    head_object_manager_port: int = 29801
    head_worker_port_min: int = 40000
    head_worker_port_max: int = 40099
    pre_tunnel_commands: list[str] = field(default_factory=lambda: [
        "sudo sed -i 's/^#\\?GatewayPorts.*/GatewayPorts clientspecified/' /etc/ssh/sshd_config"
        " && sudo mkdir -p /etc/systemd/system/ssh.service.d"
        " && echo -e '[Service]\\nLimitNOFILE=65536' | sudo tee /etc/systemd/system/ssh.service.d/limits.conf > /dev/null"
        " && sudo systemctl daemon-reload"
        " && (sudo systemctl restart sshd 2>/dev/null || sudo systemctl restart ssh)",
    ])


@dataclass
class TailnetConfig:
    """Cluster-wide config for tailnet (tailscale) native clusters.

    All cross-machine Ray traffic flows over the tailscale network
    instead of SSH tunnels. Works with fully unprivileged
    (userspace-networking) tailscaled on every machine. Each machine
    runs a small relay process; the head and every logical worker
    advertise a unique loopback IP to Ray (bindable even in userspace
    mode, unlike the real tailnet IP).

    Outbound: Ray's gRPC is pointed at the relay's per-machine HTTP
    CONNECT proxy (via grpc_proxy); the proxy reads the destination from
    each CONNECT request and forwards it through the local tailscaled
    SOCKS5 proxy to the owning machine's tailnet IP. Because there are no
    per-port outbound listeners, port blocks need only be unique PER
    MACHINE — two workers on different machines reuse the same ports.
    ChiaTool traffic is plain HTTP (can't use grpc_proxy), so peer tool
    ports keep small per-port SOCKS listeners.

    Inbound: tailscaled delivers to 127.0.0.1:<port>, where Ray's
    wildcard-bound services receive it directly (tool servers bind the
    advertise IP, so a local bridge forwards their inbound).

    ``worker_port_count`` (the per-worker Ray worker port range) and the
    head's worker port range must exceed the machine's CPU count — Ray
    prestarts one worker process per CPU and each needs a port.
    """
    # The head's tailscale 100.x address. Required — unless
    # ``manage_all`` is set, in which case CHIA discovers it at bring-up.
    head_tailnet_ip: str = ""
    socks_proxy: str = "127.0.0.1:1055"   # tailscaled --socks5-server on every machine
    connect_proxy_port: int = 13129   # relay's HTTP CONNECT listener (Ray's grpc_proxy)
    # Auth key (tskey-auth-...) used when CHIA joins machines to the
    # tailnet itself (cloud machines by default, on-prem via
    # manage_tailscale). Use a reusable — ideally ephemeral,
    # pre-authorized — key; reference it as ${TS_AUTHKEY} to keep it
    # out of the YAML file.
    auth_key: str = ""
    # Manage tailscale on EVERY machine, including the head: install the
    # userspace binaries, start tailscaled, and join the tailnet. Every
    # worker must then be addressed by an ordinary SSH-reachable IP —
    # tailscale can't be bootstrapped over tailscale, so a worker listed
    # by a 100.64.0.0/10 address is rejected at config load (opt single
    # machines out with ``manage_tailscale: false`` in auth.overrides
    # and run tailscaled there yourself).
    manage_all: bool = False
    # Userspace tailscale install used on CHIA-managed machines (no root
    # needed): the static tarball from pkgs.tailscale.com, unpacked into
    # tailscale_dir with state/socket kept alongside. The default is
    # per-cluster — /tmp/<cluster_name>/tailscale — so cluster daemons
    # never collide with each other or with a personally-run tailscaled
    # (pick a distinct socks_proxy port for full isolation). Note that
    # /tmp state does not survive reboots: a rejoin (consuming the
    # reusable auth key) happens on the next chia up.
    tailscale_version: str = "1.98.9"
    tailscale_dir: str = ""   # computed default: /tmp/<cluster_name>/tailscale
    head_advertise_ip: str = "127.200.0.1"
    gcs_port: int = 6379          # must match --port in head_start_ray_commands
    # The head owns the port block immediately BELOW worker_block_base,
    # with the same sub-block layout workers use (+0/+1 managers,
    # +16.. tools, +64.. Ray worker ports). Worker blocks growing upward
    # from worker_block_base therefore meet no head ports at all — they
    # can grow until the top of port space (65535), which with the
    # defaults fits 162 workers.
    head_node_manager_port: int = 23744
    head_object_manager_port: int = 23745
    head_tool_port_min: int = 23760
    head_tool_port_max: int = 23770
    head_worker_port_min: int = 23808
    head_worker_port_max: int = 23935
    # Each tailnet worker w gets advertise IP 127.0.0.(2+w) and a
    # contiguous block of ports at worker_block_base + w * worker_block_size:
    #   +0 node manager, +1 object manager,
    #   +16 .. +16+tool_port_count-1  tool ports,
    #   +64 .. +64+worker_port_count-1  Ray worker ports.
    worker_block_base: int = 24000
    worker_block_size: int = 256
    tool_port_count: int = 11
    worker_port_count: int = 128


@dataclass
class SSHAuthConfig:
    ssh_user: str
    ssh_private_key: str | None = None
    tunnel: TunnelConfig | None = None
    # Optional ssh ProxyCommand for reaching this host (e.g.
    # "nc -X 5 -x 127.0.0.1:1055 %h %p" for a tailscale userspace SOCKS5
    # proxy). Used by SSHClient, rsync, and TunnelManager alike.
    ssh_proxy_command: str | None = None
    # True if this host participates in the cluster over the tailnet
    # (see TailnetConfig). Mutually exclusive with ``tunnel``.
    tailnet: bool = False
    # True if CHIA installs/starts userspace tailscaled on this host and
    # joins it to the tailnet itself (default for cloud machines when a
    # tailnet section exists; per-IP opt-in for on-prem hosts).
    manage_tailscale: bool = False


@dataclass
class DockerConfig:
    image: str
    container_name: str
    pull_before_run: bool = True
    pull_timeout: int = 600
    run_options: list[str] = field(default_factory=list)
    run_setup_commands: list[str] = field(default_factory=list)
    engine: str = "docker"


@dataclass
class NodeTypeConfig:
    name: str
    resources: dict[str, float] = field(default_factory=dict)
    num_workers: int = 0
    worker_env_commands: list[str] = field(default_factory=list)
    worker_setup_commands: list[str] = field(default_factory=list)
    docker: DockerConfig | None = None
    compatible_ips: list[str] | None = None
    # How this type's workers pick among usable IPs in assign_nodes:
    #   "cluster" (default) — fewest nodes globally (across all types);
    #   "worker"            — fewest workers of THIS type (even distribution
    #                         across its own IP pool, regardless of others).
    balance_level: str = "cluster"


@dataclass
class AWSClusterConfig:
    """Optional AWS configuration for FireSim operations."""
    region: str = "us-east-1"
    key_name: str = "firesim"
    vpc_name: str = "firesim"
    security_group_name: str = "for-farms-only-firesim"
    ssh_user: str = "ubuntu"
    ssh_private_key: str | None = None
    use_public_ip: bool = False
    s3_bucket: str = "firesim-chia-builds"


@dataclass
class FireSimClusterConfig:
    """Optional FireSim configuration for FPGA build/run operations."""
    chipyard_path: str = ""
    deploy_path: str = ""
    build_recipes_file: str = "config_build_recipes.yaml"


@dataclass
class ClusterConfig:
    cluster_name: str
    head_ip: str
    worker_ips: list[str]
    ssh_user: str
    ssh_private_key: str | None
    node_types: dict[str, NodeTypeConfig]
    initialization_commands: list[str] = field(default_factory=list)
    head_env_commands: list[str] = field(default_factory=list)
    setup_commands: list[str] = field(default_factory=list)
    head_setup_commands: list[str] = field(default_factory=list)
    head_teardown_commands: list[str] = field(default_factory=list)
    head_start_ray_commands: list[str] = field(default_factory=list)
    worker_start_ray_commands: list[str] = field(default_factory=list)
    file_mounts: dict[str, str] = field(default_factory=dict)
    rsync_exclude: list[str] = field(default_factory=list)
    rsync_filter: list[str] = field(default_factory=list)
    global_docker: DockerConfig | None = None
    aws_config: AWSClusterConfig | None = None
    firesim_config: FireSimClusterConfig | None = None
    auth_overrides: dict[str, SSHAuthConfig] = field(default_factory=dict)
    ssh_proxy_command: str | None = None
    tailnet_config: TailnetConfig | None = None

    def get_ssh_auth(self, ip: str) -> SSHAuthConfig:
        """Return SSH auth for *ip*, falling back to the global config."""
        if ip in self.auth_overrides:
            return self.auth_overrides[ip]
        return SSHAuthConfig(ssh_user=self.ssh_user, ssh_private_key=self.ssh_private_key,
                             ssh_proxy_command=self.ssh_proxy_command)

    def is_tunneled(self, ip: str) -> bool:
        """Return True if *ip* requires an SSH tunnel."""
        override = self.auth_overrides.get(ip)
        return override is not None and override.tunnel is not None

    def is_tailnet(self, ip: str) -> bool:
        """Return True if *ip* joins the cluster over the tailnet."""
        override = self.auth_overrides.get(ip)
        return override is not None and override.tailnet

    def get_tunnel_config(self, ip: str) -> TunnelConfig | None:
        """Return the TunnelConfig for *ip*, or None if not tunneled."""
        override = self.auth_overrides.get(ip)
        if override is not None:
            return override.tunnel
        return None

    @property
    def head_gcs_port(self) -> int:
        """GCS port of the head node.

        Parsed from the ``--port`` flag in ``head_start_ray_commands``.
        Falls back to Ray's default (6379) if the flag is absent.
        """
        for cmd in self.head_start_ray_commands:
            m = re.search(r"--port[=\s]+(\d+)", cmd)
            if m:
                return int(m.group(1))
        return 6379

    @property
    def head_ray_address(self) -> str:
        """Explicit GCS address (``head_ip:port``) for ``ray.init``/``--address``."""
        return f"{self.head_ip}:{self.head_gcs_port}"


@dataclass
class NodeAssignment:
    ip: str
    node_type: NodeTypeConfig
    resources: dict[str, float]
    worker_index: int = 0


def _parse_docker(raw: dict, engine: str = "docker") -> DockerConfig:
    return DockerConfig(
        image=raw["image"],
        container_name=raw.get("container_name", "chia_container"),
        pull_before_run=raw.get("pull_before_run", True),
        pull_timeout=raw.get("pull_timeout", 600),
        run_options=raw.get("run_options", []),
        run_setup_commands=raw.get("run_setup_commands", []),
        engine=engine,
    )


def _parse_container_section(raw: dict, where: str) -> DockerConfig | None:
    """Parse the ``docker:`` section of *raw* (at most one)."""
    if "docker" in raw:
        return _parse_docker(raw["docker"], engine="docker")
    return None


def _expand_env_vars(data):
    """Recursively expand ${VAR} references in all string values, leaving $VAR as-is."""
    if isinstance(data, str):
        import re
        return re.sub(r'\$\{([^}]+)\}', lambda m: os.environ.get(m.group(1), m.group(0)), data)
    if isinstance(data, dict):
        return {k: _expand_env_vars(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_expand_env_vars(item) for item in data]
    return data


def parse_aws_nodes(raw: dict) -> tuple[dict[str, Any], str] | None:
    """Extract and parse the ``aws_nodes`` section from a raw config dict.

    Pops the section from *raw* so that :func:`build_config` does not see it.

    Returns ``(node_configs, region)`` or ``None`` if the section is absent.
    Each value in *node_configs* is an
    :class:`~chia.cluster.aws_nodes.AWSNodeConfig`.
    """
    from chia.cluster.aws_nodes import (
        AWSNodeConfig, DEFAULT_IMAGE_ID, DEFAULT_REGION, DEFAULT_AWS_SETUP_COMMANDS,
    )

    aws_nodes_raw = raw.pop("aws_nodes", None)
    if aws_nodes_raw is None:
        return None

    region = aws_nodes_raw.pop("region", DEFAULT_REGION)

    node_configs: dict[str, AWSNodeConfig] = {}
    known_keys = {"KeyName", "InstanceType", "count", "ImageId", "ssh_user", "ssh_private_key", "skip_default_setup", "setup_commands", "setup_timeout", "ssh_timeout", "join_tailnet"}

    for name, node_raw in aws_nodes_raw.items():
        if not isinstance(node_raw, dict):
            continue

        if "KeyName" not in node_raw:
            raise ConfigError(f"aws_nodes.{name}: missing required field 'KeyName'")
        if "InstanceType" not in node_raw:
            raise ConfigError(f"aws_nodes.{name}: missing required field 'InstanceType'")
        if "count" not in node_raw:
            raise ConfigError(f"aws_nodes.{name}: missing required field 'count'")

        extra_args = {k: v for k, v in node_raw.items() if k not in known_keys}

        node_configs[name] = AWSNodeConfig(
            KeyName=node_raw["KeyName"],
            InstanceType=node_raw["InstanceType"],
            count=node_raw["count"],
            ImageId=node_raw.get("ImageId", DEFAULT_IMAGE_ID),
            ssh_user=node_raw.get("ssh_user"),
            ssh_private_key=node_raw.get("ssh_private_key"),
            extra_args=extra_args,
            skip_default_setup=node_raw.get("skip_default_setup", False),
            setup_commands=node_raw.get("setup_commands", []),
            setup_timeout=node_raw.get("setup_timeout", 1800),
            ssh_timeout=node_raw.get("ssh_timeout", 120),
            join_tailnet=node_raw.get("join_tailnet"),
        )

    return node_configs, region


def parse_gcp_nodes(
    raw: dict,
) -> tuple[dict[str, Any], str, str, str | None, str | None] | None:
    """Extract and parse the ``gcp_nodes`` section from a raw config dict.

    Pops the section from *raw* so that :func:`build_config` does not see it.
    Mirrors :func:`parse_aws_nodes`.

    Returns ``(node_configs, project, zone, network, subnetwork)`` or ``None``
    if the section is absent.  Each value in *node_configs* is a
    :class:`~chia.cluster.gcp_nodes.GCPNodeConfig`.
    """
    from chia.cluster.gcp_nodes import GCPNodeConfig, DEFAULT_IMAGE, DEFAULT_ZONE

    gcp_raw = raw.pop("gcp_nodes", None)
    if gcp_raw is None:
        return None

    project = gcp_raw.pop("project", None)
    if not project:
        raise ConfigError("gcp_nodes: missing required field 'project'")
    zone = gcp_raw.pop("zone", DEFAULT_ZONE)
    network = gcp_raw.pop("network", None)
    subnetwork = gcp_raw.pop("subnetwork", None)

    node_configs: dict[str, GCPNodeConfig] = {}
    known_keys = {
        "machine_type", "count", "image", "zone", "disk_size_gb", "spot",
        "ssh_user", "ssh_private_key", "ssh_public_key", "use_os_login",
        "skip_default_setup", "setup_commands", "setup_timeout", "ssh_timeout",
        "join_tailnet",
    }

    for name, node_raw in gcp_raw.items():
        if not isinstance(node_raw, dict):
            continue

        # Node-type names become GCP label values (used for discovery), so they
        # must be valid label values; enforce loudly here.
        if not re.match(r"^[a-z][a-z0-9_-]*$", name) or len(name) > 63:
            raise ConfigError(
                f"gcp_nodes.{name}: node type name must be a valid GCP label "
                "value (lowercase, start with a letter, only [a-z0-9_-], <=63)"
            )
        if "machine_type" not in node_raw:
            raise ConfigError(f"gcp_nodes.{name}: missing required field 'machine_type'")
        if "count" not in node_raw:
            raise ConfigError(f"gcp_nodes.{name}: missing required field 'count'")

        extra_args = {k: v for k, v in node_raw.items() if k not in known_keys}

        node_configs[name] = GCPNodeConfig(
            machine_type=node_raw["machine_type"],
            count=node_raw["count"],
            image=node_raw.get("image", DEFAULT_IMAGE),
            zone=node_raw.get("zone"),
            disk_size_gb=node_raw.get("disk_size_gb"),
            spot=node_raw.get("spot", False),
            ssh_user=node_raw.get("ssh_user"),
            ssh_private_key=node_raw.get("ssh_private_key"),
            ssh_public_key=node_raw.get("ssh_public_key"),
            use_os_login=node_raw.get("use_os_login", False),
            extra_args=extra_args,
            skip_default_setup=node_raw.get("skip_default_setup", False),
            setup_commands=node_raw.get("setup_commands", []),
            setup_timeout=node_raw.get("setup_timeout", 1800),
            ssh_timeout=node_raw.get("ssh_timeout", 120),
            join_tailnet=node_raw.get("join_tailnet"),
        )

    return node_configs, project, zone, network, subnetwork


def _expand_node_placeholders(data: Any, ip_map: dict[str, list[str]]) -> Any:
    """Recursively replace ``@node_name:N`` placeholders with actual IPs.

    Works on both dict values and dict keys (since placeholders may appear
    as auth-override keys like ``"@ec2_worker:0"`` or ``"@gcp_worker:0"``).
    Cloud-agnostic: *ip_map* may come from AWS, GCP, or a merge of both.
    """
    if isinstance(data, str):
        def _replace(m: re.Match) -> str:
            node_name, index_str = m.group(1), m.group(2)
            index = int(index_str)
            if node_name not in ip_map:
                raise ConfigError(
                    f"Unknown cloud node reference: @{node_name}:{index} "
                    f"(available: {list(ip_map.keys())})"
                )
            ips = ip_map[node_name]
            if index >= len(ips):
                raise ConfigError(
                    f"Index {index} out of range for @{node_name} "
                    f"(has {len(ips)} instance(s))"
                )
            return ips[index]
        return re.sub(r'@(\w+):(\d+)', _replace, data)
    if isinstance(data, dict):
        return {
            _expand_node_placeholders(k, ip_map): _expand_node_placeholders(v, ip_map)
            for k, v in data.items()
        }
    if isinstance(data, list):
        return [_expand_node_placeholders(item, ip_map) for item in data]
    return data


# Back-compat alias (pre-GCP name).
_expand_aws_placeholders = _expand_node_placeholders


def _parse_tunnel_defaults(raw: dict) -> dict | bool:
    """Pop and validate the top-level ``tunnel_defaults`` block.

    Returns the validated dict of :class:`TunnelConfig` field overrides
    applied to every auto-injected cloud tunnel, or ``True`` (a default
    :class:`TunnelConfig`) when the block is absent or empty. Raises
    :class:`ConfigError` on unknown keys so a typo fails loudly here
    rather than as an opaque ``TypeError`` deep in ``build_config``.

    Accepts the neutral ``tunnel_defaults`` key, falling back to the
    pre-GCP ``aws_tunnel_defaults`` for back-compat (at most one).

    The injected tunnel value is keyed per-IP via a fresh copy, so a
    per-IP ``auth.overrides[ip].tunnel`` still wins (the inject loop only
    fills a missing ``tunnel`` key).
    """
    defaults = raw.pop("tunnel_defaults", None)
    legacy = raw.pop("aws_tunnel_defaults", None)
    if defaults is not None and legacy is not None:
        raise ConfigError(
            "Specify only one of 'tunnel_defaults' or 'aws_tunnel_defaults'"
        )
    if defaults is None:
        defaults = legacy
    if defaults is None:
        return True
    if not isinstance(defaults, dict):
        raise ConfigError(
            f"tunnel_defaults must be a mapping of TunnelConfig fields, "
            f"got {type(defaults).__name__}"
        )
    if not defaults:
        return True
    valid = {f.name for f in fields(TunnelConfig) if f.name != "tunnel_ip"}
    unknown = set(defaults) - valid
    if unknown:
        raise ConfigError(
            f"tunnel_defaults has unknown TunnelConfig field(s): "
            f"{sorted(unknown)} (valid: {sorted(valid)})"
        )
    return defaults


# Back-compat alias (pre-GCP name).
_parse_aws_tunnel_defaults = _parse_tunnel_defaults


def _parse_tailnet_section(raw: dict) -> TailnetConfig | None:
    """Parse the top-level ``tailnet`` block into a :class:`TailnetConfig`.

    Raises :class:`ConfigError` on unknown keys so a typo fails loudly,
    and on a missing ``head_tailnet_ip``.
    """
    section = raw.get("tailnet")
    if section is None:
        return None
    if not isinstance(section, dict):
        raise ConfigError(
            f"tailnet must be a mapping of TailnetConfig fields, "
            f"got {type(section).__name__}")
    valid = {f.name for f in fields(TailnetConfig)}
    unknown = set(section) - valid
    if unknown:
        raise ConfigError(
            f"tailnet has unknown field(s): {sorted(unknown)} "
            f"(valid: {sorted(valid)})")
    cfg = TailnetConfig(**section)
    if not cfg.head_tailnet_ip and not cfg.manage_all:
        raise ConfigError(
            "tailnet: missing required field 'head_tailnet_ip' "
            "(or set 'manage_all: true' to have CHIA join the head and "
            "discover it)")
    if not cfg.tailscale_dir:
        cluster = re.sub(r"[^A-Za-z0-9._-]+", "-",
                         str(raw.get("cluster_name", "default")))
        cfg.tailscale_dir = f"/tmp/{cluster}/tailscale"
    # The tailscaled control socket lives under tailscale_dir; Unix
    # socket paths are limited to ~107 bytes. Only checkable when the
    # path has no remote-expanded $VARs.
    if "$" not in cfg.tailscale_dir and \
            len(cfg.tailscale_dir) + len("/run/tailscaled.sock") > 100:
        raise ConfigError(
            f"tailnet.tailscale_dir is too long ({cfg.tailscale_dir!r}) — "
            f"the control socket under it would exceed the ~107-char Unix "
            f"socket path limit")
    return cfg


def _inject_cloud_tunnel_overrides(
    raw: dict,
    ip_map: dict[str, list[str]],
    node_configs: dict | None = None,
) -> None:
    """Add tunnel auth overrides for cloud-provisioned IPs (AWS or GCP).

    For each IP in *ip_map*, if there is no existing auth override with
    a tunnel config, one is added.  The tunnel value is taken from the
    top-level ``tunnel_defaults`` block (a :class:`TunnelConfig` field
    dict applied to every cloud tunnel) when present, else ``True``
    (a default :class:`TunnelConfig`).  If the corresponding node config
    (``AWSNodeConfig`` / ``GCPNodeConfig``) has ``ssh_user`` or
    ``ssh_private_key`` set, they are also injected into the override.
    Mutates *raw* in place.
    """
    tunnel_default = _parse_tunnel_defaults(raw)
    def _tunnel_value():
        # Fresh copy per IP so build_config's TunnelConfig(**...) never
        # shares a dict across IPs (and per-IP edits can't leak).
        return dict(tunnel_default) if isinstance(tunnel_default, dict) else tunnel_default

    auth = raw.setdefault("auth", {})
    overrides = auth.setdefault("overrides", {})

    for name, ips in ip_map.items():
        ssh_user = None
        ssh_private_key = None
        if node_configs and name in node_configs:
            ssh_user = node_configs[name].ssh_user
            ssh_private_key = node_configs[name].ssh_private_key

        for ip in ips:
            if ip not in overrides:
                overrides[ip] = {"tunnel": _tunnel_value()}
            elif "tunnel" not in overrides[ip]:
                overrides[ip]["tunnel"] = _tunnel_value()

            if ssh_user and "ssh_user" not in overrides[ip]:
                overrides[ip]["ssh_user"] = ssh_user
            if ssh_private_key and "ssh_private_key" not in overrides[ip]:
                overrides[ip]["ssh_private_key"] = ssh_private_key


# Back-compat alias (pre-GCP name).
_inject_aws_tunnel_overrides = _inject_cloud_tunnel_overrides


def _inject_cloud_tailnet_overrides(
    raw: dict,
    ip_map: dict[str, list[str]],
    node_configs: dict | None = None,
) -> None:
    """Add tailnet auth overrides for cloud-provisioned IPs (AWS or GCP).

    The tailnet counterpart of :func:`_inject_cloud_tunnel_overrides`:
    marks each IP as a CHIA-managed tailnet machine (``tailnet`` +
    ``manage_tailscale``) instead of injecting an SSH tunnel, and
    injects the machine type's ``ssh_user`` / ``ssh_private_key``.
    Mutates *raw* in place.
    """
    auth = raw.setdefault("auth", {})
    overrides = auth.setdefault("overrides", {})

    for name, ips in ip_map.items():
        ssh_user = None
        ssh_private_key = None
        if node_configs and name in node_configs:
            ssh_user = node_configs[name].ssh_user
            ssh_private_key = node_configs[name].ssh_private_key

        for ip in ips:
            entry = overrides.setdefault(ip, {})
            entry.setdefault("tailnet", True)
            entry.setdefault("manage_tailscale", True)
            if ssh_user and "ssh_user" not in entry:
                entry["ssh_user"] = ssh_user
            if ssh_private_key and "ssh_private_key" not in entry:
                entry["ssh_private_key"] = ssh_private_key


def apply_cloud_network_mode(raw, aws_result, aws_ip_map,
                             gcp_result, gcp_ip_map,
                             require_auth_key=True, logger=None):
    """Route each provisioned/discovered cloud machine type to its network mode.

    When the config has a top-level ``tailnet:`` section, cloud types
    default to joining the tailnet (no SSH tunnels); a per-type
    ``join_tailnet`` overrides the default either way. Injects the
    matching auth overrides for every IP in the maps (mutating *raw*)
    and returns ``{type_name: node_config}`` for the joining types.

    Used by both ``chia up`` (with provisioned IPs) and ``chia down``
    (with discovered IPs — pass ``require_auth_key=False`` there, since
    teardown never joins anything).
    """
    has_tailnet = raw.get("tailnet") is not None

    joining_types = {}
    for result, ip_map in ((aws_result, aws_ip_map), (gcp_result, gcp_ip_map)):
        if result is None or not ip_map:
            continue
        node_configs = result[0]
        join_map, tunnel_map = {}, {}
        for name, ips in ip_map.items():
            cfg = node_configs.get(name)
            explicit = cfg.join_tailnet if cfg is not None else None
            joins = explicit if explicit is not None else has_tailnet
            if joins and not has_tailnet:
                raise ConfigError(
                    f"cloud machine type '{name}': join_tailnet requires a "
                    f"top-level 'tailnet:' section")
            (join_map if joins else tunnel_map)[name] = ips
        if tunnel_map:
            _inject_cloud_tunnel_overrides(raw, tunnel_map, node_configs)
        if join_map:
            _inject_cloud_tailnet_overrides(raw, join_map, node_configs)
            joining_types.update({n: node_configs[n] for n in join_map})

    if joining_types and not (raw.get("tailnet") or {}).get("auth_key"):
        msg = ("tailnet.auth_key is required to join cloud machines to the "
               "tailnet — set it to a reusable (ideally ephemeral, "
               "pre-authorized) tskey-auth-... key, e.g. via ${TS_AUTHKEY}")
        if require_auth_key:
            raise ConfigError(msg)
        elif logger is not None:
            logger.warning(msg)
    return joining_types


def load_raw_config(yaml_path: str) -> dict:
    """Load a YAML config file and expand environment variables.

    Returns the raw dict before config construction, allowing callers
    to process ``aws_nodes`` and ``@`` placeholders before calling
    :func:`build_config`.
    """
    path = Path(yaml_path)
    if not path.exists():
        raise ConfigError(f"Config file not found: {yaml_path}")

    with open(path) as f:
        raw = yaml.safe_load(f)

    raw = _expand_env_vars(raw)
    logger.debug(f"Loaded raw config from {yaml_path}")
    return raw


def build_config(raw: dict) -> ClusterConfig:
    """Build a :class:`ClusterConfig` from a pre-processed raw dict."""
    provider = raw.get("provider", {})
    if "type" in provider:
        logger.warning(
            "provider.type is deprecated and ignored; CHIA always runs a local head "
            "node, with cloud workers added via aws_nodes / gcp_nodes.")
    if "worker_ips" in provider:
        logger.warning(
            "provider.worker_ips is deprecated and ignored; the worker pool is now "
            "derived from the union of every node type's compatible_ips.")

    auth = raw.get("auth", {})
    global_proxy_command = auth.get("ssh_proxy_command")
    tailnet_config = _parse_tailnet_section(raw)

    # Parse auth overrides
    auth_overrides: dict[str, SSHAuthConfig] = {}
    for ip, override_raw in auth.get("overrides", {}).items():
        tunnel_raw = override_raw.get("tunnel")
        if tunnel_raw is True:
            # Backward compat: tunnel: true → TunnelConfig with defaults
            tunnel_cfg = TunnelConfig()
        elif isinstance(tunnel_raw, dict):
            tunnel_cfg = TunnelConfig(**tunnel_raw)
        else:
            tunnel_cfg = None
        # 'tailnet: true' per-IP is accepted for explicitness but redundant:
        # when a 'tailnet:' section exists, every non-head worker is marked
        # automatically (see below).
        tailnet_flag = bool(override_raw.get("tailnet", False))
        if tailnet_flag and tunnel_cfg is not None:
            raise ConfigError(
                f"auth.overrides.{ip}: 'tailnet' and 'tunnel' are mutually "
                f"exclusive (tailnet machines need no SSH tunnel)")
        if tailnet_flag and tailnet_config is None:
            raise ConfigError(
                f"auth.overrides.{ip}: 'tailnet: true' requires a top-level "
                f"'tailnet:' section (with at least 'head_tailnet_ip')")
        # Explicit per-IP manage_tailscale wins either way; absent, it
        # defaults to tailnet.manage_all.
        manage_raw = override_raw.get("manage_tailscale")
        if manage_raw is None:
            manage_flag = bool(tailnet_config and tailnet_config.manage_all)
        else:
            manage_flag = bool(manage_raw)
        if manage_raw and tailnet_config is None:
            raise ConfigError(
                f"auth.overrides.{ip}: 'manage_tailscale: true' requires a "
                f"top-level 'tailnet:' section")
        auth_overrides[ip] = SSHAuthConfig(
            ssh_user=override_raw.get("ssh_user", auth.get("ssh_user", "")),
            ssh_private_key=override_raw.get("ssh_private_key", auth.get("ssh_private_key")),
            tunnel=tunnel_cfg,
            ssh_proxy_command=override_raw.get("ssh_proxy_command", global_proxy_command),
            tailnet=tailnet_flag,
            manage_tailscale=manage_flag,
        )

    # NOTE: tunnel_ip auto-assignment is handled per-worker in
    # bring_up_cluster() (node_setup.py), not here, so that each worker
    # on the same physical IP gets a unique loopback address.

    # Parse node types
    node_types: dict[str, NodeTypeConfig] = {}
    for name, nt_raw in raw.get("available_node_types", {}).items():
        docker = _parse_container_section(nt_raw, f"available_node_types.{name}")
        balance_level = nt_raw.get("balance_level", "cluster")
        if balance_level not in ("cluster", "worker"):
            logger.warning(
                f"Node type '{name}': unknown balance_level {balance_level!r}; "
                f"using 'cluster' (valid: 'cluster', 'worker')")
            balance_level = "cluster"
        node_types[name] = NodeTypeConfig(
            name=name,
            resources=nt_raw.get("resources", {}),
            num_workers=nt_raw.get("num_workers", nt_raw.get("max_workers", nt_raw.get("min_workers", 1))),
            worker_env_commands=nt_raw.get("worker_env_commands", []),
            worker_setup_commands=nt_raw.get("worker_setup_commands", []),
            docker=docker,
            compatible_ips=nt_raw.get("compatible_ips"),
            balance_level=balance_level,
        )
        logger.debug(f"  Node type '{name}': resources={node_types[name].resources}, "
                      f"min={node_types[name].num_workers}, max={node_types[name].num_workers}, "
                      f"compatible_ips={node_types[name].compatible_ips}")

    # The worker IP pool is derived from the union of every node type's
    # compatible_ips — there is no separate provider.worker_ips list. Each
    # node type that runs workers must therefore declare where they can run.
    for nt in node_types.values():
        if nt.num_workers > 0 and not nt.compatible_ips:
            raise ConfigError(
                f"Node type '{nt.name}' has {nt.num_workers} worker(s) but no "
                f"'compatible_ips'. Every worker-bearing node type must list the "
                f"IPs its workers may run on.")
    worker_ips = sorted({
        ip for nt in node_types.values() for ip in (nt.compatible_ips or [])
    })

    # Parse global docker
    global_docker = _parse_container_section(raw, "top-level config")

    # Parse optional AWS config
    aws_config = None
    if "aws" in raw:
        aws_raw = raw["aws"]
        aws_config = AWSClusterConfig(
            region=aws_raw.get("region", "us-east-1"),
            key_name=aws_raw.get("key_name", "firesim"),
            vpc_name=aws_raw.get("vpc_name", "firesim"),
            security_group_name=aws_raw.get("security_group_name", "for-farms-only-firesim"),
            ssh_user=aws_raw.get("ssh_user", "ubuntu"),
            ssh_private_key=aws_raw.get("ssh_private_key"),
            use_public_ip=aws_raw.get("use_public_ip", False),
            s3_bucket=aws_raw.get("s3_bucket", "firesim-chia-builds"),
        )
        logger.debug(f"  AWS config: region={aws_config.region}, key={aws_config.key_name}")

    # Parse optional FireSim config
    firesim_config = None
    if "firesim" in raw:
        fs_raw = raw["firesim"]
        firesim_config = FireSimClusterConfig(
            chipyard_path=fs_raw.get("chipyard_path", ""),
            deploy_path=fs_raw.get("deploy_path", ""),
            build_recipes_file=fs_raw.get("build_recipes_file", "config_build_recipes.yaml"),
        )
        logger.debug(f"  FireSim config: chipyard={firesim_config.chipyard_path}")

    # Tailnet clusters: every worker not colocated on the head machine IS
    # a tailnet worker, so mark them automatically — no per-IP override
    # boilerplate needed. Anything set explicitly in auth.overrides
    # (ssh user/key, a custom ssh_proxy_command) still wins; only the
    # missing pieces are filled in.
    #
    # Orchestration SSH reachability splits two ways:
    #   - Hosts addressed BY a tailnet IP (100.64.0.0/10) or hostname
    #     (e.g. MagicDNS) are dialed through the head's local SOCKS5
    #     proxy — the default ProxyCommand requires OpenBSD netcat on
    #     the head (override ssh_proxy_command per-IP or in auth: if not).
    #   - Hosts addressed by an ordinary IP (e.g. a cloud machine's public
    #     IP, which CHIA joins to the tailnet itself) are dialed
    #     directly; their tailnet IP is discovered at bring-up.
    if tailnet_config is not None:
        default_proxy = (global_proxy_command
                         or f"nc -X 5 -x {tailnet_config.socks_proxy} %h %p")
        for ip in worker_ips:
            if ip == provider["head_ip"]:
                continue  # head-colocated workers stay local
            override = auth_overrides.get(ip)
            if override is None:
                override = SSHAuthConfig(
                    ssh_user=auth.get("ssh_user", ""),
                    ssh_private_key=auth.get("ssh_private_key"),
                    manage_tailscale=tailnet_config.manage_all,
                )
                auth_overrides[ip] = override
            if override.tunnel is not None:
                continue  # rejected by the mixing check below
            override.tailnet = True
            try:
                is_ts_addr = (ipaddress.ip_address(ip)
                              in ipaddress.ip_network("100.64.0.0/10"))
                is_hostname = False
            except ValueError:
                is_ts_addr, is_hostname = False, True
            if is_ts_addr and override.manage_tailscale:
                raise ConfigError(
                    f"worker {ip} is addressed by its tailscale IP but "
                    f"marked for CHIA-managed tailscale — tailscale cannot "
                    f"be bootstrapped over tailscale. Address the machine "
                    f"by an ordinary SSH-reachable IP/hostname, or set "
                    f"'manage_tailscale: false' for it in auth.overrides "
                    f"and run tailscaled there yourself.")
            # Managed machines are always dialed directly (bootstrap needs
            # a non-tailnet path); unmanaged tailnet-addressed machines (or
            # MagicDNS hostnames) go through the SOCKS proxy.
            if (is_ts_addr or is_hostname) and not override.manage_tailscale \
                    and override.ssh_proxy_command is None:
                override.ssh_proxy_command = default_proxy
            if not is_ts_addr and not is_hostname and not override.manage_tailscale:
                logger.warning(
                    f"tailnet worker {ip} is neither a tailscale "
                    f"(100.64.0.0/10) address nor managed by CHIA "
                    f"(manage_tailscale) — Ray traffic to it will fail "
                    f"unless tailscaled is already running there")

    config = ClusterConfig(
        cluster_name=raw.get("cluster_name", "default"),
        head_ip=provider["head_ip"],
        worker_ips=worker_ips,
        ssh_user=auth.get("ssh_user", ""),
        ssh_private_key=auth.get("ssh_private_key"),
        node_types=node_types,
        initialization_commands=raw.get("initialization_commands", []),
        head_env_commands=raw.get("head_env_commands", []),
        setup_commands=raw.get("setup_commands", []),
        head_setup_commands=raw.get("head_setup_commands", []),
        head_teardown_commands=raw.get("head_teardown_commands", []),
        head_start_ray_commands=raw.get("head_start_ray_commands", []),
        worker_start_ray_commands=raw.get("worker_start_ray_commands", []),
        file_mounts=raw.get("file_mounts", {}),
        rsync_exclude=raw.get("rsync_exclude", []),
        rsync_filter=raw.get("rsync_filter", []),
        global_docker=global_docker,
        aws_config=aws_config,
        firesim_config=firesim_config,
        auth_overrides=auth_overrides,
        ssh_proxy_command=global_proxy_command,
        tailnet_config=tailnet_config,
    )

    if tailnet_config is not None:
        # The head advertises a loopback IP in tailnet mode, so ordinary
        # LAN workers could not find it — every worker must either be a
        # tailnet worker or live on the head machine itself.
        bad = [ip for ip in config.worker_ips
               if not config.is_tailnet(ip) and ip != config.head_ip]
        if bad:
            raise ConfigError(
                f"tailnet clusters require every worker to be a tailnet worker "
                f"(auth.overrides.<ip>.tailnet: true) or colocated on the head "
                f"machine; offending worker IP(s): {bad}")
        tunneled = [ip for ip in config.worker_ips if config.is_tunneled(ip)]
        if tunneled:
            raise ConfigError(
                f"tailnet clusters cannot mix with SSH-tunneled workers "
                f"(including aws_nodes/gcp_nodes) — the head advertises a "
                f"loopback IP that the tunnel path cannot route to; "
                f"tunneled IP(s): {tunneled}")

    logger.debug(f"Cluster '{config.cluster_name}': head={config.head_ip}, "
                  f"workers={config.worker_ips}, node_types={list(config.node_types.keys())}")
    return config


def load_config(yaml_path: str) -> ClusterConfig:
    """Load a YAML config file and build a ClusterConfig.

    Convenience wrapper around :func:`load_raw_config` + :func:`build_config`.
    Does not handle ``aws_nodes`` — use the split functions for that.
    """
    return build_config(load_raw_config(yaml_path))


def assign_nodes(config: ClusterConfig) -> list[NodeAssignment]:
    '''
    Node to IP assignment.

    Every node type is constrained by its ``compatible_ips`` (the worker IP
    pool is the union of these). Each node type's ``balance_level`` controls
    how its workers pick among the usable IPs:

      - ``cluster`` (default): pick the IP with the fewest nodes GLOBALLY
        (across all node types), so a type fills in around what others placed.
      - ``worker``: pick the IP with the fewest workers OF THIS TYPE, so the
        type's own workers spread as evenly as possible across its IP pool,
        independent of global load.

    Ties break toward the earliest IP in the pool. Both modes still update the
    global per-IP count so later node types remain load-aware.
    '''
    available_ips = list(config.worker_ips)
    assignments: list[NodeAssignment] = []
    nodes_per_ip = {ip: 0 for ip in available_ips}

    def _assign(nt: NodeTypeConfig, ip_pool: list[str], kind: str) -> None:
        # "worker" counts only this type's own placements (local); "cluster"
        # counts every type's (the shared nodes_per_ip). min() is first-wins, so
        # ties go to the earliest IP in ip_pool — matching the prior sorted()[0].
        local = {ip: 0 for ip in ip_pool}
        counter = local if nt.balance_level == "worker" else nodes_per_ip
        for i in range(nt.num_workers):
            chosen = min(ip_pool, key=lambda ip: counter[ip])
            assignments.append(NodeAssignment(
                ip=chosen, node_type=nt, resources=nt.resources, worker_index=i))
            counter[chosen] += 1
            if counter is not nodes_per_ip:
                nodes_per_ip[chosen] += 1  # keep global count current for later types
            logger.debug(f"  Assigned {chosen} -> {nt.name}-{i} ({kind}, {nt.balance_level})")

    for nt in config.node_types.values():
        if nt.num_workers <= 0:
            continue
        usable = [ip for ip in nt.compatible_ips if ip in available_ips]
        if len(usable) == 0:
            raise ConfigError(
                f"Node type '{nt.name}' needs workers from "
                f"compatible_ips {nt.compatible_ips}, but none "
                f"are in the worker pool"
            )
        _assign(nt, usable, "constrained")

    logger.info(f"Node assignments: {len(assignments)} workers across "
                f"{len(set(a.node_type.name for a in assignments))} node types")
    for a in assignments:
        logger.info(f"  {a.ip} -> {a.node_type.name}-{a.worker_index} (resources: {a.resources})")

    return assignments