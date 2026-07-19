from __future__ import annotations

import ipaddress
import json
import socket
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from chia.cluster.config import ClusterConfig, NodeAssignment, TunnelConfig, assign_nodes
from chia.cluster.docker import DockerManager
from chia.cluster.log import get_logger, log_phase
from chia.cluster.ssh import SSHClient
from chia.cluster.tailnet import (
    TailnetWorkerAlloc, allocate_tailnet_workers, build_relay_spec,
    start_relay, stop_relay,
)
from chia.cluster.tunnel import TunnelManager

from dataclasses import replace


logger = get_logger("setup")

_PORT_STEP_DEFAULT = 100
_PORT_STEP_WORKER = 1000

# Loopback address used on tunnelled EC2 nodes for reverse-tunnel tool
# access.  127.200.0.1 is always valid on Linux's lo interface without
# any explicit ``ip addr add``.  Using head_ip would poison the EC2
# node's routing table (packets to head_ip would loop to lo instead of
# going out over the network).
_RELAY_IP = "127.200.0.1"


def _stop_failed_worker_tunnels(
    tunnel_mgr: TunnelManager | None,
    failed: list,
    tunnel_configs: dict,
) -> None:
    """Stop tunnels only for physical IPs where EVERY tunneled worker failed.

    A blanket stop_all() here would sever healthy, already-registered
    tunneled workers (their GCS heartbeats ride these tunnels) just
    because an unrelated LAN worker failed setup.  Granularity is per
    physical IP rather than per worker because the first tunnel per IP
    carries shared -R forwards that surviving workers on the same host
    depend on.
    """
    if not tunnel_mgr or not tunnel_configs:
        return
    failed_keys = {(a.ip, a.node_type.name, a.worker_index) for a in failed}
    by_ip: dict[str, list[tuple[str, str, int]]] = defaultdict(list)
    for key in tunnel_configs:
        by_ip[key[0]].append(key)
    for ip, keys in by_ip.items():
        if all(k in failed_keys for k in keys):
            for k in keys:
                tunnel_mgr.stop_tunnel(tunnel_configs[k].tunnel_ip)


# Resources auto-added by Ray that should be excluded when matching nodes.
_RAY_AUTO_RESOURCES = {"CPU", "memory", "object_store_memory"}


def _custom_resources(resources: dict) -> dict:
    """Filter out Ray auto-assigned resources, keeping only custom ones."""
    return {k: float(v) for k, v in resources.items()
            if k not in _RAY_AUTO_RESOURCES
            and not k.startswith("node:")
            and not k.startswith("accelerator_type:")}


def _make_match_key(ip: str, resources: dict) -> tuple:
    """Create a hashable matching key from IP and custom resources."""
    try:
        resolved_ip = socket.gethostbyname(ip)
    except socket.gaierror:
        resolved_ip = ip
    custom = _custom_resources(resources)
    return (resolved_ip, tuple(sorted(custom.items())))


def query_ray_cluster_nodes(config: ClusterConfig) -> list[dict] | None:
    """Query the running Ray cluster for alive nodes via SSH to the head.

    Returns a list of node dicts (from ``ray.nodes()``) if a Ray cluster
    is running on the head, or ``None`` if no cluster is found.
    """
    ssh = _make_ssh(config, config.head_ip)
    try:
        ssh.wait_for_ssh(timeout=15)
    except Exception:
        return None

    query_script = list(config.head_env_commands) + [
        "python3 << 'CHIA_QUERY_EOF'",
        "import ray, json",
        "ray.init(address='auto', ignore_reinit_error=True)",
        "nodes = [{'NodeName': n['NodeName'], 'Alive': n['Alive'], 'Resources': n.get('Resources', {})} for n in ray.nodes()]",
        "print('CHIA_NODES:' + json.dumps(nodes))",
        "CHIA_QUERY_EOF",
    ]

    try:
        result = ssh.run_script(query_script, timeout=30, check=False)
        if result.returncode != 0:
            return None

        for line in result.stdout.strip().splitlines():
            line = line.strip()
            if line.startswith("CHIA_NODES:"):
                return json.loads(line[len("CHIA_NODES:"):])
        return None
    except Exception:
        return None


def compute_new_assignments(
    desired: list[NodeAssignment],
    existing_nodes: list[dict],
    tunnel_configs: dict[tuple[str, str, int], TunnelConfig] | None = None,
) -> list[NodeAssignment]:
    """Compare desired assignments against existing Ray nodes.

    Returns the subset of *desired* that are not already present in the
    running cluster.  Matching is by ``(resolved_ip, custom_resources)``
    with count-based tracking so multiple identical workers on the same
    IP are handled correctly.

    When *tunnel_configs* is provided, tunneled workers are matched by
    their tunnel IP (``127.0.0.x``) instead of their real IP, since Ray
    registers tunneled nodes under ``--node-ip-address=<tunnel_ip>``.
    """
    existing_counter: Counter = Counter()
    for node in existing_nodes:
        if not node.get("Alive"):
            continue
        key = _make_match_key(node["NodeName"], node.get("Resources", {}))
        existing_counter[key] += 1

    new_assignments: list[NodeAssignment] = []
    for a in desired:
        tc = (tunnel_configs or {}).get((a.ip, a.node_type.name, a.worker_index))
        match_ip = tc.tunnel_ip if tc else a.ip
        key = _make_match_key(match_ip, a.resources)
        if existing_counter[key] > 0:
            existing_counter[key] -= 1
            logger.info(
                f"Node {a.ip} ({a.node_type.name}-{a.worker_index}) "
                f"already exists in cluster, skipping")
        else:
            new_assignments.append(a)
            logger.info(
                f"Node {a.ip} ({a.node_type.name}-{a.worker_index}) "
                f"is NEW, will be added")

    return new_assignments


def add_nodes_to_cluster(
    config: ClusterConfig,
    new_assignments: list[NodeAssignment],
) -> TunnelManager | None:
    """Add only the given worker assignments to an existing Ray cluster.

    Skips head setup entirely and never calls ``ray stop`` on IPs that
    already have running Ray processes (for bare-metal workers).
    Supports tunneled (EC2) workers by computing tunnel configs for ALL
    assignments (for consistent port allocation) and only starting
    tunnels for the new ones.
    """
    if not new_assignments:
        logger.info("No new assignments to add.")
        return None

    if config.tailnet_config is not None:
        raise RuntimeError(
            "chia up --add is not yet supported for tailnet clusters — "
            "re-run a full 'chia up' instead (existing workers are detected "
            "and skipped).")

    logger.info(f"Adding {len(new_assignments)} new worker(s) to cluster")

    head_ip_resolved = socket.gethostbyname(config.head_ip)

    # Compute tunnel configs for ALL assignments so port allocation is
    # consistent with the existing cluster.  Then extract only configs
    # for the new assignments.
    all_assignments = assign_nodes(config)
    all_tunnel_configs = allocate_worker_tunnels(config, all_assignments)

    new_keys = {(a.ip, a.node_type.name, a.worker_index) for a in new_assignments}
    new_tunnel_configs: dict[tuple[str, str, int], TunnelConfig] = {
        k: tc for k, tc in all_tunnel_configs.items() if k in new_keys
    }

    # --- Tunnel setup for new tunneled workers ---
    new_tunneled_ips = {a.ip for a in new_assignments if config.is_tunneled(a.ip)}
    tunnel_mgr: TunnelManager | None = None

    if new_tunneled_ips and new_tunnel_configs:
        # Reverse tool ports: include new workers' tool ports + head tool ports.
        all_reverse_tool_ports: list[int] = []
        for tc in new_tunnel_configs.values():
            all_reverse_tool_ports.extend(range(tc.tool_port_min, tc.tool_port_max + 1))
        base_tc = next(iter(new_tunnel_configs.values()), TunnelConfig())
        all_reverse_tool_ports.extend(
            range(base_tc.head_tool_port_min, base_tc.head_tool_port_max + 1)
        )
        all_reverse_tool_ports = sorted(set(all_reverse_tool_ports))

        head_worker_ports = [
            base_tc.head_node_manager_port,
            base_tc.head_object_manager_port,
        ] + list(range(
            base_tc.head_worker_port_min, base_tc.head_worker_port_max + 1
        ))

        # Pre-tunnel setup once per new physical IP.
        for ip in new_tunneled_ips:
            ip_base_tc = config.get_tunnel_config(ip)
            if ip_base_tc and ip_base_tc.pre_tunnel_commands:
                ssh = _make_ssh(config, ip)
                ssh.wait_for_ssh()
                with log_phase(logger, f"Running pre-tunnel setup on {ip}"):
                    ssh.run_commands(ip_base_tc.pre_tunnel_commands)

        # Start tunnels.  Only the first SSH per physical IP carries
        # reverse tunnels to avoid bind conflicts on the relay IP.
        tunnel_mgr = TunnelManager()
        seen_ips: set[str] = set()
        for (ip, _, _), tc in new_tunnel_configs.items():
            is_first = ip not in seen_ips
            seen_ips.add(ip)
            ssh_auth = config.get_ssh_auth(ip)
            tunnel_mgr.start_tunnel(
                tc.tunnel_ip, ip, ssh_auth, tc,
                head_ip=head_ip_resolved,
                relay_ip=_RELAY_IP,
                reverse_tool_ports=all_reverse_tool_ports if is_first else None,
                reverse_head_worker_ports=head_worker_ports if is_first else None,
            )
            tunnel_mgr.wait_for_tunnel(tc.tunnel_ip)

        logger.info(
            f"SSH tunnels established for new workers: "
            f"{len(new_tunnel_configs)} tunnel(s) across "
            f"{len(new_tunneled_ips)} IP(s)")

        # iptables DNAT on each new AWS host.
        dport_worker = f"{base_tc.head_worker_port_min}:{base_tc.head_worker_port_max}"
        dport_raylet = f"{base_tc.head_node_manager_port}:{base_tc.head_object_manager_port}"
        for ip in new_tunneled_ips:
            ssh = _make_ssh(config, ip)
            route_localnet_cmd = (
                "sudo sysctl -w net.ipv4.conf.all.route_localnet=1 && "
                "sudo sysctl -w net.ipv4.conf.lo.route_localnet=1"
            )
            iptables_cmds = []
            for dport in [dport_worker, dport_raylet]:
                iptables_cmds.append(
                    f"sudo iptables -t nat -C OUTPUT -d {head_ip_resolved} -p tcp "
                    f"--dport {dport} -j DNAT --to-destination {_RELAY_IP} 2>/dev/null || "
                    f"sudo iptables -t nat -A OUTPUT -d {head_ip_resolved} -p tcp "
                    f"--dport {dport} -j DNAT --to-destination {_RELAY_IP}"
                )
            with log_phase(logger, f"Setting up iptables DNAT for head ports on {ip}"):
                ssh.run_commands([route_localnet_cmd] + iptables_cmds)

    # --- Set up new worker nodes ---
    by_ip: dict[str, list[NodeAssignment]] = defaultdict(list)
    for a in new_assignments:
        by_ip[a.ip].append(a)

    def _setup_ip_workers(ip: str, ip_assignments: list[NodeAssignment]):
        failed_local = []
        for i, a in enumerate(ip_assignments):
            tc = new_tunnel_configs.get((a.ip, a.node_type.name, a.worker_index))
            try:
                setup_worker_node(config, a, tunnel_config=tc,
                                  skip_ray_stop=True,
                                  head_ip=head_ip_resolved if tc else None)
                logger.info(
                    f"Worker {a.ip} ({a.node_type.name}-{a.worker_index}) ready")
            except Exception as e:
                logger.error(
                    f"Worker {a.ip} ({a.node_type.name}-{a.worker_index}) "
                    f"FAILED: {e}")
                failed_local.append(a)
        return failed_local

    with ThreadPoolExecutor(max_workers=max(1, len(by_ip))) as pool:
        futures = [
            pool.submit(_setup_ip_workers, ip, ip_assignments)
            for ip, ip_assignments in by_ip.items()
        ]
        failed = []
        for fut in as_completed(futures):
            failed.extend(fut.result())

    if failed:
        _stop_failed_worker_tunnels(tunnel_mgr, failed, new_tunnel_configs)
        raise RuntimeError(
            f"{len(failed)} worker(s) failed to set up: "
            f"{', '.join(f'{a.ip} ({a.node_type.name})' for a in failed)}"
        )

    logger.info(
        f"Added {len(new_assignments)} new worker(s) to cluster "
        f"'{config.cluster_name}'")
    return tunnel_mgr


def allocate_worker_tunnels(
    config: ClusterConfig,
    assignments: list[NodeAssignment],
) -> dict[tuple[str, str, int], TunnelConfig]:
    """Compute per-worker tunnel configs without starting any tunnels.

    Returns a dict keyed by ``(ip, node_type_name, worker_index)`` for every
    worker assignment that lands on a tunneled IP.  Each worker gets a unique
    loopback address (127.0.0.2, 127.0.0.3, ...).

    Ray/GCS ports are offset per-physical-IP (they bind on unique ``tun_ip``
    loopbacks so only workers sharing a host can collide).  Tool ports are
    offset globally (they bind on the shared ``head_ip`` so every tunnelled
    worker's tool range must be unique cluster-wide).
    """
    tunneled_ips = set(ip for ip in config.worker_ips if config.is_tunneled(ip))
    if not tunneled_ips:
        return {}

    result: dict[tuple[str, str, int], TunnelConfig] = {}
    next_addr = ipaddress.IPv4Address("127.0.0.2")
    ip_worker_count: dict[str, int] = defaultdict(int)
    global_tool_index = 0

    for a in assignments:
        if a.ip not in tunneled_ips:
            continue

        base_tc = config.get_tunnel_config(a.ip) or TunnelConfig()
        # Per-IP offset for Ray/GCS ports (bind on unique tun_ip per worker).
        ray_offset = ip_worker_count[a.ip] * _PORT_STEP_DEFAULT
        worker_offset = ip_worker_count[a.ip] * _PORT_STEP_WORKER
        ip_worker_count[a.ip] += 1
        # Global offset for tool ports (bind on shared head_ip).
        tool_offset = global_tool_index * _PORT_STEP_DEFAULT
        global_tool_index += 1
        tc = replace(base_tc,
            tunnel_ip=str(next_addr),
            gcs_tunnel_port=base_tc.gcs_tunnel_port + ray_offset,
            ray_node_manager_port=base_tc.ray_node_manager_port + ray_offset,
            ray_object_manager_port=base_tc.ray_object_manager_port + ray_offset,
            ray_worker_port_min=base_tc.ray_worker_port_min + worker_offset,
            ray_worker_port_max=base_tc.ray_worker_port_max + worker_offset,
            tool_port_min=base_tc.tool_port_min + tool_offset,
            tool_port_max=base_tc.tool_port_max + tool_offset,
            pre_tunnel_commands=[],
        )
        result[(a.ip, a.node_type.name, a.worker_index)] = tc

        next_addr += 1
        if next_addr == ipaddress.IPv4Address("127.0.0.1"):
            next_addr += 1

    return result


def _make_ssh(config: ClusterConfig, ip: str) -> SSHClient:
    auth = config.get_ssh_auth(ip)
    # Tunneled (AWS) nodes accept only their dedicated key — use it exclusively
    # so a full ssh-agent can't exhaust sshd MaxAuthTries. On-prem nodes keep the
    # agent (they authenticate via forwarded agent keys, not the AWS key).
    return SSHClient(ip, auth.ssh_user, auth.ssh_private_key,
                     identities_only=config.is_tunneled(ip),
                     proxy_command=auth.ssh_proxy_command)


def _rsync_file_mounts(ssh: SSHClient, config: ClusterConfig) -> None:
    if not config.file_mounts:
        return
    for remote_path, local_path in config.file_mounts.items():
        ssh.rsync_up(
            local_path, remote_path,
            exclude=config.rsync_exclude,
            filter_rules=config.rsync_filter,
        )


def build_head_script(config: ClusterConfig) -> list[str]:
    """Build the script that runs on the head node.

    When tunneled workers exist, the head's Ray worker ports are pinned
    to a known range so they can be reverse-tunneled to AWS workers for
    actor communication (e.g. ProfileCollectorActor).

    In tailnet mode the head additionally advertises its loopback IP
    (``head_advertise_ip``) so that tailnet workers' relays can serve
    every head address the cluster hands out.
    """
    script: list[str] = []
    script.extend(config.head_env_commands)
    script.extend(config.setup_commands)
    script.extend(config.head_setup_commands)

    # Detect if any workers are tunneled
    tunnel_cfg = None
    for ip in config.worker_ips:
        tc = config.get_tunnel_config(ip)
        if tc is not None:
            tunnel_cfg = tc
            break

    tn = config.tailnet_config
    if tn is not None:
        # ChiaTools hosted by Ray workers on the head node advertise the
        # head's loopback IP, reachable from every tailnet worker.
        script.append(f"export CHIA_TOOL_ADVERTISE_HOST={tn.head_advertise_ip}")
        script.append(f"export CHIA_TOOL_BASE_PORT={tn.head_tool_port_min}")
        script.append(f"export CHIA_TOOL_MAX_PORT={tn.head_tool_port_max}")

    for cmd in config.head_start_ray_commands:
        if tn is not None and "ray start" in cmd and "--head" in cmd:
            cmd += (f" --node-ip-address={tn.head_advertise_ip}"
                    f" --node-manager-port={tn.head_node_manager_port}"
                    f" --object-manager-port={tn.head_object_manager_port}"
                    f" --min-worker-port={tn.head_worker_port_min}"
                    f" --max-worker-port={tn.head_worker_port_max}")
        elif tunnel_cfg and "ray start" in cmd and "--head" in cmd:
            cmd += (f" --node-manager-port={tunnel_cfg.head_node_manager_port}"
                    f" --object-manager-port={tunnel_cfg.head_object_manager_port}"
                    f" --min-worker-port={tunnel_cfg.head_worker_port_min}"
                    f" --max-worker-port={tunnel_cfg.head_worker_port_max}")
        script.append(cmd)
    return script


def build_worker_script(
    config: ClusterConfig,
    assignment: NodeAssignment,
    tunnel_config: TunnelConfig | None = None,
    skip_ray_stop: bool = False,
    head_ip: str | None = None,
    tailnet_alloc: TailnetWorkerAlloc | None = None,
) -> list[str]:
    """Build the script that runs on a worker node.

    When *tunnel_config* is provided the worker is behind an SSH tunnel:

    - ``RAY_HEAD_IP`` is set to the tunnel IP so Ray connects via the
      reverse-tunnelled GCS port.
    - ``ray start --address`` is rewritten to ``<tunnel_ip>:<gcs_tunnel_port>``.
    - ``--node-ip-address`` is set to the tunnel IP.
    - Ray ports and tool ports are pinned to match the SSH tunnel forwards.

    When *tailnet_alloc* is provided the worker joins over the tailscale
    network: it registers under its advertised loopback IP, dials the
    head GCS at the head's advertised loopback IP (served by the local
    relay), and pins its ports to the allocated block.

    Non-tunneled workers let Ray auto-assign ports to avoid conflicts
    (especially when multiple --net=host containers share the same host).

    When *skip_ray_stop* is True, ``ray stop`` commands are omitted.
    This is needed for subsequent bare-SSH workers on the same host,
    where ``ray stop`` would kill the already-running first worker.
    Docker workers are unaffected (separate PID namespace).
    """
    ip = assignment.ip
    nt = assignment.node_type
    script: list[str] = []

    script.extend(nt.worker_env_commands)
    script.extend(config.setup_commands)
    script.extend(nt.worker_setup_commands)

    # Inject RAY_HEAD_IP and --resources into worker start commands
    resources_json = json.dumps(assignment.resources)
    is_head_ip = (ip == config.head_ip)

    if tailnet_alloc is not None:
        tn = config.tailnet_config
        adv_ip = tailnet_alloc.advertise_ip
        script.append(f"export RAY_HEAD_IP={tn.head_advertise_ip}")
        script.append(f"export CHIA_TOOL_BASE_PORT={tailnet_alloc.tool_port_min}")
        script.append(f"export CHIA_TOOL_MAX_PORT={tailnet_alloc.tool_port_max}")
        # Tools on this worker advertise its loopback IP — reachable from
        # every cluster node via the relays (no rewrite/relay-host needed).
        script.append(f"export CHIA_TOOL_ADVERTISE_HOST={adv_ip}")
    elif tunnel_config is not None:
        tun_ip = tunnel_config.tunnel_ip
        script.append(f"export RAY_HEAD_IP={tun_ip}")
        script.append(f"export CHIA_TOOL_BASE_PORT={tunnel_config.tool_port_min}")
        script.append(f"export CHIA_TOOL_MAX_PORT={tunnel_config.tool_port_max}")
        if head_ip:
            script.append(f"export CHIA_TOOL_ADVERTISE_HOST={head_ip}")
            script.append(f"export CHIA_TOOL_RELAY_HOST={_RELAY_IP}")
    else:
        script.append(f"export RAY_HEAD_IP={config.head_ip}")

    for cmd in config.worker_start_ray_commands:
        if "ray stop" in cmd and (is_head_ip or skip_ray_stop):
            continue
        if "ray start" in cmd and "--address" in cmd:
            cmd = f"{cmd} --resources='{resources_json}'"
            if tailnet_alloc is not None:
                cmd = cmd.replace(
                    "--address=$RAY_HEAD_IP:6379",
                    f"--address={tn.head_advertise_ip}:{tn.gcs_port}",
                )
                cmd += (
                    f" --node-ip-address={adv_ip}"
                    f" --node-manager-port={tailnet_alloc.node_manager_port}"
                    f" --object-manager-port={tailnet_alloc.object_manager_port}"
                    f" --min-worker-port={tailnet_alloc.worker_port_min}"
                    f" --max-worker-port={tailnet_alloc.worker_port_max}"
                )
            elif tunnel_config is not None:
                cmd = cmd.replace(
                    "--address=$RAY_HEAD_IP:6379",
                    f"--address={tun_ip}:{tunnel_config.gcs_tunnel_port}",
                )
                cmd += f" --node-ip-address={tun_ip}"
                cmd += (
                    f" --node-manager-port={tunnel_config.ray_node_manager_port}"
                    f" --object-manager-port={tunnel_config.ray_object_manager_port}"
                    f" --min-worker-port={tunnel_config.ray_worker_port_min}"
                    f" --max-worker-port={tunnel_config.ray_worker_port_max}"
                )
        script.append(cmd)

    return script


def setup_head_node(config: ClusterConfig) -> None:
    ssh = _make_ssh(config, config.head_ip)

    with log_phase(logger, f"Waiting for SSH to head node {config.head_ip}"):
        ssh.wait_for_ssh()

    # Rsync runs outside the script (separate process)
    with log_phase(logger, f"Syncing file mounts to head node {config.head_ip}"):
        if config.initialization_commands:
            logger.debug(f"[{config.head_ip}] Running initialization commands")
            ssh.run_commands(config.initialization_commands)
        _rsync_file_mounts(ssh, config)

    script = build_head_script(config)

    with log_phase(logger, f"Running setup and starting Ray head on {config.head_ip}"):
        ssh.run_script(script)


def setup_worker_node(
    config: ClusterConfig,
    assignment: NodeAssignment,
    tunnel_config: TunnelConfig | None = None,
    skip_ray_stop: bool = False,
    head_ip: str | None = None,
    tailnet_alloc: TailnetWorkerAlloc | None = None,
) -> None:
    ip = assignment.ip
    nt = assignment.node_type
    ssh = _make_ssh(config, ip)

    with log_phase(logger, f"Waiting for SSH to worker {ip} ({nt.name})"):
        ssh.wait_for_ssh()

    # Initialization commands + rsync run outside the main script
    with log_phase(logger, f"Syncing file mounts to worker {ip} ({nt.name})"):
        if config.initialization_commands:
            logger.debug(f"[{ip}] Running initialization commands")
            ssh.run_commands(config.initialization_commands)
        _rsync_file_mounts(ssh, config)

    # Determine execution context: docker or bare SSH
    docker_config = nt.docker or config.global_docker
    if docker_config:
        docker_config = replace(docker_config, container_name=f"{docker_config.container_name}-{assignment.worker_index}")
        docker_mgr = DockerManager(ssh, docker_config)
        with log_phase(logger, f"Setting up docker container on {ip} ({nt.name}-{assignment.worker_index})"):
            docker_mgr.setup_container()

    # Docker workers have isolated PID namespaces, so ray stop is always
    # safe.  For bare-SSH workers, honour the caller's skip_ray_stop flag
    # to avoid killing a sibling worker's Ray processes.
    effective_skip = skip_ray_stop and not docker_config
    script = build_worker_script(config, assignment, tunnel_config=tunnel_config,
                                 skip_ray_stop=effective_skip, head_ip=head_ip,
                                 tailnet_alloc=tailnet_alloc)

    with log_phase(logger, f"Running setup and starting Ray worker on {ip} ({nt.name})"):
        if docker_config:
            docker_mgr.exec_script(script)
        else:
            ssh.run_script(script)


def bring_up_cluster(config: ClusterConfig) -> TunnelManager | None:

    setup_head_node(config)

    logger.info("Head node ready")

    assignments = assign_nodes(config)
    if not assignments:
        logger.info("No worker assignments. Cluster is head-only.")
        return None

    # Resolve head IP to numeric form for tunnel bind addresses.
    head_ip_resolved = socket.gethostbyname(config.head_ip)

    # Start SSH tunnels — one per worker on a tunneled IP, each with a
    # unique loopback address so multiple containers on the same host
    # don't collide on Ray's pinned ports.
    tunneled_ips = set(ip for ip in config.worker_ips if config.is_tunneled(ip))
    tunnel_mgr: TunnelManager | None = None
    worker_tunnel_configs = allocate_worker_tunnels(config, assignments)

    if tunneled_ips:
        # Compute the full set of tool ports that EC2 workers need
        # reverse-tunnel access to (all workers' tool ports + head tool ports).
        all_reverse_tool_ports: list[int] = []
        for tc in worker_tunnel_configs.values():
            all_reverse_tool_ports.extend(range(tc.tool_port_min, tc.tool_port_max + 1))
        base_tc = next(iter(worker_tunnel_configs.values()), TunnelConfig())
        all_reverse_tool_ports.extend(
            range(base_tc.head_tool_port_min, base_tc.head_tool_port_max + 1)
        )
        all_reverse_tool_ports = sorted(set(all_reverse_tool_ports))

        # Head ports to reverse-tunnel so EC2 workers can reach
        # Ray workers, actors, and the raylet on the head node.
        # Includes the node/object manager ports (for object store
        # transfers) and the worker port range (for actor RPCs and
        # object ownership notifications).
        head_worker_ports = [
            base_tc.head_node_manager_port,
            base_tc.head_object_manager_port,
        ] + list(range(
            base_tc.head_worker_port_min, base_tc.head_worker_port_max + 1
        ))

        # Run pre-tunnel setup (e.g. GatewayPorts sshd config) once per physical IP.
        for ip in tunneled_ips:
            base_tc = config.get_tunnel_config(ip)
            if base_tc and base_tc.pre_tunnel_commands:
                ssh = _make_ssh(config, ip)
                ssh.wait_for_ssh()
                with log_phase(logger, f"Running pre-tunnel setup on {ip}"):
                    ssh.run_commands(base_tc.pre_tunnel_commands)

        # Start tunnels.  Only the first SSH process per physical EC2 IP
        # carries -R reverse tunnels (to avoid ExitOnForwardFailure bind
        # conflicts on the shared relay IP).
        tunnel_mgr = TunnelManager()
        seen_ips: set[str] = set()
        for (ip, _, _), tc in worker_tunnel_configs.items():
            is_first = ip not in seen_ips
            seen_ips.add(ip)
            ssh_auth = config.get_ssh_auth(ip)
            tunnel_mgr.start_tunnel(
                tc.tunnel_ip, ip, ssh_auth, tc,
                head_ip=head_ip_resolved,
                relay_ip=_RELAY_IP,
                reverse_tool_ports=all_reverse_tool_ports if is_first else None,
                reverse_head_worker_ports=head_worker_ports if is_first else None,
            )
            tunnel_mgr.wait_for_tunnel(tc.tunnel_ip)

        logger.info(f"SSH tunnels established: {len(worker_tunnel_configs)} worker tunnel(s) "
                     f"across {len(tunneled_ips)} IP(s), "
                     f"{len(all_reverse_tool_ports)} reverse tool port(s), "
                     f"{len(head_worker_ports)} reverse head worker port(s)")

        # iptables DNAT on each AWS host: redirect traffic destined for
        # the head's real IP on head ports to the relay IP, where the
        # reverse tunnel forwards it to the head.  This covers the
        # worker port range, node manager, and object manager ports
        # so that object store transfers and actor RPCs work.
        dport_worker = f"{base_tc.head_worker_port_min}:{base_tc.head_worker_port_max}"
        dport_raylet = f"{base_tc.head_node_manager_port}:{base_tc.head_object_manager_port}"
        for ip in tunneled_ips:
            ssh = _make_ssh(config, ip)
            # Enable route_localnet so the kernel doesn't silently drop
            # packets DNAT'd to 127.x.x.x (the relay loopback).
            route_localnet_cmd = (
                "sudo sysctl -w net.ipv4.conf.all.route_localnet=1 && "
                "sudo sysctl -w net.ipv4.conf.lo.route_localnet=1"
            )
            iptables_cmds = []
            for dport in [dport_worker, dport_raylet]:
                iptables_cmds.append(
                    f"sudo iptables -t nat -C OUTPUT -d {head_ip_resolved} -p tcp "
                    f"--dport {dport} -j DNAT --to-destination {_RELAY_IP} 2>/dev/null || "
                    f"sudo iptables -t nat -A OUTPUT -d {head_ip_resolved} -p tcp "
                    f"--dport {dport} -j DNAT --to-destination {_RELAY_IP}"
                )
            with log_phase(logger, f"Setting up iptables DNAT for head ports on {ip}"):
                ssh.run_commands([route_localnet_cmd] + iptables_cmds)

    # --- Tailnet (tailscale) workers: start the per-node relays ---
    # The head relay must be up before any tailnet worker registers
    # (GCS health-checks the worker raylet immediately), and a worker's
    # relay must be up before its `ray start` (which dials the head GCS
    # through it). Relay listeners never collide with the local Ray's
    # wildcard binds — port blocks are globally unique — so ordering
    # against the local Ray processes doesn't matter.
    tailnet_allocs: dict[tuple[str, str, int], TailnetWorkerAlloc] = {}
    if config.tailnet_config is not None:
        tailnet_allocs = allocate_tailnet_workers(config, assignments)
    if tailnet_allocs:
        with log_phase(logger, f"Starting tailnet relay on head {config.head_ip}"):
            start_relay(_make_ssh(config, config.head_ip),
                        build_relay_spec(config, tailnet_allocs, None))
        for ip in sorted({a.tailnet_ip for a in tailnet_allocs.values()}):
            ssh = _make_ssh(config, ip)
            ssh.wait_for_ssh()
            with log_phase(logger, f"Starting tailnet relay on {ip}"):
                start_relay(ssh, build_relay_spec(config, tailnet_allocs, ip))
        logger.info(f"Tailnet relays up on head + "
                    f"{len({a.tailnet_ip for a in tailnet_allocs.values()})} node(s)")

    # Group assignments by IP so we set up workers on the same machine
    # sequentially (avoids overwhelming sshd MaxStartups), while still
    # parallelizing across different machines.
    by_ip: dict[str, list[NodeAssignment]] = defaultdict(list)
    for a in assignments:
        by_ip[a.ip].append(a)

    def _setup_ip_workers(ip: str, ip_assignments: list[NodeAssignment]):
        """Set up all workers on a single IP sequentially."""
        failed_local = []
        for i, a in enumerate(ip_assignments):
            key = (a.ip, a.node_type.name, a.worker_index)
            tc = worker_tunnel_configs.get(key)
            ta = tailnet_allocs.get(key)
            try:
                setup_worker_node(config, a, tunnel_config=tc,
                                  skip_ray_stop=(i > 0),
                                  head_ip=head_ip_resolved if tc else None,
                                  tailnet_alloc=ta)
                logger.info(f"Worker {a.ip} ({a.node_type.name}-{a.worker_index}) ready")
            except Exception as e:
                logger.error(f"Worker {a.ip} ({a.node_type.name}-{a.worker_index}) FAILED: {e}")
                failed_local.append(a)
        return failed_local

    with ThreadPoolExecutor(max_workers=len(by_ip)) as pool:
        futures = [
            pool.submit(_setup_ip_workers, ip, ip_assignments)
            for ip, ip_assignments in by_ip.items()
        ]

        failed = []
        for fut in as_completed(futures):
            failed.extend(fut.result())

    if failed:
        _stop_failed_worker_tunnels(tunnel_mgr, failed, worker_tunnel_configs)
        raise RuntimeError(
            f"{len(failed)} worker(s) failed to set up: "
            f"{', '.join(f'{a.ip} ({a.node_type.name})' for a in failed)}"
        )

    logger.info(f"Cluster '{config.cluster_name}' up: 1 head + {len(assignments)} workers")
    return tunnel_mgr


# ------------------------------------------------------------------
# Teardown
# ------------------------------------------------------------------


def tear_down_head_node(config: ClusterConfig) -> None:
    ssh = _make_ssh(config, config.head_ip)

    with log_phase(logger, f"Stopping Ray head on {config.head_ip}"):
        ssh.run_script(
            config.head_env_commands
            + config.head_teardown_commands
            + ["ray stop"],
            check=False,
        )


def tear_down_worker_node(config: ClusterConfig, assignment: NodeAssignment) -> None:
    ip = assignment.ip
    nt = assignment.node_type
    ssh = _make_ssh(config, ip)

    docker_config = nt.docker or config.global_docker
    is_head_ip = (ip == config.head_ip)

    if docker_config:
        container_name = f"{docker_config.container_name}-{assignment.worker_index}"
        docker_config_copy = replace(docker_config, container_name=container_name)
        docker_mgr = DockerManager(ssh, docker_config_copy)

        with log_phase(logger, f"Stopping Ray in container '{container_name}' on {ip}"):
            docker_mgr.exec_script(nt.worker_env_commands + ["ray stop"])

        with log_phase(logger, f"Stopping and removing container '{container_name}' on {ip}"):
            docker_mgr.stop_container()
            ssh.run(f"{docker_config_copy.engine} rm -f {container_name}", check=False)
    elif not is_head_ip:
        with log_phase(logger, f"Stopping Ray on {ip} ({nt.name})"):
            ssh.run_script(nt.worker_env_commands + ["ray stop"], check=False)
    else:
        logger.debug(f"Skipping ray stop on {ip} ({nt.name}) — will be stopped with head node")


def tear_down_cluster(
    config: ClusterConfig,
    tunnel_mgr: TunnelManager | None = None,
) -> None:
    assignments = assign_nodes(config)

    if assignments:
        by_ip: dict[str, list[NodeAssignment]] = defaultdict(list)
        for a in assignments:
            by_ip[a.ip].append(a)

        def _teardown_ip_workers(ip: str, ip_assignments: list[NodeAssignment]):
            failed_local = []
            for a in ip_assignments:
                try:
                    logger.info(f"Tearing down worker {a.ip} ({a.node_type.name}-{a.worker_index})")
                    tear_down_worker_node(config, a)
                    logger.info(f"Worker {a.ip} ({a.node_type.name}-{a.worker_index}) torn down")
                except Exception as e:
                    logger.error(f"Worker {a.ip} ({a.node_type.name}-{a.worker_index}) teardown FAILED: {e}")
                    failed_local.append(a)
            return failed_local

        with ThreadPoolExecutor(max_workers=len(by_ip)) as pool:
            futures = [
                pool.submit(_teardown_ip_workers, ip, ip_assignments)
                for ip, ip_assignments in by_ip.items()
            ]

            failed = []
            for fut in as_completed(futures):
                failed.extend(fut.result())

        if failed:
            logger.warning(
                f"{len(failed)} worker(s) failed to tear down: "
                f"{', '.join(f'{a.ip} ({a.node_type.name})' for a in failed)}"
            )

    # Stop SSH tunnels
    if tunnel_mgr is not None:
        tunnel_mgr.stop_all()
    else:
        # Kill any orphaned tunnels from a previous run
        from chia.cluster.tunnel import kill_orphaned_tunnels
        kill_ips = [
            ip for ip in config.worker_ips
            if (tc := config.get_tunnel_config(ip)) is not None and tc.kill_orphaned_tunnels
        ]
        if kill_ips:
            kill_orphaned_tunnels(kill_ips)

    # Stop tailnet relays (workers + head), best effort.
    if config.tailnet_config is not None:
        tailnet_ips = sorted({ip for ip in config.worker_ips if config.is_tailnet(ip)})
        for ip in tailnet_ips + [config.head_ip]:
            try:
                with log_phase(logger, f"Stopping tailnet relay on {ip}"):
                    stop_relay(_make_ssh(config, ip))
            except Exception as e:
                logger.warning(f"Failed to stop tailnet relay on {ip}: {e}")

    tear_down_head_node(config)
    logger.info(f"Cluster '{config.cluster_name}' torn down")
