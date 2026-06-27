from __future__ import annotations

import socket
import sys

from chia.cluster.config import (
    ClusterConfig, ConfigError, NodeAssignment,
    assign_nodes, build_config, load_raw_config,
    parse_aws_nodes, parse_gcp_nodes,
    _expand_node_placeholders, _inject_cloud_tunnel_overrides,
)
from chia.cluster.log import get_logger, setup_logging
from chia.cluster.node_setup import (
    add_nodes_to_cluster, allocate_worker_tunnels, bring_up_cluster,
    build_head_script, build_worker_script,
    compute_new_assignments, query_ray_cluster_nodes,
)


def _print_aws_plan(aws_nodes, region: str):
    print(f"\nAWS EC2 Nodes (region: {region}):")
    for name, cfg in aws_nodes.items():
        print(f"  {name}: {cfg.count}x {cfg.InstanceType} "
              f"(KeyName={cfg.KeyName}, ImageId={cfg.ImageId})")
        if cfg.extra_args:
            print(f"    extra: {cfg.extra_args}")
    print()


def _print_gcp_plan(gcp_nodes, project: str, zone: str):
    print(f"\nGCP Compute Engine Nodes (project: {project}, default zone: {zone}):")
    for name, cfg in gcp_nodes.items():
        print(f"  {name}: {cfg.count}x {cfg.machine_type} "
              f"(zone={cfg.zone or zone}, image={cfg.image}"
              f"{', spot' if cfg.spot else ''})")
        if cfg.extra_args:
            print(f"    extra: {cfg.extra_args}")
    print()


def _gcp_default_auth(raw) -> tuple[str | None, str | None]:
    """Global SSH user/key, used to derive GCP metadata SSH keys at launch."""
    auth = raw.get("auth", {})
    return auth.get("ssh_user"), auth.get("ssh_private_key")


def _print_plan(config: ClusterConfig, assignments: list[NodeAssignment],
                show_scripts: bool = False):

    print(f"Cluster: {config.cluster_name}")
    print(f"Head:    {config.head_ip}")
    print(f"Workers:")
    for a in assignments:
        docker_str = f" [{a.node_type.docker.engine}: {a.node_type.docker.image}]" if a.node_type.docker else ""
        tunnel_str = " [tunneled]" if config.is_tunneled(a.ip) else ""
        print(f"  {a.ip} -> {a.node_type.name} "
              f"(resources: {a.resources}){docker_str}{tunnel_str}")

    worker_tunnels = allocate_worker_tunnels(config, assignments)
    if worker_tunnels:
        head_ip_resolved = socket.gethostbyname(config.head_ip)
        print(f"\nSSH Tunnels (tool traffic via {head_ip_resolved}):")
        for (ip, nt_name, idx), tc in worker_tunnels.items():
            auth = config.get_ssh_auth(ip)
            print(f"  {ip} ({auth.ssh_user}) {nt_name}-{idx} via {tc.tunnel_ip}")
            print(f"    GCS: {tc.tunnel_ip}:{tc.gcs_tunnel_port} -> head :6379")
            print(f"    Ray: node-mgr={tc.ray_node_manager_port}, obj-mgr={tc.ray_object_manager_port}, "
                  f"workers={tc.ray_worker_port_min}-{tc.ray_worker_port_max}")
            print(f"    Tools: {tc.tool_port_min}-{tc.tool_port_max} (on {head_ip_resolved})")
    print()

    if show_scripts:
        head_script = build_head_script(config)
        print(f"--- Script for head node ({config.head_ip}) ---")
        for line in head_script:
            print(f"  {line}")
        print()

        head_ip_for_scripts = socket.gethostbyname(config.head_ip) if worker_tunnels else None
        for a in assignments:
            tc = config.get_tunnel_config(a.ip)
            worker_script = build_worker_script(
                config, a, tunnel_config=tc,
                head_ip=head_ip_for_scripts if tc else None,
            )
            docker_str = f" [{a.node_type.docker.engine}: {a.node_type.docker.container_name}]" if a.node_type.docker else ""
            tunnel_str = " [tunneled]" if config.is_tunneled(a.ip) else ""
            print(f"--- Script for worker {a.ip} ({a.node_type.name}){docker_str}{tunnel_str} ---")
            for line in worker_script:
                print(f"  {line}")
            print()


def _print_add_plan(config: ClusterConfig,
                    all_assignments: list[NodeAssignment],
                    new_assignments: list[NodeAssignment]):
    new_ids = set(id(a) for a in new_assignments)
    print(f"Cluster: {config.cluster_name} (--add mode)")
    print(f"Head:    {config.head_ip} (already running)")
    print(f"Workers:")
    for a in all_assignments:
        status = "NEW" if id(a) in new_ids else "EXISTS"
        docker_str = (f" [{a.node_type.docker.engine}: {a.node_type.docker.image}]"
                      if a.node_type.docker else "")
        print(f"  [{status}] {a.ip} -> {a.node_type.name} "
              f"(resources: {a.resources}){docker_str}")
    print(f"\nWill add {len(new_assignments)} new worker(s), "
          f"skip {len(all_assignments) - len(new_assignments)} existing worker(s)")
    print()


def _cmd_up_add(args, raw, aws_result, gcp_result, logger):
    """Handle ``chia up --add``: discover existing cloud instances, provision
    any that are missing, query the running cluster, and only set up
    workers that don't already exist."""

    cluster_name = raw.get("cluster_name", "default")

    aws_ip_map: dict[str, list[str]] = {}
    aws_new_ip_map: dict[str, list[str]] = {}
    gcp_ip_map: dict[str, list[str]] = {}
    gcp_new_ip_map: dict[str, list[str]] = {}

    # --- AWS: discover + provision anything missing ---
    if aws_result is not None:
        aws_nodes, region = aws_result
        from chia.cluster.aws_nodes import (
            discover_aws_nodes, provision_missing_aws_nodes,
        )
        existing = discover_aws_nodes(cluster_name, region)
        missing = {name: cfg.count - len(existing.get(name, []))
                   for name, cfg in aws_nodes.items()
                   if cfg.count - len(existing.get(name, [])) > 0}
        if missing:
            print("\nAWS EC2 Nodes to provision (--add):")
            for name, n in missing.items():
                cfg = aws_nodes[name]
                print(f"  {name}: +{n}x {cfg.InstanceType} "
                      f"(existing: {len(existing.get(name, []))}, target: {cfg.count})")
            print()

        if args.dry_run:
            if missing:
                logger.info("Dry run ends here: can't simulate cluster plan "
                            "without IPs for the missing AWS instances.")
                return
            aws_ip_map = existing
        else:
            if missing and not args.yes:
                if input("Provision missing AWS instances? [y/N] ").lower() != "y":
                    print("Aborted.")
                    return
            try:
                aws_ip_map, aws_new_ip_map = provision_missing_aws_nodes(
                    cluster_name, aws_nodes, region)
                if aws_new_ip_map:
                    logger.info(f"New AWS nodes provisioned: {aws_new_ip_map}")
            except Exception as e:
                logger.error(f"AWS provisioning failed: {e}")
                sys.exit(1)

    # --- GCP: discover + provision anything missing ---
    if gcp_result is not None:
        gcp_nodes, project, zone, network, subnetwork = gcp_result
        from chia.cluster.gcp_nodes import (
            discover_gcp_nodes, provision_missing_gcp_nodes,
        )
        existing = discover_gcp_nodes(cluster_name, project)
        missing = {name: cfg.count - len(existing.get(name, []))
                   for name, cfg in gcp_nodes.items()
                   if cfg.count - len(existing.get(name, [])) > 0}
        if missing:
            print("\nGCP Compute Engine Nodes to provision (--add):")
            for name, n in missing.items():
                cfg = gcp_nodes[name]
                print(f"  {name}: +{n}x {cfg.machine_type} "
                      f"(existing: {len(existing.get(name, []))}, target: {cfg.count})")
            print()

        if args.dry_run:
            if missing:
                logger.info("Dry run ends here: can't simulate cluster plan "
                            "without IPs for the missing GCP instances.")
                return
            gcp_ip_map = existing
        else:
            if missing and not args.yes:
                if input("Provision missing GCP instances? [y/N] ").lower() != "y":
                    print("Aborted.")
                    return
            du, dk = _gcp_default_auth(raw)
            try:
                gcp_ip_map, gcp_new_ip_map = provision_missing_gcp_nodes(
                    cluster_name, gcp_nodes, project, zone,
                    network=network, subnetwork=subnetwork,
                    default_ssh_user=du, default_ssh_private_key=dk)
                if gcp_new_ip_map:
                    logger.info(f"New GCP nodes provisioned: {gcp_new_ip_map}")
            except Exception as e:
                logger.error(f"GCP provisioning failed: {e}")
                sys.exit(1)

    ip_map = {**aws_ip_map, **gcp_ip_map}
    if (aws_result is not None or gcp_result is not None) and not ip_map:
        logger.error(
            "No cloud instances exist for this cluster. "
            "Run 'chia up' (without --add) first to provision the initial cluster.")
        sys.exit(1)

    if ip_map:
        raw = _expand_node_placeholders(raw, ip_map)
        if aws_result is not None:
            _inject_cloud_tunnel_overrides(raw, aws_ip_map, aws_result[0])
        if gcp_result is not None:
            _inject_cloud_tunnel_overrides(raw, gcp_ip_map, gcp_result[0])

    # --- Build config and assignments ---
    try:
        config = build_config(raw)
    except ConfigError as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    try:
        assignments = assign_nodes(config)
    except ConfigError as e:
        logger.error(f"Node assignment error: {e}")
        sys.exit(1)

    # --- Run setup on newly provisioned instances only ---
    _run_cloud_setup(aws_result, aws_new_ip_map, gcp_result, gcp_new_ip_map,
                     config, logger)

    # --- Query existing cluster ---
    existing_nodes = query_ray_cluster_nodes(config)
    if existing_nodes is None:
        logger.error(
            "No running Ray cluster found on head %s. "
            "Run 'chia up' (without --add) first.", config.head_ip)
        sys.exit(1)

    alive_count = sum(1 for n in existing_nodes if n.get("Alive"))
    logger.info(f"Found running cluster with {alive_count} alive node(s)")

    # Compute tunnel configs so tunneled workers are matched by their
    # tunnel IP (127.0.0.x) rather than their real public IP.
    tunnel_configs = allocate_worker_tunnels(config, assignments)
    new_assignments = compute_new_assignments(
        assignments, existing_nodes, tunnel_configs=tunnel_configs)
    _print_add_plan(config, assignments, new_assignments)

    if args.dry_run:
        logger.info("Dry run complete. No changes made.")
        return

    if not new_assignments:
        logger.info("All desired workers already exist. Nothing to add.")
        return

    if not args.yes:
        if input("Proceed? [y/N] ").lower() != "y":
            print("Aborted.")
            return

    try:
        add_nodes_to_cluster(config, new_assignments)
    except Exception as e:
        logger.error(f"Add nodes failed: {e}")
        sys.exit(1)


def _run_cloud_setup(aws_result, aws_ip_map, gcp_result, gcp_ip_map,
                     config, logger):
    """Run per-cloud setup commands on the given (freshly provisioned) IPs."""
    if aws_result is not None and aws_ip_map:
        aws_nodes = aws_result[0]
        if any(cfg.effective_setup_commands
               for name, cfg in aws_nodes.items() if name in aws_ip_map):
            from chia.cluster.aws_nodes import run_aws_setup
            try:
                run_aws_setup(aws_nodes, aws_ip_map, config.get_ssh_auth)
            except Exception as e:
                logger.error(f"AWS setup failed: {e}")
                logger.error("WARNING: cloud instances are still running. "
                             "Run 'chia down' to terminate them.")
                sys.exit(1)

    if gcp_result is not None and gcp_ip_map:
        gcp_nodes = gcp_result[0]
        if any(cfg.effective_setup_commands
               for name, cfg in gcp_nodes.items() if name in gcp_ip_map):
            from chia.cluster.gcp_nodes import run_gcp_setup
            try:
                run_gcp_setup(gcp_nodes, gcp_ip_map, config.get_ssh_auth)
            except Exception as e:
                logger.error(f"GCP setup failed: {e}")
                logger.error("WARNING: cloud instances are still running. "
                             "Run 'chia down' to terminate them.")
                sys.exit(1)


def cmd_up(args):
    logger = setup_logging(verbose=args.verbose)

    try:
        raw = load_raw_config(args.config_file)
    except ConfigError as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    # Parse cloud node sections (but don't provision yet)
    aws_result = None
    gcp_result = None
    try:
        aws_result = parse_aws_nodes(raw)
        gcp_result = parse_gcp_nodes(raw)
    except ConfigError as e:
        logger.error(f"Cloud node config error: {e}")
        sys.exit(1)

    # --add mode has its own flow: discover instead of provision
    if args.add:
        return _cmd_up_add(args, raw, aws_result, gcp_result, logger)

    # --- Normal chia up flow ---
    aws_ip_map: dict[str, list[str]] = {}
    gcp_ip_map: dict[str, list[str]] = {}
    provisioned = False  # True once real cloud instances are running

    if aws_result is not None:
        aws_nodes, region = aws_result
        _print_aws_plan(aws_nodes, region)
        if args.dry_run:
            # Placeholder IPs so dry-run scripts include the same port-pinning
            # and tunnel args a real `chia up` would emit. No boto3 calls.
            aws_ip_map = {
                name: [f"203.0.113.{i + 1}" for i in range(cfg.count)]
                for name, cfg in aws_nodes.items()
            }
        else:
            if not args.yes and input("Provision AWS instances? [y/N] ").lower() != "y":
                print("Aborted.")
                return
            from chia.cluster.aws_nodes import provision_aws_nodes
            try:
                aws_ip_map = provision_aws_nodes(
                    raw.get("cluster_name", "default"), aws_nodes, region)
                provisioned = True
                logger.info(f"AWS nodes provisioned: {aws_ip_map}")
            except Exception as e:
                logger.error(f"AWS provisioning failed: {e}")
                sys.exit(1)

    if gcp_result is not None:
        gcp_nodes, project, zone, network, subnetwork = gcp_result
        _print_gcp_plan(gcp_nodes, project, zone)
        if args.dry_run:
            # Placeholder IPs in a distinct range so AWS+GCP don't collide.
            gcp_ip_map = {
                name: [f"198.51.100.{i + 1}" for i in range(cfg.count)]
                for name, cfg in gcp_nodes.items()
            }
        else:
            if not args.yes and input("Provision GCP instances? [y/N] ").lower() != "y":
                print("Aborted.")
                return
            du, dk = _gcp_default_auth(raw)
            from chia.cluster.gcp_nodes import provision_gcp_nodes
            try:
                gcp_ip_map = provision_gcp_nodes(
                    raw.get("cluster_name", "default"), gcp_nodes, project, zone,
                    network=network, subnetwork=subnetwork,
                    default_ssh_user=du, default_ssh_private_key=dk)
                provisioned = True
                logger.info(f"GCP nodes provisioned: {gcp_ip_map}")
            except Exception as e:
                logger.error(f"GCP provisioning failed: {e}")
                if provisioned:
                    logger.error("WARNING: cloud instances are still running. "
                                 "Run 'chia down' to terminate them.")
                sys.exit(1)

    # Expand placeholders once over the merged map (the expander raises on
    # unknown @node refs, so AWS and GCP refs must be resolved together).
    ip_map = {**aws_ip_map, **gcp_ip_map}
    if ip_map:
        raw = _expand_node_placeholders(raw, ip_map)
        if aws_result is not None:
            _inject_cloud_tunnel_overrides(raw, aws_ip_map, aws_result[0])
        if gcp_result is not None:
            _inject_cloud_tunnel_overrides(raw, gcp_ip_map, gcp_result[0])

    def _warn_running():
        if provisioned:
            logger.error("WARNING: cloud instances are still running. "
                         "Run 'chia down' to terminate them.")

    try:
        config = build_config(raw)
    except ConfigError as e:
        logger.error(f"Config error: {e}")
        _warn_running()
        sys.exit(1)

    try:
        assignments = assign_nodes(config)
    except ConfigError as e:
        logger.error(f"Node assignment error: {e}")
        _warn_running()
        sys.exit(1)

    # Run cloud setup commands (install deps on fresh instances). Skip in
    # dry-run: ip_map there is placeholder IPs that don't resolve.
    if not args.dry_run:
        _run_cloud_setup(aws_result, aws_ip_map, gcp_result, gcp_ip_map,
                         config, logger)

    _print_plan(config, assignments, show_scripts=args.dry_run)

    if args.dry_run:
        logger.info("Dry run complete. No changes made.")
        return

    if not args.yes:
        if input("Proceed? [y/N] ").lower() != "y":
            print("Aborted.")
            return

    try:
        bring_up_cluster(config)
    except Exception as e:
        logger.error(f"Cluster bring-up failed: {e}")
        _warn_running()
        sys.exit(1)
