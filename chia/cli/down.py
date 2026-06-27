from __future__ import annotations

import sys

from chia.cluster.config import (
    ClusterConfig, ConfigError, NodeAssignment,
    assign_nodes, build_config, load_raw_config,
    parse_aws_nodes, parse_gcp_nodes,
    _expand_node_placeholders, _inject_cloud_tunnel_overrides,
)
from chia.cluster.log import setup_logging
from chia.cluster.node_setup import tear_down_cluster


def _print_teardown_plan(config: ClusterConfig, assignments: list[NodeAssignment],
                         cloud_instance_count: int = 0):
    print(f"Cluster: {config.cluster_name}")
    print(f"Head:    {config.head_ip}")
    print(f"Workers to tear down:")
    for a in assignments:
        docker_str = f" [{a.node_type.docker.engine}: {a.node_type.docker.container_name}-{a.worker_index}]" if a.node_type.docker else ""
        tunnel_str = " [tunneled]" if config.is_tunneled(a.ip) else ""
        print(f"  {a.ip} -> {a.node_type.name}{docker_str}{tunnel_str}")

    tunneled_ips = [ip for ip in config.worker_ips if config.is_tunneled(ip)]
    if tunneled_ips:
        print(f"SSH tunnels to kill: {tunneled_ips}")

    if cloud_instance_count:
        print(f"Cloud instances to terminate: {cloud_instance_count}")
    print()


def cmd_down(args):
    logger = setup_logging(verbose=args.verbose)

    try:
        raw = load_raw_config(args.config_file)
    except ConfigError as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    # Parse cloud node sections and discover running instances
    aws_result = None
    gcp_result = None
    try:
        aws_result = parse_aws_nodes(raw)
        gcp_result = parse_gcp_nodes(raw)
    except ConfigError as e:
        logger.error(f"Cloud node config error: {e}")
        sys.exit(1)

    cluster_name = raw.get("cluster_name", "default")

    aws_region = None
    gcp_project = None
    aws_ip_map: dict[str, list[str]] = {}
    gcp_ip_map: dict[str, list[str]] = {}

    if aws_result is not None:
        aws_nodes, aws_region = aws_result
        from chia.cluster.aws_nodes import discover_aws_nodes
        aws_ip_map = discover_aws_nodes(cluster_name, aws_region)

    if gcp_result is not None:
        gcp_nodes, gcp_project, _zone, _net, _sub = gcp_result
        from chia.cluster.gcp_nodes import discover_gcp_nodes
        gcp_ip_map = discover_gcp_nodes(cluster_name, gcp_project)

    ip_map = {**aws_ip_map, **gcp_ip_map}
    cloud_instance_count = sum(len(ips) for ips in ip_map.values())
    if ip_map:
        raw = _expand_node_placeholders(raw, ip_map)
        if aws_result is not None:
            _inject_cloud_tunnel_overrides(raw, aws_ip_map, aws_result[0])
        if gcp_result is not None:
            _inject_cloud_tunnel_overrides(raw, gcp_ip_map, gcp_result[0])

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

    _print_teardown_plan(config, assignments,
                         cloud_instance_count=cloud_instance_count)

    if not args.yes:
        if input("Tear down this cluster? [y/N] ").lower() != "y":
            print("Aborted.")
            return

    # Ray teardown first (graceful stop on workers, kill tunnels)
    try:
        tear_down_cluster(config)
    except Exception as e:
        logger.error(f"Cluster teardown failed: {e}")
        if aws_result is None and gcp_result is None:
            sys.exit(1)
        logger.warning("Proceeding with cloud instance termination anyway.")

    # Terminate AWS instances and clean up the security group
    if aws_result is not None:
        from chia.cluster.aws_nodes import teardown_aws_nodes, cleanup_security_group
        try:
            terminated = teardown_aws_nodes(cluster_name, aws_region)
            if terminated:
                logger.info(f"Terminated {len(terminated)} AWS instance(s)")
        except Exception as e:
            logger.error(f"AWS instance termination failed: {e}")
        try:
            cleanup_security_group(cluster_name, aws_region)
        except Exception as e:
            logger.error(f"Security group cleanup failed: {e}")

    # Delete GCP instances and clean up the firewall rules
    if gcp_result is not None:
        from chia.cluster.gcp_nodes import teardown_gcp_nodes, cleanup_firewall
        try:
            deleted = teardown_gcp_nodes(cluster_name, gcp_project)
            if deleted:
                logger.info(f"Deleted {len(deleted)} GCP instance(s)")
        except Exception as e:
            logger.error(f"GCP instance deletion failed: {e}")
        try:
            cleanup_firewall(cluster_name, gcp_project)
        except Exception as e:
            logger.error(f"Firewall cleanup failed: {e}")
