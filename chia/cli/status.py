"""``chia status [NAME]`` — ``ray status`` for a cluster that chia brought up, found by
name in chia's records (see chia/cluster/records.py). ``chia ray status`` keeps Ray's
exact behavior (see chia/cli/main.py)."""

from __future__ import annotations

import subprocess
import sys

from chia.cluster import records


def run_status(status_argv, cluster=None) -> None:
    """``ray status`` for the cluster named first in *status_argv*, for *cluster* (a
    cluster file), or for the only chia cluster that is up; one line if it is not up."""
    if "--help" in status_argv or "-h" in status_argv:
        from chia.cli.ray_passthrough import cmd_ray_passthrough
        cmd_ray_passthrough(["status", *status_argv], cluster=cluster)
    name = status_argv[0] if status_argv and not status_argv[0].startswith("-") else None
    ray_argv = status_argv[1:] if name else status_argv

    if cluster is not None:
        from chia.cluster.config import ConfigError, build_config, load_raw_config
        try:
            config = build_config(load_raw_config(cluster))
        except ConfigError as e:
            sys.exit(f"chia: {e}")
        name, address = config.cluster_name, config.head_ray_address
    elif name is not None:
        record = records.load(name)
        if record is None:
            sys.exit(f"No chia cluster named '{name}'.")
        address = record["head_address"]
    else:
        up = [r for r in records.load_all() if records.is_up(r["head_address"])]
        if not up:
            sys.exit("No chia cluster is up.")
        if len(up) > 1:
            sys.exit(f"Several chia clusters are up: {', '.join(r['name'] for r in up)}. "
                     f"Give one: chia status <name>")
        name, address = up[0]["name"], up[0]["head_address"]

    if not records.is_up(address):
        sys.exit(f"Cluster '{name}' is not up.")
    print("Chia: forwarding to Ray's CLI\n", file=sys.stderr)
    sys.exit(subprocess.run(["ray", "status", "--address", address, *ray_argv]).returncode)
