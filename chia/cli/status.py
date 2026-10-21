"""``chia status`` — ``ray status`` for a cluster that chia brought up: the one that
``--chia-cluster`` gives (a cluster file, or a name from chia's records, see
chia/cluster/records.py), or else the only one that is up. ``chia ray status`` keeps
Ray's exact behavior (see chia/cli/main.py)."""

from __future__ import annotations

import subprocess
import sys

from chia.cluster import records


def run_status(status_argv, cluster=None) -> None:
    """``ray status`` for *cluster* (a cluster file or name), or for the only chia cluster
    that is up; one line if it is not up. With ``--help``, or an ``--address`` that the
    user gives, Ray's own ``ray status``."""
    from chia.cli.ray_passthrough import cmd_ray_passthrough, resolve_cluster

    if any(a in ("--help", "-h") or a.split("=", 1)[0] == "--address" for a in status_argv):
        cmd_ray_passthrough(["status", *status_argv], cluster=cluster)

    if cluster is not None:
        try:
            name, address = resolve_cluster(cluster)
        except Exception as e:
            sys.exit(f"chia: {e}")
    else:
        up = [r for r in records.load_all() if records.is_up(r["head_address"])]
        if not up:
            sys.exit("No chia cluster is up.")
        if len(up) > 1:
            sys.exit(f"Several chia clusters are up: {', '.join(r['name'] for r in up)}. "
                     f"Give one: chia status --chia-cluster <name>")
        name, address = up[0]["name"], up[0]["head_address"]

    if not records.is_up(address):
        sys.exit(f"Cluster '{name}' is not up.")
    print("Chia: forwarding to Ray's CLI\n", file=sys.stderr)
    sys.exit(subprocess.run(["ray", "status", "--address", address, *status_argv]).returncode)
