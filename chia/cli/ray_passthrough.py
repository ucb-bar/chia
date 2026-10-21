"""``chia ray ...`` — forward a command verbatim to the ``ray`` CLI.

``chia ray <args...>`` runs ``ray <args...>``, so every current and future
``ray`` command (and its ``--help``) works through ``chia`` without being
enumerated here.  Using an explicit ``ray`` gateway — rather than an implicit
fallback on unknown commands — keeps chia's own ``up``/``down`` from shadowing
``ray up``/``ray down``, and keeps ``ray``'s full help reachable.

An optional chia-level ``--chia-cluster`` — a cluster file, or the name of a
cluster that chia brought up, accepted either before or after the ray command —
pins ``RAY_ADDRESS`` to that cluster's head GCS address (``head_ip:port``).  On a host running
more than one cluster this is what lets ``chia ray status --chia-cluster
b.yaml`` target cluster *b* instead of whichever Ray instance ``ray``'s own
auto-discovery would pick.
"""

from __future__ import annotations

import os
import subprocess
import sys


def resolve_cluster(cluster: str) -> tuple[str, str]:
    """The name and head GCS address (``head_ip:port``) of *cluster*: a cluster file, or
    the name of a cluster that chia brought up (see chia/cluster/records.py)."""
    if os.path.isfile(cluster):
        from chia.cluster.config import build_config, load_raw_config

        config = build_config(load_raw_config(cluster))
        return config.cluster_name, config.head_ray_address
    from chia.cluster import records

    record = records.load(cluster)
    if record is None:
        raise ValueError(f"no cluster file or chia cluster named '{cluster}'")
    return record["name"], record["head_address"]


def resolve_cluster_address(cluster: str) -> str:
    """The head GCS address (``head_ip:port``) of *cluster*, a cluster file or name."""
    return resolve_cluster(cluster)[1]


def cmd_ray_passthrough(ray_argv: list[str], cluster: str | None = None) -> None:
    """Exec ``ray <ray_argv>``; if *cluster* is given, pin ``RAY_ADDRESS`` first.

    Whether the command is valid is Ray's concern, not chia's: an unknown
    command makes ``ray`` print its own error and usage and exit non-zero, and
    that exit code is propagated here.  The only failure chia explains itself is
    ``ray`` not being on PATH — that is the case where pointing at ``chia
    --help`` (maybe a chia-native command was meant) is actually useful.
    """
    env = os.environ.copy()
    if cluster is not None:
        try:
            env["RAY_ADDRESS"] = resolve_cluster_address(cluster)
        except Exception as e:
            sys.exit(f"chia: could not resolve --chia-cluster {cluster!r}: {e}")

    # stderr so it never contaminates the forwarded command's stdout (pipes/JSON).
    print(f"Chia: forwarding to Ray's CLI\n", file=sys.stderr)
    if ("--help" in ray_argv) or ("-h" in ray_argv):
        print(f"Chia Ray passthrough options:\n  --chia-cluster  None   A chia cluster yaml file, or a cluster name from `chia clusters`\n")
    try:
        result = subprocess.run(["ray", *ray_argv], env=env)
    except FileNotFoundError:
        print("chia: 'ray' was not found on PATH — is Ray installed in this "
              "environment?", file=sys.stderr)
        sys.exit(127)
    sys.exit(result.returncode)
