"""``chia job stop`` — chia's augmented override of ``ray job stop`` (optionally
killing tracked subprocesses first). Every other ``chia job <cmd>`` proxies to
``ray job <cmd>`` (see chia/cli/main.py)."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys


def run_job_stop(stop_argv, cluster=None) -> None:
    """Parse the args after ``chia job stop`` and run the augmented stop.

    This is the override entrypoint dispatched from ``main()``; it owns its own
    argument parsing (so ``chia job stop --help`` shows chia's flags, not ray's)
    and, if *cluster* is given, pins ``RAY_ADDRESS`` so both the ``ray.init``
    below and the ``ray job stop`` subprocess target that cluster.
    """
    parser = argparse.ArgumentParser(
        prog="chia job stop",
        description="Stop a Ray job (optionally kill tracked subprocesses first)")
    parser.add_argument("job_id", help="Ray job ID to stop")
    parser.add_argument(
        "--kill-tracked-pids", action="store_true",
        help="Kill tracked subprocesses (via the PID registry) before stopping the job")
    parser.add_argument(
        "--grace-period", type=int, default=25,
        help="Seconds to wait for each tracked subprocess to exit after SIGTERM "
             "before escalating to SIGKILL (only used with --kill-tracked-pids; "
             "default: 25)")
    args = parser.parse_args(stop_argv)

    if cluster is not None:
        from chia.cli.ray_passthrough import resolve_cluster_address
        os.environ["RAY_ADDRESS"] = resolve_cluster_address(cluster)

    cmd_job_stop(args)


def cmd_job_stop(args) -> None:
    """``ray job stop``; with ``--kill-tracked-pids``, kill tracked subprocesses first.

    By default this is a thin wrapper over ``ray job stop``. Only when
    ``--kill-tracked-pids`` is passed do we look up the PID registry actor and
    fire ``kill_all(grace)``, which blocks until every tracked subprocess has
    exited (escalating to SIGKILL only after ``grace`` seconds), then stop the
    job.
    """
    if args.kill_tracked_pids:
        import ray

        ray.init(ignore_reinit_error=True)

        # Look up the PID registry actor and fire kill_all().
        from chia.base.pid_registry import _REGISTRY_ACTOR_NAME, _REGISTRY_NAMESPACE
        try:
            registry = ray.get_actor(_REGISTRY_ACTOR_NAME, namespace=_REGISTRY_NAMESPACE)
        except ValueError:
            print("PID registry actor not found — no tracked subprocesses to kill.")
            registry = None

        grace = args.grace_period

        if registry is not None:
            try:
                # kill_all blocks until every tracked process is confirmed
                # dead (escalating to SIGKILL after `grace` seconds), so the
                # CLI grace drives the actual kill grace.  Give the RPC a
                # margin beyond that ceiling; no extra sleep is needed.
                n = ray.get(registry.kill_all.remote(grace), timeout=grace + 10)
                print(f"kill_all: killed {n} tracked subprocess(es).")
            except Exception as exc:
                print(f"kill_all failed: {exc}", file=sys.stderr)

    # Now let Ray clean up the job/driver.
    job_id = args.job_id
    print(f"Running: ray job stop {job_id}")
    result = subprocess.run(["ray", "job", "stop", job_id])
    sys.exit(result.returncode)
