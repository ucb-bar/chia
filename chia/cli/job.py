"""``chia job`` commands — stop (optionally killing tracked subprocesses) plus thin pass-throughs to ``ray job``."""

from __future__ import annotations

import subprocess
import sys


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


def cmd_job_passthrough(job_command: str, ray_args: list[str]) -> None:
    """Forward ``chia job <cmd> ...`` verbatim to ``ray job <cmd> ...``."""
    result = subprocess.run(["ray", "job", job_command, *ray_args])
    sys.exit(result.returncode)
