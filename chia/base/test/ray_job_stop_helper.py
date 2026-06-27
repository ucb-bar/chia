"""Helper script submitted as a Ray job for testing ray job stop cleanup.

Dispatches a ChiaFunction that spawns a nested process tree (child ->
grandchild -> great-grandchild) with start_new_session=True, writes all
PIDs to a file, then blocks until cancelled.

Usage:
    ray job submit --working-dir . -- python chia/base/test/ray_job_stop_helper.py <pid_file>
"""

import os
import subprocess
import sys
import time

import ray

from chia.base.ChiaFunction import ChiaFunction


@ChiaFunction()
def long_running_nested(pid_file: str) -> None:
    """Spawn a 3-level process tree and block."""
    # Great-grandchild: sleep
    # Grandchild: inner bash
    # Child: outer bash (process group leader)
    script = (
        "bash -c '"
        "  sleep 3600 & echo $! >> {pf};  "
        "  echo $$ >> {pf};  "
        "  wait"
        "' & "
        "echo $! >> {pf}; "
        "echo $$ >> {pf}; "
        "wait"
    ).format(pf=pid_file)
    proc = subprocess.Popen(
        ["bash", "-c", script],
        start_new_session=True,
    )
    # Write the outer Popen PID too (the one tracked by the hook).
    with open(pid_file, "a") as f:
        f.write(f"{proc.pid}\n")
    # Poll like hammer_syn_node — gives TaskCancelledError a chance to land.
    while proc.poll() is None:
        time.sleep(1)


def main():
    pid_file = sys.argv[1]
    # Signal to the test that the job has started.
    ready_file = pid_file + ".ready"

    ray.init(address="auto")

    ref = long_running_nested.chia_remote(pid_file)
    # Wait for PIDs to appear, then signal readiness.
    for _ in range(30):
        time.sleep(1)
        if os.path.exists(pid_file):
            with open(pid_file) as f:
                lines = [l.strip() for l in f.readlines() if l.strip()]
            if len(lines) >= 4:
                with open(ready_file, "w") as f:
                    f.write("ready")
                break

    # Block until the job is stopped.
    try:
        ray.get(ref)
    except Exception:
        pass


if __name__ == "__main__":
    main()
