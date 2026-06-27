"""Helper script for testing SIGTERM-based subprocess cleanup.

Spawns a ChiaFunction task with a nested process tree, then blocks.
When the test sends SIGTERM to this process, the driver cleanup handler
(installed automatically by _get_registry) should call kill_all() on
the PID registry actor, killing all tracked subprocesses.

Usage:
    python sigterm_cleanup_helper.py <pid_file>
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
    with open(pid_file, "a") as f:
        f.write(f"{proc.pid}\n")
    while proc.poll() is None:
        time.sleep(1)


def main():
    pid_file = sys.argv[1]
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
                # Write driver PID so the test can SIGTERM us.
                with open(ready_file, "w") as f:
                    f.write(str(os.getpid()))
                break

    # Block until killed.  Don't call ray.get(ref) — we want the
    # SIGTERM handler to be the *only* cleanup path, not TaskCancelledError.
    try:
        time.sleep(3600)
    except (SystemExit, KeyboardInterrupt):
        pass


if __name__ == "__main__":
    main()
