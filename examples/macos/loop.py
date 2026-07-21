"""Smallest possible CHIA loop for macOS.

Bring the cluster up first (see cluster.yaml in this directory), then:

    python examples/macos/loop.py

Each call runs inside the mac_native worker container. On an Apple-silicon
Mac the reported machine should be aarch64 — if you see x86_64, docker
pulled the emulated image variant and tasks will start killing the node.
"""

import platform

from chia.base.ChiaFunction import ChiaFunction, get


@ChiaFunction(resources={"mac_native": 1})
def report_worker(iteration: int) -> str:
    import platform
    return f"iteration {iteration}: worker container is {platform.machine()}"


def main():
    print(f"driver machine: {platform.machine()}")
    for i in range(3):
        print(get(report_worker.chia_remote(i)))


if __name__ == "__main__":
    main()
