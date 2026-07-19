"""Smoke-test flow for the tailscale example cluster.

Runs a ChiaFunction pinned to the tailscale worker, shipping ~1 MiB of
task arguments to it and ~1 MiB of results back — exercising GCS
registration, scheduling, and object transfer in both directions across
the tailnet.

Run from the head machine (after ``chia up examples/tailscale/cluster.yaml``):

    conda activate chia_env
    python examples/tailscale/flow.py
"""
import os
import socket

from chia.base.ChiaFunction import ChiaFunction, get


@ChiaFunction(resources={"tailscale_worker": 1})
def whereami(payload: bytes) -> dict:
    return {
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "received_bytes": len(payload),
        "reply": b"pong" * (256 * 1024),  # ~1 MiB back through the object store
    }


def main():
    payload = b"ping" * (256 * 1024)  # ~1 MiB out to the worker
    print(f"driver on {socket.gethostname()}, sending {len(payload)} bytes...")
    result = get(whereami.chia_remote(payload))
    reply = result.pop("reply")
    print(f"remote result: {result}")
    print(f"reply payload: {len(reply)} bytes")
    assert result["received_bytes"] == len(payload)
    assert result["hostname"] != socket.gethostname(), \
        "task ran on the head, not the tailscale worker?"
    print("tailscale cluster smoke test PASSED")


if __name__ == "__main__":
    main()
