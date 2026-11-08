"""CPU-only regression for 64 parents waiting on the same dispatch proxy.

Run against a tunnelled cluster with at least 66 live worker raylets::

    python chia/base/test/test_dispatch_proxy_concurrency_live.py --address auto

All parents reach a barrier before submitting children. A 64-call proxy then
deadlocks; parents and children complete with the larger concurrency limit.
"""
import argparse
import asyncio

import chia
import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from chia.base.ChiaFunction import ChiaFunction, chia_actor, get


class _Barrier:
    def __init__(self):
        self.arrivals = set()
        self.ready = asyncio.Event()

    async def arrive(self, index):
        self.arrivals.add(index)
        if len(self.arrivals) == 64:
            self.ready.set()
        await asyncio.wait_for(self.ready.wait(), timeout=120)


@ChiaFunction(num_cpus=0, max_retries=0)
def _child(index, fail):
    if fail:
        raise ValueError("expected child failure")
    return index * 2


@ChiaFunction(num_cpus=0, max_retries=0)
def _parent(barrier, child_node, index, failures):
    get(chia_actor(barrier).arrive.chia_remote(index), timeout=150)
    fail = failures and index % 8 == 0
    try:
        result = get(_child.options(scheduling_strategy=NodeAffinitySchedulingStrategy(
            child_node, soft=False)).chia_remote(index, fail), timeout=45)
    except ValueError as error:
        if not fail or "expected child failure" not in str(error):
            raise
        return "expected failure"
    assert not fail
    return result


@ChiaFunction(num_cpus=0, max_retries=0)
def _batch(barrier, parent_nodes, child_node, failures):
    refs = [_parent.options(scheduling_strategy=NodeAffinitySchedulingStrategy(
        node, soft=False)).chia_remote(barrier, child_node, index, failures)
        for index, node in enumerate(parent_nodes)]
    return get(refs, timeout=210)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--address", default="auto")
    args = parser.parse_args()
    ray.init(address=args.address, runtime_env={
        "py_modules": [str(next(iter(chia.__path__)))],
        "excludes": ["**/test/", "**/tests/", "**/__pycache__", "*.pyc"],
    })
    proxy_name = f"chia_dispatch_proxy_{ray.get_runtime_context().get_job_id()}"
    barriers = []
    batch = None
    try:
        nodes = [node["NodeID"] for node in ray.nodes() if node["Alive"]
                 and node.get("Resources", {}).get("CPU", 0) > 0
                 and "node:__internal_head__" not in node.get("Resources", {})]
        assert len(nodes) >= 66, f"Need 66 worker raylets, found {len(nodes)}"
        coordinator, child_node, *parents = nodes[:66]
        proxy_id = None
        for failures in (False, True):
            # This is test coordination, not application work. All work uses
            # ChiaFunction; the one async actor holds the 64-parent barrier.
            barrier = ray.remote(_Barrier).options(num_cpus=0, max_concurrency=64,
                resources={"node:__internal_head__": 0.001}).remote()
            barriers.append(barrier)
            batch = _batch.options(scheduling_strategy=NodeAffinitySchedulingStrategy(
                coordinator, soft=False)).chia_remote(barrier, parents, child_node, failures)
            results = get(batch, timeout=240)
            expected = ["expected failure" if failures and i % 8 == 0 else i * 2
                        for i in range(64)]
            assert results == expected, results
            proxy = ray.get_actor(proxy_name, namespace="chia_dispatch")
            if proxy_id is not None:
                assert proxy._actor_id == proxy_id, "Batch created another dispatch proxy"
            proxy_id = proxy._actor_id
            ray.kill(barrier, no_restart=True)
            barriers.remove(barrier)
            print(f"PASS: 64 nested parents; child failures={failures}; proxy reused", flush=True)
    finally:
        if batch is not None:
            ray.cancel(batch, force=True, recursive=True)
        for barrier in barriers:
            ray.kill(barrier, no_restart=True)
        try:
            proxy = ray.get_actor(proxy_name, namespace="chia_dispatch")
        except ValueError:
            pass
        else:
            ray.kill(proxy, no_restart=True)
        ray.shutdown()


if __name__ == "__main__":
    main()
