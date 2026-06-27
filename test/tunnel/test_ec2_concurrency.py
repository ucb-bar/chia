"""
Concurrency / worker-port-capacity stress test for the tunnelled EC2 cluster.

Where the other tunnel tests prove the reverse tunnels *establish* and that a
single tool/task works, this test proves the pinned Ray worker-port ranges are
*wide enough* to hold a node's full resource-level concurrency at once — the
one property bring-up cannot check, because at bring-up there are zero running
tasks.

Two ranges are exercised:

  * ``ray_worker_port_min..max`` on each tunnelled EC2 worker
        ``test_<resource>_worker_port_capacity`` launches exactly one task per
        unit of a custom resource (e.g. ``verilator_run``).  Each task simply
        ``sleep``s for ``CHIA_HOLD_SECONDS`` so that all of them are alive at
        once; peak concurrent worker processes — and therefore peak worker
        ports in use per node — equals the resource cap.  A head-pinned counter
        actor records the peak overlap.  If a worker-port range is too small,
        Ray cannot start all the workers and the test fails.

  * ``head_worker_port_min..max`` on the head
        ``test_head_worker_port_capacity`` pins N actors to the head (each a
        worker process holding one head worker port) and then calls them from
        EC2 tasks, so the RPCs traverse the head-worker reverse tunnel (the
        40000-range path used by, e.g., ProfileCollectorActor).

IMPORTANT — tasks must NOT block on ``ray.get`` while holding a slot.  A worker
that blocks in ``ray.get`` releases its CPU and Ray starts *replacement*
workers, inflating worker-process/port demand far beyond the true concurrency
and producing false "No available ports" failures.  Slots are therefore held
with a plain ``time.sleep`` and overlap is measured out-of-band by the counter
actor (whose ``enter``/``leave`` calls are fire-and-forget — never ``ray.get``
inside a task).

Run:
  chia up test/tunnel/test_ec2_cluster.yaml
  python -m pytest test/tunnel/test_ec2_concurrency.py -v -s

Tune via env:
  CHIA_HOLD_SECONDS  how long each task holds its worker slot (default 12)
  CHIA_HEAD_ACTORS   number of head-pinned actors; must be <= the head's
                     head_worker_port range width (default 16)
"""

import os
import socket
import time
import unittest

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


def _cap(resource: str) -> int:
    return int(ray.cluster_resources().get(resource, 0))


def _head_node_id() -> str:
    # The driver runs on the head node (where `chia up` / pytest is invoked),
    # so the driver's node id is the head's.
    return ray.get_runtime_context().get_node_id()


def _hold_seconds() -> float:
    return float(os.environ.get("CHIA_HOLD_SECONDS", "12"))


@ray.remote(num_cpus=0)
class ConcurrencyTracker:
    """Counts overlapping task slots; records the peak.

    ``enter``/``leave`` are called fire-and-forget from tasks (no ``ray.get``
    in the task), so they never cause the task's worker to release its CPU.
    The actor processes its mailbox FIFO, so a final ``ray.get(peak)`` from the
    driver observes all prior enter/leave calls.
    """

    def __init__(self):
        self._cur = 0
        self._peak = 0

    def enter(self):
        self._cur += 1
        if self._cur > self._peak:
            self._peak = self._cur

    def leave(self):
        self._cur -= 1

    def peak(self):
        return self._peak


@ray.remote
def _hold_slot(tracker, hold_s):
    """Hold a Ray worker process (and its worker port) for ``hold_s`` seconds.

    Uses a plain sleep — NO ``ray.get`` — so the worker keeps its CPU and Ray
    does not spawn replacement workers.  Reports membership to the head-pinned
    tracker via fire-and-forget actor calls (also exercises EC2->head RPC over
    the reverse tunnel).
    """
    tracker.enter.remote()        # fire-and-forget; do not ray.get
    time.sleep(hold_s)
    tracker.leave.remote()        # fire-and-forget; do not ray.get
    return socket.gethostname(), os.getpid()


class TestTunnelConcurrency(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if not ray.is_initialized():
            ray.init(address="auto")

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    # ------------------------------------------------------------------
    # Worker-side range: ray_worker_port_min..max on each EC2 worker
    # ------------------------------------------------------------------
    def _run_worker_port_capacity(self, resource: str):
        cap = _cap(resource)
        if cap <= 0:
            self.skipTest(f"cluster missing resource: {resource}")

        hold_s = _hold_seconds()

        tracker = ConcurrencyTracker.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=_head_node_id(), soft=False
            )
        ).remote()

        tasks = [
            _hold_slot.options(resources={resource: 1}).remote(tracker, hold_s)
            for _ in range(cap)
        ]
        # Generous timeout: worker cold-start + the hold window.
        results = ray.get(tasks, timeout=hold_s + 120)

        self.assertEqual(len(results), cap)

        # Each concurrent task ran in its own worker process => `cap` distinct
        # (host, pid) pairs => `cap` worker ports were held simultaneously.
        worker_procs = {(host, pid) for host, pid in results}
        self.assertEqual(
            len(worker_procs), cap,
            f"expected {cap} distinct worker processes (one port each), "
            f"got {len(worker_procs)}",
        )

        # Confirm they actually overlapped (the range held them at once).
        peak = ray.get(tracker.peak.remote())
        self.assertEqual(
            peak, cap,
            f"only {peak}/{cap} tasks were alive simultaneously — increase "
            f"CHIA_HOLD_SECONDS if this is a cold cluster, otherwise the "
            f"worker-port range cannot hold {cap} concurrent workers",
        )
        hosts = sorted({h for h, _ in results})
        print(f"[{resource}] held {cap} concurrent worker processes across {hosts}")

    def test_verilator_run_worker_port_capacity(self):
        self._run_worker_port_capacity("verilator_run")

    def test_ec2_worker_port_capacity(self):
        self._run_worker_port_capacity("ec2")

    # ------------------------------------------------------------------
    # Head-side range: head_worker_port_min..max on the head node
    # ------------------------------------------------------------------
    def test_head_worker_port_capacity(self):
        n_actors = int(os.environ.get("CHIA_HEAD_ACTORS", "16"))
        drive_res = next(
            (r for r in ("verilator_run", "ec2") if _cap(r) > 0), None
        )
        if drive_res is None:
            self.skipTest("no tunnelled resource (verilator_run/ec2) available")

        # num_cpus=0 so all N co-locate on the head regardless of head cores;
        # each is still a distinct worker process holding one head worker port.
        @ray.remote(num_cpus=0)
        class Echo:
            def ping(self, x):
                return os.getpid(), x

        head = _head_node_id()
        actors = [
            Echo.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=head, soft=False
                )
            ).remote()
            for _ in range(n_actors)
        ]

        # Direct from driver: confirm N distinct head worker processes exist.
        head_pids = {pid for pid, _ in ray.get(
            [a.ping.remote(i) for i, a in enumerate(actors)], timeout=120
        )}
        self.assertEqual(
            len(head_pids), n_actors,
            f"expected {n_actors} distinct head worker processes (one head "
            f"worker port each), got {len(head_pids)} — head_worker_port range "
            f"is too small for {n_actors} concurrent head workers",
        )

        # From EC2 tasks: hit the head actors over the reverse tunnel.
        @ray.remote
        def call_actor(actor, val):
            return ray.get(actor.ping.remote(val))

        out = ray.get(
            [call_actor.options(resources={drive_res: 1}).remote(
                actors[i % n_actors], i) for i in range(n_actors)],
            timeout=120,
        )
        self.assertEqual(len(out), n_actors)
        for pid, _ in out:
            self.assertIn(
                pid, head_pids,
                "EC2->head actor RPC did not land on a head worker process",
            )
        print(f"[head] reached {n_actors} head actors over reverse tunnel "
              f"from '{drive_res}' tasks")


if __name__ == "__main__":
    unittest.main()
