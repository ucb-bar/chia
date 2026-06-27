from __future__ import annotations

import time
import unittest

import ray

from chia.base.chia_wait import (
    TrackedRef,
    chia_wait,
    _classify_stuck,
    _resources_cover,
    _retry_stuck,
)


@ray.remote
def _quick(x: int) -> int:
    return x


@ray.remote
def _sleep_forever() -> int:
    while True:
        time.sleep(60)


class TestResourceCover(unittest.TestCase):
    def test_empty_required_always_covered(self):
        self.assertTrue(_resources_cover({}, {}))
        self.assertTrue(_resources_cover({"CPU": 1.0}, {}))

    def test_covered(self):
        self.assertTrue(_resources_cover({"CPU": 4.0, "verilator_run": 32.0},
                                         {"CPU": 1.0, "verilator_run": 1.0}))

    def test_uncovered_missing_key(self):
        self.assertFalse(_resources_cover({"CPU": 4.0},
                                          {"CPU": 1.0, "verilator_run": 1.0}))

    def test_uncovered_insufficient(self):
        self.assertFalse(_resources_cover({"CPU": 0.5}, {"CPU": 1.0}))

    def test_floating_point_tolerance(self):
        # Tasks dispatched as float may produce values slightly less than
        # the dispatched amount; tolerate epsilon.
        self.assertTrue(_resources_cover({"CPU": 1.0 - 1e-12}, {"CPU": 1.0}))


class TestChiaWaitFastPath(unittest.TestCase):
    """No stuck detection — should behave exactly like ray.wait."""

    @classmethod
    def setUpClass(cls):
        if not ray.is_initialized():
            try:
                ray.init(num_cpus=2, ignore_reinit_error=True,
                         log_to_driver=False)
                cls._ray_started_here = True
            except ValueError:
                # An existing cluster is auto-discovered; just attach.
                ray.init(ignore_reinit_error=True, log_to_driver=False)
                cls._ray_started_here = False
        else:
            cls._ray_started_here = False

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, "_ray_started_here", False) and ray.is_initialized():
            ray.shutdown()

    def test_returns_completed_in_ready(self):
        tracked = [TrackedRef(ref=_quick.remote(i)) for i in range(3)]
        ray.wait([t.ref for t in tracked], num_returns=3)  # ensure done
        ready, pending = chia_wait(tracked, num_returns=3, timeout=5.0)
        self.assertEqual(len(ready), 3)
        self.assertEqual(len(pending), 0)
        for tr in ready:
            self.assertIn(ray.get(tr.ref), [0, 1, 2])

    def test_pending_timeout_disabled_keeps_pending(self):
        tracked = [TrackedRef(ref=_sleep_forever.remote())]
        ready, pending = chia_wait(tracked, num_returns=1, timeout=0.1)
        self.assertEqual(len(ready), 0)
        self.assertEqual(len(pending), 1)
        ray.cancel(pending[0].ref, force=True)


class TestRetryStuck(unittest.TestCase):
    """Verify _retry_stuck mutates TrackedRef in place."""

    @classmethod
    def setUpClass(cls):
        if not ray.is_initialized():
            try:
                ray.init(num_cpus=2, ignore_reinit_error=True,
                         log_to_driver=False)
                cls._ray_started_here = True
            except ValueError:
                # An existing cluster is auto-discovered; just attach.
                ray.init(ignore_reinit_error=True, log_to_driver=False)
                cls._ray_started_here = False
        else:
            cls._ray_started_here = False

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, "_ray_started_here", False) and ray.is_initialized():
            ray.shutdown()

    def test_retry_swaps_ref_and_increments(self):
        old_ref = _sleep_forever.remote()
        new_ref = _quick.remote(7)

        calls = {"n": 0}
        def submit():
            calls["n"] += 1
            return new_ref

        tr = TrackedRef(ref=old_ref, submit_fn=submit, label="x")
        old_at = tr.submitted_at
        time.sleep(0.01)  # ensure monotonic moves
        _retry_stuck(tr)
        self.assertEqual(calls["n"], 1)
        self.assertIs(tr.ref, new_ref)
        self.assertGreater(tr.submitted_at, old_at)
        self.assertEqual(tr.retries, 1)
        # old_ref should be cancelled (best effort) — don't assert state, just
        # confirm we don't block waiting for it.

    def test_retry_assert_when_no_submit_fn(self):
        tr = TrackedRef(ref=_quick.remote(0), submit_fn=None)
        with self.assertRaises(AssertionError):
            _retry_stuck(tr)


class TestClassifyStuckAgeFilter(unittest.TestCase):
    """The cheap age filter — runs without hitting the state API when no
    refs are aged enough."""

    @classmethod
    def setUpClass(cls):
        if not ray.is_initialized():
            try:
                ray.init(num_cpus=2, ignore_reinit_error=True,
                         log_to_driver=False)
                cls._ray_started_here = True
            except ValueError:
                # An existing cluster is auto-discovered; just attach.
                ray.init(ignore_reinit_error=True, log_to_driver=False)
                cls._ray_started_here = False
        else:
            cls._ray_started_here = False

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, "_ray_started_here", False) and ray.is_initialized():
            ray.shutdown()

    def test_unaged_refs_return_empty_stuck(self):
        # When all refs are younger than pending_timeout, _classify_stuck
        # should short-circuit before any state API call.
        tracked = [TrackedRef(ref=_sleep_forever.remote()) for _ in range(3)]
        stuck = _classify_stuck(tracked, pending_timeout=600.0,
                                require_demand_absent=False)
        self.assertEqual(stuck, [])
        for tr in tracked:
            ray.cancel(tr.ref, force=True)

    def test_aged_refs_run_classifier(self):
        # Force-age a TrackedRef and verify _classify_stuck doesn't crash
        # when the state API is reachable. We don't assert membership —
        # the task may or may not actually be pending in time — just that
        # no exception bubbles up.
        tracked = [TrackedRef(ref=_sleep_forever.remote(), label="aged_test")]
        tracked[0].submitted_at = time.monotonic() - 1000.0
        stuck = _classify_stuck(tracked, pending_timeout=10.0,
                                require_demand_absent=False)
        # stuck is a list (possibly empty); must not raise.
        self.assertIsInstance(stuck, list)
        for tr in tracked:
            ray.cancel(tr.ref, force=True)


if __name__ == "__main__":
    unittest.main()
