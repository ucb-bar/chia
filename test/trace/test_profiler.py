"""Tests for chia.trace.profiler — timing, worker info, and dependency tracking.

TestProfilerDisabled verifies the profiler is a no-op when no collector actor
is running.  TestProfilerWithRay tests the full ChiaFunction integration
end-to-end using a local Ray instance with a collector actor.

Run:
  python -m pytest test/trace/test_profiler.py -v -s

Test cases
----------

TestProfilerDisabled (no collector):
   1. test_disabled_without_collector
        Profiler is a no-op when no collector actor exists.
   2. test_disabled_unwraps_profiled_result
        Even when disabled, on_remote_complete unwraps _ProfiledResult.

TestProfilerWithRay (local Ray + collector):
   3. test_local_start_end
        Local calls emit local_start and local_end events with timing.
   4. test_remote_dispatch_and_complete
        Simulated remote dispatch/complete records worker IP, worker ID,
        exec_time, and wall_time.
   5. test_dependency_tracking
        Dependency edges detected via id() of resolved result objects
        (non-interned types).
   6. test_dependency_skips_interned_types
        Interned types (int, str, etc.) do not produce false dependency
        edges.
   7. test_dependency_via_kwargs
        Dependency edges are detected through keyword arguments too.
   8. test_multiple_parents
        A single function can have multiple parent dependencies.
   9. test_call_ids_are_unique
        Each dispatch gets a globally unique call_id.
  10. test_complete_without_dispatch
        Calling on_remote_complete for an unknown ref still unwraps the
        value without crashing.
  11. test_remote_call_profiled
        chia_remote() + get() produces dispatch + complete events with
        real worker IP, worker ID, exec_time, and wall_time.
  12. test_get_unwraps_transparently
        get() returns the plain value, not a _ProfiledResult wrapper.
  13. test_local_call_profiled
        Local @ChiaFunction call emits local_start + local_end events.
  14. test_dependency_chain_via_ray
        Dependency edge detection when result of get() is passed to the
        next chia_remote().  Documents that int results (in
        _SKIP_DEPENDENCY_TYPES) do NOT produce edges.
  15. test_chia_call_remote_profiled
        ChiaCallRemote() convenience function is also profiled (delegates
        to chia_remote).
  16. test_options_override_profiled
        func.options().chia_remote() path is also profiled.
  17. test_remote_display_name_profiled
        _chia_display_name is recorded on dispatch/complete without being
        passed to the user function.
  18. test_options_override_display_name_profiled
        _chia_display_name works through func.options().chia_remote().
"""

import os
import time
import unittest

import ray

from chia.base.ChiaFunction import ChiaFunction, ChiaCallRemote, get
from chia.trace.profiler import (
    ChiaProfiler, _ProfiledResult, _SKIP_DEPENDENCY_TYPES,
    _COLLECTOR_ACTOR_NAME,
    start_collector, get_collector,
)
import chia.trace.profiler as _profiler_mod


def slow_add(x, y):
    time.sleep(0.1)
    return x + y


@ChiaFunction()
def chia_add(x, y):
    time.sleep(0.05)
    return x + y


@ChiaFunction()
def chia_double(x):
    return x * 2


class Artifact:
    """Dummy complex result object for dependency tracking tests."""
    def __init__(self, name):
        self.name = name
    def __repr__(self):
        return f"Artifact({self.name!r})"


def _print_events(events, header=""):
    if header:
        print(f"\n{'=' * 60}")
        print(f"  {header}")
        print(f"{'=' * 60}")
    for evt in events:
        ts = time.strftime("%H:%M:%S", time.localtime(evt["ts"]))
        ms = f"{evt['ts'] % 1:.3f}"[1:]
        etype = evt["type"].upper()
        func = evt.get("func", "")
        cid = evt.get("call_id", "")
        skip = {"type", "func", "call_id", "ts"}
        parts = []
        for key, val in evt.items():
            if key in skip:
                continue
            parts.append(f"{key}={val}")
        extra = "  ".join(parts)
        print(f"  [{ts}{ms}] {etype:<12} {func:<20} id={cid}  {extra}")
    print()


# ===================================================================
# Tests without collector (profiler disabled)
# ===================================================================

class TestProfilerDisabled(unittest.TestCase):
    """Profiler is a no-op when no collector actor is running."""

    def test_disabled_without_collector(self):
        """Profiler is disabled when no collector actor exists."""
        profiler = ChiaProfiler()
        self.assertFalse(profiler.enabled)

        # All methods should be safe no-ops
        meta = profiler.prepare_dispatch({}, (1, 2), {})
        self.assertEqual(meta, {})
        profiler.on_worker_dispatch("test", "slow_add", "1.2.3.4", "w1",
                                    "node1", {}, [], "")
        result = profiler.on_remote_complete(42)
        self.assertEqual(result, 42)

        info = profiler.on_local_start(slow_add, (1, 2), {})
        self.assertIsNone(info)
        profiler.on_local_end(info, 3)
        profiler.close()

    def test_disabled_unwraps_profiled_result(self):
        """Even when disabled, on_remote_complete unwraps _ProfiledResult."""
        profiler = ChiaProfiler()
        pr = _ProfiledResult(value=99, worker_ip="1.2.3.4",
                             worker_id="w1", node_id="node1", exec_time_s=0.5)
        result = profiler.on_remote_complete(pr)
        self.assertEqual(result, 99)


# ===================================================================
# Tests with Ray + collector actor
# ===================================================================

class TestProfilerWithRay(unittest.TestCase):
    """Tests with a local Ray instance and collector actor."""

    @classmethod
    def setUpClass(cls):
        cls._ray_started_here = False
        # Ensure Ray is initialized with working_dir so the collector actor
        # can import chia modules.  Ray may already be auto-initialized via
        # RAY_ADDRESS (without runtime_env), so we shut down and re-init.
        if ray.is_initialized():
            ray.shutdown()
        ray.init(
            ignore_reinit_error=True,
            namespace="chia",
            runtime_env={
                "working_dir": os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            },
        )
        cls._ray_started_here = True
        # Create a named collector actor so workers can discover it via
        # get_collector() / ray.get_actor().
        from chia.trace.profiler import ProfileCollectorActor
        try:
            cls._collector = ray.get_actor(_COLLECTOR_ACTOR_NAME)
        except ValueError:
            cls._collector = ray.remote(ProfileCollectorActor).options(
                name=_COLLECTOR_ACTOR_NAME,
                num_cpus=0,
            ).remote()
        ray.get(cls._collector.get_events.remote())  # verify alive

    @classmethod
    def tearDownClass(cls):
        _profiler_mod._collector_override = None
        try:
            ray.kill(cls._collector)
        except Exception:
            pass
        if cls._ray_started_here:
            ray.shutdown()

    def setUp(self):
        # Clear collected events and reset profiler singleton.
        ray.get(self._collector.clear.remote())
        # Set the override so get_collector() returns our actor handle.
        _profiler_mod._collector_override = self._collector
        _profiler_mod.get_profiler.reset()
        # Reset cached remote funcs.
        chia_add._chia_remote_func = None
        chia_add._chia_remote_profiled = False
        chia_double._chia_remote_func = None
        chia_double._chia_remote_profiled = False

    def tearDown(self):
        _profiler_mod.get_profiler.reset()
        _profiler_mod._collector_override = None

    def _get_events(self):
        """Retrieve events from the collector actor."""
        time.sleep(1.0)  # let fire-and-forget calls from workers complete
        return ray.get(self._collector.get_events.remote())

    # --- Direct profiler API tests (mock refs, no remote execution) ---

    def test_local_start_end(self):
        """Local calls emit local_start and local_end events."""
        profiler = _profiler_mod.get_profiler()
        info = profiler.on_local_start(slow_add, (1, 2), {})
        result = slow_add(1, 2)
        profiler.on_local_end(info, result)

        self.assertEqual(result, 3)
        events = self._get_events()
        _print_events(events, "test_local_start_end")

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]["type"], "local_start")
        self.assertEqual(events[0]["func"], "slow_add")
        self.assertFalse(events[0]["is_remote"])
        self.assertIn("worker_ip", events[0])
        self.assertIn("call_id", events[0])

        self.assertEqual(events[1]["type"], "local_end")
        self.assertEqual(events[1]["func"], "slow_add")
        self.assertGreaterEqual(events[1]["exec_time_s"], 0.1)
        self.assertEqual(events[0]["call_id"], events[1]["call_id"])

    def test_remote_dispatch_and_complete(self):
        """Simulated remote dispatch/complete with on_worker_dispatch + on_worker_complete."""
        profiler = _profiler_mod.get_profiler()

        call_id = profiler.next_call_id()
        # Simulate what the trampoline does on the worker.
        profiler.on_worker_dispatch(call_id, "slow_add", "10.0.1.5",
                                    "worker_abc", "node1",
                                    {"gpu": 1}, [], profiler._worker_id)
        profiler.on_worker_complete(call_id, "slow_add", time.time(), 0.123,
                                    "10.0.1.5", "worker_abc", "node1")
        pr = _ProfiledResult(value=3, worker_ip="10.0.1.5",
                             worker_id="worker_abc", node_id="node1",
                             exec_time_s=0.123, call_id=call_id,
                             func_name="slow_add")
        value = profiler.on_remote_complete(pr)

        self.assertEqual(value, 3)
        events = self._get_events()
        _print_events(events, "test_remote_dispatch_and_complete")

        self.assertEqual(len(events), 2)

        dispatch = events[0]
        self.assertEqual(dispatch["type"], "dispatch")
        self.assertEqual(dispatch["func"], "slow_add")
        self.assertTrue(dispatch["is_remote"])
        self.assertEqual(dispatch["resources"], {"gpu": 1})
        self.assertEqual(dispatch["worker_ip"], "10.0.1.5")
        self.assertEqual(dispatch["worker_id"], "worker_abc")
        self.assertEqual(dispatch["node_id"], "node1")

        complete = events[1]
        self.assertEqual(complete["type"], "complete")
        self.assertEqual(complete["call_id"], dispatch["call_id"])
        self.assertEqual(complete["worker_ip"], "10.0.1.5")
        self.assertEqual(complete["worker_id"], "worker_abc")
        self.assertAlmostEqual(complete["exec_time_s"], 0.123, places=3)

    def test_dependency_tracking(self):
        """Dependency edges detected via id() of resolved result objects."""
        profiler = _profiler_mod.get_profiler()

        cid_a = profiler.next_call_id()
        profiler.on_worker_dispatch(cid_a, "slow_add", "10.0.1.1", "w1",
                                    "node1", {}, [], profiler._worker_id)
        artifact = Artifact("build_output")
        pr_a = _ProfiledResult(value=artifact, worker_ip="10.0.1.1",
                               worker_id="w1", node_id="node1",
                               exec_time_s=1.0, call_id=cid_a,
                               func_name="slow_add")
        profiler.on_remote_complete(pr_a)

        def func_b(x): return x
        cid_b = profiler.next_call_id()
        # prepare_dispatch computes edges on the driver
        meta_b = profiler.prepare_dispatch({}, (artifact,), {})
        profiler.on_worker_dispatch(cid_b, "func_b", "10.0.1.2", "w2",
                                    "node2", meta_b.get("resources", {}),
                                    meta_b.get("obj_ref_deps", []),
                                    meta_b.get("caller_worker_id", ""))

        events = self._get_events()
        _print_events(events, "test_dependency_tracking")

        dispatches = [e for e in events if e["type"] == "dispatch"]
        self.assertEqual(dispatches[1]["obj_ref_deps"],
                         [dispatches[0]["call_id"]])

    def test_dependency_skips_interned_types(self):
        """Interned types (int, str, etc.) don't produce false dependency edges."""
        profiler = _profiler_mod.get_profiler()

        cid_a = profiler.next_call_id()
        profiler.on_worker_dispatch(cid_a, "slow_add", "10.0.1.1", "w1",
                                    "node1", {}, [], profiler._worker_id)
        pr_a = _ProfiledResult(value=42, worker_ip="10.0.1.1",
                               worker_id="w1", node_id="node1",
                               exec_time_s=0.1, call_id=cid_a,
                               func_name="slow_add")
        profiler.on_remote_complete(pr_a)

        def func_b(x): return x
        cid_b = profiler.next_call_id()
        meta_b = profiler.prepare_dispatch({}, (42,), {})
        profiler.on_worker_dispatch(cid_b, "func_b", "10.0.1.2", "w2",
                                    "node2", meta_b.get("resources", {}),
                                    meta_b.get("obj_ref_deps", []),
                                    meta_b.get("caller_worker_id", ""))

        events = self._get_events()
        _print_events(events, "test_dependency_skips_interned_types")

        dispatches = [e for e in events if e["type"] == "dispatch"]
        self.assertEqual(dispatches[1]["obj_ref_deps"], [])

    def test_dependency_via_kwargs(self):
        """Dependency edges detected through keyword arguments too."""
        profiler = _profiler_mod.get_profiler()

        cid_a = profiler.next_call_id()
        profiler.on_worker_dispatch(cid_a, "slow_add", "10.0.1.1", "w1",
                                    "node1", {}, [], profiler._worker_id)
        artifact = Artifact("kwarg_test")
        pr_a = _ProfiledResult(value=artifact, worker_ip="10.0.1.1",
                               worker_id="w1", node_id="node1",
                               exec_time_s=0.1, call_id=cid_a,
                               func_name="slow_add")
        profiler.on_remote_complete(pr_a)

        def func_b(data=None): return data
        cid_b = profiler.next_call_id()
        meta_b = profiler.prepare_dispatch({}, (), {"data": artifact})
        profiler.on_worker_dispatch(cid_b, "func_b", "10.0.1.2", "w2",
                                    "node2", meta_b.get("resources", {}),
                                    meta_b.get("obj_ref_deps", []),
                                    meta_b.get("caller_worker_id", ""))

        events = self._get_events()
        _print_events(events, "test_dependency_via_kwargs")

        dispatches = [e for e in events if e["type"] == "dispatch"]
        self.assertEqual(dispatches[1]["obj_ref_deps"],
                         [dispatches[0]["call_id"]])

    def test_multiple_parents(self):
        """A function can have multiple parent dependencies."""
        profiler = _profiler_mod.get_profiler()

        art_1 = Artifact("art_1")
        art_2 = Artifact("art_2")

        cid_1 = profiler.next_call_id()
        profiler.on_worker_dispatch(cid_1, "slow_add", "10.0.1.1", "w1",
                                    "node1", {}, [], profiler._worker_id)
        profiler.on_remote_complete(_ProfiledResult(
            art_1, "10.0.1.1", "w1", "node1", 0.1, call_id=cid_1,
            func_name="slow_add"))

        cid_2 = profiler.next_call_id()
        profiler.on_worker_dispatch(cid_2, "slow_add", "10.0.1.2", "w2",
                                    "node2", {}, [], profiler._worker_id)
        profiler.on_remote_complete(_ProfiledResult(
            art_2, "10.0.1.2", "w2", "node2", 0.2, call_id=cid_2,
            func_name="slow_add"))

        def func_c(a, b): return (a, b)
        cid_3 = profiler.next_call_id()
        meta_3 = profiler.prepare_dispatch({}, (art_1, art_2), {})
        profiler.on_worker_dispatch(cid_3, "func_c", "10.0.1.3", "w3",
                                    "node3", meta_3.get("resources", {}),
                                    meta_3.get("obj_ref_deps", []),
                                    meta_3.get("caller_worker_id", ""))

        events = self._get_events()
        _print_events(events, "test_multiple_parents")

        dispatches = [e for e in events if e["type"] == "dispatch"]
        self.assertEqual(len(dispatches), 3)
        self.assertEqual(len(dispatches[2]["obj_ref_deps"]), 2)
        self.assertIn(dispatches[0]["call_id"],
                      dispatches[2]["obj_ref_deps"])
        self.assertIn(dispatches[1]["call_id"],
                      dispatches[2]["obj_ref_deps"])

    def test_call_ids_are_unique(self):
        """Each dispatch gets a unique call_id."""
        profiler = _profiler_mod.get_profiler()

        for _ in range(5):
            cid = profiler.next_call_id()
            profiler.on_worker_dispatch(cid, "slow_add", "10.0.1.1", "w1",
                                        "node1", {}, [], profiler._worker_id)

        events = self._get_events()
        dispatch_ids = [e["call_id"] for e in events
                       if e["type"] == "dispatch"]
        self.assertEqual(len(dispatch_ids), 5)
        self.assertEqual(len(dispatch_ids), len(set(dispatch_ids)))

    def test_complete_without_dispatch(self):
        """on_remote_complete unwraps value and registers dependency."""
        profiler = _profiler_mod.get_profiler()

        pr = _ProfiledResult(value="hello", worker_ip="1.2.3.4",
                             worker_id="w1", node_id="node1",
                             exec_time_s=0.0, call_id="orphan_001",
                             func_name="orphan_func")
        result = profiler.on_remote_complete(pr)

        self.assertEqual(result, "hello")
        events = self._get_events()
        self.assertEqual(len(events), 0)

    # --- End-to-end ChiaFunction tests ---

    def test_remote_call_profiled(self):
        """chia_remote() + get() produces dispatch + complete with worker info."""
        ref = chia_add.chia_remote(10, 20)
        result = get(ref)

        self.assertEqual(result, 30)

        events = self._get_events()
        _print_events(events, "test_remote_call_profiled")

        self.assertEqual(len(events), 2)

        dispatch = events[0]
        self.assertEqual(dispatch["type"], "dispatch")
        self.assertEqual(dispatch["func"], "chia_add")
        self.assertTrue(dispatch["is_remote"])
        # Dispatch now emitted from worker — has worker metadata
        self.assertIsNotNone(dispatch["worker_ip"])
        self.assertNotEqual(dispatch["worker_ip"], "unknown")
        self.assertIsNotNone(dispatch["worker_id"])
        self.assertIn("node_id", dispatch)

        complete = events[1]
        self.assertEqual(complete["type"], "complete")
        self.assertEqual(complete["call_id"], dispatch["call_id"])
        self.assertIsNotNone(complete["worker_ip"])
        self.assertNotEqual(complete["worker_ip"], "unknown")
        self.assertIsNotNone(complete["worker_id"])
        self.assertGreaterEqual(complete["exec_time_s"], 0.05)

    def test_get_unwraps_transparently(self):
        """get() returns the plain value, not a _ProfiledResult."""
        ref = chia_add.chia_remote(3, 4)
        result = get(ref)

        self.assertEqual(result, 7)
        self.assertNotIsInstance(result, _ProfiledResult)

    def test_local_call_profiled(self):
        """Local ChiaFunction call emits local_start + local_end."""
        result = chia_add(5, 6)

        self.assertEqual(result, 11)

        events = self._get_events()
        _print_events(events, "test_local_call_profiled")

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]["type"], "local_start")
        self.assertEqual(events[0]["func"], "chia_add")
        self.assertFalse(events[0]["is_remote"])
        self.assertEqual(events[1]["type"], "local_end")
        self.assertGreaterEqual(events[1]["exec_time_s"], 0.05)

    def test_dependency_chain_via_ray(self):
        """Dependency edge detected when result of get() is passed to next chia_remote()."""
        ref_a = chia_add.chia_remote(10, 20)
        result_a = get(ref_a)  # 30

        ref_b = chia_double.chia_remote(result_a)
        result_b = get(ref_b)

        self.assertEqual(result_a, 30)
        self.assertEqual(result_b, 60)

        events = self._get_events()
        _print_events(events, "test_dependency_chain_via_ray")

        self.assertEqual(len(events), 4)
        dispatch_a = events[0]
        dispatch_b = events[2]

        # int results (30) are in _SKIP_DEPENDENCY_TYPES,
        # so this edge will NOT be detected.
        if isinstance(result_a, _SKIP_DEPENDENCY_TYPES):
            self.assertEqual(dispatch_b["obj_ref_deps"], [])
        else:
            self.assertIn(dispatch_a["call_id"],
                          dispatch_b["obj_ref_deps"])

    def test_chia_call_remote_profiled(self):
        """ChiaCallRemote() is also profiled (delegates to chia_remote)."""
        ref = ChiaCallRemote(chia_add, 7, 8)
        result = get(ref)

        self.assertEqual(result, 15)

        events = self._get_events()
        _print_events(events, "test_chia_call_remote_profiled")

        dispatches = [e for e in events if e["type"] == "dispatch"]
        completes = [e for e in events if e["type"] == "complete"]
        self.assertEqual(len(dispatches), 1)
        self.assertEqual(len(completes), 1)
        self.assertEqual(dispatches[0]["func"], "chia_add")

    def test_remote_display_name_profiled(self):
        """_chia_display_name labels profiled remote calls without leaking to args."""
        ref = chia_add.chia_remote(11, 12, _chia_display_name="main_agent")
        result = get(ref)

        self.assertEqual(result, 23)

        events = self._get_events()
        _print_events(events, "test_remote_display_name_profiled")

        dispatches = [e for e in events if e["type"] == "dispatch"]
        completes = [e for e in events if e["type"] == "complete"]
        self.assertEqual(len(dispatches), 1)
        self.assertEqual(len(completes), 1)
        self.assertEqual(dispatches[0]["func"], "chia_add")
        self.assertEqual(completes[0]["func"], "chia_add")
        self.assertEqual(dispatches[0]["display_name"], "main_agent")
        self.assertEqual(completes[0]["display_name"], "main_agent")

    def test_options_override_profiled(self):
        """func.options().chia_remote() path is also profiled."""
        ref = chia_add.options(num_cpus=1).chia_remote(2, 3)
        result = get(ref)

        self.assertEqual(result, 5)

        events = self._get_events()
        _print_events(events, "test_options_override_profiled")

        dispatches = [e for e in events if e["type"] == "dispatch"]
        completes = [e for e in events if e["type"] == "complete"]
        self.assertEqual(len(dispatches), 1)
        self.assertEqual(len(completes), 1)

    def test_options_override_display_name_profiled(self):
        """_chia_display_name works through func.options().chia_remote()."""
        ref = chia_add.options(num_cpus=1).chia_remote(
            4, 9, _chia_display_name="validator_agent")
        result = get(ref)

        self.assertEqual(result, 13)

        events = self._get_events()
        _print_events(events, "test_options_override_display_name_profiled")

        dispatches = [e for e in events if e["type"] == "dispatch"]
        completes = [e for e in events if e["type"] == "complete"]
        self.assertEqual(len(dispatches), 1)
        self.assertEqual(len(completes), 1)
        self.assertEqual(dispatches[0]["func"], "chia_add")
        self.assertEqual(completes[0]["func"], "chia_add")
        self.assertEqual(dispatches[0]["display_name"], "validator_agent")
        self.assertEqual(completes[0]["display_name"], "validator_agent")


if __name__ == "__main__":
    unittest.main()
