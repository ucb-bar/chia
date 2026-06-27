from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
import unittest

import ray

from chia.base.ChiaFunction import ChiaFunction, get, chia_cancel
from chia.base.pid_registry import (
    PidRegistryActor,
    _get_registry,
    _install_driver_cleanup,
    _install_popen_hook,
    _pid_tracking_scope,
    _reset_registry,
    _tls,
)


# --- Decorated functions for testing ---

@ChiaFunction()
def spawn_sleep(seconds: int = 60) -> int:
    """Spawn a sleep subprocess and return its PID."""
    proc = subprocess.Popen(
        ["sleep", str(seconds)],
        start_new_session=True,
    )
    return proc.pid


@ChiaFunction()
def spawn_sleep_no_session(seconds: int = 60) -> int:
    """Spawn a sleep subprocess without start_new_session."""
    proc = subprocess.Popen(["sleep", str(seconds)])
    return proc.pid


@ChiaFunction()
def no_subprocess() -> str:
    """A function that spawns no subprocesses."""
    return "done"


@ChiaFunction()
def spawn_nested(pid_file: str, seconds: int = 600) -> None:
    """Mimic hammer_syn_node: Popen with start_new_session whose child spawns a grandchild.

    bash is the direct child (process group leader). It spawns ``sleep``
    as a grandchild, writes the grandchild PID to *pid_file*, then blocks
    on ``wait``.  The polling loop mirrors hammer_syn_node's pattern.
    """
    proc = subprocess.Popen(
        ["bash", "-c", f"sleep {seconds} & echo $! > {pid_file}; wait"],
        start_new_session=True,
    )
    # Poll in short intervals like hammer_syn_node (line 159)
    while proc.poll() is None:
        time.sleep(1)


@ChiaFunction()
def spawn_deep_nested(pid_file: str, seconds: int = 600) -> None:
    """Three levels deep: child -> grandchild -> great-grandchild.

    Simulates a tool chain like: hammer-vlsi -> genus -> genus subprocess.
    The outer bash (child) spawns an inner bash (grandchild) which spawns
    sleep (great-grandchild).  All share the same process group via
    start_new_session on the outermost Popen.  PIDs of all three
    descendants are written to *pid_file*, one per line.
    """
    script = (
        f"bash -c '"
        f"  sleep {seconds} & echo $! >> {pid_file};  "  # great-grandchild
        f"  echo $$ >> {pid_file};  "                     # grandchild (inner bash)
        f"  wait"
        f"' & "
        f"echo $! >> {pid_file}; "   # grandchild PID as seen by outer bash
        f"echo $$ >> {pid_file}; "   # child (outer bash)
        f"wait"
    )
    proc = subprocess.Popen(
        ["bash", "-c", script],
        start_new_session=True,
    )
    while proc.poll() is None:
        time.sleep(1)


class TestPidRegistryLocal(unittest.TestCase):
    """Test PID registry components without Ray."""

    def test_popen_hook_install_is_idempotent(self):
        _install_popen_hook()
        _install_popen_hook()  # should not raise

    def test_tracking_scope_noop_without_ray(self):
        """If Ray is not initialized, _pid_tracking_scope is a no-op."""
        was_initialized = ray.is_initialized()
        if was_initialized:
            self.skipTest("Ray is already initialized")
        with _pid_tracking_scope():
            pass  # should not raise


class TestPidRegistryRemote(unittest.TestCase):
    """Test PID tracking and cancellation with Ray."""

    @classmethod
    def setUpClass(cls):
        cls._ray_started_here = False
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)
            cls._ray_started_here = True
        # Ensure a fresh registry handle for these tests.
        _reset_registry()

    @classmethod
    def tearDownClass(cls):
        if cls._ray_started_here:
            ray.shutdown()

    def test_registry_actor_created(self):
        """Registry actor should be lazily created on first access."""
        registry = _get_registry()
        self.assertIsNotNone(registry)

    def test_register_and_retrieve(self):
        """Direct actor test: register PIDs and retrieve them."""
        registry = _get_registry()
        ray.get(registry.register.remote("test_task_1", "node_abc", 12345, True))
        ray.get(registry.register.remote("test_task_1", "node_abc", 12346, False))
        pids = ray.get(registry.get_and_remove.remote("test_task_1"))
        self.assertEqual(len(pids), 2)
        self.assertEqual(pids[0], ("node_abc", 12345, True))
        self.assertEqual(pids[1], ("node_abc", 12346, False))
        # Should be removed now
        pids2 = ray.get(registry.get_and_remove.remote("test_task_1"))
        self.assertEqual(pids2, [])

    def test_unregister(self):
        """Direct actor test: unregister removes entries."""
        registry = _get_registry()
        ray.get(registry.register.remote("test_task_2", "node_abc", 99999, False))
        ray.get(registry.unregister.remote("test_task_2"))
        pids = ray.get(registry.get_and_remove.remote("test_task_2"))
        self.assertEqual(pids, [])

    def test_spawn_tracks_pid(self):
        """A ChiaFunction that spawns a Popen should register the PID."""
        ref = spawn_sleep.chia_remote(60)
        sleep_pid = get(ref)
        # The sleep process should have been spawned
        self.assertIsInstance(sleep_pid, int)
        self.assertGreater(sleep_pid, 0)
        # Clean up the sleep process
        try:
            os.killpg(sleep_pid, signal.SIGTERM)
        except OSError:
            pass

    def test_no_subprocess_no_crash(self):
        """A ChiaFunction with no subprocesses should work fine."""
        ref = no_subprocess.chia_remote()
        result = get(ref)
        self.assertEqual(result, "done")

    def test_chia_cancel_kills_subprocess(self):
        """chia_cancel should kill the subprocess spawned by the task."""
        ref = spawn_sleep.chia_remote(600)
        # Give the task time to start and spawn the subprocess
        time.sleep(2)
        sleep_pid = get(ref)

        # Spawn a new one to cancel
        ref2 = spawn_sleep.chia_remote(600)
        time.sleep(2)

        # Cancel the task
        chia_cancel(ref2)

        # Give the kill a moment to propagate
        time.sleep(1)

    def test_chia_cancel_kills_grandchild(self):
        """Mimic hammer_syn_node: cancel should kill both the child and grandchild.

        Dispatches spawn_nested which does:
            Popen("bash -c 'sleep 600 & echo $! > file; wait'", start_new_session=True)

        bash is the direct child (process group leader, tracked by the
        Popen hook).  ``sleep`` is a grandchild that inherits the process
        group.  chia_cancel sends killpg to the group, which should kill
        both — exactly like killing hammer-vlsi kills Genus.
        """
        import tempfile
        pid_file = tempfile.mktemp(prefix="chia_test_grandchild_")
        try:
            ref = spawn_nested.chia_remote(pid_file, 600)

            # Wait for the grandchild PID file to appear (bash writes it
            # after forking sleep).
            grandchild_pid = None
            for _ in range(15):
                time.sleep(1)
                if os.path.exists(pid_file):
                    with open(pid_file) as f:
                        content = f.read().strip()
                    if content:
                        grandchild_pid = int(content)
                        break
            self.assertIsNotNone(grandchild_pid,
                                 "Grandchild PID file was never written")

            # Verify the grandchild is alive (signal 0 = existence check).
            try:
                os.kill(grandchild_pid, 0)
            except OSError:
                self.fail("Grandchild should be alive before cancel")

            # Cancel — this kills the process group then cancels the Ray task.
            chia_cancel(ref)
            time.sleep(2)

            # Verify the grandchild is dead.
            with self.assertRaises(OSError):
                os.kill(grandchild_pid, 0)
        finally:
            if os.path.exists(pid_file):
                os.unlink(pid_file)

    def test_chia_cancel_kills_great_grandchild(self):
        """Cancel should kill 3 levels deep: child, grandchild, great-grandchild.

        Process tree:
            Ray worker
              └── bash (child, pgid=N, start_new_session=True)
                    └── bash (grandchild, pgid=N)
                          └── sleep (great-grandchild, pgid=N)

        All three inherit the same process group.  killpg(N, SIGTERM)
        should reach every level.
        """
        import tempfile
        pid_file = tempfile.mktemp(prefix="chia_test_deep_")
        try:
            ref = spawn_deep_nested.chia_remote(pid_file, 600)

            # Wait for all PIDs to be written (4 lines: great-grandchild,
            # inner bash self, outer bash's view of inner bash, outer bash self).
            pids = []
            for _ in range(15):
                time.sleep(1)
                if os.path.exists(pid_file):
                    with open(pid_file) as f:
                        lines = [l.strip() for l in f.readlines() if l.strip()]
                    if len(lines) >= 4:
                        pids = list(set(int(l) for l in lines))
                        break
            self.assertGreaterEqual(len(pids), 2,
                                    f"Expected at least 2 unique PIDs, got: {pids}")

            # Verify all are alive.
            for pid in pids:
                try:
                    os.kill(pid, 0)
                except OSError:
                    self.fail(f"PID {pid} should be alive before cancel")

            # Cancel — killpg should reach all levels.
            chia_cancel(ref)
            time.sleep(2)

            # Verify all are dead.
            for pid in pids:
                with self.assertRaises(OSError, msg=f"PID {pid} should be dead after cancel"):
                    os.kill(pid, 0)
        finally:
            if os.path.exists(pid_file):
                os.unlink(pid_file)

    def test_start_new_session_detection(self):
        """Popen with start_new_session=True should be flagged as pgid."""
        # Spawn with start_new_session=True
        ref1 = spawn_sleep.chia_remote(60)
        pid1 = get(ref1)
        try:
            os.killpg(pid1, signal.SIGTERM)
        except OSError:
            pass

        # Spawn without start_new_session
        ref2 = spawn_sleep_no_session.chia_remote(60)
        pid2 = get(ref2)
        try:
            os.kill(pid2, signal.SIGTERM)
        except OSError:
            pass


class TestPidRegistryCleanup(unittest.TestCase):
    """Test that PID entries are cleaned up after task completion."""

    @classmethod
    def setUpClass(cls):
        cls._ray_started_here = False
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)
            cls._ray_started_here = True
        # Ensure a fresh registry handle for these tests.
        _reset_registry()

    @classmethod
    def tearDownClass(cls):
        if cls._ray_started_here:
            ray.shutdown()

    def test_cleanup_after_normal_completion(self):
        """After a task completes normally, its PIDs should be unregistered."""
        ref = spawn_sleep.chia_remote(60)
        task_id = ref.task_id().hex()
        pid = get(ref)

        # After get() returns, the trampoline's finally block should have
        # called unregister. Give it a moment for the fire-and-forget call.
        time.sleep(1)

        registry = _get_registry()
        pids = ray.get(registry.get_and_remove.remote(task_id))
        self.assertEqual(pids, [])

        # Clean up the sleep process
        try:
            os.killpg(pid, signal.SIGTERM)
        except OSError:
            pass


class TestKillAll(unittest.TestCase):
    """Test the kill_all() method on the PID registry actor."""

    @classmethod
    def setUpClass(cls):
        cls._ray_started_here = False
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)
            cls._ray_started_here = True
        _reset_registry()

    @classmethod
    def tearDownClass(cls):
        if cls._ray_started_here:
            ray.shutdown()

    def test_kill_all_clears_registry(self):
        """kill_all() should remove all entries and return the count."""
        registry = _get_registry()
        ray.get(registry.register.remote("kill_all_a", "node_x", 99998, False))
        ray.get(registry.register.remote("kill_all_b", "node_x", 99997, True))
        count = ray.get(registry.kill_all.remote())
        # PIDs are fake so remote kills fail silently (OSError), but the
        # registry should be cleared and count should reflect both.
        self.assertEqual(count, 2)
        self.assertEqual(ray.get(registry.get_and_remove.remote("kill_all_a")), [])
        self.assertEqual(ray.get(registry.get_and_remove.remote("kill_all_b")), [])

    def test_kill_all_on_empty_registry(self):
        """kill_all() on an empty registry should return 0."""
        registry = _get_registry()
        count = ray.get(registry.kill_all.remote())
        self.assertEqual(count, 0)

    def test_kill_all_kills_real_processes(self):
        """kill_all() should actually kill tracked subprocess trees."""
        procs = []
        registry = _get_registry()
        node_id = ray.get_runtime_context().get_node_id()
        for i in range(3):
            proc = subprocess.Popen(["sleep", "600"], start_new_session=True)
            procs.append(proc)
            ray.get(registry.register.remote(
                f"kill_all_real_{i}", node_id, proc.pid, True))

        # All should be alive.
        for proc in procs:
            self.assertIsNone(proc.poll())

        count = ray.get(registry.kill_all.remote())
        self.assertEqual(count, 3)

        # kill_all polls for death and returns only once the targets have
        # exited, so they are already dead by the time it returns.
        for proc in procs:
            proc.poll()
            self.assertIsNotNone(proc.returncode,
                                 f"PID {proc.pid} should be dead after kill_all")

    def test_kill_all_returns_promptly(self):
        """kill_all() should return ~immediately once the targets are dead.

        The targeted ``sleep`` processes terminate on SIGTERM within
        milliseconds.  kill_all polls for their death rather than waiting
        out the full grace period (_KILL_GRACE_SECONDS, 25s), so it must
        return well before that ceiling.
        """
        from chia.base.pid_registry import _KILL_GRACE_SECONDS

        procs = []
        registry = _get_registry()
        node_id = ray.get_runtime_context().get_node_id()
        for i in range(3):
            proc = subprocess.Popen(["sleep", "600"], start_new_session=True)
            procs.append(proc)
            ray.get(registry.register.remote(
                f"kill_all_prompt_{i}", node_id, proc.pid, True))

        for proc in procs:
            self.assertIsNone(proc.poll(), "process should be alive before kill_all")

        start = time.monotonic()
        count = ray.get(registry.kill_all.remote())
        elapsed = time.monotonic() - start

        self.assertEqual(count, 3)
        # Must return well under the grace ceiling — this is what proves it
        # polls for death instead of unconditionally sleeping the grace
        # period.  The margin (10s) absorbs Ray task dispatch overhead.
        self.assertLess(
            elapsed, _KILL_GRACE_SECONDS - 10,
            f"kill_all took {elapsed:.1f}s; expected prompt return "
            f"(grace ceiling is {_KILL_GRACE_SECONDS}s)")

        # And the targets are actually dead (proc.poll reaps to confirm).
        for proc in procs:
            proc.poll()
            self.assertIsNotNone(proc.returncode,
                                 f"PID {proc.pid} should be dead after kill_all")

    def test_kill_all_grace_drives_sigkill(self):
        """The grace argument should control when kill_all escalates to SIGKILL.

        A process that ignores SIGTERM can only be ended by the SIGKILL
        that fires after the grace period.  Passing a short grace should
        make kill_all return after roughly that grace (not the 25s
        default) with the process killed — proving the CLI's grace flows
        through to the actual kill grace.
        """
        import tempfile
        ready = tempfile.mktemp(prefix="chia_grace_ready_")
        # Single process that ignores SIGTERM and has no children, so only
        # the grace-triggered SIGKILL can end it.  It touches *ready* only
        # after installing the handler, so we don't race the interpreter's
        # startup with our first signal.
        proc = subprocess.Popen(
            [sys.executable, "-c",
             "import signal, time, sys; "
             "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
             "open(sys.argv[1], 'w').close(); "
             "time.sleep(600)",
             ready],
            start_new_session=True,
        )
        try:
            # Wait until the child has installed SIG_IGN.
            for _ in range(50):
                if os.path.exists(ready):
                    break
                time.sleep(0.1)
            self.assertTrue(os.path.exists(ready),
                            "child never signalled readiness")

            registry = _get_registry()
            node_id = ray.get_runtime_context().get_node_id()
            ray.get(registry.register.remote(
                "kill_all_grace", node_id, proc.pid, True))

            # SIGTERM alone must not kill it.
            os.killpg(proc.pid, signal.SIGTERM)
            time.sleep(1)
            self.assertIsNone(proc.poll(), "process should ignore SIGTERM")

            grace = 3
            start = time.monotonic()
            ray.get(registry.kill_all.remote(grace))
            elapsed = time.monotonic() - start

            # It waited ~grace for the escalation, then SIGKILLed — not the
            # 25s default, and not instant (SIGTERM was ignored).
            self.assertGreaterEqual(elapsed, grace - 1,
                                    f"kill_all returned in {elapsed:.1f}s; expected "
                                    f"to wait ~{grace}s before SIGKILL")
            self.assertLess(elapsed, grace + 10,
                            f"kill_all took {elapsed:.1f}s; grace ({grace}s) should "
                            f"cap the wait, not the 25s default")
            proc.poll()
            self.assertIsNotNone(proc.returncode,
                                 "SIGTERM-ignoring process should be SIGKILLed after grace")
        finally:
            if proc.poll() is None:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except OSError:
                    pass
            if os.path.exists(ready):
                os.unlink(ready)

    def test_kill_all_kills_process_group(self):
        """kill_all() with is_pgid=True should kill the whole group, not just the leader."""
        import tempfile
        pid_file = tempfile.mktemp(prefix="chia_kill_all_pg_")
        try:
            # Spawn a process group: bash (leader) -> sleep (child in same group)
            proc = subprocess.Popen(
                ["bash", "-c",
                 f"sleep 600 & echo $! > {pid_file}; wait"],
                start_new_session=True,
            )
            # Wait for grandchild PID to be written.
            grandchild_pid = None
            for _ in range(10):
                time.sleep(0.5)
                if os.path.exists(pid_file):
                    with open(pid_file) as f:
                        content = f.read().strip()
                    if content:
                        grandchild_pid = int(content)
                        break
            self.assertIsNotNone(grandchild_pid, "Grandchild PID was never written")

            # Register the leader with is_pgid=True
            registry = _get_registry()
            node_id = ray.get_runtime_context().get_node_id()
            ray.get(registry.register.remote(
                "kill_all_pg", node_id, proc.pid, True))

            # kill_all should killpg → kills both leader and grandchild.
            # It returns only once the group has exited.
            ray.get(registry.kill_all.remote())

            # Both the leader and grandchild should be dead.
            proc.poll()
            self.assertIsNotNone(proc.returncode,
                                 "Process group leader should be dead")
            with self.assertRaises(OSError):
                os.kill(grandchild_pid, 0)
        finally:
            if os.path.exists(pid_file):
                os.unlink(pid_file)


class TestSigtermCleanup(unittest.TestCase):
    """Integration test: SIGTERM to the driver should kill tracked subprocesses.

    Runs a helper script as a subprocess that acts as a Ray job driver.
    The helper spawns a ChiaFunction with a nested process tree, then
    blocks.  The test sends SIGTERM to the helper, and verifies that the
    SIGTERM cleanup handler (installed by _get_registry) calls kill_all()
    on the PID registry actor, killing all subprocess trees.
    """

    @classmethod
    def setUpClass(cls):
        cls._ray_started_here = False
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)
            cls._ray_started_here = True
        _reset_registry()

    @classmethod
    def tearDownClass(cls):
        if cls._ray_started_here:
            ray.shutdown()

    def test_sigterm_kills_tracked_subprocesses(self):
        import tempfile
        pid_file = tempfile.mktemp(prefix="chia_sigterm_test_")
        ready_file = pid_file + ".ready"
        helper_script = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "sigterm_cleanup_helper.py",
        )
        chia_root = os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__)))))
        env = os.environ.copy()
        env["PYTHONPATH"] = chia_root + os.pathsep + env.get("PYTHONPATH", "")
        try:
            # Start the helper as a direct subprocess (not via ray job submit).
            driver = subprocess.Popen(
                [sys.executable, helper_script, pid_file],
                env=env,
            )

            # Wait for readiness — helper writes driver PID to ready_file.
            driver_pid = None
            subprocess_pids = []
            for _ in range(45):
                time.sleep(1)
                if os.path.exists(ready_file):
                    with open(ready_file) as f:
                        driver_pid = int(f.read().strip())
                    with open(pid_file) as f:
                        lines = [l.strip() for l in f.readlines() if l.strip()]
                    subprocess_pids = list(set(int(l) for l in lines))
                    break
            self.assertIsNotNone(driver_pid,
                                 "Helper never signalled readiness")
            self.assertGreaterEqual(len(subprocess_pids), 2,
                                    f"Expected >=2 PIDs, got {subprocess_pids}")

            # Verify all subprocess PIDs are alive.
            for pid in subprocess_pids:
                try:
                    os.kill(pid, 0)
                except OSError:
                    self.fail(f"PID {pid} should be alive before SIGTERM")

            # Send SIGTERM to the driver — this triggers the cleanup handler.
            os.kill(driver_pid, signal.SIGTERM)

            # Wait for kill_all + _kill_pid (SIGTERM, 5s sleep, SIGKILL).
            time.sleep(10)

            # Verify all subprocess PIDs are dead.
            for pid in subprocess_pids:
                with self.assertRaises(
                    OSError,
                    msg=f"PID {pid} should be dead after SIGTERM to driver",
                ):
                    os.kill(pid, 0)

            # The driver itself should have exited.
            driver.wait(timeout=5)
        finally:
            # Clean up: kill driver if still alive, remove temp files.
            if driver.poll() is None:
                driver.kill()
                driver.wait(timeout=5)
            for f in (pid_file, ready_file):
                if os.path.exists(f):
                    os.unlink(f)


class TestRayJobStop(unittest.TestCase):
    """Integration test: ``ray job stop`` should kill subprocess trees.

    Submits a Ray job that spawns a 3-level process tree via a
    ChiaFunction, stops the job with ``ray job stop``, and verifies
    that all processes (child, grandchild, great-grandchild) are dead.

    This exercises the BaseException handler in ``_pid_tracking_scope``
    which does local cleanup when TaskCancelledError is delivered.
    """

    def test_ray_job_stop_kills_process_tree(self):
        import json
        import tempfile
        pid_file = tempfile.mktemp(prefix="chia_job_stop_test_")
        ready_file = pid_file + ".ready"
        # Resolve the path to the helper script and the chia package root
        # so the Ray job driver can import chia as a namespace package.
        chia_root = os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__)))))
        helper_script = os.path.join(
            chia_root, "chia", "base", "test", "ray_job_stop_helper.py")
        runtime_env = json.dumps({
            "env_vars": {"PYTHONPATH": chia_root},
        })
        try:
            # Submit the helper script as a Ray job.
            result = subprocess.run(
                [
                    "ray", "job", "submit",
                    "--no-wait",
                    "--runtime-env-json", runtime_env,
                    "--entrypoint-num-cpus", "0",
                    "--", "python", helper_script, pid_file,
                ],
                capture_output=True, text=True, timeout=30,
            )
            self.assertEqual(result.returncode, 0, f"Job submit failed: {result.stderr}")

            # Extract submission ID from output.
            submission_id = None
            for line in result.stdout.splitlines():
                if "raysubmit_" in line:
                    for token in line.split():
                        if token.startswith("raysubmit_") or "raysubmit_" in token:
                            # Strip quotes and punctuation.
                            clean = token.strip("'\".,;:")
                            if "raysubmit_" in clean:
                                submission_id = clean
                                break
                if submission_id:
                    break
            self.assertIsNotNone(submission_id, f"Could not find submission ID in: {result.stdout}")

            # Wait for the job to signal readiness (PIDs written).
            pids = []
            for _ in range(45):
                time.sleep(1)
                if os.path.exists(ready_file):
                    with open(pid_file) as f:
                        lines = [l.strip() for l in f.readlines() if l.strip()]
                    pids = list(set(int(l) for l in lines))
                    break
            self.assertGreaterEqual(len(pids), 2,
                                    f"Expected at least 2 unique PIDs, got: {pids}")

            # Verify all are alive.
            for pid in pids:
                try:
                    os.kill(pid, 0)
                except OSError:
                    self.fail(f"PID {pid} should be alive before ray job stop")

            # Stop the job.
            stop_result = subprocess.run(
                ["ray", "job", "stop", submission_id],
                capture_output=True, text=True, timeout=30,
            )
            self.assertEqual(stop_result.returncode, 0,
                             f"ray job stop failed: {stop_result.stderr}")

            # Wait for the BaseException handler + SIGTERM/SIGKILL to propagate.
            # The handler does SIGTERM, sleeps 2s, then SIGKILL.
            time.sleep(10)

            # Verify all tracked processes are dead.
            for pid in pids:
                with self.assertRaises(OSError, msg=f"PID {pid} should be dead after ray job stop"):
                    os.kill(pid, 0)
        finally:
            for f in (pid_file, ready_file):
                if os.path.exists(f):
                    os.unlink(f)


if __name__ == "__main__":
    unittest.main()
