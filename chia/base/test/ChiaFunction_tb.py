
from __future__ import annotations

import os
import tempfile
import unittest

import ray

from chia.base.ChiaFunction import ChiaFunction, ChiaCallRemote, get


# --- Decorated standalone functions for testing ---

@ChiaFunction()
def add(x, y):
    return x + y


@ChiaFunction(resources={"test_resource": 0.001})
def multiply(x, y):
    return x * y


# --- Helpers for the worker-side setup/cleanup hook tests ---
# Module-level so Ray can pickle them by reference; they write to a shared /tmp
# path so the driver can observe what ran on the worker (local Ray = same FS).

def _append_line(path, text):
    with open(path, "a") as f:
        f.write(text)


def _cleanup_returns_value(path):
    _append_line(path, "cleanup\n")
    return "CLEANUP_RETURN"   # must be ignored (side-effects only)


def _setup_boom():
    raise RuntimeError("setup failed")


def _cleanup_boom(path):
    _append_line(path, "cleanup\n")
    raise RuntimeError("cleanup failed")


@ChiaFunction()
def record_then_read(path):
    _append_line(path, "func\n")
    with open(path) as f:
        return f.read()


@ChiaFunction()
def record_then_raise(path):
    _append_line(path, "func\n")
    raise ValueError("boom")


# --- Class with decorated methods ---

class DummyTool:
    def __init__(self, factor):
        self.factor = factor

    @ChiaFunction()
    def scale(self, x):
        return x * self.factor


class TestChiaFunctionLocal(unittest.TestCase):
    """Test that @ChiaFunction-decorated functions work as normal local calls."""

    def test_local_call_standalone(self):
        self.assertEqual(add(2, 3), 5)

    def test_local_call_with_resources(self):
        self.assertEqual(multiply(4, 5), 20)

    def test_local_call_bound_method(self):
        tool = DummyTool(10)
        self.assertEqual(tool.scale(3), 30)

    def test_functools_wraps_preserved(self):
        self.assertEqual(add.__name__, "add")
        self.assertEqual(multiply.__name__, "multiply")

    def test_chia_remote_attribute_exists(self):
        self.assertTrue(callable(getattr(add, "chia_remote", None)))
        self.assertTrue(callable(getattr(multiply, "chia_remote", None)))

    def test_chia_original_stored(self):
        self.assertIsNotNone(add._chia_original)

    def test_chia_options_stored(self):
        self.assertEqual(add._chia_options, {})
        self.assertIn("resources", multiply._chia_options)

    def test_call_remote_raises_on_undecorated(self):
        def plain(x):
            return x
        with self.assertRaises(TypeError):
            ChiaCallRemote(plain, 1)


class TestChiaFunctionRemote(unittest.TestCase):
    """Test remote execution via Ray. Requires Ray to be initialized."""

    @classmethod
    def setUpClass(cls):
        cls._ray_started_here = False
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)
            cls._ray_started_here = True

    @classmethod
    def tearDownClass(cls):
        if cls._ray_started_here:
            ray.shutdown()

    def test_remote_standalone_via_attribute(self):
        ref = add.chia_remote(10, 20)
        result = get(ref)
        self.assertEqual(result, 30)

    def test_remote_standalone_via_call_remote(self):
        ref = ChiaCallRemote(add, 7, 8)
        result = get(ref)
        self.assertEqual(result, 15)

    def test_remote_bound_method(self):
        # NOTE: Remote bound method calls require the class to be importable
        # by Ray workers. Classes defined only in test files won't work.
        # This test verifies the local path works; remote bound methods
        # are tested via the DB tool integration tests with proper imports.
        tool = DummyTool(5)
        # Local call through the decorator still works:
        self.assertEqual(tool.scale(6), 30)
        # The .chia_remote attribute exists:
        self.assertTrue(callable(tool.scale.chia_remote))

    def test_remote_with_kwargs(self):
        ref = add.chia_remote(x=100, y=200)
        result = get(ref)
        self.assertEqual(result, 300)

    def test_get_wraps_ray_get(self):
        ref = add.chia_remote(1, 1)
        self.assertEqual(get(ref), 2)
        # Also verify ray.get works identically
        ref2 = add.chia_remote(2, 2)
        self.assertEqual(ray.get(ref2), 4)

    def test_get_callback_runs_on_resolved_value(self):
        seen = {}

        def cb(val):
            seen["val"] = val
            return val * 10

        # Dispatch stays async; the callback runs when get() resolves the ref.
        out = get(add.chia_remote(3, 4), callback=cb)
        self.assertEqual(seen["val"], 7)   # callback saw the resolved value
        self.assertEqual(out, 70)          # get() returns the callback's result

    def test_get_objectrefcallback_runs_carried_callback(self):
        from chia.base.ChiaFunction import ObjectRefCallback

        seen = {}

        def cb(val):
            seen["val"] = val
            return val + 100

        # The ref carries its own callback; get() runs it (no callback= needed).
        out = get(ObjectRefCallback(add.chia_remote(3, 4), cb))
        self.assertEqual(seen["val"], 7)
        self.assertEqual(out, 107)


class TestChiaFunctionHooks(unittest.TestCase):
    """Test optional worker-side setup/cleanup hooks on chia_remote."""

    @classmethod
    def setUpClass(cls):
        cls._ray_started_here = False
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)
            cls._ray_started_here = True

    @classmethod
    def tearDownClass(cls):
        if cls._ray_started_here:
            ray.shutdown()

    def setUp(self):
        self.path = os.path.join(tempfile.mkdtemp(), "log.txt")

    def _read(self):
        with open(self.path) as f:
            return f.read()

    def test_setup_before_func_and_cleanup_after(self):
        ref = record_then_read.chia_remote(
            self.path,
            _chia_setup=_append_line, _chia_setup_args=(self.path, "setup\n"),
            _chia_cleanup=_append_line, _chia_cleanup_args=(self.path, "cleanup\n"),
        )
        # func saw setup's write but not cleanup's (setup ran first, cleanup after).
        self.assertEqual(get(ref), "setup\nfunc\n")
        # cleanup ran after func.
        self.assertEqual(self._read(), "setup\nfunc\ncleanup\n")

    def test_cleanup_runs_when_func_raises(self):
        with self.assertRaises(ValueError):
            get(record_then_raise.chia_remote(
                self.path,
                _chia_cleanup=_append_line, _chia_cleanup_args=(self.path, "cleanup\n"),
            ))
        # cleanup still ran despite func raising.
        self.assertEqual(self._read(), "func\ncleanup\n")

    def test_cleanup_return_value_is_ignored(self):
        ref = record_then_read.chia_remote(
            self.path,
            _chia_cleanup=_cleanup_returns_value, _chia_cleanup_args=(self.path,),
        )
        # func's result is returned, NOT cleanup's "CLEANUP_RETURN".
        self.assertEqual(get(ref), "func\n")

    def test_setup_exception_propagates_and_skips_func_and_cleanup(self):
        with self.assertRaises(RuntimeError):
            get(record_then_read.chia_remote(
                self.path,
                _chia_setup=_setup_boom,
                _chia_cleanup=_append_line, _chia_cleanup_args=(self.path, "cleanup\n"),
            ))
        # Neither func nor cleanup ran (nothing wrote the file).
        self.assertFalse(os.path.exists(self.path))

    def test_cleanup_exception_is_swallowed_on_success(self):
        ref = record_then_read.chia_remote(
            self.path,
            _chia_cleanup=_cleanup_boom, _chia_cleanup_args=(self.path,),
        )
        # A raising cleanup must not mask func's successful result.
        self.assertEqual(get(ref), "func\n")
        self.assertEqual(self._read(), "func\ncleanup\n")

    def test_cleanup_exception_does_not_mask_func_exception(self):
        # func raises ValueError AND cleanup raises RuntimeError -> the caller
        # must see func's ValueError, not the cleanup's RuntimeError.
        with self.assertRaises(ValueError):
            get(record_then_raise.chia_remote(
                self.path,
                _chia_cleanup=_cleanup_boom, _chia_cleanup_args=(self.path,),
            ))

    def test_hooks_via_options_handle(self):
        ref = record_then_read.options(num_cpus=1).chia_remote(
            self.path,
            _chia_setup=_append_line, _chia_setup_args=(self.path, "setup\n"),
        )
        self.assertEqual(get(ref), "setup\nfunc\n")

    def test_no_hooks_leaves_call_unchanged(self):
        self.assertEqual(get(add.chia_remote(2, 3)), 5)


if __name__ == "__main__":
    unittest.main()
