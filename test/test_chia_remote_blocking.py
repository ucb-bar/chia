"""Unit tests for ``ChiaWrapped.chia_remote_blocking``.

``chia_remote_blocking`` is the synchronous sibling of ``chia_remote``: it
dispatches the decorated function as a Ray task (honoring the resource options
declared on ``@ChiaFunction``), blocks on the result via :func:`get`, and
returns the unwrapped *value* instead of an ``ObjectRef``.

Its motivating use case is registering a ``@ChiaFunction`` as an MCP tool on a
``ChiaTool``: ``self.mcp.add_tool(foo.chia_remote_blocking)``.  For that to work
the wrapper must (a) return the value, not a ref, and (b) carry func's real
signature so FastMCP can build the tool schema — ``functools.wraps(func)``
provides (b).  These tests pin down both.

Uses a local Ray instance with a custom resource (``crb_res``) so the
resource-scheduling path is actually exercised without a cluster.

Run:
  python -m pytest test/test_chia_remote_blocking.py -v
"""

import inspect
import os
import unittest

import ray

from chia.base.ChiaFunction import ChiaFunction, get
from ray import ObjectRef


# Module-level so Ray workers can import these by reference (working_dir is set
# in setUpClass).  crb_res must exist on the local cluster (see ray.init below).
@ChiaFunction(resources={"crb_res": 1})
def add_one(x: int, y: int = 1) -> int:
    """Return the sum x + y (default y=1)."""
    return x + y


@ChiaFunction(resources={"crb_res": 1})
def worker_pid() -> int:
    """Return the OS pid of the process that executes this task."""
    return os.getpid()


class TestChiaRemoteBlocking(unittest.TestCase):
    _ray_started_here = False

    @classmethod
    def setUpClass(cls):
        if ray.is_initialized():
            ray.shutdown()
        # address="local" forces a fresh standalone instance (ignoring any
        # RAY_ADDRESS pointing at an existing cluster), so we can declare the
        # custom crb_res resource the tests schedule against.
        ray.init(
            address="local",
            ignore_reinit_error=True,
            num_cpus=2,
            resources={"crb_res": 4},
            runtime_env={
                "working_dir": os.path.dirname(os.path.dirname(__file__)),
            },
        )
        cls._ray_started_here = True

    @classmethod
    def tearDownClass(cls):
        if cls._ray_started_here:
            ray.shutdown()

    # -- value semantics ----------------------------------------------------

    def test_returns_value_not_ref(self):
        """chia_remote_blocking returns the unwrapped value, not an ObjectRef."""
        result = add_one.chia_remote_blocking(4)
        self.assertEqual(result, 5)
        self.assertNotIsInstance(result, ObjectRef)

    def test_kwargs_forwarded(self):
        """Positional and keyword args reach the function."""
        self.assertEqual(add_one.chia_remote_blocking(10, y=5), 15)
        self.assertEqual(add_one.chia_remote_blocking(x=2, y=3), 5)

    def test_matches_local_and_explicit_get(self):
        """Blocking dispatch agrees with a local call and with get(chia_remote())."""
        local = add_one(4)
        explicit = get(add_one.chia_remote(4))
        blocking = add_one.chia_remote_blocking(4)
        self.assertEqual(local, explicit)
        self.assertEqual(explicit, blocking)

    def test_runs_in_worker_not_driver(self):
        """The task executes in a Ray worker, not the driver process."""
        driver_pid = os.getpid()
        task_pid = worker_pid.chia_remote_blocking()
        self.assertNotEqual(task_pid, driver_pid)

    # -- signature preservation (the FastMCP requirement) -------------------

    def test_signature_preserved(self):
        """functools.wraps exposes func's real signature, not (*args, **kwargs)."""
        self.assertEqual(
            inspect.signature(add_one.chia_remote_blocking),
            inspect.signature(add_one._chia_original),
        )
        self.assertEqual(add_one.chia_remote_blocking.__name__, "add_one")
        self.assertEqual(
            add_one.chia_remote_blocking.__doc__,
            add_one._chia_original.__doc__,
        )

    def test_fastmcp_tool_schema_reflects_params(self):
        """Registering via add_tool yields a schema with func's real parameters.

        This is the end-to-end check for the ChiaTool use case: an agent calling
        the tool must see ``x`` and ``y``, not an opaque kwargs blob.
        """
        from mcp.server.fastmcp import FastMCP

        mcp = FastMCP("crb_test")
        mcp.add_tool(add_one.chia_remote_blocking, name="crb_add_one")
        tool = mcp._tool_manager.list_tools()[0]

        self.assertEqual(tool.name, "crb_add_one")
        props = tool.parameters["properties"]
        self.assertIn("x", props)
        self.assertIn("y", props)
        self.assertEqual(props["x"]["type"], "integer")
        self.assertEqual(tool.parameters["required"], ["x"])  # y has a default

    # -- options() override path -------------------------------------------

    def test_options_override_blocking(self):
        """func.options(...).chia_remote_blocking also returns the value."""
        handle = add_one.options(resources={"crb_res": 1})
        self.assertEqual(handle.chia_remote_blocking(7, y=2), 9)


if __name__ == "__main__":
    unittest.main()
