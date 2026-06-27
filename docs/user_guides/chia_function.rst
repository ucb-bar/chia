ChiaFunction
============

``ChiaFunction`` is the core primitive of a CHIA flow. The ``@ChiaFunction``
decorator turns *any* Python function into a **node**, a unit of work that can
be scheduled onto a worker in the cluster, chained into a task graph, profiled,
cached, and bypassed. This page explains the Ray concepts the rest of the docs
lean on, then walks through every mode of execution a ``ChiaFunction`` supports.

.. contents::
   :local:
   :depth: 2

A few Ray concepts
------------------

CHIA is built on the `Ray <https://www.ray.io/>`_ distributed-computing
platform, and the docs use Ray's vocabulary throughout. You do not need to know
Ray to use CHIA, but these terms recur:

- **Driver** — the process running your flow script (the ``main()`` you submit
  with ``python ...`` or ``chia job submit ...``). It runs on the cluster's head node, dispatches
  work, and collects results.

- **Worker** — a process that executes dispatched work. In CHIA a logical
  worker also advertises resources (CPU/GPU/FPGA counts, software
  capabilities) and only maps onto physical machines that can satisfy them. See
  :doc:`/concepts/overview` for how logical workers map onto machines.

- **Task** — a single asynchronous function invocation sent to a worker.
  Dispatching a ``@ChiaFunction`` with ``fn.chia_remote(...)`` creates a task.
  Tasks are stateless: the worker runs the function and returns the result.

- **Actor** — a stateful worker: a remote Python object whose methods run on the
  worker that holds it. CHIA uses actors for things that must persist across many
  calls, like the profile collector, the cache, and MCP tool servers.

- **Object reference (``ObjectRef``)** — a *future*: a handle to a result that
  may not exist yet. ``chia_remote(...)`` returns immediately with an
  ``ObjectRef[R]`` instead of blocking for the value. You resolve it later with
  ``get()``, or pass it straight into another call as an argument.

- **Resources** — labels with quantities a task requires (``{"chipyard": 1}``)
  and a worker advertises. Ray holds a task until a worker with enough free
  resources is available, then leases that worker for the task's duration. This
  is how CHIA places work on the right machine.

Executing a CHIA node
----------------------

CHIA provides a library of nodes decorated with ``@ChiaFunction``. Most of them
follow the same shape: a plain Python **class** that bundles the node's
construction parameters, instance state, and private helpers, with one (or more)
method decorated with ``@ChiaFunction(...)`` that is the actual dispatchable
node. The decorated method comes pre-assigned with the resources it needs.

:mod:`chia.chipyard.chisel_build_node` is a representative example. The
``ChiselBuildNode`` class holds the node's build configuration (chipyard path,
config, target, make flags) and a handful of private helpers. Its ``build``
method is the node, pre-assigned the ``chipyard`` resource:

.. code-block:: python

   class ChiselBuildNode:
       def __init__(self, chipyard_path, config,
                    target=BuildTarget.VERILATOR, ...):
           ...                              # instance state

       @ChiaFunction(resources={"chipyard": 1})
       def build(self) -> BuildArtifact:
           ...                              # runs `make` in sims/verilator
           return BuildArtifact(...)

When you call ``build`` you can choose how to execute it: locally in the driver,
remotely on a worker, and with or without blocking for the result. The
pre-assigned resource options can also be overridden per-call. The rest of this
section walks through each mode using this node.

Local call
~~~~~~~~~~~

Calling the bound method directly runs it in the same process as made the call, exactly
like an ordinary Python call:

.. code-block:: python

   cb_node = ChiselBuildNode("/home/ray/chipyard", "RocketConfig",
                             target=BuildTarget.VERILATOR)
   artifact = cb_node.build()            # runs here, in the driver

This is handy for using ChiaFunction features like profiling, bypassing, and
caching without dispatching the function to a worker. 

Remote, asynchronous (``chia_remote``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``fn.chia_remote(...)`` dispatches the function as a task onto a worker that
satisfies its resources. It is non-blocking and returns an ``ObjectRef``
immediately. Resolve and block on the ref with ``get()`` when you need the value:

.. code-block:: python

   ref = cb_node.build.chia_remote(cb_node)   # note: pass `cb_node` explicitly
   # ... dispatch other work here; it runs concurrently ...
   artifact: BuildArtifact = get(ref)         # blocks until the build finishes

Remote, blocking (``chia_remote_blocking``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When you want the value synchronously and have no other work to overlap,
``chia_remote_blocking`` dispatches remotely and returns the unwrapped value:

.. code-block:: python

   artifact: BuildArtifact = cb_node.build.chia_remote_blocking(cb_node)

.. _chaining-refs:

Chaining refs into a task graph
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Any argument to a ``chia_remote`` call may be either a plain value of type ``T``
*or* an ``ObjectRef[T]``. Passing a ref directly without ``get()`` tells Ray
that this task depends on that one, forming an explicit edge in the task graph.
Ray resolves the dependency for you and only starts the downstream task once its
inputs are ready, so independent tasks run concurrently. Dispatch everything, wire refs
together, and ``get()`` only the final result:

.. code-block:: python

   # No get() between these — refs flow straight in as arguments.
   bin_ref  = compile_program.chia_remote(c_src)
   build_ref = cb_node.build.chia_remote(cb_node)

   # run() depends on BOTH upstream tasks; Ray waits for them automatically.
   result = get(
       verilator_node.run.chia_remote(
           verilator_node, build_ref, bin_ref, "helloworld.riscv", "/home/ray"
       )
   )

The :doc:`/getting-started/quickstart`
builds up exactly this pattern step by step.

.. _per-call-options:

Per-call option overrides (``.options``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To override the decorator-level options for a single dispatch, use
``.options(...)``:

.. code-block:: python

   ref = cb_node.build.options(
       num_cpus=4, scheduling_strategy="SPREAD"
   ).chia_remote(cb_node)

Any keyword that Ray's ``.options()`` accepts is supported. The
ones CHIA flows reach for in practice are:

- **``resources``** — the resource labels (and quantities) a worker must offer
  to run the node. E.g. tag a function with ``@ChiaFunction(resources={"chipyard": 1})``
  needs a whole chipyard slot.

- **``num_cpus``** — how many CPUs the task reserves. Number of CPUs per machine is automatically discovered in cluster setup and advertised to the cluster. E.g. a chipyard node that needs 2 threads: ``@ChiaFunction(num_cpus=2, resources={"chipyard": 1})``, with default value 1.

- **``max_retries``** — how many times a task is rerun if the worker it ran
  on dies. ``0`` disables automatic retry. E.g. ``@ChiaFunction(num_cpus=0.1, max_retries=0)``, with default value 3. 

- **``scheduling_strategy``** — how tasks are scheduled across nodes. Common values are ``"DEFAULT"``, which prioritizes locality then load balancing, and
  ``"SPREAD"`` to fan tasks out evenly. E.g.
  ``run_synthesis.options(scheduling_strategy="SPREAD").chia_remote(design)``.
  This option also accepts a Ray scheduling-strategy *object*,
  most often a ``PlacementGroupSchedulingStrategy``, which binds the task to a
  placement group so a set of related ``@ChiaFunction`` calls gang-schedule onto the
  same (or co-located) logical worker(s) instead of being placed independently.

A bare ``.options()`` (no resources) can run on any worker.

Functional form (``ChiaCallRemote``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ChiaCallRemote(fn, *args, **kwargs)`` is equivalent to
``fn.chia_remote(*args, **kwargs)`` but raises a clear ``TypeError`` if ``fn``
is not a ``@ChiaFunction``. Use whichever reads better at the call site:

.. code-block:: python

   from chia.base.ChiaFunction import ChiaCallRemote

   ref = ChiaCallRemote(compile_program, src_contents)

Collecting results
------------------

``get()`` is the basic collector and is usually all you need. For flows that
dispatch many tasks and want to react as each finishes, or that run long enough
to hit a wedged worker, CHIA provides ``chia_wait``, a drop-in replacement for
``ray.wait`` that operates on :class:`TrackedRef` objects (an ``ObjectRef``
paired with a closure that can re-dispatch it). It returns the usual
``(ready, pending)`` split and can additionally detect tasks stuck in
``PENDING_NODE_ASSIGNMENT`` while the cluster has free resources, then cancel and
resubmit them:

.. code-block:: python

   from chia.base.ChiaFunction import chia_wait, TrackedRef

   tracked = [
       TrackedRef(compile_program.chia_remote(s),
                  submit_fn=lambda s=s: compile_program.chia_remote(s),
                  label=f"compile_{i}")
       for i, s in enumerate(sources)
   ]

   # Return when at least num_returns tasks are ready, or after pending_timeout seconds of no progress.
   ready, pending = chia_wait(tracked, num_returns=1,
                              pending_timeout=120, retry=True)
   for tr in ready:
       result = get(tr.ref)

Cancellation
~~~~~~~~~~~~

``chia_cancel(ref)`` cancels a running task. Unlike a bare ``ray.cancel``, it
first looks up any subprocesses the task spawned and kills them on the correct
remote nodes (process-group kill for ``start_new_session`` children) before
cancelling the Ray task, part of CHIA's process-leak prevention:

.. code-block:: python

   from chia.base.ChiaFunction import chia_cancel

   ref = build_megaboom.chia_remote(config)
   chia_cancel(ref, force=True)

Worker-side setup and cleanup hooks
-----------------------------------

A ``chia_remote`` call may carry reserved kwargs that run side-effecting
callables on the worker around the function: ``_chia_setup`` (runs *before* the
function) and ``_chia_cleanup`` (runs *after*, in a ``finally``, so it fires even
if the function raises). Each takes an optional ``_chia_setup_args`` /
``_chia_cleanup_args`` tuple. Neither can see or replace the function's return
value:

.. code-block:: python

   ref = run_sim.chia_remote(
       design,
       _chia_setup=mount_scratch, _chia_setup_args=(scratch_dir,),
       _chia_cleanup=unmount_scratch, _chia_cleanup_args=(scratch_dir,),
   )

If setup raises, the function and cleanup are skipped and the task fails. If
cleanup raises, the error is logged but never re-raised, so a failing teardown
cannot mask the function's result or its own exception.

Defining your own node
-----------------------

Decorate any function with ``@ChiaFunction``. The ``resources`` argument (and any
other keyword) names what a worker must offer to run it:

.. code-block:: python

   from chia.base.ChiaFunction import ChiaFunction, get

   @ChiaFunction(resources={"chipyard": 1})
   def compile_program(src_contents: str) -> bytes:
       ...
       return elf_data

Wrapping actors (``chia_actor``)
--------------------------------

A plain Ray actor handle can be given the same call surface as a
``ChiaFunction`` with ``chia_actor``, so actor calls read like node dispatch:

.. code-block:: python

   from chia.base.ChiaFunction import chia_actor, get

   store = chia_actor(some_actor)
   n = get(store.size_bytes.chia_remote())
   get(store.write.chia_remote(key, value))

We currently do not support profiling,
bypass, or cache machinery on actors. Recover
the raw Ray handle with ``store.actor`` (e.g. for ``ray.kill``).

Cross-cutting features
----------------------

The same ``chia_remote`` dispatch path also drives three of CHIA's framework
features, all of which are transparent to your function's body:

- **Profiling.** Once a collector is running, every ``ChiaFunction`` function call is
  instrumented automatically. Execution time, the worker it ran on, and the
  dependency edges between calls are recorded. Call ``get_profiler().add_info(...)`` from
  inside a node body to attach domain metrics. See
  :doc:`/user_guides/profiling`.

- **Caching and bypass.** Pass a per-call ``_chia_tag`` to name a dispatch. A
  function marked ``cache: true`` then has its result persisted to disk under
  that tag, and a function marked ``bypass: true`` can replay a pre-recorded
  value (or a registered provider's output) instead of running — while *still*
  dispatching through Ray so scheduling is exercised. See
  :doc:`/user_guides/caching_and_bypass`.

  .. code-block:: python

     # The _chia_tag names this call for both caching and bypass.
     ref = run_verilator_test.chia_remote(design, _chia_tag=f"iter{i}_opt{j}")

These features compose: a single dispatch can be profiled, served from cache,
and relayed through the head's dispatch proxy (on reverse-tunneled workers) all
at once, with no change to the decorated function.

See also
--------

- :doc:`/getting-started/quickstart` — a hands-on flow that uses every mode above.
- :doc:`/concepts/overview` — how nodes, edges, workers, and clusters fit together.
- :doc:`/user_guides/caching_and_bypass` — the ``_chia_tag`` cache/replay workflow.
- :doc:`/user_guides/profiling` — recording and visualizing a flow's execution.
- :ref:`cli-viz-profile` — rendering a recorded profile from the CLI.
