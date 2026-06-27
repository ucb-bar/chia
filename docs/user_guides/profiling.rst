Profiling
=========

CHIA can profile a loop's execution (how long each ``ChiaFunction`` call
took, which worker ran it, and how calls depend on one another) and render the
result as a dependency graph, an interactive timeline, or a CSV table. Profiling
is built around ``ChiaProfiler``, a singleton that instruments every
``ChiaFunction`` call automatically once a collector is running.

Start the collector on the driver, run your loop, then point
the **``chia viz-profile``** CLI at the JSONL log it wrote.

.. contents::
   :local:
   :depth: 1

Enabling profiling
------------------

Start the ``ChiaProfileCollector`` from the driver, after ``ray.init()`` and before any
profiled ``@ChiaFunction`` calls:

.. code-block:: python

   import ray
   from chia.trace.profiler import start_collector

   ray.init(address="auto")

   # Defaults to logging under /tmp/ray/{job_id}.
   start_collector(log_dir="/data/chia_profiles")

   # ... run your @ChiaFunction loop ...

The ``ChiaProfileCollector`` is cleaned up automatically on ``ray.shutdown()`` or process exit; you
can also stop it explicitly:

.. code-block:: python

   from chia.trace.profiler import stop_collector

   stop_collector()   # idempotent

What gets recorded
------------------

Once the collector is running, ``ChiaFunction`` instruments itself with no
changes to your code. Every call emits events keyed by a globally unique
``call_id``:

- ``dispatch`` — a remote call (``chia_remote()``) started on a worker. Carries
  the worker IP / id, node id, Ray ``resources``, and ``obj_ref_deps`` (the
  dependency edges, see below).
- ``complete`` — a remote call's result was retrieved via ``get()``. Carries
  ``exec_time_s`` (high-resolution execution time) plus any ``add_info`` metadata.
- ``local_start`` / ``local_end`` — a ``ChiaFunction`` that ran locally rather
  than on a worker. ``local_end`` also carries ``exec_time_s``.

Dependency edges
~~~~~~~~~~~~~~~~~

The profiler reconstructs the task graph by tracking object identity: when a call
returns a value, it records ``id(value) -> call_id``; when a later call receives
that same object as an argument, it emits a dependency edge (``obj_ref_deps``).
Small interned values (``int``, ``float``, ``str``, ``bytes``, ``bool``,
``None``) are deliberately skipped to avoid false-positive edges.

Attaching custom metadata
-------------------------

From inside any ``@ChiaFunction`` body, call ``add_info`` to merge extra fields
into that call's ``complete`` (or ``local_end``) event. It is thread-local, so
concurrent tasks don't cross metadata:

.. code-block:: python

   from chia.trace.profiler import get_profiler

   @ChiaFunction
   def run_verilator_test(design):
       cycles = ...  # run the sim
       get_profiler().add_info({"simulation_cycles": cycles})
       return result

This is how CHIA's library nodes surface domain metrics — e.g. simulation cycles
and waveform sizes from Verilator runs, or token counts and cost from LLM model
calls — into the profile. The metadata lands under the event's ``extra`` key.

For events that aren't tied to a single call, use ``log_event``:

.. code-block:: python

   get_profiler().log_event("checkpoint", iteration=3, note="post-merge")

Each custom event is timestamped and tagged with the current ``worker_id``
automatically.

Visualizing a profile
---------------------

Render a recorded log with the ``chia viz-profile`` CLI — a dependency graph
(``svg``/``png``/``pdf``), an interactive HTML timeline, or an aggregated CSV
table. See :ref:`cli-viz-profile` in the CLI Reference for the full list of
formats and flags.

.. Notes and gotchas
.. -----------------

.. - **No collector, no overhead.** Every profiler method short-circuits when the
..   collector actor isn't running, so leaving ``add_info``/``log_event`` calls in
..   production code is free until you ``start_collector()``.
.. - **Start it on the driver.** ``start_collector()`` pins the actor to the head
..   node and blocks until it's live; call it once, after ``ray.init()``. It is
..   idempotent — a second call is a no-op if the actor already exists.
.. - **The log is the source of truth.** Events are appended as JSONL and survive
..   the run; ``viz-profile`` reads the file, not a live actor. You can render a
..   profile long after the loop finished.
.. - **Dependency edges rely on object identity.** Edges are inferred from
..   ``id(value)`` matches, so they capture values passed through directly. Interned
..   scalars are skipped on purpose to avoid spurious edges.
.. - **One run per graph.** Graph and HTML formats render a single run (use
..   ``--run`` / ``--gap-threshold`` to pick it); only ``--format table`` aggregates
..   across runs and logs.
