CLI Reference
=============

The ``chia`` console script (``chia/cli/main.py``) manages clusters, visualizes
flows, and wraps Ray job management.

.. code-block:: text

   chia <command> [options]

Cluster management
------------------

``chia up <config.yaml>``
~~~~~~~~~~~~~~~~~~~~~~~~~~

Bring up a Chia/Ray cluster from a YAML config.

.. list-table::
   :header-rows: 1

   * - Flag
     - Meaning
   * - ``-y``, ``--yes``
     - Skip the confirmation prompt.
   * - ``-v``, ``--verbose``
     - DEBUG logging.
   * - ``--dry-run``
     - Print the plan and generated scripts without executing.
   * - ``--add``
     - Only add new nodes to an existing cluster (skip ones already up).

``chia down <config.yaml>``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tear down the cluster. Flags: ``-y/--yes``, ``-v/--verbose``.

Visualization
-------------

``chia viz <source.py>``
~~~~~~~~~~~~~~~~~~~~~~~~~~

Render the ``@ChiaFunction`` call graph of a flow **statically** from source.

.. list-table::
   :header-rows: 1

   * - Flag
     - Meaning
   * - ``--func NAME``
     - Orchestrator function (auto-detected if omitted).
   * - ``--format {svg,png,pdf}``
     - Output format (default ``svg``).
   * - ``--output-dir DIR``
     - Output directory (default: alongside the source).

.. _cli-viz-profile:

``chia viz-profile <log...>``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Visualize a ``ChiaProfileCollector`` JSONL log. See the
:doc:`/user_guides/profiling` guide for how to record one.

.. list-table::
   :header-rows: 1

   * - Flag
     - Meaning
   * - ``--format {svg,png,pdf,html,table}``
     - Output format (default ``svg``); ``table`` aggregates exec time to CSV.
   * - ``--output-dir DIR``
     - Output dir (ignored for ``table``).
   * - ``--output PATH``
     - (``table``) CSV path; default stdout.
   * - ``--funcs ...``
     - (``table``) function names, or a file of names.
   * - ``--run N``
     - Run index to visualize (default: last).
   * - ``--gap-threshold SEC``
     - Gap that segments runs (default 600).
   * - ``--x-scale F``
     - Horizontal inches per second of wall-clock (default 0.5).

Jobs
----

``chia job <subcommand>`` — ``submit``, ``status``, ``logs``, ``list``, and ``delete`` are
**pass-throughs** to ``ray job`` (so ``--help`` shows Ray's own help). ``stop`` is
custom:

``chia job stop <job_id>``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default a thin wrapper over ``ray job stop``. With ``--kill-tracked-pids``,
it first asks the PID registry actor to kill every tracked subprocess (SIGTERM,
then SIGKILL for any that linger past the grace period), then stops the job. The
kill returns as soon as the processes have actually exited, so it normally
completes in well under the grace period.

.. list-table::
   :header-rows: 1

   * - Flag
     - Meaning
   * - ``--kill-tracked-pids``
     - Kill tracked subprocesses (via the PID registry) before stopping the job.
       Off by default.
   * - ``--grace-period SEC``
     - Seconds to wait for each tracked subprocess to exit after SIGTERM before
       escalating to SIGKILL (only used with ``--kill-tracked-pids``; default 25).

FireSim
-------

FPGA build/run orchestration:

- ``chia firesim-build <config.yaml> --recipe NAME [--instance-type ...]``
- ``chia firesim-run <config.yaml> [--hw-config ... --workload ... | --suite ...] [...]``
- ``chia firesim-upload-workload [config.yaml] --marshal-config ... --images-dir ... --suite-name ...``
- ``chia firesim-cleanup <config.yaml>`` — terminate orphaned chia EC2 instances.

Run ``chia <command> --help`` for the full, current option list.
