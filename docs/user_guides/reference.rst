CLI Reference
=============

The ``chia`` console script (``chia/cli/main.py``) manages clusters, visualizes
flows, and proxies Ray's CLI (with optional per-cluster targeting).

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

Cluster Status
----------------

``chia status [...]``
~~~~~~~~~~~~~~~~~~~~~~~~~~

Print cluster status. Proxies to ``ray status``.

.. list-table::
   :header-rows: 1

   * - Flag
     - Meaning
   * - ``--chia-cluster {path/to/cluster.yaml}``
     - Specify the Chia cluster YAML file to target.
   * - ``...``
     - all other args (including ``--help``) go to Ray.


``chia list [...]``
~~~~~~~~~~~~~~~~~~~~~~~~~~

List all states of a given resource. Proxies to ``ray list`` (e.g. ``chia list nodes``).

.. list-table::
   :header-rows: 1

   * - Flag
     - Meaning
   * - ``--chia-cluster {path/to/cluster.yaml}``
     - Specify the Chia cluster YAML file to target.
   * - ``...``
     - all other args (including ``--help``) go to Ray.

Job Management
------------------

``chia job <subcommand> [...]``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Submit, stop, delete, or list Ray jobs.

Proxies to ``ray job`` — ``submit``, ``status``, ``logs``, ``list``, ``delete``,
etc. all forward, so ``chia job --help`` shows Ray's own job help. Supports
``--chia-cluster``. The one exception is ``stop``, which
chia *overrides* with an augmented implementation; use ``chia ray job stop`` for
Ray's unmodified behavior.

.. list-table::
   :header-rows: 1

   * - Flag
     - Meaning
   * - ``--chia-cluster {path/to/cluster.yaml}``
     - Specify the Chia cluster YAML file to target.
   * - ``stop {job_id}``
     - Stop a job by its ID.
   * - ``...``
     - all other args (including ``--help``) go to Ray.


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

.. _cli-ray-fallback:

Fallback to Ray (``chia ray ...``)
----------------------------------

``chia ray <anything>`` forwards to ``ray <anything>`` verbatim, so every
current and future Ray command (``ray memory``, ``ray timeline``, ``ray
attach`` …) is reachable — not just the promoted ``status``/``list``/``job``.
Using an explicit gateway also means chia's own ``up``/``down`` never shadow
``ray up``/``ray down``, and ``chia ray --help`` shows Ray's complete command
list.

Supports ``--chia-cluster``. Unlike the promoted
commands, the fallback applies **no** overrides — ``chia ray job stop`` runs
Ray's stop, not chia's augmented one::

   chia ray memory --chia-cluster=cluster_a.yaml
   chia ray timeline
   chia ray job stop <job_id>

FireSim
-------

FPGA build/run orchestration:

- ``chia firesim-build <config.yaml> --recipe NAME [--instance-type ...]``
- ``chia firesim-run <config.yaml> [--hw-config ... --workload ... | --suite ...] [...]``
- ``chia firesim-upload-workload [config.yaml] --marshal-config ... --images-dir ... --suite-name ...``
- ``chia firesim-cleanup <config.yaml>`` — terminate orphaned chia EC2 instances.

Run ``chia <command> --help`` for the full, current option list.
