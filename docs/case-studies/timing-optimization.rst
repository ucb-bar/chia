BOOM Critical-Path Timing Optimization
======================================

A CHIA case study that uses an LLM-in-the-loop to raise the clock frequency of a
`BOOM <https://docs.boom-core.org/>`_ core by reshaping its Chisel to shorten the
critical path — without sacrificing instructions-per-cycle (IPC). The full flow
lives in ``chia/examples/timing_opt``.

Overview
--------

The maximum frequency of a synthesized core is set by its **critical path** — the
longest register-to-register logic delay. Shortening it is expert, tedious work:
read a synthesis timing report, find the gates on the worst path, and restructure
the RTL (re-pipeline, rebalance muxing, precompute, …) to cut delay — all while
holding cycle behavior fixed, since a faster clock that costs you IPC may be a net
loss. That trade-off is the **Iron Law** of processor performance
(time/program = instructions × cycles/instruction × time/cycle): the win that
matters is the *product* of frequency and IPC, not frequency alone.

``timing_opt`` automates this as a search loop. Each iteration:

#. loads a *parent* design variant (its generated Verilog and Genus timing report)
   from a SQLite store,
#. asks an LLM to grep the timing report and edit the BOOM Chisel to shorten the
   BoomTile critical path,
#. rebuilds the design,
#. **synthesizes** BoomTile and **runs the Verilator suite in parallel** —
   synthesis measures the new worst-case slack, Verilator gates correctness and
   estimates IPC impact, and
#. records the result as a new *child* branch in a tree of variants.

Results accumulate in a SQLite database (``timing.db``), a **multi-branch tree**:
each branch stores its diff against the base RTL, generated Verilog, the Genus
timing report, synthesized area, per-benchmark top-down (TMA) counters, and logs.
You pick which parent to optimize each turn, and the flow produces one child per
invocation — so the search keeps building on its strongest candidates.

.. note:: **Result**

   Over 15 iterations of the flow, we yield more than a 2x increase in frequency
   at only 3.3% IPC loss, for a net Iron-Law performance improvement of 1.97x, in
   the Skywater 130nm process! Read section 5 of our `arXiv paper
   <https://arxiv.org/abs/2606.27350>`_ to hear more about our results.

How it works
------------

The per-iteration pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``run_improve_timing_loop()`` runs one parent → child pass. It stages the parent
exactly (so the tree reproduces it), lets the LLM edit, then validates the edit
with a build, a parallel synth + Verilator step, and a debug-retry loop on
failure::

    DRIVER (one parent -> one child branch)
      |
      +-- load parent diff / generated Verilog / timing report from DB
      +-- acquire chipyard placement group + chipyard_bash tool   [chipyard node]
      +-- stage + reset: write parent Verilog, reset chipyard, re-apply parent diff
      |
      +-- /improve_timing                                          [llm worker]
      |     (grep the staged timing report, edit Chisel, A/B-test
      |      candidate edits with the timing_experiment tool)
      |
      +-- build all thread variants (build-debug retry loop)       [chisel node]
      |
      +-- PARALLEL:
      |     dispatch BoomTile synthesis (async)                    [vlsi worker]
      |     run Verilator suite                                    [verilator nodes]
      |     on Verilator failure: cancel synth, debug_failure, rebuild, retry
      |
      +-- collect synthesis result once Verilator is clean
      +-- persist child branch: reports, area, syn_obj tarball, TMA counters,
            produced timing report (worst-slack columns), parent-vs-child summary

On any failure a ``finally`` block reverts chipyard to the parent diff and records
the status; on success the edits are left in place (already persisted).

The ``timing_experiment`` A/B tool
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A full BoomTile synthesis is expensive, so the LLM does not pay for one to test
each idea. ``TimingExperimentTool`` is an MCP tool that runs a cheap **sub-block**
Genus synthesis on the edited vs. unmodified RTL so the agent can A/B-test an edit
before committing to it. A small sub-block synth can outlast an MCP HTTP
round-trip, so the tool uses a **start / poll** split:

- ``rebuild_verilog()`` re-elaborates Chisel to Verilog (``make verilog``, no C++
  build) and caches it;
- ``list_modules()`` / ``list_modules_parent()`` enumerate valid ``vlsi_top``
  values on the edited / unmodified RTL;
- ``start_synth_child(vlsi_top, …)`` / ``start_synth_parent(vlsi_top, …)``
  dispatch the two sub-block synths and return handles in sub-seconds — issue both
  for a parallel A/B comparison;
- ``synth_status(handle, max_wait_seconds)`` polls an in-flight synth, returning
  ``running`` or the full area + worst-slack summary on completion.

Each experiment is logged to the ``llm_experiments`` table by a head-pinned
``ExperimentLogger`` actor, since the tool's worker cannot see the head's disk.

Distributing work across the cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The build/Verilator host (Chipyard) is held for one iteration via a placement
group, and the ``chipyard_bash`` tool the LLM drives is pinned to the same bundle:

.. code-block:: python

    pg = placement_group([{"CPU": 1, "chipyard": 1}], strategy="STRICT_PACK")
    ray.get(pg.ready())
    pg_opts = {"scheduling_strategy": PlacementGroupSchedulingStrategy(
        placement_group=pg, placement_group_bundle_index=0)}
    chipyard_bash = BashTool(
        name=bash_name, work_dir=CHIPYARD_PATH, task_options=pg_opts,
        timeout_seconds=600,
    )

Synthesis runs on a separate ``vlsi`` worker (requesting the ``VLSI`` / ``Syn``
resources) and the Verilator suite on the ``verilator_run`` nodes, **at the same
time**. Synthesis measures worst-case slack; Verilator is primarily a correctness
gate (and an IPC-degradation estimate). If Verilator fails, the in-flight synth is
cancelled, a shared-session ``debug_failure`` agent repairs the build, and the
step retries up to ``--max-debug-retries`` times.

The loop ships **this repo's** ``chia`` (plus the example packages) to every
worker via Ray ``py_modules``, so workers import the head's checkout regardless of
what their Docker image baked in:

.. code-block:: python

    RUNTIME_ENV = {
        "working_dir": str(_REPO_ROOT),
        "py_modules": [
            str(_REPO_ROOT / "chia"),
            str(_REPO_ROOT / "examples" / "common"),
            str(_REPO_ROOT / "examples" / "sky130_vlsi"),
            str(_REPO_ROOT / "examples" / "timing_opt"),
        ],
        "excludes": ["/DB/", "verilatorbins/ubench/", "__pycache__", ...],
    }

The LLM in the loop
~~~~~~~~~~~~~~~~~~~~

The optimizing agent runs on an ``llm`` worker via
:class:`~chia.models.claude.ClaudeCodeLLM`. It is handed the staged timing report
(too large to inline in the prompt) and two MCP tools: ``chipyard_bash`` to grep
the report and edit Chisel, and the ``timing_experiment`` A/B tool above:

.. code-block:: python

    llm = ClaudeCodeLLM(
        model=model,                       # default: claude-opus-4-8
        timeout_seconds=timeout_seconds,
        log_dir="/tmp/ray/llm_logs",
        logging_name="improve_timing",
        extra_cli_args=["--effort", "max"],
    )
    return llm.prompt(prompt_text, tools)

Three prompt variants ship in ``prompts/`` to steer the Iron-Law trade-off:

- ``improve_timing.md`` (default) — **IPC-neutral** edits only: reshape logic to
  cut the critical path without changing cycle behavior;
- ``improve_timing_ironlaw.md`` — allow IPC-trading moves when they win the
  iron-law product (frequency × IPC); pass via ``--prompt-file``;
- ``improve_timing_ironlaw_noab.md`` — the iron-law variant for runs **without**
  the ``timing_experiment`` A/B tool; pair it with ``--no-experiment-tool``.

The seed flow
~~~~~~~~~~~~~

With an empty DB there is no parent to optimize, so ``main()`` first runs
``seed_flow()``: it resets chipyard to the **unmodified** base RTL (empty diff),
builds, synthesizes BoomTile, runs Verilator for the baseline TMA counters, and
stores the result as the ``baseline`` branch — the first DB entry and the root of
the variant tree. No LLM editing step.

Setup
-----

**Note that running this flow requires creating a logical worker environment with Genus. 
We do not want to expose details of how to do this for the commercial tool 
publicly, but if you have Genus licenses, you should feel free to reach out 
to us for help setting this up.**

These steps mirror the example's ``README.md``. Run them from ``<repo>/chia``
unless noted. Because the synthesis tool (we used Cadence Genus on the open-source
Sky130 PDK) and its collateral are commercial, you must supply your own ``vlsi``
synthesis worker.

**1. Head conda env** — only the head needs it; workers get ``chia`` via Ray
``py_modules`` and the cluster's Docker images. The env is named ``timing_loop``;
fill in its ``- -e /path/to/chia`` line with your checkout first:

.. code-block:: bash

    conda env create -f examples/timing_opt/env.yml
    conda activate timing_loop

**2. Fill in the stubs** — the example ships obvious placeholders
(``/path/to/…``, ``CHANGE_ME_…``, ``${VAR}``) you must replace before the flow
runs end-to-end. See the **Paths to fill in** checklist in the README; the key
ones are the synthesis-tool binary and PDK collateral in
``sky130_vlsi/tools-chia.yml``, the timing-report relpaths and collateral paths in
``constants.py`` (or the matching ``TIMING_OPT_*`` env vars), and the head IP, EC2
key, and ``vlsi`` worker in ``timing_cluster.yaml``.

**3. Benchmarks** — fetch the Verilator test binaries (the suite reads
``asmtests/`` and ``embench/``; ``dramsim_ini/`` ships alongside):

.. code-block:: bash

    git submodule update --init examples/timing_opt/verilatorbins

**4. Bring up the cluster** — ``chia up`` expands ``${HEAD_IP}``, ``${USER}``
The reference topology is seven workers
(2 ``verilator_run`` + 2 ``chisel_build`` + 1 ``llm`` + 2 ``vlsi``):

.. code-block:: bash

    export HEAD_IP=10.0.0.10
    chia up examples/timing_opt/timing_cluster.yaml

The first bring-up is slow — it pulls the large Chipyard / Verilator images. See
:doc:`/user_guides/cluster_config_reference` for the full schema.

**5. Seed the baseline** — with an empty DB the flow builds + synthesizes the
unmodified RTL and stores it as the ``baseline`` branch. ``TIMING_OPT_DB_DIR`` is
**required** and must be a stable absolute head path; the ``chia job submit``
entrypoint runs under Ray's job manager and does **not** inherit your shell, so
pass it (and any non-default collateral paths) via ``--runtime-env-json`` rather
than ``export``. Run from the example dir so ``--working-dir .`` uploads it:

.. code-block:: bash

    cd examples/timing_opt
    chia job submit --working-dir . \
      --runtime-env-json '{"env_vars": {"TIMING_OPT_DB_DIR": "/abs/path/on/head/timing_opt_DB"}}' \
      -- python improve_timing.py --seed-only

**6. Run a timing-optimization iteration** — pick a parent branch to optimize;
each invocation produces one child (``<parent>_timing_v<N>``, auto-incremented).
Reuse the same ``--runtime-env-json`` block so the DB path stays consistent:

.. code-block:: bash

    chia job submit --working-dir . \
      --runtime-env-json '{"env_vars": {"TIMING_OPT_DB_DIR": "/abs/path/on/head/timing_opt_DB"}}' \
      -- python improve_timing.py --branch baseline

(With an empty DB you can skip step 5 — the loop auto-seeds the baseline first,
then optimizes it in the same job. Re-run with the same ``--branch`` to grow the
tree wider with another sibling.)

**7. Inspect results** — the reporting script prints worst slack / achievable
frequency per branch against a target period:

.. code-block:: bash

    python examples/timing_opt/scripts/perf_table.py \
      --db /abs/path/on/head/timing_opt_DB/timing.db --target <ns>

**8. Tear down** — when the run is done:

.. code-block:: bash

    chia down examples/timing_opt/timing_cluster.yaml
