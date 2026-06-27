gem5 ↔ BOOM Microarchitecture Alignment
=======================================

A CHIA case study that uses an LLM-in-the-loop to tune a `gem5
<https://www.gem5.org/>`_ performance model until it matches a target `BOOM
<https://docs.boom-core.org/>`_ core. The full flow lives in
``chia/examples/gem5_align``.

Overview
--------

Architects routinely keep a fast, high-level microarchitectural simulator (gem5) and a cycle-exact
RTL implementation (a BOOM core in Chipyard) of the *same* microarchitecture.
The two drift apart: the gem5 model mispredicts cycle counts because its
configuration — and sometimes its C++ source — no longer reflects the RTL.
**Alignment** is the work of closing that gap, and it's a huge lift, even for large engineering teams. It is nearly impossible for small teams and academics, and usually the simulators do not stay aligned, or are never aligned in the first place.

``gem5_align`` automates it as a search loop. Each iteration:

#. restores a *parent* gem5 source + config state sampled from the best results
   so far,
#. asks an LLM to edit the gem5 configuration and/or ``src/`` to better match
   BOOM,
#. rebuilds gem5,
#. runs a benchmark suite, and
#. compares gem5 cycle counts against cached **Verilator golden** counts to get
   a per-benchmark ``%diff``.

Results are written to a SQLite database (``alignment.db``); the next iteration
samples its parent from the top entries, so the search keeps building on its
strongest candidates. ``N`` iterations run concurrently — one per physical gem5
node — via a Ray placement group.

.. note:: **Result**

   We ran alignment on medium BOOM for 10.5 days, and achieved a gem5 core and
   configuration whose cycle counts were accurate to under 3% on average across our
   benchmarks! Read section 5 of our `arXiv paper
   <https://arxiv.org/abs/2606.27350>`_ to hear more about our results.

How it works
------------

The per-iteration flow
~~~~~~~~~~~~~~~~~~~~~~~~

The head runs ``N`` iterations concurrently — one per physical gem5 node. A
single placement group with ``N`` ``STRICT_SPREAD`` bundles pins each bundle to
a distinct node, and a thread pool on the head drives one iteration per bundle.
When an iteration finishes, the bundle is freed and the next iteration is
dispatched onto it with a freshly sampled parent::

    HEAD THREAD (one per bundle)
      |
      +-- sample parent uniformly from DB.top_k_entries(2)
      +-- restore_gem5_state(parent.config, parent.diff)   [gem5 bundle]
      +-- rebuild_gem5()                                   [gem5 bundle]
      |
      +-- align_node(...)                                  [llm worker]
      |     (analyze parent's results, read BOOM source, edit config/source)
      |
      +-- rebuild_gem5()                                   [gem5 bundle]
      +-- run_gem5_comparison()                            [gem5 bundle]
      |     (debug_node loop on failures)
      |
      +-- persist IterationResult to DB + logs, dispatch next iteration

The Verilator golden cache
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Alignment needs a ground truth to score against. ``ensure_verilator_cache``
builds the target Chipyard config once on a ``chisel_build`` node, runs every
microbenchmark on ``verilator_run`` nodes, and caches the result
(``<bench>.log`` + ``<bench>.out``) on the head. Subsequent runs reuse the
cache, so the expensive RTL simulation happens only once:

.. code-block:: python

    def ensure_verilator_cache() -> bool:
        """If verilator results are not cached, build BUILD_CONFIG and run all benchmarks."""
        benchmarks = sorted(p.stem.replace(".verilator", "")
                            for p in UBENCH_BUILD.glob("*.verilator.riscv"))
        missing = [b for b in benchmarks
                   if not (VERILATOR_CACHE / f"{b}.log").exists()
                   or not (VERILATOR_CACHE / f"{b}.out").exists()]
        if not missing:
            print(f"[verilator cache] All {len(benchmarks)} benchmarks cached. Skipping build.")
            return True
        # else: build BUILD_CONFIG on a chisel node, dispatch every missing
        # benchmark to verilator nodes, and cache <bench>.log / <bench>.out
        ...

Distributing gem5 work across the cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each gem5 node is one ``STRICT_SPREAD`` bundle of a shared placement group, with
a canonical :class:`~chia.simulators.gem5.Gem5Node` pinned to it:

.. code-block:: python

    gem5_pg = placement_group([{"CPU": 1, "gem5": 1}] * N, strategy="STRICT_SPREAD")
    ray.get(gem5_pg.ready())
    bundle_nodes = [Gem5Node(placement_group=gem5_pg, bundle_index=i) for i in range(N)]

The gem5 build / run / restore steps are ordinary CHIA functions tagged with a
*fractional* ``gem5`` resource, so a gem5 op and its co-located tool actors
share a single ``{CPU: 1, gem5: 1}`` bundle rather than each grabbing a whole
node:

.. code-block:: python

    @ChiaFunction(resources={"gem5": 0.5})
    def rebuild_gem5() -> tuple[bool, str, float, str, str, str]:
        ...

The loop ships **this repo's** ``chia`` to every worker via Ray ``py_modules``,
so workers import the head's checkout regardless of what their Docker image
baked in:

.. code-block:: python

    _RUNTIME_ENV = {
        "py_modules": [str(_CHIA_PKG)],   # ship THIS repo's chia to every worker
        "excludes": ["**/__pycache__", "**/*.pyc"],
    }
    ...
    ray.init(address=os.environ.get("RAY_ADDRESS", "auto"), runtime_env=_RUNTIME_ENV)

The LLM in the loop
~~~~~~~~~~~~~~~~~~~~

The aligning agent runs on an ``llm`` worker via
:class:`~chia.models.claude.ClaudeCodeLLM`. It is given the per-benchmark
cycle-count comparison table (gem5 vs Verilator ``%diff``), the benchmark
descriptions, and the run history. To diagnose *why* a benchmark is off, it also
gets two key diagnostic signals:

- **O3PipeView pipeline traces** — gem5 runs under ``--debug-flags=O3PipeView``,
  and the parent iteration's traces are staged on the worker so the agent can
  diff gem5 retire cycles against Verilator commit cycles at matching PCs;
- **performance counters** — BOOM's top-down (TMA) counters from the Verilator
  run (``divider_active``, ``stq_full``, ``dcache_miss``, ``br_mispredict``, …),
  which it cross-checks against the corresponding gem5 ``stats.txt`` counters to
  localize the mis-modeled mechanism.

Alongside these it has MCP tools to read the BOOM Chisel source, edit and
rebuild the gem5 tree, and quick-run gem5 on a few benchmarks to test a
hypothesis. It then proposes edits to the gem5 config and/or C++ source:

.. code-block:: python

    llm = ClaudeCodeLLM(
        model="claude-opus-4-6",
        timeout_seconds=3600,
        logging_name="align",
        resume_session=session_id is not None,
        extra_cli_args=["--effort", "max"],
    )

A separate ``debug_node`` resumes the *same* CLI session if a rebuild or run
fails, so the agent can iteratively fix its own changes within one iteration.

Targeting a different configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The target is the one thing you change to retarget the whole flow. In
``config.py``, ``BUILD_CONFIG`` is the full Chipyard config class (built on the
chisel node and surfaced to the LLM) and ``CONFIG_SLUG`` suffixes on-disk
artifacts so multiple targets can coexist:

.. code-block:: python

    BUILD_CONFIG = "SmallBoomV3HumanCommitLogTMAConfig"  # full Chipyard config class
    CONFIG_SLUG  = "smallboom"                            # suffixes on-disk artifacts

Setup
-----

These steps mirror the example's ``README.md``. Run them from ``<repo>/chia``
unless noted.

**1. Head conda env** — run from the example directory:

.. code-block:: bash

    cd examples/gem5_align
    conda env create -f env.yml
    conda activate gem5-align

Only the head needs this environment; workers get chia via Ray ``py_modules``
and run the Docker images named in ``cluster.yaml``. If you rename the env,
update ``cluster.yaml`` to match.

**2. Cluster** — in ``cluster.yaml``, set ``provider.head_ip`` (the host running
``chia up`` / the Ray head) and each node type's ``compatible_ips``. The
required worker counts (``max_workers``):

.. list-table::
   :header-rows: 1
   :widths: 20 12 68

   * - Node type
     - Workers
     - Role
   * - ``llm``
     - 6
     - aligning/debugging LLM (light)
   * - ``chisel_build``
     - 1
     - builds the verilator golden cache (heavy — Chipyard)
   * - ``verilator_run``
     - 4
     - runs verilator goldens
   * - ``gem5_worker``
     - 6
     - builds + runs gem5

``compatible_ips`` is the **pool** of hosts a type's workers may run on, not a
1:1 list — multiple workers (and multiple node types) co-locate on one host as
separate containers, so you need fewer hosts than the worker total.
``head_ip`` is a single string; ``compatible_ips`` is a list (``${ENV_VAR}`` is
expanded):

.. code-block:: yaml

    provider:
        head_ip: 10.0.0.10
    available_node_types:
        gem5_worker:
            compatible_ips: ["10.0.0.11", "10.0.0.12"]   # inline, block, or ${VAR}

Every listed host must be reachable via the SSH credentials in the top-level
``auth`` section (``ssh_user``, plus optional ``ssh_private_key`` path), have
Docker, and run an SSH agent. The ``llm`` hosts bind-mount your Claude Code
config via the ``-v <dir>:/home/ray/.claude`` run option. All four node types
are required. See :doc:`/user_guides/cluster_config_reference` for the full
schema.

**3. Benchmarks** — fetch the examples/benchmarks submodule, then compile the ``ubench`` suite the
loop runs (with the step-1 ``gem5-align`` env active — it provides the
``riscv64-unknown-elf-`` toolchain):

.. code-block:: bash

    git submodule update --init examples/benchmarks
    cd examples/benchmarks/ubench && ./compile.sh   # -> build/<bench>.{gem5.elf,verilator.riscv}

Without ``build/`` populated, the verilator cache step finds zero benchmarks and
silently skips (see the note above).

**4. Config** — in ``config.py``, set ``BUILD_CONFIG`` + ``CONFIG_SLUG``
together to your target. Optional env overrides: ``GEM5_ALIGN_BENCH_ROOT``,
``GEM5_ALIGN_LOG_DIR``.

**5. Bring up the cluster** — from ``<repo>/chia`` with the env active:

.. code-block:: bash

    chia up examples/gem5_align/cluster.yaml

This can take a while on the first run, since the Chipyard Docker image is large
to pull. On slow links the pull may time out; raise the pull timeout and retry
(progress is saved if you restart quickly).

**6. Launch the alignment job** — ``GEM5_ALIGN_VERILATOR_CACHE`` is **required**
and names the verilator golden cache on the head (an existing dir to reuse, or a
fresh writable dir to generate on the first run). The entrypoint runs under
Ray's job manager and does **not** inherit your shell, so pass it via
``--runtime-env-json`` rather than ``export``. Run from the example dir so
``--working-dir .`` uploads its files:

.. code-block:: bash

    cd examples/gem5_align
    chia job submit --working-dir . \
      --runtime-env-json '{"env_vars": {"GEM5_ALIGN_VERILATOR_CACHE": "/abs/path/on/head"}}' \
      -- python gem5_align_loop.py

Add ``GEM5_ALIGN_BENCH_ROOT`` / ``GEM5_ALIGN_LOG_DIR`` to ``env_vars`` only if
overriding their defaults.

**7. Tear down** — when the run is done:

.. code-block:: bash

    chia down examples/gem5_align/cluster.yaml

Outputs land in ``GEM5_ALIGN_LOG_DIR`` (default
``examples/gem5_align/align_loop_logs-<slug>/``): per-iteration ``iter_N/``
artifacts, ``alignment.db``, and TensorBoard ``metrics/``. Re-running resumes
from a non-empty ``alignment.db``.
