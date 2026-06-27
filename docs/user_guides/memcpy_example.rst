Example: Agentic RoCC Accelerator (MemCpy)
==========================================

A worked, end-to-end example of an agentic hardware-design loop: an LLM
(Claude Code) designs a RISC-V RoCC accelerator in Chisel, CHIA builds it
into a MegaBoom SoC, runs it against a bare-metal test, and, on any failure, 
feeds the error back to the LLM to debug and retry until the test passes.

The example lives at ``examples/memcpy/`` and is essentially self-contained: it
depends only on the installed ``chia`` package and the shared DRAMSim2 ini files
in ``examples/common/dramsim_ini``. It is a good starting point for building your
own generate, build, simulate, debug loops on top of CHIA's chipyard nodes.

.. contents::
   :local:
   :depth: 1

What it does
------------

The accelerator is a hardware ``memcpy``: it copies an array of 64-bit elements
from a source region to a destination region, driven by two custom
instructions on opcode ``custom1``. The target design is
``MegaBoomV3HumanCommitLogConfig`` (MegaBoom V3 with the human-readable
commit-log harness, so a failing run yields a readable instruction trace for
the debugger).

The loop runs two nodes in parallel, then builds, runs, and debugs:

.. code-block:: text

   test build  (parallel)        implement  (parallel)
     copy memcpy.c into            Claude writes memcpy.scala
     $chipyard/tests, run cmake    (the RoCC accelerator) and
     -> build/memcpy.riscv         wires it into the target
        build/memcpy.dump          config, via chipyard_bash
        └──────────────┬──────────────┘
                       ▼
         chisel build  (ChiselBuildNode)     target: MegaBoomV3HumanCommitLogConfig
                       ▼
         verilator run (VerilatorRunNode)    memcpy.riscv, +loadmem +verbose
                       ▼
        build failed / sim failed / incorrect?
           │ yes (≤ NUM_DEBUG_ATTEMPTS)        │ no
           ▼                                   ▼
        debug (Claude) ── rebuild + rerun     DONE (passed)

Components
----------

.. list-table::
   :header-rows: 1
   :widths: 22 78

   * - File
     - Role
   * - ``memcpy_loop.py``
     - Main orchestration: parallel test-build + implement, then the
       build → run → debug loop. Dumps all collateral to ``out/``.
   * - ``test_build.py``
     - ``build_test`` :class:`~chia.base.ChiaFunction.ChiaFunction` — copies
       ``memcpy.c`` into ``$chipyard/tests``, registers a CMake target, builds
       ``build/memcpy.riscv`` + ``build/memcpy.dump``, and reads them back.
       Runs on the chipyard container.
   * - ``claude.py``
     - The implement + debug LLM nodes (:class:`chia.models.claude.ClaudeCodeLLM`)
       and the failure-feedback formatters. LLM calls are dispatched onto the
       dedicated ``llm`` (claude) node, sharing one session.
   * - ``helpers.py``
     - Run-outcome classification (``classify_run``), the ``out/`` dumper, the
       chipyard git-diff node (``collect_diff``), and dramsim-ini loading.
   * - ``prompts/``
     - The implement (``implement.md``) and debug (``debug.md``) prompt text,
       with ``${VAR}`` placeholders filled from ``constants`` at load time.
   * - ``constants.py``
     - Every tunable knob (loop counts, configs, paths, timeouts, resources).
   * - ``cluster.yaml``
     - Minimal cluster: one chisel-build, one verilator, one claude (``llm``)
       node.
   * - ``memcpy.c``
     - The bare-metal test: issues the two RoCC instructions and checks the copy.

The accelerator contract
------------------------

``memcpy.c`` drives the accelerator at opcode ``custom1`` with two
instructions; the implement prompt specifies exactly this so the generated
design matches the test:

* ``funct == 0`` — ``rs1`` = source base address, ``rs2`` = destination base
  address. Latch both; write ``rd = 1``.
* ``funct == 1`` — ``rs1`` = array length (number of 64-bit elements). Copy the
  whole array source → destination via the RoCC memory port; write ``rd = 1``
  when done.

Correctness is judged from the ``MEMCPY Num Correct: N`` line the program
prints: the run passes iff ``N == DATA_SIZE`` (``constants.DATA_SIZE``, kept in
sync with ``memcpy.c``).

Running it
----------

The bundled ``cluster.yaml`` brings up three node types — one chisel-build
(``chipyard``), one verilator (``verilator_run``), and one claude (``llm``)
node. The ``llm`` node mounts your Claude Code credentials; see the note at the
top of ``cluster.yaml``.

.. code-block:: bash
  
   export THIS_MACHINE="your_machine_ip"
   chia up   examples/memcpy/cluster.yaml
   chia job submit -- python $PWD/examples/memcpy/memcpy_loop.py   # run from the repo root
   chia down examples/memcpy/cluster.yaml

Pass the absolute path to ``memcpy_loop.py``. The driver runs on the cluster head,
where the repo lives, so ``out/`` is written into the real
``examples/memcpy/out``.

The LLM calls run on the dedicated ``llm`` node:
:meth:`chia.models.claude.ClaudeCodeLLM.prompt` is itself a ``ChiaFunction``, so
the loop dispatches it with ``llm.prompt.options(resources={"llm": 1.0})`` and
threads the session transcript from each call into the next, so the debugger
resumes the implement conversation. (Session persistence for other backends is
in development.)

Tunable parameters
-------------------

All knobs live in ``constants.py``; container paths and cluster knobs are
``MEMCPY_*`` environment-overridable, so nothing is hardcoded into the loop.

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Constant
     - Meaning
   * - ``NUM_DEBUG_ATTEMPTS``
     - Max debug-and-retry rounds after the first failure (default 3).
   * - ``NUM_PERF_OPT_ITERS``
     - Reserved for a future post-correctness performance-optimization phase
       (defined, not yet used).
   * - ``BUILD_CONFIG``
     - Chisel config to build (``MegaBoomV3HumanCommitLogConfig``).
   * - ``DATA_SIZE``
     - Element count; must match ``memcpy.c``.
   * - ``VERILATOR_TIMEOUT_CYCLES`` / ``_SECONDS``
     - Simulation caps so a hung design fails fast.
   * - ``*_RESOURCE``
     - Ray scheduling tokens, sized so the persistent ``chipyard_bash`` actor
       coexists with the builds.

Debug feedback
--------------

On a failure the debug node (the same Claude session, resumed) receives:

* **Build failure** — build stderr tail plus stdout windowed on the first
  ``error``.
* **Simulation failure** (runtime / timeout / incorrect) — the simulator
  stdout (the commit log) and spike-dasm output tails, plus the last
  ``COMMIT_LOG_TAIL_LINES`` lines of the commit log and the last
  ``DUMP_TAIL_LINES`` lines of ``memcpy.dump`` (the test disassembly).

Output and per-iteration diffs
------------------------------

Every node result and piece of collateral is written to ``out/``, each filename
prefixed with the timestamp at the moment it is written (so files sort by when
they were produced):

.. code-block:: text

   20260626_144501_implement.md
   20260626_144502_test_build_memcpy.riscv
   20260626_144502_test_build_memcpy.dump
   20260626_144503_chisel_diff_attempt0.diff       # chipyard diff for this iteration
   20260626_144503_chisel_diff_attempt0.json       # per-repo diff dict
   20260626_145012_chisel_build_attempt0.stdout.txt
   20260626_145230_verilator_run_attempt0.log
   20260626_145231_feedback_attempt1.md
   20260626_145950_debug_attempt1.md
   20260626_153044_summary.json

Each iteration's Chisel diff is captured with ``collect_diff`` (in
``helpers.py``) just before that attempt's build, so it reflects the exact
source built from the implement node's work on attempt 0, and the cumulative
implement + debug edits on later attempts.