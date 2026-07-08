RISC-V ISA Extension Implementation
===================================

A CHIA case study that uses an LLM-in-the-loop to implement a RISC-V ISA
extension in a `BOOM <https://docs.boom-core.org/>`_ core and prove it correct
against `Spike <https://github.com/riscv-software-src/riscv-isa-sim>`_, the golden
ISA simulator. The full flow lives in ``chia/examples/riscv_extensions``.

Overview
--------

Proposing and evaluating microarchitectural improvements is a fundamental part of computer architecture research. Cross-stack approaches often propose new ISA extensions backed by additional microarchitectural features. Designing the RTL is usually the easy half, leaving thorough verification and PPA analysis (performance and ASIC quality-of-results collection) as the difficult task.

``riscv_extensions`` automates both halves as a loop. For one extension, each run:

#. resets BOOM and lets an LLM implement the extension in Chisel, guided by the
   ratified spec that is given as an input;
#. evaluates on **directed tests** — one program per instruction in the extension,
   run differentially against Spike;
#. checks functional correctness on the full **base-ISA regression** suite, so the edit can't break anything else;
#. **stress-tests** with programs that riscv-dv generates — a random mix of the
   extension's instructions and ordinary code — co-simulated against Spike in
   lockstep for several million instructions; and
#. **synthesizes** the BoomTile with and without the extensioh implementation to measure the area/timing (PPA) cost of the extension.

Spike is the oracle throughout. It is compiled *into* the simulator (cospike) and
checks every committed instruction, so a mismatch is caught the cycle it happens —
not inferred from a final result. A divergence in the stress phase hands the
failing trace to a debug LLM, which fixes the RTL; then the verify-then-stress
process restarts, and a fresh clean stress run is the bar to converge.

Results accumulate as per-extension **sweeps** in the log DB: each sweep stores the
BOOM diff that implements the extension, the LLM transcripts, per-test sim logs,
the PPA delta against the unmodified core, and the generated stimulus.

.. note:: **Result**

   We implemented and verified three extensions on a MegaBOOM core —
   Bit-Manipulation (Zba/Zbb/Zbc/Zbs), scalar Cryptography (Zbk\*/Zkn\*), and
   Zicond — each proven against Spike. They deliver ~5.6% and ~3.5% speedups
   (plus up to 10× on OpenSSL crypto), with no timing regressions and only modest
   area overhead in SkyWater 130nm. Read more in our
   `arXiv paper <https://arxiv.org/abs/2606.27350>`_.

How it works
------------

The per-run pipeline
~~~~~~~~~~~~~~~~~~~~~~

``run_vext_loop()`` drives one extension from a clean tree to a converged,
synthesized result. Implementing and testing are one step — a regression failure
feeds straight back to the LLM — and the stress phase only runs once both the
directed tests and the regression suite pass::

    DRIVER (one extension -> one sweep)
      |
      +-- reset BOOM + build the per-instruction directed tests
      |     (from specs/<ext>/instructions.json)
      |
      +-- IMPLEMENT                                       [llm worker]
      |     implement-LLM edits BOOM Chisel -> build the cospike sim ->
      |     run the directed tests vs Spike; iterate until all pass
      |
      +-- REGRESSION  run the full base-ISA riscv-test suite on the DUT
      |
      +-- STRESS TEST                                     [gen + cosim nodes]
      |     riscv-dv generates random programs mixing the extension;
      |     stream them through lockstep cosims (Spike vs BOOM).
      |     divergence -> debug-LLM fixes the RTL -> restart verify + stress
      |
      +-- PPA synth: BoomTile area/slack, baseline vs implemented  [vlsi worker]
      +-- archive the sweep (diff, transcripts, sim logs, PPA) to the DB

Differential verification against Spike
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Every test — directed or random — is judged the same way: run it on the DUT with
Spike riding inside the simulator, and fail at the first architectural
disagreement. :class:`~chia.chipyard.cosim_node.CosimNode` runs one ELF in
lockstep; the embedded Spike's ISA string comes from the DUT's own config, so the
oracle always decodes exactly what the core claims to implement. No expected
output is ever hand-encoded — the golden model *is* the check.

Directed tests: every instruction, once
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The directed tests are built from ``specs/<ext>/instructions.json``, the
extension's closed instruction set (generated from `riscv-opcodes
<https://github.com/riscv/riscv-opcodes>`_). ``isa_tests`` emits one minimal
program per instruction — a handful of executions over random operands — and
cross-compiles each to an ELF. Coverage is 100% by construction: the table *is*
the instruction set, so every instruction is exercised and checked against Spike.

The random stress test
~~~~~~~~~~~~~~~~~~~~~~~~

Directed tests prove each instruction in isolation; the stress phase proves it in
context. riscv-dv generates large random programs that weave the extension's
instructions through ordinary code — hazards, loops, memory traffic — and streams
them through parallel lockstep cosims. Generators fill a pool on the database node
from the run's start; cosims drain it, marking each test ``pending -> passed``. A
single divergence resets the pool, so after the debug LLM fixes the RTL the whole
batch re-runs and must pass clean.

Generating the extension's instructions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

riscv-dv can generate bitmanip, but has no generator for scalar crypto or Zicond.
Each extension therefore names a riscv-dv **custom target** it generates against:

.. code-block:: python

    EXTENSIONS = {
        "bitmanip": Extension(name="bitmanip", isa_suffix="_zba_zbb_zbc_zbs", ...),
        "crypto":   Extension(..., isa_suffix="_zbkb_zbkc_zbkx_zknd_zkne_zknh",
                              dv_target="riscv_dv_target_crypto"),
        "zicond":   Extension(..., isa_suffix="_zicond",
                              dv_target="riscv_dv_target_zicond"),
    }

For crypto and Zicond the target defines the missing instructions as riscv-dv
custom (``RV64X``) instructions, which the generator node installs into riscv-dv
before generating so they mix into the random stream like any ALU op; bitmanip
uses riscv-dv's native Zb\* support. The SV generation flow needs an Xcelium
license, so the flow also ships a small set of committed prebuilt programs
per extension: ``--prebuilt-stress`` seeds the pool from those instead of
generating, letting the stress phase run with no license.

Distributing work across the cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each extension pipeline holds one Chipyard build host for the duration of a run
via a placement group, and the LLM's editor tool is pinned to the same bundle so
its edits land in that host's BOOM tree. Generation, cosim, and synthesis fan out
onto their own node types by resource tag — ``gen_to_pool`` onto the riscv-dv
generators, cosims onto ``verilator_run`` nodes, BoomTile synthesis onto a
``vlsi`` worker. The loop ships **this repo's** ``chia`` and example packages to
every worker via Ray ``py_modules``, so workers run the head's checkout regardless
of what their Docker image baked in.

The LLM in the loop
~~~~~~~~~~~~~~~~~~~~

Both the implementing and debugging agents run on an ``llm`` worker via
:class:`~chia.models.claude.ClaudeCodeLLM`, with a bash tool to edit BOOM and a
spec tool to read the ratified text. Sessions resume across turns so an agent can
iterate on its own changes:

.. code-block:: python

    ClaudeCodeLLM(
        model=LLM_MODEL,
        system_message=open(PROMPTS_DIR / prompt_file).read(),
        resume_session=True, projects_cwd=None,
        extra_cli_args=LLM_EXTRA_ARGS,          # ["--effort", "max"]
    )

The implement agent (``prompts/system.md``) writes the extension; on a stress
divergence a debug agent (``prompts/debug.md``) gets the failing lockstep trace
and repairs the RTL.

Adding an extension
~~~~~~~~~~~~~~~~~~~~~

One prerequisite: **Spike and the toolchain must already implement the
extension.** Spike is the oracle, and the directed tests are assembled with
``-march=…<ext>``, so the standard tools have to know the instructions — the flow
verifies a BOOM implementation, it doesn't invent the ISA.

Then there are three things to add.

**1. Register it** in ``constants.py`` with its ISA-string suffix (and a custom
``dv_target`` only if riscv-dv can't generate it):

.. code-block:: python

    "zicond": Extension(name="zicond", isa_suffix="_zicond",
                        dv_target="riscv_dv_target_zicond"),

**2. Add the spec + instruction table** under ``specs/<ext>/``:

.. code-block:: text

    specs/<ext>/instructions.json   # the closed instruction set (from riscv-opcodes)
    specs/<ext>/spec.md             # OR spec.pdf — the spec tool renders a PDF as-is

**3. Add stress collateral only if riscv-dv can't already generate the
extension.** Bitmanip uses riscv-dv's native Zb\*, so there's nothing to add.
Crypto and Zicond don't exist in riscv-dv, so each ships a custom target — copy an
existing ``riscv_dv_target_<ext>/`` and edit the four files that matter:

.. code-block:: text

    riscv_dv_target_<ext>/
      riscv_core_setting.sv                  # enable the RV64X custom group
      testlist.yaml                          # the random program shapes
      isa/custom/riscv_custom_instr_enum.sv  # one enum name per instruction
      isa/custom/rv64x_instr.sv              # DEFINE_CUSTOM_INSTR per instruction
      isa/custom/riscv_custom_instr.sv       # render each to assembly (convert2asm)

(The copy also carries ``isa/custom/rv32x_instr.sv``, a stub — our cores are
64-bit.) Point the registry entry's ``dv_target`` at the new directory.

Everything else — directed tests, the regression suite, stress generation, cosim,
synthesis — is driven off the registry entry.

Setup
-----

Run these from ``<repo>/chia`` unless noted.

.. note:: **No licenses? Still runnable.**

   Two stages lean on commercial tools — random generation (Xcelium) and PPA
   synthesis (Cadence Genus on the open-source Sky130 PDK) — but you need neither
   to run the example. Pass ``--prebuilt-stress`` to feed the stress phase from
   prebuilt binaries instead of generating, and ``--no-synth`` to skip the
   area/timing step. The implement + verify loop runs fully with both off.

   The prebuilt binaries are vendored as a git submodule; fetch them once with
   ``git submodule update --init examples/riscv_extensions/prebuilt_stress``.

**1. Head conda env** — only the head needs it (this installs the ``chia`` command
used below); workers get ``chia`` via Ray ``py_modules`` and the cluster's Docker
images:

.. code-block:: bash

    conda create -n chia_env python=3.10.19 && conda activate chia_env
    pip install -e .

**2. Cluster** — ``examples/riscv_extensions/clusters/cluster.yaml`` is the
on-prem template; set ``provider.head_ip`` and each node type's
``compatible_ips``. The node types:

.. list-table::
   :header-rows: 1
   :widths: 16 12 72

   * - Node type
     - Resource
     - Role
   * - ``llm``
     - ``llm``
     - implement / debug LLM (Docker ``chia-claude-code``)
   * - ``head_local``
     - ``head_local``
     - the loop's read-only tools (spec / status / knowledge)
   * - ``database``
     - ``database``
     - durable sweep store + the stress test pool
   * - ``build``
     - ``chipyard``
     - resets BOOM, applies the LLM's edits, builds the cospike DUT
   * - ``riscv_build``
     - ``riscv_build``
     - cross-compiles the directed tests to ELFs
   * - ``cosim``
     - ``verilator_run``
     - lockstep Spike-vs-BOOM co-simulation
   * - ``gen``
     - ``dv`` / ``xcelium``
     - riscv-dv random generation (Xcelium; skip with ``--prebuilt-stress``)
   * - ``vlsi``
     - ``VLSI`` / ``Syn``
     - BoomTile synthesis (skip with ``--no-synth``)

The ``llm`` container bind-mounts your Claude Code config
(``-v ${HOME}/.claude:/home/ray/.claude``). See
:doc:`/user_guides/cluster_config_reference` for the full schema.

.. code-block:: bash

    chia up examples/riscv_extensions/clusters/cluster.yaml

**3. Run one extension** — ``single_loop.py`` implements and verifies a single
extension and archives it to the DB. Run the file directly (it puts the example
packages on ``sys.path``):

.. code-block:: bash

    chia job submit --working-dir . -- \
      python examples/riscv_extensions/single_loop.py --extension bitmanip

Add ``--no-synth`` on a cluster without a synth node, and ``--prebuilt-stress`` to
run the stress phase from committed binaries instead of generating (no Xcelium):

.. code-block:: bash

    chia job submit --working-dir . -- \
      python examples/riscv_extensions/single_loop.py \
        --extension zicond --no-synth --prebuilt-stress

**4. Run several in parallel** — ``multi_loop.py`` fans out one pipeline per
extension and summarizes their PPA:

.. code-block:: bash

    chia job submit --working-dir . -- \
      python examples/riscv_extensions/multi_loop.py --extensions bitmanip crypto zicond

**5. Tear down** — when the run is done:

.. code-block:: bash

    chia down examples/riscv_extensions/clusters/cluster.yaml

Each run archives a sweep under the log DB (``<ext>/sweep_<N>/``): the converged
BOOM diff, per-iteration LLM transcripts, directed and stress sim logs, the
baseline-vs-implemented PPA summary, and the generated ``.S`` stimulus.
