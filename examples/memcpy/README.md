# memcpy — Agentic RoCC Accelerator Generation (MegaBoom)

An agentic loop that has Claude design a RISC-V RoCC accelerator (a hardware
`memcpy`) in Chisel, wires it into the `MegaBoomV3HumanCommitLogConfig`
design, builds it, runs the design against a bare-metal test, and — on any
failure — debugs and retries.

It is a self-contained `examples/` port of the original top-level `memcpy/`
experiment, restructured around the chipyard test-suite build flow and the
MegaBoom commit-log config.

## Flow

Each run first **resets the chipyard checkout to its committed baseline**
(`git checkout -- . && git clean -fd` on the chipyard container). The container
is reused across runs, so without this a prior run's accelerator + config edits
would persist and the implement node would just verify them instead of
generating from scratch. Then:

```
test build  (parallel)        implement  (parallel)
  copy memcpy.c into            Claude writes memcpy.scala
  $chipyard/tests, run cmake    (the RoCC accelerator) and
  -> build/memcpy.riscv         wires it into the target
     build/memcpy.dump          config, via chipyard_bash
        └──────────────┬──────────────┘
                       ▼
         chisel build  (ChiselBuildNode)        target: MegaBoomV3HumanCommitLogConfig
                       ▼
         verilator run (VerilatorRunNode)       memcpy.riscv, +loadmem +verbose
                       ▼
        build failed / sim failed / incorrect?
           │ yes (≤ NUM_DEBUG_ATTEMPTS)        │ no
           ▼                                   ▼
        debug (Claude) ── rebuild + rerun     DONE (passed)
```

## Components

| File | Role |
|------|------|
| `memcpy_loop.py` | Main orchestration: parallel test-build + implement, then the build→run→debug loop. Dumps all collateral to `out/`. |
| `test_build.py` | `build_test` ChiaFunction — copies `memcpy.c` into `$chipyard/tests`, registers a CMake target, builds `build/memcpy.riscv` + `build/memcpy.dump`, reads them back. Runs on the chipyard container. |
| `claude.py` | The implement + debug LLM nodes (`chia.models.claude.ClaudeCodeLLM`), correctness classification, and failure-feedback formatting. The LLM calls are dispatched onto the dedicated `llm` (claude) node, sharing one session (transcript threaded across calls). |
| `prompts/` | The implement (`implement.md`) and debug (`debug.md`) prompt text, with `${VAR}` placeholders substituted from `constants` at load time. |
| `constants.py` | Every tunable knob (loop counts, configs, paths, timeouts, resource tokens). |
| `cluster.yaml` | Minimal single-machine cluster: one chisel-build, one verilator, one claude (`llm`) node, all on `${THIS_MACHINE}`. |
| `memcpy.c` | The bare-metal test: issues two RoCC custom instructions and checks the copy. |
| `out/` | Per-run dump of node results + collateral (git-ignored). |

The DRAMSim2 model config shipped to the verilator run is shared from
`examples/common/dramsim_ini` (see `constants.DRAMSIM_INI_DIR`).

## The accelerator contract

`memcpy.c` drives the accelerator at opcode `custom1` with two instructions
(the implement prompt specifies exactly this, so the design matches the test):

- `funct == 0` — `rs1` = source base address, `rs2` = destination base address.
  Latch both; write `rd = 1`.
- `funct == 1` — `rs1` = array length (number of 64-bit elements). Copy the
  whole array source→destination via the RoCC memory port; write `rd = 1` when
  done.

Correctness is judged by the `MEMCPY Num Correct: N` line the program prints:
the run passes iff `N == DATA_SIZE` (`constants.DATA_SIZE`, kept in sync with
`memcpy.c`).

## Debug feedback

On a failure the debug node (same Claude session, resumed) gets the same
information the other examples' debugger receives:

- **Build failure** — build stderr tail + stdout windowed on the first `error`.
- **Sim failure** (runtime / timeout / incorrect) — simulator stdout (the
  commit log) and spike-dasm output tails, plus the last
  `COMMIT_LOG_TAIL_LINES` lines of the commit log and the last
  `DUMP_TAIL_LINES` lines of `memcpy.dump` (the test disassembly).

## Tunables (`constants.py`)

All knobs live in `constants.py`; container paths and cluster knobs are
`MEMCPY_*` env-overridable, nothing is hardcoded into the loop. Notable:

| Constant | Meaning |
|----------|---------|
| `NUM_DEBUG_ATTEMPTS` | Max debug-and-retry rounds after the first failure (default 3). |
| `NUM_PERF_OPT_ITERS` | Reserved for a future post-correctness performance-optimization phase (defined, not yet used). |
| `BUILD_CONFIG` | Chisel config to build (`MegaBoomV3HumanCommitLogConfig`). |
| `DATA_SIZE` | Element count; must match `memcpy.c`. |
| `VERILATOR_TIMEOUT_CYCLES` / `_SECONDS` | Sim caps so a hung design fails fast. |
| `*_RESOURCE` | Ray `chipyard`/`verilator_run` scheduling tokens (sized so the persistent `chipyard_bash` actor coexists with the builds). |

## Output

Every node result and piece of collateral is written to `out/` with the run
timestamp at the start of the filename, e.g.:

```
20260625_141230_implement.md
20260625_141230_test_build_memcpy.riscv
20260625_141230_test_build_memcpy.dump
20260625_141230_chisel_diff_attempt0.diff      # cumulative chipyard git diff for this iteration
20260625_141230_chisel_diff_attempt0.json      # per-repo diff dict (root + submodules)
20260625_141230_chisel_build_attempt0.stdout.txt
20260625_141230_verilator_run_attempt0.log
20260625_141230_feedback_attempt1.md
20260625_141230_debug_attempt1.md
20260625_141230_chisel_diff_attempt1.diff      # after the debug edit
20260625_141230_summary.json
```

Each iteration's Chisel diff is captured with `collect_diff` (from
`common/common_nodes.py`) just before that attempt's build, so it reflects the
exact source that was built — the implement node's work on attempt 0, and the
cumulative implement + debug edits on later attempts. `collect_diff` captures
both tracked modifications and new untracked files (e.g. the freshly written
`MemCopyRoCC.scala`).

## Prerequisites & running

- `chia` installed (`pip install -e .` from the repo root).
- Claude Code credentials available to the `llm` node — see the mount note at
  the top of `cluster.yaml`.

Bring up the cluster (chisel-build + verilator + claude), submit the loop, then
tear it down:

```bash
chia up   examples/memcpy/cluster.yaml
chia job submit -- python "$(pwd)/examples/memcpy/memcpy_loop.py"   # run from the repo root
chia down examples/memcpy/cluster.yaml
```

Pass the **absolute** path to `memcpy_loop.py` — `chia job submit` runs the
entrypoint from your home directory, not the repo, so a relative path won't
resolve. Do **not** pass `--working-dir`: the loop's `RUNTIME_ENV` already sets
`working_dir` (the example dir, so its flat modules import on workers) and
`py_modules` (the head's current `chia`); passing `--working-dir` too makes Ray
fail to merge the two runtime envs. The driver runs on the cluster head, where
the repo lives, so `out/` is written into the real `examples/memcpy/out`.
