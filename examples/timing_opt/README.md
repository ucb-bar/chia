# timing_opt

Claude-in-the-loop **timing-improvement** for the BOOM core: read a Genus
timing report, edit Chisel to shorten the BoomTile critical path, rebuild,
re-synthesize, and record the result as a new branch in a tree of design
variants. This example optimizes **clock frequency** (worst-case setup slack) 
while keeping the design correct.

Self-contained: depends only on the installed `chia` package and the sibling
`common/` helpers.

## The improve-timing loop

The optimizer maintains a **multi-branch tree** of design variants in a
SQLite-backed store (`db.py`). Each branch records a diff against the base
RTL, the generated Verilog, the Genus timing report, the synthesized area, the
per-benchmark TMA counters, and logs. User picks a **parent** branch to optimize
each turn; the flow produces one **child** branch per invocation.

`run_improve_timing_loop()` runs this 8-step pipeline (`improve_timing.py`):

1. **Load inputs** — pull the parent branch's BoomTile module name, generated
   Verilog, and Genus timing report from the DB.
2. **Acquire a chipyard placement group** + a `chipyard_bash` tool the LLM
   drives. Also spawn the head-pinned `ExperimentLogger` actor and the LLM's
   `timing_experiment` MCP tool (see below).
3. **Stage + reset** — write the parent's generated Verilog to the chipyard
   node, reset chipyard + submodules to their recorded commits, and re-apply
   the parent's diff so the tree reproduces the parent exactly.
4. **LLM `/improve_timing`** — stage the parent's timing report on the chipyard
   node (it is too large to inline in prompt), build the prompt (`prompts/improve_timing.md`),
   and call Claude. Claude greps the report with `chipyard_bash`, edits
   Chisel, and validates candidate edits with fast sub-block syntheses via the
   `timing_experiment` tool. `--skip-llm` skips this step (ablation re-synth
   on the parent diff unchanged).
5. **Build all thread variants** with an LLM build-debug retry loop
   (`common.build.build_all_thread_variants` → `build_with_debug_retry` →
   `common.helper_nodes.debug_failure`). One MegaBoom build per distinct
   `verilator_threads` value across the benchmark suite. Threading used
   to make longer Verilator benchmarks faster.
6. **Synthesis + verilator, in parallel, with recovery** — per attempt:
   - **prep**: CACTI SRAM characterization + MacroCompiler remap, then resolve
     the BoomTile module name (`common.helper_nodes.run_cacti_macrocompiler_prep`),
     redone each attempt because debugger edits can change the Verilog.
   - **dispatch BoomTile synthesis** (async, on a VLSI worker) and **run the
     verilator suite** (`common.verilator.dispatch_verilator_tests`) at the
     same time. Verilator is a **correctness gate** here primarily, but also
     used to estimate how much IPC is degrading.
   - if verilator fails: cancel the in-flight synth, run `debug_failure` (shared
     session across retries), rebuild, and loop (up to `--max-debug-retries`).
7. **Collect synthesis result** once verilator is clean.
8. **Persist** the child branch: synthesis reports, area, `syn_obj` tarball,
   per-test TMA counters, the produced timing report (sets the queryable
   worst-slack columns), and a summary with parent-vs-child worst slack.

On any failure the `finally` block reverts chipyard to the parent diff and
records the final status; success leaves the edits in place (already persisted).

### Seed flow

When the DB is empty there is no parent to optimize, so `main()` first runs
`seed_flow()`: reset to the **unmodified** base RTL (empty diff), build,
CACTI/MacroCompiler prep, synthesize BoomTile, run verilator for the baseline
TMA, and store it as the `baseline` branch — the first DB entry and the root
of the tree. No LLM editing step.

## The `timing_experiment` tool

`TimingExperimentTool` (`timing_experiment_tool.py`) is an MCP tool the LLM
calls during step 4 to A/B-test timing edits without paying for a full
BoomTile synthesis. A small sub-block Genus run can take longer than an MCP
HTTP round-trip — so it uses a **start / poll** split:

- `rebuild_verilog()` — re-elaborate Chisel to Verilog (`make verilog`, no C++
  build) and cache the result.
- `list_modules()` / `list_modules_parent()` — list child / parent Verilog
  module names so the LLM knows the valid `vlsi_top` values.
- `start_synth_child(vlsi_top, …)` / `start_synth_parent(vlsi_top, …)` —
  dispatch a sub-block synth on the edited vs. unmodified RTL; return a handle
  in sub-seconds. Issue both for a parallel A/B comparison.
- `synth_status(handle, max_wait_seconds)` — poll an in-flight synth; returns
  `running` or the full area + worst-slack summary on completion.

Results are recorded in the `llm_experiments` table via the head-pinned
`ExperimentLogger` actor (the tool's worker can't see the head's `/scratch`).

## Running

`improve_timing.py` is the entry point. With an empty DB it seeds the baseline,
then optimizes from it:

    python examples/timing_opt/improve_timing.py \
        --branch baseline \
        [--iteration 1] \
        [--max-debug-retries 3] \
        [--build-config MegaBoomChiaBigCacheConfig] \
        [--output-suffix _timing] \         # child = <parent><suffix>_v<N> (auto-increment)
        [--output-branch NAME] \            # override the computed child name
        [--prompt-file prompts/improve_timing.md] \
        [--model claude-opus-4-8] \         # Claude model for the /improve_timing step
        [--skip-llm] \                      # ablation: apply parent diff, no LLM edit
        [--skip-verilator] \                # single build, no sim; frees the build node during synth
        [--no-experiment-tool] \            # LLM gets only chipyard_bash (no A/B sub-block synth)
        [--diff-file path/to/diff.json] \   # synth an external diff instead of the parent's
        [--synth-only] \                    # re-synth a branch's stored RTL → <branch>_synth_only
        [--seed-only] \                     # build+synth the baseline, then exit
        [--dry-run] \                       # print the parent's timing-report head, exit
        [--ray-address auto]

Three prompt variants ship in `prompts/`:

- `improve_timing.md` (default) — **IPC-neutral** edits only; reshape logic to
  cut the critical path without changing cycle behavior.
- `improve_timing_ironlaw.md` — allow IPC-trading moves when they win the
  iron-law product (frequency × IPC). Pass via `--prompt-file`.
- `improve_timing_ironlaw_noab.md` — the iron-law variant for runs **without**
  the `timing_experiment` A/B tool; pair it with `--no-experiment-tool`.

### Modes

Besides the full LLM loop, `improve_timing.py` exposes a few non-LLM modes used
to run experiments and ablations:

- **`--skip-llm`** — apply the parent's diff (or `--diff-file`'s) unchanged and
  run build → verilator → synth. No optimizer LLM.
- **`--skip-verilator`** — build MegaBoom once (just to elaborate the RTL +
  collect Verilog), skip the verilator suite, and release the chipyard
  placement group right after dispatching synthesis, so the build node is free
  during the long Genus run. No per-test TMA counters are recorded.
- **`--diff-file FILE`** — apply an external `diff.json` (the
  `{"": root, "generators/boom": …}` format `collect_diff` emits) instead of
  the parent's. The parent only supplies the staging generated_src/timing_report.
  Combine with `--skip-llm --skip-verilator` to synthesize a given diff as-is
  (e.g. an externally-produced perf-feature diff) with no LLM and no sim.
- **`--no-experiment-tool`** — don't give the LLM the `timing_experiment` MCP
  tool; it gets only `chipyard_bash`. Pair with `improve_timing_ironlaw_noab.md`.
- **`--synth-only --branch B`** — re-run CACTI + Genus on branch `B`'s stored
  generated_src (no rebuild, verilator, or LLM), writing `B_synth_only`. Used to
  re-measure existing branches under a corrected SRAM area model.

Because Ray runs the driver from the head, the DB lives at an absolute head
path (`constants.DB_DIR`, override with the `TIMING_OPT_DB_DIR` env var), never
a `Path(__file__)`-relative path (which would resolve into Ray's upload dir and
make the seed-detection lie).

### Run layout

The SQLite DB and its file tree live under `DB_DIR`:

    {DB_DIR}/
    ├── timing.db                 # branches / files / perf_results / llm_experiments
    └── files/<branch>/
        ├── logs/                 # timing.csv, improve_timing_prompt.md,
        │                         # improve_timing_llm.md, error_context_*.txt,
        │                         # debug_failure_*.md
        ├── diff.json             # branch diff vs base RTL (root + per-submodule)
        ├── generated_src.json    # generated Verilog
        ├── timing_report.rpt     # produced Genus report
        ├── synthesis_reports/    # per-run Genus reports
        ├── area_estimates.json, synthesis_log.md
        ├── syn_obj/              # extracted Hammer/Genus working tree (tarball
        │                         # transferred from the synth worker)
        ├── perf_results.*        # per-test TMA counters
        └── experiments/<id>/     # sub-block synths run by the LLM

### Reporting

`scripts/perf_table.py` reads the DB and prints a per-branch table of worst
slack / achievable frequency vs. a target period:

    python examples/timing_opt/scripts/perf_table.py \
        --db {DB_DIR}/timing.db [--suite all] [--target <ns>] [--baseline baseline]

## DB schema (`db.py`)

| table | purpose |
|-------|---------|
| `branches` | the variant tree: `name`, `parent_id`, `is_seed`, `iteration`, `status`, `boom_tile_module`, `area`, `worst_slack_ns/_met/_line`, `synthesis_success`, `verilator_passed/_failed`, `files_dir` |
| `files` | registered on-disk artifacts per branch, keyed by `role` (`diff`, `generated_src`, `timing_report`, `synthesis_reports`, `syn_obj`, `log`, …) and `kind` (`file`/`dir`) |
| `perf_results` | per-test `passed` + `counters_json` (TMA counters) |
| `llm_experiments` | sub-block synths the LLM ran via `timing_experiment`: `vlsi_top`, `status`, `area`, `worst_slack_*`, `elapsed_seconds`, `files_dir` |

## Prerequisites

> **Before running, replace every stubbed path** the example ships with —
> synthesis-tool binaries, the collateral/CACTI paths, the timing-report
> relpaths, the editable `chia` checkout, and the cluster keys/hosts. See
> [Paths to fill in](#paths-to-fill-in) for the complete checklist.

- **Ray cluster** with `chipyard`, `verilator_run`, `VLSI`/`Syn`, and `llm`
  resources. `timing_cluster.yaml` is a reference topology; `env.yml` is the
  conda env.
- **Synthesis collateral** on the VLSI/chipyard workers: CACTI
  (`constants.CACTI_PATH`) and the sky130 collateral
  (`constants.SKY130_COL_PATH`). We cannot provide a Docker container with
  commercial tools.
- **Test binaries** under `verilatorbins/` (`asmtests/`, `embench/`) — a git submodule; the `ubench` subdirs are
  git-ignored from the Ray upload. `dramsim_ini/` ships alongside for the
  verilator runs.
- **`LLM_ENV`** (`constants.LLM_ENV`) is just the working directory the Claude
  Code process runs in; the cluster setup creates it empty. **Nothing needs to
  live there** — like `core_ipc_opt`, all prompts are sent inline: the
  `/improve_timing` prompt is read from `prompts/improve_timing.md` and
  pre-expanded on the head node, and the build/verilator debugger prompt is
  read from `prompts/debugging.md` and passed inline to
  `common.helper_nodes.debug_failure` (its required-reading references,
  `common_debugging.md` + `chisel_debugging.md`, ride along as `aux_files`,
  get written to a per-call `{AUX_DIR}` temp dir on the LLM machine, and are
  deleted after the call).

## Setup: environment-specific config

Machine-/site-specific values are read from environment variables (with
reference-cluster defaults) so the example is reusable without editing code.

### Paths to fill in

Several values ship as obvious stubs (`/path/to/…`, `PATH/TO/…`,
`CHANGE_ME_…`, `${VAR}`) that you **must** replace for your environment before
the flow will run end-to-end. The complete checklist, by file:

| File | Stub | Replace with |
|------|------|--------------|
| `constants.py` | `TIMING_REPORT_RELPATHS = ("path/to/timing/report", "path/to/timing/report")` | the relative path(s), under each synthesis `obj_dir`, where **your** synthesis tool writes its timing report(s) — listed most-preferred first. The flow reads these out of the collected reports to extract worst slack, so **until you set them the `worst_slack_*` / achievable-frequency columns stay empty.** (Shipped as a stub so the example doesn't disclose a specific tool's report-directory layout.) |
| `constants.py` | `/path/to/…` defaults for the synth obj_dir scratch, sky130 collateral, and CACTI binary | prefer setting the `TIMING_OPT_*` env vars instead of editing the file — see the env-var table below |
| `sky130_vlsi/tools-chia.yml` | `synthesis.genus.genus_bin: "/path/to/cadence/GENUS"` and `cadence.cadence_home: "/path/to/cadence"` | absolute path to your synthesis tool binary and Cadence install root on the synth workers |
| `env.yml` | `- -e /path/to/chia` | path to your editable `chia` checkout (pip `-e` install in the conda env) |
| `timing_cluster.yaml` | `${HEAD_IP}`, `KeyName: CHANGE_ME_ec2_key` (×2), `${GHCR_USER}`/`${GITHUB_TOKEN}`, and the stubbed `vlsi` node `docker.image` + `compatible_ips` | your head IP, EC2 key-pair, GHCR creds, and synth-worker image/hosts — details in the "`timing_cluster.yaml`" subsection below |
| `boom_tile_syn.py` *(only if you run the standalone variant-sweep script, not the main flow)* | `OPTS_DIR = Path("PATH/TO/OPTS")`, `BASE_MEMS_CONF_PATH` (`# USER MUST FILL IN`) | the variants directory to synthesize and the base `.top.mems.conf` path |

`sky130_vlsi/tech-sky130.yml`'s `basepath: "/path/to/base/"` is intentionally
**not** on this list — it is overwritten at run time from `SKY130_COL_PATH`
(the node sets it to `dirname(sky130_col_path)`), so leave it as-is.

**`constants.py` env vars** (pass via `chia job submit --runtime-env-json`, as in
[Launching runs end-to-end](#launching-runs-end-to-end) — not a shell `export`):

| Env var | Default | What it is |
|---------|---------|------------|
| `TIMING_OPT_DB_DIR` | `~/timing_opt_DB` | SQLite store + file tree. **Must** be a stable absolute head path (never under Ray's working-dir upload). Set this. |
| `TIMING_OPT_CHIPYARD_PATH` | `/home/ray/chipyard/` | chipyard checkout inside the chisel-build / LLM images |
| `TIMING_OPT_LLM_ENV` | `/home/ray/llm_env` | LLM working dir (created empty by the cluster setup) inside the LLM image |
| `TIMING_OPT_SYN_OBJ_SCRATCH_DIR` | `/path/to/chia-logging/timing_syn_obj` | worker-local scratch for the Genus obj_dir |
| `TIMING_OPT_SKY130_COL_PATH` | `/path/to/sky130_col` | sky130 Genus collateral on the synth workers |
| `TIMING_OPT_CACTI_PATH` | `/path/to/cacti/cacti` | CACTI binary on the synth workers |

**`timing_cluster.yaml`** carries operator-specific values to set before
`chia up`:

- `KeyName: CHANGE_ME_ec2_key` (×2) — your EC2 key-pair name (edit literally).
- `${GHCR_USER}` and `${GITHUB_TOKEN}` — GHCR login for the docker images
  (exported into the node env, like the existing `${GITHUB_TOKEN}`).
- **`vlsi` synthesis worker** — left as a stub in the yaml. Our runs used
  Cadence Genus (sky130 PDK), but the tool image, its licensing, and the PDK
  collateral are commercial and intentionally omitted. Provide a worker that
  satisfies the node's `VLSI`/`Syn` resources and set its `docker.image` 
  (if you are using a Docker environment for it) +
  `compatible_ips`. If your synthesis tool needs license-server reachability,
  add it to `head_setup_commands`.

## Launching runs end-to-end

Once every stub from [Paths to fill in](#paths-to-fill-in) is replaced and the
[`constants.py` env vars](#setup-environment-specific-config) are chosen, bring
the flow up with the steps below, driven through the **`chia` CLI** (`chia up` /
`chia job submit` / `chia down`) like the `gem5_align` / `circt_issue_solver`
examples. Commands run from `<repo>/chia` unless noted.

**1. Create + activate the head conda env.** Only the head needs it — workers get
`chia` via Ray `py_modules` and the cluster images. The env is named `timing_loop`
(in `env.yml`); fill in its `- -e /path/to/chia` line with your checkout first:
```bash
conda env create -f examples/timing_opt/env.yml
conda activate timing_loop
```

**2. Fetch the verilator test binaries.** The suite reads `asmtests/`,
`embench/` under `examples/timing_opt/verilatorbins/` (a git
submodule — see [Prerequisites](#prerequisites)); `dramsim_ini/` already ships
alongside:
```bash
git submodule update --init examples/timing_opt/verilatorbins
```

**3. Bring up the cluster.** `chia up` expands `${HEAD_IP}`, `${USER}`, and the
GHCR creds from your shell (after you've set the `KeyName` and `vlsi` worker in
`timing_cluster.yaml`):
```bash
export HEAD_IP=10.0.0.10            # host running the Ray head
export GHCR_USER=<your-ghcr-user>
export GITHUB_TOKEN=<ghcr-PAT>      # read:packages
chia up examples/timing_opt/timing_cluster.yaml
```
The first bring-up is slow — it pulls the large chipyard/verilator images.
Confirm all seven workers (2 `verilator_run` + 2 `chisel_build` + 1 `llm` +
2 `vlsi`) are up in the dashboard before submitting.

**4. Seed the baseline.** With an empty DB the flow builds + synthesizes the
**unmodified** RTL and stores it as the `baseline` branch (the tree root).
Pass the required `TIMING_OPT_DB_DIR` — plus any non-default collateral paths —
via `--runtime-env-json`, **not** a shell `export`: the `chia job submit`
entrypoint runs under Ray's job manager and does *not* inherit your shell's env
(the same gotcha as `gem5_align`). Run from the example dir so `--working-dir .`
uploads it (its `.gitignore` keeps the big `verilatorbins/` subdirs out of the
upload):
```bash
cd examples/timing_opt
chia job submit --working-dir . \
  --runtime-env-json '{"env_vars": {
      "TIMING_OPT_DB_DIR": "/abs/path/on/head/timing_opt_DB",
      "TIMING_OPT_SYN_OBJ_SCRATCH_DIR": "/abs/worker/scratch/timing_syn_obj",
      "TIMING_OPT_SKY130_COL_PATH": "/abs/worker/sky130_col",
      "TIMING_OPT_CACTI_PATH": "/abs/worker/cacti/cacti"
  }}' \
  -- python improve_timing.py --seed-only
```
Include only the vars you're overriding; `TIMING_OPT_DB_DIR` is the one you should
always set (see the [env-var table](#setup-environment-specific-config)).

**5. Run a timing-optimization iteration.** Once the baseline exists, choose a
parent branch to optimize; each invocation produces one child branch named
`<parent>_timing_v<N>` (auto-incremented). Reuse the **same** `--runtime-env-json`
block on every submit so the DB and collateral paths stay consistent:
```bash
chia job submit --working-dir . \
  --runtime-env-json '{"env_vars": {"TIMING_OPT_DB_DIR": "/abs/path/on/head/timing_opt_DB"}}' \
  -- python improve_timing.py --branch baseline
```
Optimize a child instead by passing its name (e.g. `--branch baseline_timing_v1`);
re-run with the same `--branch` to grow the tree wider with another sibling. The
[Running](#running) section lists the full flag set (`--skip-llm`,
`--skip-verilator`, `--diff-file`, `--no-experiment-tool`, `--model`,
`--prompt-file`, …) — the same flags apply here.

> You can skip step 4 and run step 5 directly: with an empty DB the loop
> auto-seeds the baseline first, then optimizes it in the same job. Submitting via
> `chia job submit` (rather than running `python improve_timing.py` directly) makes
> the driver logs show up in the dashboard at `http://<HEAD_IP>:8265`.

**6. Inspect results** with the reporting script (reads the head-local DB):
```bash
python examples/timing_opt/scripts/perf_table.py \
  --db /abs/path/on/head/timing_opt_DB/timing.db --target <ns>
```

**7. Tear down** when finished:
```bash
chia down examples/timing_opt/timing_cluster.yaml
```
