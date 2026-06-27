# gem5_align — gem5↔BOOM microarchitecture alignment loop (CHIA example)

A CHIA flow that iteratively tunes a gem5 model to match a target BOOM
configuration. Each iteration restores a parent gem5 source+config state, asks
an LLM to edit the gem5 config and `src/` to better match BOOM, rebuilds
gem5, runs a microbenchmark suite (credit Professor Tony Nowatzki, https://github.com/darchr/microbench.git/), and compares gem5 cycle counts against cached
**verilator golden** cycle counts to get a per-benchmark `%diff`. Results are
persisted to a SQLite DB (`alignment.db`); each new iteration samples its parent
from the top-2 entries. `N` iterations run concurrently (one per physical gem5
node) via a Ray placement group.

## Setup

**1. Head conda env** — run from this directory:
```bash
conda env create -f env.yml
conda activate gem5-align
```
Only the head needs the environment; workers get chia via Ray `py_modules` and the
`cluster.yaml` images. If you change the name of the environment, change it in
`cluster.yaml` to match.

**2. Cluster** — in `cluster.yaml`, set `provider.head_ip` (the host running
`chia up` / the Ray head) and each node type's `compatible_ips`. Required worker
counts (`max_workers`):

| Node type | Workers | Role |
|---|---|---|
| `llm` | 6 | aligning/debugging LLM (light) |
| `chisel_build` | 1 | builds the verilator golden cache (heavy — Chipyard) |
| `verilator_run` | 4 | runs verilator goldens |
| `gem5_worker` | 6 | builds + runs gem5 |

`compatible_ips` is the **pool** of hosts a type's workers may run on — not a
1:1 list. Multiple workers (and multiple types) co-locate on one host as
separate containers, and do not consume huge quanities of resources,
so you need fewer hosts than the 17 worker slots. `head_ip` is a single string; 
`compatible_ips` is a list. (any `${ENV_VAR}` is expanded)

```yaml
provider:
    head_ip: 10.0.0.10
available_node_types:
    gem5_worker:
        compatible_ips: ["10.0.0.11", "10.0.0.12"]   # inline, block, or ${VAR}
```

Every listed host must be reachable via the SSH credentials in the top-level
`auth` section (`ssh_user`, plus optional `ssh_private_key` path), have Docker,
and run an SSH agent. The `llm` hosts bind-mount your Claude Code config via 
the `-v <dir>:/home/ray/.claude` run option. All four node types are required.

Using a different agent (like Codex) should be a fairly easy swap, but requires
a couple changes in the cluster and loop. Mainly replacing `ClaudeCodeLLM`s
with `CodexLLM`s, and switching to a docker container / environment for the llm
worker which has Codex installed and your credentials set up for Codex.

**3. Benchmarks** — fetch the submodule, then compile the `ubench` suite the loop
runs (with the step-1 `gem5-align` env active — it provides the
`riscv64-unknown-elf-` toolchain via `riscv-tools`):
```bash
git submodule update --init examples/benchmarks   # ubench sources (default GEM5_ALIGN_BENCH_ROOT)
cd examples/benchmarks/ubench && ./compile.sh      # -> build/<bench>.{gem5.elf,verilator.riscv}
```

**4. Config** — in `config.py`, set `BUILD_CONFIG` + `CONFIG_SLUG` together to
your target. Optional env: `GEM5_ALIGN_BENCH_ROOT`, `GEM5_ALIGN_LOG_DIR`. 
Optionally fill in a default value for the REQUIRED `GEM5_ALIGN_VERILATOR_CACHE`
(optional because you can also specify it in the job submission command below in
step 6).)

**5. Bring up the cluster** — from `<repo>/chia` with the env active:
```bash
chia up examples/gem5_align/cluster.yaml
```
This may take a long time, since pulling the large chipyard docker container takes a significant amount of time. You can speed it up on subsequent runs by not pulling the latest container each time, and rather using a specific version of the image - though old CHIA Docker images are periodically cleaned.

If your machines have particularly slow internet, this may time out while doing docker container pulls, in which case you should increase the pull timeouts and try to bring the cluster up again. If you restart this process quickly, it may save its pulling progress.

**6. Launch the alignment job** — pass the **required** `GEM5_ALIGN_VERILATOR_CACHE`
in the submit; the entrypoint runs under Ray's job manager and does **not**
inherit your shell's environment vars, so a plain `export` won't reach the job. It names the
verilator golden cache on the head (`<bench>.log` + `<bench>.out` per benchmark):
an existing dir to reuse, or a fresh writable dir to generate on the first run
(builds `BUILD_CONFIG` + runs verilator over every benchmark). Run from the
example dir (`--working-dir .` uploads its files):
```bash
cd examples/gem5_align
chia job submit --working-dir . \
  --runtime-env-json '{"env_vars": {"GEM5_ALIGN_VERILATOR_CACHE": "/abs/path/on/head"}}' \
  -- python gem5_align_loop.py
```
Add `GEM5_ALIGN_BENCH_ROOT` / `GEM5_ALIGN_LOG_DIR` to `env_vars` only if
overriding their defaults. Tear down when done:
`chia down examples/gem5_align/cluster.yaml`.

**7. Bring down the cluster later** — from `<repo>/chia` with the env active:
```bash
chia down examples/gem5_align/cluster.yaml
```

## Layout

| File | Purpose |
|---|---|
| `gem5_align_loop.py` | The loop: tools, gem5 ChiaFunctions, orchestration, entry point. |
| `config.py` | All paths/knobs. |
| `alignment_db.py` | `AlignmentDB`, a `chia.database.SQLiteNode` subclass storing iteration history/results. |
| `cluster.yaml` | Ray cluster definition (4 node types). |
| `env.yml` | Head-node conda env (Ray + grpcio + chia deps, all from pip). |
| `run_compare.py` | Vendored batch gem5-vs-verilator comparison harness. |
| `baseline_megaboom_conf.py` | Vendored baseline gem5 config the LLM edits. |
| `prompts/align_node_prompt.md` | The aligning LLM's instructions. |

## Canonical gem5 usage

gem5 build / single-run / source-state / O3PipeView-trace primitives are from the
canonical `chia.simulators.gem5.Gem5Node`. The loop ships **this repo's `chia`**
to every worker via Ray `py_modules` (`ray.init(runtime_env=...)`), so workers
import the head's checkout regardless of what their image baked in.

Routed through `Gem5Node`:
- `rebuild_gem5`, `init_gem5_worker`, `CompileCheckTool`, `QuickRunTool` builds →
  `Gem5Node.build_gem5`
- `_capture_config_and_diff` → `Gem5Node.capture_gem5_source_state`
- `restore_gem5_state` → `Gem5Node.restore_gem5_source_state`
- pipeline-trace truncation/summary → `Gem5Node.truncate_gz_trace` /
  `summarize_o3_pipeview`
- per-bundle placement → `Gem5Node(placement_group=..., bundle_index=i)` and its
  `task_options`


## Canonical database usage

The alignment store is the canonical `chia.database.SQLiteNode`. `AlignmentDB`
(`alignment_db.py`) subclasses it: the schema, atomic transactions, and
connection/PRAGMA handling come from the node, and `AlignmentDB` adds only the
domain reads/writes (`insert_iteration`, `top_k_entries`, `best_per_benchmark`,
`lineage`, …) by composing the node's `query` / `query_one` / `query_value` /
`transaction` members. `alignment.db` lives on the head's local disk, so the
loop constructs `AlignmentDB(path, pin_to_current_node=True)` and the LLM's
read-only SQL tool is the canonical `SQLiteQueryTool`, spawned co-located via
`db.spawn_query_tool("align_db")` — replacing the old hand-rolled
`AlignmentDbQueryTool` and NodeAffinity pin. (The original loop's one-time
legacy-schema migration — for a pre-UUID schema that a fresh DB never has — was
dropped in this port.)

## Notes

- The loop deliberately tags its gem5 ChiaFunctions `resources={"gem5": 0.5}` so
  a gem5 op and its co-located ChiaTool actors (each ~0.2 CPU) share a single
  `{CPU:1, gem5:1}` bundle. This is preserved by the port — `Gem5Node`'s static
  methods are called *in-process* inside those 0.5-gem5 wrappers, so no extra
  `gem5:1.0` Ray task is created.
- `MAX_PARALLEL_ITERATIONS` (config.py) caps how many gem5 nodes the loop uses
  at once.
