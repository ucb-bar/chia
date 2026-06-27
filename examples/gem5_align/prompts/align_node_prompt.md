# gem5-to-BOOM Alignment

## Your Job

You are aligning the gem5 O3CPU to the target BOOM config (**`{BUILD_CONFIG}`**). Cycle-count comparisons between gem5 and Verilator (running that same config) on a suite of microbenchmarks tell you *where* the gap is; your job is to figure out *why* and fix it by editing the gem5 source and gem5 config file.

**Target BOOM configuration:** `{BUILD_CONFIG}`. This is the exact Chipyard config class being built and run — locate it in `{BOOM_SRC}` (or elsewhere under `{CHIPYARD_GENERATORS}/`) and follow its mixin chain to determine parameters. If a history entry below cites a different config class name, that entry was authored against a stale target and its parameter values should not be trusted — verify against `{BUILD_CONFIG}` before using them.

**Note that the first entry in this alignment flow was aligned very closely with the medium BOOM configuration, so there may be parameters which are tuned to that configuration.**

You may write to:
- **gem5 source**: `{GEM5_SRC}` — structural changes that no config knob can express
- **Config file**: `{GEM5_CONFIG}` — parameter tuning (via `gem5_src_bash_run_command`)

We will rebuild and run gem5 automatically after source edits so you do not need to collect the final, comprehensive set of results yourself.

## Approach

Form a hypothesis about a microarchitectural structure or structures which might be
causing some of the mismatch, then fix the misalignment with a change that expresses
the right structural behavior. How you get there is up to you — different
mismatches call for different angles.

**Angles worth considering:**

- **Diff the traces.** Compare gem5 retire cycles vs Verilator commit
  cycles at the same PCs. Per-instruction deltas usually point straight
  at the offending op class, FU, or pipeline stage — often the fastest
  path from "X% off" to a concrete hypothesis (see trace sections below).
- **Analyze the performance counters.** At the end of the Verilator traces
  are a set of performance counters collected when running the binaries on
  BOOM in verilator. Use these in comparison to gem5 stats to determine what
  to do.
- **Merge multiple previous improvement attempts.** Consider the history
  of attempted alignements, and combine together multiple alignment ideas
  which have NOT ALREADY BEEN TRIED TOGETHER (do not just emulate an 
  already successful branch). This means looking for a few
  sibling branches and merging all of their changes into one big change
  applied to your parent branch.
- **Pick a single hot mismatch** and drill in: read its `bench.c`,
  trace the BOOM structure it stresses, find the gem5 counterpart,
  adjust.
- **Look across benchmarks for a shared pattern.** A single structural
  bug (e.g., a shared writeback unit modeled as parallel, an op class
  routed to the wrong issue queue) often shows up as correlated drift
  across many benchmarks. Fixing it once moves the whole table.

**Regardless of angle(s), these quality signals matter:**

- Cite exact BOOM file + line numbers for the behavior you're matching.
- Explain *why* the change closes the gap, not just *what* you changed.

## Methodology: hypothesize → instrument → verify

Cycle-count `%diff` aggregates tell you *that* something is wrong.  Per-bench tables tell you *which workload*.  Neither tells you *what mechanism in BOOM is mis-modeled*.  The fastest path from "X% off" to a correct fix is the hypothesize → instrument → verify loop.  Use `gem5_quick_run_*` to make this loop *cheap*.

1. **Pick a single benchmark** (usually the worst by `|%diff|`, but a small bench you can reason about end-to-end is also a fine choice).
2. **Read its `bench.c`** and form a one-sentence hypothesis about *which BOOM structure* dominates its cycle count: divider, L1D MSHR FSM, L2 InclusiveCache scheduler, branch predictor, store queue, etc.  Note: C-level reasoning is wrong about half the time because of compiler optimization (strength reduction, magic-number division, vectorization) and macro expansion (`#define ASIZE 65536/3` does not mean the divisor is `21845`).  Verify in step 3.
3. **Pull the BOOM verilator TMA counters** for that bench from `/home/ray/bench_workspace/verilator/<BENCH>.out` (the tail of the file).  Top-of-the-list counters that map directly to gem5 stats:

   | BOOM TMA counter        | What it means                       | gem5 stats counterpart                                       |
   |-------------------------|-------------------------------------|--------------------------------------------------------------|
   | `divider_active`        | cycles any int/fp divider was busy  | `system.cpu.statFuBusy::IntDiv` + sum of FU latencies        |
   | `int_iq_full`           | cycles INT issue queue was full     | `system.cpu.iew.iqFullEvents`                                |
   | `mem_iq_full`           | cycles MEM issue queue was full     | `system.cpu.iew.lsqFullEvents`                               |
   | `stq_full`              | cycles store queue was full         | `system.cpu.rename.SQFullEvents`                             |
   | `ldq_full`              | cycles load queue was full          | `system.cpu.rename.LQFullEvents`                             |
   | `rob_full`              | cycles ROB was full                 | `system.cpu.commit.committedInstType_*` + dispatch stalls    |
   | `dcache_miss`           | L1D demand misses                   | `system.cpu.dcache.demandMisses::cpu.data`                   |
   | `dcache_release`        | L1D dirty-line writebacks           | `system.cpu.dcache.writebacks::writebacks` (see caveat below)|
   | `l1d_miss_pending`      | cycles waiting on L1D miss          | `system.cpu.dcache.blockedCycles::no_mshrs` + miss latency   |
   | `l2_demand_alloc_dir_miss` | L2 demand misses                | `system.l2.demandMisses::cpu.data`                           |
   | `l2_demand_hit_regular` | L2 demand hits                      | `system.l2.demandHits::cpu.data`                             |
   | `l2_secondary_misses`   | L2 MSHR coalesced (secondary) misses| `system.l2.demandMshrHits::cpu.data`                         |
   | `l2_evict_dirty`        | L2 dirty evictions                  | `system.l2.writebacks::*` (mostly clean unless writeback_clean) |
   | `br_mispredict`         | branch mispredictions               | `system.cpu.commit.branchMispredicts`                        |
   | `icache_miss`           | I$ misses                           | `system.cpu.icache.demandMisses::cpu.inst`                   |

   **Caveat: gem5's `dcache.writebacks` includes `WritebackClean` evictions as well as `WritebackDirty`** — in some configs they're effectively *all* counted as "writebacks" even on load-only benches.  If that's the case for the bench you're investigating, do not assume "gem5 has lots of writebacks ⇒ lots of dirty WBs"; cross-check with the originating-MSHR-had-store check or BOOM's `dcache_release` directly.

4. **Identify the dominant counter on the BOOM side** (what % of total cycles).  If `divider_active = 36%`, the divider is the bottleneck — patching the L1D model will not help.  If `stq_full = 100%`, the bottleneck is store-queue drain rate, which is downstream of L1D miss service.  Pick the *first* mechanism in the dependency chain that *diverges*.

5. **Run `gem5_quick_run_run([BENCH])` + `gem5_quick_run_stats(BENCH, "<regex>")`** to read the corresponding gem5 counter on your *current* tree.  Compare to BOOM's TMA value.  If gem5 reports `0` where BOOM reports `1.18M`, there is a counter wired up differently or the FU is never used; if gem5 reports a smaller value than BOOM, your hypothesis (e.g. "the divider takes too few cycles") may be right.

6. **Verify operand assumptions empirically.**  When the hypothesis depends on what *operand values* the workload feeds an FU (divisor of a div, target of a load, branch direction), don't infer from the C source — the compiler often rewrites these.  The fastest verification is to add a temporary `std::cerr` print to the relevant gem5 path, run `gem5_quick_run_run(["<bench>"])`, and read the output.  Then revert the print before submitting.  This is the equivalent of `printf` debugging — it costs ~1-2 min and often saves a wasted iteration on a wrong assumption.

7. **Find the BOOM Chisel mechanism** — once you know *which* gem5 counter diverges from *which* BOOM TMA counter, locate the corresponding Chisel module under `{BOOM_SRC}` (or rocket-chip / inclusive-cache).  Cite specific file:line.  The fix is a port of that mechanism into gem5.

8. **Patch + verify with `gem5_quick_run_run`** .  If the targeted bench moved toward `ratio=1.0`, your fix is at least directionally right.  If it moved further, you got the sign wrong.  If it didn't move, your hypothesis was incorrect — go back to step 4 with new data, don't paper over it with a tuning constant.

9. **Sanity check other potentially affected benchmarks with `gem5_quick_run_run`**.  The change may have caused other benchmarks to get more or less correct. If many got worse, then it is likely that you did not make a surgical enough change, and you should go back to an earlier step and try to make the structural similarity to BOOM even better and more precise.

This loop catches three classes of mistakes that otherwise burn full iterations:
- **Wrong hypothesis** (stated mechanism isn't what's actually slow).
- **Wrong assumption** (compiler-rewrote operands invalidate the model).
- **Wrong direction** (fix moves the bench the wrong way).

## Warnings

- **Issue at most one tool call per assistant turn.** Do **not** emit multiple `tool_use` blocks in the same response — even if they look independent (e.g. three `gem5_quick_run_run` calls with different benchmarks, or a `gem5_compile_check_build` alongside another tool). The MCP streamable-HTTP transport between you and the tool servers has a known bug where parallel tool calls on the same session cause the second-and-later results to be silently dropped: the server returns 200 OK on an empty SSE stream, and your CLI then waits forever for tool_results that never arrive, killing the entire iteration. Always wait for one tool result before issuing the next call. If you need to run several `quick_run` subsets, do them sequentially across turns.
- **Do NOT grep the chipyard root.** It is >10GB with build artifacts and will hang your bash tool. Start searches inside a specific generator: `{CHIPYARD_GENERATORS}/boom/`, `{CHIPYARD_GENERATORS}/rocket-chip/`, `{CHIPYARD_GENERATORS}/rocket-chip-inclusive-cache/`, `{CHIPYARD_GENERATORS}/chipyard/`.
- You may use stderr-silencing redirects freely — `2>/dev/null`, `>/dev/null`, `2>&1` are not treated as writes by the bash tool.
- You may NOT run `git commit`. The loop captures your diff automatically from working-tree state.

---

## Your Resources

Two bash tools, each in a different container:

- **`gem5_src_bash_run_command`** — gem5 container. Read/write the config at `{GEM5_CONFIG}`. Read/write gem5 source at `{GEM5_SRC}`. Read benchmark source at `/home/ray/bench_workspace/microbench/`.
- **`chipyard_bash_run_command`** — chipyard container. Read-only. Use for BOOM source and other Chipyard generators.

A compile check tool:

- **`gem5_compile_check_build`** — runs an incremental `scons build/RISCV/gem5.opt` against the source state you've left on disk and returns either `OK (Ns)` or `FAIL (Ns):` with the relevant compiler/linker errors (one line of context above each).
  - **Use this after every meaningful structural edit** — it is much cheaper than the full alignment iteration's build step, and catches syntax / typing / privacy / undefined-symbol errors before they cost you a debug retry budget.
  - **Skip it for trivial edits** (one-line param tweaks, a config-file change). The cost is real (~20-60s for one .cc edit, 2-5 min for a widely-included header like `base.hh`); the value is in catching mistakes when you've made multi-file or class-hierarchy changes.
  - On `FAIL`, fix the error in the same iteration: the loop's debug retries are limited and an unbuildable patch wastes them. On `OK`, you still need to consider whether the change is *correct* — passing compile only means the C++ is well-formed.

A tool for running benchmarks:

- **`gem5_quick_run_run` / `gem5_quick_run_stats` / `gem5_quick_run_list_benchmarks`** — runs gem5 on a small benchmark subset (1-5 benches) against the in-progress source/config state and returns cycles + ratio + status, plus optional follow-up queries against gem5's `stats.txt`.  This is the structural-fidelity equivalent of the compile check: instead of asking "does it build?", it asks "does my structural change actually move the targeted benchmark, and does it match BOOM's TMA counters?"
  - **Run before submitting** A failed `quick_run` is one wasted ~2 min — a failed full iteration is ~30 min and consumes a debug retry budget.
  - **Run before patching** to *empirically check operand assumptions* (see "Verify operand assumptions empirically" below).  For example: don't assume `lfsr % ASIZE` divides by `ASIZE` — `quick_run` + `stats(..., "IntDiv")` will tell you what divisor is actually used after C macro expansion / compiler strength reduction.
  - **`gem5_quick_run_run(benchmarks=["MCS"], skip_build=False, timeout_per_bench_s=600)`** — run up to 5 benches.  Pass `skip_build=True` if you only changed the *config file* (not gem5 source) to avoid an unnecessary scons.  Returns a markdown table of `gem5_cy / ver_cy / ratio / %diff / status`.
  - **`gem5_quick_run_stats(benchmark="MCS", pattern="IntDiv|FuncUnit")`** — grep gem5's `stats.txt` from the most recent run for the named benchmark.  Use this to map gem5 internal counters to verilator TMA counters — the next section lists the correspondences.
  - **`gem5_quick_run_list_benchmarks()`** — quick sanity check of which benchmarks the worker has staged (uses the same set the full iteration runs).
  - Build cost is *incremental*: only files you edited get recompiled (~30s-2min typical, longer for a widely-included header).  Per-benchmark run is 30s-3min depending on the bench.  Budget your usage: maybe 2-4 calls per iteration.
  - Best practice: pick the *single bench* that targets the structure you're fixing, not the full suite.  If MCS is gem5-too-fast and you think it's the divider, run `gem5_quick_run_run(["MCS"])`.  If the result still shows gem5 too fast, your hypothesis is wrong — investigate the discrepancy *before* patching further.

Query the database of existing iterations of the flow:

  - **`align_db_query` / `align_db_schema`** — read-only SQL over the live alignment-history DB (SQLite). Use **before** proposing any config or source change to check whether the same parameter, file, or mechanism has already been tried and what it did. Every dispatched iteration that committed to the DB is visible here, including ones that don't appear in your lineage or in the best-per-bench table. Mutations are rejected by the DB — this tool cannot alter state. Do not try a change which has already been made by a child of your parent branch! 

  Tables (call `align_db_schema` for the authoritative, migration-current shape):

  ```
  iterations(entry_id, parent_id, iteration, timestamp,
             avg_pct_diff, changes_summary, source_changes,
             config_contents, gem5_source_diff, build_success,
             build_duration, llm_log_path)
  benchmark_results(entry_id, benchmark, percentage_diff,
                    gem5_status, verilator_status,
                    gem5_num_cycles, verilator_num_cycles,
                    gem5_ver_cycle_ratio, error_messages,
                    stdout_tail, pipe_trace_summary)
  ```

  Example queries you should actually run:

  ```sql
  -- Has anyone touched `mshrs` before? What did it do?
  SELECT iteration, avg_pct_diff, changes_summary, source_changes
  FROM iterations
  WHERE changes_summary LIKE '%mshrs%'
     OR source_changes  LIKE '%mshrs%'
     OR gem5_source_diff LIKE '%mshrs%'
  ORDER BY iteration DESC LIMIT 20;

  -- Trajectory of one benchmark across every iteration that ran it
  SELECT i.iteration, br.percentage_diff, br.gem5_ver_cycle_ratio
  FROM benchmark_results br JOIN iterations i USING(entry_id)
  WHERE br.benchmark = 'CCe' AND br.gem5_status = 'ok'
  ORDER BY i.iteration;

  -- Biggest regressors vs parent — strong "don't repeat this" signal
  SELECT c.iteration, c.avg_pct_diff - p.avg_pct_diff AS delta_pp,
         c.changes_summary
  FROM iterations c JOIN iterations p ON p.entry_id = c.parent_id
  WHERE c.avg_pct_diff IS NOT NULL AND p.avg_pct_diff IS NOT NULL
  ORDER BY delta_pp DESC LIMIT 10;
  ```

  Results are capped at 100 rows and 4 KB per cell. If you hit a
  `NOTE:` line about a cap, add `LIMIT` or a tighter `WHERE` and
  retry.

### Microbenchmarks

Each benchmark is a small C program that exercises one microarchitectural feature. The results table below shows gem5 vs verilator cycle mismatches; the explanation is in the benchmark source.

**Descriptions:**
{bench_descriptions}

Two benchmarks with similar mismatch magnitudes can have completely different root causes. Read the C source before forming a hypothesis.

### BOOM target config (the ground truth)

The exact config class being built is **`{BUILD_CONFIG}`**. Find its
definition first (typically in `{BOOM_SRC}/common/BoomConfigs.scala` or
a Chipyard config file under `{CHIPYARD_GENERATORS}/`) and walk the
mixin chain — the config's `With*` mixins are what actually set MSHR
counts, cache sizes, branch predictor params, etc. Parameter values
baked into related-but-different configs (other BOOM variants, big-cache
mixins, etc.) do **not** apply unless `{BUILD_CONFIG}` pulls them in.

E.g.
```
grep -rn "{BUILD_CONFIG}" {BOOM_SRC} | head
find {BOOM_SRC} -name "*.scala"
// Explore BOOM and its dependencies in Chipyard in general
```

### Chipyard Repo Map (for L2 cache and SoC-level params)

- **BOOM core**: `{BOOM_SRC}` — ifu/, exu/, lsu/, common/
- **L2 inclusive cache (SiFive)**: `{CHIPYARD_GENERATORS}/rocket-chip-inclusive-cache/design/craft/inclusivecache/src/`
  - `Configs.scala` — `InclusiveCacheParams`, `WithInclusiveCache`
  - `Parameters.scala` — `InclusiveCacheParameters`
  - `MSHR.scala`, `Scheduler.scala`, `Directory.scala`
- **Chipyard config composition**: `{CHIPYARD_GENERATORS}/chipyard/src/main/scala/config/` — `AbstractConfig.scala` composes `WithInclusiveCache`.
- **Rocket-chip subsystem**: `{CHIPYARD_GENERATORS}/rocket-chip/src/main/scala/subsystem/`

### gem5 Source Code

```
ls {GEM5_SRC}/cpu/o3/
ls {GEM5_SRC}/arch/riscv/
grep -rE "Param|addParam" {GEM5_SRC}/cpu/o3/ | head -50
// Explore the gem5 source in general
```

`.py` files define SimObject parameters (the knobs you can set from the config); `.cc`/`.hh` files show the behavior. Many useful knobs are inherited from base classes — don't stop at surface-level params (cache size, ROB entries). Explore deeper: per-op latencies, pipeline register delays, queue drain rates, prefetchers, TLBs, memory controllers.

### Current gem5 Config

```
cat {GEM5_CONFIG}
```

### Pipeline Traces

#### Pipeline Traces from Golden Verilator References

Fixed ground truth, shared across iterations:

```
/home/ray/bench_workspace/verilator/<BENCH>.log   # stdout — "after N simulation cycles" total
/home/ray/bench_workspace/verilator/<BENCH>.out   # spike-dasm'd commit trace, one line per committed inst
```

`.out` format (`<cycle>` is absolute BOOM commit cycle):

```
C <cycle>: <priv> <pc> (<insn_hex>) <disasm> [<rd> <value>]
C                 221: 3 0x0000000000010050 (0x00458613) addi    a2, a1, 4 x12 0x0000000002000004
```

`.out` can be tens–hundreds of MB, so bound the output: `head -N`,
`tail -N`, `sed -n 'N,Mp'`, `grep ... | head -N`, or reductions like
`wc -l` / `awk '{{print $5}}' | sort | uniq -c | sort -rn | head`
(opcode histogram).

Useful signals: cycle delta between consecutive lines is BOOM's commit
latency at that PC; loop-body PC ranges give steady-state cycle budgets;
jumps into the trap vector reveal unexpected exceptions. `.out` is
committed-only, so filter gem5's `pipe_trace.gz` to retired-only
(`retire` tick > 0) before aligning.

#### Pipeline Traces from Parent's Run

The parent iteration's gem5 O3PipeView traces have been staged on this worker at:

```
{PARENT_TRACES}/<BENCHMARK>/pipe_trace.gz   # raw O3PipeView trace (gzipped)
{PARENT_TRACES}/<BENCHMARK>/summary.md       # per-stage stats + slowest insts + first-30 table
{PARENT_TRACES}/INDEX.md                     # which benches have traces
```

None of the trace content is in this prompt — reach for it only via
`gem5_src_bash_run_command` when you need it for the benchmark you are
investigating:

```
cat {PARENT_TRACES}/INDEX.md
cat {PARENT_TRACES}/CCh/summary.md
zcat {PARENT_TRACES}/CCh/pipe_trace.gz | head -200
zcat {PARENT_TRACES}/CCh/pipe_trace.gz | awk -F: '$2=="fetch"{{c++}} END{{print c}}'
```

If `{PARENT_TRACES}/INDEX.md` lists zero benches, the parent predates
trace collection (old DB entry) and you have nothing to inspect.

##### Cap the output when reading pipe_trace.gz

Decompressed traces can reach 10–200 MB, so bound the output before it
leaves the pipeline. Useful patterns:

- Window: `zcat ... | head -N`, `zcat ... | tail -N`, `zcat ... | sed -n '100000,100200p'`.
- Filter then cap: `zcat ... | grep ':issue:' | head -N`.
- Reduce to a scalar: `zcat ... | wc -l`, `zcat ... | awk -F: '$2=="fetch"{{c++}} END{{print c}}'`.
- Slice by sn: `zcat ... | awk -F: '/^O3PipeView:fetch/ && $6>=1000 && $6<1050' | head -N`.

Avoid unbounded dumps (`zcat ...` alone, `zcat ... | sort`, `cat *.gz`) —
they truncate silently and fill context with boot-time lines.

`summary.md` is pre-computed and bounded; skim it first, then reach for
`pipe_trace.gz` when you need specific instructions or stage timings it
doesn't cover.

**Raw O3PipeView line format** (one line per stage per instruction; ticks are
picoseconds, 1000 per cycle at 1 GHz):

```
O3PipeView:fetch:<tick>:<pc>:<upc>:<sn>:<disasm>
O3PipeView:decode:<tick>
O3PipeView:rename:<tick>
O3PipeView:dispatch:<tick>
O3PipeView:issue:<tick>
O3PipeView:complete:<tick>
O3PipeView:retire:<tick>:store:<store_tick>
```

**What to look for**:
- Large `issue -> complete` gaps ⇒ execution latency (FU latency, D$ miss, divider bottleneck).
- Large `dispatch -> issue` gaps ⇒ IQ full / operand not ready / scheduling stall.
- Large `rename -> dispatch` gaps ⇒ register rename pressure or dispatch-width stall.
- Large `fetch -> decode` gaps ⇒ front-end stall (I$ miss, BTB miss, resteer).
- Large `complete -> retire` gaps ⇒ ROB serialization (older inst not yet complete).

The trace tells you *where* in the pipeline time is being spent; it
does not tell you *why*. Cross-check against BOOM source and the
gem5 config/source before hypothesizing a cause.

#### Cross-comparing gem5 vs Verilator traces

The aggregate cycle count is one number per benchmark; per-instruction
traces tell you *which* instructions drift. Both traces iterate the
benchmark in program order, so aligning by PC (or by sn into the
committed stream) lets you compute per-instruction cycle deltas. Useful
moves:

- **Find where the gap opens.** Pick a PC that appears many times (a
  hot loop body). Compare BOOM commit cycle N+1 − N vs gem5 retire
  tick N+1 − N. Steady per-iteration slowness ⇒ issue inside the loop
  body (FU latency, port conflict, IQ stall). Divergence across loop
  boundaries ⇒ suspect branch mispredict recovery or front-end.
- **Find which op class drifts.** Group per-instruction deltas by
  opcode. A concentrated delta on one class points at that FU's
  `opLat`, its issue queue, or forwarding.
- **Localize stalls with pipe_trace stages.** Once a PC is implicated,
  look at its gem5 stage gaps (see "What to look for" above).
- **Filter gem5 to committed only.** BOOM's `.out` is committed-only;
  `pipe_trace.gz` includes squashed insts (retire tick == 0). Use
  `awk -F: '/^O3PipeView:retire/ && $3>0'` before aligning, or the
  two streams will desynchronize around mispredicts.

Much more informative than the aggregate cycle diff when the mismatch
is concentrated in a small region of the program.

### Comparison Results (iteration {iteration})

{comparison_table}

**Reading the table:**
- **% Diff**: absolute difference between gem5 and verilator cycles (always positive).
- **gem5/ver Ratio**:
  - ratio > 1.0 → gem5 slower
  - ratio < 1.0 → gem5 faster
  - ratio ≈ 1.0 → aligned

---

## Parent Entry (what you are branching from)

{parent_block}

---

## History of Prior Iterations

{history_report}

### How to read this

Judge prior iterations by **per-benchmark movement on the feature that was targeted**, not by avg diff.

- A correct fix can still leave a benchmark unchanged when another misalignment masks it. Read the benchmark's source to reason about whether multiple mechanisms contribute to its cycle count.
- Avg diff going *up* can mean progress: the model is now more accurate, which exposes errors previously hidden by a compensating bug.
- Avg diff going *down* is not proof a change worked; something else may have improved by coincidence.
- If a parameter keeps bouncing X → Y → X across iterations, stop tuning and go re-read BOOM source for the correct value.
- **Other promising branches** (if listed) are top-scoring iterations outside your lineage. Treat them as evidence about what has worked elsewhere in the search — a hypothesis to borrow or build on, not a result you inherit. Don't re-derive a fix that a sibling already landed; do consider whether their mechanism also applies to the benchmark you're targeting.

---

## Output

End your response with exactly these two sections. The literal strings `==ALIGNMENT_OUTPUT==` and `==SOURCE_PATCH==` must appear **only** as section headers — do not mention them in your reasoning prose above.

```
### ==ALIGNMENT_OUTPUT==
What config parameters and/or source files you changed, why (citing the specific
BOOM source file and line/structure), and which benchmarks this targets.

### ==SOURCE_PATCH==
List each gem5 source file modified and what was changed, with BOOM source citations.
If no source was modified, write "None".

Example:
  /home/ray/gem5/src/cpu/o3/lsq.cc: Added store-to-load forwarding latency
  of 1 cycle to match BOOM's lsu/dcache.scala:142 forwarding behavior.
```

---

## Final Guidance

1. **Microarchitectural truth, not performance fitting.** A change that makes gem5's structure match BOOM's is correct even if avg diff goes up; a change that is architecturally wrong but happens to lower avg diff is worse than no change.
2. **Actual behavior matching, not symptom masking.** Do not just implement a change which appears to make symptoms go away. Actually implement the BOOM behavior in the Gem5 code so that the root cause of the symptoms is cured.
3. **Every change cites specific BOOM Scala code.** File path and line number. "I think this will help" is not a justification.
4. **One mismatch per iteration.** Even a single fix may touch several related knobs or files — that is fine. Don't scatter changes across unrelated subsystems.
5. **Parameters don't map 1:1.** `fetchWidth` in BOOM ≠ `fetchWidth` in gem5. Reason about the hardware meaning, not the name.
6. **Evaluate by the targeted benchmarks.** Judge a change by whether the benchmarks that exercise the changed feature moved toward ratio=1.0, not by avg diff. If a large regression (>10pp) appears, stop and ask whether the hardware story explains it; if not, investigate before continuing.
7. **Past iterations may have targeted the wrong BOOM config.** If any prior history entry cites a config class name other than `{BUILD_CONFIG}`, treat parameter values quoted from those entries (MSHR counts, cache sizes, queue depths) as unverified — re-derive them from the mixin chain of `{BUILD_CONFIG}` before citing them.
8. **Do not try a change which has already been made by a child of your parent branch.** The most obvious changes to make given the branch you are working on have likely already been tried. Check the database to see what direct children of your parent have tried before and try something new!
9. **Be ambitious.** Making small changes can be okay, but if you can drastically increase the alignment by correcting large structures or large swaths of the  gem5 implementation, that is awesome! Most low-hanging fruit have been gotten, so successful changes will likely make significant changes to Gem5 source.
10. **Don't emulate the best.** We get nothing from you mimicking the best branch we already have---we've already seen how those changes perform. Instead, it's much better to try new things and see if you can find a novel misalignment.
11. **Verify operand assumptions empirically before patching.** C-level reasoning about what values an instruction sees is wrong about half the time because of compiler optimization (strength reduction, magic-number division, unrolling) and macro-precedence traps. When your hypothesis depends on a specific operand pattern (divisor magnitude, store address pattern, branch direction), confirm with `gem5_quick_run_*` + a brief instrumentation print *before* writing the patch. The cost is ~2 min; the savings on a wrong hypothesis is one full iteration.
12. **Don't add constants without naming the BOOM mechanism they represent.** Every new `Param` you introduce should map to a real BOOM/Rocket Chisel structure cited by file:line — e.g. `boom_div_pipeline_cycles = 3` representing `IterativeFunctionalUnit (1c) + RegRead (1c) + WB (1c)`, not just "extra cycles to make the bench match." If you can't name what it models, it's a tuning knob in disguise; avoid.
13. **Check `gem5_quick_run_run` on the targeted bench before your final response.** A passing compile and a plausible-looking patch are not enough — confirm the targeted benchmark actually moved toward `ratio=1.0`. If it didn't move, or moved the wrong way, revise *the hypothesis*, not the patch.
