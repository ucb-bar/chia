<!-- This is a shared reference file.  Do not run it directly as a command.  It should be referenced by other commands. -->

# Common Debugging Principles

---

## First Principle: Minimize the Debug Inner Loop

The most important consideration when debugging is **minimizing the time to complete one iteration of the debug inner loop**: reproduce the issue → hypothesize root cause → test a fix → repeat.

Everything else in this document serves this goal. Key strategies:

1. **Reduce design complexity at the point of reproduction.** Layered validation (below) ensures you are debugging the smallest subsystem that still exhibits the bug. Don't debug an integrated system when the bug lives in an isolated sub-module.

2. **Reduce test case size.** If a benchmark suite exposes a failure, do not rerun the entire suite in the inner loop. Identify the specific benchmark that fails, then extract from it a **minimal test case** that reproduces the bug. Use that minimal test case for all subsequent hypothesis/fix iterations. Only rerun the full suite as a final regression check after the fix.

3. **One change at a time.** When testing a fix, change exactly one thing per iteration. If you change two things and the bug disappears, you don't know which was necessary — or whether both were. A muddied result forces extra iterations.

4. **Verify inputs before debugging internals.** When a module's output is wrong, check its inputs first. Many "bugs" are actually correct logic operating on garbage from upstream. This complements Layered Validation (which orders *which* module to debug) with guidance on *where to look first* within a module.

Before starting any debug session, ask: *"What is the fastest way I can see whether my next fix attempt worked?"* Optimize for that.

---

## Layered Validation

When debugging a complex design, propose a layered validation strategy (in the plan file if one exists). Validate subsystems independently, in dependency order — lowest-level first.

**Never debug a higher layer before finishing all lower layers.** A lower-layer bug corrupts higher-layer inputs, making symptoms uninterpretable.

**Example (CPU pipeline):**

1. **Functional correctness**: `pc_mismatch == 0` for all benchmarks.
2. **Memory subsystem (isolated)**: Traffic generator with deterministic addresses, compare cycle counts vs RTL.
3. **Core pipeline (isolated)**: IPC vs RTL using variable-latency magic memory. Constant error across latencies → core bug; scales with latency → load pipeline bug; unaffected → store pipeline bug.
4. **Integrated**: IPC vs RTL with real cache model.

---

## Instrumentation-Guided Localization

**Do not read RTL in circles trying to find bugs by inspection.** Instead, add instrumentation to the DUT to form and test hypotheses about where the bug lives.

**Instrumentation approaches:**
- **Performance counters** for each major subsystem (cache hits/misses, stalls per stage, queue occupancy, dispatch/commit rates). Run the failing test and examine which counters show anomalous values — the anomalous subsystem is where the bug lives.
- **Per-cycle text traces** via `$display` blocks (gated by `ifdef`, e.g. `DEBUG_CACHE`) printing key FSM state and control signals every posedge. Shows logical relationships per cycle without waveform cursor correlation. Reserve VCD for multi-signal visual correlation.

**Examples:**
- Cache miss count far exceeds expected → tag or replacement logic bug, not fill path.
- One pipeline stage shows disproportionate stall count → throughput mismatch at that stage.
- Queue unexpectedly full or empty → upstream overproduction or downstream underConsumption.

When a structural reference exists (e.g., a surrogate vs RTL), a targeted comparison-based approach works well: diff the per-counter/per-signal behavior of the reference against the RTL at the point of divergence.

---

## Assertions as Testable Hypotheses

Encode invariants you believe should hold as assertions. If one fires, you've localized the bug. If none fire, you've ruled out hypotheses. Assertions persist as regression guards after the fix.

**With SVA support:** Use `assert property` for temporal invariants (e.g., "a FIFO's read pointer never passes its write pointer", "every request eventually gets a grant").

**Without SVA (plain Verilog):** Emulate assertions with guarded `$display` + `$finish` in an `always` block:
```verilog
`ifdef DEBUG_ASSERT
always @(posedge clk) begin
    if (rd_ptr > wr_ptr) begin
        $display("ASSERT FAIL [%0t]: rd_ptr (%0d) > wr_ptr (%0d)", $time, rd_ptr, wr_ptr);
        $finish;
    end
end
`endif
```

Gate with `ifdef` so assertions are zero-cost in synthesis and can be toggled per-build.

---

## Subsystem Isolation via Benchmarks

Write benchmarks that stress a single subsystem's edge cases. The goal is to expose bugs in that subsystem (e.g., dropped requests, forwarding failures, full-queue backpressure) while keeping other subsystems trivial so symptoms are unambiguous.

**Examples:**
- **Store-heavy**: fills the SQ to capacity — tests whether the LSQ drops requests when full, whether store-to-load forwarding works under pressure, whether commit drain keeps up.
- **Load-heavy, L1-resident**: saturates load bandwidth — tests MLP, IQ scheduling priority, load pipeline structural hazards.
- **Branch-heavy**: many conditional branches — tests predictor training, redirect penalty, checkpoint allocation/deallocation under pressure.
- **Cache-miss-heavy**: stride exceeding cache capacity — tests MSHR allocation, fill path latency, eviction policy correctness.

When the command is completed, ask the user whether new microbenchmarks should be added to the design's microbenchmark library.

---

## Feature-Disable Isolation

If a bug is suspected in a sub-module or feature, temporarily disable it. If the bug disappears, it's in that feature. If it persists, look elsewhere.

---

## Root Cause Discipline

Always fix the root cause. NEVER hide bugs (e.g., by changing parameters).

- **Resource exhaustion is a symptom, not a root cause.** If changing a queue depth doesn't change IPC, something upstream is wasting entries. Fix the upstream waste first.
- **Oracle divergence does not imply incorrectness.** `pc_mismatch != 0` means speculative execution diverged, but the pipeline may still commit correctly. High mismatch counts indicate wasted work, not necessarily a bug.
- **Fixing one bug can expose others.** A large error can mask smaller ones. Re-run the full suite after every fix.

---

## Debugging Tactics

- **Event-filtered trace for narrowing long bugs.** Once you know which address/index/entry is wrong, print only state-changing events (write enables, allocations, deallocations) for that index. Example: `cda_wr_en && cda_wr_set_idx == 5` immediately reveals which writer corrupted set 5.

- **Binary search to narrow the failure.** When many tests pass and one fails, or a long trace diverges at an unknown point, bisect. Cut the search space in half each iteration — by test, by cycle range, by code region — rather than scanning linearly.

- **Destructive verification can mask bugs.** If a verification loop mutates state (e.g., reading all ways causes evictions), its side effects can obscure the real result. Break out early or use non-destructive checks.

- **Diff against last known good.** If the design used to work, diff against the last passing commit or `git bisect` to find the offending change. This is binary search applied to *changes* rather than tests or cycle ranges — often the fastest path when a regression is recent.

- **Watch for coincident-event bugs.** When two events are assumed mutually exclusive but can coincide (enqueue + dequeue, grant + new request, consumption + arrival), check the coincident case. Audit by asking: "what happens if signal A and signal B are both high in the same cycle?"

---

## Compare Symmetric Constructs

Pipelined and replicated designs repeat the same idiom across stages or instances.  Comparing these parallel structures is the fastest way to spot single-site bugs — any deviation from the pattern is immediately suspect.

**Examples:**
- Pipeline valid/kill chains: if stages 1–3 each set `valid := !kill_prev_stage`, a stage that sets `valid := true` stands out.
- Register address extraction: if all stages extract bits `[11:7]` but one uses `[12:8]`, the off-by-one is visible by comparison.
- Bypass/forwarding source conditions: if two of three use `ctrl.wxd` but one uses `true`, the unconditional enable is anomalous.

**Rule**: When investigating a signal in one stage, always check the corresponding signal in ALL other stages.  If the first match looks correct, don't stop — check the rest.

---

## Avoid Tunnel Vision

If you've spent more than ~10 tool calls investigating a single file or module without converging on a hypothesis, **stop and broaden the search**.  Systematically scan all source files in the design for anomalies.  A bug may live in an auxiliary module (instruction buffer, TLB, branch predictor) rather than the main pipeline file.

**Signs of tunnel vision:**
- Repeatedly re-reading the same function looking for something subtle
- Exploring increasingly unlikely hypotheses within the same module
- More than 3 dead-end hypotheses in the same file

---

## Performance-Only Symptoms

When the design produces correct results but cycle counts differ uniformly from baseline, the bug is on a **non-critical path** that affects throughput but not correctness:

- **Uniform speedup**: a flush, stall, or serialization point was weakened or removed (e.g., CSR side-effect detection changed, reducing pipeline flushes).
- **Uniform slowdown**: a hazard detection, prefetch, or prediction mechanism was weakened (e.g., `||` → `&&` in hazard detection means fewer stalls, but wrong results cause longer recovery paths).
- **Single-benchmark delta**: the mutation affects a functional unit only exercised by that workload (e.g., divider bug only affects division-heavy benchmark).

These are harder to localize than crash bugs because the symptom (a cycle count delta) gives less directional signal.  Focus on logic that *gates* work (stall conditions, flush triggers, prediction accuracy) rather than logic that *computes* values.
