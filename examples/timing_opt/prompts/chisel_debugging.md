# Chisel Debugging Textbook

*Comprehensive reference for debugging Chisel designs. Covers Chisel-universal traps, pipelined processor debugging methodology, BOOM-specific insights, and toolchain behaviors.*

---

## 1. Chisel Language Traps

### 1.1 Last-Connect Semantics

Chisel's last-connect rule: the last `:=` to a signal in source order wins. When debugging unexpected signal values, search for ALL assignments to that signal — the bug may be assignment ordering, not logic.

**Trap with default assignments**: When integrating a submodule whose IO depends on signals defined later in the file, avoid placing "default" assignments (`io.foo := false.B`) near the instantiation site with real connections further down. If the defaults appear *after* the real connections in source order, they silently override the real values, making the submodule a complete no-op — with no compile-time warning.

**Rule**: Default assignments must come BEFORE real connections in Chisel source order. Better yet, avoid defaults entirely — connect submodule IO at the point where the driving signal is defined.

### 1.2 Width Inference

Chisel infers widths aggressively, creating silent bugs:
- `a + b` produces `max(w(a), w(b)) + 1` bits. Truncation on assignment to a narrower target is silent.
- Bit extracts `x(hi, lo)` are zero-indexed, inclusive on both ends.
- `Cat(a, b)` width = `w(a) + w(b)`. If either operand has the wrong width, the concatenation silently shifts downstream bits.
- A `RegInit` width change (e.g., `log2Ceil(N)` to `log2Ceil(N+1)`) propagates through all downstream expressions via FIRRTL/CIRCT, changing generated Verilog structurally (wider intermediate wires, different `UIntToOH` outputs, truncation via bit-select). These are hard to reason about from Chisel source alone.
- Bit-extract range changes (e.g., `io.in2(4,0)` → `io.in2(3,0)`) can change generated Verilog structure in non-obvious ways.

**Rule**: Always diff generated Verilog for width bugs — reasoning about Chisel width inference alone is unreliable.

### 1.3 Static vs Dynamic Shifts

`x >> n.U` creates a **barrel shifter** (dynamic hardware shift); `x >> n` with a Scala `Int` creates **static bit extraction** (zero hardware cost). The `.U` suffix is easy to add accidentally.

Consequences of the accidental dynamic shift:
- **Left shifts widen the result**: `x << n.U` produces a result `n` bits wider than `x`, causing downstream width mismatches or silent truncation.
- **Right shifts create unnecessary hardware**: A barrel shifter for a constant shift amount wastes area and may affect timing.
- **Address comparisons break**: An over-wide shifted address may fail `AddressSet.contains` checks or produce incorrect tag/index extraction.

**Rule**: Always use Scala `Int` (no `.U`) for shift amounts known at elaboration time. Search for `>> .*\.U` and `<< .*\.U` patterns when debugging unexpected width behavior.

### 1.4 Decoupled / Queue Pitfalls

- Always check `fire` (`valid && ready`), not just `valid`.
- `Queue(n)` has n entries but 1-cycle latency. Off-by-one in depth can cause deadlocks or lost data.
- `Irrevocable` means the producer cannot de-assert `valid` once asserted. Violating this produces non-deterministic bugs.

### 1.5 Elaboration vs. Simulation Bugs

Know which you're dealing with:
- **Elaboration-time**: Scala type errors, FIRRTL transform failures, parameter miscalculations. Debug with Scala stack traces and `println` in generators.
- **Simulation-time**: Logic bugs in generated hardware. Debug with waveforms, `printf`, and PeekPokeTester stepping.

### 1.6 Module Hierarchy: No Cross-Module Register Writes

Chisel enforces strict module hierarchy: a parent module CANNOT assign to registers inside a child module, even through a `def` method. The hardware elaborator tracks which module owns each register and rejects cross-module writes at elaboration time.

**Rule**: For control signals that flow parent→child, use IO ports (`Input(Bool())`). For signals that flow child→parent, use `Output()`. The `def clear()` pattern from software design does not translate to Chisel hardware.

---

## 2. Toolchain Behaviors

### 2.1 firtool Dead Code Elimination

firtool aggressively eliminates dead code. Patterns where a code change has no effect on generated hardware:
- Signal only appears in `io.events` / performance counter closures.
- `RegInit` value changed but the register is constant-folded away.
- Signal defined but never used as an rvalue (dead assignment).
- Signals gated by rare events (trap returns, D-cache replays, div/rocc completion races) may be eliminated or unreachable in short benchmarks.

**Rule**: Before debugging a change that appears to have no effect, check if the signal appears in generated Verilog at all.

### 2.2 Verilator Random Init Sensitivity

Adding registers or widening Bundles changes CIRCT's `_RANDOM` initialization array sizing in the generated Verilog. This shifts Verilator's random seed sequence for *all* registers in the design, causing tiny (<0.1%) non-deterministic cycle count differences even when the new logic is functionally dead or observational. Two common triggers:

- **Widening pipeline-wide Bundles** (e.g., MicroOp): The wider Bundle propagates through every pipeline register, changing the total register count. **Fix**: Use a side-channel path (separate Reg/Vec alongside the pipeline) instead of widening the Bundle.
- **Adding register arrays** (prediction tables, counters): Even if firtool dead-code-eliminates the logic, the `_RANDOM[0:N]` array declaration persists in the `initial` block, changing the C++ model's memory layout. This is unavoidable without modifying CIRCT.

**Rule**: When a "purely observational" change produces unexpected DIFF results on a few benchmarks, check whether the generated Verilog's `_RANDOM` array size changed. If so, the difference is a simulation artifact, not a functional bug.

### 2.3 Width Propagation Through CIRCT

When a Chisel `RegInit` width changes, FIRRTL/CIRCT propagates the wider type through all downstream expressions. This changes generated Verilog structurally:
- Shift expressions get wider intermediate wires.
- `UIntToOH` outputs change width; truncation via bit-select differs from full-width usage.
- Update logic may use bit-select truncation (`_T[0]`) in the narrow case but full value in the wide case.

**Rule**: Diff the generated Verilog (`gen-collateral/*.sv`) against a known-good build to confirm which modules are actually affected.

### 2.4 Register Array Sizing vs Compilation Time

`RegInit(VecInit(...))` register arrays with thousands of entries cause Verilator to generate enormous C++ files with compilation times that dominate the build:

| Entries | Approx C++ lines | Approx compile time |
|---------|-------------------|---------------------|
| 64      | ~10K              | seconds             |
| 256     | ~50K              | ~1 min              |
| 1024    | ~200K             | ~10 min             |
| 4096    | ~1M               | ~75 min             |

**Rule**: For tables larger than ~256 entries that don't need combinational multi-port read or guaranteed initial values, use `SyncReadMem`. If combinational read is required, reduce the entry count. If both large size and combinational read are required, accept the compilation cost.

### 2.5 SyncReadMem vs Register Arrays

`SyncReadMem` has three properties that make it dangerous for prediction and tracking structures:
1. **Undefined initial values**: Unlike `RegInit`, entries start with unknown data. A predictor reading uninitialized entries may produce spurious predictions from cycle 0.
2. **1-cycle read latency**: Reads are registered — data available one cycle after address presented. Structures needing combinational reads cannot use SyncReadMem directly.
3. **Read-modify-write hazards**: Simultaneous read and write to the same address in the same cycle produces undefined read data.

**Rule**: For tables needing clean initialization, combinational multi-entry reads, or tolerance of concurrent access, use `RegInit(VecInit(...))`. Reserve SyncReadMem for large, single-ported structures where 1-cycle latency is acceptable and initialization is handled explicitly.

---

## 3. Primary Localization Techniques

### 3.1 Parallel Structure Comparison (Most Effective)

*100% success rate, avg debug time 5 minutes across tested cases.*

In-order pipelines replicate the same logical pattern across stages. Comparing equivalent constructs across stages immediately identifies anomalies. This is the single most effective technique for pipeline bugs.

**What to compare** (generalizable to any in-order pipeline):

| Pattern | What to check | Example anomaly |
|---------|---------------|-----------------|
| Valid/kill chain | Stage-to-stage valid propagation with kill guards | One stage has `true.B` or missing `!` |
| Register addressing | Write-address bit extracts across stages | `(12,8)` vs `(11,7)` or `(19,15)` vs `(11,7)` |
| Replay/xcpt propagation | Guard and value symmetry across stages | Replay uses redirect guards, xcpt uses kill guards — a swap is visible |
| Hazard detection | Cannot-bypass terms across stages | A term present in one but missing in the other |
| Instruction size constants | `Mux(rvc, 2.S, 4.S)` everywhere | A `3.S` or `5.S` stands out |
| Bypass conditions | Write-enable conditions across bypass sources | One uses `true.B` instead of the real enable |

**When it fails**: Cannot detect bugs *within* a single expression (e.g., missing term in a kill signal, replay guard bugs, scoreboard missing terms). For those, use Technique 2 or 3.

### 3.2 Diff Generated Verilog

*Catches bugs that parallel-structure and inspection miss. Especially effective for width bugs, dead code, and expression-internal changes.*

Diff `gen-collateral/*.sv` between a buggy and known-good build. This immediately reveals:
- Which modules are functionally affected vs. structurally identical.
- Dead code (signal not present in generated Verilog at all).
- Expression-internal changes (missing terms, wrong comparison constants).

Key examples:
- `< 2.U` vs `< 3.U` compiles to `~(size[1])` vs `size != 2'h3` — easy to spot in Verilog.
- A missing term in a kill signal shows up as a structurally different combinational expression.
- Width changes show up as different padding widths (e.g., `{59'h0, ...}` vs `{60'h0, ...}`).

**Rule**: When standard checklist patterns fail and >10 tool calls have been spent, diff the Verilog.

### 3.3 Decode Table Symmetry Check

*Efficient for decode bugs.*

1. Group related instructions (SB/SH/SW, ADD/SUB/AND/OR, BEQ/BNE/BLT/BGE).
2. Compare their decode fields column-by-column.
3. Any field that differs between instructions that should share the same control signal is the bug.

**Field position counting**: Decode tables are positional lists. Map each position to its semantic meaning (legal, fp, rocc, branch, jal, jalr, rxs2, rxs1, sel_alu2, sel_alu1, sel_imm, alu_dw, alu_fn, mem, mem_cmd, ...). A wrong field often changes an instruction's category entirely.

**Rule**: When cycle counts are uniformly catastrophic (10,000x+), check decode tables first — they're the fastest to audit via symmetry comparison.

### 3.4 Feature-Disabling Bisection

When an optimization causes uniform catastrophic failure and touches multiple subsystems, **disable features one at a time** to isolate which subsystem contains the bug. This is faster and more reliable than code inspection for complex multi-module optimizations.

Procedure:
1. Identify the N independently-disablable features (e.g., prediction, replacement, prefetch issuance, training).
2. Disable one feature, rebuild, test. If tests pass, the bug is in that feature.
3. If tests still fail, re-enable and disable the next.
4. Repeat until the failing feature is identified, then debug within that scope.

**Rule**: When facing total-failure symptoms from a multi-feature optimization, resist the urge to read all the code. Bisect by feature first.

### 3.5 Printf Debugging

Use Chisel's hardware `printf` (not Scala's `println`) for simulation-time:
```scala
printf(p"cycle=$cycle pc=$pc\n")  // p interpolator handles Chisel types
```
- Gate with `when(condition)` to reduce noise.
- Prefer `printf` over VCD for initial localization; switch to VCD for multi-signal correlation.

---

## 4. Symptom-to-Bug-Location Map

### 4.1 Total-Failure Patterns (All Benchmarks Affected)

| Symptom | Likely bug location |
|---------|-------------------|
| All fail, 0 instructions retired | Branch comparison (`===`/`=/=`) or adder (breaks PC calc) |
| All identical non-baseline cycle count | ADD↔SUB swap (crash during CRT startup) |
| All identical cycle count (workload-independent) | Valid/kill chain inversion or instruction-retire counting |
| Uniform CPI=4.0 | Per-instruction pipeline penalty = redirect cost. Instruction buffer width bug. |
| Massive cycle inflation (~40,000x) | Scoreboard missing term (multi-cycle ops don't stall dependents) |
| Correct data but ~100x CPI | Replay guard missing — unconditional replay on every cache miss |
| Total hang (no instructions complete) | Kill signal missing bubble-kill term — stale control propagates |
| Early stages work but no forward progress | NPC/branch target corruption (wrong immediate type, wrong constant) |
| All crash | Link address corruption (JAL/JALR write-data Mux swap) |
| All very subtle (<0.004%) | Shift decode (e.g., SRA→SLL) |

### 4.2 Partial-Failure Patterns

| Symptom | Likely bug location |
|---------|-------------------|
| Most fail, FP benchmark passes | Integer ALU logic or decode (FP uses separate path) |
| Only division benchmark affected | Division logic in multiplier module |
| Only one benchmark, timing-only | Division kill timing (`RegNext` removal) |
| Multiply benchmarks 16x slower, others match | Multiply FSM wrong state (e.g., mul→div) |
| All slower 2-9% | Output mux default (wrong result, programs survive) |
| Mixed: some crash, some massive inflation | Scoreboard missing term for a specific operation type (usage varies across programs) |
| Performance-only, single benchmark, tiny delta | Stall/replay tradeoff (e.g., slow bypass threshold) |

### 4.3 Key Diagnostic Rules

- **All-same cycle count regardless of workload**: Instruction-retire counting or valid-chain bug (pipeline behavior independent of program).
- **Varied but all failing**: ALU comparison or arithmetic bug (each program exercises the bug differently).
- **Correct data, extreme slowdown**: Replay guard or stall bug (functional correctness preserved, throughput destroyed).
- **Wrong data, massive inflation**: Scoreboard or hazard detection bug (instructions proceed with stale data).
- **Extreme cycle inflation, correct results**: Eviction-reinsertion loop or resource monopolization (forward progress but at catastrophic rate).
- **Uniform catastrophic inflation (10,000x+)**: Decode table first (fastest to audit via symmetry).

---

## 5. Bug Category Playbook

### 5.1 ALU / Comparison Bugs

- Check comparison truth tables for all branch function codes.
- Work through inverted/equal/signed/unsigned comparison paths.
- Mux arm swap on adder input inversion (ADD↔SUB) crashes at CRT startup — all benchmarks show identical cycle counts.
- Verify ALU input connections.

### 5.2 Pipeline Control Bugs

- Compare ex/mem/wb valid/kill assignments first (parallel structure).
- Check kill signals for self-referential bubble-kill terms. These are NOT part of the symmetric valid/kill chain — they are feedback within a single kill signal definition. Removing a bubble-kill term lets stale control signals from the previous instruction propagate as "valid," causing phantom operations and total hang.
- Verify each kill signal contains all required terms (redirect, exception, invalid, replay, etc.).
- Replay/xcpt swap: compare guard conditions (redirect for replay, kill for xcpt).

### 5.3 Scoreboard / Hazard Bugs

- The scoreboard-set signal must cover ALL multi-cycle operations (division, cache miss, coprocessor, vector, etc.).
- Missing one term produces a distinctive pattern: instructions that use that operation type proceed with stale data instead of stalling.
  - Missing cache-miss term: uniform massive inflation, all FAIL, programs run with wrong data.
  - Missing division term: mixed failures (some crash, some inflate — varies by division usage).
- Scoreboard `clear()` vs `set()` symmetry: `|` vs `& ~`. Missing `~` in clear inverts the mask.
- Compare cannot-bypass terms across stages — they should match semantically.

### 5.4 Bypass / Forwarding Bugs

- Compare bypass DATA sources across all entries (not just conditions/addresses).
- `0.U` as a bypass data source is always suspicious — structurally valid but semantically wrong.
- Non-load paths need registered pipeline results; load paths need cache response data. A swap causes total corruption.
- Unconditional bypass enable (`true.B` replacing a write-enable check) forwards data from non-writing instructions.
- Bypass data must follow pipeline timing: stage N bypass should use stage N+1's registered result. Using the wrong stage's data feeds stale values.

### 5.5 Branch / Redirect Bugs

- Redirect condition using `&&` instead of `||` disables one redirect path (e.g., misprediction redirect requires both misprediction AND sfence — sfence is rare, so misprediction redirect effectively disabled).
- Wrong-NPC comparison inverted (`===` instead of `=/=`) reverses misprediction detection.
- Wrong immediate type scrambles jump offsets.
- Link-address Mux swap: JAL/JALR writes target instead of link address → infinite call-return loops.

### 5.6 Register File Bugs

- Write-address Mux: long-latency vs normal writeback. Swapping these writes every long-latency result to the wrong register.
- Mux with both branches identical (e.g., `Mux(sel, wb_waddr, wb_waddr)`) — firtool optimizes it away but the semantic intent is wrong.
- Write-address bit extract must match the ISA's rd field. Using rs1/rs2 field positions writes to wrong register.

### 5.7 Timing / RegNext Bugs

- `RegNext` removal is extremely hard to spot visually — the line looks syntactically correct.
- When only one benchmark diverges, grep for uses of the differentiating instruction and trace surrounding timing-sensitive signals.
- Counter-intuitive performance direction (faster instead of slower) can occur when a stall prevents a more expensive replay.

### 5.8 Decode Table Bugs

- Use symmetry check (§3.3) as primary technique.
- Decode bugs on common instructions cause uniform massive cycle inflation (10,000x+).
- Decode bugs on rare instructions (CSR, fence) may only cause subtle performance changes or be equivalent.
- A wrong field can change an instruction's category entirely (e.g., store becomes register-writing).

---

## 6. Debugging Anti-Patterns

The most common wasted-time patterns:

1. **Tunnel vision on one file**: Bugs in instruction buffers, branch predictors, multipliers, frontends, or caches are invisible if you only read the main pipeline file. Check ALL files in the design.

2. **Re-reading the same function >3 times**: If 10+ tool calls in one file haven't converged, stop and broaden the search. Diff the Verilog instead.

3. **Ignoring code comments**: Comments documenting what a signal should cover (e.g., "stall for RAW/WAW hazards on load/AMO misses and mul/div in writeback") are invaluable for missing-term bugs.

4. **Structural-only comparison for bypass sources**: Bypass entries look structurally valid even with wrong data values. Must compare both structure AND data semantics.

5. **Not using git diff**: Exhaustive source-level searching without `git diff` wastes significant context when multiple files are changed. Always check `git status`/`git diff` in submodules first.

---

## 7. BOOM-Specific Insights

### 7.1 I-Cache resp.valid is NOT a Miss Indicator

In BOOM's frontend, `icache.io.resp.valid` deasserts not only on true I-cache misses but also during refill beats when `io.req.ready := !refill_one_beat`. Using `!resp.valid` as a miss indicator causes false activation on nearly every cycle, producing 50-100x cycle inflation.

**Rule**: To detect a true I-cache miss, check MSHR allocation or refill state — not `resp.valid` negation.

### 7.2 IQ Dispatch Livelock from Cross-IQ Stalling

When dynamically resizing instruction queue capacity, do NOT stall ALL dispatch when ANY single IQ exceeds its effective limit. If the MEM IQ is full but INT IQ has space, blocking INT dispatch prevents forward progress on non-memory instructions, creating a livelock.

**Rule**: Limit ROB occupancy (a single shared resource) rather than per-IQ dispatch. ROB limiting constrains the instruction window without creating cross-IQ dependency deadlocks.

### 7.3 Prefetcher Config Enablement

`enablePrefetching` defaults differ across BOOM configs:
- **SmallBoom**: `false` → `NullPrefetcher` (no-op)
- **Medium/Large/MegaBoom**: `true` → `NLPrefetcher` (next-line)

**Rule**: Before implementing a feature-gated module, check the target config's existing defaults. Test on a config that already enables the feature (MegaBoom for prefetchers). If no existing config enables the feature, create a dedicated test config rather than modifying a shared one.

### 7.4 Counting Bloom Filter Pitfalls (LSU)

Counting Bloom filters have three subtle false-negative failure modes:

1. **Bulk clear of surviving entries**: On exception, committed stores survive but their BF entries are destroyed. New loads get false negatives. **Fix**: Only clear entries for killed operations; decrement surviving entries individually.

2. **Multi-port same-cycle collisions**: Multiple inserts to the same BF position lose information via Chisel last-connect-wins. Subsequent individual removals cause counter underflow. **Fix**: Use per-caller-group `Vec[Bool]` Wires summed with widening adds (`+&`).

3. **Counter saturation**: Small counters overflow under pathological hash distribution. **Fix**: Size counter width so `2^width` exceeds the maximum possible entries per position.

**Config sensitivity**: SmallBoom (memWidth=1) hides multi-port collision bugs. Always test on MegaBoom (memWidth=2) for structures with concurrent access.

---

## 8. Design Patterns for Correctness

### 8.1 Module Replacement: Preserve Fallback Behavior

When replacing a module with a new implementation, always replicate the original module's core behavior as a fallback path. New module = original behavior + new behavior, not just new behavior. The new functionality augments; it does not replace the baseline until proven beneficial.

This also simplifies debugging: if the combined module matches baseline, the original-behavior path is correct and any remaining bugs are isolated to the new logic.

### 8.2 Cold-Start Protection for Learned Predictors

Predictors that learn from runtime observations are dangerous during cold start: their tables contain uninitialized data or trivial signatures that spuriously match. A false prediction from an untrained predictor can be catastrophic (e.g., a dead-block predictor that evicts live cache blocks creates a positive feedback loop).

Two complementary defenses:
1. **Warmup period**: Suppress predictions until a minimum number of observations have been recorded.
2. **Minimum-evidence threshold**: Require a per-entry observation count before allowing predictions.

**Rule**: Any predictor that acts on learned state must have both defenses. Size the warmup period to exceed the predictor's table fill time.

### 8.3 Resource-Aware Prefetch Rate Limiting

Prefetch engines that can issue multiple requests per trigger event must be rate-limited relative to available MSHRs. Without rate limiting, prefetches monopolize MSHRs, starving demand misses and causing livelock.

The failure mode is configuration-dependent: a prefetcher that works on MegaBoom (nMSHRs=8) may livelock on SmallBoom (nMSHRs=2).

**Rule**: Never allow prefetch traffic to consume more than `nMSHRs / 2` entries simultaneously. Test on the smallest available config (fewest MSHRs) to catch monopolization.

### 8.4 Speculative Eviction Loop Detection

Any mechanism that evicts instructions from a primary structure to a side buffer and later reinjects them must guarantee **no reinsertion loops**. The failure mode is distinctive: extreme cycle inflation (100–2500x) with no data corruption.

Common loop triggers:
- **Stale predictor state**: Eviction predicate remains true after reinsertion because state wasn't cleared on completion.
- **Timing race**: Completion signal clears wait bits on the same cycle as re-evaluation, but new evaluation reads old values.
- **Over-broad eviction predicate**: Predicate matches too many instructions, flooding the side buffer.

**Rule**: Before implementing any eviction-reinsertion mechanism, prove that the eviction predicate is *strictly monotonically resolved* — once cleared, it cannot reassert without a new triggering event.

### 8.5 Baseline Re-collection for Performance-Changing Optimizations

When the optimization is expected to change cycle counts (prefetchers, predictors, accelerators), DIFF results on the first test run are expected behavior, not bugs. Re-collect baselines with the optimization-enabled simulator before classifying DIFFs as failures.

### 8.6 Zero-Bug Implementation Patterns

Five patterns consistently produce zero-bug implementations:

1. **Purely observational**: Module reads existing pipeline signals and writes only to its own internal state — never to pipeline control paths.
2. **NL-fallback preserving**: Prefetchers that keep next-line as default and only augment it.
3. **Side-channel wiring**: Using `debug_pc`, separate `Reg`/`Vec` alongside the pipeline, and read-only taps instead of widening Bundles avoids CIRCT `_RANDOM` init shifts.
4. **Feature-disabled dead code**: Optimization compiles but target config doesn't enable it — tests trivially pass but optimization is not exercised. This is a test adequacy gap, not a quality signal.
5. **Activation-threshold dormant**: Optimization is present and enabled, but trigger conditions aren't met by the workload.

**Rule**: Aim for patterns 1–3 by design. Recognize patterns 4–5 as false confidence — synthesize targeted benchmarks that exercise the optimization's trigger conditions before claiming correctness.

### 8.7 Combined Observational Modules

When two observational optimizations share no pipeline signals, they can be safely co-implemented. Observational modules by definition don't write to pipeline control paths, so they can't interfere with each other.

**Rule**: Ensure distinct namespace prefixes (e.g., `cfp_` vs `vp_`) to avoid signal name collisions. No other precautions needed.

---

## 9. Ordered Debug Checklist

When all benchmarks fail (total-failure), check in this order (fastest-to-check first):

1. **ALU comparison/arithmetic** — Comparison truth tables, adder, input connections.
2. **Pipeline valid/kill chain** — Compare symmetric pattern across all stages.
3. **Register addressing** — Compare bit extracts across all stages.
4. **PC/target computation** — Branch target immediate types, link address Mux polarity, instruction size constants.
5. **Decode table** — Symmetry check on related instruction groups.
6. **Bypass conditions + data** — Both structural and semantic comparison.
7. **Scoreboard** — All multi-cycle operation terms present, set/clear symmetry.
8. **Replay guards** — Guarded conditions on replay signals.
9. **Kill signal internals** — Bubble-kill terms, full term lists for each kill signal.
10. **Generated Verilog diff** — Catches everything the above missed.
