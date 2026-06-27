Reduce the critical path of an implementation of the BOOM out of order processor core.

## Arguments

`$ARGUMENTS` = `[config_name]`

- **config_name** (optional) — chipyard config to target. Defaults to `MegaBoomChiaBigCacheConfig` if omitted.

**Target config:** If `$1` is provided and is a valid config name (contains "Config"), use it. Otherwise default to `MegaBoomChiaBigCacheConfig`.

Later, we will build, run, and synthesize the target config to test your changes. Make sure that the changes you make are reflected in this config. Chipyard configs live in `generators/chipyard/src/main/scala/config/`.

## Input

You will receive a timing report. This timing report will contain the longest critical paths in an implementation of the BOOM core.

## Goal

Maximize **iron-law performance** of the BOOM core: minimize the product
`CPI × cycle_time` (equivalently, maximize `IPC × frequency`) on representative
workloads. **IPC loss is fully acceptable** — even substantial IPC loss — as
long as the frequency improvement more than pays for it. A microarchitectural
change that drops IPC by 10% but lets the design close ~25% more frequency is
a clear win; one that drops IPC by 25% for ~5% more frequency is a loss.

You are explicitly allowed to consider IPC-affecting moves: extra pipeline
stages, reduced issue/dispatch width, smaller queues, simpler schedulers,
retimed wakeup paths that delay producer→consumer by a cycle, etc. Pick
whichever level of intervention buys the most iron-law throughput on the
critical structures — even if it changes the architectural surface a little.

**Attack every high-latency cone you can.** The timing report typically shows
multiple distinct cones (logically independent critical-path families) at
similar slack. Worst-slack only drops when **all** of the top cones come down
together — fixing a single cone usually just unmasks the next one. Do not stop
after addressing one bottleneck: cluster the paths by endpoint family, then
plan and implement an edit for **each cluster that is critical or
near-critical**, not just the single worst one. Cross-cone interactions
(placement, fanout, area pressure) are unavoidable but secondary; the dominant
effect is that the wall is set by the highest cone, so the wall only moves
when you push them down in parallel.

## Available Tools

You have one MCP tool: `chipyard_bash`. Use it for everything — reading the
timing report, reading source, editing source, and structural verification.

### `chipyard_bash` — bash on the chipyard build node

Runs bash commands on a worker that has the BOOM source checked out. The BOOM v3 Scala sources are at:

    /home/ray/chipyard/generators/boom/src/main/scala/v3/

The preliminary Verilog (already elaborated from the unmodified baseline) is at:

    /home/ray/chipyard/preliminary-generated-src/

### Editing rules

**Edit files directly in-place** using targeted bash commands:

- **Read a file:** `cat .../v3/ifu/fetch-buffer.scala`
- **Read a line range:** `sed -n '10,50p' .../v3/common/parameters.scala`
- **Search:** `grep -rn 'class FetchBuffer' .../v3/`
- **Insert lines after a match:** `sed -i '/pattern/a\  new_line_here' file.scala`
- **Replace a line:** `sed -i 's/old_pattern/new_pattern/' file.scala`
- **Insert a block after a line number:** `sed -i '42a\  line1\n  line2' file.scala`
- **Append to a file:** `cat >> file.scala <<'EOF' ... EOF`
- **Create a new file:** `cat > .../v3/subdir/new_module.scala <<'EOF' ... EOF`
- **Verify your edit:** After each edit, use `sed -n 'start,endp' file.scala` to confirm the change is correct.

**IMPORTANT: Edit files in-place. Do NOT write complete file copies.** Make targeted insertions, replacements, and additions. The system will automatically detect which files you changed by diffing against the baseline.

---

## Phase 1: Read timing report and locate critical paths in BOOM core

1. Read through the timing report. Do not worry about the specific target clock period, as it is intentionally significantly overconstrained (note: the target frequency has been increased — the period is now 5 ns / 200 MHz, tighter than prior iterations). The goal is **iron-law throughput**: minimize `CPI × cycle_time`. A change that adds a pipeline stage (CPI goes up by some fraction of a cycle on dependent ops) but pulls a wide carry-chain off the critical path can easily be a net win. Compute the rough break-even — "how much IPC am I willing to give up for this much frequency?" — before deciding the move is worth it.
2. Categorize the listed critical paths by the structures they touch. A typical BOOM timing report shows several independent cones (e.g. µBTB index hash, MSHR refill drain, LSU forwarding-stall, BPD bim SRAM address, TMA counter accumulators). **Build an explicit list of every cone whose worst path is within ~500 ps of the report's worst slack.** That's your target set for this iteration.
3. Find and study the verilog implementations of these structures/paths using the `chipyard_bash` tool at path /home/ray/chipyard/preliminary-generated-src/
4. Find and study the Chisel implementations of these structures/paths in the BOOM core in /home/ray/chipyard/generators/boom/src/main/scala/v3/

---

## Phase 2: Plan

For **each cone** identified in Phase 1, reason about how to refactor the structures along it so the critical path is shortened. Don't pick one and stop — produce a planned edit for every cone in your target set, even if some edits are smaller or less aggressive than others. Worst-slack is the *max* over all cones; one edit per cone is the minimum to move it.

This may require using clever hardware techniques to reduce the length of long paths, or to eliminate the need for long logic chains entirely. Prefer no-hardware logic like shifts and low-hardware logic like bitwise operations for long logic chains over arithmetic blocks.

In some cases the easiest solution will be to add pipelining. If this is the case, then make sure that the pipelining is propogated to all structures which are affected by the pipelining.

**Details**
- The processor must still be **functionally correct**. ISA-visible behavior, memory ordering, exception semantics, and architectural register state all have to be preserved.
- IPC loss is allowed when it's earned by a larger frequency gain. **Justify each IPC-affecting move with a rough iron-law estimate** in your final response (target endpoint, expected slack improvement, expected IPC cost, why the product wins).
- Smaller structures (fewer entries, narrower issue/dispatch width, fewer ALUs) **are on the table** if the iron-law math works out — but they're the bluntest tool and often lose more IPC than the frequency they buy. Try a logic-depth restructuring first, fall back to pipelining, and only then consider shrinking a structure.
- Pipelining is a first-class option here. If you add a pipeline stage, **propagate the latency through every consumer** of the affected signal (wakeup mask, bypass, scoreboard, branch resolution, etc.) so behavior remains correct even if it stretches by a cycle.
- **Coordinate edits across cones.** If two cones share endpoints (e.g. an LSU stall signal and a perf counter that aggregates that stall), an edit on one may shift critical path inside the other — design the multi-cone plan together so the edits don't accidentally re-elongate each other.

## Phase 3: Implement

Implement the changes that you planned in phase 2 into the Boom core. Edit files directly on the chipyard node using `chipyard_bash`. Address **every cone** in your target set, not just the worst one.

**If creating a new module** (new `.scala` file):
   - Use `cat >` to create the file
   - Use the same package as related modules
   - Extend `BoomModule` with `HasBoomCoreParameters`

### Chisel Guidelines

1. **Connection operators**: Use `:=` for all connections. Only use `<>` when BOTH sides are `DecoupledIO` or `ValidIO`.
2. **Bundles**: BOOM Bundles require implicit `Parameters`.
3. **Hardware vs elaboration conditionals**: Use `when`/`.elsewhen`/`.otherwise` for hardware mux. Use Scala `if` for elaboration-time gating.
4. **Registers**: Use `RegInit(value)` when a known reset value is needed.
5. **Imports**: Preserve ALL original imports. Add new ones as needed.
6. **Unconnected ports**: Use `:= DontCare` for unconnected Bundle fields.

---

## Phase 4: Verify

After editing, verify your changes compile structurally:

1. **Check each edited file** with `sed -n` to review the modified sections.
2. **Package and imports**: confirm they are intact (`head -20 file.scala`).
3. **New IO ports**: verify both the module and its parent have matching connections.
4. **Walk the target set**: for each cone you planned to fix, confirm at least one source edit lands on the relevant module/signal.

In addition, sanity check your changes, make improvements, and fix any bugs you find.

You will **not** run any sub-block synthesis from this prompt — the surrounding pipeline runs the full BoomTile synth on what you produce. Make the multi-cone plan crisp, implement it carefully, and let the full synth report back. Your final response should list each cone you targeted, what edit you applied, and the rough iron-law math.

---

# Timing Report

The timing report has been staged on the chipyard build node at:

    $TIMING_REPORT_PATH

Use the `chipyard_bash` MCP tool to read it. The report can be large (often
hundreds of KB to several MB, with up to 500 timing paths) — **do NOT cat the
whole file** in one tool call. Instead, target only what you need:

- **Count paths:** `grep -c '^Path [0-9]' $TIMING_REPORT_PATH`
- **Top-N paths' headers (slack + endpoint at a glance):**
  `grep -E '^(Path [0-9]+:|Endpoint:|Slack)' $TIMING_REPORT_PATH | head -90`
- **Walk the top 10 paths' full detail:**
  `awk '/^Path 1:/,/^Path 11:/' $TIMING_REPORT_PATH`
- **Drill into a specific path:**
  `sed -n '/^Path 5:/,/^Path 6:/p' $TIMING_REPORT_PATH`
- **Find paths matching an endpoint pattern (e.g., a particular register):**
  `grep -B2 -A1 'tma_ctr_backend_bound' $TIMING_REPORT_PATH | head -30`

Cluster paths by endpoint family before recommending fixes — multiple paths
ending at the same register class (e.g., `int_issue_unit/slots_*/p[123]_reg`,
`tma_ctr_*_reg`) usually share a structural bottleneck and one targeted edit
can take them all out at once. **Plan an edit for every distinct critical
cluster within ~500 ps of the worst slack** — this is the "tackle all cones"
mandate at the top of this prompt.
