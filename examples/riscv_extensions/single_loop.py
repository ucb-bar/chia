"""VEXT inner loop — implement ONE ISA extension on MegaBOOM and prove it.

Three stages driven by chia workers (field guide §2: the inner loop owns its
workers, iteration, snapshots, and tools):

    S1 IMPLEMENT   ONE step, iterated: implement-LLM edits BOOM -> build the
                   8-thread commit-log sim -> run the extension's self-checking
                   riscv-tests on the DUT — until all pass
    S2 REGRESSION  run the full base-ISA asm riscv-test suite on the DUT
    S3 STRESS_TEST        a batch of random tests (riscv-dv/xlm, the STRESS_TEST_MIX of
                   lengths and generator weights), generated into a pool on the
                   database node FROM RUN START and streamed through
                   parallel lockstep cosims (pending -> passed); a
                   divergence resets the pool so the whole batch re-runs after
                   the fix

Implementing and testing the implementation are two halves of the same step —
there is no separate "test stage" before S2. An S2 regression failure feeds back
to the LLM and re-enters S1 (a fix may touch anything). Once both pass, S3 runs;
the first divergence hands the failing trace to a debug-specialised LLM, which
fixes the RTL, and the verify-then-stress_test process restarts (a fresh clean stress_test is
the bar).
"""

from __future__ import annotations

import argparse
import datetime
import gzip
import io
import json
import os
import sys
import tarfile
import threading
import time
import uuid
from dataclasses import dataclass

_VEXT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_VEXT_DIR, "..", "..")) 
sys.path.insert(0, _REPO_ROOT)
sys.path.insert(0, os.path.join(_REPO_ROOT, "examples"))

import ray
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

from chia.base.ChiaFunction import get
from chia.models.claude import ClaudeCodeLLM
from chia.base.tools.BashTool import BashTool
from chia.trace.profiler import get_profiler, start_collector

from common.helper_nodes import (
    _build_children_map,
    _get_all_descendants,
    _parse_verilog_modules,
    parse_area_from_reports,
    run_cacti_macrocompiler_prep,
)
from riscv_extensions.synth_node import run_boom_tile_synthesis
from timing_opt.db import parse_worst_slack
from riscv_extensions.constants import (
    ASM_TESTS,
    SYNTH_CONFIG,
    BOOM_SRC_REL,
    CACTI_PATH,
    CHIPYARD_PATH,
    EXTENSIONS,
    Extension,
    LLM_EXTRA_ARGS,
    LLM_MODEL,
    DEBUG_MAX_ITERS,
    MAX_ITERS,
    COSIM_CONFIG,
    STRESS_TEST_HOURS,
    STRESS_TEST_MIX,
    COSIM_VRUN,
    STRESS_TEST_VRUN_FRACTION,
    SYNTH_OBJ_ROOT,
    SYNTH_TIMEOUT_S,
    VEXT_LOG_ROOT,
    sky130_vlsi_runtime_env,
)
import riscv_extensions.db_node as db_node
from riscv_extensions.nodes import (
    apply_diff,
    build_isa_test,
    build_megaboom,
    collect_diff,
    cosim_run,
    gen_to_pool,
    load_prebuilt_to_pool,
    reset_chipyard,
    verilator_run_remote,
)
from riscv_extensions.isa_tests import emit_tests, instr_table
from riscv_extensions.tools import FinishTool, KnowledgeTool, SpecTool, StatusTool

PROMPTS_DIR = os.path.join(_VEXT_DIR, "prompts")
SPECS_DIR = os.path.join(_VEXT_DIR, "specs")
HEAD_LOCAL = {"resources": {"head_local": 0.1}}   # sealed-tool placement

# In-container scratch (worker-local, ephemeral).
SIM_WORK_DIR = "/tmp/vext_verilator"
GEN_WORK_DIR = "/tmp/vext_gen"
COSIM_WORK_DIR = "/tmp/vext_cosim"
ISA_BUILD_WORK_DIR = "/tmp/vext_isabuild"   # riscv_build node: emitted diff tests


@dataclass
class VextResult:
    """Inner-loop outcome the outer driver summarizes."""

    extension: str
    converged: bool
    iterations: int
    num_pass: int
    num_tests: int
    baseline_area: float | None = None    # sky130 Total Area (Cell+Net, incl. SRAMs) — display only
    impl_area: float | None = None
    baseline_slack: float | None = None   # worst setup slack, ns
    impl_slack: float | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tail(s: str, n: int) -> str:
    return s[-n:] if len(s) > n else s


def _head(s: str, n: int) -> str:
    return s[:n] if len(s) > n else s


def _clean_conversation(transcript: bytes) -> str:
    """A readable user/assistant-only view of a claude session transcript (the
    detailed .jsonl is kept too). Pulls the text blocks and drops tool call/result
    internals. Best-effort — never raises, since logging must not break the loop."""
    out = []
    for line in transcript.splitlines():
        try:
            e = json.loads(line)
        except Exception:
            continue
        if e.get("type") not in ("user", "assistant"):
            continue
        content = (e.get("message") or {}).get("content")
        if isinstance(content, str):
            text = content
        elif isinstance(content, list):
            text = "\n".join(b.get("text", "") for b in content
                             if isinstance(b, dict) and b.get("type") == "text")
        else:
            text = ""
        if text.strip():
            out.append(f"### {(e.get('message') or {}).get('role', e['type'])}\n\n{text.strip()}\n")
    return "\n".join(out)


# Per-run extension, set by run_vext_loop. Thread-local so the outer driver's
# concurrent per-extension runs (multi_loop) tag their events correctly in
# the one shared profiler log.
_ctx = threading.local()


def _event(event, **kw):
    """Structured trace event into the chia profiler (JSONL, archived per sweep;
    no-op if the collector isn't running). Every event carries the run's
    `extension` so the shared trace splits into one top-level entry per
    extension. Our sections + intermediate results."""
    ext = getattr(_ctx, "extension", None)
    if ext and "extension" not in kw:
        kw["extension"] = ext
    get_profiler().log_event(event, **kw)


def _save_bytes(directory, name, data):
    with open(os.path.join(directory, name), "wb") as f:
        f.write(data)


def _mirror(rel: str, content) -> None:
    """Durably ship one artifact to the DB sweep AS it is produced (incremental
    archival), so a mid-run kill keeps it instead of losing everything but the
    end-of-run tar. No-op when the run isn't archiving (archive_dir unset).
    `_ctx.mirror_dir` is thread-local, so the outer driver's concurrent
    pipelines write to their own sweeps. `content` is str or bytes."""
    d = getattr(_ctx, "mirror_dir", None)
    if d:
        get(db_node.put.chia_remote(d, rel, content, extension=getattr(_ctx, "extension", "")))


def _mirror_tar(name: str, tarball: bytes) -> None:
    """Extract a tar blob into the DB sweep under `name` (e.g. a full Genus
    rundir). No-op when the run isn't archiving or the blob is empty."""
    d = getattr(_ctx, "mirror_dir", None)
    if d and tarball:
        get(db_node.archive_dir.chia_remote(d, name, tarball, extension=getattr(_ctx, "extension", "")))


def _dut_passed(run) -> bool:
    """Did the self-checking test pass on the DUT? riscv-tests signal pass via
    HTIF (clean exit); unimplemented instructions trap -> fail/timeout."""
    log = run.log or ""
    if "FAILED" in log:
        return False
    if "*** PASSED ***" in log:
        return True
    return bool(run.success)


def _stage_inputs(extension: Extension, work_root: str) -> tuple[str, str]:
    """Set up the per-pipeline status + knowledge files. The spec is read straight
    from specs/<ext>/ by SpecTool (read-only), so it isn't staged. `status.md` is
    the per-instruction status, recomputed from test results each iteration
    (read-only to the LLM, written by the loop)."""
    status_path = os.path.join(work_root, "status.md")
    knowledge_path = os.path.join(work_root, "knowledge.md")
    if not os.path.exists(knowledge_path):
        with open(knowledge_path, "w") as f:
            f.write(f"# Knowledge log — {extension.name}\n")
    return status_path, knowledge_path


def _run_tests(artifact, tests, tag, sim_logs_dir, verdict="self_check") -> list[tuple[str, str]]:
    """Run the directed tests on the freshly-built core and return [(name, log)]
    for the FAILURES. verdict="self_check": the riscv-tests assert their own
    pass/fail via HTIF (DUT alone). verdict="differential": cospike runs the DUT
    against Spike and a mismatch is the failure — for extensions with no
    self-checking tests (crypto), Spike is the oracle. Persists per-test logs."""
    # TODO: ray re-serializes `artifact` (~20MB sim + build logs) into the
    # object store once PER task here (and per stress_test cosim_run) — ray.put it
    # once and pass the ref, or dedup large args inside chia_remote.
    sl = os.path.join(sim_logs_dir, tag)
    os.makedirs(sl, exist_ok=True)
    fails: list[tuple[str, str]] = []

    if verdict == "differential":
        refs = {name: cosim_run.chia_remote(artifact, content, name, 0, COSIM_WORK_DIR, extension=getattr(_ctx, "extension", ""))
                for name, content in tests}
        for name, _ in tests:
            res = get(refs[name])
            log = f"cospike match={res.match} after {res.matched} instrs"
            with open(os.path.join(sl, f"{name}.log"), "w") as f:
                f.write(log + "\n")
            _mirror(f"sim_logs/{tag}/{name}.log", log + "\n")
            if not res.match:
                fails.append((name, log))
        return fails

    refs = {name: verilator_run_remote.chia_remote(artifact, content, name, SIM_WORK_DIR, extension=getattr(_ctx, "extension", ""))
            for name, content in tests}
    for name, _ in tests:
        vr = get(refs[name])
        with open(os.path.join(sl, f"{name}.log"), "w") as f:
            f.write(vr.log or "")
        _mirror(f"sim_logs/{tag}/{name}.log", vr.log or "")
        if not _dut_passed(vr):
            fails.append((name, vr.log or ""))
    return fails


def _directed_set(extension: Extension, march: str, work_dir: str) -> list[tuple[str, bytes]]:
    """The S1 directed test set, keyed by instruction (test NAME = instruction).
    Same for every extension: emit one program (N random executions) per
    instruction in specs/<ext>/instructions.json and cross-compile it on the
    riscv_build node. Each is then run differentially against Spike (cospike)."""
    emitted = emit_tests(extension.name)            # {f"{ext}-{mnem}": asm}
    refs = {}
    for m in instr_table(extension.name):
        asm = emitted.get(f"{extension.name}-{m}")
        if asm is not None:
            refs[m] = build_isa_test.chia_remote(asm, m.replace(".", "_"), march, work_dir, extension=getattr(_ctx, "extension", ""))
    built = [(m, get(r)) for m, r in refs.items()]
    missing = [m for m, elf in built if not elf]
    if missing:
        print(f"[directed] WARNING: {len(missing)} tests failed to build: {missing}")
    return [(m, elf) for m, elf in built if elf]


def _write_status(status_path: str, ext_name: str, ext_tests, fails) -> None:
    """Recompute the per-instruction status from the latest run for read_status —
    ground truth (pass / FAIL / no-test per instruction), not LLM self-report."""
    instrs = list(instr_table(ext_name))
    tested = {n for n, _ in ext_tests}
    failed = {n for n, _ in fails}
    npass = sum(1 for i in instrs if i in tested and i not in failed)
    lines = [f"# {ext_name} — per-instruction status (computed from the latest run)",
             f"\n{npass}/{len(instrs)} instructions passing\n"]
    for i in instrs:
        mark = "· no test" if i not in tested else ("✗ FAIL" if i in failed else "✓ pass")
        lines.append(f"- [{mark}] {i}")
    content = "\n".join(lines) + "\n"
    with open(status_path, "w") as f:
        f.write(content)
    _mirror("status.md", content)


def _first_prompt(extension: Extension, tests, fails) -> str:
    with open(os.path.join(PROMPTS_DIR, "task.md")) as f:
        template = f.read()
    failing = ", ".join(name for name, _ in fails[:40])
    return template.format(
        ext_name=extension.name,
        ext_desc=extension.description,
        boom_src=os.path.join(CHIPYARD_PATH, BOOM_SRC_REL),
        config=COSIM_CONFIG,
        isa_suffix=extension.isa_suffix,
        num_tests=len(tests),
        num_failing=len(fails),
        failing_tests=failing or "(none — the core already passes the baseline!)",
    )


def _feedback(artifact, fails, total=0, phase="extension") -> str:
    """Directed-test feedback to the LLM (build failure or failing self-checks)."""
    if artifact is not None and not artifact.success:
        return (
            f"BUILD FAILED (returncode={artifact.returncode}).\n\n"
            f"stderr (tail):\n{_tail(artifact.stderr, 3000)}\n\n"
            "Fix the BOOM Scala so the core elaborates and builds, then end your turn."
        )
    lines = [f"RESULTS ({phase}): {total - len(fails)}/{total} self-checking tests "
             "pass on your core (DUT)."]
    if fails:
        lines.append(f"\n{len(fails)} test(s) FAIL their self-check — fix these:")
        for name, log in fails[:8]:
            lines.append(f"\n----- {name} — DUT log head -----\n{_head(log, 1200)}")
        if len(fails) > 8:
            lines.append(f"\n...and {len(fails) - 8} more (read_status for the full set).")
        lines.append("\nLocate where each instruction's decode/execute is missing or wrong "
                     "in BOOM, edit the Scala, append_knowledge, and end "
                     "your turn — the loop rebuilds and re-tests. Do NOT build/run yourself.")
    return "\n".join(lines)


def _debug_prompt(res, fail_dir) -> str:
    """First message to the debug-LLM: the stress_test divergence + trace window. Also
    persists the failing trace (the only trace we keep)."""
    os.makedirs(fail_dir, exist_ok=True)
    window = ""
    if res.failing_trace_gz:
        with open(os.path.join(fail_dir, f"{res.elf_name}.faildiff.gz"), "wb") as f:
            f.write(res.failing_trace_gz)
        window = gzip.decompress(res.failing_trace_gz).decode("utf-8", "replace")
    d = res.first_divergence or {}
    return (
        f"RANDOM-STRESS_TEST DIVERGENCE on {res.elf_name} after {res.matched} matching commits.\n"
        f"Your core disagreed with Spike (golden) at this commit:\n"
        f"  spike: {d.get('spike')}\n  dut:   {d.get('dut')}\n\n"
        f"Trace window (golden vs DUT around the divergence):\n{window}\n\n"
        "Find the RTL bug in generators/boom that yields the wrong (pc,insn,rd,val), "
        "fix the Scala, and end your turn. Be surgical — do not regress passing tests."
    )


# ---------------------------------------------------------------------------
# Directed phase (S1 implement+test, S2 regression) — shared by implement and debug
# ---------------------------------------------------------------------------

def _directed_loop(llm, first_message, *, label, ext_name,
                   ext_tests, asm_tests, status_path, pg_opts, tools, finish_tool,
                   dirs, bins_dir, max_iters=MAX_ITERS,
                   failing=None, fail_dir=None) -> tuple[bool, object]:
    """Loop edit->build->directed->full-asm until all pass (return True + the
    built artifact) or max_iters (return False). `label` namespaces snapshots.
    In debug rounds `failing`=(name, elf, instr) gates FIRST on the bmk that
    diverged — the fix must beat the stimulus that broke it before anything
    else counts.
    TODO: give the debug-LLM tools to write a MINIMAL repro of the failing
    sequence (compile via RiscvBuildNode(asm) + cosim it) for fast iteration,
    and add that repro's binary to the directed suite as a regression check."""
    impl_dir, llm_logs_dir, sim_logs_dir = dirs
    message, artifact = first_message, None
    _event("section_start", name=f"directed:{label}")
    for i in range(1, max_iters + 1):
        print(f"  [{label}] iter {i}")
        # One standard LLM node per turn; the reused instance auto-resumes its
        # session (the _session_tracked decorator syncs the transcript back on get).
        cli = get(llm.prompt.options(resources={"llm": 1.0}).chia_remote(llm, message, tools))
        transcript = cli.session_transcript or b""
        if transcript:
            with open(os.path.join(llm_logs_dir, f"{label}.{i:03d}.jsonl"), "wb") as f:
                f.write(transcript)
            _mirror(f"llm_logs/{label}.{i:03d}.jsonl", transcript)
            convo = _clean_conversation(transcript)   # readable user/assistant-only view
            if convo:
                with open(os.path.join(llm_logs_dir, f"{label}.{i:03d}.md"), "w") as f:
                    f.write(convo)
                _mirror(f"llm_logs/{label}.{i:03d}.md", convo)
        diff = get(collect_diff.options(**pg_opts).chia_remote(extension=getattr(_ctx, "extension", "")))
        with open(os.path.join(impl_dir, f"{label}.{i:03d}.diff"), "w") as f:
            f.write(diff)
        _mirror(f"implementations/{label}.{i:03d}.diff", diff)

        artifact = get(build_megaboom.options(**pg_opts).chia_remote(extension=getattr(_ctx, "extension", "")))
        if not artifact.success:
            _event("directed_iter", label=label, i=i, build_ok=False)
            message = _feedback(artifact, []); continue
        _save_bytes(bins_dir, f"{label}.{i:03d}.{artifact.simulator_binary_name}",
                    artifact.simulator_binary_content)

        if failing:   # debug gate: the diverging bmk must cosim clean first
            name, elf, instr = failing
            res = get(cosim_run.chia_remote(artifact, elf, name, instr, COSIM_WORK_DIR, extension=getattr(_ctx, "extension", "")))
            if not res.match:
                _event("directed_iter", label=label, i=i, phase="failing_bmk")
                message = _debug_prompt(res, fail_dir)
                continue

        ext_fails = _run_tests(artifact, ext_tests, f"{label}.{i:03d}.{ext_name}",
                               sim_logs_dir, "differential")
        _write_status(status_path, ext_name, ext_tests, ext_fails)
        if ext_fails:
            _event("directed_iter", label=label, i=i, phase=ext_name, fails=len(ext_fails))
            message = _feedback(artifact, ext_fails, len(ext_tests), ext_name)
            if finish_tool.was_finished():
                message += "\n\nYou called finish, but tests still fail. Keep going."
                finish_tool.reset()
            continue

        asm_fails = _run_tests(artifact, asm_tests, f"{label}.{i:03d}.asm", sim_logs_dir)
        if asm_fails:
            _event("directed_iter", label=label, i=i, phase="asm", fails=len(asm_fails))
            message = _feedback(artifact, asm_fails, len(asm_tests), "full-asm regression"); continue

        print(f"  [{label}] directed PASS @ iter {i} "
              f"({len(ext_tests)} {ext_name} + {len(asm_tests)} asm)")
        _event("directed_pass", label=label, i=i,
               n_ext=len(ext_tests), n_asm=len(asm_tests))
        _event("section_end", name=f"directed:{label}", passed=True, iters=i)
        return True, artifact
    print(f"  [{label}] iteration cap {max_iters} hit without directed pass")
    _event("section_end", name=f"directed:{label}", passed=False, iters=max_iters)
    return False, artifact


# ---------------------------------------------------------------------------
# Stress_test phase (S3) — gen (xlm) -> cosim, producer/consumer
# ---------------------------------------------------------------------------

def _stress_test(artifact, gen_refs, pool_dir, deadline, fail_dir, archive_dir=None, total=None):
    """Stream the pool through the cosims until the batch passes or the first
    divergence. Returns (failing CosimResult | None, cosims done, batch complete?);
    archives the .S once generation finishes. `total` sizes the progress logs."""
    inflight, n_done, seen, archived = {}, 0, set(), False
    total = len(gen_refs) if total is None else total
    while time.time() < deadline:
        state = get(db_node.pool_state.chia_remote(pool_dir, extension=getattr(_ctx, "extension", "")))
        for name in sorted(state.keys() - seen):      # log tests as generators add them
            seen.add(name)
            print(f"  stress_test: generated {name} ({len(seen)}/{total} in pool)")
        running = set(inflight.values())
        pending = [(n, instr) for n, (s, instr) in state.items()
                   if s == "pending" and n not in running]
        # Use half the cluster's live cosim slots; adapts as verilator_run nodes
        # are added/removed (the other half is headroom for co-located roles).
        cap = max(1, int(ray.cluster_resources().get("verilator_run", 0)
                         / COSIM_VRUN * STRESS_TEST_VRUN_FRACTION))
        while pending and len(inflight) < cap:
            name, instr = pending.pop(0)
            elf = get(db_node.pool_elf.chia_remote(pool_dir, name, extension=getattr(_ctx, "extension", "")))
            inflight[cosim_run.chia_remote(artifact, elf, name, instr, COSIM_WORK_DIR, extension=getattr(_ctx, "extension", ""))] = name

        # TODO: a crashed generator counts as "done", silently shrinking the
        # batch below the dispatched count — check len(state) == len(gen_refs)
        # (or harvest gen_refs errors) before declaring the batch complete.
        gens_busy = ray.wait(gen_refs, num_returns=len(gen_refs), timeout=0)[1]
        if archive_dir and not gens_busy and not archived:   # generation done -> bank all .S once
            n = get(db_node.archive_asm.chia_remote(pool_dir, archive_dir, extension=getattr(_ctx, "extension", "")))
            if n >= 0:
                print(f"  stress_test: generation complete — archived {n} .S to {archive_dir}/asm.tar.gz")
            archived = True
        if not inflight and not pending and not gens_busy:
            return None, n_done, True                     # whole batch passed
        if not inflight:                                  # pool empty: wake on the next gen
            ray.wait(gens_busy, num_returns=1, timeout=30)
            continue
        done, _ = ray.wait(list(inflight), num_returns=1, timeout=30)
        for ref in done:
            name = inflight.pop(ref)
            res = get(ref)
            n_done += 1
            _event("stress_test_cosim", elf=name, match=res.match,
                   matched=res.matched, cycles=res.sim_cycles)
            if not res.match:
                _event("stress_test_divergence", elf=name, index=res.matched)
                return res, n_done, False                 # divergence -> stop the stress_test
            get(db_node.pool_mark.chia_remote(pool_dir, name, "passed", extension=getattr(_ctx, "extension", "")))
            print(f"  stress_test: passed {name} ({n_done}/{total} clean, "
                  f"{res.matched:,} instrs executed)")
    return None, n_done, False                            # deadline before batch finished


# ---------------------------------------------------------------------------
# sky130 PPA synthesis (area + timing): async, off the critical path
# ---------------------------------------------------------------------------

def _synth_available():
    """True if PPA synthesis should run: the run opted in (default; --no-synth sets
    `_ctx.synth=False`) AND the cluster has a VLSI (synth) node right now. Either skip
    avoids a wasted baseline build and a get() on a task that could never schedule."""
    return getattr(_ctx, "synth", True) and ray.cluster_resources().get("VLSI", 0) >= 1


def _dispatch_synth(artifact, label, run_id):
    """CACTI-characterize + MacroCompiler-remap the build's RTL onto real SRAM
    macros (driver-side, head->worker — never reverse-submitted from the synth
    worker), then fire an async BoomTile synth. None if it has no RTL or the
    BoomTile top can't be resolved. Same flow as the timing-improvement loop."""
    src = getattr(artifact, "generated_src_files", None)
    if not _synth_available():
        print(f"[{run_id}] WARNING: skipping {label} synthesis (--no-synth or no VLSI node in cluster)", flush=True)
        return None
    if not getattr(artifact, "success", False) or not src:
        return None
    print(f"[{run_id}] {label} synth prep: CACTI + MacroCompiler remap ({len(src)} RTL files)")
    gen_src, cacti_libs, vlsi_top = run_cacti_macrocompiler_prep(src, {}, CACTI_PATH)
    if vlsi_top is None:
        print(f"[{run_id}] {label} synth: no BoomTile top after prep — skipping PPA", flush=True)
        return None
    modules = _parse_verilog_modules(gen_src)
    keep = {vlsi_top} | _get_all_descendants(vlsi_top, _build_children_map(modules, set(modules)))
    filtered = [(f, c) for f, c in gen_src
                if any(m in c for m in keep) or f.endswith(".top.mems.conf")]
    print(f"[{run_id}] dispatching {label} synth (top={vlsi_top}, {len(filtered)}/{len(gen_src)} files)")
    obj_dir = os.path.join(SYNTH_OBJ_ROOT, run_id, label)
    # TODO: parametrize the synth node (run_boom_tile_synthesis) to accept a
    # clock-period / frequency setting — for now it runs at the node's default.
    return run_boom_tile_synthesis.chia_remote(filtered, vlsi_top, obj_dir, SYNTH_TIMEOUT_S, cacti_libs)


def _await_synth(ref, run_id, label):
    """get() a PPA synth result without a stuck or failed synth wedging the run: a
    timeout (never scheduled / hung) or an error logs the miss and returns None, so
    the run still finishes and PPA falls back to N/A."""
    if ref is None:
        return None
    try:
        return get(ref, timeout=SYNTH_TIMEOUT_S + 1800)   # chia get() unwraps _ProfiledResult; ray.get does not
    except ray.exceptions.GetTimeoutError:
        print(f"[{run_id}] {label} synth unfinished after {SYNTH_TIMEOUT_S + 1800}s "
              f"(never scheduled or hung) — PPA N/A", flush=True)
    except Exception as e:
        print(f"[{run_id}] {label} synth errored: {type(e).__name__}: {e} — PPA N/A", flush=True)
    ray.cancel(ref, force=True)
    return None


def _log_ppa(run_id, base, comp):
    """Baseline-vs-implemented Total Area + worst-slack delta, and archive the
    FULL Genus rundir for each. `base`/`comp` are (SynthesisResult, syn_obj
    tarball) from our run_boom_tile_synthesis node, or None.

    Area is parse_area_from_reports (Total Area = Cell + Net, includes the CACTI
    SRAM macros); slack is parse_worst_slack (ns) — both reused from the timing
    flow, no parsers of our own."""
    def metrics(x):
        if not x:
            return None, None
        result, _tar = x
        slack = parse_worst_slack(result.reports.get("syn-rundir/reports/final_constrained.rpt") or "")[0]
        return parse_area_from_reports(result.reports), slack
    ba, bs = metrics(base)
    ca, cs = metrics(comp)

    def line(a, s):
        if a is None and s is None:
            return "N/A"
        astr = f"{a:,.0f}" if a is not None else "N/A"
        sstr = (f"{s:.3f}ns ({'VIOLATED' if s < 0 else 'MET'})" if s is not None else "N/A")
        return f"area={astr} slack=[{sstr}]"
    da = f"{ca - ba:+,.0f} ({(ca - ba) / ba * 100:+.1f}%)" if (ba and ca is not None) else "N/A"
    ds = f"{cs - bs:+.3f}ns" if (bs is not None and cs is not None) else "N/A"
    print(f"[{run_id}] PPA  baseline: {line(ba, bs)}")
    print(f"[{run_id}] PPA  implemented: {line(ca, cs)}")
    print(f"[{run_id}] PPA  area delta: {da}    slack delta: {ds}")
    _event("ppa", baseline_area=ba, impl_area=ca, baseline_slack=bs, impl_slack=cs)
    _mirror("ppa.md", f"# PPA — {run_id}\n\nTotal Area = Cell + Net (incl. SRAM macros).\n\n"
            f"- baseline: {line(ba, bs)}\n- implemented: {line(ca, cs)}\n"
            f"- area delta: {da}\n- slack delta: {ds}\n")
    # Archive the full Genus rundir + quick-access reports per target.
    for label, x in (("baseline", base), ("impl", comp)):
        if not x:
            continue
        result, tar = x
        _mirror_tar(f"synth/{label}", tar)
        for rel, text in (result.reports or {}).items():
            if rel.endswith(("final_area.rpt", "final_qor.rpt", "final_constrained.rpt")):
                _mirror(f"synth/{label}/{os.path.basename(rel)}", text)
    return ba, ca, bs, cs   # (baseline_area, impl_area, baseline_slack, impl_slack) for the summary


# ---------------------------------------------------------------------------
# The loop
# ---------------------------------------------------------------------------

def run_vext_loop(extension: Extension, run_id: str, work_root: str,
                  seed_diff: str | None = None, archive_dir: str | None = None,
                  synth: bool = True, prebuilt: bool = False) -> VextResult:
    """One end-to-end loop implementing + proving `extension` on MegaBOOM.
    `seed_diff` (a prior run's probe diff) is applied after the reset so the
    pipeline can resume from a known implementation; if it already passes
    everything, the implement loop is skipped and we go straight to the stress_test.
    `archive_dir` (durable, e.g. the sweep dir) is where the stress_test packs every
    generated .S once generation completes. `synth=False` skips PPA synthesis
    entirely — for clusters with no synth node."""
    _ctx.extension = extension.name        # tag this thread's trace events
    _ctx.synth = synth                     # --no-synth / synth-less cluster -> skip PPA
    tag = run_id.rsplit("-", 1)[-1]
    # Incremental archival target: the same sweep/<ext>-<tag> dir the outer loop
    # tars into at the end, but written artifact-by-artifact as the loop runs, so
    # a mid-run kill keeps everything. Thread-local -> concurrent pipelines don't
    # collide. None when the run isn't archiving.
    _ctx.mirror_dir = os.path.join(archive_dir, f"{extension.name}-{tag}") if archive_dir else None
    os.makedirs(work_root, exist_ok=True)
    impl_dir = os.path.join(work_root, "implementations")
    llm_logs_dir = os.path.join(work_root, "llm_logs")
    sim_logs_dir = os.path.join(work_root, "sim_logs")
    fail_dir = os.path.join(work_root, "stress_test_fails")
    bins_dir = os.path.join(work_root, "bins")     # built sims (all kept)
    for d in (impl_dir, llm_logs_dir, sim_logs_dir, fail_dir, bins_dir):
        os.makedirs(d, exist_ok=True)
    dirs = (impl_dir, llm_logs_dir, sim_logs_dir)
    sentinel_path = os.path.join(work_root, "finished")
    status_path, knowledge_path = _stage_inputs(extension, work_root)
    spec_dir = os.path.join(SPECS_DIR, extension.name)
    spec_rel = os.path.relpath(spec_dir, os.path.dirname(_VEXT_DIR))   # path the LLM opens in its workdir

    # Per-pipeline placement group: editor BashTool + every build remote share
    # one container exclusively for this pipeline's lifetime.
    pg = placement_group([{"CPU": 1, "chipyard": 1}], strategy="STRICT_PACK")
    ray.get(pg.ready())
    pg_opts = {"scheduling_strategy": PlacementGroupSchedulingStrategy(
        placement_group=pg, placement_group_bundle_index=0)}

    editor = BashTool(name=f"vext_edit_{tag}", work_dir=CHIPYARD_PATH,
                      timeout_seconds=300, task_options=pg_opts)
    spec_tool = SpecTool(name=f"vext_spec_{tag}", spec_dir=spec_dir, spec_rel=spec_rel, task_options=HEAD_LOCAL)
    status_tool = StatusTool(name=f"vext_status_{tag}", status_path=status_path, task_options=HEAD_LOCAL)
    knowledge_tool = KnowledgeTool(name=f"vext_knowledge_{tag}", knowledge_path=knowledge_path, task_options=HEAD_LOCAL)
    finish_tool = FinishTool(name=f"vext_finish_{tag}", sentinel_path=sentinel_path, task_options=HEAD_LOCAL)
    finish_tool.reset()
    tools = [editor, spec_tool, status_tool, knowledge_tool, finish_tool]

    def _llm(prompt_file):
        # log_dir stays None: the prompt node runs on a worker, so its own log_dir
        # write would land on the worker's fs. We persist the session transcript on
        # the driver from cli.session_transcript instead (see _directed_loop).
        return ClaudeCodeLLM(
            model=LLM_MODEL, system_message=open(os.path.join(PROMPTS_DIR, prompt_file)).read(),
            resume_session=True, projects_cwd=None, extra_cli_args=LLM_EXTRA_ARGS)

    converged, iters = False, 0
    n_tests = 0
    gen_refs: list = []
    baseline_syn_ref = comp_syn_ref = None   # async sky130 PPA synths
    ppa = (None, None, None, None)           # (baseline_area, impl_area, baseline_slack, impl_slack)
    try:
        _event("run_start", extension=extension.name, run_id=run_id)
        # Start generating the stress_test batch NOW, in parallel with everything else:
        # the xcelium seats pace the tasks; each registers itself in the pool
        # (scratch under DB_ROOT/tmp/, finalized+deleted by the caller).
        pool_dir = db_node.pool_path(run_id)
        if prebuilt:
            gen_refs = []
            n_stress = get(load_prebuilt_to_pool.chia_remote(extension.name, pool_dir,
                                                             extension=extension.name))
            print(f"[{run_id}] --prebuilt-stress: loaded {n_stress} prebuilt tests -> pool {pool_dir} (skipping generation)")
            if n_stress == 0:
                print(f"[{run_id}] FATAL: no prebuilt binaries for {extension.name} "
                      f"(prebuilt_stress/{extension.name}.tar.gz)")
                return VextResult(extension.name, False, 0, 0, 0)
        else:
            gen_isa = f"rv64gc{extension.isa_suffix}"
            gen_refs = [gen_to_pool.chia_remote(spec, gen_isa, pool_dir, GEN_WORK_DIR, extension.dv_target, extension=extension.name)
                        for n, spec in STRESS_TEST_MIX for _ in range(n)]
            n_stress = len(gen_refs)
            print(f"[{run_id}] dispatched {n_stress} generators -> pool {pool_dir}")
        print(f"[{run_id}] resetting BOOM + fetching tests")
        get(reset_chipyard.options(**pg_opts).chia_remote(extension=extension.name))
        isa_march = f"rv64imafd_zicsr_zifencei{extension.isa_suffix}"
        # Emit the per-instruction directed tests + cross-compile them — a one-time
        # step at run start (driver-side emit, riscv_build cross-compile), traced so
        # it shows up as its own slice instead of an untracked gap before S1.
        _event("section_start", name="directed:build")
        ext_tests = _directed_set(extension, isa_march, ISA_BUILD_WORK_DIR)
        _event("section_end", name="directed:build", n_ext=len(ext_tests))
        asm_tests = get(db_node.fetch_tests.chia_remote(ASM_TESTS))
        if not ext_tests:
            print(f"[{run_id}] FATAL: no directed tests built for {extension.name} "
                  f"— check specs/{extension.name}/instructions.json and the riscv_build node")
            return VextResult(extension.name, False, 0, 0, 0)
        if not asm_tests:
            print(f"[{run_id}] WARNING: no full-asm suite staged at tests/{ASM_TESTS}/ "
                  "— the S2 regression gate will be trivially empty")
        n_tests = len(ext_tests) + len(asm_tests)

        # Pristine PPA baseline: build a STOCK config (the cosim config doesn't
        # exist in a fresh tree) on the just-reset tree and synth its BoomTile
        # for the unmodified-core area/timing reference — async, off the critical
        # path. Built before the seed so the baseline is always the pristine core,
        # and only when a synth node exists (else the extra build is wasted).
        if _synth_available():
            base_art = get(build_megaboom.options(**pg_opts).chia_remote(SYNTH_CONFIG, extension=getattr(_ctx, "extension", "")))
            baseline_syn_ref = _dispatch_synth(base_art, "baseline", run_id)
        else:
            print(f"[{run_id}] WARNING: skipping baseline synthesis (--no-synth or no VLSI node in cluster)", flush=True)
        if seed_diff:
            r = get(apply_diff.options(**pg_opts).chia_remote(seed_diff, extension=extension.name))
            print(f"[{run_id}] seed diff: {r}")
            _event("seed_diff", result=r)

        # Cosim build + verify-first. A fresh tree fails the build BY DESIGN
        # (the cosim config is the LLM's to write — prompts/system.md), so a
        # build failure enters the implement loop instead of aborting.
        artifact = get(build_megaboom.options(**pg_opts).chia_remote(extension=getattr(_ctx, "extension", "")))
        base_fails: list[tuple[str, str]] = []
        ok = False
        if artifact.success:
            _save_bytes(bins_dir, f"baseline.{artifact.simulator_binary_name}",
                        artifact.simulator_binary_content)
            base_fails = _run_tests(artifact, ext_tests, f"baseline.{extension.name}",
                                    sim_logs_dir, "differential")
            _write_status(status_path, extension.name, ext_tests, base_fails)
            # Verify-first: if the (possibly seeded) core already passes
            # everything, skip the implement loop and go straight to the stress_test.
            ok = (not base_fails
                  and not _run_tests(artifact, asm_tests, "baseline.asm", sim_logs_dir))
        if ok:
            print(f"[{run_id}] baseline passes all tests — skipping the implement loop")
            _event("baseline_pass", n_ext=len(ext_tests), n_asm=len(asm_tests))
        else:
            # S1 implement+test (with the S2 regression gate) — the implement-LLM.
            first = _first_prompt(extension, ext_tests, base_fails)
            if not artifact.success:
                first += "\n\n" + _feedback(artifact, [])
            ok, artifact = _directed_loop(
                _llm("system.md"), first, label="impl",
                ext_name=extension.name, ext_tests=ext_tests, asm_tests=asm_tests,
                status_path=status_path,
                pg_opts=pg_opts, tools=tools, finish_tool=finish_tool, dirs=dirs, bins_dir=bins_dir)
            iters += 1
        if not ok:
            return VextResult(extension.name, False, iters, len(ext_tests), n_tests)

        # Implemented PPA: synth the SYNTH config (SYNTH_CONFIG = MegaBoomChiaBigCache),
        # rebuilt on the converged tree so it carries the extension's RTL — NOT the cosim
        # artifact, whose cospike/trace/commit-log harness (the rob_debug arrays) would
        # swamp the area delta vs the harness-free baseline. Re-dispatched after each
        # debug rebuild (below); compared against the pristine baseline at convergence.
        impl_art = get(build_megaboom.options(**pg_opts).chia_remote(SYNTH_CONFIG, extension=getattr(_ctx, "extension", "")))
        comp_syn_ref = _dispatch_synth(impl_art, "impl", run_id)

        # S3 stress_test: stream the pool through the cosims; on divergence a debug-LLM
        # fixes the RTL, the pool resets, and the WHOLE batch (failing test
        # included) re-runs. Converged = every test in the batch passed.
        debug_llm, dbg_round = _llm("debug.md"), 0
        while True:
            _event("section_start", name="stress_test", round=dbg_round)
            print(f"[{run_id}] stress_test round {dbg_round}: {n_stress}-test mix, "
                  "half the cluster's cosim slots")
            fail, n_cosims, complete = _stress_test(artifact, gen_refs, pool_dir,
                                             time.time() + STRESS_TEST_HOURS * 3600, fail_dir,
                                             archive_dir=archive_dir, total=n_stress)
            _event("section_end", name="stress_test", round=dbg_round, clean=fail is None,
                   cosims=n_cosims, complete=complete)
            if fail is None:
                if complete and n_cosims > 0:
                    converged = True
                    print(f"[{run_id}] CONVERGED — full batch passed ({n_cosims} cosims)")
                    # the final, complete chipyard+BOOM diff that implements the
                    # extension — surfaced as one durable file (not just a probe).
                    _mirror("implementation.diff", get(collect_diff.options(**pg_opts).chia_remote(extension=getattr(_ctx, "extension", ""))))
                else:
                    print(f"[{run_id}] FATAL: stress_test ended with {n_cosims} cosims and an "
                          "incomplete batch (generation broken or deadline) — not converged")
                break
            dbg_round += 1
            print(f"[{run_id}] STRESS_TEST DIVERGENCE on {fail.elf_name} -> debug round {dbg_round}")
            # The stress_tested design just regressed — kill its now-stale synth; a
            # fresh one is dispatched once the debug round re-passes (below).
            if comp_syn_ref is not None:
                ray.cancel(comp_syn_ref, force=False)
                comp_syn_ref = None
            fail_elf = get(db_node.pool_elf.chia_remote(pool_dir, fail.elf_name, extension=getattr(_ctx, "extension", "")))
            fail_instr = get(db_node.pool_state.chia_remote(pool_dir, extension=getattr(_ctx, "extension", "")))[fail.elf_name][1]
            ok, artifact = _directed_loop(
                debug_llm, _debug_prompt(fail, fail_dir),
                label=f"dbg{dbg_round}", ext_name=extension.name,
                ext_tests=ext_tests, asm_tests=asm_tests, status_path=status_path,
                pg_opts=pg_opts, tools=tools, finish_tool=finish_tool, dirs=dirs,
                bins_dir=bins_dir, max_iters=DEBUG_MAX_ITERS,
                failing=(fail.elf_name, fail_elf, fail_instr), fail_dir=fail_dir)
            iters += 1
            if not ok:
                print(f"[{run_id}] debug round {dbg_round} could not re-pass directed tests")
                break
            n = get(db_node.pool_reset.chia_remote(pool_dir, extension=getattr(_ctx, "extension", "")))
            print(f"[{run_id}] pool reset — {n} tests re-run (failing one included)")
            impl_art = get(build_megaboom.options(**pg_opts).chia_remote(SYNTH_CONFIG, extension=getattr(_ctx, "extension", "")))
            comp_syn_ref = _dispatch_synth(impl_art, f"impl-dbg{dbg_round}", run_id)

        if converged:                           # compare PPA (waits on the synths)
            base = _await_synth(baseline_syn_ref, run_id, "baseline")
            comp = _await_synth(comp_syn_ref, run_id, "impl")
            baseline_syn_ref = comp_syn_ref = None   # consumed
            ppa = _log_ppa(run_id, base, comp)
    finally:
        for ref in gen_refs:                    # the run owns its generators
            ray.cancel(ref, force=True)
        for ref in (baseline_syn_ref, comp_syn_ref):   # async synths it owns too
            if ref is not None:
                ray.cancel(ref, force=True)
        for t in tools:
            t.stop()
        remove_placement_group(pg)
        # durable copy of the static bits not streamed per iteration: the spec
        # docs (read straight from specs/<ext>/) + the LLM's knowledge.md notes.
        to_mirror = [("knowledge.md", knowledge_path)]
        if os.path.isdir(spec_dir):
            to_mirror += [(f"spec/{n}", os.path.join(spec_dir, n)) for n in os.listdir(spec_dir)]
        for rel, p in to_mirror:
            if os.path.isfile(p):
                with open(p, "rb") as fh:
                    _mirror(rel, fh.read())

    _event("run_end", extension=extension.name, converged=converged, iterations=iters)
    ba, ca, bs, cs = ppa
    return VextResult(extension.name, converged, iters,
                      n_tests if converged else len(ext_tests), n_tests,
                      baseline_area=ba, impl_area=ca, baseline_slack=bs, impl_slack=cs)


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

# --- DB archival helpers (shared shape with multi_loop's per-sweep setup) ---
_VEXT_DIR = os.path.dirname(os.path.abspath(__file__))
_SNAPSHOT_EXCLUDE = ("__pycache__", ".pyc")


def _tar_dir(path: str) -> bytes:
    """Tar a dir to bytes for object-store shipping, skipping pyc/__pycache__."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as t:
        t.add(path, arcname=".",
              filter=lambda ti: None if any(x in ti.name for x in _SNAPSHOT_EXCLUDE) else ti)
    return buf.getvalue()


def _render_summary(result, sweep_n: int) -> str:
    verdict = "✓ converged" if result.converged else "✗ did not converge"
    return (f"# VEXT sweep_{sweep_n} — {result.extension}\n\n"
            f"- model: `{LLM_MODEL}` {LLM_EXTRA_ARGS}\n- result: **{verdict}**\n"
            f"- iterations: {result.iterations}\n"
            f"- tests passing: {result.num_pass}/{result.num_tests}\n"
            f"- pipeline dir: implementation.diff, ppa.md, implementations/, llm_logs/, sim_logs/, status.md\n")


def _parse_args():
    p = argparse.ArgumentParser(description="VEXT inner loop — one extension; archives to the DB.")
    p.add_argument("--extension", default="bitmanip", choices=sorted(EXTENSIONS))
    p.add_argument("--run-id", default=None)
    p.add_argument("--work-root", default=None)
    p.add_argument("--seed-diff", default=None,
                   help="probe diff from a prior run to seed BOOM with (resume)")
    p.add_argument("--no-synth", action="store_true",
                   help="skip sky130 PPA synthesis (run on a cluster with no synth node)")
    p.add_argument("--prebuilt-stress", action="store_true",
                   help="seed the stress pool from committed prebuilt binaries instead of "
                        "generating with riscv-dv (run on a cluster with no Xcelium license)")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    run_id = args.run_id or f"{ts}-{uuid.uuid4().hex[:6]}"
    work_root = args.work_root or os.path.join(VEXT_LOG_ROOT, args.extension, run_id)
    os.makedirs(work_root, exist_ok=True)

    ray.init(address="auto", runtime_env=sky130_vlsi_runtime_env())
    start_collector(log_dir=os.path.join(work_root, "profiler"))

    seed = open(args.seed_diff).read() if args.seed_diff else None
    # Durable DB sweep: claim it + snapshot src, then run with archive_dir set so the
    # loop streams every artifact (transcripts, diffs, sim logs, status, ppa,
    # implementation.diff) to the DB AS it runs — not only at the end.
    sweep_n, sweep_path = get(db_node.claim_sweep.chia_remote(args.extension))
    get(db_node.archive_dir.chia_remote(sweep_path, "src", _tar_dir(_VEXT_DIR), extension=args.extension))
    result = run_vext_loop(EXTENSIONS[args.extension], run_id, work_root,
                           seed_diff=seed, archive_dir=sweep_path, synth=not args.no_synth,
                           prebuilt=args.prebuilt_stress)
    get(db_node.pool_finalize.chia_remote(db_node.pool_path(run_id), extension=args.extension))   # .S already in the sweep
    get(db_node.archive_dir.chia_remote(sweep_path, "profiler", _tar_dir(os.path.join(work_root, "profiler")), extension=args.extension))
    get(db_node.write_text.chia_remote(sweep_path, "summary.md", _render_summary(result, sweep_n), extension=args.extension))
    print(f"\n{result}  ->  {sweep_path}")
    return 0 if result.converged else 1


if __name__ == "__main__":
    raise SystemExit(main())
