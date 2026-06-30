"""Chia loop that reduces BoomTile critical-path delays.

Given a parent branch in the SQLite-backed DB (see db.py) that has already been
built and synthesized, and asks Claude to edit Chisel to shorten critical paths,
rebuilds with a debug loop, then re-runs verilator and re-synthesizes BoomTile 
in parallel to measure the result. The new child branch (diff, generated source, 
reports, perf counters, logs) is recorded in the DB under DB/files/<branch>/.

When the DB is empty there is no parent to optimize, so the flow first runs
``seed_flow()``: it resets chipyard to the unmodified base RTL, builds + synthesizes
it, and stores that as the seed ("baseline") branch — the first DB entry.

Usage:
    chia job submit --working-dir . -- \\
        python improve_timing.py --branch <branch_name>
"""

import argparse
import json
import os
import re
import shutil
import sys
import time
from pathlib import Path
from uuid import uuid4

# Allow direct execution (``python examples/timing_opt/improve_timing.py``):
# the sibling-package imports below (``common.*``, ``timing_opt.*``) resolve
# relative to ``examples/``, which is only on sys.path automatically when the
# example is deployed as a ray-job working_dir or imported as a package.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import ray
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import (
    NodeAffinitySchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)

from chia.base.ChiaFunction import ChiaFunction, chia_cancel, get
from chia.models.claude import CLIResult, ClaudeCodeLLM
from chia.base.tools.BashTool import BashTool

# Build / verilator / synthesis-prep primitives now live in common (the old
# standalone timing_backend.py is gone — its verbatim-shared helpers and the
# non-debug build/verilator variants were folded into common.*).
from common.build import build_all_thread_variants, build_with_debug_retry
from common.verilator import dispatch_verilator_tests
from common.common_nodes import (
    _parse_verilog_modules,
    _resolve_boom_tile_module,
    collect_diff,
    debug_failure,
    load_prompt,
    parse_area_from_reports,
    reset_and_apply_diff,
    run_cacti_macrocompiler_prep,
)
from common.common_helpers import (
    format_test_error,
    load_test_binaries,
    parse_tma_counters,
)

from timing_opt.constants import (
    ASMTESTS_WORKLOADS_THREAD_MAP,
    BOOM_REPO_PATH,
    BUILD_CONFIG,
    CHIPYARD_PATH,
    DB_DIR,
    LLM_ENV,
    EMBENCH_TIMEOUT_CYCLES,
    EMBENCH_WORKLOADS_THREAD_MAP,
    IMPROVE_TIMING_PROMPT_PATH,
    SYN_OBJ_SCRATCH_DIR,
    TIMING_REPORT_LLM_PATH,
    TIMING_REPORT_RELPATHS,
    UBENCH_DIR,
)
from timing_opt.common_constants import RUNTIME_ENV
from timing_opt.db import TimingDB, parse_worst_slack
from timing_opt.boom_tile_syn import (
    _save_variant_results,
    run_boom_tile_synthesis,
)
from timing_opt.timing_experiment_tool import TimingExperimentTool


def load_and_configure_test_binaries(
    include_asm: bool = True,
    include_embench: bool = True,
) -> list:
    """Load and configure asmtests + embench test binaries.

    Ported from the original ``timing_backend.load_and_configure_test_binaries``
    — kept here as timing-specific glue since it is coupled to this example's
    workload thread/timeout maps (in ``timing_opt.constants``) and reads the
    binaries from this package's ``verilatorbins/`` tree.
    """
    _asmtests = load_test_binaries(UBENCH_DIR / "verilatorbins" / "asmtests") if include_asm else []
    _embench = load_test_binaries(UBENCH_DIR / "verilatorbins" / "embench") if include_embench else []
    for tb in _asmtests:
        tb.log_to_db = False
        tb.timeout_cycles = 200_000
        tb.verilator_threads = ASMTESTS_WORKLOADS_THREAD_MAP.get(tb.name, 1)
    for tb in _embench:
        tb.timeout_cycles = EMBENCH_TIMEOUT_CYCLES.get(tb.name, EMBENCH_TIMEOUT_CYCLES["default"])
        tb.verilator_threads = EMBENCH_WORKLOADS_THREAD_MAP.get(tb.name, 1)
    return _asmtests + _embench

PRELIMINARY_GENERATED_SRC_DIR = "/home/ray/chipyard/preliminary-generated-src/"

# Per-flow override of the global SYNTHESIS_TIMEOUT_SECONDS. improve_timing
# needs a longer ceiling — Genus on
# MegaBoom at sky130 can run many hours on hard-to-close corners, and the
# verilator-failure retry loop can repeat the synth dispatch up to
# max_debug_retries+1 times per run.
IMPROVE_TIMING_SYNTHESIS_TIMEOUT_SECONDS = 172800  # 48 hours

# Name of the seed (baseline) branch created from unmodified RTL.
SEED_BRANCH_NAME = "baseline"

# Inline debugger prompt: shipped in this package (no /debugging slash command
# installed in LLM_ENV required). The full markdown is read in Python and
# sent inline to debug_failure as prompt_text; the aux reference files it tells
# the LLM to read are shipped as {filename: content} and written onto the LLM
# machine per call (the {AUX_DIR} placeholder), then deleted after the call.
PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
DEBUG_AUX_FILE_NAMES = ("common_debugging.md", "chisel_debugging.md")


def _load_debug_prompt() -> tuple[str, dict[str, str]]:
    """Return (debug_prompt_text, debug_aux_files) for inline debug_failure calls."""
    text = load_prompt(PROMPTS_DIR / "debugging.md")
    aux = {name: (PROMPTS_DIR / name).read_text() for name in DEBUG_AUX_FILE_NAMES}
    return text, aux


# ---------------------------------------------------------------------------
# Remote helpers
# ---------------------------------------------------------------------------

@ChiaFunction(resources={"chipyard": 1.0})
def stage_preliminary_generated_src(
    generated_src: list[tuple[str, str]],
    dest_dir: str = PRELIMINARY_GENERATED_SRC_DIR,
) -> int:
    """Write each (filename, contents) from generated_src into dest_dir.

    Clears dest_dir first so the staging directory reflects exactly the
    input branch's generated Verilog with no leftover files.
    """
    shutil.rmtree(dest_dir, ignore_errors=True)
    os.makedirs(dest_dir, exist_ok=True)
    for fname, contents in generated_src:
        path = os.path.join(dest_dir, fname)
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(path, "w") as f:
            f.write(contents)
    return len(generated_src)


@ChiaFunction(resources={"chipyard": 1.0})
def stage_timing_report_for_llm(
    report_text: str,
    dest_path: str = TIMING_REPORT_LLM_PATH,
) -> int:
    """
    Must be scheduled on the same chipyard PG slot as chipyard_bash so the
    write lands on the host the LLM's bash commands run on.
    """
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    with open(dest_path, "w") as f:
        f.write(report_text)
    return len(report_text)


@ChiaFunction(resources={"claude_creds": 0.01})
def improve_timing_llm(
    prompt_text: str,
    tools: list,
    timeout_seconds: int = 3600,
    model: str = "claude-opus-4-8",
) -> CLIResult:
    """Invoke Claude Code on a pre-expanded /improve_timing prompt.

    Ray schedules this task directly onto a credentialed LLM worker via the
    ``claude_creds`` resource. It chdirs into LLM_ENV (an empty working
    directory created by the cluster setup), then runs the prompt
    IN-PROCESS via ``chia.models.claude.ClaudeCodeLLM`` — a local ``prompt()``
    call does not re-dispatch, so the CLI runs right here on the same worker.

    The caller is responsible for expanding the slash command (substituting
    $ARGUMENTS/$1 and concatenating the timing report) on the head node,
    because claude --print -p - does not expand project slash commands from
    piped stdin.

    ``tools`` is a list of ChiaTool instances (e.g. ``[chipyard_bash,
    experiment_tool]``); each becomes an MCP server the LLM can call.
    """
    os.chdir(LLM_ENV)
    os.makedirs("/tmp/ray/llm_logs", exist_ok=True)
    llm = ClaudeCodeLLM(
        model=model,
        timeout_seconds=timeout_seconds,
        log_dir="/tmp/ray/llm_logs",
        logging_name="improve_timing",
        extra_cli_args=["--effort", "max"],
    )
    return llm.prompt(prompt_text, tools)


# ---------------------------------------------------------------------------
# Shared pipeline helpers (used by both run_improve_timing_loop and seed_flow)
# ---------------------------------------------------------------------------


def _extract_syn_obj(archive_bytes: bytes, local_dest) -> bool:
    """Extract the worker-tarred syn_obj into the head-node DB files dir.

    The remote synthesis task tars its obj_dir on its own machine-local
    /scratch (no shared filesystem) and returns the bytes; this writes them
    out under ``local_dest``. Best-effort: returns True if the tree is in
    place. Safe to call with empty/missing bytes (e.g. after a failure).
    """
    import io
    import tarfile
    dest = Path(local_dest)
    if not archive_bytes:
        print(f"  syn_obj extract: no archive bytes, skipping ({dest})")
        return False
    try:
        if dest.exists():
            shutil.rmtree(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as tf:
            # Worker tars with arcname="syn_obj", so extracting into dest.parent
            # yields dest itself (we always name local_dest "syn_obj").
            tf.extractall(dest.parent)
    except Exception as e:
        print(f"  syn_obj extract FAILED ({dest}): {e}")
        return False
    print(f"  syn_obj extracted: {len(archive_bytes)/1e6:.1f} MB -> {dest}")
    return True


def _save_cacti_libs(db, branch, out_dir, cacti_libs) -> int:
    """Write the CACTI-generated LEF + Liberty files into out_dir/cacti_libs/ on
    the head, BEFORE synthesis runs.

    These are the exact macro views Genus consumes: filenames mirror what the
    synth node writes on the worker (``cacti_<name>.lef`` and
    ``<name>_<corner>.lib``). Saving them here lets you inspect the post-
    MacroCompiler SRAM area/timing model without unpacking the syn_obj tarball.
    Returns the number of files written; no-op on empty cacti_libs.
    """
    if not cacti_libs:
        print("  (no cacti_libs to save)")
        return 0
    lib_dir = out_dir / "cacti_libs"
    lib_dir.mkdir(parents=True, exist_ok=True)
    n = 0
    for lp in cacti_libs:
        name = lp.get("name")
        if not name:
            continue
        lef = lp.get("lef_content")
        if lef:
            (lib_dir / f"cacti_{name}.lef").write_text(lef)
            n += 1
        lib_contents = lp.get("lib_contents") or (
            {"tt_025C_1v80": lp["lib_content"]} if lp.get("lib_content") else {})
        for corner, content in lib_contents.items():
            (lib_dir / f"{name}_{corner}.lib").write_text(content)
            n += 1
    db.register_dir(branch, "cacti_libs", lib_dir)
    print(f"  saved {n} CACTI lib/lef files -> {lib_dir}")
    return n


def _save_synthesis_to_db(db, branch, out_dir, new_boom_tile, syn_result, area,
                          syn_elapsed, archive_bytes):
    """Write synthesis artifacts into the branch's files dir and register them.

    ``_save_variant_results`` writes area_estimates.json, synthesis_log.md and
    the synthesis_reports/ tree into ``out_dir`` (head-node disk) from the
    in-memory ``syn_result.reports``. ``archive_bytes`` is the worker-tarred
    syn_obj; we extract it into ``out_dir/syn_obj`` and point the DB at the
    local copy.
    """
    _save_variant_results(out_dir, new_boom_tile, syn_result, area, syn_elapsed)
    db.register_dir(branch, "synthesis_reports", out_dir / "synthesis_reports")
    db.register_file(branch, "area_estimates", out_dir / "area_estimates.json")
    db.register_file(branch, "synthesis_log", out_dir / "synthesis_log.md")
    local_syn_obj = out_dir / "syn_obj"
    _extract_syn_obj(archive_bytes, local_syn_obj)
    db.register_dir(branch, "syn_obj", local_syn_obj)


def _collect_perf(verilator_outcome):
    """Build the per-test TMA list for db.save_perf_results.

    Skips tests with no counters (e.g. riscv-tests ISA-compliance binaries that
    don't emit TMA data); records parse errors so they're visible in the DB.
    """
    perf = []
    for r in verilator_outcome.results:
        try:
            tma = parse_tma_counters(r)
            if not tma.counters:
                continue
            perf.append({
                "test_name": r.test_binary_name,
                "passed": tma.passed,
                "counters": tma.counters,
            })
        except Exception as e:
            perf.append({
                "test_name": r.test_binary_name,
                "passed": None,
                "counters": {"_tma_parse_error": str(e)},
            })
    return perf


# ---------------------------------------------------------------------------
# ExperimentLogger — head-node-pinned Ray actor for LLM-experiment DB writes
# ---------------------------------------------------------------------------

@ray.remote(num_cpus=0)
class ExperimentLogger:
    """The DB lives only on the head node (TIMING_OPT_DB_DIR), but the LLM's
    experiment tool runs in a Ray actor on the chipyard worker. This actor sits
    on the head node and is the only thing that writes to the experiments
    tables — the tool calls into it via ``ray.get_actor(name)``.
    """

    def __init__(self, db_dir: str, parent_branch: str):
        self.db = TimingDB(db_dir)
        self.parent_branch = parent_branch

    def record(self, vlsi_top, status, syn_result, archive_bytes,
               elapsed_seconds) -> dict:
        """Persist a single experiment (one synth_module call's outcome).

        ``syn_result`` may be None and ``archive_bytes`` may be empty (e.g. when
        the synth task raised) — we still record the row + status so the LLM's
        attempt is traceable.
        """
        exp_id, files_dir = self.db.create_experiment(self.parent_branch, vlsi_top)
        area = None
        if syn_result is not None:
            try:
                _save_variant_results(
                    files_dir, vlsi_top, syn_result, parse_area_from_reports(
                        syn_result.reports), elapsed_seconds,
                )
                area = parse_area_from_reports(syn_result.reports)
            except Exception as e:
                print(f"  [logger] _save_variant_results failed: {e}")
        if archive_bytes:
            _extract_syn_obj(archive_bytes, files_dir / "syn_obj")
        # Pick the preferred timing report from the in-memory reports dict and
        # store + parse worst-slack columns.
        rpt_text = ""
        if syn_result is not None:
            for relpath in TIMING_REPORT_RELPATHS:
                rpt_text = syn_result.reports.get(relpath, "") or rpt_text
                if rpt_text:
                    break
            try:
                self.db.save_experiment_timing_report(exp_id, rpt_text)
            except Exception as e:
                print(f"  [logger] save_experiment_timing_report failed: {e}")
        self.db.set_experiment_metrics(
            exp_id, status=status, area=area,
            elapsed_seconds=float(elapsed_seconds),
        )
        row = self.db.get_experiment(exp_id) or {}
        return {
            "exp_id": exp_id,
            "files_dir": str(files_dir),
            "worst_slack_ns": row.get("worst_slack_ns"),
            "worst_slack_met": row.get("worst_slack_met"),
            "worst_slack_line": row.get("worst_slack_line"),
        }


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

def _compute_out_branch(branch_name: str, output_suffix: str) -> str:
    """Return the output branch name with an auto-incremented version tag.

    If ``branch_name`` already ends with ``<output_suffix>_v<N>``, return the
    same prefix with ``N+1``. Otherwise append ``<output_suffix>_v1``.
    """
    m = re.match(rf'^(.*){re.escape(output_suffix)}_v(\d+)$', branch_name)
    if m:
        prefix, n = m.group(1), int(m.group(2))
        return f"{prefix}{output_suffix}_v{n + 1}"
    return f"{branch_name}{output_suffix}_v1"


def run_improve_timing_loop(
    db: TimingDB,
    branch_name: str,
    iteration: int = 1,
    max_debug_retries: int = 3,
    test_binaries: list | None = None,
    build_config: str = BUILD_CONFIG,
    output_suffix: str = "_timing",
    skip_llm: bool = False,
    skip_verilator: bool = False,
    output_branch: str | None = None,
    prompt_path: str = IMPROVE_TIMING_PROMPT_PATH,
    enable_experiment_tool: bool = True,
    llm_model: str = "claude-opus-4-8",
    diff_override: dict | None = None,
) -> dict:
    """Reduce BoomTile critical paths on an existing DB branch.

    Output branch name defaults to ``<branch_name><output_suffix>_v1``, or — if
    ``branch_name`` already ends with ``<output_suffix>_v<N>`` — the same
    prefix with ``N+1``. ``output_branch`` overrides the computed name (used
    for ablation re-synth runs that don't follow the auto-increment scheme).

    When ``skip_llm`` is set, Step 4 (the /improve_timing LLM call) is
    bypassed; the parent branch's diff is applied and the rest of the
    pipeline (build → verilator → synth) runs against that RTL unchanged.
    Useful for ablation re-synthesis where the diff itself is the experiment.

    When ``skip_verilator`` is set, MegaBoom is built once (only to elaborate
    the RTL + collect generated Verilog) and verilator is never dispatched —
    just CACTI + synth. No per-test TMA counters are recorded, and the chipyard
    placement group is released right after the synthesis dispatch (the build
    node is free during the long Genus run). Combine with ``skip_llm`` +
    ``diff_override`` to synthesize an external diff with no LLM and no sim.

    ``diff_override`` (a ``collect_diff``-format dict) is applied INSTEAD of the
    parent branch's diff; the parent then only supplies the staging
    generated_src/timing_report.
    """
    out_branch = output_branch or _compute_out_branch(branch_name, output_suffix)
    db.create_branch(out_branch, parent=branch_name, iteration=iteration,
                     build_config=build_config)
    out_dir = db.branch_files_dir(out_branch)
    instance_log_dir = os.path.join(str(out_dir), "logs")
    os.makedirs(instance_log_dir, exist_ok=True)
    timing_path = os.path.join(instance_log_dir, "timing.csv")
    if not os.path.isfile(timing_path):
        with open(timing_path, "w") as f:
            f.write("iteration,step,duration_s\n")

    def _write_log(text: str) -> None:
        p = out_dir / "improve_timing_log.md"
        p.write_text(text)
        db.register_file(out_branch, "improve_timing_log", p)

    # Inline debugger prompt: threaded into every debug_failure call (build +
    # verilator failures) so the example needs no /debugging slash command
    # installed in LLM_ENV.
    debug_prompt_text, debug_aux_files = _load_debug_prompt()

    pg = None
    pg_opts: dict | None = None
    chipyard_bash = None
    experiment_tool = None
    experiment_logger = None
    inputs: dict | None = None
    diff_applied = False
    final_result: dict | None = None
    try:
        # ---- Step 1: load inputs from the DB ----
        print(f"\n{'='*60}\nStep 1: loading inputs for '{branch_name}'\n{'='*60}")
        inputs = db.load_inputs(branch_name)
        print(
            f"  BoomTile module: {inputs['boom_tile']}\n"
            f"  generated_src files: {len(inputs['generated_src'])}\n"
            f"  timing report: {inputs['timing_report_path']} "
            f"({len(inputs['timing_report'])} chars)"
        )
        # diff_override: synthesize an *externally supplied* diff
        # instead of the parent branch's diff.
        # The parent only provides the vestigial generated_src/timing_report/
        # boom_tile (Step 3a staging + the old-vs-new slack compare); Step 3b
        # applies this diff and Step 5 rebuilds Chisel from it. Pair with
        # skip_llm=True to get apply-diff -> build -> verilator -> synth, no LLM.
        if diff_override is not None:
            nonempty = [k for k, v in diff_override.items() if v]
            total = sum(len(v or "") for v in diff_override.values())
            print(f"  diff override: using supplied diff "
                  f"({len(nonempty)} non-empty part(s): {nonempty}, {total} chars) "
                  f"instead of parent '{branch_name}' diff")
            inputs["diff_dict"] = diff_override

        # ---- Step 2: acquire chipyard placement group + bash tool ----
        print(f"\n{'='*60}\nStep 2: acquire chipyard placement group + bash tool\n{'='*60}")
        pg = placement_group([{"CPU": 1, "chipyard": 1}], strategy="STRICT_PACK")
        ray.get(pg.ready())
        pg_opts = {
            "scheduling_strategy": PlacementGroupSchedulingStrategy(
                placement_group=pg, placement_group_bundle_index=0,
            )
        }
        bash_name = f"chipyard_bash_improve_timing_{uuid4().hex[:8]}"
        chipyard_bash = BashTool(
            name=bash_name, work_dir=CHIPYARD_PATH, task_options=pg_opts,
            timeout_seconds=600,
        )

        # ---- Step 2b: spawn ExperimentLogger (head-node-pinned) + the LLM's
        # experiment tool (chipyard-pg-pinned). The tool lets Claude run cheap
        # sub-block syntheses during Step 4; the logger is the only DB writer
        # for experiments since the tool actor (on a worker) can't see the
        # head's DB dir.
        if not skip_llm and enable_experiment_tool:
            head_node_id = ray.get_runtime_context().get_node_id()
            logger_name = f"experiment_logger_{uuid4().hex[:8]}"
            experiment_logger = ExperimentLogger.options(
                name=logger_name,
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=head_node_id, soft=False,
                ),
            ).remote(DB_DIR, out_branch)

            # Pin the experiment-tool actor to the head node.
            tool_head_pin_opts = {
                "scheduling_strategy": NodeAffinitySchedulingStrategy(
                    node_id=head_node_id, soft=False,
                )
            }
            experiment_tool = TimingExperimentTool(
                name=f"timing_experiment_{uuid4().hex[:8]}",
                chipyard_path=CHIPYARD_PATH,
                build_config=build_config,
                parent_branch=out_branch,
                logger_actor_name=logger_name,
                pg_opts=pg_opts,
                head_node_id=head_node_id,
                task_options=tool_head_pin_opts,
                # Parent's unmodified RTL — already loaded by load_inputs above.
                # Lets synth_module_parent run an A/B baseline on the same
                # vlsi_top without re-elaborating Chisel.
                parent_generated_src=inputs["generated_src"],
            )

        # ---- Step 3a: stage generated_src onto the chipyard node ----
        print(f"\n{'='*60}\nStep 3a: stage generated_src into {PRELIMINARY_GENERATED_SRC_DIR}\n{'='*60}")
        n_staged = get(
            stage_preliminary_generated_src.options(**pg_opts).chia_remote(
                inputs["generated_src"], PRELIMINARY_GENERATED_SRC_DIR,
            )
        )
        print(f"  Staged {n_staged} files")

        # ---- Step 3b: reset chipyard + apply diff ----
        print(f"\n{'='*60}\nStep 3b: reset chipyard and apply diff\n{'='*60}")
        err, msg = get(
            reset_and_apply_diff.options(**pg_opts).chia_remote(inputs["diff_dict"])
        )
        print(f"  reset_and_apply_diff: {msg}")
        if err:
            return (final_result := {"status": "diff_apply_failed", "message": msg, "out_branch": out_branch})
        diff_applied = True

        # ---- Step 4: LLM — /improve_timing with timing report ----
        if skip_llm:
            print(f"\n{'='*60}\nStep 4: SKIPPED (--skip-llm) — running build+verilator+synth on parent diff unchanged\n{'='*60}")
        else:
            print(f"\n{'='*60}\nStep 4: LLM /improve_timing {build_config}\n{'='*60}")
            # Stage the parent branch's timing report on the chipyard node so the
            # LLM can read it via chipyard_bash
            report_chars = get(
                stage_timing_report_for_llm.options(**pg_opts).chia_remote(
                    inputs["timing_report"], TIMING_REPORT_LLM_PATH,
                )
            )
            print(f"  Timing report staged at {TIMING_REPORT_LLM_PATH} "
                  f"({report_chars} chars on the chipyard node)")

            # Pre-expand the slash command on the head node. claude --print -p -
            # (piped stdin, print mode) does not expand project slash commands,
            # so the LLM would otherwise receive a literal "/improve_timing ..."
            # string and have to manually read the command file — observed to
            # cause the model to flail with its local Bash tool before
            # eventually routing to chipyard_bash, and sometimes to skip the
            # Phase 3 implement step entirely.
            with open(prompt_path) as f:
                cmd_template = f.read()
            prompt_text = (
                cmd_template
                .replace("$ARGUMENTS", build_config)
                .replace("$1", build_config)
                .replace("$TIMING_REPORT_PATH", TIMING_REPORT_LLM_PATH)
            )
            prompt_log_path = os.path.join(instance_log_dir, "improve_timing_prompt.md")
            with open(prompt_log_path, "w") as f:
                f.write(prompt_text)
            print(f"  Prompt saved to {prompt_log_path} ({len(prompt_text)} chars)")

            t0 = time.time()
            llm_tools = [chipyard_bash]
            if experiment_tool is not None:
                llm_tools.append(experiment_tool)
            cli = get(improve_timing_llm.chia_remote(
                prompt_text, llm_tools, model=llm_model,
            ))
            print(f"  LLM finished in {time.time()-t0:.1f}s (success={cli.success})")
            llm_log_path = os.path.join(instance_log_dir, "improve_timing_llm.md")
            with open(llm_log_path, "w") as f:
                f.write(f"# /improve_timing response\nSuccess: {cli.success}\n\n")
                f.write(cli.result or "")
                if cli.stream_result:
                    f.write("\n\n## Stream log (tool calls, file reads, bash)\n\n")
                    f.write(cli.stream_result)
                else:
                    f.write("\n\n## Stream log\n\n(cli.stream_result was empty)\n")
            if not cli.success:
                print(f"  LLM failed — exiting (diff revert handled in finally)")
                return (final_result := {"status": "llm_failed", "out_branch": out_branch})

        # ---- Step 5: build MegaBoom with debug retry ----
        if skip_verilator:
            # Skipping verilator: a single Chisel build is enough to elaborate
            # the RTL and collect the generated Verilog for synthesis — the
            # multiple thread variants only exist to run verilator at different
            # thread counts, which we won't do.
            print(f"\n{'='*60}\nStep 5: build MegaBoom (single variant — verilator skipped)\n{'='*60}")
            artifact = build_with_debug_retry(
                chipyard_bash, iteration=iteration, log_dir=instance_log_dir,
                timing_path=timing_path, max_retries=max_debug_retries,
                label="improve_timing", chipyard_task_options=pg_opts,
                verilator_threads=1,
                collect_generated_src=True, debug_log_base_dir=instance_log_dir,
                branch_name=out_branch,
                debug_prompt_text=debug_prompt_text,
                debug_aux_files=debug_aux_files,
            )
            artifacts_by_threads = {1: artifact} if artifact is not None else None
        else:
            print(f"\n{'='*60}\nStep 5: build MegaBoom (all thread variants)\n{'='*60}")
            if test_binaries is None:
                test_binaries = load_and_configure_test_binaries()
            print(f"  Loaded {len(test_binaries)} test binaries")
            artifacts_by_threads = build_all_thread_variants(
                test_binaries=test_binaries,
                chipyard_bash=chipyard_bash,
                iteration=iteration,
                log_dir=instance_log_dir,
                timing_path=timing_path,
                max_debug_retries=max_debug_retries,
                label="improve_timing",
                chipyard_task_options=pg_opts,
                collect_generated_src_for_first=True,
                debug_log_base_dir=instance_log_dir,
                branch_name=out_branch,
                debug_prompt_text=debug_prompt_text,
                debug_aux_files=debug_aux_files,
            )
        if artifacts_by_threads is None:
            print(f"  Build failed after {max_debug_retries} retries")
            _write_log(
                f"# improve_timing\n\nBuild failed after {max_debug_retries} debug retries.\n"
                f"Parent branch: {branch_name}\n"
            )
            return (final_result := {"status": "build_failed", "out_branch": out_branch})

        first_artifact = next(iter(artifacts_by_threads.values()))
        print(f"  Built {len(artifacts_by_threads)} thread variant(s)")

        # Persist the post-LLM diff + generated_src immediately so they survive
        # any downstream failure.
        _err, fresh_diff = get(
            collect_diff.options(**pg_opts).chia_remote()
        )
        if not _err:
            db.save_diff(out_branch, fresh_diff)
        db.save_generated_src(out_branch, first_artifact.generated_src_files)

        # ---- Step 6 loop: prep → dispatch synthesis → run verilator, with
        #      verilator-failure recovery (cancel syn, debug, rebuild, retry). ----
        # Each iteration redoes CACTI + MacroCompiler because the debugger may
        # have changed RTL-generator code → different generated Verilog.
        debug_session_id = str(uuid4())
        syn_ref = None
        syn_result = None
        verilator_outcome = None
        gen_src = None
        new_boom_tile = None
        cacti_libs = None
        # obj_dir is a path on the synth worker's local /scratch (the cluster
        # has no shared filesystem); the worker creates/cleans it, tars it, and
        # returns the bytes. The head node never reads this path directly.
        obj_dir = os.path.join(SYN_OBJ_SCRATCH_DIR, out_branch, "syn_obj")
        t_syn = 0.0

        for retry_attempt in range(max_debug_retries + 1):
            # ---- Step 6 prep: CACTI + MacroCompiler remap ----
            print(f"\n{'='*60}\nStep 6 prep: CACTI + MacroCompiler remap (attempt {retry_attempt+1})\n{'='*60}")
            gen_src, cacti_libs, new_boom_tile = run_cacti_macrocompiler_prep(
                first_artifact.generated_src_files, pg_opts,
            )
            if new_boom_tile is None:
                print(f"  ERROR: could not resolve BoomTile in new generated_src")
                return (final_result := {"status": "boom_tile_resolve_failed", "out_branch": out_branch})

            # Save the post-MacroCompiler LEF/Liberty (what Genus will consume)
            # to the head before synthesis kicks off.
            _save_cacti_libs(db, out_branch, out_dir, cacti_libs)

            # ---- Step 6a: dispatch synthesis (async) ----
            # The worker rmtrees its obj_dir at start of run_boom_tile_synthesis,
            # so each attempt gets a fresh tree regardless of where it lands.
            t_syn = time.time()
            print(f"\n{'='*60}\nStep 6a: dispatching BoomTile synthesis (vlsi_top={new_boom_tile})\n{'='*60}")
            syn_ref = run_boom_tile_synthesis.chia_remote(
                gen_src, new_boom_tile, obj_dir, IMPROVE_TIMING_SYNTHESIS_TIMEOUT_SECONDS, cacti_libs,
            )

            # ---- Step 6b: run verilator in parallel (skipped if --skip-verilator) ----
            if skip_verilator:
                # Synthesis is already dispatched to a VLSI node (it never used
                # the chipyard PG), and with verilator skipped there is no retry
                # loop that would need the chipyard node again. So free the
                # chipyard build node NOW instead of holding it idle through the
                # (potentially many-hour) Genus run — this decouples chipyard-slot
                # count from synth time. Nulling pg/chipyard_bash/pg_opts stops
                # the finally block from double-freeing; it also disables the
                # finally diff-revert, which is fine because the next run's
                # reset_and_apply_diff restores a clean tree regardless.
                print(f"\n{'='*60}\nStep 6b: SKIPPED verilator (--skip-verilator); releasing chipyard PG, then awaiting synthesis\n{'='*60}")
                verilator_outcome = None
                try:
                    if chipyard_bash is not None:
                        chipyard_bash.stop()
                except Exception as e:
                    print(f"  early-release: chipyard_bash.stop() failed: {e}")
                try:
                    if pg is not None:
                        remove_placement_group(pg)
                        print(f"  early-release: freed chipyard placement group "
                              f"(build node available for other jobs during synth)")
                except Exception as e:
                    print(f"  early-release: remove_placement_group failed: {e}")
                pg = None
                chipyard_bash = None
                pg_opts = None
                break
            print(f"\n{'='*60}\nStep 6b: dispatch verilator tests (synthesis runs in parallel)\n{'='*60}")
            t_ver = time.time()
            verilator_outcome = dispatch_verilator_tests(
                artifacts_by_threads=artifacts_by_threads,
                test_binaries=test_binaries,
            )
            ver_elapsed = time.time() - t_ver
            print(
                f"  Verilator returned in {ver_elapsed:.1f}s: "
                f"{len(verilator_outcome.results)} total, "
                f"{len(verilator_outcome.failed)} failed, "
                f"cancelled={verilator_outcome.cancelled}"
            )

            if len(verilator_outcome.failed) == 0:
                break

            # ---- Verilator failed: cancel syn_ref, wait for it to stop, debug, rebuild, loop ----
            print(
                f"\n[improve_timing] Verilator failed with {len(verilator_outcome.failed)} "
                f"failures (attempt {retry_attempt+1}/{max_debug_retries+1})"
            )
            try:
                print(f"  Cancelling in-flight synthesis (force=False) and waiting for it to stop")
                chia_cancel(syn_ref, force=False)
                # Block until the cancelled task actually stops, so its final
                # writes don't race the next iteration's obj_dir rmtree +
                # re-dispatch.
                ray.wait([syn_ref])
            except Exception as e:
                print(f"  chia_cancel/wait failed: {e}")
            syn_ref = None

            if retry_attempt >= max_debug_retries:
                print(f"  Max debug retries exhausted — giving up on verilator")
                # syn_ref was cancelled and never returned, so we have no
                # archive bytes to extract — syn_obj is not recoverable here.
                failed_summary = "\n".join(
                    f"- {r.test_binary_name} (rc={r.returncode})"
                    for r in verilator_outcome.failed
                ) or "(none listed)"
                retry_artifacts = []
                for i in range(1, max_debug_retries + 1):
                    ec = os.path.join(instance_log_dir, f"error_context_retry{i}.txt")
                    dl = os.path.join(instance_log_dir, f"debug_failure_retry{i}.md")
                    if os.path.isfile(ec) or os.path.isfile(dl):
                        retry_artifacts.append(
                            f"- retry {i}: "
                            f"{ec if os.path.isfile(ec) else '(no error_context)'}, "
                            f"{dl if os.path.isfile(dl) else '(no debug_log)'}"
                        )
                retry_artifacts_text = "\n".join(retry_artifacts) or "(none)"
                _write_log(
                    f"# improve_timing: {out_branch}\n"
                    f"Status: verilator_failed after {max_debug_retries} debug retries\n"
                    f"Parent branch: {branch_name}\n"
                    f"Build config: {build_config}\n"
                    f"Log dir: {instance_log_dir}\n\n"
                    f"## Final verilator failures "
                    f"({len(verilator_outcome.failed)}/{len(verilator_outcome.results)})\n"
                    f"{failed_summary}\n\n"
                    f"## Debug retry artifacts\n"
                    f"{retry_artifacts_text}\n\n"
                    f"## /improve_timing LLM response\n"
                    f"- {os.path.join(instance_log_dir, 'improve_timing_llm.md')}\n"
                )
                return (final_result := {
                    "status": "verilator_failed",
                    "out_branch": out_branch,
                    "verilator_failed": len(verilator_outcome.failed),
                    "failed_tests": [r.test_binary_name for r in verilator_outcome.failed],
                    "log_dir": instance_log_dir,
                })

            # Run debug_failure with persistent session so the LLM remembers
            # prior fix attempts across retries.
            error_ctx = format_test_error(
                verilator_outcome.failed, iteration, retry_attempt + 1, BOOM_REPO_PATH,
            )
            err_ctx_path = os.path.join(
                instance_log_dir, f"error_context_retry{retry_attempt+1}.txt"
            )
            with open(err_ctx_path, "w") as f:
                f.write(error_ctx)
            t_dbg = time.time()
            dbg_cli = get(debug_failure.chia_remote(
                error_ctx, chipyard_bash, retry_attempt + 1,
                session_id=debug_session_id,
                llm_env=LLM_ENV,
                prompt_text=debug_prompt_text,
                aux_files=debug_aux_files,
            ))
            dbg_log_path = os.path.join(
                instance_log_dir, f"debug_failure_retry{retry_attempt+1}.md"
            )
            with open(dbg_log_path, "w") as f:
                f.write(
                    f"# debug_failure (retry={retry_attempt+1})\n"
                    f"Success: {dbg_cli.success}\n\n"
                )
                f.write(dbg_cli.result or "")
                if dbg_cli.stream_result:
                    f.write("\n\n## Stream log\n\n")
                    f.write(dbg_cli.stream_result)
            print(f"  debug_failure finished in {time.time()-t_dbg:.1f}s "
                  f"(success={dbg_cli.success})")
            if not dbg_cli.success:
                print(f"  Debugger failed — giving up")
                return (final_result := {"status": "debug_failed", "out_branch": out_branch})

            # Rebuild after debugger edits
            print(f"\n{'='*60}\nStep 6c: rebuild after debug (retry {retry_attempt+1})\n{'='*60}")
            artifacts_by_threads = build_all_thread_variants(
                test_binaries=test_binaries,
                chipyard_bash=chipyard_bash,
                iteration=iteration,
                log_dir=instance_log_dir,
                timing_path=timing_path,
                max_debug_retries=max_debug_retries,
                label=f"improve_timing_retry{retry_attempt+1}",
                chipyard_task_options=pg_opts,
                collect_generated_src_for_first=True,
                debug_log_base_dir=instance_log_dir,
                branch_name=out_branch,
                debug_prompt_text=debug_prompt_text,
                debug_aux_files=debug_aux_files,
            )
            if artifacts_by_threads is None:
                print(f"  Rebuild failed after debug — giving up")
                return (final_result := {"status": "rebuild_failed", "out_branch": out_branch})
            first_artifact = next(iter(artifacts_by_threads.values()))

            # Refresh persisted diff + generated_src so they reflect the debugger's fix.
            _err, fresh_diff = get(collect_diff.options(**pg_opts).chia_remote())
            if not _err:
                db.save_diff(out_branch, fresh_diff)
            db.save_generated_src(out_branch, first_artifact.generated_src_files)

        # ---- Step 7: collect synthesis result (may have already finished) ----
        print(f"\n{'='*60}\nStep 7: awaiting synthesis result\n{'='*60}")
        ready, _ = ray.wait([syn_ref], timeout=0.0)
        if ready:
            print(f"  Synthesis finished before verilator")
        else:
            print(f"  Synthesis still running — waiting")
        try:
            syn_result, archive_bytes = get(syn_ref)
        except Exception as e:
            print(f"  Synthesis task raised: {e}")
            # No archive bytes returned, so syn_obj is not recoverable here.
            _write_log(
                f"# improve_timing\n\nSynthesis task raised: {e}\n"
                f"Parent branch: {branch_name}\n"
            )
            return (final_result := {
                "status": "synthesis_failed",
                "out_branch": out_branch,
                "error": str(e),
            })
        syn_elapsed = time.time() - t_syn
        area = parse_area_from_reports(syn_result.reports)
        print(
            f"  Synthesis done in {syn_elapsed:.1f}s: "
            f"success={syn_result.success}, area={area}, reports={len(syn_result.reports)}"
        )

        # ---- Step 8: persist results to the DB ----
        print(f"\n{'='*60}\nStep 8: persist results to {out_dir}\n{'='*60}")
        _save_synthesis_to_db(db, out_branch, out_dir, new_boom_tile, syn_result, area, syn_elapsed, archive_bytes)

        # perf_results: per-test TMA counters (md file + queryable rows). None
        # when verilator was skipped (--skip-verilator).
        if verilator_outcome is not None:
            db.save_perf_results(out_branch, _collect_perf(verilator_outcome))
            ver_passed = len(verilator_outcome.results) - len(verilator_outcome.failed)
            ver_failed = len(verilator_outcome.failed)
            ver_summary = f"Verilator results: {len(verilator_outcome.results)} ({ver_failed} failed)"
        else:
            ver_passed = ver_failed = None
            ver_summary = "Verilator: skipped (--skip-verilator)"

        # produced timing report (sets the queryable worst-slack columns).
        new_rpt_text = ""
        for relpath in TIMING_REPORT_RELPATHS:
            new_rpt_text = syn_result.reports.get(relpath, "")
            if new_rpt_text:
                break
        db.save_timing_report(out_branch, new_rpt_text)

        # improve_timing_log.md: short summary + old-vs-new worst slack
        old_worst = _worst_slack_line(inputs["timing_report"])
        new_worst = _worst_slack_line(new_rpt_text) if new_rpt_text else "(no report)"
        summary = [
            f"# improve_timing: {out_branch}",
            f"Parent branch: {branch_name}",
            f"Build config: {build_config}",
            f"BoomTile (parent): {inputs['boom_tile']}",
            f"BoomTile (child):  {new_boom_tile}",
            ver_summary,
            f"Synthesis success: {syn_result.success}, area: {area}",
            f"",
            f"## Worst-slack comparison",
            f"Parent: {old_worst}",
            f"Child:  {new_worst}",
        ]
        _write_log("\n".join(summary) + "\n")

        db.set_metrics(
            out_branch,
            status="ok",
            area=area,
            synthesis_success=int(bool(syn_result.success)),
            verilator_passed=ver_passed,
            verilator_failed=ver_failed,
            boom_tile_module=new_boom_tile,
        )

        print(f"\n{'='*60}\nDone: {out_branch}\n{'='*60}")
        return (final_result := {
            "status": "ok",
            "out_branch": out_branch,
            "area": area,
            "synthesis_success": syn_result.success,
            "verilator_passed": ver_passed,
            "verilator_failed": ver_failed,
            "worst_slack_parent": old_worst,
            "worst_slack_child": new_worst,
        })
    finally:
        # On failure (or unhandled exception), revert chipyard to the parent
        # branch's diff so the tree is left in a clean, reproducible state.
        # Success leaves the LLM/debugger edits in place — they're already
        # persisted to the DB.
        on_failure = final_result is None or final_result.get("status") != "ok"
        if diff_applied and on_failure and pg_opts is not None and inputs is not None:
            try:
                print(f"  cleanup: reverting diff to parent branch state")
                get(reset_and_apply_diff.options(**pg_opts).chia_remote(inputs["diff_dict"]))
            except Exception as e:
                print(f"  cleanup: revert diff failed: {e}")
        # Register the per-run log directory and record the final status.
        try:
            db.register_dir(out_branch, "log", instance_log_dir)
            if final_result is not None:
                db.set_metrics(out_branch, status=final_result.get("status"))
            elif on_failure:
                db.set_metrics(out_branch, status="error")
        except Exception as e:
            print(f"  cleanup: db status/log update failed: {e}")
        if experiment_tool is not None:
            try:
                experiment_tool.stop()
            except Exception as e:
                print(f"  cleanup: experiment_tool.stop() failed: {e}")
        if experiment_logger is not None:
            try:
                ray.kill(experiment_logger)
            except Exception as e:
                print(f"  cleanup: experiment_logger kill failed: {e}")
        if chipyard_bash is not None:
            try:
                chipyard_bash.stop()
            except Exception as e:
                print(f"  cleanup: chipyard_bash.stop() failed: {e}")
        if pg is not None:
            try:
                remove_placement_group(pg)
            except Exception as e:
                print(f"  cleanup: remove_placement_group failed: {e}")


def seed_flow(
    db: TimingDB,
    build_config: str = BUILD_CONFIG,
    max_debug_retries: int = 3,
    iteration: int = 0,
) -> str | None:
    """Bootstrap the DB: build + synthesize the *unmodified* base RTL as the seed.

    Runs when there is no parent to optimize. Resets chipyard to a clean tree
    (empty diff), builds MegaBoom, runs CACTI + MacroCompiler, synthesizes
    BoomTile (so the seed carries the timing report the optimizer reads), runs
    verilator for the baseline TMA, and stores everything as the ``baseline``
    branch (the first DB entry). Returns the seed branch name, or None on failure.

    No LLM editing step; a clean baseline is expected to build and pass (build
    errors still go through build_all_thread_variants' debug retry).
    """
    out_branch = SEED_BRANCH_NAME
    db.create_branch(out_branch, parent=None, is_seed=True, iteration=iteration,
                     build_config=build_config)
    out_dir = db.branch_files_dir(out_branch)
    instance_log_dir = os.path.join(str(out_dir), "logs")
    os.makedirs(instance_log_dir, exist_ok=True)
    timing_path = os.path.join(instance_log_dir, "timing.csv")
    if not os.path.isfile(timing_path):
        with open(timing_path, "w") as f:
            f.write("iteration,step,duration_s\n")

    debug_prompt_text, debug_aux_files = _load_debug_prompt()

    pg = None
    pg_opts: dict | None = None
    chipyard_bash = None
    status = "seed_failed"
    try:
        # ---- acquire chipyard placement group + bash tool ----
        print(f"\n{'='*60}\nSeed: acquire chipyard placement group + bash tool\n{'='*60}")
        pg = placement_group([{"CPU": 1, "chipyard": 1}], strategy="STRICT_PACK")
        ray.get(pg.ready())
        pg_opts = {
            "scheduling_strategy": PlacementGroupSchedulingStrategy(
                placement_group=pg, placement_group_bundle_index=0,
            )
        }
        bash_name = f"chipyard_bash_seed_{uuid4().hex[:8]}"
        chipyard_bash = BashTool(
            name=bash_name, work_dir=CHIPYARD_PATH, task_options=pg_opts,
            timeout_seconds=600,
        )

        # ---- reset chipyard to clean, unmodified RTL (empty diff) ----
        print(f"\n{'='*60}\nSeed: reset chipyard to unmodified base RTL\n{'='*60}")
        err, msg = get(reset_and_apply_diff.options(**pg_opts).chia_remote({}))
        print(f"  reset_and_apply_diff: {msg}")

        # ---- build all thread variants ----
        print(f"\n{'='*60}\nSeed: build MegaBoom (all thread variants)\n{'='*60}")
        test_binaries = load_and_configure_test_binaries()
        print(f"  Loaded {len(test_binaries)} test binaries")
        artifacts_by_threads = build_all_thread_variants(
            test_binaries=test_binaries,
            chipyard_bash=chipyard_bash,
            iteration=iteration,
            log_dir=instance_log_dir,
            timing_path=timing_path,
            max_debug_retries=max_debug_retries,
            label="seed_baseline",
            chipyard_task_options=pg_opts,
            collect_generated_src_for_first=True,
            debug_log_base_dir=instance_log_dir,
            branch_name=out_branch,
            debug_prompt_text=debug_prompt_text,
            debug_aux_files=debug_aux_files,
        )
        if artifacts_by_threads is None:
            print(f"  Seed build failed after {max_debug_retries} retries")
            status = "build_failed"
            return None
        first_artifact = next(iter(artifacts_by_threads.values()))

        # ---- persist clean diff + generated_src ----
        _err, seed_diff = get(collect_diff.options(**pg_opts).chia_remote())
        db.save_diff(out_branch, seed_diff if not _err else {})
        db.save_generated_src(out_branch, first_artifact.generated_src_files)

        # ---- CACTI + MacroCompiler remap → resolve BoomTile ----
        print(f"\n{'='*60}\nSeed: CACTI + MacroCompiler remap\n{'='*60}")
        gen_src, cacti_libs, new_boom_tile = run_cacti_macrocompiler_prep(
            first_artifact.generated_src_files, pg_opts,
        )
        if new_boom_tile is None:
            print(f"  Seed: could not resolve BoomTile in generated_src")
            status = "boom_tile_resolve_failed"
            return None

        # ---- synthesize BoomTile (async) + run verilator in parallel ----
        # obj_dir is a path on the synth worker's local /scratch (the cluster
        # has no shared filesystem); the worker creates/cleans it, tars it, and
        # returns the bytes. The head node never reads this path directly.
        obj_dir = os.path.join(SYN_OBJ_SCRATCH_DIR, out_branch, "syn_obj")
        print(f"\n{'='*60}\nSeed: synthesize BoomTile (vlsi_top={new_boom_tile})\n{'='*60}")
        t_syn = time.time()
        syn_ref = run_boom_tile_synthesis.chia_remote(
            gen_src, new_boom_tile, obj_dir, IMPROVE_TIMING_SYNTHESIS_TIMEOUT_SECONDS, cacti_libs,
        )

        print(f"\n{'='*60}\nSeed: dispatch verilator tests\n{'='*60}")
        verilator_outcome = dispatch_verilator_tests(
            artifacts_by_threads=artifacts_by_threads, test_binaries=test_binaries,
        )
        print(f"  Verilator: {len(verilator_outcome.results)} total, "
              f"{len(verilator_outcome.failed)} failed")
        if len(verilator_outcome.failed) > 0:
            print(f"  Seed: verilator failures on clean baseline — aborting seed")
            try:
                chia_cancel(syn_ref, force=False)
                ray.wait([syn_ref])
            except Exception as e:
                print(f"  chia_cancel/wait failed: {e}")
            # syn_ref was cancelled; no archive bytes to extract.
            status = "verilator_failed"
            return None

        # ---- collect synthesis result ----
        try:
            syn_result, archive_bytes = get(syn_ref)
        except Exception as e:
            print(f"  Seed synthesis task raised: {e}")
            # No archive bytes returned; syn_obj is not recoverable here.
            status = "synthesis_failed"
            return None
        syn_elapsed = time.time() - t_syn
        area = parse_area_from_reports(syn_result.reports)
        print(f"  Seed synthesis done in {syn_elapsed:.1f}s: "
              f"success={syn_result.success}, area={area}")

        # ---- persist results ----
        _save_synthesis_to_db(db, out_branch, out_dir, new_boom_tile, syn_result, area, syn_elapsed, archive_bytes)
        db.save_perf_results(out_branch, _collect_perf(verilator_outcome))
        new_rpt_text = ""
        for relpath in TIMING_REPORT_RELPATHS:
            new_rpt_text = syn_result.reports.get(relpath, "")
            if new_rpt_text:
                break
        db.save_timing_report(out_branch, new_rpt_text)
        worst = _worst_slack_line(new_rpt_text) if new_rpt_text else "(no report)"
        log_path = out_dir / "improve_timing_log.md"
        log_path.write_text(
            f"# seed baseline: {out_branch}\n"
            f"Build config: {build_config}\n"
            f"BoomTile: {new_boom_tile}\n"
            f"Verilator results: {len(verilator_outcome.results)} "
            f"({len(verilator_outcome.failed)} failed)\n"
            f"Synthesis success: {syn_result.success}, area: {area}\n"
            f"Worst slack: {worst}\n"
        )
        db.register_file(out_branch, "improve_timing_log", log_path)
        db.set_metrics(
            out_branch,
            status="ok",
            area=area,
            synthesis_success=int(bool(syn_result.success)),
            verilator_passed=len(verilator_outcome.results) - len(verilator_outcome.failed),
            verilator_failed=len(verilator_outcome.failed),
            boom_tile_module=new_boom_tile,
        )
        status = "ok"
        print(f"\n{'='*60}\nSeed done: {out_branch}\n{'='*60}")
        return out_branch
    finally:
        try:
            db.register_dir(out_branch, "log", instance_log_dir)
            if status != "ok":
                db.set_metrics(out_branch, status=status)
        except Exception as e:
            print(f"  cleanup: db status/log update failed: {e}")
        if chipyard_bash is not None:
            try:
                chipyard_bash.stop()
            except Exception as e:
                print(f"  cleanup: chipyard_bash.stop() failed: {e}")
        if pg is not None:
            try:
                remove_placement_group(pg)
            except Exception as e:
                print(f"  cleanup: remove_placement_group failed: {e}")


def _worst_slack_line(report_text: str) -> str:
    """Return the worst-slack line (MET or VIOLATED), or a placeholder."""
    _, _, line = parse_worst_slack(report_text)
    return line if line else "(no slack line found)"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _dry_run(db: TimingDB, branch_name: str) -> int:
    inputs = db.load_inputs(branch_name)
    print(f"branch={branch_name}")
    print(f"  BoomTile: {inputs['boom_tile']}")
    print(f"  generated_src files: {len(inputs['generated_src'])}")
    print(f"  timing report path: {inputs['timing_report_path']}")
    print(f"  timing report head:")
    for line in inputs["timing_report"].splitlines()[:20]:
        print(f"    {line}")
    return 0


def synth_only_flow(
    db: TimingDB,
    branch_name: str,
    output_branch: str | None = None,
    build_config: str = BUILD_CONFIG,
    iteration: int = 1,
) -> dict:
    """Re-synthesize an existing branch's stored generated_src — CACTI + Genus
    only. No rebuild, no Verilator, no LLM.

    The branch's generated_src already contains the .top.mems.conf and is
    already MacroCompiler-remapped (cacti_* stubs present), so we only re-run
    CACTI characterization (dispatched from the driver, so it is head-owned and
    tunnel-safe) to regenerate the SRAM Liberty/LEF, then synthesize BoomTile
    on the stored RTL with 1 VLSI. This picks up the corrected CACTI area model.

    Result is written to a NEW branch (default ``<branch>_synth_only``) so the
    original results are preserved for comparison.
    """
    out_branch = output_branch or f"{branch_name}_synth_only"
    if db.branch_exists(out_branch):
        print(f"ERROR: output branch '{out_branch}' already exists — pass "
              f"--output-branch or remove it first")
        return {"status": "exists", "out_branch": out_branch}

    print(f"\n{'='*60}\nSynth-only resynth: {branch_name} -> {out_branch}\n{'='*60}")
    inputs = db.load_inputs(branch_name)
    generated_src = inputs["generated_src"]
    boom_tile = inputs.get("boom_tile")
    if not boom_tile:
        mods = _parse_verilog_modules(generated_src)
        boom_tile = _resolve_boom_tile_module(mods, set(mods))
    if boom_tile is None:
        print("ERROR: could not resolve BoomTile module")
        return {"status": "no_boom_tile"}
    print(f"  generated_src files: {len(generated_src)}; BoomTile: {boom_tile}")

    db.create_branch(out_branch, parent=branch_name, iteration=iteration,
                     build_config=build_config)
    out_dir = db.branch_files_dir(out_branch)

    final_result: dict | None = None
    try:
        print("  CACTI + MacroCompiler remap (corrected area model) ...")
        gen_src, cacti_libs, new_boom_tile = run_cacti_macrocompiler_prep(generated_src, {})
        if new_boom_tile is None:
            print("  ERROR: could not resolve BoomTile in remapped generated_src")
            return {"status": "boom_tile_resolve_failed", "out_branch": out_branch}
        print(f"  CACTI libs: {len(cacti_libs) if cacti_libs else 0}; "
              f"remapped BoomTile: {new_boom_tile}")

        # Save the post-MacroCompiler LEF/Liberty (what Genus consumes) to the
        # head before synthesis runs.
        _save_cacti_libs(db, out_branch, out_dir, cacti_libs)

        # BoomTile synthesis — 1 VLSI, no Verilator, no nested resource calls.
        # Synthesize the REMAPPED source with the remapped BoomTile top.
        obj_dir = os.path.join(SYN_OBJ_SCRATCH_DIR, out_branch, "syn_obj")
        print(f"  dispatching synthesis (vlsi_top={new_boom_tile}) ...")
        t0 = time.time()
        syn_result, archive_bytes = get(run_boom_tile_synthesis.chia_remote(
            gen_src, new_boom_tile, obj_dir,
            IMPROVE_TIMING_SYNTHESIS_TIMEOUT_SECONDS, cacti_libs))
        elapsed = time.time() - t0
        area = parse_area_from_reports(syn_result.reports)
        print(f"  synthesis {'OK' if syn_result.success else 'FAILED'}: "
              f"area={area}, elapsed={elapsed:.0f}s")

        # Persist — same DB calls as the main loop's Step 8, minus Verilator.
        _save_synthesis_to_db(db, out_branch, out_dir, new_boom_tile,
                              syn_result, area, elapsed, archive_bytes)
        new_rpt_text = ""
        for relpath in TIMING_REPORT_RELPATHS:
            new_rpt_text = syn_result.reports.get(relpath, "")
            if new_rpt_text:
                break
        db.save_timing_report(out_branch, new_rpt_text)
        old_worst = _worst_slack_line(inputs["timing_report"])
        new_worst = _worst_slack_line(new_rpt_text) if new_rpt_text else "(no report)"
        (out_dir / "improve_timing_log.md").write_text(
            f"# resynth (synth-only): {out_branch}\n"
            f"Parent: {branch_name}\nBoomTile: {new_boom_tile}\n"
            f"Synthesis success: {syn_result.success}, area: {area}\n\n"
            f"## Worst-slack\nParent: {old_worst}\nChild:  {new_worst}\n")
        db.set_metrics(out_branch, status="ok", area=area,
                       synthesis_success=int(bool(syn_result.success)),
                       boom_tile_module=new_boom_tile)
        final_result = {
            "status": "ok" if syn_result.success else "synth_failed",
            "out_branch": out_branch, "area": area,
            "worst_slack_parent": old_worst, "worst_slack_child": new_worst,
            "elapsed": elapsed,
        }
        print(f"\nDone: {out_branch} (area={area}, child {new_worst})")
        return final_result
    finally:
        if final_result is None:
            try:
                db.set_metrics(out_branch, status="error")
            except Exception as e:
                print(f"  cleanup: db status update failed: {e}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--branch", default=None,
                    help="DB branch (parent) to optimize. If omitted and the DB "
                         "is empty, the baseline is seeded first.")
    ap.add_argument("--iteration", type=int, default=1)
    ap.add_argument("--max-debug-retries", type=int, default=3)
    ap.add_argument("--build-config", default=BUILD_CONFIG)
    ap.add_argument("--output-suffix", default="_timing",
                    help="Suffix base. Output branch is "
                         "<parent><suffix>_v<N>, where N auto-increments if "
                         "<parent> already matches that pattern.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Load inputs from the DB, print timing-report head, exit")
    ap.add_argument("--skip-llm", action="store_true",
                    help="Skip the /improve_timing LLM step. Apply the parent diff "
                         "and run build+verilator+synth on it unchanged. Used for "
                         "ablation re-synthesis.")
    ap.add_argument("--skip-verilator", action="store_true",
                    help="Skip verilator entirely: build MegaBoom once (just to "
                         "elaborate the RTL + collect Verilog), then CACTI + synth. "
                         "No simulation, no per-test TMA counters. Still does the "
                         "chisel build. Pair with --skip-llm + --diff-file to "
                         "synthesize a diff with no LLM and no verilator.")
    ap.add_argument("--output-branch", default=None,
                    help="Override the auto-computed output branch name. "
                         "Useful for ablation runs that don't follow the _v<N> "
                         "auto-increment scheme.")
    ap.add_argument("--seed-only", action="store_true",
                    help="Build+synthesize the unmodified baseline into the DB, "
                         "then exit without running an optimization.")
    ap.add_argument("--prompt-file", default=IMPROVE_TIMING_PROMPT_PATH,
                    help="Path to the /improve_timing prompt markdown. Defaults "
                         "to improve_timing.md (IPC-neutral edits only). Pass "
                         "improve_timing_ironlaw.md to allow IPC-trading moves "
                         "when they win the iron-law product, or "
                         "improve_timing_ironlaw_noab.md (pair with "
                         "--no-experiment-tool) for a no-A/B lineage.")
    ap.add_argument("--no-experiment-tool", action="store_true",
                    help="Do NOT give the LLM the timing_experiment (sub-block A/B "
                         "synthesis) MCP tool; it gets only chipyard_bash. Pair with "
                         "improve_timing_ironlaw_noab.md.")
    ap.add_argument("--model", default="claude-opus-4-8",
                    help="Claude model id for the /improve_timing LLM step "
                         "(e.g. claude-opus-4-8, claude-fable-5).")
    ap.add_argument("--synth-only", action="store_true",
                    help="Re-synthesize an existing --branch's stored "
                         "generated_src (CACTI + Genus only — no rebuild, no "
                         "Verilator, no LLM). Picks up the corrected CACTI area "
                         "model. Writes a new branch <branch>_synth_only (override "
                         "with --output-branch). Requires --branch.")
    ap.add_argument("--diff-file", default=None,
                    help="Path to a diff.json (the {'' : root, 'generators/boom': "
                         "..., ...} format collect_diff emits) to apply INSTEAD of "
                         "the parent --branch's diff. The parent only supplies the "
                         "baseline generated_src/timing_report. Pair with "
                         "--skip-llm to synthesize an external diff with no LLM "
                         "(e.g. an angela perf-feature diff).")
    ap.add_argument("--ray-address", default="auto",
                    help="Ray cluster address to connect to (default: auto).")
    args = ap.parse_args()

    # TimingDB is a chia SQLiteNode pinned to the current node, so it needs an
    # initialized Ray context — init Ray before constructing it (and before the
    # --dry-run path, which reads the DB).
    ray.init(address=args.ray_address, runtime_env=RUNTIME_ENV,
             ignore_reinit_error=True)

    db = TimingDB(DB_DIR)

    if args.dry_run:
        return _dry_run(db, args.branch)

    # Synth-only resynthesis: no LLM, no build, no Verilator — just re-run
    # CACTI + Genus on the branch's stored generated_src.     
    if args.synth_only:
        if not args.branch or not db.branch_exists(args.branch):
            print(f"ERROR: --synth-only requires an existing --branch "
                  f"(got '{args.branch}')")
            return 1
        result = synth_only_flow(
            db, args.branch, output_branch=args.output_branch,
            build_config=args.build_config, iteration=args.iteration)
        print(f"\nresult: {json.dumps(result, indent=2, default=str)}")
        return 0 if result.get("status") == "ok" else 1

    # Bootstrap: with no entries in the DB there is no parent — seed the baseline
    # from the unmodified RTL first, then optimize from it.
    parent = args.branch
    if not db.has_any_branch():
        print("DB is empty — seeding baseline from unmodified RTL via seed_flow()")
        if args.branch is not None:
            print(f"  (ignoring --branch '{args.branch}'; DB has no entries to use as parent)")
        seed = seed_flow(db, build_config=args.build_config)
        if seed is None:
            print("seed_flow failed — see DB status and logs")
            return 1
        parent = seed
    elif parent is None:
        print("ERROR: DB already seeded; pass --branch <parent> to choose what to optimize")
        return 1
    elif not db.branch_exists(parent):
        print(f"ERROR: --branch '{parent}' not found in DB")
        return 1

    if args.seed_only:
        print(f"--seed-only: baseline ready ({parent}); exiting without optimization")
        return 0

    diff_override = None
    if args.diff_file:
        with open(args.diff_file) as f:
            diff_override = json.load(f)
        print(f"Loaded diff override from {args.diff_file} "
              f"({len(diff_override)} repo parts)")

    result = run_improve_timing_loop(
        db=db,
        branch_name=parent,
        skip_llm=args.skip_llm,
        skip_verilator=args.skip_verilator,
        output_branch=args.output_branch,
        iteration=args.iteration,
        max_debug_retries=args.max_debug_retries,
        build_config=args.build_config,
        output_suffix=args.output_suffix,
        prompt_path=args.prompt_file,
        enable_experiment_tool=not args.no_experiment_tool,
        llm_model=args.model,
        diff_override=diff_override,
    )
    print(f"\nresult: {json.dumps(result, indent=2, default=str)}")
    return 0 if result.get("status") == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
