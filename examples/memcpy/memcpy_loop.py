"""MemCpy RoCC accelerator — agentic build/run/debug loop.

Flow
----

Each run first resets the chipyard checkout to its committed baseline. Then:

    ┌─────────────────────-─┐       ┌──────────────────────────┐
    │  test build (parallel)│       │  implement (parallel)    │
    │  copy memcpy.c into   │       │  Claude writes           │
    │  chipyard/tests, cmake│       │  memcpy.scala + wires it │
    │  -> memcpy.riscv      │       │  into the target config  │
    │     memcpy.dump       │       │  via chipyard_bash       │
    └──────────┬─────────-──┘       └────────────┬─────────────┘
               └──────────────┬──────────────────┘
                              ▼
                  ┌───────────────────────┐
                  │ chisel build          │  target: MegaBoomV3HumanCommitLogConfig
                  │ (ChiselBuildNode)     │
                  └───────────┬───────────┘
                              ▼
                  ┌───────────────────────┐
                  │ verilator run         │  on memcpy.riscv (+loadmem, +verbose)
                  │ (VerilatorRunNode)    │
                  └───────────┬───────────┘
                              ▼
                  build failed / sim failed / incorrect?
                     │ yes (≤ NUM_DEBUG_ATTEMPTS)        │ no
                     ▼                                   ▼
              ┌──────────────┐                         DONE (passed)
              │ debug (Claude)│  build error, or sim log + commit-log
              │ fixes Chisel  │  tail + memcpy.dump tail
              └──────┬───────┘
                     └── rebuild + rerun (back to chisel build)

All intermediate node results and file collateral are written to ``out/``, each
filename prefixed with the timestamp at write time.

Run the loop using the full path of the CHIA driver::

    chia job submit -- python "$(pwd)/examples/memcpy/memcpy_loop.py"
"""

from __future__ import annotations

import logging
from datetime import datetime
from uuid import uuid4
import ray

from chia.base.ChiaFunction import get
from chia.base.tools.BashTool import BashTool
from chia.chipyard.chisel_build_node import ChiselBuildNode
from chia.chipyard.state_def import BuildTarget
from chia.chipyard.verilator_run_node import VerilatorRunNode

from constants import (
    BUILD_CONFIG,
    BUILD_CONFIG_PACKAGE,
    CHIPYARD_BASH_RESOURCE,
    CHIPYARD_DIFF_SUBMODULES,
    CHIPYARD_PATH,
    CHIPYARD_TESTS_DIR,
    CHISEL_BUILD_MAKE_JOBS,
    CHISEL_BUILD_RESOURCE,
    CHISEL_BUILD_TIMEOUT_SECONDS,
    MEMCPY_C_PATH,
    NUM_DEBUG_ATTEMPTS,
    OUT_DIR,
    RUNTIME_ENV,
    TEST_BUILD_RESOURCE,
    TEST_BUILD_TIMEOUT_SECONDS,
    TEST_NAME,
    VERILATOR_RUN_RESOURCE,
    VERILATOR_TIMEOUT_CYCLES,
    VERILATOR_TIMEOUT_SECONDS,
    VERILATOR_WORK_DIR,
)
from claude import (
    debug,
    format_build_failure,
    format_sim_failure,
    implement,
)
from helpers import (
    Dumper,
    classify_run,
    collect_diff,
    dump_llm,
    load_dramsim_ini,
    reset_chipyard,
)
from test_build import build_test

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("memcpy_loop")

# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------

def chisel_build(dump: Dumper, attempt: int):
    """Build the target config; dump stdout/stderr; return the BuildArtifact."""
    node = ChiselBuildNode(
        chipyard_path=CHIPYARD_PATH,
        config=BUILD_CONFIG,
        config_package=BUILD_CONFIG_PACKAGE,
        target=BuildTarget.VERILATOR,
        make_jobs=CHISEL_BUILD_MAKE_JOBS,
        timeout_seconds=CHISEL_BUILD_TIMEOUT_SECONDS,
    )
    logger.info("Building %s (attempt %d)", BUILD_CONFIG, attempt)
    artifact = get(
        node.build.options(resources={"chipyard": CHISEL_BUILD_RESOURCE}).chia_remote(node)
    )
    dump.text(f"chisel_build_attempt{attempt}.stdout.txt", artifact.stdout)
    dump.text(f"chisel_build_attempt{attempt}.stderr.txt", artifact.stderr)
    logger.info("Build %s (rc=%s)", "OK" if artifact.success else "FAILED", artifact.returncode)
    return artifact


def collect_chisel_diff(dump: Dumper, attempt: int) -> None:
    """Capture the chipyard git diff for this iteration and dump it.

    Records the cumulative Chisel state that produced this attempt's build —
    after the implement node (attempt 0) and after each debug edit (later
    attempts). Writes the full per-repo diff dict as JSON plus the root
    chipyard diff as a readable .diff. Never raises: a diff-capture hiccup must
    not fail the build/run loop.

    collect_diff captures both tracked modifications and new untracked files
    (read-only) for the root + submodules, so the freshly written accelerator
    (MemCopyRoCC.scala, tests/memcpy.c) shows up without any staging.
    """
    try:
        err, diffs = get(
            collect_diff.options(resources={"chipyard": 0.01}).chia_remote(
                CHIPYARD_PATH, CHIPYARD_DIFF_SUBMODULES
            )
        )
    except Exception as e:  # noqa: BLE001 - diagnostic only
        logger.warning("collect_diff failed (attempt %d): %s", attempt, e)
        return
    if err:
        logger.warning("collect_diff returned error=%d (attempt %d)", err, attempt)
        return
    dump.json(f"chisel_diff_attempt{attempt}.json", diffs)
    # The accelerator + config + test edits live in the root chipyard repo
    # (key ""); surface it as a directly-readable .diff. Append any non-empty
    # submodule diffs below it.
    parts = []
    for repo, text in diffs.items():
        if not text:
            continue
        label = repo or "(chipyard root)"
        parts.append(f"# ===== diff: {label} =====\n{text}")
    dump.text(f"chisel_diff_attempt{attempt}.diff", "\n\n".join(parts))
    logger.info("Collected chisel diff (attempt %d): %d repo(s) changed",
                attempt, sum(1 for t in diffs.values() if t))


def verilator_run(dump: Dumper, attempt: int, artifact, riscv_name, riscv_content, dramsim_ini):
    """Run memcpy.riscv on the built simulator; dump log/out; return RunResult."""
    node = VerilatorRunNode()
    logger.info("Running verilator on %s (attempt %d)", riscv_name, attempt)
    run = get(
        node.run.options(resources={"verilator_run": VERILATOR_RUN_RESOURCE}).chia_remote(
            node,
            artifact,
            riscv_content,
            riscv_name,
            VERILATOR_WORK_DIR,
            plusargs={"+loadmem": riscv_name},
            timeout_cycles=VERILATOR_TIMEOUT_CYCLES,
            timeout_seconds=VERILATOR_TIMEOUT_SECONDS,
            dramsim_ini_files=dramsim_ini,
        )
    )
    dump.text(f"verilator_run_attempt{attempt}.log", run.log)
    dump.text(f"verilator_run_attempt{attempt}.out", run.out)
    return run


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run_loop() -> dict:
    ray.init(address="auto", runtime_env=RUNTIME_ENV)
    dump = Dumper(OUT_DIR)
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary: dict = {"timestamp": run_ts, "config": BUILD_CONFIG, "attempts": []}

    # ----- deploy the chipyard bash MCP tool -----------------------------
    logger.info("Deploying chipyard_bash into the chipyard container")
    chipyard_bash = BashTool(
        name="chipyard_bash",
        work_dir=CHIPYARD_PATH,
        timeout_seconds=300,
        task_options={"resources": {"chipyard": CHIPYARD_BASH_RESOURCE}},
    )

    dramsim_ini = load_dramsim_ini()

    # ----- reset chipyard to baseline ------------------------------------
    logger.info("Resetting chipyard checkout to baseline")
    reset_out = get(
        reset_chipyard.options(resources={"chipyard": 0.01}).chia_remote(CHIPYARD_PATH)
    )
    logger.info("Chipyard reset: %s", reset_out or "clean")

    # One shared LLM session across implement + every debug call; the transcript
    # bytes are threaded from each call into the next so the debugger resumes
    # the implement conversation.
    session_id = str(uuid4())
    transcript: bytes = b""

    # ----- parallel phase: test build || implement ----------------------
    logger.info("Dispatching test build (parallel with implement)")
    source_content = MEMCPY_C_PATH.read_bytes()
    test_ref = build_test.options(
        resources={"chipyard": TEST_BUILD_RESOURCE}
    ).chia_remote(source_content, CHIPYARD_TESTS_DIR, TEST_NAME, TEST_BUILD_TIMEOUT_SECONDS)

    logger.info("Running implement node (Claude writes memcpy.scala)")
    impl = implement(chipyard_bash, session_id)
    transcript = impl.session_transcript or transcript
    dump_llm(dump, "implement", impl)
    logger.info("Implement finished (success=%s)", impl.success)

    logger.info("Waiting for test build")
    test = get(test_ref)
    dump.text("test_build.stdout.txt", test.stdout)
    dump.text("test_build.stderr.txt", test.stderr)
    if test.riscv_content:
        dump.bytes(f"test_build_{test.riscv_name}", test.riscv_content)
    if test.dump:
        dump.text(f"test_build_{test.dump_name}", test.dump)
    summary["test_build_success"] = test.success
    logger.info("Test build %s (rc=%s)", "OK" if test.success else "FAILED", test.returncode)

    if not test.success:
        logger.error("Test build failed — cannot run the design. Aborting.")
        summary["status"] = "test_build_failed"
        dump.json("summary.json", summary)
        return summary

    # ----- build → run → debug loop -------------------------------------
    status = "exhausted"
    for attempt in range(NUM_DEBUG_ATTEMPTS + 1):
        rec: dict = {"attempt": attempt}

        # Snapshot the Chisel diff for this iteration (implement edits on
        # attempt 0; cumulative implement + debug edits on later attempts).
        collect_chisel_diff(dump, attempt)

        artifact = chisel_build(dump, attempt)
        rec["build_success"] = artifact.success

        if not artifact.success:
            rec["kind"] = "build_failure"
            summary["attempts"].append(rec)
            if attempt >= NUM_DEBUG_ATTEMPTS:
                logger.error("Build still failing after %d debug attempts", NUM_DEBUG_ATTEMPTS)
                status = "build_failed"
                break
            feedback = format_build_failure(artifact, attempt + 1)
            dump.text(f"feedback_attempt{attempt + 1}.md", feedback)
            logger.info("Debugging build failure (attempt %d)", attempt + 1)
            dbg = debug(chipyard_bash, session_id, transcript, feedback)
            transcript = dbg.session_transcript or transcript
            dump_llm(dump, f"debug_attempt{attempt + 1}", dbg)
            if not dbg.success:
                logger.error("Debug node failed — giving up")
                status = "debug_failed"
                break
            continue

        run = verilator_run(dump, attempt, artifact, test.riscv_name,
                             test.riscv_content, dramsim_ini)
        outcome = classify_run(run)
        rec["kind"] = outcome.kind
        rec["detail"] = outcome.detail
        logger.info("Run outcome: %s — %s", outcome.kind, outcome.detail)

        if outcome.passed:
            summary["attempts"].append(rec)
            status = "passed"
            break

        summary["attempts"].append(rec)
        if attempt >= NUM_DEBUG_ATTEMPTS:
            logger.error("Still failing after %d debug attempts", NUM_DEBUG_ATTEMPTS)
            break
        feedback = format_sim_failure(run, outcome, test.dump, attempt + 1)
        dump.text(f"feedback_attempt{attempt + 1}.md", feedback)
        logger.info("Debugging sim failure (attempt %d)", attempt + 1)
        dbg = debug(chipyard_bash, session_id, transcript, feedback)
        transcript = dbg.session_transcript or transcript
        dump_llm(dump, f"debug_attempt{attempt + 1}", dbg)
        if not dbg.success:
            logger.error("Debug node failed — giving up")
            status = "debug_failed"
            break

    summary["status"] = status
    dump.json("summary.json", summary)
    logger.info("Loop finished: status=%s", status)
    return summary


if __name__ == "__main__":
    run_loop()
