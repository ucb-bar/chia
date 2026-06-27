"""Helper functions
"""

import re
import sys
from pathlib import Path
from typing import Optional

from chia.base.ChiaFunction import ChiaFunction
from chia.chipyard.state_def import BuildArtifact, RunResult
from chia.models.claude import ClaudeCodeLLM
from common.state_def import OptContext, TestBinary, TMACounters

_MAX_OUTPUT_CHARS = 100_000


def load_test_binaries(
    ubench_dir: Path,
    ubench_names: list[str] | None = None,
) -> list[TestBinary]:
    """Load prebuilt microbenchmark verilator binaries from *ubench_dir*.

    If *ubench_names* is provided, loads only those ``<name>.riscv`` files.
    Otherwise, collects every ``.riscv`` file in *ubench_dir*.
    """
    test_binaries: list[TestBinary] = []
    if not ubench_dir.is_dir():
        print(f"Warning: benchmark directory not found: {ubench_dir}", file=sys.stderr)
        return test_binaries

    if ubench_names is not None:
        for name in ubench_names:
            filepath = ubench_dir / f"{name}.riscv"
            if not filepath.exists():
                print(f"Warning: benchmark binary not found: {filepath}", file=sys.stderr)
                continue
            with open(filepath, "rb") as f:
                test_binaries.append(TestBinary(name=name, content=f.read()))
    else:
        for filepath in sorted(ubench_dir.glob("*.riscv")):
            with open(filepath, "rb") as f:
                test_binaries.append(TestBinary(name=filepath.stem, content=f.read()))

    return test_binaries


def parse_tma_counters(run_result: RunResult) -> TMACounters:
    """Parse TMA counter lines from a verilator RunResult log.

    Uses a state-machine parser that detects the TMA block delimited by:
        ===== TMA PERFORMANCE COUNTERS ... =====
        ...
        ==============================================
    and extracts counter lines matching:
        <whitespace> <counter_name> = <integer_value>
    """
    counters: dict[str, float] = {}
    counter_re = re.compile(r"^\s+(\w+)\s+=\s+(\d+)\s*$")
    in_tma_block = False

    # TMA counters are printed to stderr by the DPI-C SimTMACounterDump
    # module.  VerilatorRunNode pipes stderr through spike-dasm into
    # run_result.out (spike-dasm passes non-instruction lines through
    # unchanged).
    for line in run_result.out.splitlines():
        if "TMA PERFORMANCE COUNTERS" in line:
            in_tma_block = True
            continue
        if in_tma_block:
            # Closing delimiter: a line of only = signs
            if line.strip() and all(c == "=" for c in line.strip()):
                in_tma_block = False
                continue
            m = counter_re.match(line)
            if m:
                try:
                    counters[m.group(1)] = float(m.group(2))
                except ValueError:
                    pass

    passed = "*** PASSED ***" in run_result.out

    return TMACounters(
        test_name=run_result.test_binary_name,
        counters=counters,
        passed=passed,
    )


def _truncate(text: str, max_chars: int, keep: str = "tail") -> str:
    """Truncate text to max_chars, keeping head or tail."""
    if not text or len(text) <= max_chars:
        return text
    if keep == "tail":
        return f"... [{len(text) - max_chars} chars truncated] ...\n" + text[-max_chars:]
    return text[:max_chars] + f"\n... [{len(text) - max_chars} chars truncated] ..."


def _format_opt_context_section(opt_context: OptContext | None) -> str:
    """Render the optimization-context block for the debugger prompt.

    Returns an empty string when opt_context is None, so callers can
    unconditionally concatenate the result.
    """
    if opt_context is None:
        return ""

    # Cap the implementation summary so we don't blow out the context window.
    impl_summary = _truncate(opt_context.implement_summary, _MAX_OUTPUT_CHARS,
                             keep="tail")
    title = opt_context.recommendation_title or "(unknown)"
    goal = opt_context.design_goal or "(not extracted from implement summary)"

    return (
        f"## Optimization you are debugging\n"
        f"Title: {title}\n\n"
        f"Design goal (files changed and key decisions):\n{goal}\n\n"
        f"Full implementation notes:\n{impl_summary}\n\n"
        f"The bug is inside this optimization's changes or in an interaction "
        f"between them and pre-existing code.\n"
        f"Read the design-goal section FIRST. Do not edit anything until you "
        f"understand what changed.\n\n"
    )


def format_build_error(
    artifact: BuildArtifact,
    iteration: int,
    attempt: int,
    boom_repo_path: str,
    opt_context: OptContext | None = None,
) -> str:
    """Format build failure context for the debugger LLM."""
    stderr = _truncate(artifact.stderr, _MAX_OUTPUT_CHARS, keep="tail")
    # Find first case-insensitive occurrence of "error" and start 5 lines before it
    stdout_lines = artifact.stdout.splitlines()
    first_error_idx = None
    for i, line in enumerate(stdout_lines):
        if "error" in line.lower():
            first_error_idx = i
            break
    if first_error_idx is not None:
        start_idx = max(0, first_error_idx - 5)
        stdout = "\n".join(stdout_lines[start_idx:])
    else:
        stdout = _truncate(artifact.stdout, _MAX_OUTPUT_CHARS, keep="tail")
    return (
        f"# Build Failure — Iteration {iteration}, Debug Attempt {attempt}\n\n"
        f"The MegaBoom Chisel build failed with return code {artifact.returncode}.\n\n"
        f"## Build stderr\n```\n{stderr}\n```\n\n"
        f"## Build stdout\n```\n{stdout}\n```\n\n"
        f"{_format_opt_context_section(opt_context)}"
        f"## BOOM source location\n"
        f"The BOOM v3 Scala sources are at: {boom_repo_path}\n"
        f"Use the chipyard_bash tool to read files, diagnose the error, and write fixes.\n"
        f"ONLY fix the compilation errors. Do not refactor or optimize.\n"
    )


def format_test_error(
    failed_results: list[RunResult],
    iteration: int,
    attempt: int,
    boom_repo_path: str,
    opt_context: OptContext | None = None,
) -> str:
    """Format test failure context for the debugger LLM."""
    parts = [
        f"# Test Failure — Iteration {iteration}, Debug Attempt {attempt}\n\n",
        f"The MegaBoom build succeeded but {len(failed_results)} verilator test(s) failed.\n\n",
    ]
    for vr in failed_results:
        log = _truncate(vr.log, _MAX_OUTPUT_CHARS // 10, keep="tail")
        out = _truncate(vr.out, _MAX_OUTPUT_CHARS // 10, keep="tail")
        parts.append(
            f"## {vr.test_binary_name} (rc={vr.returncode})\n```\nLog: \n{log}\n```\nOut: \n{out}\n```\n"
        )

    parts.append(_format_opt_context_section(opt_context))
    parts.append(
        f"## BOOM source location\n"
        f"The BOOM v3 Scala sources are at: {boom_repo_path}\n"
        f"Use the chipyard_bash tool to read files, diagnose the failures, and write fixes.\n"
    )
    return "".join(parts)


@ChiaFunction(resources={"claude_creds": 0.01})
def _creds_prompt_task(
    config: dict, user_message: str, tools,
    setup=None, setup_args: tuple = (),
    cleanup=None, cleanup_args: tuple = (),
    session_id: Optional[str] = None,
    session_transcript: Optional[bytes] = None,
):
    """Run *setup* then a :class:`ClaudeCodeLLM` prompt on a claude_creds worker.

    Creates a :class:`ClaudeCodeLLM` from *config* on the worker and calls
    :meth:`ClaudeCodeLLM.prompt` in-process (a local call does NOT re-dispatch,
    so the prompt runs right here, on the same worker *setup* prepared).

    When ``session_id`` is provided the session is pinned on the instance, and
    when ``session_transcript`` bytes are also provided they are carried in so
    the CLI ``--resume``s the prior conversation (the bytes are pasted to disk
    by :meth:`ClaudeCodeLLM._restore_transcript` inside ``prompt``). The
    post-run transcript is read back off the result and returned so the caller
    can thread it into the next call (which may land on a different worker).

    Returns ``(cli, out_transcript)`` by default, or *cleanup*'s return
    (called as ``cleanup(cli, out_transcript, *cleanup_args)``) when supplied.
    """
    if setup is not None:
        setup(*setup_args)

    llm = ClaudeCodeLLM(**config)
    if session_id is not None:
        llm._session_id = session_id
        if session_transcript:
            # Carry the prior conversation; prompt()'s _restore_transcript
            # pastes it to disk and forces --resume semantics.
            llm._session_transcript = session_transcript
            llm._call_counter = 1
    cli = llm.prompt(user_message, tools)

    out_transcript = getattr(cli, "session_transcript", None) or b""
    if cleanup is not None:
        return cleanup(cli, out_transcript, *cleanup_args)
    return cli, out_transcript
