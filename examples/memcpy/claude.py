"""LLM nodes for the MemCpy example: implement + debug.

Both nodes drive a :class:`chia.models.claude.ClaudeCodeLLM` over the
``chipyard_bash`` MCP tool (a BashTool deployed into the chipyard container).
A single LLM instance is shared across the whole loop so the debug calls
``--resume`` the implement session and remember what they already tried.

  * :func:`implement` — first call. Writes ``memcpy.scala`` (the RoCC
    accelerator) and wires it into the target config. Runs in parallel with
    the test build.
  * :func:`debug` — feedback call. Given a formatted failure context (build
    error, runtime/timeout, or incorrect result), edits the Chisel to fix it.

The feedback builders mirror the information the other chia examples hand their
debugger (build stderr/stdout windowed on the first error; sim log/out tails;
the BOOM/chipyard source location) and, for verilator failures, additionally
include the last N lines of ``memcpy.dump`` and of the commit log — per this
example's spec.
"""

from __future__ import annotations

from pathlib import Path

from chia.base.ChiaFunction import get
from chia.base.tools.BashTool import BashTool
from chia.models.claude import ClaudeCodeLLM, ClaudeCodeQueryResult

from constants import (
    BUILD_CONFIG,
    CHIPYARD_SRC_PATH,
    COMMIT_LOG_TAIL_LINES,
    DATA_SIZE,
    DUMP_TAIL_LINES,
    LLM_EXTRA_CLI_ARGS,
    LLM_MODEL,
    LLM_RESOURCE,
    LLM_SYSTEM_MESSAGE,
    LLM_TIMEOUT_SECONDS,
    MAX_OUTPUT_CHARS,
)
from helpers import Outcome
from chia.chipyard.state_def import BuildArtifact, RunResult


# ---------------------------------------------------------------------------
# Prompts (loaded from prompts/, with ${VAR} placeholders substituted)
# ---------------------------------------------------------------------------

PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"


def _load_prompt(name: str, **subs: str) -> str:
    """Read prompts/<name> and replace ${KEY} placeholders with subs values.

    ``${KEY}`` (rather than str.format's ``{KEY}``) so prompt text can contain
    literal braces (Chisel/Scala snippets) without escaping.
    """
    text = (PROMPTS_DIR / name).read_text()
    for key, val in subs.items():
        text = text.replace("${" + key + "}", val)
    return text


# implement.md: the accelerator spec + custom-instruction ABI + the "only write
# Chisel" task wrapper. Kept in lockstep with memcpy.c, which issues
# ROCC_INSTRUCTION_DSS(1, rd, src, dst, 0) then ROCC_INSTRUCTION_DS(1, rd, len, 1)
# — opcode custom1, funct 0 = latch src/dst addrs, funct 1 = length + start.
_IMPLEMENT_TASK = _load_prompt(
    "implement.md",
    BUILD_CONFIG=BUILD_CONFIG,
    DATA_SIZE=str(DATA_SIZE),
    CHIPYARD_SRC_PATH=CHIPYARD_SRC_PATH,
)

# debug.md: the debugger charter — find the real root cause, don't revert /
# disable the feature, don't run tests yourself.
_DEBUGGER_PREAMBLE = _load_prompt(
    "debug.md",
    BUILD_CONFIG=BUILD_CONFIG,
    CHIPYARD_SRC_PATH=CHIPYARD_SRC_PATH,
)


# ---------------------------------------------------------------------------
# Feedback formatting
# ---------------------------------------------------------------------------

def _truncate(text: str, max_chars: int, keep: str = "tail") -> str:
    if not text or len(text) <= max_chars:
        return text or ""
    if keep == "tail":
        return f"... [{len(text) - max_chars} chars truncated] ...\n" + text[-max_chars:]
    return text[:max_chars] + f"\n... [{len(text) - max_chars} chars truncated] ..."


def _tail_lines(text: str, n: int) -> str:
    lines = (text or "").splitlines()
    return "\n".join(lines[-n:])


def format_build_failure(artifact: BuildArtifact, attempt: int) -> str:
    """Build-error context — stderr tail + stdout windowed on the first error.

    Same shape as the other examples' build-failure feedback.
    """
    stderr = _truncate(artifact.stderr, MAX_OUTPUT_CHARS, keep="tail")
    stdout_lines = artifact.stdout.splitlines()
    first_error = next((i for i, ln in enumerate(stdout_lines) if "error" in ln.lower()), None)
    if first_error is not None:
        stdout = "\n".join(stdout_lines[max(0, first_error - 5):])
    else:
        stdout = _truncate(artifact.stdout, MAX_OUTPUT_CHARS, keep="tail")
    return (
        f"# Build Failure — Debug Attempt {attempt}\n\n"
        f"The `{BUILD_CONFIG}` Chisel build failed (rc={artifact.returncode}).\n\n"
        f"## Build stderr\n```\n{stderr}\n```\n\n"
        f"## Build stdout (from first error)\n```\n{stdout}\n```\n\n"
        f"Fix ONLY the compilation errors. Do not refactor or optimize.\n"
    )


def format_sim_failure(
    run: RunResult,
    outcome: Outcome,
    dump: str,
    attempt: int,
) -> str:
    """Simulation-failure context (runtime / timeout / incorrect).

    Includes the sim log + spike-dasm'd out tails (the commit log, since the
    target is a HumanCommitLog config), and — per this example's spec — the
    last N lines of the test disassembly (memcpy.dump) and of the commit log.
    """
    log_tail = _truncate(run.log, MAX_OUTPUT_CHARS // 5, keep="tail")
    out_tail = _truncate(run.out, MAX_OUTPUT_CHARS // 5, keep="tail")
    commit_tail = _tail_lines(run.out or run.log, COMMIT_LOG_TAIL_LINES)
    dump_tail = _tail_lines(dump, DUMP_TAIL_LINES)

    if outcome.kind == "timeout":
        headline = (
            "The simulation TIMED OUT (the design likely hung — e.g. a RoCC or "
            "memory handshake that never completes, or a copy loop that never "
            "terminates)."
        )
    elif outcome.kind == "runtime":
        headline = f"The simulation hit a RUNTIME ERROR ({outcome.detail})."
    else:  # incorrect
        headline = (
            f"The simulation ran but produced an INCORRECT result "
            f"({outcome.detail}). The copied data does not match the source."
        )

    return (
        f"# Simulation Failure — Debug Attempt {attempt}\n\n"
        f"{headline}\n\n"
        f"## Simulator stdout (commit log + program output)\n```\n{log_tail}\n```\n\n"
        f"## spike-dasm output tail\n```\n{out_tail}\n```\n\n"
        f"## Commit log — last {COMMIT_LOG_TAIL_LINES} lines\n```\n{commit_tail}\n```\n\n"
        f"## Test disassembly (memcpy.dump) — last {DUMP_TAIL_LINES} lines\n"
        f"```\n{dump_tail}\n```\n\n"
        f"Use the disassembly to confirm which custom instructions the test "
        f"issues (funct 0 = load src/dst addresses, funct 1 = length + start) "
        f"and check your accelerator handles both and drives the RoCC response.\n"
    )


# ---------------------------------------------------------------------------
# LLM nodes  (dispatched onto the dedicated claude / "llm" worker)
# ---------------------------------------------------------------------------
#
# chia.models.claude.ClaudeCodeLLM.prompt is itself a @ChiaFunction, so the
# claude CLI runs on the worker that holds the "llm" resource, not on the head.
# implement (the first call) and every debug call share ONE session: we pin a
# fixed session_id and thread the session transcript bytes from each call into
# the next (the @_session_tracked wrapper syncs cli.session_transcript on get),
# so the debugger resumes the implement conversation and remembers prior fixes.


def implement(chipyard_bash: BashTool, session_id: str) -> ClaudeCodeQueryResult:
    """First call: write the accelerator + config wiring via chipyard_bash."""
    llm = ClaudeCodeLLM(
        model=LLM_MODEL,
        system_message=LLM_SYSTEM_MESSAGE,
        timeout_seconds=LLM_TIMEOUT_SECONDS,
        logging_name="memcpy_generator",
        resume_session=True,
        projects_cwd=None,  # derive from the llm worker's CWD
        extra_cli_args=list(LLM_EXTRA_CLI_ARGS),
    )
    llm._session_id = session_id
    return get(
        llm.prompt.options(resources={"llm": LLM_RESOURCE}).chia_remote(
            llm, _IMPLEMENT_TASK, [chipyard_bash]
        )
    )


def debug(
    chipyard_bash: BashTool, session_id: str, transcript: bytes, feedback: str
) -> ClaudeCodeQueryResult:
    """Feedback call: resume the session to diagnose and fix *feedback*."""
    llm = ClaudeCodeLLM(
        model=LLM_MODEL,
        system_message=LLM_SYSTEM_MESSAGE,
        timeout_seconds=LLM_TIMEOUT_SECONDS,
        logging_name="memcpy_generator",
        resume_session=True,
        projects_cwd=None,  # derive from the llm worker's CWD
        extra_cli_args=list(LLM_EXTRA_CLI_ARGS),
    )
    # Resume the shared session and paste the carried transcript onto the chosen
    # worker before the CLI runs.
    llm._session_id = session_id
    llm._call_counter = 1
    if transcript:
        llm._session_transcript = transcript
    prompt = f"{_DEBUGGER_PREAMBLE}\n\n{feedback}"
    return get(
        llm.prompt.options(resources={"llm": LLM_RESOURCE}).chia_remote(
            llm, prompt, [chipyard_bash]
        )
    )
