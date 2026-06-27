"""Worker-side per-issue pipeline for the CIRCT issue flow.

Shipped to chia-circt workers via runtime_env (py_modules). MUST NOT import
head-only modules (db / triage) or read files at import time — all config
arrives in the ``cfg`` dict. Imports the local circt_util.py shipped alongside
it, plus the BuildTool / LitTool MCP wrappers from chia.chipyard.circt.
"""
from __future__ import annotations

import glob
import json
import os
import re
from string import Template

from chia.base.ChiaFunction import ChiaFunction, get


def _assess_decision(text: str) -> dict:
    """Parse the assess turn's footer into {proceed: bool, status: str|None, note}.

    Reads the last ``DECISION: CLEAR|NOT_A_BUG|UNCLEAR`` line. CLEAR -> proceed;
    NOT_A_BUG / UNCLEAR -> skip with that status and the ``REASON:`` text as the
    note. Defaults to proceed if the footer is unparseable — better to attempt a
    possibly-good issue than silently drop it.
    """
    t = text or ""
    verdicts = re.findall(r"(?im)^\s*DECISION:\s*(CLEAR|UNCLEAR|NOT[_ ]?A[_ ]?BUG)\b", t)
    v = verdicts[-1].upper() if verdicts else "CLEAR"
    if v == "CLEAR":
        return {"proceed": True, "status": None, "note": None}
    status = "not_a_bug" if v.startswith("NOT") else "unclear"
    m = re.search(r"(?is)\bREASON:\s*(.+)$", t)
    note = (m.group(1).strip() if m else t.strip()[-1500:]) or f"marked {status} (no reason given)"
    return {"proceed": False, "status": status, "note": note}


@ChiaFunction(resources={"circt": 1})
def run_issue_remote(issue_md: str, number: int, cfg: dict,
                     resume: dict | None = None, assess_only: bool = False) -> dict:
    """Reproduce -> fix -> verify -> writeup for one issue, pinned to ONE
    chia-circt container.

    Each phase prompts an llm while the bash/build/lit MCP
    servers stay on this chia-circt worker, reached over HTTP. Phases are
    stateless — each is its own `claude --print`; the context a later phase needs
    is inlined into its prompt (the repro.sh into fix; the diff/verdict into
    writeup).

    ``resume`` (optional) REPLAYS a prior attempt instead of running repro+fix:
    ``{"diff": <saved fix.diff>, "repro_files": {relpath: content}}`` — the saved
    repro is restored and the diff re-applied, then we jump straight to verify
    (which still triggers the regression-repair turn). Used to exercise a later
    phase without redoing the expensive/non-deterministic earlier ones.

    Returns a result dict (status / verdict / diff / writeup / per-phase logs).
    The head persists it.
    """
    import ray
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
    from chia.models.claude import ClaudeCodeLLM
    from chia.base.tools.BashTool import BashTool

    import circt_util
    from chia.chipyard.circt import BuildTool, LitTool

    node = ray.get_runtime_context().get_node_id()
    here = {"scheduling_strategy": NodeAffinitySchedulingStrategy(node_id=node, soft=False)}
    logs: dict = {}

    def _turn(phase: str, prompt: str, tools: list):
        # chia.models.claude: `prompt` is a ChiaFunction we dispatch
        # onto an `llm` worker (1.0/call, so the cluster's `llm` slots cap
        # concurrency) — claude runs there while the bash/build/lit MCP servers
        # stay on this chia-circt worker, reached over HTTP. log_dir is None: the
        # CLI's on-worker log would land on the ephemeral llm container, so we
        # persist cli.stream_result centrally instead. resume_session +
        # projects_cwd=None give each phase a fresh session whose .jsonl
        # transcript we read back for logging (no actual --resume — a new LLM is
        # built per phase).
        llm = ClaudeCodeLLM(
            model=cfg["model"], system_message=cfg["system_prompt"],
            timeout_seconds=cfg["timeouts"][phase],
            extra_cli_args=["--effort", "max"],
            resume_session=True, projects_cwd=None,
        )
        cli = get(llm.prompt.options(resources={"llm": 1.0}).chia_remote(llm, prompt, tools))
        transcript = getattr(cli, "session_transcript", None) or b""
        logs[phase] = {
            "result": cli.result, "stream": cli.stream_result,
            "stderr": cli.stderr, "success": bool(getattr(cli, "success", False)),
            "transcript": transcript if isinstance(transcript, (bytes, bytearray)) else b"",
        }
        return cli

    def _render(key: str, **kw) -> str:
        # Template/safe_substitute (not str.format) so MLIR/shell braces in the
        # prompts and issue body don't blow up substitution.
        return Template(cfg[key]).safe_substitute(**kw)

    # 0) Trust the checkout (git safe.directory) + idempotent warm-up (lit + tool
    #    targets) + clean source back to the tag. assess_only reads source only,
    #    so it skips the (slow) warm build.
    circt_util.circt_trust_source()
    if not assess_only:
        circt_util.circt_warm_build(cfg["tool_targets"], num_cpus=cfg["build_jobs"])
    reset = circt_util.circt_git_reset(cfg["tag"])
    if not reset["success"]:
        return {"status": "error", "reproduced": False, "logs": logs,
                "notes": "git reset failed:\n" + reset["log"]}

    bash = build = lit = None
    try:
        # 300s cap: a synchronous tool call held much longer risks the same
        # MCP-transport response loss the async build/lit tools avoid. Builds and
        # test runs should go through those async tools, not bash.
        bash = BashTool(name=f"bash_{number}", work_dir=circt_util._CIRCT_SOURCE_TREE,
                        task_options=here, timeout_seconds=300)

        # assess_only: run JUST the assess turn (read-only bash), return its
        # decision, and stop — no repro/fix/verify, no build/lit tools spun up.
        # Used to spot-check the assess prompt without a full pipeline run.
        if assess_only:
            assess = _turn("assess", _render("assess_prompt", issue=issue_md), [bash])
            decision = _assess_decision(assess.result)
            return {"status": "clear" if decision["proceed"] else decision["status"],
                    "assess_only": True, "reproduced": False,
                    "notes": decision["note"], "logs": logs}

        build = BuildTool(name=f"build_{number}", num_cpus=cfg["build_jobs"], task_options=here)
        lit = LitTool(name=f"lit_{number}", task_options=here)
        agent_tools = [bash, build, lit]

        if resume:
            # REPLAY — restore the saved repro + re-apply the saved fix, skip the
            # repro/fix turns, and fall through to verify (which fires the
            # regression-repair turn if the replayed diff regresses the suite).
            circt_util.circt_write_files(resume.get("repro_files") or {}, cfg["repro_dir"])
            ap = circt_util.circt_apply_diff(resume.get("diff") or "")
            if not ap["success"]:
                return {"status": "error", "reproduced": False, "logs": logs,
                        "notes": "git apply (replay) failed:\n" + ap["log"]}
            try:
                repro_text = open(cfg["repro_path"]).read()
            except OSError:
                repro_text = "(repro.sh not found)"
        else:
            # 0.5) ASSESS — before spending a repro/fix attempt, decide (a) is
            #      this actually a bug, and (b) are both the bug and the correct
            #      behavior clear enough to act on autonomously (the agent may
            #      read the source/docs read-only). If it's not a bug or either is
            #      unclear, log the reason and skip — there is no human to ask.
            assess = _turn("assess", _render("assess_prompt", issue=issue_md), [bash])
            decision = _assess_decision(assess.result)
            if not decision["proceed"]:
                return {"status": decision["status"], "reproduced": False,
                        "logs": logs, "notes": decision["note"]}

            # 1) REPRODUCE — the LLM writes <repro_path> (exit 0 iff fixed).
            _turn("repro", _render("repro_prompt", issue=issue_md), agent_tools)
            clean = circt_util.circt_run_script(cfg["repro_path"])
            if cfg["require_repro"] and clean["exit_code"] == 0:
                return {"status": "no_repro", "reproduced": False, "logs": logs,
                        "repro_tail": clean["log_tail"]}

            # 2) FIX — fresh session; inline the repro.sh the previous turn wrote
            #    so the agent has full context without --resume.
            try:
                repro_text = open(cfg["repro_path"]).read()
            except OSError:
                repro_text = "(repro.sh not found)"
            _turn("fix", _render("fix_prompt", issue=issue_md, repro=repro_text), agent_tools)

        # 3) VERIFY — deterministic, no LLM. Regression gate runs the WHOLE lit
        #    suite (minus baseline-red dirs) — ~6s and always non-empty, so it
        #    catches regressions outside the touched dialect (e.g. a lib/Firtool
        #    or tools/ change) and never vacuously passes.
        tps = circt_util.circt_lit_gate_paths()

        def _verify():
            diff = circt_util.circt_capture_diff(cfg["tag"])
            rebuild = circt_util.circt_ninja_build(cfg["tool_targets"], num_cpus=cfg["build_jobs"])
            repro_after = circt_util.circt_run_script(cfg["repro_path"])
            repro_fixed = rebuild["success"] and repro_after["exit_code"] == 0
            lit_res = circt_util.circt_run_lit(tuple(tps), filter_out=circt_util._LIT_GATE_FILTER_OUT)
            return diff, rebuild, repro_after, repro_fixed, lit_res

        diff, rebuild, repro_after, repro_fixed, lit_res = _verify()

        # 3b) FIX REGRESSION — the repro is fixed but the change broke other
        #     tests. Give the agent ONE more turn (with the failing tests + their
        #     output inlined) to repair the regression without un-fixing the bug,
        #     then re-verify. Only triggered when there's actually a regression to
        #     chase (repro green, suite red).
        notes = None
        if repro_fixed and lit_res["failed"] > 0:
            fails = lit_res["failures"]
            fail_paths = circt_util.circt_lit_failure_paths(fails)
            focused = (circt_util.circt_run_lit(tuple(fail_paths)) if fail_paths
                       else {"log_tail": lit_res.get("log_tail", "")})
            _turn("regression", _render("regression_prompt", issue=issue_md,
                                        repro=repro_text, diff=diff["diff"],
                                        failures="\n".join(fails) or "(none parsed)",
                                        failure_log=focused.get("log_tail", "")),
                  agent_tools)
            diff, rebuild, repro_after, repro_fixed, lit_res = _verify()
            notes = "regression turn run; initial lit failures: " + "; ".join(fails)

        verdict = {
            "reproduced": True, "build_ok": rebuild["success"], "fixed": repro_fixed,
            "lit_ok": lit_res["success"], "lit_passed": lit_res["passed"],
            "lit_failed": lit_res["failed"], "lit_failures": lit_res["failures"],
            "test_paths": tps, "notes": notes,
        }

        # 4) WRITEUP — fresh session, no tools; diff + verdict inlined.
        wr = _turn("writeup", _render("writeup_prompt", issue=issue_md,
                                      diff=diff["diff"],
                                      verdict=json.dumps(verdict, indent=2)), [])

        # Capture the agent's repro artifacts (.circtissues/: repro.sh, input
        # MLIR, NOTES.md, ...) so they're saved centrally — the container's FS is
        # ephemeral. (lit tests the agent added under test/... ride in the diff.)
        repro_files: dict = {}
        for p in sorted(glob.glob(f"{cfg['repro_dir']}/**", recursive=True)):
            if os.path.isfile(p) and os.path.getsize(p) <= 256_000:
                try:
                    repro_files[os.path.relpath(p, cfg["repro_dir"])] = open(p, errors="replace").read()
                except OSError:
                    pass

        status = "fixed" if (repro_fixed and lit_res["success"]) else "attempted"
        return {
            "status": status, **verdict,
            "diff": diff["diff"], "added": diff["added"], "removed": diff["removed"],
            "rebuild_tail": rebuild["log_tail"], "repro_tail": repro_after["log_tail"],
            "lit_tail": lit_res.get("log_tail", ""),
            "writeup": wr.result, "repro_files": repro_files, "logs": logs,
        }
    finally:
        for t in (lit, build, bash):
            if t is not None:
                try:
                    t.stop()
                except Exception:
                    pass
