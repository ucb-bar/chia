"""Worker-side per-PR review round for the CIRCT flow.

Shipped to chia-circt workers via runtime_env (py_modules) alongside
circt_util.py; the BuildTool / LitTool MCP wrappers come from
chia.chipyard.circt. Reconstructs a PR's state on the pinned tree (re-apply its
fix.diff), then runs one LLM turn that addresses reviewer feedback by editing the
code, rebuilds + re-verifies (repro still passes, lit suite still green), and
returns the updated diff plus the author replies it would post. Nothing is
committed or posted — no GitHub writes.
"""
from __future__ import annotations

import re
from string import Template

from chia.base.ChiaFunction import ChiaFunction, get


def _review_decision(text: str) -> dict:
    """Parse the review-triage footer into {actionable: bool, which: str|None}.

    Reads the last ``DECISION: ACTIONABLE|NO_CHANGES`` line; on ACTIONABLE,
    pulls the ``ACTIONABLE:`` summary line as *which*. Defaults to actionable
    if the footer is unparseable — acting on possibly-empty feedback is safer
    than silently ignoring a real change request.
    """
    t = text or ""
    verdicts = re.findall(r"(?im)^\s*DECISION:\s*(ACTIONABLE|NO[_ ]?CHANGES)\b", t)
    v = verdicts[-1].upper() if verdicts else "ACTIONABLE"
    if v.startswith("NO"):
        return {"actionable": False, "which": None}
    m = re.findall(r"(?im)^\s*ACTIONABLE:\s*(.+)$", t)
    return {"actionable": True, "which": (m[-1].strip() if m else None)}


@ChiaFunction(resources={"circt": 1})
def run_review_round_remote(issue_md: str, number: int, cfg: dict,
                            base_diff: str, repro_files: dict,
                            review_text: str, pr_description: str = "") -> dict:
    """Address one PR's review feedback on a chia-circt container.

    *base_diff* is the PR's fix (from issue_logs) — re-applied to the
    firtool-1.148.0 tree to reconstruct the PR state. *review_text* is the
    rendered reviewer feedback. Returns the updated diff, the per-comment
    replies, and a verify verdict (build/repro/lit). The head persists it.
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
        # chia.models.claude: dispatch `prompt` onto an `llm` worker
        # (1.0/call) — claude runs there, the bash/build/lit MCP servers stay on
        # this chia-circt worker (HTTP).
        # log_dir=None (the on-worker log would be ephemeral; we persist
        # cli.stream_result). A fresh session per phase (resume_session +
        # projects_cwd=None) gives a .jsonl transcript we read back for logging.
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
        return Template(cfg[key]).safe_substitute(**kw)

    def _restore_pr_state() -> dict | None:
        """Reset the tree to the tag, re-apply the PR (diff + repro files), and
        incrementally REBUILD so build/bin matches the restored source. Without
        the rebuild, a turn that runs circt-opt/firtool before its first build
        would exercise stale binaries — e.g. the fixer testing against code the
        assesser's discarded experiments compiled in, or the assesser probing
        binaries that predate the PR diff. Returns an error dict or None."""
        reset = circt_util.circt_git_reset(cfg["tag"])
        if not reset["success"]:
            return {"status": "error", "logs": logs,
                    "notes": "git reset failed:\n" + reset["log"]}
        circt_util.circt_write_files(repro_files or {}, cfg["repro_dir"])
        ap = circt_util.circt_apply_diff(base_diff or "")
        if not ap["success"]:
            return {"status": "error", "logs": logs,
                    "notes": "git apply (PR base diff) failed:\n" + ap["log"]}
        rb = circt_util.circt_ninja_build(cfg["tool_targets"], num_cpus=cfg["build_jobs"])
        if not rb["success"]:
            return {"status": "error", "logs": logs,
                    "notes": "rebuild after PR-state restore failed:\n" + rb["log_tail"]}
        return None

    # 0) Reconstruct the PR state: trust + warm-up + reset to tag, restore the
    #    repro, re-apply the PR's fix.diff. The warm build runs up front because
    #    the triage turn can now build/test its experiments.
    circt_util.circt_trust_source()
    circt_util.circt_warm_build(cfg["tool_targets"], num_cpus=cfg["build_jobs"])
    if (err := _restore_pr_state()) is not None:
        return err

    bash = build = lit = None
    try:
        bash = BashTool(name=f"bash_rv_{number}", work_dir=circt_util._CIRCT_SOURCE_TREE,
                        task_options=here, timeout_seconds=300)
        build = BuildTool(name=f"build_rv_{number}", num_cpus=cfg["build_jobs"], task_options=here)
        lit = LitTool(name=f"lit_rv_{number}", task_options=here)
        agent_tools = [bash, build, lit]

        # 1) TRIAGE — is any of the feedback actually actionable (warrants a
        #    code change)? Gets the FULL toolset so it can run experiments
        #    (prototype edits, rebuild, lit) to ground its judgment — but its
        #    edits are throwaway: the tree is restored to the PR state before
        #    the change-making turn. If nothing is actionable, the turn itself
        #    writes the author replies and we stop — no separate fix turn.
        ta = _turn("review_assess",
                   _render("review_assess_prompt", issue=issue_md,
                           pr=pr_description or "(no PR description provided)",
                           diff=base_diff, review=review_text), agent_tools)
        decision = _review_decision(ta.result)
        if not decision["actionable"]:
            # Best-effort hygiene: drop the assesser's experimental edits rather
            # than leaving the container tree dirty between jobs. (Every run
            # restores at start anyway, so failure here is non-fatal — and no
            # rebuild: nothing further runs in this round.)
            try:
                circt_util.circt_git_reset(cfg["tag"])
            except Exception:
                pass
            return {"status": "no_changes", "build_ok": None, "repro_ok": None,
                    "lit_ok": None, "lit_passed": None, "lit_failed": None,
                    "lit_failures": [], "diff": base_diff, "added": None,
                    "removed": None, "replies": ta.result, "logs": logs,
                    "notes": "review triage: no actionable feedback — replies only"}

        # 1.5) RESET — discard the triage turn's experimental edits so the
        #      change-making turn starts from the exact PR state.
        if (err := _restore_pr_state()) is not None:
            return err

        # 2) REVIEW — address the feedback, then write the replies.
        rv = _turn("review", _render("review_prompt", issue=issue_md,
                                     pr=pr_description or "(no PR description provided)",
                                     diff=base_diff, review=review_text,
                                     actionable=decision["which"]
                                     or "(triage did not list specific items)"),
                   agent_tools)

        # 3) VERIFY — deterministic. Capture the updated diff, rebuild, confirm
        #    the repro still passes (when one was provided — the repro is a
        #    local-flow artifact; PRs sourced straight from GitHub may have
        #    none, in which case repro_ok is None and doesn't gate the status),
        #    and run the full lit gate.
        diff = circt_util.circt_capture_diff(cfg["tag"])
        rebuild = circt_util.circt_ninja_build(cfg["tool_targets"], num_cpus=cfg["build_jobs"])
        repro_after = circt_util.circt_run_script(cfg["repro_path"]) if repro_files else None
        repro_ok = (repro_after["exit_code"] == 0) if repro_after is not None else None
        tps = circt_util.circt_lit_gate_paths()
        lit_res = circt_util.circt_run_lit(tuple(tps), filter_out=circt_util._LIT_GATE_FILTER_OUT)

        ok = rebuild["success"] and lit_res["success"] and repro_ok is not False
        return {
            "status": "ok" if ok else "issues",
            "build_ok": rebuild["success"], "repro_ok": repro_ok,
            "lit_ok": lit_res["success"], "lit_passed": lit_res["passed"],
            "lit_failed": lit_res["failed"], "lit_failures": lit_res["failures"],
            "diff": diff["diff"], "added": diff["added"], "removed": diff["removed"],
            "replies": rv.result,
            "rebuild_tail": rebuild["log_tail"],
            "repro_tail": repro_after["log_tail"] if repro_after is not None else "",
            "lit_tail": lit_res.get("log_tail", ""), "logs": logs,
        }
    finally:
        for t in (lit, build, bash):
            if t is not None:
                try:
                    t.stop()
                except Exception:
                    pass
