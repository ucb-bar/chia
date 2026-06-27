"""Top-level driver for the CIRCT GitHub-issue solving flow (circt_issue_solver).

  chia up cluster.yaml                            # 2 LLM + 2 CIRCT workers (1 host)
  GITHUB_TOKEN=... ./fix_issues_submit.sh --max-issues 5

Triage open issues (from config.GITHUB_REPO) on the head, fan one
run_issue_remote task per candidate across the CIRCT containers, prompt via
chia.models.claude (dispatched onto the llm workers), and persist the local diff
+ the PR writeup it WOULD submit. No GitHub writes.
"""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import ray
from chia.base.ChiaFunction import get, chia_wait, TrackedRef

import db
import triage
from config import GITHUB_REPO
from issue_task import run_issue_remote

# --------------------------------------------------------------------------- #
# Parameters — globals, not env vars (project convention). GITHUB_TOKEN is the
# one exception: a secret, read from the env by GithubIssuesNode.
# --------------------------------------------------------------------------- #
FLOW_DIR = Path(__file__).resolve().parent

GH_REPO   = GITHUB_REPO       # the repo to triage/fetch issues from (see config.py)
CIRCT_TAG = "HEAD"            # the chia-circt checkout is pinned at firtool-1.148.0
# circt-verilog is omitted: its ninja target doesn't exist in the SDK-based
# build (no slang/ImportVerilog), so including it fails the whole `ninja` call.
# The SDK ships a prebuilt circt-verilog at /opt/circt-sdk/bin for read-only
# repro use. The other six build cleanly from source.
TOOL_TARGETS = ("circt-opt", "firtool", "circt-translate",
                "arcilator", "circt-lec", "circt-bmc")
REPRO_DIR  = "/workspace/circt/.circtissues"
REPRO_PATH = f"{REPRO_DIR}/repro.sh"

MAX_ISSUES    = 20
TRIAGE_POOL   = 2000  # cover the full open backlog (~857); listed w/o comments,
                      # ~ceil(pool/100) requests, then triage samples RANDOMLY
TRIAGE_LABELS = []   # no label gate — the assess phase decides bug-ness per issue
REQUIRE_REPRO = True

LLM_MODEL  = "claude-opus-4-6"
BUILD_JOBS = 16
TIMEOUTS   = {"assess": 1800, "repro": 1800, "fix": 7200, "regression": 3600, "writeup": 1200}
PENDING_TIMEOUT_S = 1800      # chia_wait stuck-task detection / retry threshold

DB_PATH      = str(FLOW_DIR / "issues.db")
ARTIFACT_DIR = FLOW_DIR / "issue_logs"

_P = FLOW_DIR / "prompts"
CFG = {
    "tag": CIRCT_TAG, "tool_targets": TOOL_TARGETS, "repro_dir": REPRO_DIR,
    "repro_path": REPRO_PATH, "require_repro": REQUIRE_REPRO,
    "model": LLM_MODEL, "build_jobs": BUILD_JOBS, "timeouts": TIMEOUTS,
    "system_prompt":  (_P / "system.md").read_text(),
    "assess_prompt":  (_P / "assess.md").read_text(),
    "repro_prompt":   (_P / "reproduce.md").read_text(),
    "fix_prompt":     (_P / "fix.md").read_text(),
    "regression_prompt": (_P / "regression.md").read_text(),
    "writeup_prompt": (_P / "writeup.md").read_text(),
}

# Worker modules shipped so chia-circt workers can import run_issue_remote and the
# local circt_util it depends on — no image rebuild for edits to these. The
# BuildTool / LitTool MCP wrappers now live in chia.chipyard.circt, so they ride
# along in the chia package below (no separate circt_tools.py to ship).
# The chia PACKAGE itself ships too (~3 MB): Ray puts py_modules ahead of the
# image's site-packages on workers' sys.path, so every task imports the head's
# CURRENT chia checkout instead of whatever was baked into the image at build
# time (its deps still come from the image). This
# example lives at <repo>/examples/circt_issue_solver, so the chia package is
# two levels up: <repo>/chia.
_CHIA_PKG = FLOW_DIR.parent.parent / "chia"
_PY_MODULES = [str(FLOW_DIR / "circt_util.py"),
               str(FLOW_DIR / "issue_task.py"),
               str(_CHIA_PKG)]
# excludes applies to runtime-env uploads (working_dir + py_modules).
_RUNTIME_ENV_EXCLUDES = ["**/__pycache__", "**/*.pyc"]

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("circtissues")


def _persist(issue, res: dict) -> None:
    art = ARTIFACT_DIR / f"issue_{issue.number}"
    art.mkdir(parents=True, exist_ok=True)
    art.joinpath("issue.md").write_text(issue.to_markdown())
    if res.get("diff"):
        art.joinpath("fix.diff").write_text(res["diff"])
    if res.get("writeup"):
        art.joinpath("pr_writeup.md").write_text(res["writeup"])
    art.joinpath("verdict.json").write_text(json.dumps(
        {k: res.get(k) for k in ("status", "reproduced", "build_ok", "fixed",
                                 "lit_ok", "lit_passed", "lit_failed",
                                 "lit_failures", "added", "removed", "test_paths",
                                 "notes")},
        indent=2))
    for phase, blob in (res.get("logs") or {}).items():
        art.joinpath(f"llm_{phase}.md").write_text(blob.get("stream") or blob.get("result") or "")
        if blob.get("stderr"):
            art.joinpath(f"llm_{phase}.stderr").write_text(blob["stderr"])
        tr = blob.get("transcript")
        if isinstance(tr, (bytes, bytearray)) and tr:
            art.joinpath(f"llm_{phase}.jsonl").write_bytes(tr)   # full raw session transcript
    rf = res.get("repro_files") or {}
    if rf:
        rdir = art / "repro"
        rdir.mkdir(exist_ok=True)
        for rel, content in rf.items():
            dest = rdir / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(content)
    if res.get("rebuild_tail"):
        art.joinpath("verify_build.log").write_text(res["rebuild_tail"])
    if res.get("repro_tail"):
        art.joinpath("verify_repro.log").write_text(res["repro_tail"])
    if res.get("lit_tail"):
        art.joinpath("verify_lit.log").write_text(res["lit_tail"])
    db.record(issue, res, LLM_MODEL, str(art))
    logger.info("issue #%d -> %s  (+%s/-%s, lit_ok=%s)", issue.number, res.get("status"),
                res.get("added"), res.get("removed"), res.get("lit_ok"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-issues", type=int, default=MAX_ISSUES)
    ap.add_argument("--issue", type=int, default=None, help="run one specific issue, skip triage")
    ap.add_argument("--replay-regression", type=int, default=None, metavar="N",
                    help="replay issue N's saved fix.diff + repro and jump to the "
                         "regression-repair turn (skips repro+fix)")
    ap.add_argument("--assess-only", type=int, default=None, metavar="N",
                    help="run ONLY the assess turn for issue N; print the decision "
                         "and exit. Writes nothing to issue_logs or the DB.")
    args = ap.parse_args()

    ray.init(address="auto",
             runtime_env={"py_modules": _PY_MODULES,
                          "excludes": _RUNTIME_ENV_EXCLUDES},
             logging_level=logging.WARNING)

    # Spot-check path: assess one issue, print the verdict, persist NOTHING (no DB).
    if args.assess_only is not None:
        from chia.github.github_issues_node import GithubIssuesNode
        issue = GithubIssuesNode(GH_REPO).get_issue(args.assess_only)
        res = get(run_issue_remote.chia_remote(issue.to_markdown(), issue.number,
                                               CFG, assess_only=True))
        print(f"\n===== assess-only #{issue.number}: {issue.title} =====")
        print(f"DECISION -> status={res.get('status')!r}")
        print(f"NOTE: {res.get('notes')}")
        blob = (res.get("logs") or {}).get("assess") or {}
        print("\n----- assess transcript (tail) -----")
        print((blob.get("stream") or blob.get("result") or "")[-2500:])
        return

    # SQLiteNode-backed store; pins to this (head) Ray node, so it must come
    # after ray.init(). Skipped on the assess-only path above (it persists nothing).
    db.init_db(DB_PATH)

    resume_by_num: dict = {}
    if args.replay_regression is not None:
        from chia.github.github_issues_node import GithubIssuesNode
        n = args.replay_regression
        art = ARTIFACT_DIR / f"issue_{n}"
        diff_text = (art / "fix.diff").read_text()
        rdir = art / "repro"
        repro_files = ({str(p.relative_to(rdir)): p.read_text()
                        for p in rdir.rglob("*") if p.is_file()} if rdir.is_dir() else {})
        resume_by_num[n] = {"diff": diff_text, "repro_files": repro_files}
        candidates = [GithubIssuesNode(GH_REPO).get_issue(n)]
        logger.info("replay-regression #%d: %d-line diff, %d repro file(s)",
                    n, diff_text.count("\n") + 1, len(repro_files))
    elif args.issue is not None:
        from chia.github.github_issues_node import GithubIssuesNode
        candidates = [GithubIssuesNode(GH_REPO).get_issue(args.issue)]
    else:
        candidates = triage.select(GH_REPO, TRIAGE_POOL, TRIAGE_LABELS,
                                   args.max_issues, db.attempted_numbers())
    logger.info("triage selected %d: %s", len(candidates), [c.number for c in candidates])
    if not candidates:
        return

    # Fan out — Ray spreads these across the circt slots; each task in turn
    # dispatches its prompts onto the llm workers (chia.models.claude).
    def _submit(c):
        return run_issue_remote.chia_remote(c.to_markdown(), c.number, CFG,
                                            resume=resume_by_num.get(c.number))

    tracked, tr_issue = [], {}
    for c in candidates:
        tr = TrackedRef(ref=_submit(c), submit_fn=(lambda c=c: _submit(c)),
                        label=f"issue_{c.number}")
        tracked.append(tr)
        tr_issue[id(tr)] = c

    pending = tracked
    try:
        while pending:
            done, pending = chia_wait(pending, num_returns=1,
                                      pending_timeout=PENDING_TIMEOUT_S, retry=True)
            for tr in done:
                issue = tr_issue[id(tr)]
                try:
                    _persist(issue, get(tr.ref))
                except Exception:
                    logger.exception("issue #%d failed", issue.number)
    finally:
        db.close_db()


if __name__ == "__main__":
    main()
