"""Top-level driver for the CIRCT PR-review-feedback flow.

  GITHUB_TOKEN=... ./review_submit.sh --pr 10650:7949

For each PR:ISSUE pair, everything that exists on GitHub comes from GitHub
(read-only): the reviewer feedback, the PR's current unified diff (so rebases /
amendments are reflected), the PR description, and the original issue. The only
local artifact is the repro (issue_logs_to_pr/issue_<ISSUE>/repro/) — it isn't
part of any PR; if absent, the round runs without a repro gate. One
run_review_round_remote task per pair addresses the feedback on a chia-circt
container; the updated diff + the author replies it WOULD post are persisted.
No GitHub writes (the flow only READS).
"""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import ray
from chia.base.ChiaFunction import get, chia_wait, TrackedRef
from chia.github.github_issues_node import GithubIssuesNode
from chia.github.github_pulls_node import GithubPullsNode

from config import GITHUB_REPO
from review_task import run_review_round_remote

FLOW_DIR = Path(__file__).resolve().parent

# Repo we read PRs (feedback, diff, description) FROM — read-only, nothing is
# posted. ISSUE_REPO is where the paired issue lives; --pr maps a REVIEW_REPO
# PR number to an ISSUE_REPO issue number. Both default to the single
# GITHUB_REPO in config.py; split them only if you review PRs on a fork while
# the issues live upstream.
REVIEW_REPO = GITHUB_REPO
ISSUE_REPO  = GITHUB_REPO
CIRCT_TAG   = "HEAD"          # chia-circt checkout pinned at firtool-1.148.0
TOOL_TARGETS = ("circt-opt", "firtool", "circt-translate",
                "arcilator", "circt-lec", "circt-bmc")
REPRO_DIR  = "/workspace/circt/.circtissues"
REPRO_PATH = f"{REPRO_DIR}/repro.sh"
LLM_MODEL  = "claude-opus-4-6"
BUILD_JOBS = 16
TIMEOUTS   = {"review_assess": 3600, "review": 7200}
PENDING_TIMEOUT_S = 1800

SRC_DIR     = FLOW_DIR / "issue_logs_to_pr"   # curated PR candidates live here
ARTIFACT_DIR = FLOW_DIR / "review_logs"

_P = FLOW_DIR / "prompts"
CFG = {
    "tag": CIRCT_TAG, "tool_targets": TOOL_TARGETS, "repro_dir": REPRO_DIR,
    "repro_path": REPRO_PATH, "model": LLM_MODEL, "build_jobs": BUILD_JOBS,
    "timeouts": TIMEOUTS,
    "system_prompt": (_P / "system.md").read_text(),
    "review_assess_prompt": (_P / "review_assess.md").read_text(),
    "review_prompt": (_P / "review.md").read_text(),
}

# Ships the head's CURRENT chia checkout to workers (ahead of the image's baked
# install on sys.path) — see the matching note in circt_issue_loop.py.
_CHIA_PKG = FLOW_DIR.parent.parent / "chia"
_PY_MODULES = [str(FLOW_DIR / "circt_util.py"),
               str(FLOW_DIR / "review_task.py"),
               str(_CHIA_PKG)]
_RUNTIME_ENV_EXCLUDES = ["**/__pycache__", "**/*.pyc"]

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("circtreview")


def _pr_issue(pair: str) -> tuple[int, int]:
    pr, _, issue = pair.partition(":")
    if not issue:
        raise argparse.ArgumentTypeError(f"--pr must be PR:ISSUE, got {pair!r}")
    return int(pr), int(issue)


def _load_repro_files(issue_num: int) -> dict:
    """Best-effort: the locally-saved repro for ISSUE (issue_logs_to_pr/.../repro).

    The repro is the one artifact that exists only in this flow, not on GitHub.
    Missing -> {} (the round then runs without a repro gate; repro_ok=None).
    """
    rdir = SRC_DIR / f"issue_{issue_num}" / "repro"
    if not rdir.is_dir():
        logger.warning("issue #%d: no local repro at %s — round will run "
                       "without a repro gate", issue_num, rdir)
        return {}
    return {str(p.relative_to(rdir)): p.read_text()
            for p in rdir.rglob("*") if p.is_file()}


def _persist(pr_num: int, issue_num: int, feedback_md: str, res: dict) -> None:
    art = ARTIFACT_DIR / f"issue_{issue_num}_pr_{pr_num}"
    art.mkdir(parents=True, exist_ok=True)
    art.joinpath("review_feedback.md").write_text(feedback_md)        # the INPUT feedback
    if res.get("replies"):
        art.joinpath("replies.md").write_text(res["replies"])         # author replies (would-post)
    if res.get("diff"):
        art.joinpath("updated.diff").write_text(res["diff"])          # PR diff after the round
    art.joinpath("verdict.json").write_text(json.dumps(
        {k: res.get(k) for k in ("status", "build_ok", "repro_ok", "lit_ok",
                                 "lit_passed", "lit_failed", "lit_failures",
                                 "added", "removed", "notes")}, indent=2))
    for phase, blob in (res.get("logs") or {}).items():
        art.joinpath(f"llm_{phase}.md").write_text(blob.get("stream") or blob.get("result") or "")
        tr = blob.get("transcript")
        if isinstance(tr, (bytes, bytearray)) and tr:
            art.joinpath(f"llm_{phase}.jsonl").write_bytes(tr)
    for key, fname in (("rebuild_tail", "verify_build.log"),
                       ("repro_tail", "verify_repro.log"),
                       ("lit_tail", "verify_lit.log")):
        if res.get(key):
            art.joinpath(fname).write_text(res[key])
    logger.info("PR #%d (issue #%d) -> %s  (+%s/-%s, repro_ok=%s, lit_ok=%s) -> %s",
                pr_num, issue_num, res.get("status"), res.get("added"),
                res.get("removed"), res.get("repro_ok"), res.get("lit_ok"), art)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pr", action="append", type=_pr_issue, required=True,
                    metavar="PR:ISSUE", dest="pairs",
                    help="REVIEW_REPO PR number : issue_logs issue number "
                         "(repeatable, e.g. --pr 5:7949)")
    args = ap.parse_args()

    ray.init(address="auto",
             runtime_env={"py_modules": _PY_MODULES,
                          "excludes": _RUNTIME_ENV_EXCLUDES},
             logging_level=logging.WARNING)

    prs = GithubPullsNode(REVIEW_REPO)
    issues = GithubIssuesNode(ISSUE_REPO)

    submissions = []
    for pr_num, issue_num in args.pairs:
        fb = prs.review_feedback(pr_num)
        feedback_md = fb.to_markdown()
        if fb.is_empty():
            logger.warning("PR #%d (issue #%d): NO review feedback found — skipping",
                           pr_num, issue_num)
            continue
        # Everything that exists on GitHub comes from GitHub: the PR's CURRENT
        # diff (authoritative — reflects rebases/amendments, so reviewer
        # comments anchor against what they actually reviewed), its
        # description, and the real issue. Only the repro is local.
        cand = {
            "issue_md": issues.get_issue(issue_num).to_markdown(),
            "base_diff": prs.pull_diff(pr_num),
            "pr_description": fb.pull.body or "",
            "repro_files": _load_repro_files(issue_num),
        }
        logger.info("PR #%d (issue #%d): %d review(s), %d inline comment(s), "
                    "%d FAILED CI check(s); diff %d lines; repro files: %d",
                    pr_num, issue_num, len(fb.reviews), len(fb.review_comments),
                    len(fb.failed_checks()),
                    cand["base_diff"].count("\n") + 1, len(cand["repro_files"]))
        submissions.append((pr_num, issue_num, feedback_md, cand))

    if not submissions:
        logger.info("nothing to do (no PRs with feedback)")
        return

    def _submit(issue_num, cand, feedback_md):
        return run_review_round_remote.chia_remote(
            cand["issue_md"], issue_num, CFG, cand["base_diff"],
            cand["repro_files"], feedback_md, cand["pr_description"])

    tracked, meta = [], {}
    for pr_num, issue_num, feedback_md, cand in submissions:
        tr = TrackedRef(
            ref=_submit(issue_num, cand, feedback_md),
            submit_fn=(lambda i=issue_num, c=cand, f=feedback_md: _submit(i, c, f)),
            label=f"review_pr_{pr_num}")
        tracked.append(tr)
        meta[id(tr)] = (pr_num, issue_num, feedback_md)

    pending = tracked
    while pending:
        done, pending = chia_wait(pending, num_returns=1,
                                  pending_timeout=PENDING_TIMEOUT_S, retry=True)
        for tr in done:
            pr_num, issue_num, feedback_md = meta[id(tr)]
            try:
                _persist(pr_num, issue_num, feedback_md, get(tr.ref))
            except Exception:
                logger.exception("PR #%d (issue #%d) failed", pr_num, issue_num)


if __name__ == "__main__":
    main()
