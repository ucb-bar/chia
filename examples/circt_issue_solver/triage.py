"""Issue triage for the CIRCT issue flow (head node only).

Heuristic filter over open llvm/circt issues: keep issues that carry a code-block
repro and aren't obvious feature requests (by title), already attempted, or
already covered by an open PR. There is NO label gate by default — deciding
whether an issue is actually a bug is left to the assess phase, which reads it
(and the code) per issue. The qualifying set is sampled at random.
"""
from __future__ import annotations

import logging
import random
import re
from typing import Iterable

from chia.github.github_issues_node import GithubIssuesNode

logger = logging.getLogger("circtissues")

_FEATURE_RE = re.compile(
    r"(?i)\b(feature request|feature:|add (support|an?\b)|enhance|enhancement|"
    r"proposal|RFC|\[RFC\])\b")
_CMD_RE = re.compile(
    r"circt-opt|firtool|arcilator|circt-verilog|circt-translate|circt-lec|circt-bmc")
_SIGNAL_RE = re.compile(
    r"(?i)crash|assert|segfault|stack dump|UNREACHABLE|error:|miscompile|incorrect|wrong")


def has_repro(issue) -> bool:
    """A self-contained repro looks like: a fenced code block + either a tool
    command or a failure signal (crash/assert/miscompile)."""
    body = issue.body or ""
    return "```" in body and (bool(_CMD_RE.search(body)) or bool(_SIGNAL_RE.search(body)))


def select(repo: str, pool: int, labels: list[str], max_issues: int,
           already: Iterable[int], seed: int | None = None) -> list:
    """Return up to *max_issues* candidate GithubIssues chosen at random.

    Lists the *pool* most-recent open issues without comments — the heuristic
    filter only needs list-payload fields (title/body/labels), so we avoid the
    N+1 per-issue comment fetch over the whole pool (set *pool* high enough to
    cover the full open backlog and the listing is just ~ceil(pool/100)
    requests). Drops: already attempted, label misses, feature requests, and
    repro-less issues, and issues with an OPEN PULL REQUEST attached (someone is
    actively on it — closed/unmerged PRs are abandoned attempts and stay
    eligible). Finally re-fetches each survivor via get() to attach comments 
    (useful repro context for the LLM) — only for the handful chosen.

    The PR check costs one timeline request per issue, so it's done lazily: the
    cheap-qualifying set is shuffled, then we walk it checking PRs and keep the
    first *max_issues* with no open PR — ~max_issues+dropped requests, not one
    per qualifier. (Shuffle-then-take-first-K-matching is still a uniform draw.)

    *seed* makes the draw reproducible (None = nondeterministic).
    """
    already = set(already)
    wanted = set(labels)
    node = GithubIssuesNode(repo, state="open")
    qualifying = []
    for issue in node.recent(n=pool, fetch_comments=False):
        if issue.number in already:
            continue
        if wanted and not (wanted & set(issue.labels)):
            continue
        if _FEATURE_RE.search(issue.title or ""):
            continue
        if not has_repro(issue):
            continue
        qualifying.append(issue)

    rng = random.Random(seed)
    rng.shuffle(qualifying)
    chosen, dropped_pr = [], 0
    for issue in qualifying:
        if len(chosen) >= max_issues:
            break
        if node.linked_pull_requests(issue.number, open_only=True):
            dropped_pr += 1
            continue
        chosen.append(issue)
    logger.info("triage: %d qualifying, %d skipped (open PR attached), %d chosen",
                len(qualifying), dropped_pr, len(chosen))
    # Re-fetch only the survivors with their comments attached.
    return [node.get_issue(issue.number) for issue in chosen]
