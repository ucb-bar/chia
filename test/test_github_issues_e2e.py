"""End-to-end test for GithubIssuesNode.

Hits api.github.com directly (no mocks). Defaults to a small, stable public
repo so the test stays fast and works without a token. Set ``GITHUB_TOKEN`` in
the environment to raise the rate-limit ceiling from 60 to 5000 requests/hour.

Usage:
    python test/test_github_issues_e2e.py

Environment variables:
    CHIA_GH_REPO   Target repo for read tests (default: chipsalliance/chisel)
    GITHUB_TOKEN   Optional auth token; falls back to unauthenticated requests
"""

import logging
import os
import sys

from chia.github.github_issues_node import (
    GithubIssuesNode,
    GithubNotFoundError,
)


REPO = os.environ.get("CHIA_GH_REPO", "chipsalliance/chisel")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_github_issues_e2e")


def _node(state: str = "all") -> GithubIssuesNode:
    return GithubIssuesNode(REPO, state=state)


def test_recent_one():
    issues = _node().recent(n=1)
    assert len(issues) == 1, f"expected 1, got {len(issues)}"
    iss = issues[0]
    assert iss.number > 0
    assert iss.title
    assert iss.state in ("open", "closed")
    assert iss.author
    assert iss.created_at
    assert iss.updated_at
    assert iss.url.startswith("https://github.com/")
    assert iss.is_pull_request is False


def test_recent_top_5():
    issues = _node().recent(n=5)
    assert len(issues) == 5, f"expected 5, got {len(issues)}"
    numbers = [i.number for i in issues]
    assert numbers == sorted(numbers, reverse=True), f"not strictly desc: {numbers}"
    assert len(set(numbers)) == 5, f"duplicate numbers: {numbers}"
    assert all(i.is_pull_request is False for i in issues)


def test_recent_after_ref():
    node = _node()
    first = node.recent(n=1)[0]
    next_three = node.recent(n=3, after=first.number)
    assert len(next_three) == 3
    assert all(i.number < first.number for i in next_three), \
        f"some numbers >= ref {first.number}: {[i.number for i in next_three]}"
    nums = [i.number for i in next_three]
    assert nums == sorted(nums, reverse=True), f"not strictly desc: {nums}"


def test_recent_pulls():
    pulls = _node().recent_pulls(n=2)
    assert len(pulls) == 2, f"expected 2, got {len(pulls)}"
    assert all(p.is_pull_request is True for p in pulls)
    nums = [p.number for p in pulls]
    assert nums == sorted(nums, reverse=True)


def test_get_by_number():
    node = _node()
    target = node.recent(n=1)[0].number
    fetched = node.get(target)
    assert fetched.number == target
    assert fetched.title


def test_url_input():
    a = GithubIssuesNode(REPO, state="all").recent(n=1)[0]
    b = GithubIssuesNode(f"https://github.com/{REPO}.git", state="all").recent(n=1)[0]
    assert a.number == b.number, f"slug and URL forms disagree: {a.number} vs {b.number}"


def test_not_found():
    node = GithubIssuesNode("definitely-nonexistent-owner-xyz/definitely-nonexistent-repo-xyz")
    try:
        node.recent(n=1)
    except GithubNotFoundError:
        return
    raise AssertionError("expected GithubNotFoundError")


def test_to_markdown():
    iss = _node().recent(n=1)[0]
    md = iss.to_markdown()
    assert f"#{iss.number}" in md
    assert iss.title in md
    if iss.body:
        assert iss.body in md
    assert ("## Comments" in md) == bool(iss.comments)


def test_state_filter():
    closed = GithubIssuesNode(REPO, state="closed").recent(n=3)
    assert len(closed) == 3
    assert all(i.state == "closed" for i in closed), \
        f"some not closed: {[i.state for i in closed]}"


TESTS = [
    test_recent_one,
    test_recent_top_5,
    test_recent_after_ref,
    test_recent_pulls,
    test_get_by_number,
    test_url_input,
    test_not_found,
    test_to_markdown,
    test_state_filter,
]


def main():
    failed: list[str] = []
    for t in TESTS:
        name = t.__name__
        logger.info("RUN  %s", name)
        try:
            t()
        except Exception as exc:
            failed.append(name)
            logger.error("FAIL %s: %s", name, exc)
        else:
            logger.info("PASS %s", name)
    if failed:
        print(f"\n{len(failed)}/{len(TESTS)} tests failed: {failed}")
        sys.exit(1)
    print(f"\nAll {len(TESTS)} tests passed against {REPO}")


if __name__ == "__main__":
    main()
