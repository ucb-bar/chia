"""Read-only GitHub issues client, exposed as a chia service-pattern node.


Bind to one repo at construction and expose a small, synchronous API. Errors are
raised as typed exceptions (no in-band ``success: bool``); see ``GithubError``
and its subclasses (defined in ``chia.github.github_client`` and re-exported
here for backwards compatibility). For pull-request reviews/comments, see
:class:`chia.github.github_pulls_node.GithubPullsNode`.
"""

from __future__ import annotations

import logging
from typing import Any

from chia.github.github_client import (  # noqa: F401  (re-exported for back-compat)
    GithubAuthError,
    GithubClient,
    GithubError,
    GithubNotFoundError,
    GithubRateLimitError,
    GithubRequestError,
    GithubServerError,
)
from chia.github.state_def import GithubComment, GithubIssue


class GithubIssuesNode(GithubClient):
    """Read-only client for one GitHub repo's issues.

    Service-pattern node (head-node only, not a Ray task). Bind to one repo
    at construction; fetch via :meth:`recent` or :meth:`get_issue`. All calls
    are synchronous and raise typed exceptions on failure. Pull requests live
    on :class:`chia.github.github_pulls_node.GithubPullsNode`.
    """

    logging_name = "GithubIssuesNode"
    _USER_AGENT = "chia-github-issues-node"

    def __init__(
        self,
        repo: str,
        token: str | None = None,
        state: str = "open",
        timeout_seconds: int = 30,
        logging_level: int = logging.DEBUG,
    ):
        if state not in ("open", "closed", "all"):
            raise ValueError(f"state must be one of 'open'/'closed'/'all', got {state!r}")
        super().__init__(repo, token=token, timeout_seconds=timeout_seconds,
                         logging_level=logging_level)
        self.state = state

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def recent(self, n: int = 1, after: int | None = None,
               fetch_comments: bool = True) -> list[GithubIssue]:
        """Return up to *n* most-recent issues (sorted by created desc).

        With ``after=None``, returns the *n* newest issues. With ``after=K``,
        returns the *n* newest issues whose number is strictly less than *K*.
        Pull requests are excluded — to list PRs use
        :meth:`chia.github.github_pulls_node.GithubPullsNode.recent`.

        ``fetch_comments`` defaults to True (one extra paginated request per
        issue that has comments). Pass False to skip that N+1 cost when you only
        need list-payload fields (title/body/labels) — e.g. to filter a large
        pool cheaply, then re-fetch comments for the few survivors via
        :meth:`get_issue`.
        """
        return self._list(n=n, after=after, fetch_comments=fetch_comments)

    def get_issue(self, number: int, allow_pull_request: bool = False) -> GithubIssue:
        """Fetch one ISSUE by number.

        GitHub serves pull requests through the issues endpoint too (shared
        number space), so the endpoint CAN return a PR — but a PR-as-issue is a
        degraded view (no head/base/branch data) and almost never what an
        issues-node caller wants, so by default a PR number raises
        :class:`GithubRequestError` instead of silently succeeding. Pass
        ``allow_pull_request=True`` to opt in (the result's ``is_pull_request``
        field will be True). For a proper PR view use
        :meth:`chia.github.github_pulls_node.GithubPullsNode.get_pull`.
        """
        payload = self._request(f"/repos/{self.owner}/{self.name}/issues/{number}")
        if payload.get("pull_request") is not None and not allow_pull_request:
            raise GithubRequestError(
                f"#{number} in {self.owner}/{self.name} is a pull request, not an"
                " issue — use GithubPullsNode.get_pull for the PR view, or pass"
                " allow_pull_request=True for the degraded issue-shaped view")
        return self._build_issue(payload, fetch_comments=True)

    def get(self, number: int) -> GithubIssue:
        """Deprecated alias for :meth:`get_issue` with ``allow_pull_request=True``
        (the historical permissive behavior). Prefer :meth:`get_issue`, or
        :meth:`GithubPullsNode.get_pull` for pull requests."""
        return self.get_issue(number, allow_pull_request=True)

    def linked_pull_requests(self, number: int, open_only: bool = False) -> list[dict]:
        """Pull requests attached to issue *number*, via its timeline.

        A PR that references the issue (``Fixes #N``, or a manual link) leaves a
        ``cross-referenced`` timeline event whose ``source`` is a PR; we collect
        those. Each result is ``{"number": int, "state": str}`` (state is the
        PR's open/closed, deduped by number). ``open_only`` keeps just open PRs.

        Note: relies on cross-reference events (the common case, incl. the
        closing-keyword link). A PR linked ONLY via the sidebar with no
        cross-reference may not appear.
        """
        path = f"/repos/{self.owner}/{self.name}/issues/{number}/timeline"
        found: dict[int, str] = {}
        page = 1
        while True:
            items = self._request(path, params={"per_page": self._PER_PAGE, "page": page})
            if not isinstance(items, list) or not items:
                break
            for ev in items:
                if ev.get("event") != "cross-referenced":
                    continue
                src = (ev.get("source") or {}).get("issue") or {}
                if src.get("pull_request") is None:
                    continue  # cross-ref from a plain issue, not a PR
                n = src.get("number")
                if n is not None:
                    found[n] = src.get("state") or ""
            if len(items) < self._PER_PAGE:
                break
            page += 1
        prs = [{"number": n, "state": s} for n, s in sorted(found.items())]
        return [p for p in prs if p["state"] == "open"] if open_only else prs

    # ------------------------------------------------------------------
    # Internals  (HTTP plumbing lives in GithubClient)
    # ------------------------------------------------------------------

    def _list(
        self,
        *,
        n: int,
        after: int | None,
        fetch_comments: bool = True,
    ) -> list[GithubIssue]:
        if n <= 0:
            return []

        path = f"/repos/{self.owner}/{self.name}/issues"
        params: dict[str, Any] = {
            "state": self.state,
            "sort": "created",
            "direction": "desc",
            "per_page": self._PER_PAGE,
            "page": 1,
        }

        out: list[GithubIssue] = []
        while len(out) < n:
            page_items = self._request(path, params=params)
            if not isinstance(page_items, list) or not page_items:
                break

            for item in page_items:
                if after is not None and item.get("number", 0) >= after:
                    continue
                # /issues conflates issues + PRs; this node lists issues only.
                if item.get("pull_request") is not None:
                    continue
                out.append(self._build_issue(item, fetch_comments=fetch_comments))
                if len(out) >= n:
                    break

            if len(page_items) < self._PER_PAGE:
                break  # last page
            params["page"] += 1

        return out

    def _build_issue(
        self,
        payload: dict,
        *,
        fetch_comments: bool,
        is_pull_request: bool | None = None,
    ) -> GithubIssue:
        if is_pull_request is None:
            is_pull_request = payload.get("pull_request") is not None
        number = payload["number"]
        comments: list[GithubComment] = []
        if fetch_comments and payload.get("comments", 0) > 0:
            comments = self._fetch_comments(number)
        return GithubIssue(
            number=number,
            title=payload.get("title") or "",
            state=payload.get("state") or "",
            author=(payload.get("user") or {}).get("login") or "",
            created_at=payload.get("created_at") or "",
            updated_at=payload.get("updated_at") or "",
            closed_at=payload.get("closed_at"),
            body=payload.get("body") or "",
            labels=[lbl.get("name", "") for lbl in payload.get("labels", []) if lbl.get("name")],
            url=payload.get("html_url") or "",
            is_pull_request=is_pull_request,
            comments=comments,
        )

    def _fetch_comments(self, issue_number: int) -> list[GithubComment]:
        """Fetch all comments on an issue/PR, paginating if needed."""
        path = f"/repos/{self.owner}/{self.name}/issues/{issue_number}/comments"
        out: list[GithubComment] = []
        page = 1
        while True:
            items = self._request(path, params={"per_page": self._PER_PAGE, "page": page})
            if not isinstance(items, list) or not items:
                break
            for c in items:
                out.append(GithubComment(
                    author=(c.get("user") or {}).get("login") or "",
                    created_at=c.get("created_at") or "",
                    body=c.get("body") or "",
                ))
            if len(items) < self._PER_PAGE:
                break
            page += 1
        return out
