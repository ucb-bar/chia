"""Read-only GitHub pull-request client, exposed as a chia service-pattern node.

Companion to :class:`chia.github.github_issues_node.GithubIssuesNode`, sharing
the same repo-bound, head-node-only conventions and the
:class:`chia.github.github_client.GithubClient` HTTP plumbing. Use this for
pull-request metadata and — its main purpose — reading the REVIEW FEEDBACK on a
PR (review summaries + inline line comments + conversation comments) so an agent
can act on it like a PR author.

Errors raise the typed exceptions from ``chia.github.github_client``.
"""

from __future__ import annotations

import logging

from chia.github.github_client import GithubClient, GithubNotFoundError
from chia.github.state_def import (
    GithubCheckAnnotation,
    GithubCheckRun,
    GithubComment,
    GithubPull,
    GithubPullFeedback,
    GithubReview,
    GithubReviewComment,
)


class GithubPullsNode(GithubClient):
    """Read-only client for one GitHub repo's pull requests.

    Bind to one repo at construction, then:
      * :meth:`recent` — list the newest PRs (open/closed/all, per call).
      * :meth:`get_pull` — PR metadata (branches, head sha, draft/merged, body).
      * :meth:`pull_diff` — the PR's current unified diff as raw text.
      * :meth:`check_runs` — CI check results (+ annotations for failures).
      * :meth:`reviews` — submitted review summaries.
      * :meth:`review_comments` — inline, line-anchored review comments.
      * :meth:`conversation_comments` — general (non-inline) conversation comments.
      * :meth:`review_feedback` — all of the above bundled into a
        :class:`GithubPullFeedback` (with ``to_markdown()`` for LLM input).
    """

    logging_name = "GithubPullsNode"
    _USER_AGENT = "chia-github-pulls-node"

    def __init__(self, repo: str, token: str | None = None,
                 timeout_seconds: int = 30, logging_level: int = logging.DEBUG):
        super().__init__(repo, token=token, timeout_seconds=timeout_seconds,
                         logging_level=logging_level)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def recent(self, n: int = 1, after: int | None = None,
               state: str = "open") -> list[GithubPull]:
        """Return up to *n* most-recent pull requests (sorted by created desc).

        With ``after=None``, the *n* newest PRs; with ``after=K``, the *n*
        newest PRs whose number is strictly less than *K*. *state* is
        ``"open"`` / ``"closed"`` / ``"all"`` (per-call — this node binds no
        state at construction).

        Note: the ``/pulls`` LIST payload omits the computed ``merged`` flag;
        ``merged`` on results from this method is derived from ``merged_at``
        (equivalent for merged-vs-not).
        """
        if state not in ("open", "closed", "all"):
            raise ValueError(f"state must be one of 'open'/'closed'/'all', got {state!r}")
        if n <= 0:
            return []
        path = f"/repos/{self.owner}/{self.name}/pulls"
        params = {"state": state, "sort": "created", "direction": "desc",
                  "per_page": self._PER_PAGE, "page": 1}
        out: list[GithubPull] = []
        while len(out) < n:
            page_items = self._request(path, params=params)
            if not isinstance(page_items, list) or not page_items:
                break
            for item in page_items:
                if after is not None and item.get("number", 0) >= after:
                    continue
                out.append(self._build_pull(item))
                if len(out) >= n:
                    break
            if len(page_items) < self._PER_PAGE:
                break  # last page
            params["page"] += 1
        return out

    def get_pull(self, number: int) -> GithubPull:
        """Fetch one PULL REQUEST's metadata by number.

        Unlike the shared issues endpoint, ``/pulls/{number}`` serves ONLY pull
        requests — a plain issue number 404s even though it exists in the shared
        number space. That 404 is re-raised with a clearer message, since "not
        found" usually means "exists, but as an issue". For issues use
        :meth:`chia.github.github_issues_node.GithubIssuesNode.get_issue`.
        """
        try:
            p = self._request(f"/repos/{self.owner}/{self.name}/pulls/{number}")
        except GithubNotFoundError as exc:
            raise GithubNotFoundError(
                f"#{number} is not a pull request in {self.owner}/{self.name} —"
                " it may be a plain issue (use GithubIssuesNode.get_issue), not"
                " exist, or be in a private repo your token can't see (GitHub"
                " returns 404, not 401/403, for those)") from exc
        return self._build_pull(p)

    def get(self, number: int) -> GithubPull:
        """Deprecated alias for :meth:`get_pull`."""
        return self.get_pull(number)

    def pull_diff(self, number: int) -> str:
        """The PR's CURRENT unified diff (head vs base), as raw text.

        Uses the ``application/vnd.github.diff`` media type on the single-PR
        endpoint — this is the diff of the PR as it exists NOW (rebases and
        force-pushes included), which makes it the authoritative base for
        reconstructing the PR's state. GitHub refuses very large diffs
        (~20k+ lines) with a 406, surfaced as :class:`GithubRequestError`.
        """
        try:
            return self._request(f"/repos/{self.owner}/{self.name}/pulls/{number}",
                                 accept="application/vnd.github.diff")
        except GithubNotFoundError as exc:
            raise GithubNotFoundError(
                f"#{number} is not a pull request in {self.owner}/{self.name} —"
                " it may be a plain issue, not exist, or be in a private repo"
                " your token can't see") from exc

    def reviews(self, number: int) -> list[GithubReview]:
        """Submitted review summaries on PR *number* (oldest first)."""
        items = self._paginate(f"/repos/{self.owner}/{self.name}/pulls/{number}/reviews")
        out: list[GithubReview] = []
        for r in items:
            # GitHub emits a synthetic state="COMMENTED" review with empty body
            # for every PR that has inline comments; keep only ones that carry
            # signal (a body, or a decision state).
            state = r.get("state") or ""
            body = r.get("body") or ""
            if not body and state in ("", "COMMENTED", "PENDING"):
                continue
            out.append(GithubReview(
                author=(r.get("user") or {}).get("login") or "",
                state=state, body=body, submitted_at=r.get("submitted_at") or "",
            ))
        return out

    def review_comments(self, number: int) -> list[GithubReviewComment]:
        """Inline, line-anchored review comments on PR *number* (oldest first)."""
        items = self._paginate(f"/repos/{self.owner}/{self.name}/pulls/{number}/comments")
        out: list[GithubReviewComment] = []
        for c in items:
            out.append(GithubReviewComment(
                author=(c.get("user") or {}).get("login") or "",
                path=c.get("path") or "",
                line=c.get("line") if c.get("line") is not None else c.get("original_line"),
                body=c.get("body") or "",
                diff_hunk=c.get("diff_hunk") or "",
                in_reply_to=c.get("in_reply_to_id"),
                created_at=c.get("created_at") or "",
                id=c.get("id"),
            ))
        return out

    def conversation_comments(self, number: int) -> list[GithubComment]:
        """General (non-inline) conversation comments on PR *number*.

        PRs share the issue number space, so these come from the issues
        comments endpoint — the back-and-forth that isn't anchored to a line.
        """
        items = self._paginate(f"/repos/{self.owner}/{self.name}/issues/{number}/comments")
        return [GithubComment(
            author=(c.get("user") or {}).get("login") or "",
            created_at=c.get("created_at") or "",
            body=c.get("body") or "",
        ) for c in items]

    def check_runs(self, number: int, head_sha: str | None = None,
                   annotations_for_failures: bool = True) -> list[GithubCheckRun]:
        """CI check results on PR *number*'s head commit (latest run per check).

        Uses the Checks API with ``filter=latest`` so a re-run replaces its
        earlier attempt. For FAILED runs (conclusion failure/timed_out), also
        fetches the run's file/line annotations — typically the compiler/test
        errors — capped at 50 per run. *head_sha* avoids a refetch of the PR
        when the caller already has it.
        """
        if head_sha is None:
            head_sha = (self._request(
                f"/repos/{self.owner}/{self.name}/pulls/{number}").get("head") or {}).get("sha")
        if not head_sha:
            return []
        path = f"/repos/{self.owner}/{self.name}/commits/{head_sha}/check-runs"
        out: list[GithubCheckRun] = []
        page = 1
        while True:
            # Not a bare list (so no _paginate): {"total_count": N, "check_runs": [...]}.
            data = self._request(path, params={"per_page": self._PER_PAGE,
                                               "page": page, "filter": "latest"})
            items = data.get("check_runs") or [] if isinstance(data, dict) else []
            if not items:
                break
            for r in items:
                output = r.get("output") or {}
                summary = " — ".join(s for s in (output.get("title"), output.get("summary")) if s)
                run = GithubCheckRun(
                    name=r.get("name") or "",
                    status=r.get("status") or "",
                    conclusion=r.get("conclusion") or "",
                    summary=summary,
                    url=r.get("html_url") or "",
                )
                if run.failed and annotations_for_failures and r.get("id"):
                    anns = self._request(
                        f"/repos/{self.owner}/{self.name}/check-runs/{r['id']}/annotations",
                        params={"per_page": 50})
                    run.annotations = [GithubCheckAnnotation(
                        path=a.get("path") or "",
                        start_line=a.get("start_line") or 0,
                        end_line=a.get("end_line") or 0,
                        level=a.get("annotation_level") or "",
                        message=a.get("message") or "",
                        title=a.get("title") or "",
                    ) for a in (anns if isinstance(anns, list) else [])]
                out.append(run)
            if len(items) < self._PER_PAGE:
                break
            page += 1
        return out

    def review_feedback(self, number: int, include_conversation: bool = True,
                        include_checks: bool = True) -> GithubPullFeedback:
        """Bundle the PR + its reviews, inline comments, (optionally) the
        conversation comments, and (optionally) the CI check results into one
        :class:`GithubPullFeedback`. A PR with no human feedback but FAILING CI
        is NOT ``is_empty()`` — red CI counts as feedback to act on."""
        pull = self.get_pull(number)
        return GithubPullFeedback(
            pull=pull,
            reviews=self.reviews(number),
            review_comments=self.review_comments(number),
            conversation_comments=self.conversation_comments(number) if include_conversation else [],
            check_runs=self.check_runs(number, head_sha=pull.head_sha) if include_checks else [],
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _build_pull(p: dict) -> GithubPull:
        return GithubPull(
            number=p["number"],
            title=p.get("title") or "",
            state=p.get("state") or "",
            author=(p.get("user") or {}).get("login") or "",
            body=p.get("body") or "",
            base_ref=(p.get("base") or {}).get("ref") or "",
            head_ref=(p.get("head") or {}).get("ref") or "",
            head_sha=(p.get("head") or {}).get("sha") or "",
            draft=bool(p.get("draft")),
            # The /pulls LIST payload omits the computed `merged` flag but does
            # carry `merged_at`, so derive: merged == merged_at set.
            merged=bool(p.get("merged")) or p.get("merged_at") is not None,
            url=p.get("html_url") or "",
        )
