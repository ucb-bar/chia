from dataclasses import dataclass, field


@dataclass
class GithubComment:
    author: str       # commenter login
    created_at: str   # verbatim from GitHub
    body: str         # comment markdown body


@dataclass
class GithubIssue:
    number: int                                  # primary key; pass back as `after` for pagination
    title: str
    state: str                                   # "open" | "closed"
    author: str                                  # opener login
    created_at: str                              # strings verbatim — no datetime parsing
    updated_at: str
    closed_at: str | None
    body: str                                    # issue markdown body (may be empty)
    labels: list[str]                            # label names only
    url: str                                     # html_url
    is_pull_request: bool                        # True if GitHub returned a non-null pull_request field
    comments: list[GithubComment] = field(default_factory=list)

    def to_markdown(self) -> str:
        """Render the issue + comments as one LLM-ready text blob."""
        kind = "PR" if self.is_pull_request else "Issue"
        lines: list[str] = [
            f"# {kind} #{self.number}: {self.title}",
            "",
            f"- State: {self.state}",
            f"- Author: {self.author}",
            f"- Created: {self.created_at}",
            f"- Updated: {self.updated_at}",
        ]
        if self.closed_at is not None:
            lines.append(f"- Closed: {self.closed_at}")
        if self.labels:
            lines.append(f"- Labels: {', '.join(self.labels)}")
        lines.append(f"- URL: {self.url}")
        lines += ["", "## Body", "", self.body if self.body else "_(no body)_"]
        if self.comments:
            lines += ["", f"## Comments ({len(self.comments)})"]
            for c in self.comments:
                lines += [
                    "",
                    f"### Comment by {c.author} — {c.created_at}",
                    "",
                    c.body if c.body else "_(empty comment)_",
                ]
        return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Pull-request review state (see chia.github.github_pulls_node)
# ---------------------------------------------------------------------------

@dataclass
class GithubReview:
    """A submitted review on a PR (the summary, not the inline comments)."""
    author: str
    state: str          # APPROVED | CHANGES_REQUESTED | COMMENTED | DISMISSED | PENDING
    body: str           # the review's top-level markdown (may be empty)
    submitted_at: str   # verbatim


@dataclass
class GithubReviewComment:
    """An inline (line-anchored) review comment on a PR's diff."""
    author: str
    path: str           # file the comment is anchored to
    line: int | None    # line in the file (head side); None if outdated
    body: str           # comment markdown
    diff_hunk: str      # the diff context GitHub shows above the comment
    in_reply_to: int | None  # parent comment id if this is a threaded reply
    created_at: str
    id: int             # comment id (stable handle for replies)


@dataclass
class GithubPull:
    """A pull request's metadata (separate from issue/PR-as-issue)."""
    number: int
    title: str
    state: str          # "open" | "closed"
    author: str
    body: str
    base_ref: str       # branch the PR merges INTO
    head_ref: str       # the PR's source branch
    head_sha: str
    draft: bool
    merged: bool
    url: str            # html_url


@dataclass
class GithubCheckAnnotation:
    """A file/line-anchored message a CI check attached to its run."""
    path: str
    start_line: int
    end_line: int
    level: str          # notice | warning | failure
    message: str
    title: str = ""


@dataclass
class GithubCheckRun:
    """One CI check's latest result on a PR's head commit."""
    name: str
    status: str         # queued | in_progress | completed
    conclusion: str     # success | failure | neutral | cancelled | skipped | timed_out | action_required ("" while running)
    summary: str        # output title + summary excerpt (may be empty)
    url: str            # html_url of the run
    annotations: list[GithubCheckAnnotation] = field(default_factory=list)

    _FAILED = ("failure", "timed_out")

    @property
    def failed(self) -> bool:
        return self.conclusion in self._FAILED


@dataclass
class GithubPullFeedback:
    """Everything a PR author needs to act on a review round: the PR plus its
    reviews, inline review comments, general conversation comments, and the CI
    check results on its head commit."""
    pull: GithubPull
    reviews: list[GithubReview] = field(default_factory=list)
    review_comments: list[GithubReviewComment] = field(default_factory=list)
    conversation_comments: list[GithubComment] = field(default_factory=list)
    check_runs: list[GithubCheckRun] = field(default_factory=list)

    def failed_checks(self) -> list[GithubCheckRun]:
        return [c for c in self.check_runs if c.failed]

    def is_empty(self) -> bool:
        """True if there is nothing to act on: no reviews/comments AND no
        failing CI checks (green/neutral CI alone is not feedback)."""
        return not (self.reviews or self.review_comments
                    or self.conversation_comments or self.failed_checks())

    def to_markdown(self) -> str:
        """Render the review feedback as one LLM-ready, reviewer-style blob.

        Inline comments are numbered [C1], [C2], ... so an author can reference
        them when replying. Each shows its file, line, the diff hunk it anchors
        to, and the reviewer's text.
        """
        p = self.pull
        lines: list[str] = [
            f"# Review feedback on PR #{p.number}: {p.title}",
            "",
            f"- Base: {p.base_ref}  <-  Head: {p.head_ref} ({p.head_sha[:10]})",
            f"- URL: {p.url}",
        ]
        if self.reviews:
            lines += ["", "## Review summaries"]
            for r in self.reviews:
                lines += [
                    "",
                    f"### {r.author} — {r.state}" + (f" ({r.submitted_at})" if r.submitted_at else ""),
                    "",
                    r.body if r.body else "_(no summary text)_",
                ]
        if self.review_comments:
            lines += ["", f"## Inline comments ({len(self.review_comments)})"]
            for i, c in enumerate(self.review_comments, 1):
                tag = f"[C{i}] {c.path}" + (f":{c.line}" if c.line else "")
                if c.in_reply_to:
                    tag += "  (reply in thread)"
                lines += ["", f"### {tag} — @{c.author}"]
                if c.diff_hunk:
                    lines += ["", "```diff", c.diff_hunk.strip("\n"), "```"]
                lines += ["", c.body if c.body else "_(empty comment)_"]
        if self.conversation_comments:
            lines += ["", f"## Conversation comments ({len(self.conversation_comments)})"]
            for c in self.conversation_comments:
                lines += ["", f"### @{c.author} — {c.created_at}", "",
                          c.body if c.body else "_(empty comment)_"]
        if self.check_runs:
            failed = self.failed_checks()
            ok = sum(1 for c in self.check_runs if c.conclusion == "success")
            other = len(self.check_runs) - ok - len(failed)
            lines += ["", f"## CI status ({len(self.check_runs)} checks: "
                          f"{ok} success, {len(failed)} FAILED, {other} other)"]
            for c in self.check_runs:
                if not c.failed:
                    lines += [f"- [{c.conclusion or c.status}] {c.name}"]
            for c in failed:
                lines += ["", f"### FAILED: {c.name} ({c.conclusion})", f"- run: {c.url}"]
                if c.summary:
                    lines += ["", c.summary[:1500]]
                if c.annotations:
                    lines += ["", f"Annotations ({len(c.annotations)}):"]
                    for a in c.annotations[:20]:
                        loc = f"{a.path}:{a.start_line}" + (
                            f"-{a.end_line}" if a.end_line != a.start_line else "")
                        head = f" {a.title}:" if a.title else ""
                        lines += [f"- {loc} [{a.level}]{head} {a.message[:500]}"]
                    if len(c.annotations) > 20:
                        lines += [f"- (+{len(c.annotations) - 20} more annotations)"]
        return "\n".join(lines) + "\n"
