"""Shared read-only GitHub REST client plumbing.

Base class + typed exceptions used by the repo-bound nodes
(:class:`~chia.github.github_issues_node.GithubIssuesNode` and
:class:`~chia.github.github_pulls_node.GithubPullsNode`). Bind to one repo at
construction; all calls are synchronous and raise the typed exceptions below on
failure (no in-band ``success: bool``). Head-node only — not a Ray task.
"""

from __future__ import annotations

import logging
import os
import re
import time
from typing import Any

import requests


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class GithubError(Exception):
    """Base class for all GitHub client errors."""


class GithubAuthError(GithubError):
    """401 Unauthorized — missing or bad token for a private resource."""


class GithubRateLimitError(GithubError):
    """403 with X-RateLimit-Remaining: 0. ``reset_time`` is a unix timestamp."""

    def __init__(self, reset_time: int, message: str = ""):
        self.reset_time = reset_time
        super().__init__(message or f"GitHub rate limit hit; resets at unix={reset_time}")


class GithubNotFoundError(GithubError):
    """404 — repo, issue, or pull request not found."""


class GithubRequestError(GithubError):
    """422 or other 4xx that doesn't fall into auth / rate-limit / not-found."""


class GithubServerError(GithubError):
    """5xx after one retry, or a network timeout."""


# ---------------------------------------------------------------------------
# Base client
# ---------------------------------------------------------------------------

_REPO_RE = re.compile(
    r"^(?:https?://github\.com/)?([^/\s]+)/([^/\s]+?)(?:\.git)?/?$"
)


class GithubClient:
    """Repo-bound read-only GitHub REST client.

    Holds the session, auth header, and the retrying :meth:`_request` helper.
    Subclasses add resource-specific read methods (issues, pulls, ...). Pass a
    ``token`` or set ``GITHUB_TOKEN`` in the environment for private repos and a
    higher rate limit.
    """

    logging_name = "GithubClient"
    _API_ROOT = "https://api.github.com"
    _PER_PAGE = 100  # max allowed by the GitHub REST API
    _USER_AGENT = "chia-github-client"

    def __init__(
        self,
        repo: str,
        token: str | None = None,
        timeout_seconds: int = 30,
        logging_level: int = logging.DEBUG,
    ):
        self.owner, self.name = self._parse_repo(repo)
        self.timeout_seconds = timeout_seconds
        self.token = token if token is not None else os.environ.get("GITHUB_TOKEN")

        self.logger = logging.getLogger(self.logging_name)
        self.logger.setLevel(logging_level)

        self._session = requests.Session()
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": self._USER_AGENT,
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        self._session.headers.update(headers)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_repo(repo: str) -> tuple[str, str]:
        m = _REPO_RE.match(repo.strip())
        if not m:
            raise ValueError(
                f"repo must be 'owner/name' or a github.com URL, got {repo!r}"
            )
        return m.group(1), m.group(2)

    def _paginate(self, path: str, params: dict | None = None) -> list:
        """GET all pages of a list endpoint (per_page=100), concatenated."""
        params = dict(params or {})
        params.setdefault("per_page", self._PER_PAGE)
        out: list = []
        page = 1
        while True:
            params["page"] = page
            items = self._request(path, params=params)
            if not isinstance(items, list) or not items:
                break
            out.extend(items)
            if len(items) < self._PER_PAGE:
                break
            page += 1
        return out

    def _request(self, path: str, params: dict | None = None,
                 accept: str | None = None) -> Any:
        """GET ``{API_ROOT}{path}``, retrying once on 5xx, raising typed errors.

        ``accept`` overrides the Accept header for this one request and switches
        the return to the RAW response text (e.g. ``application/vnd.github.diff``
        for a PR's unified diff). Default: GitHub's JSON media type, parsed.
        """
        url = f"{self._API_ROOT}{path}"
        attempts = 2  # one retry on 5xx / network timeout
        for attempt in range(attempts):
            try:
                resp = self._session.get(url, params=params, timeout=self.timeout_seconds,
                                         headers={"Accept": accept} if accept else None)
            except requests.Timeout as exc:
                if attempt + 1 < attempts:
                    self.logger.warning("Timeout on %s (attempt %d), retrying", url, attempt + 1)
                    time.sleep(2)
                    continue
                raise GithubServerError(f"Timeout after {self.timeout_seconds}s on {url}") from exc
            except requests.RequestException as exc:
                raise GithubServerError(f"Network error on {url}: {exc}") from exc

            status = resp.status_code
            self.logger.debug("GET %s → %d", url, status)

            if status == 200:
                return resp.text if accept else resp.json()

            if status == 401:
                raise GithubAuthError(self._error_message(resp))

            if status == 403:
                remaining = resp.headers.get("X-RateLimit-Remaining")
                if remaining == "0":
                    reset = int(resp.headers.get("X-RateLimit-Reset", "0"))
                    raise GithubRateLimitError(reset_time=reset, message=self._error_message(resp))
                raise GithubRequestError(self._error_message(resp))

            if status == 404:
                raise GithubNotFoundError(self._error_message(resp))

            if 500 <= status < 600:
                if attempt + 1 < attempts:
                    self.logger.warning("Server %d on %s (attempt %d), retrying", status, url, attempt + 1)
                    time.sleep(2)
                    continue
                raise GithubServerError(self._error_message(resp))

            # 4xx other than 401/403/404 (e.g. 422)
            raise GithubRequestError(self._error_message(resp))

        # Unreachable: every branch above either returns or raises.
        raise GithubServerError(f"Exhausted retries for {url}")

    @staticmethod
    def _error_message(resp: requests.Response) -> str:
        try:
            data = resp.json()
            msg = data.get("message") if isinstance(data, dict) else None
        except ValueError:
            msg = None
        return f"{resp.status_code} {resp.reason} for {resp.url}" + (f": {msg}" if msg else "")
