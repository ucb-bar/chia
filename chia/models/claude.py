from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import threading
from typing import TYPE_CHECKING, List, Optional
from uuid import uuid4

import ray

from chia.base.ChiaFunction import ChiaFunction, ObjectRefCallback
from chia.base.llm_call import QueryResult, LLMCallBase

if TYPE_CHECKING:
    from chia.base.tools.ChiaTool import ChiaTool


# ---------------------------------------------------------------------------
# Result type — claude-specific
# ---------------------------------------------------------------------------


@dataclass
class ClaudeCodeQueryResult(QueryResult):
    """
    Derived QueryResult specialized for the Claude Code CLI backend.

    Carries the on-disk ``<session_id>.jsonl`` transcript bytes back to the
    caller so a ``resume_session=True`` LLM can continue the same conversation
    on a different worker — written by :meth:`ClaudeCodeLLM._capture_transcript`
    after a CLI run and consumed by :meth:`ClaudeCodeLLM._restore_transcript`
    before the next ``--resume`` invocation.
    """

    session_transcript: Optional[bytes] = None
    session_transcript_path: Optional[str] = None


# ``CLIResult`` is the generic alias for any LLM result — kept pointing at
# ``QueryResult`` for back-compat. Use ``ClaudeCodeQueryResult`` directly when
# you need to construct or access the session-transcript fields.
CLIResult = QueryResult


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class ClaudeCodeError(Exception):
    """Base for all Claude Code CLI errors.

    Every subclass must implement ``__reduce__`` for Ray serialization.
    """

    def __init__(
        self,
        node_id: str,
        error_type: str,
        exit_code: int = -1,
        raw_message: str = "",
    ):
        self.node_id = node_id
        self.error_type = error_type
        self.exit_code = exit_code
        self.raw_message = raw_message
        super().__init__(f"{error_type} on {node_id}: {raw_message[:200]}")

    def __reduce__(self):
        return (
            self.__class__,
            (self.node_id, self.error_type, self.exit_code, self.raw_message),
        )


class RateLimitError(ClaudeCodeError):
    """Raised when the Claude CLI response indicates a usage-limit hit."""

    def __init__(
        self,
        node_id: str,
        reset_time: datetime,
        raw_message: str = "",
        exit_code: int = -1,
    ):
        self.reset_time = reset_time
        super().__init__(
            node_id=node_id,
            error_type="rate_limit",
            exit_code=exit_code,
            raw_message=raw_message,
        )

    def __reduce__(self):
        return (
            self.__class__,
            (self.node_id, self.reset_time, self.raw_message, self.exit_code),
        )


class AuthenticationError(ClaudeCodeError):
    """Raised when the CLI's auth token/API key is invalid or expired."""

    def __init__(self, node_id: str, exit_code: int = -1, raw_message: str = ""):
        super().__init__(node_id, "authentication_failed", exit_code, raw_message)

    def __reduce__(self):
        return (self.__class__, (self.node_id, self.exit_code, self.raw_message))


class BillingError(ClaudeCodeError):
    """Raised when the billing account has payment issues."""

    def __init__(self, node_id: str, exit_code: int = -1, raw_message: str = ""):
        super().__init__(node_id, "billing_error", exit_code, raw_message)

    def __reduce__(self):
        return (self.__class__, (self.node_id, self.exit_code, self.raw_message))


class InvalidRequestError(ClaudeCodeError):
    """Raised when the request is malformed (bad prompt, unsupported params, etc.)."""

    def __init__(self, node_id: str, exit_code: int = -1, raw_message: str = ""):
        super().__init__(node_id, "invalid_request", exit_code, raw_message)

    def __reduce__(self):
        return (self.__class__, (self.node_id, self.exit_code, self.raw_message))


class ServerError(ClaudeCodeError):
    """Raised when Anthropic's API returns a server-side error (500/503)."""

    def __init__(
        self,
        node_id: str,
        exit_code: int = -1,
        raw_message: str = "",
        retry_after: Optional[int] = None,
    ):
        self.retry_after = retry_after
        super().__init__(node_id, "server_error", exit_code, raw_message)

    def __reduce__(self):
        return (
            self.__class__,
            (self.node_id, self.exit_code, self.raw_message, self.retry_after),
        )


class MaxOutputTokensError(ClaudeCodeError):
    """Raised when the LLM's response was truncated by the output token limit."""

    def __init__(
        self,
        node_id: str,
        exit_code: int = -1,
        raw_message: str = "",
        partial_text: str = "",
    ):
        self.partial_text = partial_text
        super().__init__(node_id, "max_output_tokens", exit_code, raw_message)

    def __reduce__(self):
        return (
            self.__class__,
            (self.node_id, self.exit_code, self.raw_message, self.partial_text),
        )


class UnknownClaudeError(ClaudeCodeError):
    """Raised for unclassified CLI errors."""

    def __init__(
        self,
        node_id: str,
        exit_code: int = -1,
        raw_message: str = "",
        stderr: str = "",
    ):
        self.stderr = stderr
        super().__init__(node_id, "unknown", exit_code, raw_message)

    def __reduce__(self):
        return (
            self.__class__,
            (self.node_id, self.exit_code, self.raw_message, self.stderr),
        )


# ---------------------------------------------------------------------------
# Rate-limit text parser
# ---------------------------------------------------------------------------

_RATE_LIMIT_RE = re.compile(
    r"You've hit your limit\s*[·•\-—]\s*resets?\s+(\d{1,2})\s*(am|pm)\s*\(([^)]+)\)",
    re.IGNORECASE,
)


def parse_rate_limit_reset(text: str) -> Optional[datetime]:
    """Parse a Claude rate-limit message and return the UTC reset time.

    Expected format: ``"You've hit your limit · resets 4pm (America/Los_Angeles)"``

    Returns ``None`` when no rate-limit message is found.
    """
    m = _RATE_LIMIT_RE.search(text)
    if m is None:
        return None

    hour = int(m.group(1))
    ampm = m.group(2).lower()
    tz_str = m.group(3).strip()

    # Convert 12-hour → 24-hour
    if ampm == "pm" and hour != 12:
        hour += 12
    elif ampm == "am" and hour == 12:
        hour = 0

    # Resolve timezone
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(tz_str)
    except Exception:
        # Fallback: common abbreviations
        _abbrev = {
            "PST": -8, "PDT": -7, "MST": -7, "MDT": -6,
            "CST": -6, "CDT": -5, "EST": -5, "EDT": -4,
            "UTC": 0, "GMT": 0,
        }
        offset_hours = _abbrev.get(tz_str.upper(), 0)
        tz = timezone(timedelta(hours=offset_hours))

    now_in_tz = datetime.now(tz)
    reset_local = now_in_tz.replace(hour=hour, minute=0, second=0, microsecond=0)

    # If reset hour is in the past, it means tomorrow
    if reset_local <= now_in_tz:
        reset_local += timedelta(days=1)

    return reset_local.astimezone(timezone.utc)


def parse_rate_limit_event(event: dict) -> Optional[datetime]:
    """Parse a ``rate_limit_event`` JSON object and return the UTC reset time.

    Only triggers when ``rate_limit_info.status`` is ``"rejected"`` — the event
    is also emitted with other statuses as an informational notice, which should
    NOT be treated as a rate limit.
    """
    info = event.get("rate_limit_info", {})
    if info.get("status") != "rejected":
        return None
    resets_at = info.get("resetsAt")
    if resets_at is None:
        return None
    try:
        return datetime.fromtimestamp(int(resets_at), tz=timezone.utc)
    except (ValueError, TypeError, OSError):
        return None


# ---------------------------------------------------------------------------
# Session-tracking decorator
# ---------------------------------------------------------------------------


def _session_tracked(chia_fn):
    """Stack ABOVE ``@ChiaFunction`` on ``prompt`` to auto-attach transcript sync.

    Remote dispatch still targets the inner ``@ChiaFunction`` (the CLI runs on a
    worker). This outer layer only changes what ``chia_remote`` *returns*: when
    the instance is resuming (``resume_session=True`` -> ``_session_id`` set) it
    wraps the ObjectRef in an :class:`ObjectRefCallback` carrying
    ``_sync_transcript``, so ``get(ref)`` harvests the transcript onto the
    instance with no explicit ``callback=``. When not resuming it returns the
    plain ObjectRef unchanged.

    Dispatch stays async (the ObjectRef/ObjectRefCallback returns immediately).
    ``instance.prompt(...)`` (local) is unchanged; ``ClaudeCodeLLM.prompt``
    (class access) exposes the raw inner ``@ChiaFunction``.
    """

    def _wrap(instance, ref):
        if instance._session_id is not None:
            return ObjectRefCallback(ref, instance._sync_transcript)
        return ref

    class _TrackedHandle:
        def __init__(self, inner_handle, instance):
            self._inner = inner_handle
            self._instance = instance

        def chia_remote(self, *args, **kwargs):
            return _wrap(self._instance, self._inner.chia_remote(*args, **kwargs))

        def remote(self, *args, **kwargs):
            return self.chia_remote(*args, **kwargs)

    class _BoundTracked:
        def __init__(self, instance):
            self._instance = instance

        def __call__(self, *args, **kwargs):
            # Local, in-process call — prompt() captures the transcript onto
            # self directly (it runs here), so no wrapping is needed.
            return chia_fn(self._instance, *args, **kwargs)

        def chia_remote(self, *args, **kwargs):
            return _wrap(self._instance, chia_fn.chia_remote(*args, **kwargs))

        def options(self, **opts):
            return _TrackedHandle(chia_fn.options(**opts), self._instance)

        def __getattr__(self, name):
            return getattr(chia_fn, name)

    class _TrackedDescriptor:
        def __get__(self, obj, objtype=None):
            if obj is None:
                return chia_fn  # class access -> raw ChiaFunction
            return _BoundTracked(obj)

        def __getattr__(self, name):
            return getattr(chia_fn, name)

    return _TrackedDescriptor()


class ClaudeCodeLLM(LLMCallBase):
    """Wraps the Claude Code CLI (``claude --print``) as an LLM backend.

    Each call to :meth:`prompt` spawns a ``claude`` subprocess that can
    optionally connect to MCP tool servers (e.g. :class:`BashTool`).
    """

    def __init__(
        self,
        model: str = "claude-sonnet-4-6",
        system_message: str = "",
        timeout_seconds: int = 600,
        retries: int = 3,
        logging_name: str = "claude_code",
        logging_level: int = logging.DEBUG,
        log_dir: Optional[str] = None,
        resume_session: bool = False,
        projects_cwd: Optional[str] = "/home/ray/.claude/projects/-home-ray-llm-env",
        extra_cli_args: Optional[List[str]] = None,
        log_stream: bool = True,
        log_all: bool = False,
        backend: str = "cli",
        api_key: Optional[str] = None,
        max_tokens: int = 16000,
        thinking: Optional[str] = "adaptive",
        max_tool_iterations: int = 100,
    ):
        super().__init__(system_message=system_message)
        self.logging_level = logging_level
        self.logging_name = logging_name
        self.retries = retries
        self.timeout_seconds = timeout_seconds
        self.model = model
        self.extra_cli_args = extra_cli_args or []
        self.logger = logging.getLogger(logging_name)
        self.log_stream = log_stream
        self.log_all = log_all

        # Backend selection: "cli" (default, the ``claude --print`` subprocess)
        # or "api" (the Anthropic Python SDK; see the Anthropic API backend
        # section below).
        if backend not in ("cli", "api"):
            raise ValueError(f"backend must be 'cli' or 'api', got {backend!r}")
        self.backend = backend
        self.api_key = api_key
        self.max_tokens = max_tokens
        self.thinking = thinking
        self.max_tool_iterations = max_tool_iterations

        # The CLI backend ignores the API-only parameters; warn if any were
        # set away from their defaults so a misdirected config doesn't pass
        # silently.
        if self.backend == "cli":
            ignored = [
                name
                for name, value, default in (
                    ("api_key", api_key, None),
                    ("max_tokens", max_tokens, 16000),
                    ("thinking", thinking, "adaptive"),
                    ("max_tool_iterations", max_tool_iterations, 100),
                )
                if value != default
            ]
            if ignored:
                self.logger.warning(
                    "backend='cli' ignores API-only parameter(s): %s",
                    ", ".join(ignored),
                )
        # NOTE: the "api" backend has so far only been exercised by the tests
        # in chia/models/tests/test_claude_api.py (mocked unit tests, plus the
        # opt-in live tests). It has NOT been validated in production use —
        # treat it as experimental until it has real mileage.
        elif self.backend == "api":
            self.logger.warning(
                "ClaudeCodeLLM backend='api' is experimental: it has only been "
                "exercised by unit tests so far, not validated in production."
            )

        self._call_counter = 0
        self._session_id = str(uuid4()) if resume_session else None
        self._last_metadata: dict = {}  # populated by _process_event_line
        self._rate_limit_event: Optional[dict] = None  # populated by _process_event_line

        self._projects_cwd = projects_cwd
        self._session_transcript: Optional[bytes] = None
        self._session_transcript_path: Optional[str] = None

        self._log_dir = log_dir
        if log_dir is not None:
            os.makedirs(log_dir, exist_ok=True)
            session_tag = f"_{self._session_id[:8]}" if self._session_id else ""
            run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
            self._log_prefix = os.path.join(log_dir, f"{logging_name}_{run_id}{session_tag}")
        else:
            self._log_prefix = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @_session_tracked
    @ChiaFunction(resources={"claude_creds": 0.01})
    def prompt(
        self,
        user_message: str,
        tools: Optional[List[ChiaTool]] = [],
    ) -> ClaudeCodeQueryResult:
        """Send *user_message* to Claude Code CLI and return the response.

        Returns:
            :class:`ClaudeCodeQueryResult` with ``success=True`` when the CLI ran cleanly,
            or ``success=False`` when every retry attempt failed (in which
            case ``result`` is empty and ``returncode`` is ``-1``).

        Raises:
            RateLimitError: Usage limit hit — propagates immediately.
            AuthenticationError: Auth failure — propagates immediately.
            BillingError: Billing/payment issue — propagates immediately.
            InvalidRequestError: Malformed request — propagates immediately.
            ServerError: After all retries with exponential backoff.
            MaxOutputTokensError: After one retry attempt.
        """
        import time as _time

        from chia.trace.profiler import get_profiler

        profiler = get_profiler()

        for attempt in range(self.retries):
            try:
                self._last_metadata = {}
                self._rate_limit_event = None
                # Paste any carried transcript onto this machine so a --resume
                # run finds the conversation, regardless of which worker the
                # previous call landed on. No-op when not resuming.
                self._restore_transcript()
                if self.backend == "api":
                    cli = self._run_api(user_message, tools)
                elif self.log_stream:
                    cli = self._run_claude_streaming(user_message, tools)
                else:
                    cli = self._run_claude(user_message, tools)
                self._call_counter += 1
                self._last_metadata["model"] = self.model
                self._last_metadata["tools"] = [
                    {"name": t.name, "hostname": getattr(t, "hostname", None),
                     "port": getattr(t, "port", None),
                     "node_id": getattr(t, "node_id", None)}
                    for t in tools
                ]

                if profiler.enabled and self._last_metadata:
                    profiler.add_info(self._last_metadata)

                # Classify and raise typed errors
                self._classify_error(cli)

                # Read the freshly-written transcript back into memory (and
                # onto the ClaudeCodeQueryResult) so the caller can resume on another worker.
                self._capture_transcript(cli)

                cli.success = True
                return cli

            # -- Never retry: propagate immediately --
            except (RateLimitError, AuthenticationError, BillingError, InvalidRequestError):
                raise

            # -- Retry once: stochastic generation may produce shorter output --
            except MaxOutputTokensError:
                if attempt == 0:
                    self.logger.warning(
                        "Max output tokens on attempt %d/%d, retrying once",
                        attempt + 1, self.retries,
                    )
                    continue
                raise

            # -- Retry with exponential backoff: transient API issue --
            except ServerError:
                backoff = min(5 * 2 ** attempt, 60)
                self.logger.warning(
                    "Server error on attempt %d/%d, backing off %ds",
                    attempt + 1, self.retries, backoff,
                )
                _time.sleep(backoff)

            # -- Standard retry for unknown errors --
            except UnknownClaudeError as exc:
                self.logger.warning(
                    "Unknown error on attempt %d/%d: %s",
                    attempt + 1, self.retries, exc,
                )

            except subprocess.TimeoutExpired:
                # A timeout means the session was likely created;
                # switch to --resume for subsequent attempts.
                if self._session_id is not None and self._call_counter == 0:
                    self._call_counter = 1
                self.logger.warning(
                    "Timeout on attempt %d/%d", attempt + 1, self.retries,
                )

            except Exception as exc:
                self.logger.warning(
                    "Unexpected error on attempt %d/%d: %s",
                    attempt + 1, self.retries, exc,
                )
        return ClaudeCodeQueryResult(result="", returncode=-1, stderr="", stream_result="", success=False)

    def _sync_transcript(self, cli: ClaudeCodeQueryResult) -> ClaudeCodeQueryResult:
        """Copy a worker-captured transcript off *cli* onto this instance.

        When ``resume_session=True``, ``prompt.chia_remote`` returns an
        :class:`ObjectRefCallback` carrying this method, so ``get(ref)`` runs it
        automatically — the caller doesn't pass a ``callback``::

            cli = get(llm.prompt.chia_remote(llm, msg))   # auto-syncs transcript

        The remote worker's own ``self`` mutations are discarded, so the
        transcript must be harvested locally (in the process that calls
        ``get``). A follow-up call then resumes the same session even if it
        lands on a different worker. Guarded so a transcript-less result (api
        backend / error path) doesn't clobber a prior capture, and a no-op when
        ``resume_session`` is False. Returns *cli* (so it's a pass-through
        callback).
        """
        if self._session_id is not None and cli.session_transcript is not None:
            self._session_transcript = cli.session_transcript
            self._session_transcript_path = cli.session_transcript_path
        return cli

    def _get_node_id(self) -> str:
        try:
            return ray.get_runtime_context().get_node_id()
        except Exception:
            return "unknown"

    def _classify_error(self, cli: ClaudeCodeQueryResult) -> None:
        """Inspect *cli* and raise a typed error if something went wrong.

        Check order:
        1. Rate limit (text regex + streaming event) — highest priority
        2. Non-zero exit code — classify by stderr patterns
        3. Success — return without raising
        """
        # --- 1. Rate limit (can appear even with exit code 0) ---
        reset_time = parse_rate_limit_reset(cli.result)
        if reset_time is None and self._rate_limit_event is not None:
            reset_time = parse_rate_limit_event(self._rate_limit_event)

        if reset_time is not None:
            node_id = self._get_node_id()
            self.logger.warning(
                "Rate limit detected on %s (resets %s). Response text:\n%s",
                node_id, reset_time.isoformat(), cli.result,
            )
            raise RateLimitError(
                node_id=node_id,
                reset_time=reset_time,
                raw_message=cli.result[:300],
                exit_code=cli.returncode,
            )

        # --- 2. Non-zero exit code → classify by stderr ---
        if cli.returncode != 0:
            node_id = self._get_node_id()
            stderr_lower = cli.stderr.lower()

            if any(kw in stderr_lower for kw in (
                "authentication", "unauthorized", "401", "not authenticated",
                "login", "auth token",
            )):
                raise AuthenticationError(
                    node_id=node_id,
                    exit_code=cli.returncode,
                    raw_message=cli.stderr[:300],
                )

            if any(kw in stderr_lower for kw in (
                "billing", "payment", "402", "overdue", "subscription",
                "plan expired",
            )):
                raise BillingError(
                    node_id=node_id,
                    exit_code=cli.returncode,
                    raw_message=cli.stderr[:300],
                )

            if any(kw in stderr_lower for kw in (
                "invalid request", "malformed", "400", "bad request",
                "invalid model",
            )):
                raise InvalidRequestError(
                    node_id=node_id,
                    exit_code=cli.returncode,
                    raw_message=cli.stderr[:300],
                )

            if any(kw in stderr_lower for kw in (
                "500", "503", "server error", "overloaded",
                "internal error", "service unavailable",
            )):
                raise ServerError(
                    node_id=node_id,
                    exit_code=cli.returncode,
                    raw_message=cli.stderr[:300],
                )

            if any(kw in stderr_lower for kw in (
                "max_output_tokens", "output token limit", "maximum output",
                "response too long",
            )):
                raise MaxOutputTokensError(
                    node_id=node_id,
                    exit_code=cli.returncode,
                    raw_message=cli.stderr[:300],
                    partial_text=cli.result,
                )

            # Fallback: unknown error
            raise UnknownClaudeError(
                node_id=node_id,
                exit_code=cli.returncode,
                raw_message=cli.stderr[:300],
                stderr=cli.stderr,
            )

    # ------------------------------------------------------------------
    # Session transcript (CLI backend, resume across machines)
    # ------------------------------------------------------------------
    #
    # The Claude Code CLI persists each session to
    # ``<projects_dir>/<session_id>.jsonl`` where ``<projects_dir>`` is
    # derived from the CLI process CWD (leading "/" dropped, remaining "/"
    # turned into "-") under ``~/.claude/projects/``. Because :meth:`prompt`
    # may be scheduled on a different ``claude_creds`` worker each call, the
    # on-disk transcript does not follow the conversation. We therefore carry
    # the bytes in-process (``self._session_transcript``, also surfaced on
    # :class:`ClaudeCodeQueryResult`) and re-paste them before a ``--resume`` run.

    def _resolve_projects_dir(self) -> str:
        """Directory holding ``<session_id>.jsonl`` on the current machine."""
        if self._projects_cwd:
            return self._projects_cwd
        # The CLI escapes every non-alphanumeric char in its CWD to "-".
        escaped = re.sub(r"[^a-zA-Z0-9]", "-", os.getcwd())
        return os.path.join(os.path.expanduser("~"), ".claude", "projects", escaped)

    def _transcript_path(self) -> Optional[str]:
        """Full path to this session's transcript, or None when not resuming."""
        if self._session_id is None:
            return None
        return os.path.join(self._resolve_projects_dir(), f"{self._session_id}.jsonl")

    def _restore_transcript(self) -> None:
        """Paste a carried transcript onto this machine before a resume run.

        Unconditional: overwrites any existing file so the resumed session
        always reflects the conversation we hold. Forces ``--resume`` semantics
        even when this object/worker has never run the CLI itself. No-op unless
        ``resume_session=True`` and a transcript has been captured.
        """
        if self._session_id is None or self._session_transcript is None:
            return
        path = self._transcript_path()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as fh:
            fh.write(self._session_transcript)
        self._session_transcript_path = path
        if self._call_counter == 0:
            self._call_counter = 1

    def _capture_transcript(self, cli: ClaudeCodeQueryResult) -> None:
        """Read the on-disk transcript after a run into memory and onto *cli*.

        No-op unless ``resume_session=True`` and the CLI actually wrote a
        transcript (the ``api`` backend keeps history in memory and writes
        nothing, so this leaves the state untouched there).
        """
        if self._session_id is None:
            return
        path = self._transcript_path()
        if path and os.path.exists(path):
            with open(path, "rb") as fh:
                data = fh.read()
            self._session_transcript = data
            self._session_transcript_path = path
            cli.session_transcript = data
            cli.session_transcript_path = path

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _build_mcp_config(self, tools: List[ChiaTool]) -> dict:
        """Build the JSON object expected by ``--mcp-config``."""
        servers = {}
        for tool in tools:
            port = getattr(tool, "port", 8000)
            servers[tool.name] = {
                "type": "http",
                "url": f"http://{tool.hostname}:{port}/{tool.name}/mcp",
            }
        return {"mcpServers": servers}

    def _build_allowed_tools(self, tools: List[ChiaTool]) -> List[str]:
        """Return ``--allowedTools`` entries for every registered MCP tool."""
        allowed: list[str] = []
        for tool in tools:
            # FastMCP registers tools under server_name; the MCP tool ID
            # that Claude Code recognises is  mcp__<server>__<tool_name>.
            for fn_info in tool.mcp._tool_manager.list_tools():
                allowed.append(f"mcp__{tool.name}__{fn_info.name}")
        return allowed

    def _build_cmd(self, tools: Optional[List[ChiaTool]] = None) -> list:
        """Build the ``claude`` CLI command list.

        The user message is piped via stdin (``-p -``) to avoid OS
        argument-length limits with long prompts.
        """
        cmd = [
            "claude",
            "--print",
            "--model", self.model,
            "--dangerously-skip-permissions",
        ]

        if self.extra_cli_args:
            cmd += self.extra_cli_args

        if self.log_stream:
            cmd += ["--output-format", "stream-json", "--verbose"]

        if self.system_message:
            cmd += ["--system-prompt", self.system_message]

        if self._session_id is not None:
            if self._call_counter > 0:
                cmd += ["--resume", self._session_id]
            else:
                cmd += ["--session-id", self._session_id]

        if tools:
            cfg = self._build_mcp_config(tools)
            tmp = tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", delete=False
            )
            json.dump(cfg, tmp)
            tmp.close()
            cmd += ["--mcp-config", tmp.name]

            allowed = self._build_allowed_tools(tools)
            if allowed:
                cmd += ["--allowedTools", ",".join(allowed)]

        cmd += ["-p", "-"]
        return cmd

    def _run_claude(
        self,
        user_message: str,
        tools: Optional[List[ChiaTool]] = None,
    ) -> ClaudeCodeQueryResult:
        """Run claude with simple capture (no event streaming)."""
        cmd = self._build_cmd(tools)
        self.logger.info("Running: %s", " ".join(cmd[:6]) + " ...")
        env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}

        result = subprocess.run(
            cmd,
            input=user_message,
            capture_output=True,
            text=True,
            timeout=self.timeout_seconds,
            env=env,
        )

        if result.returncode != 0:
            self.logger.warning("claude exited %d: %s", result.returncode, result.stderr[:500])

        if self._log_prefix is not None:
            truncated = user_message[:500] + ("..." if len(user_message) > 500 else "")
            with open(f"{self._log_prefix}.log", "a") as f:
                f.write("=" * 80 + "\n")
                f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Prompt #{self._call_counter}\n")
                f.write("=" * 80 + "\n\n")
                f.write(f"[User Message]\n{truncated}\n\n")
                f.write(f"[Response]\n{result.stdout}\n\n")
                f.write("-" * 80 + "\n\n")

        return ClaudeCodeQueryResult(
            result=result.stdout,
            returncode=result.returncode,
            stderr=result.stderr,
            stream_result="",
        )

    # ------------------------------------------------------------------
    # Streaming log implementation
    # ------------------------------------------------------------------

    def _run_claude_streaming(
        self,
        user_message: str,
        tools: Optional[List[ChiaTool]] = None,
    ) -> ClaudeCodeQueryResult:
        """Run claude with ``--output-format stream-json``.

        Stdout (NDJSON events) is parsed by ``_process_event_line`` into
        structured entries.  Stderr lines are captured with a ``[stderr]``
        prefix.  A lock serialises writes from the two drain threads.

        Every parsed entry is appended to an in-memory ``stream_result``
        buffer (returned on ``ClaudeCodeQueryResult``) so callers can surface the
        event trace — tool calls, thinking, metadata — directly.  When
        ``log_dir`` was set on the constructor, the same entries are
        also mirrored to ``<prefix>.log`` on disk.
        """
        cmd = self._build_cmd(tools)
        self.logger.info("Running: %s", " ".join(cmd[:6]) + " ...")
        env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}

        result_text_parts: list[str] = []
        stderr_parts: list[str] = []
        stream_parts: list[str] = []
        lock = threading.Lock()

        class _TeeWriter:
            """File-like wrapper that always mirrors to *accumulator* and
            optionally writes through to *file* when one is provided."""
            def __init__(self, file, accumulator: list[str]):
                self._file = file
                self._accumulator = accumulator
            def write(self, s: str):
                if self._file is not None:
                    self._file.write(s)
                self._accumulator.append(s)
            def flush(self):
                if self._file is not None:
                    self._file.flush()
            def close(self):
                if self._file is not None:
                    self._file.close()

        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )

        proc.stdin.write(user_message)
        proc.stdin.close()

        # Only mirror the live event stream to disk when log_all is set.
        # Without log_all we still accumulate stream_parts in memory but
        # write a compact result-only entry to the log after the run.
        file_handle = (
            open(f"{self._log_prefix}.log", "a")
            if self._log_prefix is not None and self.log_all else None
        )
        log_file = _TeeWriter(file_handle, stream_parts)

        log_file.write("=" * 80 + "\n")
        log_file.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Prompt #{self._call_counter}\n")
        log_file.write("=" * 80 + "\n\n")
        truncated = user_message[:500] + ("..." if len(user_message) > 500 else "")
        log_file.write(f"[User Message]\n{truncated}\n\n")
        log_file.flush()

        def drain_stdout():
            for line in proc.stdout:
                line = line.strip()
                if line:
                    with lock:
                        self._process_event_line(line, log_file, result_text_parts)
                        log_file.flush()

        def drain_stderr():
            for line in proc.stderr:
                with lock:
                    stderr_parts.append(line)
                    log_file.write(f"[stderr] {line}")
                    log_file.flush()

        t1 = threading.Thread(target=drain_stderr)
        t2 = threading.Thread(target=drain_stdout)
        t1.start()
        t2.start()

        proc.wait()
        t1.join()
        t2.join()

        if not result_text_parts:
            log_file.write("[DEBUG] No events parsed.\n")

        log_file.write("-" * 80 + "\n\n")
        log_file.flush()
        log_file.close()

        if self._log_prefix is not None and not self.log_all:
            with open(f"{self._log_prefix}.log", "a") as f:
                f.write("=" * 80 + "\n")
                f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Prompt #{self._call_counter}\n")
                f.write("=" * 80 + "\n\n")
                f.write(f"[User Message]\n{truncated}\n\n")
                f.write(f"[Response]\n{''.join(result_text_parts)}\n\n")
                f.write("-" * 80 + "\n\n")

        if proc.returncode != 0:
            self.logger.warning("claude exited %d", proc.returncode)

        return ClaudeCodeQueryResult(
            result="".join(result_text_parts),
            returncode=proc.returncode,
            stderr="".join(stderr_parts),
            stream_result="".join(stream_parts),
        )

    def _process_event_line(self, line: str, f, result_text_parts: list) -> None:
        """Parse a single NDJSON event line and write to the log file."""
        if not line:
            return

        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            f.write(f"[UNPARSED] {line[:200]}\n")
            f.flush()
            return

        event_type = event.get("type", "")

        if event_type == "assistant":
            msg = event.get("message", {})
            for block in msg.get("content", []):
                block_type = block.get("type", "")
                if block_type == "thinking":
                    f.write("[Thinking]\n")
                    f.write(block.get("thinking", ""))
                    f.write("\n\n")
                    f.flush()
                elif block_type == "text":
                    text = block.get("text", "")
                    result_text_parts.append(text)
                    f.write("[Response]\n")
                    f.write(text)
                    f.write("\n\n")
                    f.flush()
                elif block_type == "tool_use":
                    tool_name = block.get("name", "unknown")
                    tool_input = json.dumps(block.get("input", {}))
                    if len(tool_input) > 2000:
                        tool_input = tool_input[:2000] + "\n... [truncated]"
                    f.write(f"[Tool Call: {tool_name}]\n")
                    f.write(f"Args: {tool_input}\n\n")
                    f.flush()
                elif block_type == "tool_result":
                    content = block.get("content", "")
                    if isinstance(content, list):
                        content = "\n".join(
                            c.get("text", "") for c in content
                            if isinstance(c, dict)
                        )
                    if len(content) > 2000:
                        content = content[:2000] + "\n... [truncated]"
                    f.write(f"[Tool Result]\n{content}\n\n")
                    f.flush()

        elif event_type == "user":
            # Tool results ride on user events — the CLI echoes each tool
            # reply back to the assistant as a user-turn ``tool_result``
            # block.  Capturing them here is the only way to see what the
            # assistant actually saw when it chose its next action.
            msg = event.get("message", {})
            content_blocks = msg.get("content", [])
            if isinstance(content_blocks, str):
                # Plain-text user message (initial prompt echo); skip.
                return
            for block in content_blocks:
                if not isinstance(block, dict):
                    continue
                if block.get("type") != "tool_result":
                    continue
                content = block.get("content", "")
                if isinstance(content, list):
                    content = "\n".join(
                        c.get("text", "") for c in content
                        if isinstance(c, dict)
                    )
                if len(content) > 2000:
                    content = content[:2000] + "\n... [truncated]"
                label = "Tool Result (error)" if block.get("is_error") else "Tool Result"
                f.write(f"[{label}]\n{content}\n\n")
                f.flush()

        elif event_type == "result":
            result_text = event.get("result", "")
            if result_text and not result_text_parts:
                result_text_parts.append(result_text)

            parts = []
            meta: dict = {}
            cost = event.get("total_cost_usd")
            if cost is not None:
                parts.append(f"Cost: ${cost:.4f}")
                meta["cost_usd"] = cost
            duration = event.get("duration_ms")
            if duration is not None:
                parts.append(f"Duration: {duration / 1000:.1f}s")
                meta["duration_s"] = round(duration / 1000, 2)
            turns = event.get("num_turns")
            if turns is not None:
                parts.append(f"Turns: {turns}")
                meta["num_turns"] = turns
            usage = event.get("usage", {})
            in_tok = usage.get("input_tokens", 0)
            out_tok = usage.get("output_tokens", 0)
            cc_tok = usage.get("cache_creation_input_tokens", 0)
            cr_tok = usage.get("cache_read_input_tokens", 0)
            if in_tok:
                parts.append(f"Input tokens: {in_tok}")
                meta["input_tokens"] = in_tok
            if cc_tok:
                parts.append(f"Cache creation: {cc_tok}")
                meta["cache_creation_input_tokens"] = cc_tok
            if cr_tok:
                parts.append(f"Cache read: {cr_tok}")
                meta["cache_read_input_tokens"] = cr_tok
            if out_tok:
                parts.append(f"Output tokens: {out_tok}")
                meta["output_tokens"] = out_tok
            if parts:
                f.write(f"[Metadata]\n{' | '.join(parts)}\n")
                f.flush()
            if meta:
                self._last_metadata = meta

        elif event_type == "rate_limit_event":
            self._rate_limit_event = event

        # system — skip silently

    # ==================================================================
    # Anthropic Python API backend
    # ------------------------------------------------------------------
    # Everything below is NEW: an alternative to the ``claude --print``
    # CLI path, selected with ``backend="api"`` in the constructor. It
    # calls the Anthropic Messages API (``anthropic`` SDK) directly and
    # runs the agentic tool loop client-side, executing each ChiaTool's
    # MCP server over HTTP. Returns the same :class:`ClaudeCodeQueryResult` shape as
    # the CLI path so callers don't change; ``returncode`` is synthesised
    # (0 on success). Errors are translated to the same typed exceptions
    # the CLI path raises, so :meth:`prompt`'s retry loop is unchanged.
    #
    # WARNING: this backend is experimental. It is only exercised by the
    # tests in chia/models/tests/test_claude_api.py and has not been
    # validated in production — selecting backend="api" logs a warning.
    # ==================================================================

    def _run_api(
        self,
        user_message: str,
        tools: Optional[List[ChiaTool]] = None,
    ) -> ClaudeCodeQueryResult:
        """Synchronous entry point for the API backend.

        Drives an async agent loop (MCP connections + tool execution are
        async) from :meth:`prompt`, which is synchronous.
        """
        return self._run_coroutine(self._run_api_async(user_message, tools or []))

    def _run_coroutine(self, coro):
        """Run *coro* to completion, whether or not an event loop is live.

        ``prompt`` is sync and may be invoked either from plain sync code
        or from inside a running event loop (e.g. a Ray async actor). When
        a loop is already running, ``asyncio.run`` would raise, so the
        coroutine is offloaded to a worker thread with its own loop.
        """
        import asyncio

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)

        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, coro).result()

    async def _run_api_async(
        self,
        user_message: str,
        tools: Optional[List[ChiaTool]] = None,
    ) -> ClaudeCodeQueryResult:
        """Connect to each ChiaTool's MCP server, then run the agent loop.

        The raw Messages API returns one assistant turn at a time; when it
        emits ``tool_use`` blocks we execute them against the MCP servers
        and feed ``tool_result`` blocks back, looping until the model stops
        requesting tools (or :attr:`max_tool_iterations` is reached).
        """
        import anthropic
        from contextlib import AsyncExitStack

        from mcp import ClientSession
        from mcp.client.streamable_http import streamable_http_client

        client = (
            anthropic.AsyncAnthropic(api_key=self.api_key)
            if self.api_key
            else anthropic.AsyncAnthropic()
        )

        stream_parts: list[str] = []
        stream_parts.append("=" * 80 + "\n")
        stream_parts.append(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Prompt #{self._call_counter} (api)\n"
        )
        stream_parts.append("=" * 80 + "\n\n")
        truncated = user_message[:500] + ("..." if len(user_message) > 500 else "")
        stream_parts.append(f"[User Message]\n{truncated}\n\n")

        async with AsyncExitStack() as stack:
            # --- Connect to every MCP server and gather tool schemas ---
            anthropic_tools: list[dict] = []
            dispatch: dict = {}  # api tool name -> (session, mcp tool name)
            for tool in tools or []:
                port = getattr(tool, "port", 8000)
                url = f"http://{tool.hostname}:{port}/{tool.name}/mcp"
                transport = await stack.enter_async_context(streamable_http_client(url))
                read, write = transport[0], transport[1]
                session = await stack.enter_async_context(ClientSession(read, write))
                await session.initialize()
                listed = await session.list_tools()
                for fn in listed.tools:
                    api_name = f"{tool.name}__{fn.name}"[:64]
                    anthropic_tools.append({
                        "name": api_name,
                        "description": fn.description or "",
                        "input_schema": fn.inputSchema
                        or {"type": "object", "properties": {}},
                    })
                    dispatch[api_name] = (session, fn.name)

            # --- Prompt caching: cache the stable tools+system prefix.
            # Render order is tools -> system, so a breakpoint on the last
            # system block caches both; with no system prompt, cache the
            # last tool definition instead. ---
            system = None
            if self.system_message:
                system = [{
                    "type": "text",
                    "text": self.system_message,
                    "cache_control": {"type": "ephemeral"},
                }]
            elif anthropic_tools:
                anthropic_tools[-1]["cache_control"] = {"type": "ephemeral"}

            messages: list[dict] = [{"role": "user", "content": user_message}]
            meta = {
                "input_tokens": 0,
                "output_tokens": 0,
                "cache_creation_input_tokens": 0,
                "cache_read_input_tokens": 0,
                "num_turns": 0,
            }
            final_text = ""

            for _ in range(self.max_tool_iterations):
                kwargs: dict = {
                    "model": self.model,
                    "max_tokens": self.max_tokens,
                    "messages": messages,
                }
                if system is not None:
                    kwargs["system"] = system
                if anthropic_tools:
                    kwargs["tools"] = anthropic_tools
                if self.thinking:
                    kwargs["thinking"] = {"type": self.thinking}

                try:
                    resp = await client.messages.create(**kwargs)
                except Exception as exc:
                    translated = self._translate_api_error(exc)
                    if translated is not None:
                        raise translated from exc
                    raise

                meta["num_turns"] += 1
                usage = getattr(resp, "usage", None)
                if usage is not None:
                    meta["input_tokens"] += getattr(usage, "input_tokens", 0) or 0
                    meta["output_tokens"] += getattr(usage, "output_tokens", 0) or 0
                    meta["cache_creation_input_tokens"] += (
                        getattr(usage, "cache_creation_input_tokens", 0) or 0
                    )
                    meta["cache_read_input_tokens"] += (
                        getattr(usage, "cache_read_input_tokens", 0) or 0
                    )

                # --- Log this turn's content blocks ---
                turn_text_parts: list[str] = []
                tool_uses = []
                for block in resp.content:
                    btype = getattr(block, "type", "")
                    if btype == "thinking":
                        stream_parts.append(
                            f"[Thinking]\n{getattr(block, 'thinking', '')}\n\n"
                        )
                    elif btype == "text":
                        turn_text_parts.append(block.text)
                        stream_parts.append(f"[Response]\n{block.text}\n\n")
                    elif btype == "tool_use":
                        tool_uses.append(block)
                        tool_input = json.dumps(block.input)
                        if len(tool_input) > 2000:
                            tool_input = tool_input[:2000] + "\n... [truncated]"
                        stream_parts.append(
                            f"[Tool Call: {block.name}]\nArgs: {tool_input}\n\n"
                        )
                if turn_text_parts:
                    final_text = "".join(turn_text_parts)

                # Preserve the assistant turn verbatim (thinking + tool_use
                # blocks must round-trip for the next request).
                messages.append({"role": "assistant", "content": resp.content})

                if resp.stop_reason == "max_tokens":
                    raise MaxOutputTokensError(
                        node_id=self._get_node_id(),
                        exit_code=-1,
                        raw_message="response truncated at max_tokens",
                        partial_text=final_text,
                    )

                if resp.stop_reason != "tool_use" or not tool_uses:
                    break

                # --- Execute each requested tool over its MCP server ---
                tool_results = []
                for tu in tool_uses:
                    session, fn_name = dispatch.get(tu.name, (None, None))
                    if session is None:
                        result_text = f"Unknown tool: {tu.name}"
                        is_error = True
                    else:
                        try:
                            mcp_result = await session.call_tool(
                                fn_name, tu.input or {}
                            )
                            result_text = self._mcp_result_to_text(mcp_result)
                            is_error = bool(getattr(mcp_result, "isError", False))
                        except Exception as exc:  # tool failure is recoverable
                            result_text = f"Tool execution error: {exc}"
                            is_error = True
                    logged = result_text
                    if len(logged) > 2000:
                        logged = logged[:2000] + "\n... [truncated]"
                    label = "Tool Result (error)" if is_error else "Tool Result"
                    stream_parts.append(f"[{label}]\n{logged}\n\n")
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": tu.id,
                        "content": result_text,
                        "is_error": is_error,
                    })

                messages.append({"role": "user", "content": tool_results})
            else:
                stream_parts.append(
                    f"[DEBUG] Reached max_tool_iterations={self.max_tool_iterations}\n"
                )

        # --- Metadata + log file (mirrors the CLI path's output) ---
        self._last_metadata = {k: v for k, v in meta.items() if v}

        stream_parts.append("-" * 80 + "\n\n")
        if self._log_prefix is not None:
            with open(f"{self._log_prefix}.log", "a") as f:
                f.write("".join(stream_parts))

        return ClaudeCodeQueryResult(
            result=final_text,
            returncode=0,
            stderr="",
            stream_result="".join(stream_parts),
        )

    @staticmethod
    def _mcp_result_to_text(result) -> str:
        """Flatten an MCP ``CallToolResult`` into plain text for a tool_result."""
        parts = []
        for item in getattr(result, "content", None) or []:
            text = getattr(item, "text", None)
            parts.append(text if text is not None else str(item))
        return "\n".join(parts)

    def _translate_api_error(self, exc) -> Optional[Exception]:
        """Map an ``anthropic`` SDK exception to a local typed error.

        Returns the local exception to raise, or ``None`` when *exc* is not
        an Anthropic API error (so the caller re-raises it untouched). The
        local types mirror the CLI path so :meth:`prompt`'s retry loop sees
        the same exceptions regardless of backend.
        """
        import anthropic

        node_id = self._get_node_id()
        msg = str(exc)[:300]

        if isinstance(exc, anthropic.RateLimitError):
            retry_after = 60
            try:
                ra = exc.response.headers.get("retry-after")
                if ra:
                    retry_after = int(ra)
            except Exception:
                pass
            reset_time = datetime.now(timezone.utc) + timedelta(seconds=retry_after)
            return RateLimitError(
                node_id=node_id, reset_time=reset_time, raw_message=msg, exit_code=-1
            )

        if isinstance(exc, anthropic.AuthenticationError):
            return AuthenticationError(node_id=node_id, raw_message=msg)

        if isinstance(exc, anthropic.PermissionDeniedError):
            if "billing" in (getattr(exc, "type", "") or ""):
                return BillingError(node_id=node_id, raw_message=msg)
            return AuthenticationError(node_id=node_id, raw_message=msg)

        if isinstance(exc, (anthropic.BadRequestError, anthropic.NotFoundError)):
            return InvalidRequestError(node_id=node_id, raw_message=msg)

        if isinstance(exc, (anthropic.InternalServerError,
                            anthropic.APIConnectionError,
                            anthropic.APITimeoutError)):
            return ServerError(node_id=node_id, raw_message=msg)

        if isinstance(exc, anthropic.APIStatusError):
            if getattr(exc, "status_code", 0) >= 500:
                return ServerError(node_id=node_id, raw_message=msg)
            return UnknownClaudeError(node_id=node_id, raw_message=msg, stderr=msg)

        return None

