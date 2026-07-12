"""GitHub Copilot CLI LLM backend.

``CopilotLLM`` wraps ``copilot --prompt`` (non-interactive mode) behind the
same synchronous ``prompt`` shape as the other Chia LLM backends.  Chia MCP
tools are passed as a per-session ``--additional-mcp-config`` override, so
this backend does not mutate the user's persistent Copilot configuration.

**Auth.** ``copilot`` signs in with a GitHub account (``copilot login``) or a
token from ``COPILOT_GITHUB_TOKEN`` / ``GH_TOKEN`` / ``GITHUB_TOKEN``; stored
credentials live in the system credential store, or under ``COPILOT_HOME``
(default ``~/.copilot``) when no credential store is available (e.g. in a
container).

The system prompt is folded into the user message (non-interactive mode has no
system-prompt flag), mirroring :class:`~chia.models.codex.CodexLLM`.
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
from glob import glob
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import ray

from chia.base.ChiaFunction import ChiaFunction, ObjectRefCallback
from chia.base.llm_call import QueryResult, LLMCallBase, UNSET

if TYPE_CHECKING:
    from chia.base.tools.ChiaTool import ChiaTool


class CopilotError(Exception):
    """Base for Copilot CLI errors. Subclasses are Ray-serializable."""

    error_type = "unknown"

    def __init__(self, node_id: str, exit_code: int = -1, raw_message: str = ""):
        self.node_id = node_id
        self.exit_code = exit_code
        self.raw_message = raw_message
        super().__init__(f"{self.error_type} on {node_id}: {raw_message[:200]}")

    def __reduce__(self):
        return (self.__class__, (self.node_id, self.exit_code, self.raw_message))


class RateLimitError(CopilotError):
    error_type = "rate_limit"

    def __init__(
        self,
        node_id: str,
        reset_time: datetime | None = None,
        raw_message: str = "",
        exit_code: int = -1,
    ):
        self.reset_time = reset_time or datetime.now(timezone.utc) + timedelta(minutes=1)
        super().__init__(node_id=node_id, exit_code=exit_code, raw_message=raw_message)

    def __reduce__(self):
        return (
            self.__class__,
            (self.node_id, self.reset_time, self.raw_message, self.exit_code),
        )


class AuthenticationError(CopilotError):
    error_type = "authentication_failed"


class BillingError(CopilotError):
    error_type = "billing_error"


class InvalidRequestError(CopilotError):
    error_type = "invalid_request"


class ServerError(CopilotError):
    error_type = "server_error"


class MaxOutputTokensError(CopilotError):
    error_type = "max_output_tokens"


class UnknownCopilotError(CopilotError):
    error_type = "unknown"


_RESET_RE = re.compile(
    r"(?:reset|resets|retry(?:\s|-)?after)\D+(\d{1,2})\s*(am|pm)?(?:\s*\(([^)]+)\))?",
    re.IGNORECASE,
)

_ERROR_PATTERNS: tuple[tuple[type[CopilotError], tuple[str, ...]], ...] = (
    (
        AuthenticationError,
        ("not logged in", "login", "authentication", "unauthorized", "401", "api key", "auth token"),
    ),
    (BillingError, ("billing", "payment", "402", "credit", "quota exceeded")),
    (
        InvalidRequestError,
        ("invalid request", "malformed", "bad request", "invalid model", "unknown model",
         "not available", "invalid config", "unrecognized option", "unknown option", "400"),
    ),
    (
        ServerError,
        ("500", "503", "server error", "overloaded", "internal error", "service unavailable",
         "connection", "timeout", "timed out"),
    ),
    (
        MaxOutputTokensError,
        ("max output", "maximum output", "output token limit", "context length",
         "context window", "truncated"),
    ),
)


_TOKEN_ALIASES = {
    "input_tokens": ("input_tokens", "inputTokens"),
    "output_tokens": ("output_tokens", "outputTokens"),
    "total_tokens": ("total_tokens", "totalTokens"),
    # copilot meters non-interactive runs in premium requests as well as tokens
    "premium_requests": ("premiumRequests",),
}


_RATE_LIMIT_429_RE = re.compile(
    r"\b(?:http(?:\s+status)?|status(?:code)?|code|apierror|error)\s*[:=]?\s*429\b"
    r"|\b429\s+(?:too many requests|rate limit(?:ed)?)\b",
    re.IGNORECASE,
)

_UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)

_COPILOT_SESSION_STATE_FILES = (
    "session-store.db",
    "session-store.db-wal",
    "session-store.db-shm",
)


@dataclass
class CopilotQueryResult(QueryResult):
    """QueryResult specialized for the Copilot CLI backend.

    Copilot stores resumable sessions under ``COPILOT_HOME`` (the
    ``session-store.db`` index plus a ``session-state/<session_id>/``
    directory) rather than in a portable JSONL transcript. The state fields
    carry the needed opaque files back to the caller so a later
    ``copilot --resume=<session_id>`` can run on a different Chia worker.
    """

    session_id: str | None = None
    session_state: dict[str, bytes] | None = None
    session_state_paths: tuple[str, ...] = ()


def parse_session_id(stdout: str) -> str | None:
    """Extract a Copilot session id from JSONL stdout."""
    unparsed: list[str] = []
    for line in stdout.splitlines():
        event = CopilotLLM._json_or_none(line.strip())
        if event is None:
            unparsed.append(line)
            continue
        sid = _find_session_id(event)
        if sid:
            return sid
    # Fallback for non-JSONL output. Restricted to the unparsed lines because
    # copilot's event envelopes pair "session"-flavored types with unrelated
    # ``id``/``parentId`` UUIDs.
    text = "\n".join(unparsed)
    lower = text.lower()
    if any(token in lower for token in ("session", "conversation", "thread")):
        match = _UUID_RE.search(text)
        if match:
            return match.group(0)
    return None


def _find_session_id(value: Any) -> str | None:
    # Only explicitly session-named keys (e.g. the ``result`` event's
    # ``sessionId``) are trusted: copilot event envelopes carry unrelated
    # ``id``/``parentId`` UUIDs on every event.
    if isinstance(value, dict):
        for key, item in value.items():
            if isinstance(item, str) and "session" in str(key).lower():
                return item
        for item in value.values():
            sid = _find_session_id(item)
            if sid:
                return sid
    elif isinstance(value, list):
        for item in value:
            sid = _find_session_id(item)
            if sid:
                return sid
    return None


def _session_tracked(chia_fn):
    """Attach Copilot session sync to remote prompt calls when persistence is on."""

    def _wrap(instance, ref):
        if getattr(instance, "_resume_session", False):
            return ObjectRefCallback(ref, instance._sync_session)
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
            original = getattr(chia_fn, "_chia_original", chia_fn)
            return original(self._instance, *args, **kwargs)

        def chia_remote(self, *args, **kwargs):
            return _wrap(self._instance, chia_fn.chia_remote(*args, **kwargs))

        def options(self, **opts):
            return _TrackedHandle(chia_fn.options(**opts), self._instance)

        def __getattr__(self, name):
            return getattr(chia_fn, name)

    class _TrackedDescriptor:
        def __get__(self, obj, objtype=None):
            if obj is None:
                return chia_fn
            return _BoundTracked(obj)

        def __getattr__(self, name):
            return getattr(chia_fn, name)

    return _TrackedDescriptor()


def parse_rate_limit_reset(text: str) -> datetime | None:
    """Parse a human reset time such as ``resets 4pm (America/Los_Angeles)``."""
    match = _RESET_RE.search(text)
    if match is None:
        return None

    hour = int(match.group(1))
    ampm = (match.group(2) or "").lower()
    if ampm == "pm" and hour != 12:
        hour += 12
    elif ampm == "am" and hour == 12:
        hour = 0

    try:
        import zoneinfo

        tz = zoneinfo.ZoneInfo((match.group(3) or "UTC").strip())
    except Exception:
        tz = timezone.utc

    now = datetime.now(tz)
    reset = now.replace(hour=hour, minute=0, second=0, microsecond=0)
    if reset <= now:
        reset += timedelta(days=1)
    return reset.astimezone(timezone.utc)


def _truncate(text: str, limit: int = 2000) -> str:
    return text if len(text) <= limit else text[:limit] + "\n... [truncated]"


def _payload(event: dict) -> dict:
    data = event.get("data")
    return data if isinstance(data, dict) else event


class CopilotLLM(LLMCallBase):
    """Wrap the GitHub Copilot CLI (``copilot --prompt``) as a Chia LLM backend."""

    # Honors --allow-all (via its own kwarg); has no opencode-style permission block.
    supports_dangerously_skip_permissions = True

    def __init__(
        self,
        model: str | None = None,
        system_message: str = "",
        timeout_seconds: int = 600,
        retries: int = 3,
        logging_name: str = "copilot",
        logging_level: int = logging.DEBUG,
        log_dir: str | None = None,
        copilot_bin: str = "copilot",
        work_dir: str | None = None,
        extra_cli_args: list[str] | None = None,
        allow_tools: list[str] | None = None,
        deny_tools: list[str] | None = None,
        allow_all: bool = True,
        reasoning_effort: str | None = None,
        resume_session: bool = False,
        config=UNSET,
    ):
        # copilot's --allow-all also disables path and URL verification, so it
        # keeps its own (more specific) kwarg; mirror it onto the canonical
        # base flag.
        super().__init__(system_message=system_message,
                         dangerously_skip_permissions=allow_all,
                         config=config)
        self.logging_level = logging_level
        self.logging_name = logging_name
        self.retries = retries
        self.timeout_seconds = timeout_seconds
        self.model = model
        self.copilot_bin = copilot_bin
        self.work_dir = work_dir
        self.extra_cli_args = extra_cli_args or []
        self.allow_tools = allow_tools or []
        self.deny_tools = deny_tools or []
        self.allow_all = allow_all
        self.reasoning_effort = reasoning_effort
        self.logger = logging.getLogger(logging_name)
        self._call_counter = 0
        self._resume_session = resume_session
        self._session_id: str | None = None
        self._session_state: dict[str, bytes] | None = None
        self._session_state_paths: tuple[str, ...] = ()
        self._last_metadata: dict = {}
        self._log_prefix = None

        self.logger.warning("CopilotLLM is experimental and has not been production-validated.")
        if self.model is None:
            self.logger.info("CopilotLLM model is unset; copilot will use its configured default model.")
        if log_dir is not None:
            os.makedirs(log_dir, exist_ok=True)
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self._log_prefix = os.path.join(log_dir, f"{logging_name}_{stamp}")

    @_session_tracked
    @ChiaFunction(resources={"copilot_creds": 0.01})
    def prompt(
        self,
        user_message: str,
        tools: list[ChiaTool] | None = None,
    ) -> CopilotQueryResult:
        """Send *user_message* to ``copilot --prompt``."""
        import time as _time

        from chia.trace.profiler import get_profiler

        profiler = get_profiler()
        last_error = ""
        for attempt in range(self.retries):
            try:
                tool_list = tools or []
                self._last_metadata = {}
                self._restore_session_state()
                cli = self._run_copilot(user_message, tool_list)
                self._call_counter += 1
                self._last_metadata.update({
                    "model": self.model or "copilot-default",
                    "tools": [
                        {"name": t.name, "hostname": getattr(t, "hostname", None),
                         "port": getattr(t, "port", None), "node_id": getattr(t, "node_id", None)}
                        for t in tool_list
                    ],
                })
                if profiler.enabled:
                    profiler.add_info(self._last_metadata)
                self._classify_error(cli)
                self._capture_session_state(cli)
                cli.success = True
                return cli
            except (RateLimitError, AuthenticationError, BillingError, InvalidRequestError):
                raise
            except MaxOutputTokensError:
                if attempt == 0:
                    self.logger.warning("Max output tokens on attempt %d/%d, retrying once",
                                        attempt + 1, self.retries)
                    continue
                raise
            except ServerError as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                backoff = min(5 * 2 ** attempt, 60)
                self.logger.warning("Server error on attempt %d/%d, backing off %ds",
                                    attempt + 1, self.retries, backoff)
                _time.sleep(backoff)
            except (UnknownCopilotError, subprocess.TimeoutExpired) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                self.logger.warning("Copilot attempt %d/%d failed: %s",
                                    attempt + 1, self.retries, exc)
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                self.logger.warning("Unexpected Copilot error on attempt %d/%d: %s",
                                    attempt + 1, self.retries, exc)
        return CopilotQueryResult(
            result="",
            returncode=-1,
            stderr=last_error,
            stream_result="",
            success=False,
            session_id=self._session_id,
            session_state=self._session_state,
            session_state_paths=self._session_state_paths,
        )

    def _sync_session(self, cli: CopilotQueryResult) -> CopilotQueryResult:
        """Copy worker-captured Copilot session state onto this instance."""
        if not self._resume_session:
            return cli
        session_id = getattr(cli, "session_id", None)
        session_state = getattr(cli, "session_state", None)
        if session_id:
            self._session_id = session_id
        if session_state is not None:
            self._session_state = session_state
            self._session_state_paths = getattr(cli, "session_state_paths", tuple(sorted(session_state)))
        return cli

    def _get_node_id(self) -> str:
        try:
            return ray.get_runtime_context().get_node_id()
        except Exception:
            return "unknown"

    def _format_prompt(self, user_message: str) -> str:
        if not self.system_message:
            return user_message
        return f"[System Instructions]\n{self.system_message}\n\n[User Request]\n{user_message}"

    def _mcp_config_args(self, tools: list[ChiaTool]) -> list[str]:
        # --additional-mcp-config augments the persistent
        # ~/.copilot/mcp-config.json for this session only.
        if not tools:
            return []
        servers: dict[str, dict] = {}
        for tool in tools:
            port = getattr(tool, "port", 8000)
            servers[tool.name] = {
                "type": "http",
                "url": f"http://{tool.hostname}:{port}/{tool.name}/mcp",
                "tools": ["*"],
            }
        return ["--additional-mcp-config", json.dumps({"mcpServers": servers})]

    def _build_cmd(
        self,
        user_message: str,
        tools: list[ChiaTool] | None = None,
        resume_session_id: str | None = None,
    ) -> list[str]:
        # A worker subprocess must not self-update mid-run, hence --no-auto-update.
        cmd = [self.copilot_bin, "--output-format", "json", "--no-color", "--no-auto-update"]
        if resume_session_id:
            # --resume takes an *optional* value, so it must be one =-joined token.
            cmd.append(f"--resume={resume_session_id}")
        if self.model:
            cmd += ["--model", self.model]
        if self.work_dir:
            cmd += ["-C", self.work_dir]
        if self.allow_all:
            cmd.append("--allow-all")
        else:
            # --allow-tool/--deny-tool also take optional values: =-joined tokens.
            # Without --allow-all(-tools), non-interactive copilot auto-approves
            # only safe tool uses and denies the rest rather than prompting.
            for tool in self.allow_tools:
                cmd.append(f"--allow-tool={tool}")
            for tool in self.deny_tools:
                cmd.append(f"--deny-tool={tool}")
        if self.reasoning_effort:
            cmd += ["--effort", self.reasoning_effort]
        cmd += self._mcp_config_args(tools or [])
        cmd += self.extra_cli_args
        # The prompt is the value of --prompt; pass it last.
        cmd += ["--prompt", self._format_prompt(user_message)]
        return cmd

    def _run_copilot(self, user_message: str, tools: list[ChiaTool] | None = None) -> CopilotQueryResult:
        resume_session_id = self._session_id if self._resume_session else None
        result = subprocess.run(
            self._build_cmd(
                user_message,
                tools or [],
                resume_session_id=resume_session_id,
            ),
            capture_output=True,
            text=True,
            timeout=self.timeout_seconds,
            cwd=self.work_dir or None,
            env=os.environ.copy(),
        )
        stream, meta, final_text, result_code = self._parse_jsonl_stream(result.stdout, result.stderr)
        parsed_session_id = parse_session_id(result.stdout)
        if self._resume_session and parsed_session_id:
            self._session_id = parsed_session_id
        if self._session_id:
            meta["session_id"] = self._session_id
        self._last_metadata = meta
        if self._log_prefix is not None:
            self._write_log(user_message, final_text, stream)
        # copilot can exit 0 even when the CLI fails outright (e.g. an unknown
        # --model prints an error and exits 0), so a successful run is
        # recognized by the trailing JSONL ``result`` event instead.
        returncode = result.returncode
        if returncode == 0 and result_code != 0:
            returncode = 1 if result_code is None else result_code
        if returncode != 0:
            self.logger.warning("copilot exited %d: %s", returncode, result.stderr[:500])
        return CopilotQueryResult(
            final_text,
            returncode,
            result.stderr,
            stream,
            session_id=self._session_id,
        )

    def _copilot_home(self) -> str:
        return os.environ.get("COPILOT_HOME") or os.path.join(os.path.expanduser("~"), ".copilot")

    def _session_state_files(self) -> list[tuple[str, str]]:
        home = self._copilot_home()
        paths: list[tuple[str, str]] = []
        for name in _COPILOT_SESSION_STATE_FILES:
            full = os.path.join(home, name)
            if os.path.isfile(full):
                paths.append((name, full))
        return sorted(set(paths))

    def _path_relative_to_copilot_home(self, path: str) -> str | None:
        home = os.path.abspath(self._copilot_home())
        full = os.path.abspath(path if os.path.isabs(path) else os.path.join(home, path))
        try:
            if os.path.commonpath([home, full]) != home:
                return None
        except ValueError:
            return None
        return os.path.relpath(full, home)

    def _session_dir_files(self) -> list[tuple[str, str]]:
        if self._session_id is None:
            return []
        home = self._copilot_home()
        paths: list[tuple[str, str]] = []
        pattern = os.path.join(home, "session-state", self._session_id, "**")
        for full_path in glob(pattern, recursive=True):
            rel_path = self._path_relative_to_copilot_home(full_path)
            if rel_path is not None and os.path.isfile(full_path):
                paths.append((rel_path, full_path))
        return sorted(set(paths))

    def _restore_session_state(self) -> None:
        if not self._resume_session or self._session_state is None:
            return
        home = self._copilot_home()
        for rel_path, data in self._session_state.items():
            if os.path.isabs(rel_path) or rel_path.startswith(".."):
                continue
            path = os.path.join(home, rel_path)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(data)

    def _capture_session_state(self, cli: CopilotQueryResult) -> None:
        if not self._resume_session or self._session_id is None:
            return
        state: dict[str, bytes] = {}
        for rel_path, path in self._session_state_files() + self._session_dir_files():
            try:
                with open(path, "rb") as f:
                    state[rel_path] = f.read()
            except OSError:
                continue
        if state:
            self._session_state = state
            self._session_state_paths = tuple(sorted(state))
            cli.session_state = state
            cli.session_state_paths = self._session_state_paths
        cli.session_id = self._session_id

    def _write_log(self, user_message: str, final_text: str, stream: str) -> None:
        prompt = user_message[:500] + ("..." if len(user_message) > 500 else "")
        with open(f"{self._log_prefix}.log", "a") as f:
            f.write("=" * 80 + "\n")
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                    f"Prompt #{self._call_counter} (copilot)\n")
            f.write("=" * 80 + f"\n\n[User Message]\n{prompt}\n\n")
            f.write(stream if stream else f"[Response]\n{final_text}\n\n")
            if stream and not stream.endswith("\n"):
                f.write("\n")
            f.write("-" * 80 + "\n\n")

    @classmethod
    def _parse_jsonl_stream(cls, stdout: str, stderr: str = "") -> tuple[str, dict, str, int | None]:
        stream_parts: list[str] = []
        result_parts: list[str] = []
        meta: dict[str, Any] = {}
        result_code: int | None = None
        for line in stdout.splitlines():
            event = cls._json_or_none(line)
            if event is None:
                stream_parts.append(f"[UNPARSED] {_truncate(line.strip(), 200)}\n")
                continue
            if event.get("ephemeral"):
                # Streaming deltas / status pings; final events repeat their content.
                continue
            if str(event.get("type") or "").lower() == "result" and isinstance(event.get("exitCode"), int):
                result_code = event["exitCode"]
            cls._record_usage(event, meta)
            cls._record_event(event, stream_parts, result_parts)
        if stderr:
            stream_parts.append(f"[stderr]\n{_truncate(stderr)}\n\n")
        return ("".join(stream_parts), {k: v for k, v in meta.items() if v},
                "".join(result_parts), result_code)

    @staticmethod
    def _json_or_none(line: str) -> dict | None:
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            return None

    @classmethod
    def _record_event(cls, event: dict, stream: list[str], results: list[str]) -> None:
        payload = _payload(event)
        # Event types are namespaced, e.g. "assistant.message" / "tool.execution_start".
        etype = str(event.get("type") or "").lower()
        text = cls._text(payload)

        if etype.startswith("tool.") and any(k in etype for k in ("result", "output", "finish", "complete")):
            stream.append(f"[Tool Result]\n{_truncate(text)}\n\n")
        elif etype.startswith("tool.") and any(k in etype for k in ("call", "start", "begin")):
            name = payload.get("toolName") or payload.get("name") or payload.get("tool") or "unknown"
            args = payload.get("arguments", payload.get("args", payload.get("input", {})))
            stream.append(f"[Tool Call: {name}]\nArgs: {_truncate(json.dumps(args))}\n\n")
        elif "reason" in etype or "thinking" in etype:
            if text:
                stream.append(f"[Thinking]\n{_truncate(text)}\n\n")
        elif "error" in etype:
            stream.append(f"[Error]\n{_truncate(text or json.dumps(event, sort_keys=True))}\n\n")
        elif etype.startswith("assistant.") and "message" in etype and text:
            results.append(text)
            stream.append(f"[Response]\n{_truncate(text)}\n\n")

    @classmethod
    def _text(cls, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        if isinstance(value, list):
            return "\n".join(filter(None, (cls._text(v) for v in value)))
        if isinstance(value, dict):
            for key in ("text", "content", "message", "output", "result", "delta"):
                text = cls._text(value.get(key))
                if text:
                    return text
            return ""
        return str(value)

    @staticmethod
    def _record_usage(event: dict, meta: dict) -> None:
        payload = _payload(event)
        etype = str(event.get("type") or "").lower()
        if "turn" in etype and any(token in etype for token in ("complete", "end", "done")):
            meta["num_turns"] = meta.get("num_turns", 0) + 1
        usage = payload.get("usage") or payload.get("tokens") or payload.get("token_usage")
        if not isinstance(usage, dict):
            # assistant.message events carry e.g. ``outputTokens`` inline.
            usage = payload
        for dest, sources in _TOKEN_ALIASES.items():
            for source in sources:
                value = usage.get(source)
                if isinstance(value, (int, float)):
                    meta[dest] = meta.get(dest, 0) + value

    def _classify_error(self, cli: QueryResult) -> None:
        combined = "\n".join(part for part in (cli.stderr, cli.result, cli.stream_result) if part)
        lower = combined.lower()
        if (
            any(k in lower for k in ("rate limit", "usage limit", "too many requests"))
            or _RATE_LIMIT_429_RE.search(combined)
        ):
            raise RateLimitError(
                node_id=self._get_node_id(),
                reset_time=parse_rate_limit_reset(combined),
                raw_message=combined[:300],
                exit_code=cli.returncode,
            )
        if cli.returncode == 0:
            return
        node_id = self._get_node_id()
        for error_cls, patterns in _ERROR_PATTERNS:
            if any(pattern in lower for pattern in patterns):
                raise error_cls(node_id=node_id, exit_code=cli.returncode, raw_message=combined[:300])
        raise UnknownCopilotError(node_id=node_id, exit_code=cli.returncode, raw_message=combined[:300])
