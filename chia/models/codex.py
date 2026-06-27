"""Codex CLI LLM backend.

``CodexLLM`` wraps ``codex exec`` behind the same synchronous ``prompt`` shape
as the other Chia LLM backends.  Chia MCP tools are passed as per-run Codex
config overrides, so this backend does not mutate the user's persistent Codex
configuration.
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import tempfile
from glob import glob
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import ray

from chia.base.ChiaFunction import ChiaFunction, ObjectRefCallback
from chia.base.llm_call import QueryResult, LLMCallBase

if TYPE_CHECKING:
    from chia.base.tools.ChiaTool import ChiaTool


class CodexError(Exception):
    """Base for Codex CLI errors. Subclasses are Ray-serializable."""

    error_type = "unknown"

    def __init__(self, node_id: str, exit_code: int = -1, raw_message: str = ""):
        self.node_id = node_id
        self.exit_code = exit_code
        self.raw_message = raw_message
        super().__init__(f"{self.error_type} on {node_id}: {raw_message[:200]}")

    def __reduce__(self):
        return (self.__class__, (self.node_id, self.exit_code, self.raw_message))


class RateLimitError(CodexError):
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


class AuthenticationError(CodexError):
    error_type = "authentication_failed"


class BillingError(CodexError):
    error_type = "billing_error"


class InvalidRequestError(CodexError):
    error_type = "invalid_request"


class ServerError(CodexError):
    error_type = "server_error"


class MaxOutputTokensError(CodexError):
    error_type = "max_output_tokens"


class UnknownCodexError(CodexError):
    error_type = "unknown"


_RESET_RE = re.compile(
    r"(?:reset|resets|retry(?:\s|-)?after)\D+(\d{1,2})\s*(am|pm)?(?:\s*\(([^)]+)\))?",
    re.IGNORECASE,
)

_ERROR_PATTERNS: tuple[tuple[type[CodexError], tuple[str, ...]], ...] = (
    (
        AuthenticationError,
        ("not logged in", "login", "authentication", "unauthorized", "401", "api key", "auth token"),
    ),
    (BillingError, ("billing", "payment", "402", "credit", "quota exceeded")),
    (
        InvalidRequestError,
        ("invalid request", "malformed", "bad request", "invalid model", "unknown model",
         "invalid config", "unrecognized option", "400"),
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
    "input_tokens": ("input_tokens", "prompt_tokens", "input"),
    "output_tokens": ("output_tokens", "completion_tokens", "output"),
    "total_tokens": ("total_tokens",),
    "reasoning_tokens": ("reasoning_tokens", "reasoning_output_tokens", "reasoning"),
    "cache_read_input_tokens": ("cached_input_tokens", "cache_read_input_tokens"),
    "cache_creation_input_tokens": ("cache_creation_input_tokens",),
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

_CODEX_SESSION_STATE_PATTERNS = (
    "state_*.sqlite",
    "state_*.sqlite-wal",
    "state_*.sqlite-shm",
)


@dataclass
class CodexQueryResult(QueryResult):
    """QueryResult specialized for the Codex CLI backend.

    Codex stores resumable conversations under ``CODEX_HOME`` rather than in a
    portable JSONL transcript. The state fields carry the needed opaque files
    back to the caller so a later ``codex exec resume <session_id> -`` can run
    on a different Chia worker.
    """

    session_id: str | None = None
    session_state: dict[str, bytes] | None = None
    session_state_paths: tuple[str, ...] = ()


def parse_session_id(stdout: str) -> str | None:
    """Extract a Codex session id from JSONL stdout."""
    for line in stdout.splitlines():
        event = CodexLLM._json_or_none(line.strip())
        if event is None:
            continue
        sid = _find_session_id(event)
        if sid:
            return sid
    lower = stdout.lower()
    if any(token in lower for token in ("session", "conversation", "thread")):
        match = _UUID_RE.search(stdout)
        if match:
            return match.group(0)
    return None


def _find_session_id(value: Any) -> str | None:
    if isinstance(value, dict):
        type_text = str(value.get("type") or value.get("event") or "").lower()
        id_context = any(token in type_text for token in ("session", "conversation", "thread"))
        for key, item in value.items():
            key_text = str(key).lower()
            if isinstance(item, str):
                if any(token in key_text for token in ("session", "conversation", "thread")):
                    return item
                if key_text == "id" and id_context:
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
    """Attach Codex session sync to remote prompt calls when persistence is on."""

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


def _toml(value: str) -> str:
    return json.dumps(value)


def _toml_key(value: str) -> str:
    return value if re.fullmatch(r"[A-Za-z0-9_-]+", value) else json.dumps(value)


def _truncate(text: str, limit: int = 2000) -> str:
    return text if len(text) <= limit else text[:limit] + "\n... [truncated]"


def _payload(event: dict) -> dict:
    payload = event.get("payload")
    if isinstance(payload, dict):
        return payload
    item = event.get("item")
    return item if isinstance(item, dict) else event


class CodexLLM(LLMCallBase):
    """Wrap ``codex exec`` as a Chia LLM backend."""

    def __init__(
        self,
        model: str | None = None,
        system_message: str = "",
        timeout_seconds: int = 600,
        retries: int = 3,
        logging_name: str = "codex",
        logging_level: int = logging.DEBUG,
        log_dir: str | None = None,
        codex_bin: str = "codex",
        work_dir: str | None = None,
        extra_cli_args: list[str] | None = None,
        sandbox: str = "workspace-write",
        approval_policy: str = "never",
        dangerously_bypass_approvals_and_sandbox: bool = True,
        skip_git_repo_check: bool = True,
        ephemeral: bool = False,
        ignore_rules: bool = False,
        profile: str | None = None,
        reasoning_effort: str | None = None,
        resume_session: bool = False,
        auto_compact_token_limit: int | None = 200_000,
    ):
        super().__init__(system_message=system_message)
        self.logging_level = logging_level
        self.logging_name = logging_name
        self.retries = retries
        self.timeout_seconds = timeout_seconds
        self.model = model
        self.codex_bin = codex_bin
        self.work_dir = work_dir
        self.extra_cli_args = extra_cli_args or []
        self.sandbox = sandbox
        self.approval_policy = approval_policy
        self.dangerously_bypass_approvals_and_sandbox = dangerously_bypass_approvals_and_sandbox
        self.skip_git_repo_check = skip_git_repo_check
        self.ephemeral = ephemeral
        self.ignore_rules = ignore_rules
        self.profile = profile
        self.reasoning_effort = reasoning_effort
        self.auto_compact_token_limit = auto_compact_token_limit
        self.logger = logging.getLogger(logging_name)
        self._call_counter = 0
        self._resume_session = resume_session
        self._session_id: str | None = None
        self._session_state: dict[str, bytes] | None = None
        self._session_state_paths: tuple[str, ...] = ()
        self._last_metadata: dict = {}
        self._log_prefix = None

        self.logger.warning("CodexLLM is experimental and has not been production-validated.")
        if self.model is None:
            self.logger.info("CodexLLM model is unset; codex exec will use its configured default model.")
        if log_dir is not None:
            os.makedirs(log_dir, exist_ok=True)
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self._log_prefix = os.path.join(log_dir, f"{logging_name}_{stamp}")

    @_session_tracked
    @ChiaFunction(resources={"codex_creds": 0.01})
    def prompt(
        self,
        user_message: str,
        tools: list[ChiaTool] | None = None,
    ) -> CodexQueryResult:
        """Send *user_message* to ``codex exec``."""
        import time as _time

        from chia.trace.profiler import get_profiler

        profiler = get_profiler()
        for attempt in range(self.retries):
            try:
                tool_list = tools or []
                self._last_metadata = {}
                self._restore_session_state()
                cli = self._run_codex(user_message, tool_list)
                self._call_counter += 1
                self._last_metadata.update({
                    "model": self.model or "codex-default",
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
            except ServerError:
                backoff = min(5 * 2 ** attempt, 60)
                self.logger.warning("Server error on attempt %d/%d, backing off %ds",
                                    attempt + 1, self.retries, backoff)
                _time.sleep(backoff)
            except (UnknownCodexError, subprocess.TimeoutExpired) as exc:
                self.logger.warning("Codex attempt %d/%d failed: %s",
                                    attempt + 1, self.retries, exc)
            except Exception as exc:
                self.logger.warning("Unexpected Codex error on attempt %d/%d: %s",
                                    attempt + 1, self.retries, exc)
        return CodexQueryResult(
            result="",
            returncode=-1,
            stderr="",
            stream_result="",
            success=False,
            session_id=self._session_id,
            session_state=self._session_state,
            session_state_paths=self._session_state_paths,
        )

    def _sync_session(self, cli: CodexQueryResult) -> CodexQueryResult:
        """Copy worker-captured Codex session state onto this instance."""
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
        args: list[str] = []
        for tool in tools:
            port = getattr(tool, "port", 8000)
            url = f"http://{tool.hostname}:{port}/{tool.name}/mcp"
            args += ["-c", f"mcp_servers.{_toml_key(tool.name)}.url={_toml(url)}"]
        return args

    def _build_cmd(
        self,
        tools: list[ChiaTool] | None = None,
        output_last_message_path: str | None = None,
        resume_session_id: str | None = None,
    ) -> list[str]:
        cmd = [self.codex_bin]
        if not self.dangerously_bypass_approvals_and_sandbox and self.approval_policy:
            cmd += ["--ask-for-approval", self.approval_policy]
        if resume_session_id:
            cmd += ["exec", "resume", "--json"]
        else:
            cmd += ["exec", "--json", "--color", "never"]
        if self.model:
            cmd += ["--model", self.model]
        if self.profile:
            cmd += ["--profile", self.profile]
        if self.work_dir and not resume_session_id:
            cmd += ["--cd", self.work_dir]
        if self.skip_git_repo_check:
            cmd.append("--skip-git-repo-check")
        if self.ephemeral:
            cmd.append("--ephemeral")
        if self.ignore_rules:
            cmd.append("--ignore-rules")
        if self.dangerously_bypass_approvals_and_sandbox:
            cmd.append("--dangerously-bypass-approvals-and-sandbox")
        else:
            cmd += ["--sandbox", self.sandbox]
        if output_last_message_path:
            cmd += ["--output-last-message", output_last_message_path]
        if self.reasoning_effort:
            cmd += ["-c", f"model_reasoning_effort={_toml(self.reasoning_effort)}"]
        if self.auto_compact_token_limit is not None:
            cmd += ["-c", f"model_auto_compact_token_limit={self.auto_compact_token_limit}"]
        cmd += self._mcp_config_args(tools or [])
        cmd += self.extra_cli_args
        if resume_session_id:
            return cmd + [resume_session_id, "-"]
        return cmd + ["-"]

    def _run_codex(self, user_message: str, tools: list[ChiaTool] | None = None) -> CodexQueryResult:
        fd, output_path = tempfile.mkstemp(suffix=".txt")
        os.close(fd)
        try:
            resume_session_id = self._session_id if self._resume_session else None
            result = subprocess.run(
                self._build_cmd(
                    tools or [],
                    output_last_message_path=output_path,
                    resume_session_id=resume_session_id,
                ),
                input=self._format_prompt(user_message),
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
                cwd=self.work_dir or None,
                env=os.environ.copy(),
            )
            with open(output_path) as f:
                final_text = f.read()
            stream, meta, fallback = self._parse_jsonl_stream(result.stdout, result.stderr)
            parsed_session_id = parse_session_id(result.stdout)
            if self._resume_session and parsed_session_id:
                self._session_id = parsed_session_id
            if self._session_id:
                meta["session_id"] = self._session_id
            self._last_metadata = meta
            final_text = final_text or fallback
            if self._log_prefix is not None:
                self._write_log(user_message, final_text, stream)
            if result.returncode != 0:
                self.logger.warning("codex exited %d: %s", result.returncode, result.stderr[:500])
            return CodexQueryResult(
                final_text,
                result.returncode,
                result.stderr,
                stream,
                session_id=self._session_id,
            )
        finally:
            try:
                os.unlink(output_path)
            except FileNotFoundError:
                pass

    def _codex_home(self) -> str:
        return os.environ.get("CODEX_HOME") or os.path.join(os.path.expanduser("~"), ".codex")

    def _session_state_files(self) -> list[tuple[str, str]]:
        home = self._codex_home()
        paths: list[tuple[str, str]] = []
        for pattern in _CODEX_SESSION_STATE_PATTERNS:
            dirname = os.path.dirname(pattern)
            basename = os.path.basename(pattern)
            root = os.path.join(home, dirname)
            if not os.path.isdir(root):
                continue
            for name in os.listdir(root):
                if not re.fullmatch(basename.replace(".", r"\.").replace("*", ".*"), name):
                    continue
                full = os.path.join(root, name)
                if os.path.isfile(full):
                    paths.append((os.path.relpath(full, home), full))
        return sorted(set(paths))

    def _path_relative_to_codex_home(self, path: str) -> str | None:
        home = os.path.abspath(self._codex_home())
        full = os.path.abspath(path if os.path.isabs(path) else os.path.join(home, path))
        try:
            if os.path.commonpath([home, full]) != home:
                return None
        except ValueError:
            return None
        return os.path.relpath(full, home)

    def _session_rollout_files(self) -> list[tuple[str, str]]:
        if self._session_id is None:
            return []
        home = self._codex_home()
        paths: list[tuple[str, str]] = []
        pattern = os.path.join(home, "sessions", "**", f"rollout-*{self._session_id}*.jsonl")
        for full_rollout in glob(pattern, recursive=True):
            rel_rollout = self._path_relative_to_codex_home(full_rollout)
            if rel_rollout is not None and os.path.isfile(full_rollout):
                paths.append((rel_rollout, full_rollout))
        return sorted(set(paths))

    def _restore_session_state(self) -> None:
        if not self._resume_session or self._session_state is None:
            return
        home = self._codex_home()
        for rel_path, data in self._session_state.items():
            if os.path.isabs(rel_path) or rel_path.startswith(".."):
                continue
            path = os.path.join(home, rel_path)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(data)

    def _capture_session_state(self, cli: CodexQueryResult) -> None:
        if not self._resume_session or self._session_id is None:
            return
        state: dict[str, bytes] = {}
        for rel_path, path in self._session_state_files() + self._session_rollout_files():
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
                    f"Prompt #{self._call_counter} (codex)\n")
            f.write("=" * 80 + f"\n\n[User Message]\n{prompt}\n\n")
            f.write(stream if stream else f"[Response]\n{final_text}\n\n")
            if stream and not stream.endswith("\n"):
                f.write("\n")
            f.write("-" * 80 + "\n\n")

    @classmethod
    def _parse_jsonl_stream(cls, stdout: str, stderr: str = "") -> tuple[str, dict, str]:
        stream_parts: list[str] = []
        result_parts: list[str] = []
        meta: dict[str, Any] = {}
        for line in stdout.splitlines():
            event = cls._json_or_none(line)
            if event is None:
                stream_parts.append(f"[UNPARSED] {_truncate(line.strip(), 200)}\n")
                continue
            cls._record_usage(event, meta)
            cls._record_event(event, stream_parts, result_parts)
        if stderr:
            stream_parts.append(f"[stderr]\n{_truncate(stderr)}\n\n")
        return "".join(stream_parts), {k: v for k, v in meta.items() if v}, "".join(result_parts)

    @staticmethod
    def _json_or_none(line: str) -> dict | None:
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            return None

    @classmethod
    def _record_event(cls, event: dict, stream: list[str], results: list[str]) -> None:
        payload = _payload(event)
        etype = str(payload.get("type") or event.get("type") or "").lower()
        text = cls._text(payload)

        if "tool" in etype and any(k in etype for k in ("result", "output", "finish", "complete")):
            stream.append(f"[Tool Result]\n{_truncate(text)}\n\n")
        elif "tool" in etype and any(k in etype for k in ("call", "start", "begin")):
            name = payload.get("name") or payload.get("tool_name") or payload.get("tool") or "unknown"
            args = payload.get("arguments", payload.get("args", payload.get("input", {})))
            stream.append(f"[Tool Call: {name}]\nArgs: {_truncate(json.dumps(args))}\n\n")
        elif "reason" in etype or "thinking" in etype:
            if text:
                stream.append(f"[Thinking]\n{_truncate(text)}\n\n")
        elif "error" in etype:
            stream.append(f"[Error]\n{_truncate(text or json.dumps(event, sort_keys=True))}\n\n")
        elif any(k in etype for k in ("message", "response", "assistant", "final")) and text:
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
        etype = str(payload.get("type") or event.get("type") or "").lower()
        if "turn" in etype and any(token in etype for token in ("complete", "end", "done")):
            meta["num_turns"] = meta.get("num_turns", 0) + 1
        usage = payload.get("usage") or payload.get("tokens") or payload.get("token_usage")
        if isinstance(payload.get("info"), dict):
            usage = usage or payload["info"].get("last_token_usage") or payload["info"].get("total_token_usage")
        if not isinstance(usage, dict):
            return
        for dest, sources in _TOKEN_ALIASES.items():
            for source in sources:
                value = usage.get(source)
                if isinstance(value, (int, float)):
                    meta[dest] = meta.get(dest, 0) + value
        cache = usage.get("cache")
        if isinstance(cache, dict):
            for source, dest in (("read", "cache_read_input_tokens"),
                                 ("write", "cache_creation_input_tokens")):
                if isinstance(cache.get(source), (int, float)):
                    meta[dest] = meta.get(dest, 0) + cache[source]

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
        raise UnknownCodexError(node_id=node_id, exit_code=cli.returncode, raw_message=combined[:300])
