"""Codex CLI LLM backend.

``CodexLLM`` wraps ``codex exec`` behind the same synchronous ``prompt`` shape
as the other Chia LLM backends.  Chia MCP tools are passed as per-run Codex
config overrides, so this backend does not mutate the user's persistent Codex
configuration.
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
import random
import re
import shutil
import sqlite3
import stat
import subprocess
import tarfile
import tempfile
from glob import glob

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import ray

from chia.base.ChiaFunction import ChiaFunction, ObjectRefCallback
from chia.base.llm_call import QueryResult, LLMCallBase, UNSET

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
        return (
            self.__class__,
            (self.node_id, self.exit_code, self.raw_message),
            self.__dict__,
        )


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
            self.__dict__,
        )


class AuthenticationError(CodexError):
    error_type = "authentication_failed"


class BillingError(CodexError):
    error_type = "billing_error"


class InvalidRequestError(CodexError):
    error_type = "invalid_request"


class ServerError(CodexError):
    error_type = "server_error"


class ModelCapacityError(CodexError):
    """Transient exhaustion of capacity for the selected Codex model."""

    error_type = "model_capacity"


class MaxOutputTokensError(CodexError):
    error_type = "max_output_tokens"


class UnknownCodexError(CodexError):
    error_type = "unknown"


_RESET_RE = re.compile(
    r"(?:reset|resets|retry(?:\s|-)?after)\D+(\d{1,2})\s*(am|pm)?(?:\s*\(([^)]+)\))?",
    re.IGNORECASE,
)

_MAX_OUTPUT_RE = re.compile(
    r"\b(?:max(?:imum)? output token(?:s| limit)?|output token limit)"
    r"(?:\s+(?:has been\s+)?(?:reached|exceeded))?\b",
    re.IGNORECASE,
)
_MODEL_CAPACITY_RE = re.compile(
    re.escape("Selected model is at capacity. Please try a different model."),
    re.IGNORECASE,
)
_RATE_LIMIT_RE = re.compile(
    r"\b(?:rate limit(?:ed| exceeded)?|usage limit|too many requests)\b",
    re.IGNORECASE,
)
_AUTHENTICATION_RE = re.compile(
    r"\b(?:not logged in|authentication (?:failed|required)|unauthorized|"
    r"invalid (?:api key|auth token)|expired (?:api key|auth token))\b",
    re.IGNORECASE,
)
_AUTHENTICATION_STATUS_RE = re.compile(
    r"\b(?:http(?:\s+status)?|status(?:\s+code)?|code|apierror|error)"
    r"\s*[:=]?\s*401\b|\b401\s+unauthorized\b",
    re.IGNORECASE,
)
_BILLING_RE = re.compile(
    r"\b(?:billing(?: error)?|payment required|insufficient credits?|"
    r"credit balance|quota exceeded)\b",
    re.IGNORECASE,
)
_INVALID_REQUEST_RE = re.compile(
    r"\b(?:invalid request|malformed request|bad request|invalid model|"
    r"unknown model|invalid config(?:uration)?|unrecognized option)\b",
    re.IGNORECASE,
)
_INVALID_REQUEST_STATUS_RE = re.compile(
    r"\b(?:http(?:\s+status)?|status(?:\s+code)?|code|apierror|error)"
    r"\s*[:=]?\s*400\b|\b400\s+bad request\b",
    re.IGNORECASE,
)
_SERVER_RE = re.compile(
    r"\b(?:internal server error|server error|service unavailable|"
    r"server overloaded|temporarily overloaded)\b",
    re.IGNORECASE,
)
_SERVER_STATUS_RE = re.compile(
    r"\b(?:http(?:\s+status)?|status(?:\s+code)?|code|apierror|error)"
    r"\s*[:=]?\s*50[0234]\b|\b50[0234]\s+"
    r"(?:internal server error|bad gateway|service unavailable|gateway timeout)\b",
    re.IGNORECASE,
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

_RESTRICTED_CODEX_FEATURES = (
    "shell_tool",
    "unified_exec",
    "apps",
    "multi_agent",
    "code_mode",
    "browser_use",
    "computer_use",
    "image_generation",
    "goals",
)

_CODEX_SESSION_MANIFEST = ".chia_codex_session.json"
_CODEX_SESSION_MANIFEST_VERSION = 2
_CODEX_SESSION_ROOT_ENV = "CHIA_CODEX_SESSION_ROOT"
_SESSION_STORAGE_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_MAX_OUTPUT_CONTINUATION = "Continue where you left off. Do not repeat work you already completed."
_MAX_OUTPUT_RETRIES = 2


@dataclass(frozen=True)
class CodexTerminalOutcome:
    """Structured outcome reported for the latest Codex JSONL turn."""

    status: str
    message: str = ""


@dataclass
class CodexQueryResult(QueryResult):
    """QueryResult specialized for the Codex CLI backend.

    Codex stores resumable ``exec`` state under ``CODEX_HOME``. The session
    bundle carries the small session-bearing subset of that state between
    workers, analogous to Claude Code's transcript bytes.
    """

    session_id: str | None = None
    session_bundle: bytes | None = None
    session_bundle_paths: tuple[str, ...] = ()
    terminal_outcome: CodexTerminalOutcome | None = None
    node_id: str | None = None


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
            return self.options().chia_remote(*args, **kwargs)

        def options(self, **opts):
            opts = {**self._instance.node_affinity_options(), **opts}
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

    # Honors --dangerously-bypass-approvals-and-sandbox (via its own kwarg);
    # has no opencode-style permission block.
    supports_dangerously_skip_permissions = True

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
        sandbox: str = "read-only",
        approval_policy: str = "never",
        dangerously_bypass_approvals_and_sandbox: bool = False,
        allow_builtin_tools: bool = True,
        skip_git_repo_check: bool = True,
        ephemeral: bool = False,
        ignore_rules: bool = False,
        profile: str | None = None,
        reasoning_effort: str | None = None,
        resume_session: bool = False,
        auto_compact_token_limit: int | None = 200_000,
        config=UNSET,
        session_storage_key: str | None = None,
        capacity_attempts: int = 8,
        capacity_backoff_base_seconds: float = 15.0,
        capacity_backoff_multiplier: float = 2.0,
        capacity_backoff_max_seconds: float = 300.0,
        capacity_backoff_jitter: float = 0.2,
    ):
        # codex's bypass also disables the sandbox, so it keeps its own
        # (more specific) kwarg; mirror it onto the canonical base flag.
        super().__init__(system_message=system_message,
                         dangerously_skip_permissions=dangerously_bypass_approvals_and_sandbox,
                         config=config)
        self.logging_level = logging_level
        self.logging_name = logging_name
        self.retries = retries
        if capacity_attempts < 1:
            raise ValueError("capacity_attempts must be at least 1")
        if capacity_backoff_base_seconds < 0:
            raise ValueError("capacity_backoff_base_seconds must be non-negative")
        if capacity_backoff_multiplier < 1:
            raise ValueError("capacity_backoff_multiplier must be at least 1")
        if capacity_backoff_max_seconds < 0:
            raise ValueError("capacity_backoff_max_seconds must be non-negative")
        if not 0 <= capacity_backoff_jitter <= 1:
            raise ValueError("capacity_backoff_jitter must be between 0 and 1")
        self.capacity_attempts = capacity_attempts
        self.capacity_backoff_base_seconds = capacity_backoff_base_seconds
        self.capacity_backoff_multiplier = capacity_backoff_multiplier
        self.capacity_backoff_max_seconds = capacity_backoff_max_seconds
        self.capacity_backoff_jitter = capacity_backoff_jitter
        self.timeout_seconds = timeout_seconds
        self.model = model
        self.codex_bin = codex_bin
        self.work_dir = work_dir
        self.extra_cli_args = extra_cli_args or []
        self.sandbox = sandbox
        self.approval_policy = approval_policy
        self.dangerously_bypass_approvals_and_sandbox = dangerously_bypass_approvals_and_sandbox
        self.allow_builtin_tools = allow_builtin_tools
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
        self._last_node_id: str | None = None
        self._session_bundle: bytes | None = None
        self._session_bundle_paths: tuple[str, ...] = ()
        self._session_storage_key = self._validated_session_storage_key(
            session_storage_key or uuid4().hex
        )
        self._session_storage_key_explicit = session_storage_key is not None
        self._restricted_work_dir = os.path.join(
            tempfile.gettempdir(), "chia-codex", str(uuid4())
        )
        self._last_metadata: dict = {}
        self._log_prefix = None

        if not self.allow_builtin_tools and self.dangerously_bypass_approvals_and_sandbox:
            raise ValueError(
                "dangerously_bypass_approvals_and_sandbox requires "
                "allow_builtin_tools=True"
            )
        if not self.allow_builtin_tools and self.profile is not None:
            raise ValueError("profile requires allow_builtin_tools=True")
        if (
            not self.allow_builtin_tools
            and "--dangerously-bypass-approvals-and-sandbox" in self.extra_cli_args
        ):
            raise ValueError(
                "dangerous permission bypass is unavailable when built-in tools are disabled"
            )

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
        last_error = ""
        attempt_records: list[dict[str, Any]] = []

        def record_attempt(success: bool, error: str = "") -> None:
            attempt_records.append({
                "attempt": len(attempt_records) + 1,
                "success": success,
                "error": error,
                "metadata": self._last_metadata.copy(),
            })

        def attach_attempt_metadata(
            exception: BaseException | None = None,
        ) -> None:
            self._last_metadata = {
                **self._last_metadata.copy(),
                "provider_attempts": len(attempt_records),
                "attempts": list(attempt_records),
            }
            if exception is not None:
                exception.usage_metadata = self._last_metadata.copy()
        tool_list = tools or []
        capacity_attempt = 0

        def invoke_codex(
            prompt_message: str,
            continuation_session_id: str | None,
        ) -> CodexQueryResult:
            self._last_metadata = {}
            if continuation_session_id is None:
                cli = self._run_codex(prompt_message, tool_list)
            else:
                cli = self._run_codex(
                    prompt_message,
                    tool_list,
                    resume_session_id=continuation_session_id,
                )
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
            return cli

        for attempt in range(self.retries):
            prompt_message = user_message
            continuation_session_id: str | None = None
            for continuation_attempt in range(_MAX_OUTPUT_RETRIES + 1):
                try:
                    while True:
                        try:
                            cli = invoke_codex(
                                prompt_message,
                                continuation_session_id,
                            )
                            self._classify_error(cli)
                            break
                        except ModelCapacityError as exc:
                            record_attempt(False, exc.error_type)
                            capacity_attempt += 1
                            exc.capacity_attempts = capacity_attempt
                            if capacity_attempt >= self.capacity_attempts:
                                exc.retries_exhausted = True
                                attach_attempt_metadata(exc)
                                raise
                            retry_index = capacity_attempt - 1
                            backoff = self._capacity_backoff_delay(retry_index)
                            self.logger.warning(
                                "Model capacity error on attempt %d/%d, backing off %.1fs",
                                capacity_attempt,
                                self.capacity_attempts,
                                backoff,
                            )
                            _time.sleep(backoff)
                    record_attempt(True)
                    attach_attempt_metadata()
                    cli.success = True
                    cli.node_id = self._get_node_id()
                    return cli
                except (
                    RateLimitError,
                    AuthenticationError,
                    BillingError,
                    InvalidRequestError,
                ) as exc:
                    record_attempt(False, exc.error_type)
                    attach_attempt_metadata(exc)
                    raise
                except ModelCapacityError:
                    raise
                except MaxOutputTokensError as exc:
                    record_attempt(False, exc.error_type)
                    if continuation_attempt >= _MAX_OUTPUT_RETRIES:
                        attach_attempt_metadata(exc)
                        raise
                    continuation_session_id = getattr(cli, "session_id", None)
                    if not continuation_session_id:
                        attach_attempt_metadata(exc)
                        raise
                    prompt_message = _MAX_OUTPUT_CONTINUATION
                    self.logger.warning(
                        "Max output tokens; continuing session (%d/%d)",
                        continuation_attempt + 1,
                        _MAX_OUTPUT_RETRIES,
                    )
                except ServerError as exc:
                    record_attempt(False, exc.error_type)
                    last_error = f"{type(exc).__name__}: {exc}"
                    if attempt + 1 < self.retries:
                        backoff = min(5 * 2 ** attempt, 60)
                        self.logger.warning(
                            "Server error on attempt %d/%d, backing off %ds",
                            attempt + 1,
                            self.retries,
                            backoff,
                        )
                        _time.sleep(backoff)
                    break
                except (UnknownCodexError, subprocess.TimeoutExpired) as exc:
                    record_attempt(
                        False,
                        getattr(exc, "error_type", type(exc).__name__),
                    )
                    last_error = f"{type(exc).__name__}: {exc}"
                    self.logger.warning("Codex attempt %d/%d failed: %s",
                                        attempt + 1, self.retries, exc)
                    break
                except Exception as exc:
                    record_attempt(False, type(exc).__name__)
                    last_error = f"{type(exc).__name__}: {exc}"
                    self.logger.warning("Unexpected Codex error on attempt %d/%d: %s",
                                        attempt + 1, self.retries, exc)
                    break
        attach_attempt_metadata()
        result = CodexQueryResult(
            result="",
            returncode=-1,
            stderr=last_error,
            stream_result="",
            success=False,
            session_id=self._session_id,
            session_bundle=self._session_bundle,
            session_bundle_paths=self._session_bundle_paths,
        )
        result.usage_metadata = self._last_metadata.copy()
        return result

    def _sync_session(self, cli: CodexQueryResult) -> CodexQueryResult:
        """Copy worker-observed Codex session state onto this instance."""
        if not self._resume_session:
            return cli
        node_id = getattr(cli, "node_id", None)
        if node_id and node_id != "unknown":
            self._last_node_id = node_id
        session_id = getattr(cli, "session_id", None)
        session_bundle = getattr(cli, "session_bundle", None)
        if session_id:
            self._session_id = session_id
        if session_bundle is not None:
            self._session_bundle = session_bundle
            self._session_bundle_paths = getattr(cli, "session_bundle_paths", ())
        return cli

    def node_affinity_options(self) -> dict:
        """Prefer the last session worker, falling back if it is unavailable.

        Custom dispatch wrappers can pass these options to their ChiaFunction
        and copy the returned ``CodexQueryResult.node_id`` to ``_last_node_id``.
        A busy live worker may queue the call. Explicit scheduling overrides
        on ``prompt.options()`` take precedence over this preference.
        """
        if not self._resume_session or not self._last_node_id:
            return {}
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

        return {"scheduling_strategy": NodeAffinitySchedulingStrategy(
            node_id=self._last_node_id, soft=True,
        )}

    def _capacity_backoff_delay(self, retry_index: int) -> float:
        delay = min(
            self.capacity_backoff_base_seconds
            * self.capacity_backoff_multiplier ** retry_index,
            self.capacity_backoff_max_seconds,
        )
        jitter = random.uniform(
            1 - self.capacity_backoff_jitter,
            1 + self.capacity_backoff_jitter,
        )
        return delay * jitter

    @staticmethod
    def _validated_session_storage_key(value: str) -> str:
        key = str(value).strip()
        if not _SESSION_STORAGE_KEY_RE.fullmatch(key):
            raise ValueError(
                "session_storage_key must be 1-128 characters containing only "
                "letters, digits, dot, underscore, or hyphen"
            )
        return key

    def set_session_storage_key(self, value: str) -> None:
        """Select the stable on-worker directory name for one logical session."""
        key = self._validated_session_storage_key(value)
        if self._session_storage_key_explicit and key != self._session_storage_key:
            raise ValueError(
                "cannot change session_storage_key after it has been explicitly set"
            )
        self._session_storage_key = key
        self._session_storage_key_explicit = True

    def _get_node_id(self) -> str:
        if not ray.is_initialized():
            return "unknown"
        try:
            return ray.get_runtime_context().get_node_id()
        except Exception:
            return "unknown"

    def _format_prompt(self, user_message: str) -> str:
        if not self.system_message:
            return user_message
        return f"[System Instructions]\n{self.system_message}\n\n[User Request]\n{user_message}"

    @staticmethod
    def _mcp_tool_names(tool: ChiaTool) -> list[str]:
        return [fn.name for fn in tool.mcp._tool_manager.list_tools()]

    def _mcp_config_args(self, tools: list[ChiaTool]) -> list[str]:
        from chia.base.tools.ChiaTool import resolve_tool_url

        args: list[str] = []
        for tool in tools:
            port = getattr(tool, "port", 8000)
            url = resolve_tool_url(f"http://{tool.hostname}:{port}/{tool.name}/mcp")
            prefix = f"mcp_servers.{_toml_key(tool.name)}"
            args += ["-c", f"{prefix}.url={_toml(url)}"]
            args += ["-c", f"{prefix}.enabled=true"]
            # The caller explicitly supplied this ChiaTool and the
            # ``enabled_tools`` list below restricts the server to those exact
            # methods.  Pre-approve that allowlist so non-interactive Codex
            # runs with ``approval_policy=never`` do not fail closed before
            # the MCP request reaches the Chia worker.
            args += ["-c", f'{prefix}.default_tools_approval_mode="approve"']
            args += [
                "-c",
                f"{prefix}.enabled_tools={json.dumps(self._mcp_tool_names(tool))}",
            ]
        return args

    def _configured_mcp_server_names(self) -> list[str]:
        config_path = os.path.join(self._codex_home(), "config.toml")
        try:
            with open(config_path, "rb") as config_file:
                config = tomllib.load(config_file)
        except (OSError, tomllib.TOMLDecodeError):
            return []
        servers = config.get("mcp_servers", {})
        return sorted(servers) if isinstance(servers, dict) else []

    def _restricted_args(self) -> list[str]:
        if self.allow_builtin_tools:
            return []
        args = ["--ignore-rules"]
        for feature in _RESTRICTED_CODEX_FEATURES:
            args += ["--disable", feature]
        for server_name in self._configured_mcp_server_names():
            prefix = f"mcp_servers.{_toml_key(server_name)}"
            args += ["-c", f"{prefix}.enabled=false"]
        return args

    def _effective_work_dir(self) -> str | None:
        if self.allow_builtin_tools:
            return self.work_dir
        os.makedirs(self._restricted_work_dir, exist_ok=True)
        return self._restricted_work_dir

    def _effective_sandbox(self, tools: list[ChiaTool]) -> str:
        # Codex's read-only sandbox also blocks the network used by HTTP MCP.
        # Restricted mode still exposes only the explicitly supplied tools.
        if tools and not self.allow_builtin_tools:
            return "danger-full-access"
        return self.sandbox

    def _build_cmd(
        self,
        tools: list[ChiaTool] | None = None,
        output_last_message_path: str | None = None,
        resume_session_id: str | None = None,
    ) -> list[str]:
        cmd = [self.codex_bin]
        if not self.dangerously_bypass_approvals_and_sandbox and self.approval_policy:
            cmd += ["--ask-for-approval", self.approval_policy]
        if not self.dangerously_bypass_approvals_and_sandbox:
            cmd += ["--sandbox", self._effective_sandbox(tools or [])]
        if resume_session_id:
            cmd += ["exec", "resume", "--json"]
        else:
            cmd += ["exec", "--json", "--color", "never"]
        if self.model:
            cmd += ["--model", self.model]
        if self.profile:
            cmd += ["--profile", self.profile]
        work_dir = self._effective_work_dir()
        if work_dir and not resume_session_id:
            cmd += ["--cd", work_dir]
        if self.skip_git_repo_check:
            cmd.append("--skip-git-repo-check")
        if self.ephemeral:
            cmd.append("--ephemeral")
        if self.ignore_rules and self.allow_builtin_tools:
            cmd.append("--ignore-rules")
        if self.dangerously_bypass_approvals_and_sandbox:
            cmd.append("--dangerously-bypass-approvals-and-sandbox")
        if output_last_message_path:
            cmd += ["--output-last-message", output_last_message_path]
        if self.reasoning_effort:
            cmd += ["-c", f"model_reasoning_effort={_toml(self.reasoning_effort)}"]
        if self.auto_compact_token_limit is not None:
            cmd += ["-c", f"model_auto_compact_token_limit={self.auto_compact_token_limit}"]
        cmd += self.extra_cli_args
        cmd += self._restricted_args()
        cmd += self._mcp_config_args(tools or [])
        if resume_session_id:
            return cmd + [resume_session_id, "-"]
        return cmd + ["-"]

    def _run_codex(
        self,
        user_message: str,
        tools: list[ChiaTool] | None = None,
        *,
        resume_session_id: str | None = None,
    ) -> CodexQueryResult:
        fd, output_path = tempfile.mkstemp(suffix=".txt")
        os.close(fd)
        session_home = None
        session_lock_fd = None
        try:
            env = os.environ.copy()
            if self._resume_session:
                session_lock_fd = self._acquire_session_lock()
                session_home = self._prepare_session_home()
                env["CODEX_HOME"] = session_home
            telemetry_home = session_home or self._codex_home()
            active_resume_session_id = (
                resume_session_id
                or (self._session_id if self._resume_session else None)
            )
            rollout_offsets = self._rollout_offsets(
                telemetry_home,
                active_resume_session_id,
            )
            result = subprocess.run(
                self._build_cmd(
                    tools or [],
                    output_last_message_path=output_path,
                    resume_session_id=active_resume_session_id,
                ),
                input=self._format_prompt(user_message),
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
                cwd=self._effective_work_dir(),
                env=env,
            )
            with open(output_path) as f:
                final_text = f.read()
            stream, meta, fallback, terminal_outcome = self._parse_jsonl_stream(
                result.stdout,
                result.stderr,
                resumed=active_resume_session_id is not None,
            )
            parsed_session_id = parse_session_id(result.stdout)
            if self._resume_session and parsed_session_id:
                self._session_id = parsed_session_id
            returned_session_id = (
                parsed_session_id
                or active_resume_session_id
                or self._session_id
            )
            meta.update(
                self._rollout_context_metadata(
                    telemetry_home,
                    returned_session_id,
                    rollout_offsets,
                )
            )
            if self._session_id:
                meta["session_id"] = self._session_id
            meta["codex_terminal_status"] = terminal_outcome.status
            self._last_metadata = meta
            final_text = final_text or fallback
            if self._log_prefix is not None:
                self._write_log(user_message, final_text, stream)
            if result.returncode != 0:
                self.logger.warning("codex exited %d: %s", result.returncode, result.stderr[:500])
            if self._resume_session and session_home is not None and self._session_id:
                self._capture_session_bundle(session_home)
            return CodexQueryResult(
                final_text,
                result.returncode,
                result.stderr,
                stream,
                session_id=returned_session_id,
                session_bundle=self._session_bundle,
                session_bundle_paths=self._session_bundle_paths,
                terminal_outcome=terminal_outcome,
            )
        finally:
            try:
                os.unlink(output_path)
            except FileNotFoundError:
                pass
            if session_home is not None:
                shutil.rmtree(session_home, ignore_errors=True)
            if session_lock_fd is not None:
                self._release_session_lock(session_lock_fd)

    def _codex_home(self) -> str:
        return os.environ.get("CODEX_HOME") or os.path.join(os.path.expanduser("~"), ".codex")

    @staticmethod
    def _rollout_paths(home: str, session_id: str | None) -> list[str]:
        if not session_id:
            return []
        pattern = os.path.join(
            home,
            "sessions",
            "*",
            "*",
            "*",
            f"rollout-*{session_id}*.jsonl",
        )
        return sorted(glob(pattern))

    @classmethod
    def _rollout_offsets(
        cls,
        home: str,
        session_id: str | None,
    ) -> dict[str, int]:
        offsets = {}
        for path in cls._rollout_paths(home, session_id):
            try:
                offsets[path] = os.path.getsize(path)
            except OSError:
                continue
        return offsets

    @classmethod
    def _rollout_context_metadata(
        cls,
        home: str,
        session_id: str | None,
        offsets: dict[str, int],
    ) -> dict[str, Any]:
        """Read only the rollout events appended by one Codex invocation."""
        events = []
        for path in cls._rollout_paths(home, session_id):
            try:
                size = os.path.getsize(path)
                offset = offsets.get(path, 0)
                if offset > size:
                    offset = 0
                with open(path, "rb") as rollout:
                    rollout.seek(offset)
                    for line in rollout:
                        try:
                            event = json.loads(line)
                        except (UnicodeDecodeError, json.JSONDecodeError):
                            continue
                        if not isinstance(event, dict):
                            continue
                        payload = _payload(event)
                        event_type = cls._event_type(event)
                        if event_type == "compacted":
                            events.append({
                                "type": "compacted",
                                "payload": {"trigger": payload.get("trigger")},
                            })
                        elif event_type == "token_count":
                            events.append(event)
            except OSError:
                continue
        return cls._context_metadata_from_rollout_events(events)

    @classmethod
    def _context_metadata_from_rollout_events(
        cls,
        events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Normalize Codex context gauges and compaction markers."""
        context_tokens = None
        peak_context_tokens = None
        model_context_window = None
        compactions: list[dict[str, Any]] = []
        pending_compaction = None
        saw_context_telemetry = False

        for event in events:
            payload = _payload(event)
            event_type = cls._event_type(event)
            if event_type == "compacted":
                trigger = payload.get("trigger")
                compactions.append({
                    "trigger": trigger if isinstance(trigger, str) else None,
                    "before_tokens": context_tokens,
                    "after_tokens": None,
                })
                pending_compaction = len(compactions) - 1
                saw_context_telemetry = True
                continue
            if event_type != "token_count":
                continue

            info = payload.get("info")
            if not isinstance(info, dict):
                continue
            window = info.get("model_context_window")
            if isinstance(window, (int, float)):
                model_context_window = int(window)
            usage = info.get("last_token_usage")
            if not isinstance(usage, dict):
                continue

            input_tokens = usage.get("input_tokens")
            total_tokens = usage.get("total_tokens")
            component_fields = (
                "input_tokens",
                "cached_input_tokens",
                "output_tokens",
                "reasoning_output_tokens",
            )
            zero_component_gauge = (
                isinstance(total_tokens, (int, float))
                and total_tokens > 0
                and all(
                    isinstance(usage.get(field), (int, float))
                    and usage.get(field) == 0
                    for field in component_fields
                )
            )
            if zero_component_gauge:
                next_context_tokens = int(total_tokens)
            elif isinstance(input_tokens, (int, float)):
                next_context_tokens = int(input_tokens)
            else:
                continue

            context_tokens = next_context_tokens
            peak_context_tokens = max(
                next_context_tokens,
                peak_context_tokens or 0,
            )
            if pending_compaction is not None:
                compactions[pending_compaction]["after_tokens"] = (
                    next_context_tokens
                )
                pending_compaction = None
            saw_context_telemetry = True

        if not saw_context_telemetry:
            return {}
        utilization = (
            context_tokens / model_context_window
            if context_tokens is not None and model_context_window else None
        )
        return {
            "context_tokens": context_tokens,
            "peak_context_tokens": peak_context_tokens,
            "model_context_window": model_context_window,
            "context_window_utilization": utilization,
            "context_compactions": compactions,
        }

    def _prepare_session_home(self) -> str:
        """Create the stable CODEX_HOME for this session and restore its state."""
        base_home = self._codex_home()
        if self._session_bundle:
            manifest = self._session_bundle_manifest(self._session_bundle)
            self._apply_session_bundle_manifest(manifest)
        session_home = self._session_home_path()
        try:
            if os.path.lexists(session_home):
                if os.path.islink(session_home) or not os.path.isdir(session_home):
                    raise ValueError(
                        f"refusing unsafe Codex session home {session_home!r}"
                    )
                shutil.rmtree(session_home)
            os.mkdir(session_home, mode=0o700)
            self._seed_session_home(base_home, session_home)
            if self._session_bundle:
                self._restore_session_bundle(session_home, self._session_bundle)
            return session_home
        except BaseException:
            shutil.rmtree(session_home, ignore_errors=True)
            raise

    def _session_root(self) -> str:
        configured = os.environ.get(_CODEX_SESSION_ROOT_ENV)
        root = os.path.abspath(
            os.path.expanduser(
                configured
                or os.path.join(os.path.expanduser("~"), ".chia-codex-sessions")
            )
        )
        os.makedirs(root, mode=0o700, exist_ok=True)
        root_stat = os.lstat(root)
        if stat.S_ISLNK(root_stat.st_mode) or not stat.S_ISDIR(root_stat.st_mode):
            raise ValueError(f"refusing unsafe Codex session root {root!r}")
        mode = stat.S_IMODE(root_stat.st_mode)
        if mode & 0o077:
            os.chmod(root, 0o700)
        return root

    def _session_home_path(self) -> str:
        root = self._session_root()
        session_home = os.path.abspath(
            os.path.join(root, self._session_storage_key)
        )
        if os.path.commonpath((root, session_home)) != root or session_home == root:
            raise ValueError(f"refusing unsafe Codex session home {session_home!r}")
        return session_home

    def _acquire_session_lock(self) -> int:
        if self._session_bundle:
            manifest = self._session_bundle_manifest(self._session_bundle)
            self._apply_session_bundle_manifest(manifest)
        root = self._session_root()
        lock_path = os.path.join(root, f".{self._session_storage_key}.lock")
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            os.close(fd)
            raise
        return fd

    @staticmethod
    def _release_session_lock(fd: int) -> None:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def _seed_session_home(self, base_home: str, session_home: str) -> None:
        for name in ("auth.json", "AGENTS.md"):
            src = os.path.join(base_home, name)
            dst = os.path.join(session_home, name)
            if not os.path.exists(src):
                continue
            try:
                shutil.copy2(src, dst, follow_symlinks=False)
            except OSError:
                self.logger.debug("failed to seed Codex session home entry %s", src, exc_info=True)
        config_src = os.path.join(base_home, "config.toml")
        if os.path.exists(config_src):
            self._seed_config_file(config_src, os.path.join(session_home, "config.toml"), base_home)

    def _seed_config_file(self, src: str, dst: str, base_home: str) -> None:
        try:
            with open(src, encoding="utf-8") as f:
                text = f.read()
            parsed = tomllib.loads(text)
            rewritten = self._rewrite_local_marketplace_sources(text, parsed, base_home)
            if rewritten == text:
                shutil.copy2(src, dst, follow_symlinks=False)
                return
            with open(dst, "w", encoding="utf-8") as f:
                f.write(rewritten)
            shutil.copystat(src, dst, follow_symlinks=False)
        except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError):
            self.logger.debug("failed to rewrite Codex config %s", src, exc_info=True)
            try:
                shutil.copy2(src, dst, follow_symlinks=False)
            except OSError:
                self.logger.debug("failed to seed Codex config %s", src, exc_info=True)

    def _rewrite_local_marketplace_sources(
        self,
        text: str,
        parsed: dict[str, Any],
        base_home: str,
    ) -> str:
        rewrites = self._local_marketplace_source_rewrites(parsed, base_home)
        if not rewrites:
            return text
        current_marketplace: str | None = None
        lines: list[str] = []
        for line in text.splitlines(keepends=True):
            stripped = line.strip()
            if stripped.startswith("[") and stripped.endswith("]") and not stripped.startswith("[["):
                section = stripped[1:-1].strip()
                current_marketplace = None
                if section.startswith("marketplaces."):
                    current_marketplace = section.split(".", 1)[1].strip('"')
            if current_marketplace in rewrites:
                match = re.match(r"^(\s*)source\s*=", line)
                if match:
                    newline = "\n" if line.endswith("\n") else ""
                    line = f"{match.group(1)}source = {json.dumps(rewrites[current_marketplace])}{newline}"
            lines.append(line)
        return "".join(lines)

    def _local_marketplace_source_rewrites(
        self,
        parsed: dict[str, Any],
        base_home: str,
    ) -> dict[str, str]:
        rewrites: dict[str, str] = {}
        marketplaces = parsed.get("marketplaces")
        if not isinstance(marketplaces, dict):
            return rewrites
        for name, config in marketplaces.items():
            if not isinstance(config, dict) or config.get("source_type") != "local":
                continue
            source = config.get("source")
            if not isinstance(source, str) or not source:
                continue
            marketplace_dir = os.path.basename(source.rstrip(os.sep))
            candidate = os.path.join(base_home, "local-marketplaces", marketplace_dir)
            if os.path.isdir(candidate) and os.path.abspath(candidate) != os.path.abspath(source):
                rewrites[str(name)] = candidate
        return rewrites

    @staticmethod
    def _session_bundle_manifest(bundle: bytes) -> dict[str, Any]:
        with tarfile.open(fileobj=BytesIO(bundle), mode="r:gz") as tar:
            for member in tar.getmembers():
                if member.name != _CODEX_SESSION_MANIFEST or not member.isfile():
                    continue
                source = tar.extractfile(member)
                if source is None:
                    continue
                with source:
                    manifest = json.loads(source.read().decode("utf-8"))
                if not isinstance(manifest, dict):
                    break
                return manifest
        raise ValueError("Codex session bundle is missing a valid manifest")

    def _apply_session_bundle_manifest(self, manifest: dict[str, Any]) -> None:
        version = manifest.get("version")
        if version != _CODEX_SESSION_MANIFEST_VERSION:
            raise ValueError(
                "unsupported Codex session bundle version "
                f"{version!r}; expected {_CODEX_SESSION_MANIFEST_VERSION}"
            )
        bundle_key = self._validated_session_storage_key(
            manifest.get("session_storage_key", "")
        )
        if (
            self._session_storage_key_explicit
            and bundle_key != self._session_storage_key
        ):
            raise ValueError(
                "Codex session bundle belongs to a different session_storage_key"
            )
        self._session_storage_key = bundle_key
        self._session_storage_key_explicit = True
        expected_home = self._session_home_path()
        bundle_home = manifest.get("session_home")
        if bundle_home != expected_home:
            raise ValueError(
                "Codex session bundle requires a different absolute session home: "
                f"bundle={bundle_home!r}, worker={expected_home!r}"
            )
        session_id = manifest.get("session_id")
        if isinstance(session_id, str) and session_id:
            self._session_id = session_id

    def _restore_session_bundle(self, session_home: str, bundle: bytes) -> None:
        manifest = self._session_bundle_manifest(bundle)
        self._apply_session_bundle_manifest(manifest)
        with tarfile.open(fileobj=BytesIO(bundle), mode="r:gz") as tar:
            for member in tar.getmembers():
                if not member.isfile():
                    continue
                rel_path = member.name
                if rel_path == _CODEX_SESSION_MANIFEST:
                    continue
                if self._unsafe_bundle_path(rel_path):
                    continue
                if not self._is_session_bundle_file(rel_path):
                    continue
                target = os.path.join(session_home, rel_path)
                os.makedirs(os.path.dirname(target), exist_ok=True)
                source = tar.extractfile(member)
                if source is None:
                    continue
                with source, open(target, "wb") as f:
                    shutil.copyfileobj(source, f)

    @staticmethod
    def _unsafe_bundle_path(rel_path: str) -> bool:
        normalized = rel_path.replace("\\", "/")
        return (
            normalized.startswith("/")
            or any(part == ".." for part in normalized.split("/"))
        )

    def _session_bundle_files(self, session_home: str) -> list[tuple[str, str]]:
        paths: list[tuple[str, str]] = []
        for root, _dirs, files in os.walk(session_home):
            for name in files:
                full = os.path.join(root, name)
                rel = os.path.relpath(full, session_home).replace(os.sep, "/")
                if self._is_session_bundle_file(rel):
                    paths.append((rel, full))
        return sorted(paths)

    def _is_session_bundle_file(self, rel_path: str) -> bool:
        rel_path = rel_path.replace("\\", "/")
        base = os.path.basename(rel_path)
        if re.fullmatch(r"state_\d+\.sqlite", base):
            return True
        return (
            rel_path.startswith("sessions/")
            and base.startswith("rollout-")
            and base.endswith(".jsonl")
        )

    def _capture_session_bundle(self, session_home: str) -> None:
        paths = self._session_bundle_files(session_home)
        if not paths and not self._session_id:
            return
        buf = BytesIO()
        bundle_paths: list[str] = []
        with tempfile.TemporaryDirectory(prefix="chia-codex-snapshot-") as snapshot_dir:
            with tarfile.open(fileobj=buf, mode="w:gz") as tar:
                manifest = json.dumps(
                    {
                        "version": _CODEX_SESSION_MANIFEST_VERSION,
                        "session_id": self._session_id,
                        "session_storage_key": self._session_storage_key,
                        "session_home": os.path.abspath(session_home),
                    },
                    sort_keys=True,
                ).encode("utf-8")
                info = tarfile.TarInfo(_CODEX_SESSION_MANIFEST)
                info.mode = 0o600
                info.size = len(manifest)
                tar.addfile(info, BytesIO(manifest))
                bundle_paths.append(_CODEX_SESSION_MANIFEST)
                for rel_path, full in paths:
                    source = full
                    if rel_path.endswith(".sqlite"):
                        source = os.path.join(snapshot_dir, rel_path)
                        os.makedirs(os.path.dirname(source), exist_ok=True)
                        self._snapshot_sqlite(full, source)
                    tar.add(source, arcname=rel_path, recursive=False)
                    bundle_paths.append(rel_path)
        self._session_bundle = buf.getvalue()
        self._session_bundle_paths = tuple(bundle_paths)

    @staticmethod
    def _snapshot_sqlite(source: str, destination: str) -> None:
        """Create a complete SQLite backup without copying WAL/SHM files."""
        source_db = sqlite3.connect(source)
        destination_db = sqlite3.connect(destination)
        try:
            source_db.backup(destination_db)
        finally:
            destination_db.close()
            source_db.close()

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
    def _parse_jsonl_stream(
        cls,
        stdout: str,
        stderr: str = "",
        *,
        resumed: bool = False,
    ) -> tuple[str, dict, str, CodexTerminalOutcome]:
        stream_parts: list[str] = []
        result_parts: list[str] = []
        events: list[dict[str, Any]] = []
        for line in stdout.splitlines():
            event = cls._json_or_none(line)
            if event is None:
                stream_parts.append(f"[UNPARSED] {_truncate(line.strip(), 200)}\n")
                continue
            events.append(event)
            cls._record_event(event, stream_parts, result_parts)
        meta = cls._usage_for_latest_task(events, resumed=resumed)
        terminal_outcome = cls._terminal_outcome(events)
        if stderr:
            stream_parts.append(f"[stderr]\n{_truncate(stderr)}\n\n")
        return (
            "".join(stream_parts),
            meta,
            "".join(result_parts),
            terminal_outcome,
        )

    @staticmethod
    def _json_or_none(line: str) -> dict | None:
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            return None

    @classmethod
    def _record_event(cls, event: dict, stream: list[str], results: list[str]) -> None:
        payload = _payload(event)
        etype = cls._event_type(event)
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
    def _usage_values(usage: dict | None) -> dict[str, int]:
        """Normalize one provider usage object without changing subset semantics."""
        if not isinstance(usage, dict):
            return {}
        values: dict[str, int] = {}
        for dest, sources in _TOKEN_ALIASES.items():
            for source in sources:
                value = usage.get(source)
                if isinstance(value, (int, float)):
                    values[dest] = int(value)
                    break
        cache = usage.get("cache")
        if isinstance(cache, dict):
            if isinstance(cache.get("read"), (int, float)):
                values["cache_read_input_tokens"] = int(cache["read"])
            if isinstance(cache.get("write"), (int, float)):
                values["cache_creation_input_tokens"] = int(cache["write"])
        return values

    @staticmethod
    def _event_type(event: dict) -> str:
        payload = _payload(event)
        return str(payload.get("type") or event.get("type") or "").lower()

    @classmethod
    def _terminal_outcome(
        cls,
        events: list[dict[str, Any]],
    ) -> CodexTerminalOutcome:
        """Read the latest turn's structured outcome from Codex JSONL.

        Codex reports one failed request with both a top-level ``error`` event
        and a following ``turn.failed`` event.  The former describes the
        error; it is not a second turn ending.  Prefer an unambiguous turn
        outcome and use a top-level error only when no turn outcome exists.
        """
        latest_turn_start = None
        for index, event in enumerate(events):
            if str(event.get("type") or "").lower() == "turn.started":
                latest_turn_start = index

        scoped_events = (
            events[latest_turn_start:]
            if latest_turn_start is not None
            else events
        )
        turn_outcomes: list[CodexTerminalOutcome] = []
        stream_errors: list[str] = []
        for event in scoped_events:
            # Lifecycle state comes from the top-level protocol event. In
            # particular, item.completed can contain item.type == "error"
            # without failing the turn.
            event_type = str(event.get("type") or "").lower()
            if event_type == "turn.completed":
                turn_outcomes.append(CodexTerminalOutcome("completed"))
            elif event_type == "turn.failed":
                error = event.get("error")
                message = cls._text(error) if isinstance(error, dict) else ""
                turn_outcomes.append(CodexTerminalOutcome("failed", message))
            elif event_type == "error":
                stream_errors.append(
                    cls._text(event) or json.dumps(event, sort_keys=True)
                )

        if len(turn_outcomes) == 1:
            return turn_outcomes[0]
        if len(turn_outcomes) > 1:
            return CodexTerminalOutcome(
                "invalid",
                "Codex JSONL stream contained multiple turn outcomes for the latest turn",
            )
        if stream_errors:
            return CodexTerminalOutcome("fatal", stream_errors[-1])
        return CodexTerminalOutcome(
            "missing",
            "Codex JSONL stream ended without a terminal turn event",
        )

    @classmethod
    def _usage_for_latest_task(
        cls,
        events: list[dict[str, Any]],
        *,
        resumed: bool = False,
    ) -> dict[str, Any]:
        """Return additive usage for only the task started by this invocation.

        ``codex exec resume --json`` may replay token-count events for the
        complete session. Those events include cumulative session totals, so
        summing the whole stream charges every prior task again.
        """
        latest_task_start = None
        latest_turn_start = None
        for index, event in enumerate(events):
            event_type = cls._event_type(event)
            if "task" in event_type and any(
                token in event_type for token in ("start", "begin", "created")
            ):
                latest_task_start = index
            if "turn" in event_type and any(
                token in event_type for token in ("start", "begin")
            ):
                latest_turn_start = index
        start = (
            latest_task_start
            if latest_task_start is not None
            else latest_turn_start
            if latest_turn_start is not None
            else 0
        )

        baseline_total: dict[str, int] = {}
        for event in events[:start]:
            info = _payload(event).get("info")
            if not isinstance(info, dict):
                continue
            values = cls._usage_values(info.get("total_token_usage"))
            if values:
                baseline_total = values

        direct_usage_events: list[dict[str, int]] = []
        last_usage_events: list[dict[str, int]] = []
        latest_total: dict[str, int] = {}
        turns = 0
        for event in events[start:]:
            payload = _payload(event)
            event_type = cls._event_type(event)
            if "turn" in event_type and any(
                token in event_type for token in ("complete", "end", "done")
            ):
                turns += 1

            info = payload.get("info")
            if isinstance(info, dict):
                values = cls._usage_values(info.get("last_token_usage"))
                if values:
                    last_usage_events.append(values)
                values = cls._usage_values(info.get("total_token_usage"))
                if values:
                    latest_total = values

            direct_usage = (
                payload.get("usage")
                or payload.get("tokens")
                or payload.get("token_usage")
            )
            values = cls._usage_values(direct_usage)
            if values and "token_count" not in event_type:
                direct_usage_events.append(values)

        usage: dict[str, int] = {}
        usage_source = "unavailable"
        if direct_usage_events:
            for values in direct_usage_events:
                for key, value in values.items():
                    usage[key] = usage.get(key, 0) + value
            usage_source = "turn_usage"
        elif latest_total and baseline_total:
            keys = set(latest_total) | set(baseline_total)
            candidate = {
                key: latest_total.get(key, 0) - baseline_total.get(key, 0)
                for key in keys
            }
            if all(value >= 0 for value in candidate.values()):
                usage = candidate
                usage_source = "session_total_delta"
        elif latest_total and not resumed:
            usage = dict(latest_total)
            usage_source = "session_total"

        if not usage and last_usage_events:
            # Some CLI versions expose only per-subturn snapshots. Deduplicate
            # replayed copies while retaining distinct subturns.
            seen: set[tuple[tuple[str, int], ...]] = set()
            for values in last_usage_events:
                signature = tuple(sorted(values.items()))
                if signature in seen:
                    continue
                seen.add(signature)
                for key, value in values.items():
                    usage[key] = usage.get(key, 0) + value
            usage_source = "latest_task_usage"

        meta: dict[str, Any] = {
            **usage,
            "usage_source": usage_source,
            "num_turns": turns,
        }
        if latest_total:
            meta["session_total_usage"] = latest_total
        return meta

    def _classify_error(self, cli: QueryResult) -> None:
        outcome = getattr(cli, "terminal_outcome", None)
        if outcome is None:
            outcome = CodexTerminalOutcome(
                "missing",
                "Codex result did not include a JSONL terminal outcome",
            )

        # Match normal CLI process semantics.  A zero exit status accompanied
        # by the requested final-message output is a usable successful call.
        # JSONL lifecycle records remain valuable for failure classification,
        # but inconsistencies or duplicate diagnostic events must not override
        # a response that Codex itself completed and wrote to the output file.
        if cli.returncode == 0 and bool((cli.result or "").strip()):
            return

        node_id = self._get_node_id()
        message = outcome.message or cli.stderr
        if cli.returncode == 0 and outcome.status != "failed":
            message = (
                "Codex exited successfully but did not write a final message "
                f"(terminal status: {outcome.status})"
            )
        elif not message:
            message = f"Codex terminal status: {outcome.status}"
        if outcome.status != "failed":
            raise UnknownCodexError(
                node_id=node_id,
                exit_code=cli.returncode,
                raw_message=message,
            )

        # Codex commonly embeds provider errors as JSON text inside the
        # turn.failed message (for example ``"status":400`` and
        # ``"type":"invalid_request_error"``). Normalize JSON punctuation
        # for classification while preserving the original message in the
        # exception and logs.
        classification_text = message.replace("_", " ").replace('"', " ")

        if _MODEL_CAPACITY_RE.search(message):
            raise ModelCapacityError(
                node_id=node_id,
                exit_code=cli.returncode,
                raw_message=message,
            )
        if _MAX_OUTPUT_RE.search(classification_text):
            raise MaxOutputTokensError(
                node_id=node_id,
                exit_code=cli.returncode,
                raw_message=message,
            )
        if _RATE_LIMIT_RE.search(classification_text) or _RATE_LIMIT_429_RE.search(classification_text):
            raise RateLimitError(
                node_id=node_id,
                reset_time=parse_rate_limit_reset(message),
                raw_message=message,
                exit_code=cli.returncode,
            )
        for error_cls, patterns in (
            (AuthenticationError, (_AUTHENTICATION_RE, _AUTHENTICATION_STATUS_RE)),
            (BillingError, (_BILLING_RE,)),
            (InvalidRequestError, (_INVALID_REQUEST_RE, _INVALID_REQUEST_STATUS_RE)),
            (ServerError, (_SERVER_RE, _SERVER_STATUS_RE)),
        ):
            if any(pattern.search(classification_text) for pattern in patterns):
                raise error_cls(
                    node_id=node_id,
                    exit_code=cli.returncode,
                    raw_message=message,
                )
        raise UnknownCodexError(
            node_id=node_id,
            exit_code=cli.returncode,
            raw_message=message,
        )
