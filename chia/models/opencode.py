"""opencode CLI LLM backend.

:class:`OpenCodeLLM` wraps the ``opencode`` CLI (https://opencode.ai) as an LLM
backend, opencode is provider-agnostic: the model is given as ``provider/model`` (e.g.
``anthropic/claude-sonnet-4-6``) and opencode runs its own server-side agentic
tool loop, so there is no client-side MCP loop here.

WARNING: experimental. Only exercised by the tests in
chia/models/tests/test_opencode.py (mocked unit tests, plus opt-in live tests).
Not validated in production. Auth is environment-driven: opencode uses its own
stored credentials (``opencode auth login``) or provider env vars.
"""

from __future__ import annotations

import json
import logging
import math
import os
import random
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING, Dict, List, Optional, Union

import ray

from chia.base.ChiaFunction import ChiaFunction, ObjectRefCallback
from chia.base.llm_call import QueryResult, LLMCallBase

if TYPE_CHECKING:
    from chia.base.tools.ChiaTool import ChiaTool


# ---------------------------------------------------------------------------
# Exceptions
#
# A parallel taxonomy to claude.py. Kept separate so this module stands alone;
# each carries ``__reduce__`` for Ray serialization.
# ---------------------------------------------------------------------------


class OpenCodeError(Exception):
    """Base for all opencode CLI errors."""

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


class RateLimitError(OpenCodeError):
    """The provider behind opencode reported a usage/rate limit."""

    def __init__(
        self,
        node_id: str,
        reset_time: datetime,
        raw_message: str = "",
        exit_code: int = -1,
    ):
        self.reset_time = reset_time
        super().__init__(node_id, "rate_limit", exit_code, raw_message)

    def __reduce__(self):
        return (
            self.__class__,
            (self.node_id, self.reset_time, self.raw_message, self.exit_code),
        )


class AuthenticationError(OpenCodeError):
    """opencode has no/invalid credentials for the selected provider."""

    def __init__(self, node_id: str, exit_code: int = -1, raw_message: str = ""):
        super().__init__(node_id, "authentication_failed", exit_code, raw_message)

    def __reduce__(self):
        return (self.__class__, (self.node_id, self.exit_code, self.raw_message))


class BillingError(OpenCodeError):
    """The provider account has a billing/payment problem."""

    def __init__(self, node_id: str, exit_code: int = -1, raw_message: str = ""):
        super().__init__(node_id, "billing_error", exit_code, raw_message)

    def __reduce__(self):
        return (self.__class__, (self.node_id, self.exit_code, self.raw_message))


class InvalidRequestError(OpenCodeError):
    """Malformed request — bad model string, invalid config, unknown agent, etc."""

    def __init__(self, node_id: str, exit_code: int = -1, raw_message: str = ""):
        super().__init__(node_id, "invalid_request", exit_code, raw_message)

    def __reduce__(self):
        return (self.__class__, (self.node_id, self.exit_code, self.raw_message))


class ServerError(OpenCodeError):
    """Transient provider/server-side failure (5xx, overloaded, connection)."""

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


class ModelCapacityError(ServerError):
    """The provider temporarily lacks capacity for this model."""

    def __init__(self, node_id: str, exit_code: int = -1,
                 raw_message: str = "", retry_after: Optional[int] = None):
        self.retry_after = retry_after
        OpenCodeError.__init__(self, node_id, "model_capacity", exit_code, raw_message)


class MaxOutputTokensError(OpenCodeError):
    """The response was truncated at the output token limit."""

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


class UnknownOpenCodeError(OpenCodeError):
    """Unclassified opencode CLI error."""

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
# Session-id parser
# ---------------------------------------------------------------------------

_SESSION_ID_RE = re.compile(r"\bses_[A-Za-z0-9]+\b")


def parse_session_id(stdout: str) -> Optional[str]:
    """Pull the opencode session id out of ``run --format json`` stdout.

    Each event is ``{type, sessionID, part:{sessionID, ...}}``; the opening
    ``step_start`` line reliably carries it. Falls back to a regex scan if the
    JSON shape changes.
    """
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        sid = event.get("sessionID") or (event.get("part") or {}).get("sessionID")
        if sid:
            return sid
    m = _SESSION_ID_RE.search(stdout)
    return m.group(0) if m else None


def parse_run_error(stdout: str) -> Optional[dict]:
    """Pull the first structured error out of ``run --format json`` stdout.

    opencode emits ``{"type":"error", "sessionID":..., "error":{name, data}}``
    events for failures that happen before/around the model request — notably an
    unknown model id, which surfaces as ``{"name":"UnknownError","data":{"message":
    "Model not found: ..."}}``. These never reach the session export's
    ``messages[].info.error`` because no assistant message is ever created, so
    the run stream is the *only* place they appear (confirmed against opencode
    1.15.13). Returns the first such ``error`` dict (``{name, data}``), or
    ``None``. Genuine provider errors (e.g. APIError 401) appear in *both* the run
    stream and the export; the export copy is richer (full ``responseHeaders``),
    so callers prefer it and use this only as a fallback.
    """
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("type") == "error":
            err = event.get("error")
            if isinstance(err, dict) and "name" in err:
                return err
    return None


# ---------------------------------------------------------------------------
# Custom model providers
# ---------------------------------------------------------------------------


@dataclass
class AdditionalModelProvider:
    """A custom model provider to inject into the opencode config's ``provider`` block.

    opencode is provider-agnostic: beyond its built-in providers you can declare a
    custom or self-hosted one — an OpenAI-compatible endpoint, a private gateway, a
    local vLLM/Ollama server, etc. — directly in the config under ``provider.<id>``.
    Each entry names the AI-SDK loader package (``npm``), a display ``name``, the SDK
    ``options`` (notably ``base_url`` / ``api_key``), and the ``models`` the provider
    serves. See https://opencode.ai/docs/providers (Custom providers) for the schema.

    Once declared, select one of its models by passing ``model="<id>/<model-id>"`` to
    :class:`OpenCodeLLM` — e.g. ``AdditionalModelProvider(id="my-vllm", ...)`` exposes
    its models as ``my-vllm/<model-id>``.

    Attributes:
        id: Provider key. Used both as the key under ``provider`` in the config and
            as the ``provider`` half of ``provider/model``. Must be unique.
        models: Either a list of model-id strings (each expands to a bare ``{}``
            entry) or a dict mapping model-id -> model config, e.g.
            ``{"name": ..., "limit": {...}, "cost": {...}, "options": {...}}``.
        npm: The AI-SDK package opencode loads to talk to this provider. Defaults to
            ``@ai-sdk/openai-compatible``, which fits any OpenAI-compatible endpoint.
        name: Human-readable display name (defaults to ``id`` when omitted).
        base_url: Endpoint URL; written to ``options.baseURL``. Optional if supplied
            via ``options`` instead.
        api_key: Credential; written to ``options.apiKey``. May be a literal secret
            or an opencode ``{env:NAME}`` template that opencode expands at runtime
            (preferred — keeps the secret out of the on-disk config file).
        options: Extra SDK options merged into the provider's ``options`` block
            (e.g. ``headers``, custom timeouts). On conflict these win over
            ``base_url`` / ``api_key``.
    """

    id: str
    models: Union[List[str], Dict[str, dict]]
    npm: str = "@ai-sdk/openai-compatible"
    name: Optional[str] = None
    base_url: Optional[str] = None
    api_key: Optional[str] = None
    options: Optional[Dict[str, object]] = None

    def to_config_entry(self) -> dict:
        """Render this provider as an opencode ``provider.<id>`` config value."""
        options: dict = {}
        if self.base_url is not None:
            options["baseURL"] = self.base_url
        if self.api_key is not None:
            options["apiKey"] = self.api_key
        if self.options:
            options.update(self.options)

        # A list of ids -> bare ``{id: {}}`` entries; a dict is passed through.
        if isinstance(self.models, dict):
            models = dict(self.models)
        else:
            models = {model_id: {} for model_id in self.models}

        entry: dict = {"npm": self.npm, "name": self.name or self.id, "models": models}
        if options:
            entry["options"] = options
        return entry


@dataclass
class OpenCodeQueryResult(QueryResult):
    """:class:`QueryResult` specialised for the opencode CLI.

    ``usage`` holds the run's summed metrics from ``opencode export`` (the same
    dict pushed to the profiler as ``_last_metadata``): ``input_tokens``,
    ``output_tokens``, ``reasoning_tokens``, ``cache_read``, ``cache_write``,
    ``cost_usd`` (from opencode's model price table) and ``num_turns``.
    ``session_id`` is opencode's id for the session the run created or resumed.
    With ``resume_session=True``, ``session_transcript`` is the portable JSON
    export as bytes. ``call_export`` contains only this call's new messages;
    ``usage`` and ``stream_result`` likewise exclude previous calls. Output and
    reasoning token counts are separate in OpenCode's native usage schema.
    """

    usage: Optional[dict] = None
    session_id: Optional[str] = None
    session_transcript: Optional[bytes] = None
    call_export: Optional[dict] = None


def _session_tracked(chia_fn):
    """Attach OpenCode session sync to remote prompt calls when persistence is on."""

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


class OpenCodeLLM(LLMCallBase):
    """Wraps the ``opencode`` CLI as an LLM backend.

    Each :meth:`prompt` call runs ``opencode run`` (to create a session and get
    its id) then ``opencode export`` (to read the assistant response + usage
    from opencode's local DB). Returns the same :class:`QueryResult` shape as the
    other backends; ``returncode`` is the ``run`` exit code.

    Set ``resume_session=True`` to keep conversation history across sequential
    calls, including calls scheduled on different workers. ``get()`` synchronizes
    the returned export onto this instance after remote calls. Each invocation
    imports into a temporary private database and exports before removing it.
    Credentials and workspace files are not transported. ``work_dir`` must be
    available on each worker; if omitted in resume mode, a directory under the
    worker's temporary directory is used. Without resume mode, calls remain
    independent as before.

    ``capacity_attempts`` bounds temporary capacity and rate-limit failures
    across one prompt (default 8). Retries wait 15 seconds, doubling to a
    300-second base with 20% jitter, or longer when Retry-After requires it.
    Authentication, billing and invalid-request errors are not retried.
    """

    # Honors both --dangerously-skip-permissions and a `permission` config block.
    supports_dangerously_skip_permissions = True
    supports_config = True

    def __init__(
        self,
        model: Optional[str] = None,
        system_message: str = "",
        timeout_seconds: int = 600,
        retries: int = 3,
        logging_name: str = "opencode",
        logging_level: int = logging.DEBUG,
        log_dir: Optional[str] = None,
        opencode_bin: str = "opencode",
        agent_name: str = "chia",
        work_dir: Optional[str] = None,
        extra_cli_args: Optional[List[str]] = None,
        additional_providers: Optional[List[AdditionalModelProvider]] = None,
        dangerously_skip_permissions: bool = True,
        config: Optional[dict] = None,
        resume_session: bool = False,
        capacity_attempts: int = 8,
    ):
        super().__init__(system_message=system_message,
                         dangerously_skip_permissions=dangerously_skip_permissions,
                         config=config)
        self.logging_level = logging_level
        self.logging_name = logging_name
        self.retries = retries
        if isinstance(capacity_attempts, bool) or not isinstance(capacity_attempts, int) or capacity_attempts < 1:
            raise ValueError("capacity_attempts must be a positive integer")
        self.capacity_attempts = capacity_attempts
        self.timeout_seconds = timeout_seconds
        self.model = model
        self.opencode_bin = opencode_bin
        self.agent_name = agent_name
        self.work_dir = os.path.abspath(work_dir) if work_dir else (
            os.path.join(tempfile.gettempdir(), "chia-opencode-work") if resume_session else None
        )
        self._resume_session = resume_session
        self._session_export: Optional[dict] = None
        self._call_previous_export: Optional[dict] = None
        self.extra_cli_args = extra_cli_args or []
        self.additional_providers = additional_providers or []
        self.logger = logging.getLogger(logging_name)
        self._last_metadata: dict = {}
        self._last_export_error: Optional[dict] = None

        self.logger.warning(
            "OpenCodeLLM is experimental: only exercised by unit tests so far, "
            "not validated in production."
        )

        # opencode falls back to its own configured default model when none is
        # passed on the CLI, so model is optional here — just say so.
        if self.model is None:
            self.logger.info(
                "OpenCodeLLM: no model specified; opencode will use its "
                "configured default model."
            )

        self._log_dir = log_dir
        if log_dir is not None:
            os.makedirs(log_dir, exist_ok=True)
            run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
            self._log_prefix = os.path.join(log_dir, f"{logging_name}_{run_id}")
        else:
            self._log_prefix = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def restore_session(self, transcript: Optional[bytes]) -> None:
        """Restore an ``OpenCodeQueryResult.session_transcript`` on any worker.

        The export carries conversation history, not credentials or workspace
        files. Calls within one session must be sequential.
        """
        if not self._resume_session:
            raise ValueError("Session restoration requires resume_session=True")
        export = json.loads(transcript) if transcript else None
        if export is not None:
            session_id = export.get("info", {}).get("id", "")
            if not isinstance(session_id, str) or not re.fullmatch(r"ses_[A-Za-z0-9]+", session_id):
                raise ValueError("Invalid OpenCode session ID in exported state")
            self._current_call_export(None, export)
        self._session_export = export

    def _sync_session(self, result):
        if result is not None and result.session_transcript is not None:
            self.restore_session(result.session_transcript)
        return result

    @staticmethod
    def _current_call_export(previous: Optional[dict], current: dict) -> dict:
        """Select only new messages so resumed history is never billed twice."""
        def messages(export):
            entries = export.get("messages")
            if not isinstance(entries, list):
                raise ValueError("OpenCode session export has no messages")
            ids = [m.get("info", {}).get("id") for m in entries]
            if any(not isinstance(i, str) or not i for i in ids) or len(set(ids)) != len(ids):
                raise ValueError("OpenCode export has missing or duplicate message IDs")
            return dict(zip(ids, entries))

        old = messages(previous) if previous else {}
        new = messages(current)
        if not old.keys() <= new.keys():
            raise ValueError("OpenCode resume lost previous messages")
        for key, message in old.items():
            if message.get("info", {}).get("role") == "assistant":
                for field in ("tokens", "cost"):
                    if message["info"].get(field) != new[key]["info"].get(field):
                        raise ValueError("OpenCode changed previously accounted usage")
        return {"messages": [m for key, m in new.items() if key not in old]}

    @_session_tracked
    @ChiaFunction(resources={"opencode_creds": 0.01})
    def prompt(
        self,
        user_message: str,
        tools: Optional[List[ChiaTool]] = [],
    ) -> QueryResult:
        """Send *user_message* to opencode and return the response.

        Returns:
            :class:`QueryResult` with ``success=True`` when opencode ran cleanly,
            or ``success=False`` when every retry attempt failed.

        Raises:
            RateLimitError / ModelCapacityError: after capacity_attempts failures.
            AuthenticationError / BillingError / InvalidRequestError: immediately.
            ServerError: after all retries with exponential backoff.
            MaxOutputTokensError: after one retry attempt.
        """
        import time as _time

        from chia.trace.profiler import get_profiler

        profiler = get_profiler()
        self._call_previous_export = self._session_export
        capacity_attempt = 0

        for attempt in range(self.retries):
            try:
                while True:
                    try:
                        self._last_metadata = {}
                        self._last_export_error = None
                        cli = self._run_opencode(user_message, tools)
                        self._last_metadata["model"] = self.model or "<opencode default>"
                        self._last_metadata["tools"] = [
                            {"name": t.name, "hostname": getattr(t, "hostname", None),
                             "port": getattr(t, "port", None),
                             "node_id": getattr(t, "node_id", None)}
                            for t in tools
                        ]
                        if profiler.enabled and self._last_metadata:
                            profiler.add_info(self._last_metadata)

                        self._classify_error(
                            cli, export_error=getattr(self, "_last_export_error", None),
                        )
                        break
                    except (RateLimitError, ModelCapacityError) as exc:
                        capacity_attempt += 1
                        if capacity_attempt >= self.capacity_attempts:
                            self.logger.warning(
                                "%s exhausted after %d/%d capacity attempts",
                                exc.error_type, capacity_attempt, self.capacity_attempts,
                            )
                            raise
                        delay = min(15 * 2 ** (capacity_attempt - 1), 300)
                        delay *= random.uniform(0.8, 1.2)
                        if isinstance(exc, RateLimitError):
                            delay = max(delay, (exc.reset_time - datetime.now(timezone.utc)).total_seconds())
                        elif exc.retry_after is not None:
                            delay = max(delay, exc.retry_after)
                        self.logger.warning(
                            "%s on capacity attempt %d/%d, retrying in %.1fs",
                            exc.error_type, capacity_attempt, self.capacity_attempts, delay,
                        )
                        _time.sleep(delay)

                cli.success = True
                return cli

            # -- Permanent failures or exhausted capacity retries --
            except (RateLimitError, ModelCapacityError, AuthenticationError, BillingError, InvalidRequestError):
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

            # -- Retry with exponential backoff: transient service issue --
            except ServerError:
                backoff = min(5 * 2 ** attempt, 60)
                self.logger.warning(
                    "Server error on attempt %d/%d, backing off %ds",
                    attempt + 1, self.retries, backoff,
                )
                if attempt + 1 < self.retries:
                    _time.sleep(backoff)

            except UnknownOpenCodeError as exc:
                self.logger.warning(
                    "Unknown error on attempt %d/%d: %s",
                    attempt + 1, self.retries, exc,
                )

            except subprocess.TimeoutExpired:
                self.logger.warning(
                    "Timeout on attempt %d/%d", attempt + 1, self.retries,
                )

            except Exception as exc:
                self.logger.warning(
                    "Unexpected error on attempt %d/%d: %s",
                    attempt + 1, self.retries, exc,
                )
        return OpenCodeQueryResult(
            result="", returncode=-1, stderr="", stream_result="", success=False,
            session_transcript=json.dumps(self._session_export).encode() if self._session_export else None,
        )

    def _get_node_id(self) -> str:
        try:
            return ray.get_runtime_context().get_node_id()
        except Exception:
            return "unknown"

    @staticmethod
    def _retry_reset_time(headers: dict) -> datetime:
        """Parse Retry-After seconds or HTTP date; otherwise use local backoff."""
        now = datetime.now(timezone.utc)
        value = next((v for k, v in headers.items() if k.lower() == "retry-after"), None)
        if value is None:
            return now
        try:
            seconds = float(value)
            if math.isfinite(seconds) and seconds >= 0:
                return now + timedelta(seconds=seconds)
        except (TypeError, ValueError, OverflowError):
            pass
        try:
            date = parsedate_to_datetime(str(value))
            if date.tzinfo is not None:
                return max(now, date)
        except (TypeError, ValueError, OverflowError):
            pass
        return now

    def _classify_error(self, cli: QueryResult,
                        export_error: Optional[dict] = None) -> None:
        """Inspect *cli* and *export_error* and raise a typed error if wrong.

        ``opencode run`` almost always exits 0 even on failure (opencode bug
        #14551), so the exit code alone is not trustworthy. Logic:

        1. Clean run (exit 0, non-empty result, no structured error) -> return.
        2. Structured ``export_error`` (a ``{name, data}`` object from the
           session export or run stream) -> map to a typed error. This is the
           only reliable signal and the only thing we classify on.
        3. Any other failure (no structured error: a CLI/process-level failure
           on stderr, or an empty response) -> :class:`UnknownOpenCodeError`
           with the raw stderr attached. We deliberately do NOT keyword-match
           stderr: opencode emits its real errors as structured JSON (handled by
           (2)), so stderr only carries CLI/usage text, and guessing a type from
           it was imprecise — better to surface it honestly as unknown.

        Note the guard requires ``not export_error``: a structured error is
        honored even on an exit-0 run that returned partial text.
        """
        if cli.returncode == 0 and cli.result and not export_error:
            return

        node_id = self._get_node_id()

        # -- Path A: structured error from the session export (preferred) --
        if export_error:
            name = export_error.get("name", "")
            data = export_error.get("data", {}) or {}
            message = data.get("message", "") or ""
            status = data.get("statusCode")

            headers = data.get("responseHeaders", {}) or {}
            reset_time = self._retry_reset_time(headers)

            # Permanent failures win over 429 when an exhausted account uses
            # that status too. Ordinary request/token rate limits remain retryable.
            if name == "ProviderAuthError" or status in (401, 403):
                raise AuthenticationError(node_id, cli.returncode, message)
            exhausted = re.search(
                r"insufficient[_ ]quota|insufficient (?:credits?|balance)|"
                r"(?:credits?|balance|spending limit).{0,30}(?:exhausted|depleted|exceeded)|"
                r"out of credits|payment required", message, re.I,
            )
            if status == 402 or exhausted:
                raise BillingError(node_id, cli.returncode, message)
            if status == 429:
                raise RateLimitError(
                    node_id=node_id, reset_time=reset_time,
                    raw_message=message, exit_code=cli.returncode,
                )
            if name == "APIError" and any(kw in message.lower() for kw in (
                "billing", "quota", "payment", "credit", "subscription", "plan",
            )):
                raise BillingError(node_id, cli.returncode, message)

            if name == "APIError" and (status is None or status >= 500) and re.search(
                r"overloaded|(?:at|model|available) capacity|capacity (?:exceeded|unavailable)",
                message, re.I,
            ):
                raise ModelCapacityError(
                    node_id, cli.returncode, message,
                    max(0, (reset_time - datetime.now(timezone.utc)).total_seconds()),
                )

            # Output token limit / context overflow.
            if name in ("ContextOverflowError", "MessageOutputLengthError"):
                raise MaxOutputTokensError(
                    node_id, cli.returncode, message, partial_text=cli.result,
                )

            # Server error (5xx or explicitly retryable) vs. invalid request.
            if name == "APIError":
                if (status and status >= 500) or data.get("isRetryable"):
                    raise ServerError(
                        node_id, exit_code=cli.returncode, raw_message=message,
                    )
                raise InvalidRequestError(node_id, cli.returncode, message)

            # Any other structured error (MessageAbortedError, UnknownError, ...).
            raise UnknownOpenCodeError(
                node_id, cli.returncode, message or str(export_error),
                stderr=cli.stderr,
            )

        # No structured error: a CLI/process-level failure (its message is on
        # stderr) or an empty response. opencode surfaces its real errors as
        # structured JSON (handled above), so there's nothing reliable to
        # classify here — report it honestly as unknown with the stderr attached.
        raise UnknownOpenCodeError(
            node_id, cli.returncode, cli.stderr[:300] or "empty response",
            stderr=cli.stderr,
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _build_config(self, tools: List[ChiaTool]) -> dict:
        """Build the opencode config (written to OPENCODE_CONFIG).

        Defines the ``chia`` agent carrying our system prompt, one remote MCP
        server per ChiaTool, and any custom model providers passed at construction.
        """
        cfg: dict = {
            "$schema": "https://opencode.ai/config.json",
            "agent": {
                self.agent_name: {
                    "mode": "primary",
                    "prompt": self.system_message or "You are a helpful assistant.",
                }
            },
        }
        if tools:
            mcp: dict = {}
            for tool in tools:
                port = getattr(tool, "port", 8000)
                mcp[tool.name] = {
                    "type": "remote",
                    "url": f"http://{tool.hostname}:{port}/{tool.name}/mcp",
                    "enabled": True,
                }
            cfg["mcp"] = mcp

        # Config block. Defaults to allowing all tools — notably
        # external_directory, whose "ask" default blocks a non-interactive run
        # (the --dangerously-skip-permissions flag does NOT cover it).
        # Override via the `permission` kwarg.
        cfg["permission"] = self.config if self.config is not None else {
            "edit": "allow",
            "bash": "allow",
            "webfetch": "allow",
            "external_directory": "allow",
        }
        if self.additional_providers:
            provider_cfg: dict = cfg.setdefault("provider", {})
            for provider in self.additional_providers:
                provider_cfg[provider.id] = provider.to_config_entry()
        return cfg

    def _build_run_cmd(self, user_message: str) -> list:
        """Build the ``opencode run`` command list (message is a positional arg)."""
        cmd = [
            self.opencode_bin,
            "run",
            "--format", "json",
            "--agent", self.agent_name,
        ]
        if self.dangerously_skip_permissions:
            cmd.append("--dangerously-skip-permissions")
        if self.model:  # omit --model so opencode uses its configured default
            cmd += ["--model", self.model]
        if self.work_dir:
            cmd += ["--dir", self.work_dir]
        if self.extra_cli_args:
            cmd += self.extra_cli_args
        cmd.append(user_message)
        return cmd

    def _run_opencode(
        self,
        user_message: str,
        tools: Optional[List[ChiaTool]] = None,
    ) -> QueryResult:
        """Run ``opencode run`` then ``opencode export`` and assemble a QueryResult."""
        env = dict(os.environ)
        if not self._resume_session:
            return self._run_in_state(user_message, tools, env)
        os.makedirs(self.work_dir, exist_ok=True)
        # Each invocation gets its own SQLite database. Only this session's
        # exported JSON crosses workers; credentials remain worker-local.
        with tempfile.TemporaryDirectory(prefix="chia-opencode-") as directory:
            original_data = env.get("XDG_DATA_HOME") or os.path.expanduser("~/.local/share")
            env["XDG_DATA_HOME"] = os.path.join(directory, "data")
            env["XDG_STATE_HOME"] = os.path.join(directory, "state")
            auth = os.path.join(original_data, "opencode", "auth.json")
            if os.path.isfile(auth):
                auth_dir = os.path.join(env["XDG_DATA_HOME"], "opencode")
                os.makedirs(auth_dir, mode=0o700)
                target = os.path.join(auth_dir, "auth.json")
                shutil.copyfile(auth, target)
                os.chmod(target, 0o600)
            if self._session_export:
                path = os.path.join(directory, "session.json")
                with open(path, "w") as handle:
                    json.dump(self._session_export, handle)
                imported = self._capture([self.opencode_bin, "import", path], env)
                if imported.returncode != 0:
                    raise RuntimeError("OpenCode session import failed")
            return self._run_in_state(user_message, tools, env)

    def _run_in_state(self, user_message, tools, env):
        tools = tools or []
        cfg = self._build_config(tools)

        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", prefix="opencode_cfg_", delete=False
        )
        json.dump(cfg, tmp)
        tmp.close()
        cfg_path = tmp.name

        # opencode picks up our config via OPENCODE_CONFIG; disable project
        # config so a stray opencode.json in cwd can't shadow it. Stored
        # credentials / provider env vars in the inherited environment provide
        # auth (we don't touch them).
        env["OPENCODE_CONFIG"] = cfg_path
        env["OPENCODE_DISABLE_PROJECT_CONFIG"] = "1"

        run_cmd = self._build_run_cmd(user_message)
        if self._resume_session and self._session_export:
            run_cmd[-1:-1] = ["--session", self._session_export["info"]["id"]]
        self.logger.info("Running: %s ...", " ".join(run_cmd[:6]))

        try:
            # Capture via a file, not a pipe: a large run stream would otherwise
            # be truncated at 64 KiB (see _capture / module docstring), which can
            # drop the trailing error events parse_run_error looks for.
            run = self._capture(run_cmd, env)
        finally:
            try:
                os.unlink(cfg_path)
            except OSError:
                pass

        session_id = parse_session_id(run.stdout)
        # Errors that happen before an assistant message exists (e.g. unknown
        # model) only appear as `type:"error"` events in the run stream, never
        # in the export — capture them here so they can be classified too.
        run_error = parse_run_error(run.stdout)

        # A failed run (non-zero, or no session created) → return so the caller
        # classifies. Surface any run-stream error so it isn't lost. Don't
        # attempt an export without a session id.
        if run.returncode != 0 or session_id is None:
            if run.returncode != 0:
                self.logger.warning(
                    "opencode run exited %d: %s", run.returncode, run.stderr[:500]
                )
            self._last_export_error = run_error
            return OpenCodeQueryResult(
                result="",
                returncode=run.returncode if run.returncode != 0 else -1,
                stderr=run.stderr or "no session id in opencode output",
                stream_result=run.stdout,
            )

        export = self._run_export(session_id, env)
        call_export = export
        if self._resume_session:
            if export.get("info", {}).get("id") != session_id:
                raise ValueError("OpenCode session export has a mismatched session ID")
            if self._session_export and session_id != self._session_export["info"]["id"]:
                raise ValueError("OpenCode resumed a different session")
            call_export = self._current_call_export(self._call_previous_export, export)
            # A failed prior attempt must not make all later attempts fail.
            attempt_export = self._current_call_export(self._session_export, export)
            final_text, _, _, export_error = self._extract_from_export(attempt_export)
            self._session_export = export
            _, meta, stream, _ = self._extract_from_export(call_export)
        else:
            final_text, meta, stream, export_error = self._extract_from_export(export)
        self._last_metadata = meta
        # Prefer the export's error (richer — full responseHeaders); fall back to
        # the run-stream error for pre-request failures the export never records.
        self._last_export_error = export_error or run_error

        if self._log_prefix is not None:
            # Best-effort only: prompt() may run on a remote worker whose
            # filesystem lacks the (driver-side) log_dir — a logging hiccup must
            # never fail an otherwise-successful call. The transcript is always
            # returned in stream_result regardless.
            try:
                os.makedirs(os.path.dirname(self._log_prefix), exist_ok=True)
                truncated = user_message[:500] + ("..." if len(user_message) > 500 else "")
                with open(f"{self._log_prefix}.log", "a") as f:
                    f.write("=" * 80 + "\n")
                    f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] session {session_id}\n")
                    f.write("=" * 80 + "\n\n")
                    f.write(f"[User Message]\n{truncated}\n\n")
                    f.write(stream)
                    f.write("-" * 80 + "\n\n")
            except OSError as exc:
                self.logger.warning(
                    "Could not write opencode log %s.log: %s", self._log_prefix, exc
                )

        return OpenCodeQueryResult(
            result=final_text,
            returncode=0,
            stderr=run.stderr,
            stream_result=stream,
            # A copy: prompt() extends _last_metadata (model, tools) afterwards
            # and usage must stay the pure token/cost totals.
            usage=dict(meta) if meta else None,
            session_id=session_id,
            session_transcript=json.dumps(export).encode() if self._resume_session else None,
            call_export=call_export,
        )

    def _capture(self, cmd: list, env: dict) -> SimpleNamespace:
        """Run *cmd* capturing stdout to a temp FILE and return it.

        Returns a ``SimpleNamespace(returncode, stdout, stderr)`` (the same shape
        ``subprocess.run`` would, so callers read ``.stdout`` etc. unchanged).

        Why a file instead of ``capture_output=True``: opencode truncates its
        stdout at the OS pipe buffer (64 KiB on Linux) and still exits 0 when
        stdout is a pipe, so large ``run`` streams / ``export`` payloads come back
        cut mid-JSON and unparseable. A regular file has no such limit. stderr is
        small, so it stays on a pipe. ``stdin=DEVNULL`` because ``run`` blocks on
        an open stdin pipe. ``subprocess.TimeoutExpired`` propagates to the caller.
        """
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".out", prefix="opencode_out_", delete=False
        )
        out_path = tmp.name
        tmp.close()
        try:
            with open(out_path, "w") as out_fh:
                proc = subprocess.run(
                    cmd,
                    stdin=subprocess.DEVNULL,
                    stdout=out_fh,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=self.timeout_seconds,
                    env=env,
                    cwd=self.work_dir,
                )
            with open(out_path, "r") as in_fh:
                stdout = in_fh.read()
            return SimpleNamespace(
                returncode=proc.returncode, stdout=stdout, stderr=proc.stderr or ""
            )
        finally:
            try:
                os.unlink(out_path)
            except OSError:
                pass

    def _run_export(self, session_id: str, env: dict) -> dict:
        """``opencode export <id>`` → parsed session JSON (``{}`` on failure)."""
        cmd = [self.opencode_bin, "export", session_id]
        try:
            proc = self._capture(cmd, env)
        except subprocess.TimeoutExpired:
            self.logger.warning("opencode export timed out for %s", session_id)
            return {}
        if proc.returncode != 0:
            self.logger.warning(
                "opencode export exited %d: %s", proc.returncode, proc.stderr[:300]
            )
            return {}
        try:
            return json.loads(proc.stdout)
        except json.JSONDecodeError:
            self.logger.warning("opencode export returned non-JSON for %s", session_id)
            return {}

    def _extract_from_export(self, export: dict):
        """Pull final assistant text, usage metadata, a stream trace, and any error.

        Export shape::

            {info, messages: [{info:{role, tokens, cost, error}, parts: [...]}]}

        Parts: ``{type:"text", text}``, ``{type:"reasoning", text}``,
        ``{type:"tool", tool, state:{status, input, output}}``, plus
        ``step-start``/``step-finish`` — the latter carrying that model step's
        ``tokens``, ``cost`` and stop ``reason``, rendered as a ``[Usage]`` block
        so the transcript shows per-step metrics (same shape as the Antigravity
        backend's). The final answer is the text of the last assistant message;
        tokens/cost are summed across assistant messages and appended as a
        closing ``[Result]`` line.

        Returns:
            ``(text, metadata, stream, export_error)`` where *export_error* is the
            ``{name, data}`` dict from the first assistant message carrying an
            ``info.error`` (opencode's discriminated error object), or ``None``.
            This matters because ``opencode run`` almost always exits 0 even on
            failure (opencode bug #14551), so the exit code alone can't be trusted
            — the structured error in the export is the reliable signal.
        """
        stream_parts: list[str] = []
        meta = {"input_tokens": 0, "output_tokens": 0, "reasoning_tokens": 0,
                "cache_read": 0, "cache_write": 0, "cost_usd": 0.0, "num_turns": 0}
        last_assistant_text = ""
        export_error = None

        for msg in export.get("messages", []) or []:
            info = msg.get("info", {}) if isinstance(msg, dict) else {}
            role = info.get("role")
            parts = msg.get("parts", []) if isinstance(msg, dict) else []

            if role == "assistant":
                if export_error is None:
                    err = info.get("error")
                    if isinstance(err, dict) and "name" in err:
                        export_error = err

                meta["num_turns"] += 1
                tok = info.get("tokens") or {}
                meta["input_tokens"] += tok.get("input", 0) or 0
                meta["output_tokens"] += tok.get("output", 0) or 0
                meta["reasoning_tokens"] += tok.get("reasoning", 0) or 0
                cache = tok.get("cache") or {}
                meta["cache_read"] += cache.get("read", 0) or 0
                meta["cache_write"] += cache.get("write", 0) or 0
                meta["cost_usd"] += info.get("cost", 0) or 0

                turn_text: list[str] = []
                for p in parts:
                    if not isinstance(p, dict):
                        continue
                    ptype = p.get("type")
                    if ptype == "text":
                        txt = p.get("text", "")
                        turn_text.append(txt)
                        stream_parts.append(f"[Response]\n{txt}\n\n")
                    elif ptype == "reasoning":
                        stream_parts.append(f"[Thinking]\n{p.get('text', '')}\n\n")
                    elif ptype == "tool":
                        state = p.get("state") or {}
                        args = json.dumps(state.get("input", {}))
                        if len(args) > 2000:
                            args = args[:2000] + "\n... [truncated]"
                        stream_parts.append(
                            f"[Tool Call: {p.get('tool', 'unknown')}]\nArgs: {args}\n\n"
                        )
                        out = state.get("output", "")
                        if not isinstance(out, str):
                            out = json.dumps(out)
                        if len(out) > 2000:
                            out = out[:2000] + "\n... [truncated]"
                        if out:
                            stream_parts.append(f"[Tool Result]\n{out}\n\n")
                    elif ptype == "step-finish":
                        step = dict(p.get("tokens") or {})
                        if p.get("cost") is not None:
                            step["cost_usd"] = p["cost"]
                        if p.get("reason"):
                            step["reason"] = p["reason"]
                        if step:
                            stream_parts.append(f"[Usage]\n{json.dumps(step)}\n\n")
                if turn_text:
                    last_assistant_text = "".join(turn_text)

        meta = {k: v for k, v in meta.items() if v}
        if meta:
            stream_parts.append(f"[Result]\n{json.dumps(meta)}\n\n")
        return last_assistant_text, meta, "".join(stream_parts), export_error
