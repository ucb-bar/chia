"""OpenAI-compatible LLM backend built on the openai SDK Chat Completions API.

:class:`OpenAICompatLLM` talks to **any** provider that implements the OpenAI
Chat Completions wire format and drives the agentic tool loop client-side —
executing each ChiaTool's MCP server over HTTP, like the other backends.

It is deliberately *not* provider-specific. The OpenAI Chat Completions format
is a de-facto multi-vendor standard, so the only provider-specific inputs are:

* ``base_url`` — selects the provider's endpoint (default: OpenAI itself).
* auth — the credential for that endpoint.

The same loop/tool/parse/error code therefore covers OpenAI, Fireworks, Groq,
OpenRouter, self-hosted vLLM/TGI, Vertex MaaS, etc. Each provider is
configuration, not a new module.

Auth, by default, lives entirely in the environment (``OPENAI_API_KEY`` /
``OPENAI_BASE_URL``) — construct with no key and the SDK reads them, matching
the other backends. For providers whose credential is a **rotating token**
(Vertex MaaS GCP token, Azure AD), pass ``token_provider`` — a zero-arg
callable that returns a fresh token; it's invoked when the client is built.

WARNING: experimental. Only exercised by the tests in
chia/models/tests/test_openai_compat.py. Not validated in production.
``openai`` is imported lazily, so importing this module does not require it.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Callable, List, Optional

import ray

from chia.base.ChiaFunction import ChiaFunction
from chia.base.llm_call import QueryResult, LLMCallBase

if TYPE_CHECKING:
    from chia.base.tools.ChiaTool import ChiaTool


# ---------------------------------------------------------------------------
# Exceptions
#
# A parallel taxonomy to the other backends. Kept separate so this module
# stands alone; each carries ``__reduce__`` for Ray transport.
# ---------------------------------------------------------------------------


class OpenAICompatError(Exception):
    """Base for all OpenAI-compatible backend errors."""

    def __init__(
        self,
        node_id: str,
        error_type: str,
        status_code: Optional[int] = None,
        raw_message: str = "",
    ):
        self.node_id = node_id
        self.error_type = error_type
        self.status_code = status_code
        self.raw_message = raw_message
        super().__init__(f"{error_type} on {node_id}: {raw_message[:200]}")

    def __reduce__(self):
        return (
            self.__class__,
            (self.node_id, self.error_type, self.status_code, self.raw_message),
        )


class RateLimitError(OpenAICompatError):
    """HTTP 429."""

    def __init__(
        self,
        node_id: str,
        reset_time: datetime,
        raw_message: str = "",
        status_code: Optional[int] = None,
    ):
        self.reset_time = reset_time
        super().__init__(node_id, "rate_limit", status_code, raw_message)

    def __reduce__(self):
        return (
            self.__class__,
            (self.node_id, self.reset_time, self.raw_message, self.status_code),
        )


class AuthenticationError(OpenAICompatError):
    """HTTP 401 / 403."""

    def __init__(self, node_id: str, status_code: Optional[int] = None, raw_message: str = ""):
        super().__init__(node_id, "authentication_failed", status_code, raw_message)

    def __reduce__(self):
        return (self.__class__, (self.node_id, self.status_code, self.raw_message))


class InvalidRequestError(OpenAICompatError):
    """HTTP 400 / 404."""

    def __init__(self, node_id: str, status_code: Optional[int] = None, raw_message: str = ""):
        super().__init__(node_id, "invalid_request", status_code, raw_message)

    def __reduce__(self):
        return (self.__class__, (self.node_id, self.status_code, self.raw_message))


class ServerError(OpenAICompatError):
    """HTTP 5xx, connection, or timeout."""

    def __init__(self, node_id: str, status_code: Optional[int] = None, raw_message: str = ""):
        super().__init__(node_id, "server_error", status_code, raw_message)

    def __reduce__(self):
        return (self.__class__, (self.node_id, self.status_code, self.raw_message))


class MaxOutputTokensError(OpenAICompatError):
    """The response was truncated at ``max_tokens`` (finish_reason='length')."""

    def __init__(
        self,
        node_id: str,
        status_code: Optional[int] = None,
        raw_message: str = "",
        partial_text: str = "",
    ):
        self.partial_text = partial_text
        super().__init__(node_id, "max_output_tokens", status_code, raw_message)

    def __reduce__(self):
        return (
            self.__class__,
            (self.node_id, self.status_code, self.raw_message, self.partial_text),
        )


class ContextLengthExceededError(InvalidRequestError):
    """HTTP 400 (or 413) whose body shows the prompt exceeded the context window.

    Subclasses :class:`InvalidRequestError` so it inherits never-retry semantics
    (re-sending the same oversized prompt cannot help) while letting callers
    detect the context-overflow case specifically. Distinct from
    :class:`MaxOutputTokensError`, which is about *output* truncation.
    """

    def __init__(self, node_id: str, status_code: Optional[int] = None, raw_message: str = ""):
        OpenAICompatError.__init__(
            self, node_id, "context_length_exceeded", status_code, raw_message
        )

    def __reduce__(self):
        return (self.__class__, (self.node_id, self.status_code, self.raw_message))


class BillingError(OpenAICompatError):
    """Payment / quota problem (HTTP 402, or a 429 carrying ``insufficient_quota``).

    Never retried: spending more requests cannot restore quota or credit.
    """

    def __init__(self, node_id: str, status_code: Optional[int] = None, raw_message: str = ""):
        super().__init__(node_id, "billing", status_code, raw_message)

    def __reduce__(self):
        return (self.__class__, (self.node_id, self.status_code, self.raw_message))


class UnknownOpenAIError(OpenAICompatError):
    """Unclassified OpenAI-compatible error."""

    def __init__(self, node_id: str, status_code: Optional[int] = None, raw_message: str = ""):
        super().__init__(node_id, "unknown", status_code, raw_message)

    def __reduce__(self):
        return (self.__class__, (self.node_id, self.status_code, self.raw_message))


# ---------------------------------------------------------------------------
# Body-level disambiguation
#
# Some providers collapse semantically distinct failures into the same HTTP
# status (e.g. context-overflow and a bad parameter are both 400; quota
# exhaustion arrives as a 429). These patterns let ``_translate_error`` split
# them by inspecting the error code / message. Kept deliberately conservative
# to avoid false positives.
#
# - context-length: backed by a REAL OpenRouter capture (the message hits both
#   "context length" and "reduce the length"); the string codes cover
#   OpenAI/vLLM-native ("context_length_exceeded").
# - billing: follows common provider conventions (OpenAI insufficient_quota,
#   402 Payment Required). NOT yet backed by a real capture.
# ---------------------------------------------------------------------------

_CONTEXT_LENGTH_CODES = {"context_length_exceeded", "string_above_max_length"}
_CONTEXT_LENGTH_PHRASES = (
    "context length",
    "context_length_exceeded",
    "maximum context",
    "prompt is too long",
    "too many tokens",
    "reduce the length",
    "maximum prompt",
)
_BILLING_CODES = {"insufficient_quota", "billing_hard_limit_reached"}
_BILLING_PHRASES = (
    "insufficient_quota",
    "exceeded your current quota",
    "billing",
    "payment required",
    "not enough credits",
    "insufficient credits",
)


def _error_code_and_text(exc):
    """Extract a string error ``code`` (or None) and a lowercased message."""
    code = getattr(exc, "code", None)
    if not isinstance(code, str):
        code = None
    msg = getattr(exc, "message", None) or str(exc)
    return code, (msg or "").lower()


def _unwrap_exception_group(exc):
    """Drill through (possibly nested) ExceptionGroups to a representative leaf.

    MCP's anyio task group re-wraps any error that propagates out of the
    tool-connected ``AsyncExitStack`` in an ``ExceptionGroup`` (the
    ``exceptiongroup`` backport on Python < 3.11), which would otherwise hide a
    perfectly classifiable provider error. Prefer an already-typed
    :class:`OpenAICompatError`, then an ``openai`` API error, else the first
    leaf. Non-group exceptions are returned unchanged.
    """
    subs = getattr(exc, "exceptions", None)
    if not subs or not isinstance(subs, (list, tuple)):
        return exc
    leaves = [_unwrap_exception_group(s) for s in subs]
    for leaf in leaves:
        if isinstance(leaf, OpenAICompatError):
            return leaf
    try:
        import openai
        for leaf in leaves:
            if isinstance(leaf, openai.APIError):
                return leaf
    except Exception:
        pass
    return leaves[0]


class OpenAICompatLLM(LLMCallBase):
    """OpenAI-compatible Chat Completions backend with client-side MCP tools.

    Returns the same :class:`QueryResult` shape as the other backends so callers
    are interchangeable; ``returncode`` is synthesised (0 on success, -1 when
    every retry fails) and ``stderr`` is unused.
    """

    def __init__(
        self,
        model: str,
        system_message: str = "",
        timeout_seconds: int = 600,
        retries: int = 3,
        logging_name: str = "openai_compat_llm",
        logging_level: int = logging.DEBUG,
        log_dir: Optional[str] = None,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        token_provider: Optional[Callable[[], str]] = None,
        max_tokens: int = 16000,
        max_tool_iterations: int = 100,
        client_kwargs: Optional[dict] = None,
    ):
        super().__init__(system_message=system_message)
        self.logging_level = logging_level
        self.logging_name = logging_name
        self.retries = retries
        self.timeout_seconds = timeout_seconds
        self.model = model
        self.base_url = base_url  # None -> OPENAI_BASE_URL env / OpenAI default
        self.api_key = api_key    # None -> OPENAI_API_KEY env
        self.token_provider = token_provider
        self.max_tokens = max_tokens
        self.max_tool_iterations = max_tool_iterations
        self.client_kwargs = client_kwargs or {}
        self.logger = logging.getLogger(logging_name)
        self._last_metadata: dict = {}

        self.logger.warning(
            "OpenAICompatLLM is experimental: only exercised by unit tests so "
            "far, not validated in production."
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

    @ChiaFunction(resources={"openai_creds": 0.01})
    def prompt(
        self,
        user_message: str,
        tools: Optional[List[ChiaTool]] = [],
    ) -> QueryResult:
        """Send *user_message* via Chat Completions and return the response,
        retrying transient failures with the same policy the other backends use.
        """
        import time as _time

        from chia.trace.profiler import get_profiler

        profiler = get_profiler()

        for attempt in range(self.retries):
            try:
                self._last_metadata = {}
                cli = self._run_openai(user_message, tools)
                self._last_metadata["model"] = self.model
                self._last_metadata["tools"] = [
                    {"name": t.name, "hostname": getattr(t, "hostname", None),
                     "port": getattr(t, "port", None),
                     "node_id": getattr(t, "node_id", None)}
                    for t in tools
                ]
                if profiler.enabled and self._last_metadata:
                    profiler.add_info(self._last_metadata)

                cli.success = True
                return cli

            # -- Never retry: propagate immediately --
            # (ContextLengthExceededError is caught here as an InvalidRequestError
            # subclass; BillingError is listed explicitly.)
            except (RateLimitError, AuthenticationError, InvalidRequestError, BillingError):
                raise

            # -- Retry once: a shorter generation may fit --
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
                _time.sleep(backoff)

            # -- Standard retry for unknown errors --
            except UnknownOpenAIError as exc:
                self.logger.warning(
                    "Unknown error on attempt %d/%d: %s",
                    attempt + 1, self.retries, exc,
                )

            except Exception as exc:
                self.logger.warning(
                    "Unexpected error on attempt %d/%d: %s",
                    attempt + 1, self.retries, exc,
                )
        return QueryResult(result="", returncode=-1, stderr="", stream_result="", success=False)

    def _get_node_id(self) -> str:
        try:
            return ray.get_runtime_context().get_node_id()
        except Exception:
            return "unknown"

    # ------------------------------------------------------------------
    # Chat Completions implementation
    # ------------------------------------------------------------------

    def _run_openai(
        self,
        user_message: str,
        tools: Optional[List[ChiaTool]] = None,
    ) -> QueryResult:
        """Synchronous entry point; drives the async agent loop.

        A second translation guard catches the case where a typed error (or a
        raw openai error) escapes the tool-connected ``AsyncExitStack`` wrapped
        in an ``ExceptionGroup`` by MCP's anyio task group — without this, the
        wrapped error never matches ``prompt()``'s ``except`` clauses and every
        provider error in the tools path is misfiled as "unexpected".
        """
        try:
            return self._run_coroutine(self._run_openai_async(user_message, tools or []))
        except Exception as exc:
            translated = self._translate_error(exc)
            if translated is not None:
                raise translated from exc
            raise

    def _run_coroutine(self, coro):
        """Run *coro* whether or not an event loop is already running."""
        import asyncio

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)

        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, coro).result()

    def _make_client(self):
        """Build the AsyncOpenAI client.

        With no ``base_url``/``api_key``/``token_provider`` the SDK reads
        ``OPENAI_API_KEY`` / ``OPENAI_BASE_URL`` from the environment — the
        same env-driven auth model as the other backends.
        """
        from openai import AsyncOpenAI

        kwargs = dict(self.client_kwargs)
        if self.base_url is not None:
            kwargs["base_url"] = self.base_url
        if self.token_provider is not None:
            kwargs["api_key"] = self.token_provider()
        elif self.api_key is not None:
            kwargs["api_key"] = self.api_key
        return AsyncOpenAI(**kwargs)

    async def _run_openai_async(
        self,
        user_message: str,
        tools: Optional[List[ChiaTool]] = None,
    ) -> QueryResult:
        """Connect to each ChiaTool's MCP server, then run the Chat Completions
        loop: on ``tool_calls`` execute them against the MCP servers and feed
        ``role="tool"`` messages back, until the model stops requesting tools
        (or :attr:`max_tool_iterations` is hit).
        """
        from contextlib import AsyncExitStack

        from mcp import ClientSession
        from mcp.client.streamable_http import streamable_http_client

        client = self._make_client()

        stream_parts: list[str] = []
        stream_parts.append("=" * 80 + "\n")
        stream_parts.append(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] chat.completions ({self.model})\n"
        )
        stream_parts.append("=" * 80 + "\n\n")
        truncated = user_message[:500] + ("..." if len(user_message) > 500 else "")
        stream_parts.append(f"[User Message]\n{truncated}\n\n")

        async with AsyncExitStack() as stack:
            # --- Connect to every MCP server and gather tool schemas ---
            tool_schemas: list[dict] = []
            dispatch: dict = {}  # openai tool name -> (session, mcp tool name)
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
                    tool_schemas.append({
                        "type": "function",
                        "function": {
                            "name": api_name,
                            "description": fn.description or "",
                            "parameters": fn.inputSchema
                            or {"type": "object", "properties": {}},
                        },
                    })
                    dispatch[api_name] = (session, fn.name)

            messages: list[dict] = []
            if self.system_message:
                messages.append({"role": "system", "content": self.system_message})
            messages.append({"role": "user", "content": user_message})

            meta = {"input_tokens": 0, "output_tokens": 0, "num_turns": 0}
            final_text = ""

            for _ in range(self.max_tool_iterations):
                kwargs: dict = {
                    "model": self.model,
                    "messages": messages,
                    "max_tokens": self.max_tokens,
                }
                if tool_schemas:
                    kwargs["tools"] = tool_schemas
                    kwargs["tool_choice"] = "auto"

                try:
                    resp = await client.chat.completions.create(**kwargs)
                except Exception as exc:
                    translated = self._translate_error(exc)
                    if translated is not None:
                        raise translated from exc
                    raise

                meta["num_turns"] += 1
                usage = getattr(resp, "usage", None)
                if usage is not None:
                    meta["input_tokens"] += getattr(usage, "prompt_tokens", 0) or 0
                    meta["output_tokens"] += getattr(usage, "completion_tokens", 0) or 0

                choice = resp.choices[0]
                msg = choice.message
                finish_reason = choice.finish_reason
                tool_calls = getattr(msg, "tool_calls", None) or []

                if msg.content:
                    final_text = msg.content
                    stream_parts.append(f"[Response]\n{msg.content}\n\n")

                # Echo the assistant turn back (content + tool_calls must
                # round-trip for the follow-up request).
                assistant_msg: dict = {"role": "assistant", "content": msg.content}
                if tool_calls:
                    assistant_msg["tool_calls"] = [
                        {
                            "id": tc.id,
                            "type": "function",
                            "function": {
                                "name": tc.function.name,
                                "arguments": tc.function.arguments,
                            },
                        }
                        for tc in tool_calls
                    ]
                messages.append(assistant_msg)

                if finish_reason == "length":
                    raise MaxOutputTokensError(
                        node_id=self._get_node_id(),
                        raw_message="response truncated at max_tokens",
                        partial_text=final_text,
                    )

                if not tool_calls:
                    break

                # --- Execute each requested tool over its MCP server ---
                for tc in tool_calls:
                    name = tc.function.name
                    try:
                        args = json.loads(tc.function.arguments or "{}")
                    except (json.JSONDecodeError, TypeError):
                        args = None

                    if args is None:
                        result_text = f"Invalid tool arguments (not valid JSON): {tc.function.arguments!r}"
                        is_error = True
                    else:
                        session, fn_name = dispatch.get(name, (None, None))
                        if session is None:
                            result_text = f"Unknown tool: {name}"
                            is_error = True
                        else:
                            stream_parts.append(
                                f"[Tool Call: {name}]\nArgs: {json.dumps(args)[:2000]}\n\n"
                            )
                            try:
                                mcp_result = await session.call_tool(fn_name, args)
                                result_text = self._mcp_result_to_text(mcp_result)
                                is_error = bool(getattr(mcp_result, "isError", False))
                            except Exception as exc:
                                result_text = f"Tool execution error: {exc}"
                                is_error = True

                    logged = result_text
                    if len(logged) > 2000:
                        logged = logged[:2000] + "\n... [truncated]"
                    label = "Tool Result (error)" if is_error else "Tool Result"
                    stream_parts.append(f"[{label}]\n{logged}\n\n")

                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": result_text,
                    })
            else:
                stream_parts.append(
                    f"[DEBUG] Reached max_tool_iterations={self.max_tool_iterations}\n"
                )

        # --- Metadata + log file ---
        self._last_metadata = {k: v for k, v in meta.items() if v}

        stream_parts.append("-" * 80 + "\n\n")
        if self._log_prefix is not None:
            with open(f"{self._log_prefix}.log", "a") as f:
                f.write("".join(stream_parts))

        return QueryResult(
            result=final_text,
            returncode=0,
            stderr="",
            stream_result="".join(stream_parts),
        )

    @staticmethod
    def _mcp_result_to_text(result) -> str:
        """Flatten an MCP ``CallToolResult`` into plain text."""
        parts = []
        for item in getattr(result, "content", None) or []:
            text = getattr(item, "text", None)
            parts.append(text if text is not None else str(item))
        return "\n".join(parts)

    def _translate_error(self, exc) -> Optional[Exception]:
        """Map an openai SDK error to a local typed error.

        Returns the local exception to raise, or ``None`` when *exc* is not a
        recognised openai API error (so the caller re-raises it).
        """
        # MCP's anyio task group wraps errors that escape the tool-connected
        # AsyncExitStack in (possibly nested) ExceptionGroups — drill down to
        # the real error first, otherwise nothing classifies in the tools path.
        exc = _unwrap_exception_group(exc)

        # Already one of ours (e.g. translated inside the loop, then re-wrapped
        # by the task group) -> pass straight through.
        if isinstance(exc, OpenAICompatError):
            return exc

        try:
            import openai
        except Exception:
            return None

        node_id = self._get_node_id()
        msg = str(exc)[:300]
        err_code, err_text = _error_code_and_text(exc)

        def _is_billing():
            return err_code in _BILLING_CODES or any(p in err_text for p in _BILLING_PHRASES)

        def _is_context_length():
            return err_code in _CONTEXT_LENGTH_CODES or any(
                p in err_text for p in _CONTEXT_LENGTH_PHRASES
            )

        # Billing/quota first: providers smuggle it inside a 429
        # (insufficient_quota) or a 402, which would otherwise look like a rate
        # limit or a generic status error. Restrict phrase-matching to 4xx
        # (or status-less) so a 5xx message can't be misread as billing.
        if isinstance(exc, openai.APIStatusError):
            status = getattr(exc, "status_code", None)
            in_4xx = status is None or 400 <= status < 500
            if status == 402 or (in_4xx and _is_billing()):
                return BillingError(node_id, status_code=status, raw_message=msg)

        if isinstance(exc, openai.RateLimitError):
            retry_after = 60
            try:
                ra = exc.response.headers.get("retry-after")
                if ra:
                    retry_after = int(ra)
            except Exception:
                pass
            reset_time = datetime.now(timezone.utc) + timedelta(seconds=retry_after)
            return RateLimitError(
                node_id=node_id, reset_time=reset_time, raw_message=msg,
                status_code=429,
            )

        if isinstance(exc, (openai.AuthenticationError, openai.PermissionDeniedError)):
            return AuthenticationError(
                node_id, status_code=getattr(exc, "status_code", None), raw_message=msg
            )

        if isinstance(exc, (openai.BadRequestError, openai.NotFoundError)):
            status = getattr(exc, "status_code", None)
            # Context-overflow comes back as a 400 with a length message; split
            # it out so callers can detect "prompt too big" specifically.
            if _is_context_length():
                return ContextLengthExceededError(node_id, status_code=status, raw_message=msg)
            return InvalidRequestError(node_id, status_code=status, raw_message=msg)

        if isinstance(exc, (openai.APITimeoutError, openai.APIConnectionError,
                            openai.InternalServerError)):
            return ServerError(
                node_id, status_code=getattr(exc, "status_code", None), raw_message=msg
            )

        if isinstance(exc, openai.APIStatusError):
            code = getattr(exc, "status_code", None)
            if isinstance(code, int) and code >= 500:
                return ServerError(node_id, status_code=code, raw_message=msg)
            # e.g. a 413 Payload Too Large used by some providers for overflow.
            if _is_context_length():
                return ContextLengthExceededError(node_id, status_code=code, raw_message=msg)
            return UnknownOpenAIError(node_id, status_code=code, raw_message=msg)

        return None
