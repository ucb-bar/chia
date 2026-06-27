"""Google Vertex AI LLM backend built on the google-genai SDK.

:class:`VertexGeminiLLM` runs Google's **Gemini** models on Vertex AI and drives the
agentic tool loop client-side — executing each ChiaTool's MCP server over HTTP,
exactly like the Bedrock and Claude API backends.

Vertex has **no single unified API** across model families —
Gemini goes through google-genai, Claude-on-Vertex through ``AnthropicVertex``,
and Llama/Mistral through OpenAI-compatible MaaS endpoints. Two separate
classes are provided here, one for Gemini and one for MaaS.

WARNING: experimental. Only exercised by the tests in
chia/models/tests/test_vertex.py (mocked unit tests, plus opt-in live tests).
Not validated in production.

Auth/config: Vertex needs a GCP project + location and Application Default
Credentials (``gcloud auth application-default login``, a service-account key
via ``GOOGLE_APPLICATION_CREDENTIALS``, or a workload identity). Pass
``project``/``location`` or rely on ``GOOGLE_CLOUD_PROJECT`` /
``GOOGLE_CLOUD_LOCATION``. ``google-genai`` is imported lazily.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, List, Optional

import ray

from chia.base.ChiaFunction import ChiaFunction
from chia.base.llm_call import QueryResult, LLMCallBase
from chia.models.openai_compat import OpenAICompatLLM

if TYPE_CHECKING:
    from chia.base.tools.ChiaTool import ChiaTool


# ---------------------------------------------------------------------------
# Exceptions
#
# A parallel taxonomy to claude.py / bedrock.py. Kept separate so this module
# stands alone; each carries ``__reduce__`` for Ray transport.
# ---------------------------------------------------------------------------


class VertexError(Exception):
    """Base for all Vertex backend errors."""

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


class RateLimitError(VertexError):
    """Quota / rate exhaustion (HTTP 429, ``RESOURCE_EXHAUSTED``)."""

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


class AuthenticationError(VertexError):
    """Invalid / missing credentials or permission (HTTP 401 / 403)."""

    def __init__(self, node_id: str, status_code: Optional[int] = None, raw_message: str = ""):
        super().__init__(node_id, "authentication_failed", status_code, raw_message)

    def __reduce__(self):
        return (self.__class__, (self.node_id, self.status_code, self.raw_message))


class InvalidRequestError(VertexError):
    """Malformed request or unknown model (HTTP 400 / 404)."""

    def __init__(self, node_id: str, status_code: Optional[int] = None, raw_message: str = ""):
        super().__init__(node_id, "invalid_request", status_code, raw_message)

    def __reduce__(self):
        return (self.__class__, (self.node_id, self.status_code, self.raw_message))


class ServerError(VertexError):
    """Transient service-side failure (HTTP 5xx)."""

    def __init__(self, node_id: str, status_code: Optional[int] = None, raw_message: str = ""):
        super().__init__(node_id, "server_error", status_code, raw_message)

    def __reduce__(self):
        return (self.__class__, (self.node_id, self.status_code, self.raw_message))


class MaxOutputTokensError(VertexError):
    """The response was truncated at ``max_output_tokens``."""

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


class UnknownVertexError(VertexError):
    """Unclassified Vertex error."""

    def __init__(self, node_id: str, status_code: Optional[int] = None, raw_message: str = ""):
        super().__init__(node_id, "unknown", status_code, raw_message)

    def __reduce__(self):
        return (self.__class__, (self.node_id, self.status_code, self.raw_message))


class ContentBlockedError(VertexError):
    """The model returned no usable content because the prompt or the response
    was blocked (safety, recitation, blocklist, ...).

    This is NOT an HTTP/API error — Gemini reports it as a 200-OK response whose
    candidate carries a blocking ``finish_reason`` (or whose ``prompt_feedback``
    carries a ``block_reason`` with no candidates). Surfacing it as a typed error
    keeps a block from masquerading as a successful empty answer. Never automatically 
    retried: re-sending the same prompt will be blocked again.
    """

    def __init__(self, node_id: str, block_reason: str = "", raw_message: str = ""):
        self.block_reason = block_reason
        super().__init__(node_id, "content_blocked", None, raw_message)

    def __reduce__(self):
        return (self.__class__, (self.node_id, self.block_reason, self.raw_message))


# Gemini ``finish_reason`` values that mean the candidate was blocked / unusable
# rather than a normal STOP or a MAX_TOKENS truncation (which is handled
# separately as MaxOutputTokensError).
_BLOCK_FINISH_REASONS = frozenset({
    "SAFETY", "RECITATION", "BLOCKLIST", "PROHIBITED_CONTENT", "SPII",
    "IMAGE_SAFETY",
})


def _unwrap_exception_group(exc):
    """Drill through (possibly nested) ExceptionGroups to a representative leaf.

    MCP's anyio task group re-wraps any error that escapes the tool-connected
    ``AsyncExitStack`` in an ``ExceptionGroup`` (the ``exceptiongroup`` backport
    on Python < 3.11), which would otherwise hide a perfectly classifiable
    google-genai error. Prefer an already-typed :class:`VertexError`, then a
    google-genai ``APIError``, else the first leaf. Non-group exceptions are
    returned unchanged.
    """
    subs = getattr(exc, "exceptions", None)
    if not subs or not isinstance(subs, (list, tuple)):
        return exc
    leaves = [_unwrap_exception_group(s) for s in subs]
    for leaf in leaves:
        if isinstance(leaf, VertexError):
            return leaf
    try:
        from google.genai import errors as genai_errors
        for leaf in leaves:
            if isinstance(leaf, genai_errors.APIError):
                return leaf
    except Exception:
        pass
    return leaves[0]


class VertexGeminiLLM(LLMCallBase):
    """Gemini-on-Vertex LLM backend with client-side MCP tool execution.

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
        logging_name: str = "vertex_gemini",
        logging_level: int = logging.DEBUG,
        log_dir: Optional[str] = None,
        project: Optional[str] = None,
        location: Optional[str] = None,
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
        self.project = project or os.environ.get("GOOGLE_CLOUD_PROJECT")
        self.location = (
            location
            or os.environ.get("GOOGLE_CLOUD_LOCATION")
            or "us-central1"
        )
        self.max_tokens = max_tokens
        self.max_tool_iterations = max_tool_iterations
        self.client_kwargs = client_kwargs or {}
        self.logger = logging.getLogger(logging_name)
        self._last_metadata: dict = {}

        self.logger.warning(
            "VertexGeminiLLM is experimental: only exercised by unit tests so far, "
            "not validated in production."
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

    @ChiaFunction(resources={"vertex_creds": 0.01})
    def prompt(
        self,
        user_message: str,
        tools: Optional[List[ChiaTool]] = [],
    ) -> QueryResult:
        """Send *user_message* via Gemini on Vertex and return the response,
        retrying transient failures with the same policy the other backends use.
        """
        import time as _time

        from chia.trace.profiler import get_profiler

        profiler = get_profiler()

        for attempt in range(self.retries):
            try:
                self._last_metadata = {}
                cli = self._run_generate(user_message, tools)
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
            except (RateLimitError, AuthenticationError, InvalidRequestError,
                    ContentBlockedError):
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
            except UnknownVertexError as exc:
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
    # generate_content implementation
    # ------------------------------------------------------------------

    def _run_generate(
        self,
        user_message: str,
        tools: Optional[List[ChiaTool]] = None,
    ) -> QueryResult:
        """Synchronous entry point; drives the async agent loop.

        A second translation guard catches the case where a typed error (or a
        raw google-genai error) escapes the tool-connected ``AsyncExitStack``
        wrapped in an ``ExceptionGroup`` by MCP's anyio task group — without
        this, the wrapped error never matches ``prompt()``'s ``except`` clauses
        and every API error in the tools path is misfiled as "unexpected".
        """
        try:
            return self._run_coroutine(self._run_generate_async(user_message, tools or []))
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

    async def _run_generate_async(
        self,
        user_message: str,
        tools: Optional[List[ChiaTool]] = None,
    ) -> QueryResult:
        """Connect to each ChiaTool's MCP server, then run the Gemini loop.

        Gemini returns one model turn at a time; when that turn contains
        ``function_call`` parts we execute them against their MCP servers and
        feed ``function_response`` parts back, looping until the model returns
        no more calls (or :attr:`max_tool_iterations` is hit).
        """
        import asyncio
        from contextlib import AsyncExitStack

        from google import genai
        from google.genai import types
        from mcp import ClientSession
        from mcp.client.streamable_http import streamable_http_client

        client = genai.Client(
            vertexai=True,
            project=self.project,
            location=self.location,
            **self.client_kwargs,
        )

        stream_parts: list[str] = []
        stream_parts.append("=" * 80 + "\n")
        stream_parts.append(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] generate_content ({self.model})\n"
        )
        stream_parts.append("=" * 80 + "\n\n")
        truncated = user_message[:500] + ("..." if len(user_message) > 500 else "")
        stream_parts.append(f"[User Message]\n{truncated}\n\n")

        async with AsyncExitStack() as stack:
            # --- Connect to every MCP server and gather function declarations ---
            function_decls = []
            dispatch: dict = {}  # gemini function name -> (session, mcp tool name)
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
                    function_decls.append(types.FunctionDeclaration(
                        name=api_name,
                        description=fn.description or " ",
                        parameters=self._sanitize_schema(
                            fn.inputSchema or {"type": "object", "properties": {}}
                        ),
                    ))
                    dispatch[api_name] = (session, fn.name)

            config_kwargs: dict = {
                "max_output_tokens": self.max_tokens,
                # We run the loop ourselves; don't let the SDK auto-call.
                "automatic_function_calling": types.AutomaticFunctionCallingConfig(
                    disable=True
                ),
            }
            if self.system_message:
                config_kwargs["system_instruction"] = self.system_message
            if function_decls:
                config_kwargs["tools"] = [types.Tool(function_declarations=function_decls)]
            config = types.GenerateContentConfig(**config_kwargs)

            contents = [types.Content(
                role="user", parts=[types.Part.from_text(text=user_message)]
            )]
            meta = {"input_tokens": 0, "output_tokens": 0, "num_turns": 0}
            final_text = ""

            for _ in range(self.max_tool_iterations):
                try:
                    resp = await asyncio.to_thread(
                        client.models.generate_content,
                        model=self.model,
                        contents=contents,
                        config=config,
                    )
                except Exception as exc:
                    translated = self._translate_error(exc)
                    if translated is not None:
                        raise translated from exc
                    raise

                meta["num_turns"] += 1
                usage = getattr(resp, "usage_metadata", None)
                if usage is not None:
                    meta["input_tokens"] += getattr(usage, "prompt_token_count", 0) or 0
                    meta["output_tokens"] += getattr(usage, "candidates_token_count", 0) or 0

                candidate = (resp.candidates or [None])[0]
                if candidate is None or candidate.content is None:
                    # No candidate at all -> the prompt itself may have been
                    # blocked (safety etc.); surface that rather than returning
                    # a silent empty success.
                    block = self._prompt_block_reason(resp)
                    if block:
                        raise ContentBlockedError(
                            node_id=self._get_node_id(), block_reason=block,
                            raw_message=f"prompt blocked: {block}",
                        )
                    break

                finish = getattr(candidate, "finish_reason", None)
                finish_name = getattr(finish, "name", str(finish) if finish else "")

                # --- Log this turn's parts; collect function calls ---
                turn_text_parts: list[str] = []
                function_calls = []
                for part in candidate.content.parts or []:
                    fc = getattr(part, "function_call", None)
                    if fc is not None:
                        function_calls.append(fc)
                        args_str = json.dumps(dict(fc.args or {}))
                        if len(args_str) > 2000:
                            args_str = args_str[:2000] + "\n... [truncated]"
                        stream_parts.append(
                            f"[Tool Call: {fc.name}]\nArgs: {args_str}\n\n"
                        )
                    elif getattr(part, "thought", False) and part.text:
                        stream_parts.append(f"[Thinking]\n{part.text}\n\n")
                    elif getattr(part, "text", None):
                        turn_text_parts.append(part.text)
                        stream_parts.append(f"[Response]\n{part.text}\n\n")
                if turn_text_parts:
                    final_text = "".join(turn_text_parts)

                # Echo the model turn back verbatim (function_call parts must
                # round-trip for the follow-up request).
                contents.append(candidate.content)

                if finish_name == "MAX_TOKENS":
                    raise MaxOutputTokensError(
                        node_id=self._get_node_id(),
                        raw_message="response truncated at max_output_tokens",
                        partial_text=final_text,
                    )

                # The response was content-blocked (safety/recitation/...) -> the
                # turn has no trustworthy output; surface it instead of breaking
                # into a silent empty success.
                if finish_name in _BLOCK_FINISH_REASONS and not function_calls:
                    raise ContentBlockedError(
                        node_id=self._get_node_id(), block_reason=finish_name,
                        raw_message=f"response blocked: {finish_name}",
                    )

                if not function_calls:
                    break

                # --- Execute each requested tool over its MCP server ---
                response_parts = []
                for fc in function_calls:
                    session, fn_name = dispatch.get(fc.name, (None, None))
                    if session is None:
                        result_text = f"Unknown tool: {fc.name}"
                        is_error = True
                    else:
                        try:
                            mcp_result = await session.call_tool(
                                fn_name, dict(fc.args or {})
                            )
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
                    response_parts.append(types.Part.from_function_response(
                        name=fc.name,
                        response={"error": result_text} if is_error
                        else {"result": result_text},
                    ))

                contents.append(types.Content(role="user", parts=response_parts))
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
    def _sanitize_schema(schema):
        """Drop JSON-schema keys Gemini's function-declaration schema rejects.

        Gemini accepts only a subset of OpenAPI schema; keys like
        ``$schema``/``additionalProperties``/``title`` cause 400s, so strip
        them recursively. MCP servers often emit them.
        """
        drop = {"$schema", "$id", "$defs", "definitions", "additionalProperties", "title"}
        if isinstance(schema, dict):
            return {
                k: VertexGeminiLLM._sanitize_schema(v)
                for k, v in schema.items()
                if k not in drop
            }
        if isinstance(schema, list):
            return [VertexGeminiLLM._sanitize_schema(v) for v in schema]
        return schema

    @staticmethod
    def _prompt_block_reason(resp) -> str:
        """The ``prompt_feedback.block_reason`` name when the prompt was blocked
        (no candidates produced), else ``""``."""
        feedback = getattr(resp, "prompt_feedback", None)
        if feedback is None:
            return ""
        reason = getattr(feedback, "block_reason", None)
        if reason is None:
            return ""
        return getattr(reason, "name", str(reason))

    @staticmethod
    def _mcp_result_to_text(result) -> str:
        """Flatten an MCP ``CallToolResult`` into plain text."""
        parts = []
        for item in getattr(result, "content", None) or []:
            text = getattr(item, "text", None)
            parts.append(text if text is not None else str(item))
        return "\n".join(parts)

    def _translate_error(self, exc) -> Optional[Exception]:
        """Map a google-genai error to a local typed error.

        Returns the local exception to raise, or ``None`` when *exc* is not a
        recognised google-genai API error (so the caller re-raises it).
        """
        # MCP's anyio task group wraps errors that escape the tool-connected
        # AsyncExitStack in (possibly nested) ExceptionGroups — drill down to
        # the real error first, otherwise nothing classifies in the tools path.
        exc = _unwrap_exception_group(exc)

        # Already one of ours (e.g. translated inside the loop, then re-wrapped
        # by the task group) -> pass straight through.
        if isinstance(exc, VertexError):
            return exc

        try:
            from google.genai import errors as genai_errors
        except Exception:
            return None

        if not isinstance(exc, genai_errors.APIError):
            return None

        node_id = self._get_node_id()
        code = getattr(exc, "code", None)
        msg = (getattr(exc, "message", None) or str(exc))[:300]

        if code == 429:
            reset_time = datetime.now(timezone.utc) + timedelta(seconds=60)
            return RateLimitError(
                node_id=node_id, reset_time=reset_time,
                raw_message=msg, status_code=code,
            )
        if code in (401, 403):
            return AuthenticationError(node_id, status_code=code, raw_message=msg)
        if code in (400, 404):
            return InvalidRequestError(node_id, status_code=code, raw_message=msg)
        if isinstance(code, int) and code >= 500:
            return ServerError(node_id, status_code=code, raw_message=msg)
        return UnknownVertexError(node_id, status_code=code, raw_message=msg)


# ---------------------------------------------------------------------------
# Non-Gemini Vertex models via the OpenAI-compatible MaaS endpoint
# ---------------------------------------------------------------------------


def _vertex_adc_token_provider() -> str:
    """Mint a fresh GCP access token from Application Default Credentials.

    Used as :class:`OpenAICompatLLM`'s ``token_provider`` for Vertex MaaS,
    where auth is a short-lived GCP bearer token rather than a static key. It's
    invoked each time the OpenAI client is built, so the token is always
    current. Runs wherever the client is constructed — ADC must be available
    there (``gcloud auth application-default login`` / service account / WI).
    """
    import google.auth
    import google.auth.transport.requests

    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    creds.refresh(google.auth.transport.requests.Request())
    return creds.token


class VertexGenericLLM(OpenAICompatLLM):
    """Non-Gemini Vertex models (Llama, Mistral, ...) via the Vertex Model-as-a-
    Service **OpenAI-compatible** endpoint.

    Vertex has no single unified API, so Gemini uses google-genai
    (:class:`VertexGeminiLLM`) while the open/partner families are reached
    through the OpenAI-compatible MaaS endpoint — which is exactly what
    :class:`OpenAICompatLLM` already speaks. The only Vertex-specifics are the
    endpoint URL (built from project/location) and auth: a GCP ADC bearer token
    that rotates, supplied via ``token_provider``. Everything else — the agent
    loop, tool handling, error translation — is inherited unchanged.

    ``model`` is the MaaS model id, e.g. ``meta/llama-3.1-8b-instruct-maas``.
    """

    def __init__(
        self,
        model: str,
        project: Optional[str] = None,
        location: Optional[str] = None,
        **kwargs,
    ):
        project = project or os.environ.get("GOOGLE_CLOUD_PROJECT")
        location = (
            location
            or os.environ.get("GOOGLE_CLOUD_LOCATION")
            or "us-central1"
        )
        kwargs.setdefault(
            "base_url",
            f"https://{location}-aiplatform.googleapis.com/v1beta1/"
            f"projects/{project}/locations/{location}/endpoints/openapi",
        )
        kwargs.setdefault("logging_name", "vertex_maas")
        kwargs.setdefault("token_provider", _vertex_adc_token_provider)

        # User ADC requires a billing/quota project. google-auth normally sends
        # it via the ``x-goog-user-project`` header from the ADC quota project,
        # but we hand the OpenAI client only a bearer token (the token_provider),
        # so that header would be lost and Vertex 403s with "requires a quota
        # project". Set it explicitly when we know the project. Harmless for
        # service-account creds (they carry their own project).
        if project:
            client_kwargs = dict(kwargs.get("client_kwargs") or {})
            headers = dict(client_kwargs.get("default_headers") or {})
            headers.setdefault("x-goog-user-project", project)
            client_kwargs["default_headers"] = headers
            kwargs["client_kwargs"] = client_kwargs

        super().__init__(model=model, **kwargs)

    @ChiaFunction(resources={"vertex_creds": 0.01})
    def prompt(
        self,
        user_message: str,
        tools: Optional[List[ChiaTool]] = [],
    ) -> QueryResult:
        return OpenAICompatLLM.prompt(self, user_message, tools)
