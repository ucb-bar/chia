"""Amazon Bedrock LLM backend built on the boto3 Converse API.

:class:`BedrockLLM` talks to **any** tool-capable chat model on Amazon
Bedrock (Claude, Amazon Nova, Llama, Mistral, Command R, ...) It uses 
the Bedrock Runtime ``converse`` API, which normalises
messages, system prompts, and tool use across model families, and runs the
agentic tool loop client-side — executing each ChiaTool's MCP server over
HTTP exactly like :class:`chia.models.claude.ClaudeCodeLLM`'s API backend.

WARNING: experimental. Only exercised by the tests in
chia/models/tests/test_bedrock.py (mocked unit tests, plus opt-in live
tests). Not validated in production.

Auth/config come from the standard AWS chain (env vars, shared profile, or
IAM role); pass ``region`` or rely on ``AWS_REGION`` / ``AWS_DEFAULT_REGION``.
``boto3`` is imported lazily, so importing this module does not require it.
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

if TYPE_CHECKING:
    from chia.base.tools.ChiaTool import ChiaTool


# ---------------------------------------------------------------------------
# Exceptions
#
# A parallel taxonomy to claude.py's. Kept separate (rather than imported)
# so this module stands alone; each carries ``__reduce__`` for Ray transport.
# ---------------------------------------------------------------------------


class BedrockError(Exception):
    """Base for all Bedrock backend errors."""

    def __init__(
        self,
        node_id: str,
        error_type: str,
        status_code: str = "",
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


class RateLimitError(BedrockError):
    """Throttling / quota exhaustion (``ThrottlingException`` etc.)."""

    def __init__(
        self,
        node_id: str,
        reset_time: datetime,
        raw_message: str = "",
        status_code: str = "",
    ):
        self.reset_time = reset_time
        super().__init__(node_id, "rate_limit", status_code, raw_message)

    def __reduce__(self):
        return (
            self.__class__,
            (self.node_id, self.reset_time, self.raw_message, self.status_code),
        )


class AuthenticationError(BedrockError):
    """Invalid / expired / unauthorized AWS credentials."""

    def __init__(self, node_id: str, status_code: str = "", raw_message: str = ""):
        super().__init__(node_id, "authentication_failed", status_code, raw_message)

    def __reduce__(self):
        return (self.__class__, (self.node_id, self.status_code, self.raw_message))


class InvalidRequestError(BedrockError):
    """Malformed request or unknown model (``ValidationException`` etc.)."""

    def __init__(self, node_id: str, status_code: str = "", raw_message: str = ""):
        super().__init__(node_id, "invalid_request", status_code, raw_message)

    def __reduce__(self):
        return (self.__class__, (self.node_id, self.status_code, self.raw_message))


class ServerError(BedrockError):
    """Transient service-side failure (5xx, model timeout, connection)."""

    def __init__(self, node_id: str, status_code: str = "", raw_message: str = ""):
        super().__init__(node_id, "server_error", status_code, raw_message)

    def __reduce__(self):
        return (self.__class__, (self.node_id, self.status_code, self.raw_message))


class MaxOutputTokensError(BedrockError):
    """The response was truncated at ``maxTokens``."""

    def __init__(
        self,
        node_id: str,
        status_code: str = "",
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


class UnknownBedrockError(BedrockError):
    """Unclassified Bedrock error."""

    def __init__(self, node_id: str, status_code: str = "", raw_message: str = ""):
        super().__init__(node_id, "unknown", status_code, raw_message)

    def __reduce__(self):
        return (self.__class__, (self.node_id, self.status_code, self.raw_message))


def _unwrap_exception_group(exc):
    """Drill through (possibly nested) ExceptionGroups to a representative leaf.

    MCP's anyio task group re-wraps any error that escapes the tool-connected
    ``AsyncExitStack`` in an ``ExceptionGroup`` (the ``exceptiongroup`` backport
    on Python < 3.11), which would otherwise hide a perfectly classifiable AWS
    error. Prefer an already-typed :class:`BedrockError`, then a botocore error,
    else the first leaf. Non-group exceptions are returned unchanged.
    """
    subs = getattr(exc, "exceptions", None)
    if not subs or not isinstance(subs, (list, tuple)):
        return exc
    leaves = [_unwrap_exception_group(s) for s in subs]
    for leaf in leaves:
        if isinstance(leaf, BedrockError):
            return leaf
    try:
        from botocore.exceptions import BotoCoreError, ClientError
        for leaf in leaves:
            if isinstance(leaf, (ClientError, BotoCoreError)):
                return leaf
    except Exception:
        pass
    return leaves[0]


class BedrockLLM(LLMCallBase):
    """Bedrock Converse-API LLM backend with client-side MCP tool execution.

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
        logging_name: str = "bedrock_llm",
        logging_level: int = logging.DEBUG,
        log_dir: Optional[str] = None,
        region: Optional[str] = None,
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
        self.region = (
            region
            or os.environ.get("AWS_REGION")
            or os.environ.get("AWS_DEFAULT_REGION")
        )
        self.max_tokens = max_tokens
        self.max_tool_iterations = max_tool_iterations
        self.client_kwargs = client_kwargs or {}
        self.logger = logging.getLogger(logging_name)
        self._last_metadata: dict = {}

        self.logger.warning(
            "BedrockLLM is experimental: only exercised by unit tests so far, "
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

    @ChiaFunction(resources={"bedrock_creds": 0.01})
    def prompt(
        self,
        user_message: str,
        tools: Optional[List[ChiaTool]] = [],
    ) -> QueryResult:
        """Send *user_message* via the Bedrock Converse API and return the
        response, retrying transient failures with the same policy the other
        backends use.
        """
        import time as _time

        from chia.trace.profiler import get_profiler

        profiler = get_profiler()

        for attempt in range(self.retries):
            try:
                self._last_metadata = {}
                cli = self._run_converse(user_message, tools)
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
            except (RateLimitError, AuthenticationError, InvalidRequestError):
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
            except UnknownBedrockError as exc:
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
    # Converse implementation
    # ------------------------------------------------------------------

    def _run_converse(
        self,
        user_message: str,
        tools: Optional[List[ChiaTool]] = None,
    ) -> QueryResult:
        """Synchronous entry point; drives the async agent loop.

        A second translation guard catches the case where a typed error (or a
        raw botocore error) escapes the tool-connected ``AsyncExitStack`` wrapped
        in an ``ExceptionGroup`` by MCP's anyio task group — without this, the
        wrapped error never matches ``prompt()``'s ``except`` clauses and every
        AWS error in the tools path is misfiled as "unexpected".
        """
        try:
            return self._run_coroutine(self._run_converse_async(user_message, tools or []))
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

    async def _run_converse_async(
        self,
        user_message: str,
        tools: Optional[List[ChiaTool]] = None,
    ) -> QueryResult:
        """Connect to each ChiaTool's MCP server, then run the Converse loop.

        Converse returns one assistant turn at a time; on ``stopReason ==
        "tool_use"`` we execute the requested tools against their MCP servers
        and feed ``toolResult`` blocks back, looping until the model stops
        (or :attr:`max_tool_iterations` is hit).
        """
        import asyncio
        from contextlib import AsyncExitStack

        import boto3
        from mcp import ClientSession
        from mcp.client.streamable_http import streamable_http_client

        client = boto3.client(
            "bedrock-runtime", region_name=self.region, **self.client_kwargs
        )

        stream_parts: list[str] = []
        stream_parts.append("=" * 80 + "\n")
        stream_parts.append(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Converse ({self.model})\n"
        )
        stream_parts.append("=" * 80 + "\n\n")
        truncated = user_message[:500] + ("..." if len(user_message) > 500 else "")
        stream_parts.append(f"[User Message]\n{truncated}\n\n")

        async with AsyncExitStack() as stack:
            # --- Connect to every MCP server and gather tool specs ---
            tool_specs: list[dict] = []
            dispatch: dict = {}  # converse tool name -> (session, mcp tool name)
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
                    tool_specs.append({
                        "toolSpec": {
                            "name": api_name,
                            "description": fn.description or " ",
                            "inputSchema": {
                                "json": fn.inputSchema
                                or {"type": "object", "properties": {}}
                            },
                        }
                    })
                    dispatch[api_name] = (session, fn.name)

            messages: list[dict] = [
                {"role": "user", "content": [{"text": user_message}]}
            ]
            meta = {"input_tokens": 0, "output_tokens": 0, "num_turns": 0}
            final_text = ""

            for _ in range(self.max_tool_iterations):
                kwargs: dict = {
                    "modelId": self.model,
                    "messages": messages,
                    "inferenceConfig": {"maxTokens": self.max_tokens},
                }
                if self.system_message:
                    kwargs["system"] = [{"text": self.system_message}]
                if tool_specs:
                    kwargs["toolConfig"] = {"tools": tool_specs}

                try:
                    # boto3 is synchronous; keep the event loop free for MCP.
                    resp = await asyncio.to_thread(client.converse, **kwargs)
                except Exception as exc:
                    translated = self._translate_error(exc)
                    if translated is not None:
                        raise translated from exc
                    raise

                meta["num_turns"] += 1
                usage = resp.get("usage", {})
                meta["input_tokens"] += usage.get("inputTokens", 0) or 0
                meta["output_tokens"] += usage.get("outputTokens", 0) or 0

                out_message = resp["output"]["message"]
                stop_reason = resp.get("stopReason")

                # --- Log this turn's content blocks ---
                turn_text_parts: list[str] = []
                tool_uses = []
                for block in out_message.get("content", []):
                    if "text" in block:
                        turn_text_parts.append(block["text"])
                        stream_parts.append(f"[Response]\n{block['text']}\n\n")
                    elif "reasoningContent" in block:
                        reasoning = block["reasoningContent"]
                        text = reasoning.get("reasoningText", {}).get("text", "")
                        if text:
                            stream_parts.append(f"[Thinking]\n{text}\n\n")
                    elif "toolUse" in block:
                        tu = block["toolUse"]
                        tool_uses.append(tu)
                        tool_input = json.dumps(tu.get("input", {}))
                        if len(tool_input) > 2000:
                            tool_input = tool_input[:2000] + "\n... [truncated]"
                        stream_parts.append(
                            f"[Tool Call: {tu.get('name')}]\nArgs: {tool_input}\n\n"
                        )
                if turn_text_parts:
                    final_text = "".join(turn_text_parts)

                # Echo the assistant turn back verbatim (toolUse blocks must
                # round-trip for the follow-up request).
                messages.append(out_message)

                if stop_reason == "max_tokens":
                    raise MaxOutputTokensError(
                        node_id=self._get_node_id(),
                        raw_message="response truncated at maxTokens",
                        partial_text=final_text,
                    )

                if stop_reason != "tool_use" or not tool_uses:
                    break

                # --- Execute each requested tool over its MCP server ---
                result_blocks = []
                for tu in tool_uses:
                    name = tu.get("name")
                    tool_use_id = tu.get("toolUseId")
                    session, fn_name = dispatch.get(name, (None, None))
                    if session is None:
                        result_text = f"Unknown tool: {name}"
                        is_error = True
                    else:
                        try:
                            mcp_result = await session.call_tool(
                                fn_name, tu.get("input") or {}
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
                    result_blocks.append({
                        "toolResult": {
                            "toolUseId": tool_use_id,
                            "content": [{"text": result_text}],
                            "status": "error" if is_error else "success",
                        }
                    })

                messages.append({"role": "user", "content": result_blocks})
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
        """Map a botocore exception to a local typed error.

        Returns the local exception to raise, or ``None`` when *exc* is not a
        recognised AWS/botocore error (so the caller re-raises it untouched).
        """
        # MCP's anyio task group wraps errors that escape the tool-connected
        # AsyncExitStack in (possibly nested) ExceptionGroups — drill down to
        # the real error first, otherwise nothing classifies in the tools path.
        exc = _unwrap_exception_group(exc)

        # Already one of ours (e.g. translated inside the loop, then re-wrapped
        # by the task group) -> pass straight through.
        if isinstance(exc, BedrockError):
            return exc

        try:
            from botocore.exceptions import (
                BotoCoreError,
                ClientError,
                ConnectTimeoutError,
                EndpointConnectionError,
                ParamValidationError,
                ReadTimeoutError,
            )
        except Exception:
            return None

        node_id = self._get_node_id()

        if isinstance(exc, ClientError):
            code = exc.response.get("Error", {}).get("Code", "")
            msg = exc.response.get("Error", {}).get("Message", str(exc))[:300]

            if code in ("ThrottlingException", "TooManyRequestsException",
                        "ServiceQuotaExceededException"):
                reset_time = datetime.now(timezone.utc) + timedelta(seconds=60)
                return RateLimitError(
                    node_id=node_id, reset_time=reset_time,
                    raw_message=msg, status_code=code,
                )
            if code in ("AccessDeniedException", "UnauthorizedException",
                        "ExpiredTokenException", "InvalidSignatureException",
                        "UnrecognizedClientException"):
                return AuthenticationError(node_id, status_code=code, raw_message=msg)
            if code in ("ValidationException", "ResourceNotFoundException",
                        "ServiceUnavailableException"):
                # ServiceUnavailable is transient -> ServerError below; everything
                # else here is a bad request.
                if code == "ServiceUnavailableException":
                    return ServerError(node_id, status_code=code, raw_message=msg)
                return InvalidRequestError(node_id, status_code=code, raw_message=msg)
            if code in ("InternalServerException", "ModelTimeoutException",
                        "ModelNotReadyException", "ModelErrorException"):
                return ServerError(node_id, status_code=code, raw_message=msg)
            return UnknownBedrockError(node_id, status_code=code, raw_message=msg)

        if isinstance(exc, (EndpointConnectionError, ConnectTimeoutError,
                            ReadTimeoutError)):
            return ServerError(node_id, raw_message=str(exc)[:300])

        # Client-side parameter validation (e.g. maxTokens below the minimum) is
        # a deterministic bad request — never-retry, not an Unknown that burns
        # the retry budget.
        if isinstance(exc, ParamValidationError):
            return InvalidRequestError(node_id, raw_message=str(exc)[:300])

        if isinstance(exc, BotoCoreError):
            return UnknownBedrockError(node_id, raw_message=str(exc)[:300])

        return None
