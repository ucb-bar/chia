"""Tests for :class:`chia.models.openai_compat.OpenAICompatLLM`.

Run from the repo root with the chia venv active::

    pytest chia/models/tests/test_openai_compat.py -v

Layers, matching the other backends' tests:

* **Offline unit tests** — construction, the client/auth selection
  (``base_url`` picks the provider; default is env-driven OpenAI), helpers.
* **Mocked-loop tests** — inject a fake ``openai`` module (+ fake MCP) so the
  full Chat Completions agent loop runs offline: request shaping, the
  tool_calls -> role="tool" loop (including JSON-string argument parsing),
  and metadata accumulation.
* **Tier 1 error translation** — build the *real* ``openai`` SDK exception
  objects and assert ``_translate_error`` maps each to the right typed error
  (covers every branch incl. transport errors and the retry-after math).
  Deterministic, in-process, no network.
* **Tier 2 stub-server tests** — stand up an in-repo OpenAI-compatible HTTP
  stub that returns *real* HTTP error codes (+ ``retry-after``), point the real
  ``openai`` client at it via ``base_url``, and assert the full path
  (SDK raises -> ``_translate_error`` classifies) produces the right typed
  error. Confirms the SDK genuinely produces those exception types for those
  statuses. Needs ``openai``/``httpx`` importable but **no API key** (loopback).
* **Live API tests** — skipped unless ``OPENAI_API_KEY`` is set and ``openai``
  is importable. Override the model with ``OPENAI_TEST_MODEL`` (default
  ``gpt-4o-mini``). Point at another provider by also setting
  ``OPENAI_BASE_URL`` + that provider's key.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import types
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from types import SimpleNamespace

import pytest

from chia.models.openai_compat import (
    AuthenticationError,
    BillingError,
    QueryResult,
    ContextLengthExceededError,
    InvalidRequestError,
    MaxOutputTokensError,
    OpenAICompatLLM,
    RateLimitError,
    ServerError,
    UnknownOpenAIError,
)


# ---------------------------------------------------------------------------
# Fakes — a stand-in ``openai`` module + MCP transport for offline tests
# ---------------------------------------------------------------------------


def _msg(content=None, tool_calls=None):
    return SimpleNamespace(role="assistant", content=content, tool_calls=tool_calls)


def _tc(call_id, name, arguments):
    return SimpleNamespace(
        id=call_id, type="function",
        function=SimpleNamespace(name=name, arguments=arguments),
    )


def _choice(message, finish_reason):
    return SimpleNamespace(message=message, finish_reason=finish_reason)


def _resp(choices, prompt_tokens=0, completion_tokens=0):
    return SimpleNamespace(
        choices=choices,
        usage=SimpleNamespace(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=prompt_tokens + completion_tokens,
        ),
    )


def _install_fake_openai(monkeypatch, responses, capture):
    """Inject a fake ``openai`` whose AsyncOpenAI.chat.completions.create
    returns ``responses`` in order, recording call payloads + client kwargs."""

    mod = types.ModuleType("openai")

    class _FakeCompletions:
        async def create(self, **kwargs):
            snapshot = dict(kwargs)
            if "messages" in snapshot:
                snapshot["messages"] = list(snapshot["messages"])
            capture["calls"].append(snapshot)
            return responses.pop(0)

    class _FakeChat:
        def __init__(self):
            self.completions = _FakeCompletions()

    class _FakeAsyncOpenAI:
        def __init__(self, **kwargs):
            capture["client_kwargs"] = kwargs
            self.chat = _FakeChat()

    mod.AsyncOpenAI = _FakeAsyncOpenAI
    monkeypatch.setitem(sys.modules, "openai", mod)
    return mod


def _install_fake_mcp(monkeypatch, capture, tool_result_text="42"):
    import mcp
    import mcp.client.streamable_http as streamable_mod

    class _FakeStreamCM:
        async def __aenter__(self):
            return (object(), object(), None)

        async def __aexit__(self, *exc):
            return False

    def _fake_streamable(url):
        capture["urls"].append(url)
        return _FakeStreamCM()

    class _FakeSession:
        def __init__(self, read, write):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def initialize(self):
            pass

        async def list_tools(self):
            return SimpleNamespace(tools=[
                SimpleNamespace(
                    name="run",
                    description="run a thing",
                    inputSchema={"type": "object", "properties": {}},
                )
            ])

        async def call_tool(self, name, args):
            capture["tool_calls"].append((name, args))
            return SimpleNamespace(
                content=[SimpleNamespace(type="text", text=tool_result_text)],
                isError=False,
            )

    monkeypatch.setattr(streamable_mod, "streamable_http_client", _fake_streamable)
    monkeypatch.setattr(mcp, "ClientSession", _FakeSession)


# ---------------------------------------------------------------------------
# Offline unit tests
# ---------------------------------------------------------------------------


def test_constructor_defaults():
    llm = OpenAICompatLLM(model="gpt-4o")
    assert llm.model == "gpt-4o"
    assert llm.base_url is None
    assert llm.api_key is None
    assert llm.max_tokens == 16000


def test_experimental_warning(caplog):
    with caplog.at_level("WARNING", logger="openai_compat_llm"):
        OpenAICompatLLM(model="gpt-4o")
    assert "experimental" in caplog.text


def test_prompt_exposes_chia_remote_surface():
    # @ChiaFunction makes prompt opt-in remotable: prompt(...) is local,
    # prompt.chia_remote(self, ...) / .options(resources=...).chia_remote(...)
    # dispatch to a (resource-gated) worker.
    assert hasattr(OpenAICompatLLM.prompt, "chia_remote")
    assert hasattr(OpenAICompatLLM.prompt, "options")


def test_make_client_env_default(monkeypatch):
    # No base_url / api_key / token_provider -> SDK reads the environment.
    capture = {"calls": []}
    _install_fake_openai(monkeypatch, [], capture)
    OpenAICompatLLM(model="gpt-4o")._make_client()
    assert capture["client_kwargs"] == {}


def test_make_client_base_url_selects_provider(monkeypatch):
    capture = {"calls": []}
    _install_fake_openai(monkeypatch, [], capture)
    OpenAICompatLLM(
        model="meta-llama/Llama-3.1-8B-Instruct",
        base_url="https://api.example-provider.com/v1",
        api_key="provider-key",
    )._make_client()
    assert capture["client_kwargs"]["base_url"] == "https://api.example-provider.com/v1"
    assert capture["client_kwargs"]["api_key"] == "provider-key"


def test_make_client_token_provider(monkeypatch):
    capture = {"calls": []}
    _install_fake_openai(monkeypatch, [], capture)
    OpenAICompatLLM(model="m", token_provider=lambda: "fresh-token")._make_client()
    assert capture["client_kwargs"]["api_key"] == "fresh-token"


def test_mcp_result_to_text_joins_text_blocks():
    result = SimpleNamespace(content=[
        SimpleNamespace(text="hello"),
        SimpleNamespace(text="world"),
    ])
    assert OpenAICompatLLM._mcp_result_to_text(result) == "hello\nworld"


def test_mcp_result_to_text_empty():
    assert OpenAICompatLLM._mcp_result_to_text(SimpleNamespace(content=None)) == ""


# ---------------------------------------------------------------------------
# Mocked-loop tests (fake openai; no network, no SDK required)
# ---------------------------------------------------------------------------


def test_chat_no_tools_request_shaping_and_result(monkeypatch):
    capture = {"calls": []}
    _install_fake_openai(
        monkeypatch,
        [_resp([_choice(_msg(content="PONG"), "stop")], prompt_tokens=10, completion_tokens=5)],
        capture,
    )

    llm = OpenAICompatLLM(model="gpt-4o", system_message="be terse", max_tokens=1234)
    cli = llm.prompt("ping", tools=[])

    assert cli.success is True
    assert cli.result == "PONG"
    assert cli.returncode == 0

    # Default client -> env-driven (no base_url/api_key passed).
    assert capture["client_kwargs"] == {}

    assert len(capture["calls"]) == 1
    kw = capture["calls"][0]
    assert kw["model"] == "gpt-4o"
    assert kw["max_tokens"] == 1234
    assert "tools" not in kw
    assert kw["messages"][0] == {"role": "system", "content": "be terse"}
    assert kw["messages"][1] == {"role": "user", "content": "ping"}

    assert llm._last_metadata["input_tokens"] == 10
    assert llm._last_metadata["output_tokens"] == 5
    assert llm._last_metadata["num_turns"] == 1
    assert llm._last_metadata["model"] == "gpt-4o"


def test_chat_tool_loop_executes_mcp_and_feeds_results(monkeypatch):
    capture = {"calls": [], "urls": [], "tool_calls": []}
    _install_fake_openai(
        monkeypatch,
        [
            _resp([_choice(
                _msg(content=None, tool_calls=[_tc("call_1", "calc__run", '{"x": 21}')]),
                "tool_calls",
            )], prompt_tokens=10, completion_tokens=5),
            _resp([_choice(_msg(content="the answer is 42"), "stop")],
                  prompt_tokens=3, completion_tokens=4),
        ],
        capture,
    )
    _install_fake_mcp(monkeypatch, capture, tool_result_text="42")

    tool = SimpleNamespace(name="calc", hostname="localhost", port=9001)
    llm = OpenAICompatLLM(model="gpt-4o")
    cli = llm.prompt("what is 21 doubled?", tools=[tool])

    assert cli.success is True
    assert cli.result == "the answer is 42"
    assert capture["urls"] == ["http://localhost:9001/calc/mcp"]
    # Arguments arrive as a JSON *string* and must be parsed.
    assert capture["tool_calls"] == [("run", {"x": 21})]
    assert len(capture["calls"]) == 2

    # First request advertised the namespaced tool in OpenAI function format.
    tools_arg = capture["calls"][0]["tools"]
    assert tools_arg[0]["type"] == "function"
    assert tools_arg[0]["function"]["name"] == "calc__run"
    assert capture["calls"][0]["tool_choice"] == "auto"

    # Second request carried the assistant tool_calls echo + the tool result.
    msgs = capture["calls"][1]["messages"]
    assert msgs[-2]["role"] == "assistant"
    assert msgs[-2]["tool_calls"][0]["id"] == "call_1"
    assert msgs[-1] == {"role": "tool", "tool_call_id": "call_1", "content": "42"}

    assert llm._last_metadata["num_turns"] == 2
    assert llm._last_metadata["input_tokens"] == 13


def test_chat_unknown_tool_reports_error(monkeypatch):
    capture = {"calls": [], "urls": [], "tool_calls": []}
    _install_fake_openai(
        monkeypatch,
        [
            _resp([_choice(
                _msg(tool_calls=[_tc("call_1", "calc__missing", "{}")]), "tool_calls",
            )]),
            _resp([_choice(_msg(content="done"), "stop")]),
        ],
        capture,
    )
    _install_fake_mcp(monkeypatch, capture)

    tool = SimpleNamespace(name="calc", hostname="localhost", port=9001)
    cli = OpenAICompatLLM(model="m").prompt("go", tools=[tool])

    assert cli.success is True
    assert capture["tool_calls"] == []  # real tool never called
    tool_msg = capture["calls"][1]["messages"][-1]
    assert tool_msg["role"] == "tool"
    assert "Unknown tool" in tool_msg["content"]


def test_chat_malformed_tool_arguments(monkeypatch):
    capture = {"calls": [], "urls": [], "tool_calls": []}
    _install_fake_openai(
        monkeypatch,
        [
            _resp([_choice(
                _msg(tool_calls=[_tc("call_1", "calc__run", "{not valid json")]),
                "tool_calls",
            )]),
            _resp([_choice(_msg(content="done"), "stop")]),
        ],
        capture,
    )
    _install_fake_mcp(monkeypatch, capture)

    tool = SimpleNamespace(name="calc", hostname="localhost", port=9001)
    cli = OpenAICompatLLM(model="m").prompt("go", tools=[tool])

    assert cli.success is True
    # Malformed JSON => the tool is NOT invoked; an error is fed back.
    assert capture["tool_calls"] == []
    tool_msg = capture["calls"][1]["messages"][-1]
    assert "Invalid tool arguments" in tool_msg["content"]


def test_chat_length_finish_raises_max_tokens(monkeypatch):
    capture = {"calls": []}
    _install_fake_openai(
        monkeypatch,
        [
            _resp([_choice(_msg(content="partial"), "length")]),
            _resp([_choice(_msg(content="partial again"), "length")]),
        ],
        capture,
    )
    llm = OpenAICompatLLM(model="m", retries=2)
    with pytest.raises(MaxOutputTokensError):
        llm.prompt("go", tools=[])
    assert len(capture["calls"]) == 2  # original + one retry


# ---------------------------------------------------------------------------
# Tier 1 — error translation (real openai SDK exception instances, no network)
#
# Build the genuine openai exception objects (same classes the SDK raises) and
# assert ``_translate_error`` maps each to the right typed error. This pins the
# type->classification contract deterministically and covers every branch,
# including the awkward request-only transport errors and the retry-after math.
# ---------------------------------------------------------------------------


def _api_error(exc_cls, status, headers=None):
    """A real ``APIStatusError`` subclass carrying an httpx.Response."""
    httpx = pytest.importorskip("httpx")
    req = httpx.Request("POST", "https://api.openai.com/v1/chat/completions")
    resp = httpx.Response(status, headers=headers or {}, request=req)
    return exc_cls("boom", response=resp, body=None)


def _api_error_body(exc_cls, status, message, code=None, headers=None):
    """A real ``APIStatusError`` subclass carrying an OpenAI-shaped error body
    (so ``exc.message`` / ``exc.code`` are populated like the SDK does)."""
    httpx = pytest.importorskip("httpx")
    req = httpx.Request("POST", "https://api.openai.com/v1/chat/completions")
    err = {"message": message}
    if code is not None:
        err["code"] = code
    body = {"error": err}
    resp = httpx.Response(status, headers=headers or {}, json=body, request=req)
    return exc_cls(message, response=resp, body=err)


def _transport_error(exc_cls):
    """A real ``APITimeoutError`` / ``APIConnectionError`` (request, no response)."""
    httpx = pytest.importorskip("httpx")
    req = httpx.Request("POST", "https://api.openai.com/v1/chat/completions")
    try:
        return exc_cls(request=req)  # APITimeoutError(request=...)
    except TypeError:
        return exc_cls(message="boom", request=req)  # APIConnectionError(message=, request=)


def test_translate_non_openai_returns_none():
    pytest.importorskip("openai")
    # An exception the SDK never raises -> caller re-raises it unchanged.
    assert OpenAICompatLLM(model="m")._translate_error(ValueError("nope")) is None


@pytest.mark.parametrize(
    "exc_name, status, expected, expected_code",
    [
        ("BadRequestError", 400, InvalidRequestError, 400),
        ("NotFoundError", 404, InvalidRequestError, 404),
        ("AuthenticationError", 401, AuthenticationError, 401),
        ("PermissionDeniedError", 403, AuthenticationError, 403),
        ("InternalServerError", 500, ServerError, 500),
    ],
)
def test_translate_status_errors(exc_name, status, expected, expected_code):
    openai = pytest.importorskip("openai")
    llm = OpenAICompatLLM(model="m")
    out = llm._translate_error(_api_error(getattr(openai, exc_name), status))
    assert isinstance(out, expected)
    assert out.status_code == expected_code


@pytest.mark.parametrize("exc_name", ["APITimeoutError", "APIConnectionError"])
def test_translate_transport_errors_are_server_errors(exc_name):
    openai = pytest.importorskip("openai")
    llm = OpenAICompatLLM(model="m")
    # No HTTP response at all (the request never completed) -> ServerError, no code.
    out = llm._translate_error(_transport_error(getattr(openai, exc_name)))
    assert isinstance(out, ServerError)
    assert out.status_code is None


def test_translate_generic_status_error_5xx_is_server():
    openai = pytest.importorskip("openai")
    # A bare APIStatusError (not one of the named subclasses) with a 5xx code
    # falls through to ServerError.
    out = OpenAICompatLLM(model="m")._translate_error(
        _api_error(openai.APIStatusError, 502)
    )
    assert isinstance(out, ServerError)
    assert out.status_code == 502


def test_translate_generic_status_error_4xx_is_unknown():
    openai = pytest.importorskip("openai")
    # An unrecognised 4xx (e.g. 418) is neither auth/invalid/server -> Unknown,
    # but the status code is preserved for diagnostics.
    out = OpenAICompatLLM(model="m")._translate_error(
        _api_error(openai.APIStatusError, 418)
    )
    assert isinstance(out, UnknownOpenAIError)
    assert out.status_code == 418


def test_translate_rate_limit_honors_retry_after():
    openai = pytest.importorskip("openai")
    llm = OpenAICompatLLM(model="m")
    before = datetime.now(timezone.utc)
    rl = llm._translate_error(
        _api_error(openai.RateLimitError, 429, headers={"retry-after": "30"})
    )
    assert isinstance(rl, RateLimitError)
    assert rl.status_code == 429
    # reset_time should be ~30s out (honoring the header, not the 60s default).
    delta = (rl.reset_time - before).total_seconds()
    assert 28 <= delta <= 32


def test_translate_rate_limit_defaults_without_header():
    openai = pytest.importorskip("openai")
    llm = OpenAICompatLLM(model="m")
    before = datetime.now(timezone.utc)
    rl = llm._translate_error(_api_error(openai.RateLimitError, 429))
    assert isinstance(rl, RateLimitError)
    # No retry-after header -> the 60s fallback.
    delta = (rl.reset_time - before).total_seconds()
    assert 58 <= delta <= 62


# ---------------------------------------------------------------------------
# Tier 1c — ExceptionGroup unwrapping (the tools-path wrapping bug)
#
# When MCP tools are connected, a provider error propagates out of the
# AsyncExitStack and anyio's task group re-wraps it in an ExceptionGroup
# (the `exceptiongroup` backport on Python < 3.11). Without unwrapping, the
# typed error never matches prompt()'s except clauses and is misfiled as
# "unexpected" and retried. Found by running the live tools test on OpenRouter.
# ---------------------------------------------------------------------------

try:  # py<3.11 backport (an anyio dependency); builtin on 3.11+
    from exceptiongroup import ExceptionGroup as _ExcGroup
except ImportError:  # pragma: no cover
    _ExcGroup = ExceptionGroup


def test_translate_unwraps_exception_group():
    openai = pytest.importorskip("openai")
    grp = _ExcGroup("unhandled errors in a TaskGroup",
                    [_api_error(openai.AuthenticationError, 401)])
    out = OpenAICompatLLM(model="m")._translate_error(grp)
    assert isinstance(out, AuthenticationError)


def test_translate_unwraps_nested_exception_group():
    openai = pytest.importorskip("openai")
    inner = _ExcGroup("inner", [_api_error(openai.RateLimitError, 429,
                                           headers={"retry-after": "9"})])
    outer = _ExcGroup("outer", [inner])  # MCP can nest groups two deep
    out = OpenAICompatLLM(model="m")._translate_error(outer)
    assert isinstance(out, RateLimitError)
    assert out.status_code == 429


def test_translate_passes_through_wrapped_typed_error():
    # A typed error that was translated inside the loop and then re-wrapped by
    # the task group must survive as the SAME typed error, not be re-derived.
    pytest.importorskip("openai")
    typed = AuthenticationError("node-x", status_code=401, raw_message="orig")
    grp = _ExcGroup("g", [_ExcGroup("g2", [typed])])
    out = OpenAICompatLLM(model="m")._translate_error(grp)
    assert out is typed


def test_translate_exception_group_billing_and_context():
    openai = pytest.importorskip("openai")
    llm = OpenAICompatLLM(model="m")
    ctx = _ExcGroup("g", [_api_error_body(openai.BadRequestError, 400,
                                          "maximum context length is 8192 tokens")])
    assert type(llm._translate_error(ctx)) is ContextLengthExceededError
    bill = _ExcGroup("g", [_api_error_body(openai.RateLimitError, 429,
                                           "insufficient_quota", code="insufficient_quota")])
    assert type(llm._translate_error(bill)) is BillingError


# ---------------------------------------------------------------------------
# Tier 1b — body-aware disambiguation (context-length, billing/quota)
# ---------------------------------------------------------------------------


def test_translate_context_length_by_message():
    openai = pytest.importorskip("openai")
    out = OpenAICompatLLM(model="m")._translate_error(_api_error_body(
        openai.BadRequestError, 400,
        "This model's maximum context length is 8192 tokens. Please reduce the length.",
    ))
    assert type(out) is ContextLengthExceededError
    assert out.status_code == 400
    # Subclass of InvalidRequestError => inherits never-retry semantics.
    assert isinstance(out, InvalidRequestError)


def test_translate_context_length_by_code():
    openai = pytest.importorskip("openai")
    out = OpenAICompatLLM(model="m")._translate_error(_api_error_body(
        openai.BadRequestError, 400, "prompt too big", code="context_length_exceeded",
    ))
    assert type(out) is ContextLengthExceededError


def test_translate_context_length_on_413():
    openai = pytest.importorskip("openai")
    # Some providers use 413 Payload Too Large -> generic APIStatusError.
    out = OpenAICompatLLM(model="m")._translate_error(_api_error_body(
        openai.APIStatusError, 413, "context length exceeded for this request",
    ))
    assert type(out) is ContextLengthExceededError


def test_translate_plain_400_stays_invalid_request():
    openai = pytest.importorskip("openai")
    # A non-context 400 must NOT be swept into ContextLengthExceededError.
    out = OpenAICompatLLM(model="m")._translate_error(_api_error_body(
        openai.BadRequestError, 400, "Expected max_tokens to be at least 1",
    ))
    assert type(out) is InvalidRequestError


def test_translate_billing_429_insufficient_quota():
    openai = pytest.importorskip("openai")
    # Quota exhaustion arrives as a 429 but must classify as billing, not a
    # rate limit (retrying cannot restore quota).
    out = OpenAICompatLLM(model="m")._translate_error(_api_error_body(
        openai.RateLimitError, 429,
        "You exceeded your current quota, please check your plan and billing details.",
        code="insufficient_quota", headers={"retry-after": "5"},
    ))
    assert type(out) is BillingError
    assert out.status_code == 429


def test_translate_billing_402_payment_required():
    openai = pytest.importorskip("openai")
    out = OpenAICompatLLM(model="m")._translate_error(_api_error_body(
        openai.APIStatusError, 402, "Payment required: insufficient credits.",
    ))
    assert type(out) is BillingError
    assert out.status_code == 402


def test_translate_plain_429_stays_rate_limit():
    openai = pytest.importorskip("openai")
    # A genuine rate limit (no billing words) is still a RateLimitError.
    out = OpenAICompatLLM(model="m")._translate_error(_api_error_body(
        openai.RateLimitError, 429, "Rate limit exceeded, slow down.",
        headers={"retry-after": "3"},
    ))
    assert type(out) is RateLimitError


# ---------------------------------------------------------------------------
# Tier 3 — real captured provider errors (gold-standard fixtures)
#
# Pinned from a live OpenRouter run (2026-06-01) under
# ``fixtures/openai_compat/`` — see that dir's README. These guard against the
# idealized-shape trap: each fixture is the *real* (status, headers, body) a
# provider returned, reconstructed into the openai exception the SDK actually
# raised, then run through ``_translate_error``. They caught two wrong
# assumptions: OpenRouter returns **400 (not 404)** for an unknown model, and
# context-overflow is a **400 with a length message** (classified
# InvalidRequestError, NOT MaxOutputTokensError — a known body-level gap).
# ---------------------------------------------------------------------------

import pathlib  # noqa: E402

_OPENAI_COMPAT_FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "openai_compat"

_REAL_FIXTURES = [
    "openrouter_auth_401.json",
    "openrouter_bad_model.json",
    "openrouter_over_context_400.json",
    "openrouter_invalid_param_400.json",
    "openrouter_rate_limit_429.json",
    # Synthetic (no real capture) — billing/quota patterns from provider conventions.
    "synthetic_billing_429_insufficient_quota.json",
    "synthetic_billing_402_payment.json",
]

_LOCAL_ERROR_TYPES = {
    "AuthenticationError": AuthenticationError,
    "BillingError": BillingError,
    "ContextLengthExceededError": ContextLengthExceededError,
    "InvalidRequestError": InvalidRequestError,
    "ServerError": ServerError,
    "RateLimitError": RateLimitError,
    "UnknownOpenAIError": UnknownOpenAIError,
}


def _exc_from_fixture(fx):
    """Rebuild the genuine openai exception from a captured (status, headers, body)."""
    openai = pytest.importorskip("openai")
    httpx = pytest.importorskip("httpx")
    req = httpx.Request("POST", "https://openrouter.ai/api/v1/chat/completions")
    resp = httpx.Response(fx["status"], headers=fx.get("headers", {}),
                          json=fx.get("body", {}), request=req)
    exc_cls = getattr(openai, fx["openai_exc_type"])
    return exc_cls(fx["body"].get("error", {}).get("message", "boom") if
                   isinstance(fx.get("body"), dict) else "boom",
                   response=resp, body=fx.get("body"))


@pytest.mark.parametrize("fname", _REAL_FIXTURES)
def test_classify_on_real_openrouter_fixture(fname):
    pytest.importorskip("openai")
    fx = json.loads((_OPENAI_COMPAT_FIXTURES / fname).read_text())
    out = OpenAICompatLLM(model="m")._translate_error(_exc_from_fixture(fx))
    assert type(out) is _LOCAL_ERROR_TYPES[fx["expected_chia"]]
    if fx["status"] in (400, 401, 429):
        assert out.status_code == fx["status"]
    # The real rate-limit header must drive the reset_time.
    if "expected_reset_after_seconds" in fx:
        before = datetime.now(timezone.utc)
        delta = (out.reset_time - before).total_seconds()
        secs = fx["expected_reset_after_seconds"]
        assert secs - 2 <= delta <= secs + 2


def test_real_fixtures_cover_documented_assumptions():
    # Guard the surprises real capture revealed, so a future refactor has to
    # consciously update these fixtures.
    bad_model = json.loads((_OPENAI_COMPAT_FIXTURES / "openrouter_bad_model.json").read_text())
    assert bad_model["status"] == 400  # unknown model is 400, NOT 404
    overflow = json.loads((_OPENAI_COMPAT_FIXTURES / "openrouter_over_context_400.json").read_text())
    assert overflow["status"] == 400  # context-overflow rides on a 400
    assert "context length" in overflow["body"]["error"]["message"]
    # Body-aware classification now splits it out from a generic bad request.
    assert overflow["expected_chia"] == "ContextLengthExceededError"


# ---------------------------------------------------------------------------
# Live tests (real API; need OPENAI_API_KEY + openai installed)
# ---------------------------------------------------------------------------

live = pytest.mark.skipif(
    not os.environ.get("OPENAI_API_KEY"),
    reason="OPENAI_API_KEY not set",
)


@live
def test_live_chat_simple_prompt():
    pytest.importorskip("openai")
    llm = OpenAICompatLLM(
        model=os.environ.get("OPENAI_TEST_MODEL", "gpt-4o-mini"),
        system_message="You answer with a single word and nothing else.",
        max_tokens=64,
    )
    cli = llm.prompt("Reply with exactly the word: PONG", tools=[])
    assert cli.success is True
    assert "PONG" in cli.result.upper()
    assert llm._last_metadata.get("output_tokens", 0) > 0


@pytest.fixture
def local_bash_tool():
    pytest.importorskip("openai")
    if not os.environ.get("OPENAI_API_KEY"):
        pytest.skip("OPENAI_API_KEY not set")

    import uuid

    import ray

    from chia.base.tools.BashTool import BashTool

    ray.init(ignore_reinit_error=True)
    tool = BashTool(name=f"echo_{uuid.uuid4().hex[:8]}", work_dir="/tmp")
    try:
        yield tool
    finally:
        tool.stop()


@live
def test_live_chat_with_bash_tool(local_bash_tool):
    llm = OpenAICompatLLM(
        model=os.environ.get("OPENAI_TEST_MODEL", "gpt-4o-mini"),
        system_message=(
            "You have a bash tool available. To answer the user you MUST run "
            "the requested shell command with that tool and report its output "
            "verbatim. Never guess the output."
        ),
        max_tokens=2048,
    )
    cli = llm.prompt(
        "Run the shell command:  echo CHIA_TOOL_OK\n"
        "Then reply with exactly the line it printed.",
        tools=[local_bash_tool],
    )

    assert cli.success is True
    assert "CHIA_TOOL_OK" in cli.result
    assert llm._last_metadata.get("num_turns", 0) >= 2
    assert "Tool Call" in cli.stream_result
    assert "Tool Result" in cli.stream_result


# ---------------------------------------------------------------------------
# Tier 2 — live tests against an in-repo OpenAI-compatible stub server
#
# A tiny stdlib HTTP server that speaks just enough of the Chat Completions
# wire protocol to drive every error path. The status code to return is encoded
# in the requested model name (``status-429`` -> HTTP 429); ``status-200``
# returns a valid completion. The real ``openai`` client connects over loopback,
# so the SDK constructs its genuine exception objects from real HTTP responses
# and ``_translate_error`` classifies them — the same end-to-end path a live
# provider exercises, but deterministic and credential-free.
#
# Off-the-shelf OpenAI mock servers (e.g. dwmkerr/mock-llm) can't set a custom
# ``retry-after`` header, so they can't exercise the rate-limit reset math; this
# stub can, which is why it lives in-repo.
# ---------------------------------------------------------------------------


class _StubHandler(BaseHTTPRequestHandler):
    """Map the requested model ``status-<code>`` to that HTTP status."""

    def log_message(self, *args):  # silence per-request logging
        pass

    def do_POST(self):
        length = int(self.headers.get("content-length", 0) or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            body = {}
        model = body.get("model", "")
        # model name encodes the response: "status-<code>" or
        # "status-<code>-<kind>" (kind in {context, quota} shapes the body).
        code, kind = 200, ""
        if model.startswith("status-"):
            parts = model.split("-")
            try:
                code = int(parts[1])
            except (IndexError, ValueError):
                code = 200
            if len(parts) > 2:
                kind = parts[2]

        if code == 200:
            payload = {
                "id": "chatcmpl-stub", "object": "chat.completion", "created": 0,
                "model": model,
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": "PONG"},
                    "finish_reason": "stop",
                }],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            }
        elif kind == "context":
            payload = {"error": {
                "message": "This model's maximum context length is 8192 tokens. "
                           "Please reduce the length.",
                "type": "invalid_request_error", "code": "context_length_exceeded",
            }}
        elif kind == "quota":
            payload = {"error": {
                "message": "You exceeded your current quota, check your billing details.",
                "type": "insufficient_quota", "code": "insufficient_quota",
            }}
        else:
            payload = {"error": {
                "message": f"stub error {code}", "type": "stub_error", "code": str(code),
            }}

        data = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("content-type", "application/json")
        if code == 429:
            self.send_header("retry-after", "7")  # exercised by the reset_time assertion
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


@pytest.fixture(scope="module")
def stub_server():
    pytest.importorskip("openai")
    pytest.importorskip("httpx")
    server = HTTPServer(("127.0.0.1", 0), _StubHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/v1"
    finally:
        server.shutdown()
        thread.join(timeout=5)


def _stub_llm(base_url, status):
    # max_retries=0 so the SDK doesn't transparently retry 429/5xx (which would
    # also make the test wait out the retry-after header). We want the first
    # error surfaced straight to _translate_error.
    return OpenAICompatLLM(
        model=f"status-{status}",
        base_url=base_url,
        api_key="sk-stub-not-a-real-key",
        client_kwargs={"max_retries": 0},
    )


def test_stub_success_returns_completion(stub_server):
    cli = _stub_llm(stub_server, 200)._run_openai("ping", [])
    assert cli.result == "PONG"
    assert cli.returncode == 0


@pytest.mark.parametrize(
    "status, expected",
    [
        (400, InvalidRequestError),
        (401, AuthenticationError),
        (403, AuthenticationError),
        (404, InvalidRequestError),
        (500, ServerError),
        (503, ServerError),
        (418, UnknownOpenAIError),  # unrecognised 4xx -> Unknown
    ],
)
def test_stub_error_status_classifies(stub_server, status, expected):
    with pytest.raises(expected):
        _stub_llm(stub_server, status)._run_openai("ping", [])


def test_stub_rate_limit_reads_retry_after_header(stub_server):
    before = datetime.now(timezone.utc)
    with pytest.raises(RateLimitError) as excinfo:
        _stub_llm(stub_server, 429)._run_openai("ping", [])
    # The reset_time comes from the live response's retry-after: 7 header — the
    # path an off-the-shelf mock without header support can't reach.
    delta = (excinfo.value.reset_time - before).total_seconds()
    assert 5 <= delta <= 9
    assert excinfo.value.status_code == 429


def test_stub_prompt_auth_error_propagates_without_retry(stub_server):
    # Through the public prompt() surface: AuthenticationError is in the
    # never-retry set, so it propagates rather than being swallowed into a
    # failure QueryResult.
    llm = _stub_llm(stub_server, 401)
    with pytest.raises(AuthenticationError):
        llm.prompt("ping", tools=[])


def test_stub_context_length_classifies(stub_server):
    # 400 with a context-length body -> ContextLengthExceededError end-to-end.
    with pytest.raises(ContextLengthExceededError):
        _stub_llm(stub_server, "400-context")._run_openai("ping", [])


def test_stub_billing_quota_classifies(stub_server):
    # 429 carrying insufficient_quota -> BillingError, not RateLimitError.
    with pytest.raises(BillingError):
        _stub_llm(stub_server, "429-quota")._run_openai("ping", [])


def test_stub_billing_402_classifies(stub_server):
    # A bare 402 is billing regardless of body.
    with pytest.raises(BillingError):
        _stub_llm(stub_server, 402)._run_openai("ping", [])


def test_stub_prompt_billing_propagates_without_retry(stub_server):
    # BillingError must be in the never-retry set (added alongside the body-aware
    # classification) so it propagates instead of being retried/swallowed.
    with pytest.raises(BillingError):
        _stub_llm(stub_server, "429-quota").prompt("ping", tools=[])
