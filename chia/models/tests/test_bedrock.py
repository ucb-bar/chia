"""Tests for :class:`chia.models.bedrock.BedrockLLM` (Converse API backend).

Run from the repo root with the chia venv active::

    pytest chia/models/tests/test_bedrock.py -v

Three layers, matching test_claude_api.py:

* **Offline unit tests** — construction, helpers, the experimental warning.
* **Mocked-loop tests** — inject a fake ``boto3`` (and fake MCP transport)
  so the full Converse agent loop runs offline: request shaping, the
  toolUse -> toolResult loop, and metadata accumulation.
* **Live tests** — skipped unless ``BEDROCK_TEST_MODEL`` is set (and, for the
  tool test, Ray can start locally). They actually call Bedrock, so they need
  AWS credentials + region in the environment and a model your account has
  access to, e.g.::

      AWS_REGION=us-east-1 BEDROCK_TEST_MODEL=us.amazon.nova-lite-v1:0 \
          pytest chia/models/tests/test_bedrock.py -v
"""

from __future__ import annotations

import os
import sys
import types
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from chia.models.bedrock import (
    AuthenticationError,
    BedrockLLM,
    QueryResult,
    InvalidRequestError,
    MaxOutputTokensError,
    RateLimitError,
    ServerError,
    UnknownBedrockError,
)


# ---------------------------------------------------------------------------
# Fakes — a stand-in ``boto3`` and MCP transport for offline tests
# ---------------------------------------------------------------------------


def _cv_text(text):
    return {"text": text}


def _cv_tool_use(tool_use_id, name, tool_input):
    return {"toolUse": {"toolUseId": tool_use_id, "name": name, "input": tool_input}}


def _cv_response(blocks, stop_reason, in_tok=0, out_tok=0):
    return {
        "output": {"message": {"role": "assistant", "content": blocks}},
        "stopReason": stop_reason,
        "usage": {
            "inputTokens": in_tok,
            "outputTokens": out_tok,
            "totalTokens": in_tok + out_tok,
        },
    }


def _install_fake_boto3(monkeypatch, responses, capture):
    """Inject a fake ``boto3`` whose client.converse returns ``responses`` in
    order, recording each call payload (with a snapshot of ``messages``)."""

    mod = types.ModuleType("boto3")

    class _FakeClient:
        def converse(self, **kwargs):
            snapshot = dict(kwargs)
            if "messages" in snapshot:
                snapshot["messages"] = list(snapshot["messages"])
            capture["calls"].append(snapshot)
            return responses.pop(0)

    def _client(name, region_name=None, **kw):
        capture["client"] = {"name": name, "region": region_name, "kwargs": kw}
        return _FakeClient()

    mod.client = _client
    monkeypatch.setitem(sys.modules, "boto3", mod)
    return mod


def _install_fake_mcp(monkeypatch, capture, tool_result_text="42"):
    """Patch the MCP transport + session so a fake server advertises one tool
    (``run``) and echoes a result."""

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


def test_constructor_basics(monkeypatch):
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    llm = BedrockLLM(model="meta.llama3-1-8b-instruct-v1:0")
    assert llm.model == "meta.llama3-1-8b-instruct-v1:0"
    assert llm.region == "us-west-2"
    assert llm.max_tokens == 16000


def test_explicit_region_overrides_env(monkeypatch):
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    llm = BedrockLLM(model="m", region="eu-central-1")
    assert llm.region == "eu-central-1"


def test_experimental_warning(caplog):
    with caplog.at_level("WARNING", logger="bedrock_llm"):
        BedrockLLM(model="m")
    assert "experimental" in caplog.text


def test_mcp_result_to_text_joins_text_blocks():
    result = SimpleNamespace(content=[
        SimpleNamespace(text="hello"),
        SimpleNamespace(text="world"),
    ])
    assert BedrockLLM._mcp_result_to_text(result) == "hello\nworld"


def test_mcp_result_to_text_empty():
    assert BedrockLLM._mcp_result_to_text(SimpleNamespace(content=None)) == ""


# ---------------------------------------------------------------------------
# Mocked-loop tests (fake boto3; no network, no AWS creds required)
# ---------------------------------------------------------------------------


def test_converse_no_tools_request_shaping_and_result(monkeypatch):
    capture = {"calls": []}
    _install_fake_boto3(
        monkeypatch,
        [_cv_response([_cv_text("PONG")], "end_turn", in_tok=10, out_tok=5)],
        capture,
    )

    llm = BedrockLLM(model="amazon.nova-lite-v1:0", system_message="be terse",
                     region="us-east-1", max_tokens=1234)
    cli = llm.prompt("ping", tools=[])

    assert cli.success is True
    assert cli.result == "PONG"
    assert cli.returncode == 0

    assert capture["client"]["name"] == "bedrock-runtime"
    assert capture["client"]["region"] == "us-east-1"

    assert len(capture["calls"]) == 1
    kw = capture["calls"][0]
    assert kw["modelId"] == "amazon.nova-lite-v1:0"
    assert kw["inferenceConfig"]["maxTokens"] == 1234
    assert kw["system"] == [{"text": "be terse"}]
    assert "toolConfig" not in kw

    assert llm._last_metadata["input_tokens"] == 10
    assert llm._last_metadata["output_tokens"] == 5
    assert llm._last_metadata["num_turns"] == 1
    assert llm._last_metadata["model"] == "amazon.nova-lite-v1:0"


def test_converse_tool_loop_executes_mcp_and_feeds_results(monkeypatch):
    capture = {"calls": [], "urls": [], "tool_calls": []}
    _install_fake_boto3(
        monkeypatch,
        [
            _cv_response(
                [_cv_tool_use("tu1", "calc__run", {"x": 21})],
                "tool_use", in_tok=10, out_tok=5,
            ),
            _cv_response([_cv_text("the answer is 42")], "end_turn",
                         in_tok=3, out_tok=4),
        ],
        capture,
    )
    _install_fake_mcp(monkeypatch, capture, tool_result_text="42")

    tool = SimpleNamespace(name="calc", hostname="localhost", port=9001)
    llm = BedrockLLM(model="anthropic.claude-sonnet-4-6", region="us-east-1")
    cli = llm.prompt("what is 21 doubled?", tools=[tool])

    assert cli.success is True
    assert cli.result == "the answer is 42"
    assert capture["urls"] == ["http://localhost:9001/calc/mcp"]
    assert capture["tool_calls"] == [("run", {"x": 21})]
    assert len(capture["calls"]) == 2

    # First request advertised the namespaced tool via Converse toolConfig.
    spec = capture["calls"][0]["toolConfig"]["tools"][0]["toolSpec"]
    assert spec["name"] == "calc__run"
    assert "json" in spec["inputSchema"]

    # Second request carried the toolResult back.
    last_msg = capture["calls"][1]["messages"][-1]
    assert last_msg["role"] == "user"
    tool_result = last_msg["content"][0]["toolResult"]
    assert tool_result["toolUseId"] == "tu1"
    assert tool_result["status"] == "success"
    assert tool_result["content"][0]["text"] == "42"

    assert llm._last_metadata["num_turns"] == 2
    assert llm._last_metadata["input_tokens"] == 13


def test_converse_unknown_tool_reports_error(monkeypatch):
    capture = {"calls": [], "urls": [], "tool_calls": []}
    _install_fake_boto3(
        monkeypatch,
        [
            _cv_response([_cv_tool_use("tu1", "calc__missing", {})], "tool_use"),
            _cv_response([_cv_text("done")], "end_turn"),
        ],
        capture,
    )
    _install_fake_mcp(monkeypatch, capture)

    tool = SimpleNamespace(name="calc", hostname="localhost", port=9001)
    llm = BedrockLLM(model="m", region="us-east-1")
    cli = llm.prompt("go", tools=[tool])

    assert cli.success is True
    assert capture["tool_calls"] == []  # real tool never called
    tool_result = capture["calls"][1]["messages"][-1]["content"][0]["toolResult"]
    assert tool_result["status"] == "error"
    assert "Unknown tool" in tool_result["content"][0]["text"]


def test_converse_max_tokens_raises(monkeypatch):
    capture = {"calls": []}
    # A max_tokens stop is retried once; the second truncation propagates.
    _install_fake_boto3(
        monkeypatch,
        [
            _cv_response([_cv_text("partial")], "max_tokens"),
            _cv_response([_cv_text("partial again")], "max_tokens"),
        ],
        capture,
    )
    llm = BedrockLLM(model="m", region="us-east-1", retries=2)
    with pytest.raises(MaxOutputTokensError):
        llm.prompt("go", tools=[])
    assert len(capture["calls"]) == 2  # original + one retry


# ---------------------------------------------------------------------------
# Error translation (boto3/botocore is a dependency, so these run)
# ---------------------------------------------------------------------------


def _client_error(code, message="boom"):
    botocore = pytest.importorskip("botocore")
    from botocore.exceptions import ClientError
    return ClientError({"Error": {"Code": code, "Message": message}}, "Converse")


def test_translate_non_botocore_returns_none():
    llm = BedrockLLM(model="m")
    assert llm._translate_error(ValueError("nope")) is None


def test_translate_throttling_to_rate_limit():
    llm = BedrockLLM(model="m")
    t = llm._translate_error(_client_error("ThrottlingException"))
    assert isinstance(t, RateLimitError)
    assert t.reset_time > datetime.now(timezone.utc)


def test_translate_access_denied_to_auth():
    llm = BedrockLLM(model="m")
    assert isinstance(
        llm._translate_error(_client_error("AccessDeniedException")),
        AuthenticationError,
    )


def test_translate_validation_to_invalid_request():
    llm = BedrockLLM(model="m")
    assert isinstance(
        llm._translate_error(_client_error("ValidationException")),
        InvalidRequestError,
    )


def test_translate_service_unavailable_to_server_error():
    llm = BedrockLLM(model="m")
    assert isinstance(
        llm._translate_error(_client_error("ServiceUnavailableException")),
        ServerError,
    )


def test_translate_internal_error_to_server_error():
    llm = BedrockLLM(model="m")
    assert isinstance(
        llm._translate_error(_client_error("InternalServerException")),
        ServerError,
    )


def test_translate_unknown_code():
    llm = BedrockLLM(model="m")
    assert isinstance(
        llm._translate_error(_client_error("SomethingWeird")),
        UnknownBedrockError,
    )


def test_translate_param_validation_to_invalid_request():
    # Client-side ParamValidationError (a BotoCoreError) is a deterministic bad
    # request -> InvalidRequestError (never-retry), NOT UnknownBedrockError
    # (which would burn the retry budget). Real capture: converse maxTokens=0.
    from botocore.exceptions import ParamValidationError
    llm = BedrockLLM(model="m")
    exc = ParamValidationError(
        report="Invalid value for parameter inferenceConfig.maxTokens, "
               "value: 0, valid min value: 1"
    )
    assert isinstance(llm._translate_error(exc), InvalidRequestError)


# ---------------------------------------------------------------------------
# ExceptionGroup unwrapping (the tools-path wrapping bug)
#
# When MCP tools are connected, a botocore error propagates out of the
# AsyncExitStack and anyio's task group re-wraps it in an ExceptionGroup
# (the `exceptiongroup` backport on Python < 3.11). Without unwrapping, the
# typed error never matches prompt()'s except clauses and is misfiled as
# "unexpected" and retried. Confirmed live via a real BashTool + boto3 stub.
# ---------------------------------------------------------------------------

try:  # py<3.11 backport (an anyio dependency); builtin on 3.11+
    from exceptiongroup import ExceptionGroup as _ExcGroup
except ImportError:  # pragma: no cover
    _ExcGroup = ExceptionGroup


def test_translate_unwraps_exception_group():
    llm = BedrockLLM(model="m")
    grp = _ExcGroup("unhandled errors in a TaskGroup",
                    [_client_error("ThrottlingException")])
    assert isinstance(llm._translate_error(grp), RateLimitError)


def test_translate_unwraps_nested_exception_group():
    llm = BedrockLLM(model="m")
    inner = _ExcGroup("inner", [_client_error("AccessDeniedException")])
    outer = _ExcGroup("outer", [inner])  # MCP can nest groups two deep
    assert isinstance(llm._translate_error(outer), AuthenticationError)


def test_translate_passes_through_wrapped_typed_error():
    # A typed error translated inside the loop and then re-wrapped by the task
    # group must survive as the SAME object, not be re-derived.
    typed = AuthenticationError("node-x", status_code="AccessDeniedException",
                                raw_message="orig")
    grp = _ExcGroup("g", [_ExcGroup("g2", [typed])])
    assert BedrockLLM(model="m")._translate_error(grp) is typed


# ---------------------------------------------------------------------------
# Fixture-driven classification (real + synthetic Converse error shapes)
#
# Real captures pinned from a live Nova Lite run (us-east-1, 2026-06-01) under
# fixtures/bedrock/; throttling/auth/5xx are synthetic (can't be triggered
# deterministically here). Each rebuilds a botocore ClientError from the
# captured {Code, Message, HTTPStatusCode} and asserts _translate_error
# classifies it as documented. See that dir's README.
# ---------------------------------------------------------------------------

import pathlib  # noqa: E402

_BEDROCK_FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "bedrock"

_LOCAL_ERROR_TYPES = {
    "RateLimitError": RateLimitError,
    "AuthenticationError": AuthenticationError,
    "InvalidRequestError": InvalidRequestError,
    "ServerError": ServerError,
    "UnknownBedrockError": UnknownBedrockError,
}

_BEDROCK_FIXTURE_FILES = [
    "bedrock_validation_unknown_model.json",
    "bedrock_validation_over_context.json",
    "bedrock_throttling_429.json",
    "bedrock_access_denied.json",
    "bedrock_service_unavailable.json",
    "bedrock_internal_server.json",
]


def _client_error_from_fixture(fx):
    from botocore.exceptions import ClientError
    response = {
        "Error": {"Code": fx["code"], "Message": fx["message"]},
        "ResponseMetadata": {"HTTPStatusCode": fx.get("http_status")},
    }
    return ClientError(response, "Converse")


@pytest.mark.parametrize("fname", _BEDROCK_FIXTURE_FILES)
def test_classify_on_bedrock_fixture(fname):
    pytest.importorskip("botocore")
    import json as _json
    fx = _json.loads((_BEDROCK_FIXTURES / fname).read_text())
    out = BedrockLLM(model="m")._translate_error(_client_error_from_fixture(fx))
    assert type(out) is _LOCAL_ERROR_TYPES[fx["expected_chia"]]
    assert out.status_code == fx["code"]  # the AWS error Code is carried through


def test_bedrock_real_fixtures_pin_assumptions():
    import json as _json
    # Unknown model is ValidationException (NOT ResourceNotFoundException).
    unk = _json.loads((_BEDROCK_FIXTURES / "bedrock_validation_unknown_model.json").read_text())
    assert unk["code"] == "ValidationException"
    # Over-context rides on a ValidationException with an "Input Tokens Exceeded"
    # message — currently classified InvalidRequestError (no length-specific
    # disambiguation, unlike openai_compat's ContextLengthExceededError).
    over = _json.loads((_BEDROCK_FIXTURES / "bedrock_validation_over_context.json").read_text())
    assert over["code"] == "ValidationException"
    assert "Input Tokens Exceeded" in over["message"]
    assert over["expected_chia"] == "InvalidRequestError"


# ---------------------------------------------------------------------------
# Live tests (real Bedrock; need BEDROCK_TEST_MODEL + AWS creds/region)
# ---------------------------------------------------------------------------

live = pytest.mark.skipif(
    not os.environ.get("BEDROCK_TEST_MODEL"),
    reason="BEDROCK_TEST_MODEL not set",
)


@live
def test_live_converse_simple_prompt():
    pytest.importorskip("boto3")
    llm = BedrockLLM(
        model=os.environ["BEDROCK_TEST_MODEL"],
        system_message="You answer with a single word and nothing else.",
        max_tokens=64,
    )
    cli = llm.prompt("Reply with exactly the word: PONG", tools=[])
    assert cli.success is True
    assert "PONG" in cli.result.upper()
    assert llm._last_metadata.get("output_tokens", 0) > 0


@pytest.fixture
def local_bash_tool():
    """Spin up a real BashTool MCP server on the local Ray instance."""
    pytest.importorskip("boto3")
    if not os.environ.get("BEDROCK_TEST_MODEL"):
        pytest.skip("BEDROCK_TEST_MODEL not set")

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
def test_live_converse_with_bash_tool(local_bash_tool):
    """End-to-end: Converse connects to a live MCP server, the model calls the
    bash tool, and the result flows back. Requires a tool-capable model in
    BEDROCK_TEST_MODEL (e.g. a Claude, Nova, or Llama 3.1+ model)."""
    llm = BedrockLLM(
        model=os.environ["BEDROCK_TEST_MODEL"],
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
# Live tests: the full stream_result conversation log
#
# Bedrock's stream_result is the Converse transcript: banner "Converse (model)",
# the echoed [User Message], per-turn [Response] / [Thinking] / [Tool Call:
# name] + Args: / [Tool Result] sections, and a trailing rule. These assert the
# whole structure (presence + ordering) against real Bedrock, not just that a
# couple of markers appear. Same BEDROCK_TEST_MODEL gate as the others.
# ---------------------------------------------------------------------------

_BANNER_RULE = "=" * 80
_TRAILING_RULE = "-" * 80
_TOOL_PROMPT = (
    "Run the shell command:  echo CHIA_TOOL_OK\n"
    "Then reply with exactly the line it printed."
)
_TOOL_SYSTEM = (
    "You have a bash tool available. To answer the user you MUST run the "
    "requested shell command with that tool and report its output verbatim. "
    "Never guess the output."
)


def _assert_log_banner(stream: str, model: str) -> None:
    assert stream, "stream_result is empty"
    assert _BANNER_RULE in stream, "missing the '=' banner rule"
    assert f"Converse ({model})" in stream, "banner missing the 'Converse (model)' header"
    assert "[User Message]" in stream, "missing the [User Message] section"
    assert _TRAILING_RULE in stream, "missing the trailing '-' rule"


@live
def test_live_converse_stream_log_simple():
    pytest.importorskip("boto3")
    prompt = "Reply with exactly the word: PONG"
    model = os.environ["BEDROCK_TEST_MODEL"]
    llm = BedrockLLM(model=model, system_message="You answer with a single word and nothing else.", max_tokens=64)
    stream = llm.prompt(prompt, tools=[]).stream_result

    _assert_log_banner(stream, model)
    assert prompt in stream, "the user message is not echoed into the log"
    assert "[Response]" in stream
    assert "[Tool Call:" not in stream
    assert "[Tool Result" not in stream
    assert stream.index("[User Message]") < stream.index("[Response]")


@live
def test_live_converse_stream_log_tool_conversation(local_bash_tool):
    model = os.environ["BEDROCK_TEST_MODEL"]
    llm = BedrockLLM(model=model, system_message=_TOOL_SYSTEM, max_tokens=2048)
    stream = llm.prompt(_TOOL_PROMPT, tools=[local_bash_tool]).stream_result

    _assert_log_banner(stream, model)
    assert f"[Tool Call: {local_bash_tool.name}" in stream, (
        "missing/misnamed [Tool Call:] section\n" + stream
    )
    assert "Args:" in stream, "tool call missing its Args: line\n" + stream
    assert "[Tool Result]" in stream, "missing [Tool Result] section\n" + stream
    assert "[Response]" in stream

    user_idx = stream.index("[User Message]")
    call_idx = stream.index("[Tool Call:")
    result_idx = stream.index("[Tool Result]")
    final_response_idx = stream.rindex("[Response]")

    # Conversation order: user -> tool call -> tool result -> final answer.
    assert user_idx < call_idx < result_idx, "log sections out of order\n" + stream
    assert final_response_idx > result_idx, (
        "the final [Response] should follow the [Tool Result]\n" + stream
    )
    assert "CHIA_TOOL_OK" in stream[result_idx:], (
        "tool-result section missing the echoed sentinel\n" + stream
    )
