"""Tests for :class:`chia.models.vertex.VertexGeminiLLM` (Gemini-on-Vertex backend).

Run from the repo root with the chia venv active::

    pytest chia/models/tests/test_vertex.py -v

Three layers, matching test_bedrock.py / test_claude_api.py:

* **Offline unit tests** — construction, schema sanitising, helpers, warning.
* **Mocked-loop tests** — monkeypatch ``genai.Client`` to a fake (real
  ``types`` objects, fake MCP transport) so the full Gemini agent loop runs
  offline: request shaping, the function_call -> function_response loop, and
  metadata accumulation.
* **Live tests** — skipped unless ``GOOGLE_CLOUD_PROJECT`` is set (and, for the
  tool tests, Ray can start). They actually call Vertex, so they need
  Application Default Credentials. The Gemini model defaults to
  ``gemini-2.5-flash`` (override with ``VERTEX_TEST_MODEL``); the MaaS model
  and region have their own defaults (override with ``VERTEX_MAAS_TEST_MODEL`` /
  ``VERTEX_MAAS_TEST_LOCATION``). Both the Gemini and MaaS live tests gate on
  ``GOOGLE_CLOUD_PROJECT``, so select with ``-k "live and not generic"`` (Gemini)
  or ``-k generic`` (MaaS) to run one set, e.g.::

      GOOGLE_CLOUD_PROJECT=my-proj \
          pytest chia/models/tests/test_vertex.py -k "live and not generic" -v
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from google.genai import types

from chia.models.openai_compat import OpenAICompatLLM
from chia.models.vertex import (
    AuthenticationError,
    ContentBlockedError,
    InvalidRequestError,
    MaxOutputTokensError,
    RateLimitError,
    ServerError,
    UnknownVertexError,
    VertexGeminiLLM,
    VertexGenericLLM,
)


# ---------------------------------------------------------------------------
# Fakes — a fake genai.Client (real types) + fake MCP transport
# ---------------------------------------------------------------------------


def _text_part(text):
    return types.Part(text=text)


def _fc_part(name, args):
    return types.Part(function_call=types.FunctionCall(name=name, args=args))


def _resp(parts, finish="STOP", in_tok=0, out_tok=0):
    return types.GenerateContentResponse(
        candidates=[types.Candidate(
            content=types.Content(role="model", parts=parts),
            finish_reason=getattr(types.FinishReason, finish),
        )],
        usage_metadata=types.GenerateContentResponseUsageMetadata(
            prompt_token_count=in_tok,
            candidates_token_count=out_tok,
            total_token_count=in_tok + out_tok,
        ),
    )


def _install_fake_genai(monkeypatch, responses, capture):
    """Patch ``genai.Client`` so the loop talks to a fake whose
    ``models.generate_content`` returns ``responses`` in order."""
    from google import genai

    class _FakeModels:
        def generate_content(self, *, model, contents, config):
            capture["calls"].append({
                "model": model,
                "contents": list(contents),  # snapshot; loop mutates in place
                "config": config,
            })
            return responses.pop(0)

    class _FakeClient:
        def __init__(self, **kwargs):
            capture["client_kwargs"] = kwargs
            self.models = _FakeModels()

    monkeypatch.setattr(genai, "Client", lambda **kwargs: _FakeClient(**kwargs))


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


def test_constructor_basics(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "proj-123")
    monkeypatch.delenv("GOOGLE_CLOUD_LOCATION", raising=False)
    llm = VertexGeminiLLM(model="gemini-2.0-flash-001")
    assert llm.model == "gemini-2.0-flash-001"
    assert llm.project == "proj-123"
    assert llm.location == "us-central1"  # default
    assert llm.max_tokens == 16000


def test_explicit_location_overrides_env(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLOUD_LOCATION", "us-central1")
    llm = VertexGeminiLLM(model="m", location="europe-west4")
    assert llm.location == "europe-west4"


def test_experimental_warning(caplog):
    with caplog.at_level("WARNING", logger="vertex_gemini"):
        VertexGeminiLLM(model="m")
    assert "experimental" in caplog.text


def test_sanitize_schema_drops_unsupported_keys():
    schema = {
        "type": "object",
        "title": "Foo",
        "additionalProperties": False,
        "$schema": "http://json-schema.org/draft-07/schema#",
        "properties": {
            "x": {"type": "integer", "title": "X"},
        },
    }
    out = VertexGeminiLLM._sanitize_schema(schema)
    assert "title" not in out
    assert "additionalProperties" not in out
    assert "$schema" not in out
    assert "title" not in out["properties"]["x"]
    assert out["properties"]["x"]["type"] == "integer"


def test_mcp_result_to_text_joins_text_blocks():
    result = SimpleNamespace(content=[
        SimpleNamespace(text="hello"),
        SimpleNamespace(text="world"),
    ])
    assert VertexGeminiLLM._mcp_result_to_text(result) == "hello\nworld"


def test_mcp_result_to_text_empty():
    assert VertexGeminiLLM._mcp_result_to_text(SimpleNamespace(content=None)) == ""


# ---------------------------------------------------------------------------
# VertexGenericLLM (MaaS / OpenAI-compat) — construction only
# ---------------------------------------------------------------------------


def test_generic_builds_maas_base_url():
    llm = VertexGenericLLM(
        model="meta/llama-3.1-8b-instruct-maas",
        project="proj-123", location="us-central1",
    )
    assert isinstance(llm, OpenAICompatLLM)
    assert llm.base_url == (
        "https://us-central1-aiplatform.googleapis.com/v1beta1/"
        "projects/proj-123/locations/us-central1/endpoints/openapi"
    )
    assert callable(llm.token_provider)
    # User ADC needs the quota-project header; we set it from the project.
    assert llm.client_kwargs["default_headers"]["x-goog-user-project"] == "proj-123"


def test_generic_location_defaults(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "p")
    monkeypatch.delenv("GOOGLE_CLOUD_LOCATION", raising=False)
    llm = VertexGenericLLM(model="m")
    assert "us-central1-aiplatform.googleapis.com" in llm.base_url
    assert "projects/p/" in llm.base_url


# ---------------------------------------------------------------------------
# Mocked-loop tests (fake genai.Client; no network, no GCP creds required)
# ---------------------------------------------------------------------------


def test_generate_no_tools_request_shaping_and_result(monkeypatch):
    capture = {"calls": []}
    _install_fake_genai(
        monkeypatch,
        [_resp([_text_part("PONG")], "STOP", in_tok=10, out_tok=5)],
        capture,
    )

    llm = VertexGeminiLLM(model="gemini-2.0-flash-001", system_message="be terse",
                    project="proj-123", location="us-central1", max_tokens=1234)
    cli = llm.prompt("ping", tools=[])

    assert cli.success is True
    assert cli.result == "PONG"
    assert cli.returncode == 0

    # Client constructed for Vertex with project/location.
    ck = capture["client_kwargs"]
    assert ck["vertexai"] is True
    assert ck["project"] == "proj-123"
    assert ck["location"] == "us-central1"

    assert len(capture["calls"]) == 1
    call = capture["calls"][0]
    assert call["model"] == "gemini-2.0-flash-001"
    assert call["config"].max_output_tokens == 1234
    assert call["config"].tools is None  # none passed

    assert llm._last_metadata["input_tokens"] == 10
    assert llm._last_metadata["output_tokens"] == 5
    assert llm._last_metadata["num_turns"] == 1
    assert llm._last_metadata["model"] == "gemini-2.0-flash-001"


def test_generate_tool_loop_executes_mcp_and_feeds_results(monkeypatch):
    capture = {"calls": [], "urls": [], "tool_calls": []}
    _install_fake_genai(
        monkeypatch,
        [
            _resp([_fc_part("calc__run", {"x": 21})], "STOP", in_tok=10, out_tok=5),
            _resp([_text_part("the answer is 42")], "STOP", in_tok=3, out_tok=4),
        ],
        capture,
    )
    _install_fake_mcp(monkeypatch, capture, tool_result_text="42")

    tool = SimpleNamespace(name="calc", hostname="localhost", port=9001)
    llm = VertexGeminiLLM(model="gemini-2.0-flash-001", project="p", location="us-central1")
    cli = llm.prompt("what is 21 doubled?", tools=[tool])

    assert cli.success is True
    assert cli.result == "the answer is 42"
    assert capture["urls"] == ["http://localhost:9001/calc/mcp"]
    assert capture["tool_calls"] == [("run", {"x": 21})]
    assert len(capture["calls"]) == 2

    # First request advertised the namespaced function declaration.
    decls = capture["calls"][0]["config"].tools[0].function_declarations
    assert decls[0].name == "calc__run"

    # Second request carried the function_response back to the model.
    last_content = capture["calls"][1]["contents"][-1]
    assert last_content.role == "user"
    fr = last_content.parts[0].function_response
    assert fr.name == "calc__run"
    assert fr.response == {"result": "42"}

    assert llm._last_metadata["num_turns"] == 2
    assert llm._last_metadata["input_tokens"] == 13


def test_generate_unknown_tool_reports_error(monkeypatch):
    capture = {"calls": [], "urls": [], "tool_calls": []}
    _install_fake_genai(
        monkeypatch,
        [
            _resp([_fc_part("calc__missing", {})], "STOP"),
            _resp([_text_part("done")], "STOP"),
        ],
        capture,
    )
    _install_fake_mcp(monkeypatch, capture)

    tool = SimpleNamespace(name="calc", hostname="localhost", port=9001)
    llm = VertexGeminiLLM(model="m", project="p")
    cli = llm.prompt("go", tools=[tool])

    assert cli.success is True
    assert capture["tool_calls"] == []  # real tool never called
    fr = capture["calls"][1]["contents"][-1].parts[0].function_response
    assert "error" in fr.response
    assert "Unknown tool" in fr.response["error"]


def test_generate_max_tokens_raises(monkeypatch):
    capture = {"calls": []}
    _install_fake_genai(
        monkeypatch,
        [
            _resp([_text_part("partial")], "MAX_TOKENS"),
            _resp([_text_part("partial again")], "MAX_TOKENS"),
        ],
        capture,
    )
    llm = VertexGeminiLLM(model="m", project="p", retries=2)
    with pytest.raises(MaxOutputTokensError):
        llm.prompt("go", tools=[])
    assert len(capture["calls"]) == 2  # original + one retry


# ---------------------------------------------------------------------------
# Content / prompt blocking (Gemini "soft" failures — 200-OK, no API error)
#
# Gemini reports safety/recitation/etc. blocks as a normal 200 response (a
# candidate with a blocking finish_reason, or no candidates + a
# prompt_feedback.block_reason). Without handling these the loop returns an
# empty success — a silent failure. They must surface as ContentBlockedError.
# ---------------------------------------------------------------------------


def _prompt_block_resp(reason="SAFETY"):
    """A response with no candidates and a prompt-level block_reason."""
    return types.GenerateContentResponse(
        candidates=[],
        prompt_feedback=types.GenerateContentResponsePromptFeedback(
            block_reason=getattr(types.BlockedReason, reason)
        ),
    )


@pytest.mark.parametrize("reason", ["SAFETY", "RECITATION", "BLOCKLIST", "PROHIBITED_CONTENT"])
def test_content_block_finish_reason_raises(monkeypatch, reason):
    capture = {"calls": []}
    # Candidate with a blocking finish_reason and no usable parts.
    _install_fake_genai(monkeypatch, [_resp([], finish=reason)], capture)
    llm = VertexGeminiLLM(model="m")
    with pytest.raises(ContentBlockedError) as exc:
        llm.prompt("say something disallowed", tools=[])
    assert exc.value.block_reason == reason
    # Never-retry: only one model call, no retry storm.
    assert len(capture["calls"]) == 1


def test_prompt_block_raises(monkeypatch):
    capture = {"calls": []}
    _install_fake_genai(monkeypatch, [_prompt_block_resp("SAFETY")], capture)
    llm = VertexGeminiLLM(model="m")
    with pytest.raises(ContentBlockedError) as exc:
        llm.prompt("disallowed prompt", tools=[])
    assert exc.value.block_reason == "SAFETY"
    assert len(capture["calls"]) == 1


def test_normal_empty_stop_is_not_blocked(monkeypatch):
    # An ordinary STOP finish with no text must NOT be mistaken for a block;
    # it returns an (empty) success like before.
    capture = {"calls": []}
    _install_fake_genai(monkeypatch, [_resp([], finish="STOP")], capture)
    cli = VertexGeminiLLM(model="m").prompt("hi", tools=[])
    assert cli.success is True
    assert cli.result == ""


def test_content_blocked_error_is_picklable():
    import pickle
    e = ContentBlockedError("node-7", block_reason="SAFETY", raw_message="response blocked: SAFETY")
    r = pickle.loads(pickle.dumps(e))
    assert type(r) is ContentBlockedError
    assert r.block_reason == "SAFETY" and r.node_id == "node-7"


def test_content_blocked_survives_exception_group():
    # In the tools path the block is raised inside the AsyncExitStack and gets
    # wrapped by MCP's task group; _translate_error must pass it through.
    typed = ContentBlockedError("node-x", block_reason="RECITATION", raw_message="blocked")
    grp = _ExcGroup("g", [_ExcGroup("g2", [typed])])
    assert VertexGeminiLLM(model="m")._translate_error(grp) is typed


# ---------------------------------------------------------------------------
# Error translation (google-genai is a dependency, so these run)
# ---------------------------------------------------------------------------


def _api_error(code):
    from google.genai import errors
    cls = errors.ServerError if code >= 500 else errors.ClientError
    return cls(code, {"error": {"code": code, "status": "S", "message": "boom"}})


def test_translate_non_genai_returns_none():
    llm = VertexGeminiLLM(model="m")
    assert llm._translate_error(ValueError("nope")) is None


def test_translate_429_to_rate_limit():
    llm = VertexGeminiLLM(model="m")
    t = llm._translate_error(_api_error(429))
    assert isinstance(t, RateLimitError)
    assert t.reset_time > datetime.now(timezone.utc)


def test_translate_403_to_auth():
    llm = VertexGeminiLLM(model="m")
    assert isinstance(llm._translate_error(_api_error(403)), AuthenticationError)


def test_translate_400_to_invalid_request():
    llm = VertexGeminiLLM(model="m")
    assert isinstance(llm._translate_error(_api_error(400)), InvalidRequestError)


def test_translate_500_to_server_error():
    llm = VertexGeminiLLM(model="m")
    assert isinstance(llm._translate_error(_api_error(500)), ServerError)


def test_translate_unknown_code():
    llm = VertexGeminiLLM(model="m")
    assert isinstance(llm._translate_error(_api_error(418)), UnknownVertexError)


# ---------------------------------------------------------------------------
# ExceptionGroup unwrapping (the tools-path wrapping bug)
#
# When MCP tools are connected, a google-genai error propagates out of the
# AsyncExitStack and anyio's task group re-wraps it in an ExceptionGroup (the
# `exceptiongroup` backport on Python < 3.11). Without unwrapping, the typed
# error never matches prompt()'s except clauses and is misfiled as "unexpected"
# and retried. Confirmed via a real BashTool + genai.Client stub.
# ---------------------------------------------------------------------------

try:  # py<3.11 backport (an anyio dependency); builtin on 3.11+
    from exceptiongroup import ExceptionGroup as _ExcGroup
except ImportError:  # pragma: no cover
    _ExcGroup = ExceptionGroup


def test_translate_unwraps_exception_group():
    llm = VertexGeminiLLM(model="m")
    grp = _ExcGroup("unhandled errors in a TaskGroup", [_api_error(429)])
    t = llm._translate_error(grp)
    assert isinstance(t, RateLimitError)


def test_translate_unwraps_nested_exception_group():
    llm = VertexGeminiLLM(model="m")
    inner = _ExcGroup("inner", [_api_error(403)])
    outer = _ExcGroup("outer", [inner])  # MCP can nest groups two deep
    assert isinstance(llm._translate_error(outer), AuthenticationError)


def test_translate_passes_through_wrapped_typed_error():
    # A typed error translated inside the loop and then re-wrapped by the task
    # group must survive as the SAME object, not be re-derived.
    typed = AuthenticationError("node-x", status_code=403, raw_message="orig")
    grp = _ExcGroup("g", [_ExcGroup("g2", [typed])])
    assert VertexGeminiLLM(model="m")._translate_error(grp) is typed


# ---------------------------------------------------------------------------
# Fixture-driven classification (real + synthetic google-genai error shapes)
#
# Real captures pinned from a live Gemini run (gemini-2.5-flash, us-central1,
# 2026-06-01) under fixtures/vertex/; 401/403/429/5xx are synthetic (can't be
# triggered cheaply here). Each rebuilds a real genai APIError from the captured
# {code, status, message} and asserts _translate_error classifies it as
# documented. NOTE this covers the non-generic (Gemini) backend; VertexGenericLLM
# (MaaS) is an OpenAICompatLLM subclass, covered by fixtures/openai_compat/.
# ---------------------------------------------------------------------------

import pathlib  # noqa: E402

_VERTEX_FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "vertex"

_LOCAL_ERROR_TYPES = {
    "RateLimitError": RateLimitError,
    "AuthenticationError": AuthenticationError,
    "InvalidRequestError": InvalidRequestError,
    "ServerError": ServerError,
    "UnknownVertexError": UnknownVertexError,
}

_VERTEX_FIXTURE_FILES = [
    "vertex_not_found_unknown_model.json",
    "vertex_invalid_argument.json",
    "vertex_resource_exhausted_429.json",
    "vertex_permission_denied_403.json",
    "vertex_unauthenticated_401.json",
    "vertex_internal_500.json",
    "vertex_unavailable_503.json",
]


def _genai_error_from_fixture(fx):
    from google.genai import errors
    code = fx["code"]
    cls = errors.ServerError if code >= 500 else errors.ClientError
    return cls(code, {"error": {"code": code, "status": fx.get("status", ""),
                                "message": fx.get("message", "")}})


@pytest.mark.parametrize("fname", _VERTEX_FIXTURE_FILES)
def test_classify_on_vertex_fixture(fname):
    import json as _json
    fx = _json.loads((_VERTEX_FIXTURES / fname).read_text())
    out = VertexGeminiLLM(model="m")._translate_error(_genai_error_from_fixture(fx))
    assert type(out) is _LOCAL_ERROR_TYPES[fx["expected_chia"]]
    assert out.status_code == fx["code"]


def test_vertex_real_fixture_pins_unknown_model_is_404():
    import json as _json
    # Vertex Gemini returns 404 NOT_FOUND for an unknown model (Bedrock and
    # OpenRouter both use 400) — still InvalidRequestError since we map both.
    fx = _json.loads((_VERTEX_FIXTURES / "vertex_not_found_unknown_model.json").read_text())
    assert fx["code"] == 404 and fx["status"] == "NOT_FOUND"
    assert fx["expected_chia"] == "InvalidRequestError"


# ---------------------------------------------------------------------------
# Live tests (real Vertex; need VERTEX_TEST_MODEL + GOOGLE_CLOUD_PROJECT + ADC)
# ---------------------------------------------------------------------------

# Gated only on GOOGLE_CLOUD_PROJECT; the model has a default so VERTEX_TEST_MODEL
# is an optional override, not required.
live = pytest.mark.skipif(
    not os.environ.get("GOOGLE_CLOUD_PROJECT"),
    reason="GOOGLE_CLOUD_PROJECT not set",
)

_VERTEX_GEMINI_DEFAULT_MODEL = "gemini-2.5-flash"


@live
def test_live_generate_simple_prompt():
    llm = VertexGeminiLLM(
        model=os.environ.get("VERTEX_TEST_MODEL", _VERTEX_GEMINI_DEFAULT_MODEL),
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
    if not os.environ.get("GOOGLE_CLOUD_PROJECT"):
        pytest.skip("GOOGLE_CLOUD_PROJECT not set")

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
def test_live_generate_with_bash_tool(local_bash_tool):
    """End-to-end: Gemini connects to a live MCP server, calls the bash tool,
    and the result flows back into the final answer."""
    llm = VertexGeminiLLM(
        model=os.environ.get("VERTEX_TEST_MODEL", _VERTEX_GEMINI_DEFAULT_MODEL),
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
# Live tests for VertexGenericLLM (Vertex MaaS / OpenAI-compatible path)
#
# These exercise the open/partner families (Llama, ...) reached through the
# Vertex Model-as-a-Service OpenAI-compatible endpoint, NOT Gemini. The project
# name is the input: set ``GOOGLE_CLOUD_PROJECT`` and it is passed explicitly to
# ``VertexGenericLLM(project=...)``. Auth is a rotating GCP ADC bearer token
# (``gcloud auth application-default login`` / service account / workload
# identity), minted by the class's ``token_provider`` — no api_key is passed.
#
# Vertex MaaS has NO free tier, so the default is the cheapest tool-capable MaaS
# model (Llama 3.1 8B Instruct). Override with ``VERTEX_MAAS_TEST_MODEL``. The
# model must be enabled in the project's Model Garden and offered in the
# location (default us-central1). Needs ``openai`` installed.
#
# Gated only on ``GOOGLE_CLOUD_PROJECT`` (distinct from the Gemini ``live``
# marker above, which also needs ``VERTEX_TEST_MODEL``), so select with
# ``-k generic`` to run just these and avoid paid calls you didn't intend.
# ---------------------------------------------------------------------------

generic_live = pytest.mark.skipif(
    not os.environ.get("GOOGLE_CLOUD_PROJECT"),
    reason="GOOGLE_CLOUD_PROJECT not set",
)

# Cheapest tool-capable MaaS model confirmed available in-project (Llama 4 Scout,
# 17B active via MoE). Llama 3.3 70B (meta/llama-3.3-70b-instruct-maas) is the
# more battle-tested tool caller if Scout's function-calling proves flaky; the
# Llama 3.1 family returns 403 (present but not enabled for the project).
_VERTEX_MAAS_DEFAULT_MODEL = "meta/llama-4-scout-17b-16e-instruct-maas"

# Llama MaaS is region-specific; us-east5 is where Llama 4 is served (not the
# us-central1 default that VertexGenericLLM falls back to). Passed explicitly so
# it isn't shadowed by a GOOGLE_CLOUD_LOCATION set for the Gemini tests; override
# with VERTEX_MAAS_TEST_LOCATION.
_VERTEX_MAAS_DEFAULT_LOCATION = os.environ.get("VERTEX_MAAS_TEST_LOCATION", "us-east5")


@generic_live
def test_live_vertex_adc_token_provider():
    """Directly exercise the ADC token minter used by VertexGenericLLM.

    Needs Application Default Credentials available wherever this runs
    (``gcloud auth application-default login`` / service account / WI). A
    user-ADC token looks like ``ya29.…``; a service-account token is a JWT.
    """
    from chia.models.vertex import _vertex_adc_token_provider

    token = _vertex_adc_token_provider()
    assert isinstance(token, str) and len(token) > 20


@generic_live
def test_live_generic_simple_prompt():
    pytest.importorskip("openai")
    llm = VertexGenericLLM(
        model=os.environ.get("VERTEX_MAAS_TEST_MODEL", _VERTEX_MAAS_DEFAULT_MODEL),
        project=os.environ["GOOGLE_CLOUD_PROJECT"],
        location=_VERTEX_MAAS_DEFAULT_LOCATION,
        system_message="You answer with a single word and nothing else.",
        max_tokens=64,
    )
    cli = llm.prompt("Reply with exactly the word: PONG", tools=[])
    assert cli.success is True
    assert "PONG" in cli.result.upper()
    assert llm._last_metadata.get("output_tokens", 0) > 0


@pytest.fixture
def maas_bash_tool():
    """Spin up a real BashTool MCP server on the local Ray instance (MaaS path)."""
    pytest.importorskip("openai")
    if not os.environ.get("GOOGLE_CLOUD_PROJECT"):
        pytest.skip("GOOGLE_CLOUD_PROJECT not set")

    import uuid

    import ray

    from chia.base.tools.BashTool import BashTool

    ray.init(ignore_reinit_error=True)
    tool = BashTool(name=f"echo_{uuid.uuid4().hex[:8]}", work_dir="/tmp")
    try:
        yield tool
    finally:
        tool.stop()


@generic_live
def test_live_generic_with_bash_tool(maas_bash_tool):
    """End-to-end through Vertex MaaS: the model connects to a live MCP server,
    calls the bash tool, and the result flows back into the final answer."""
    llm = VertexGenericLLM(
        model=os.environ.get("VERTEX_MAAS_TEST_MODEL", _VERTEX_MAAS_DEFAULT_MODEL),
        project=os.environ["GOOGLE_CLOUD_PROJECT"],
        location=_VERTEX_MAAS_DEFAULT_LOCATION,
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
        tools=[maas_bash_tool],
    )

    assert cli.success is True
    assert "CHIA_TOOL_OK" in cli.result
    assert llm._last_metadata.get("num_turns", 0) >= 2
    assert "Tool Call" in cli.stream_result
    assert "Tool Result" in cli.stream_result


# ---------------------------------------------------------------------------
# Live tests: the full stream_result conversation log (both Vertex paths)
#
# stream_result is the human-readable transcript both Vertex backends build for
# every prompt: a banner with the model, the echoed [User Message], then per-turn
# [Response] / [Tool Call: ...] / [Tool Result] sections (Gemini may also emit
# [Thinking]), and a trailing rule. The banner VERB differs by path:
# VertexGeminiLLM (google-genai) writes "generate_content (model)", while
# VertexGenericLLM (OpenAI-compat MaaS) writes "chat.completions (model)". These
# assert the whole structure (presence + ordering) against the real APIs. Select
# Gemini with `-k "stream_log and not generic"`, MaaS with
# `-k "stream_log and generic"`.
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


def _assert_log_banner(stream: str, model: str, verb: str) -> None:
    assert stream, "stream_result is empty"
    assert _BANNER_RULE in stream, "missing the '=' banner rule"
    assert f"{verb} ({model})" in stream, f"banner missing the '{verb} ({model})' header"
    assert "[User Message]" in stream, "missing the [User Message] section"
    assert _TRAILING_RULE in stream, "missing the trailing '-' rule"


def _assert_simple_log(stream: str, model: str, verb: str, prompt: str) -> None:
    _assert_log_banner(stream, model, verb)
    assert prompt in stream, "the user message is not echoed into the log"
    assert "[Response]" in stream
    assert "[Tool Call:" not in stream
    assert "[Tool Result" not in stream
    assert stream.index("[User Message]") < stream.index("[Response]")


def _assert_tool_conversation_log(stream: str, tool_name: str) -> None:
    assert f"[Tool Call: {tool_name}" in stream, (
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


@live
def test_live_generate_stream_log_simple():
    prompt = "Reply with exactly the word: PONG"
    model = os.environ.get("VERTEX_TEST_MODEL", _VERTEX_GEMINI_DEFAULT_MODEL)
    llm = VertexGeminiLLM(model=model, system_message="You answer with a single word and nothing else.", max_tokens=64)
    _assert_simple_log(llm.prompt(prompt, tools=[]).stream_result, model, "generate_content", prompt)


@live
def test_live_generate_stream_log_tool_conversation(local_bash_tool):
    model = os.environ.get("VERTEX_TEST_MODEL", _VERTEX_GEMINI_DEFAULT_MODEL)
    llm = VertexGeminiLLM(model=model, system_message=_TOOL_SYSTEM, max_tokens=2048)
    stream = llm.prompt(_TOOL_PROMPT, tools=[local_bash_tool]).stream_result
    _assert_log_banner(stream, model, "generate_content")
    _assert_tool_conversation_log(stream, local_bash_tool.name)


@generic_live
def test_live_generic_stream_log_simple():
    pytest.importorskip("openai")
    prompt = "Reply with exactly the word: PONG"
    model = os.environ.get("VERTEX_MAAS_TEST_MODEL", _VERTEX_MAAS_DEFAULT_MODEL)
    llm = VertexGenericLLM(
        model=model,
        project=os.environ["GOOGLE_CLOUD_PROJECT"],
        location=_VERTEX_MAAS_DEFAULT_LOCATION,
        system_message="You answer with a single word and nothing else.",
        max_tokens=64,
    )
    _assert_simple_log(llm.prompt(prompt, tools=[]).stream_result, model, "chat.completions", prompt)


@generic_live
def test_live_generic_stream_log_tool_conversation(maas_bash_tool):
    model = os.environ.get("VERTEX_MAAS_TEST_MODEL", _VERTEX_MAAS_DEFAULT_MODEL)
    llm = VertexGenericLLM(
        model=model,
        project=os.environ["GOOGLE_CLOUD_PROJECT"],
        location=_VERTEX_MAAS_DEFAULT_LOCATION,
        system_message=_TOOL_SYSTEM,
        max_tokens=2048,
    )
    stream = llm.prompt(_TOOL_PROMPT, tools=[maas_bash_tool]).stream_result
    _assert_log_banner(stream, model, "chat.completions")
    _assert_tool_conversation_log(stream, maas_bash_tool.name)
