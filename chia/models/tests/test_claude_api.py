"""Tests for :class:`ClaudeCodeLLM`.

Run from the repo root with the chia venv active::

    # Offline only (default — live tests skip without their env vars):
    pytest chia/models/tests/test_claude_api.py -v

    # Live API tests (billable; hit the real Anthropic API):
    ANTHROPIC_API_KEY=sk-... pytest chia/models/tests/test_claude_api.py -v

    # Live cluster tests (need a running Ray cluster). RAY_ADDRESS points at
    # the head; -s shows the printed node id / hostname output. e.g.:
    CHIA_LIVE_CLUSTER=1 RAY_ADDRESS=IP:6379 \
        pytest chia/models/tests/test_claude_api.py -k simple_call -s
    CHIA_LIVE_CLUSTER=1 RAY_ADDRESS=IP:6379 \
        pytest chia/models/tests/test_claude_api.py -k across_machines -s

The cluster tests ship this checkout's ``chia`` package to the workers via
``runtime_env={"py_modules": [...]}`` so they can import the code under test
even if the worker image predates it. Override the shipped path with
``CHIA_SHIP_PY_MODULES=/path/to/chia``, or set it to ``""`` to skip shipping
when the cluster already has the code deployed.

Layers of tests:

* **Offline unit tests** — construction, validation, helpers, and that
  :meth:`ClaudeCodeLLM.prompt` routes to the right backend. No network,
  no ``anthropic`` install needed.
* **Mocked-loop tests** — inject a fake ``anthropic`` module (and fake MCP
  transport) so the full ``backend="api"`` agent loop runs offline: request
  shaping, prompt-cache breakpoints, the tool_use -> tool_result loop, and
  metadata accumulation.
* **Session-transcript tests** — the CLI backend's ``resume_session`` carry:
  the ``projects_cwd`` resolution, ``_capture``/``_restore``/``_sync_transcript``
  helpers, and that ``prompt.chia_remote`` returns an ``ObjectRefCallback``
  (so ``get`` auto-syncs) when resuming. Offline (no cluster).
* **Live tests** — skipped unless ``ANTHROPIC_API_KEY`` is set (API tests) or
  ``CHIA_LIVE_CLUSTER=1`` (the cross-machine resume test). These hit the real
  API / a running Ray cluster. The tools-backed live API test additionally
  stands up a real :class:`BashTool` MCP server on the local Ray instance.
"""

from __future__ import annotations

import asyncio
import os
import sys
import types
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from chia.models.claude import (
    AuthenticationError,
    BillingError,
    ClaudeCodeLLM,
    ClaudeCodeQueryResult,
    InvalidRequestError,
    QueryResult,
    RateLimitError,
    ServerError,
)

from chia.base.ChiaFunction import ChiaFunction, ObjectRefCallback
import time

# Cheapest current Claude model — used for the live (billable) tests. Haiku
# 4.5 doesn't support adaptive thinking / effort, but the live tests run with
# thinking disabled, so no extra params are sent.
CHEAP_MODEL = "claude-haiku-4-5"


# ---------------------------------------------------------------------------
# Fakes — a stand-in ``anthropic`` module and MCP transport for offline tests
# ---------------------------------------------------------------------------


def _text_block(text):
    return SimpleNamespace(type="text", text=text)


def _thinking_block(text):
    return SimpleNamespace(type="thinking", thinking=text)


def _tool_use_block(block_id, name, tool_input):
    return SimpleNamespace(type="tool_use", id=block_id, name=name, input=tool_input)


def _usage(inp=0, out=0, cc=0, cr=0):
    return SimpleNamespace(
        input_tokens=inp,
        output_tokens=out,
        cache_creation_input_tokens=cc,
        cache_read_input_tokens=cr,
    )


def _response(content, stop_reason, usage=None):
    return SimpleNamespace(
        content=content, stop_reason=stop_reason, usage=usage or _usage()
    )


def _install_fake_anthropic(monkeypatch, responses, capture):
    """Inject a fake ``anthropic`` module whose AsyncAnthropic returns
    ``responses`` (one per ``messages.create`` call), recording every call
    payload into ``capture``."""

    mod = types.ModuleType("anthropic")

    class _FakeMessages:
        async def create(self, **kwargs):
            # Snapshot the messages list — the agent loop mutates it in place
            # after this call returns, so storing the live reference would let
            # later turns leak into earlier captured payloads.
            snapshot = dict(kwargs)
            if "messages" in snapshot:
                snapshot["messages"] = list(snapshot["messages"])
            capture["calls"].append(snapshot)
            return responses.pop(0)

    class _FakeAsyncAnthropic:
        def __init__(self, api_key=None):
            capture["api_key"] = api_key
            self.messages = _FakeMessages()

    mod.AsyncAnthropic = _FakeAsyncAnthropic
    monkeypatch.setitem(sys.modules, "anthropic", mod)
    return mod


def _install_fake_mcp(monkeypatch, capture, tool_result_text="42"):
    """Patch the MCP transport + session that ``_run_api_async`` imports so a
    single fake server advertises one tool (``run``) and echoes a result."""

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
            capture["initialized"] = True

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


def test_constructor_defaults_cli():
    llm = ClaudeCodeLLM()
    assert llm.backend == "cli"
    assert llm.model == "claude-sonnet-4-6"


def test_constructor_api_params():
    llm = ClaudeCodeLLM(
        backend="api", api_key="sk-x", max_tokens=2048,
        thinking=None, max_tool_iterations=7,
    )
    assert llm.backend == "api"
    assert llm.api_key == "sk-x"
    assert llm.max_tokens == 2048
    assert llm.thinking is None
    assert llm.max_tool_iterations == 7


def test_invalid_backend_raises():
    with pytest.raises(ValueError):
        ClaudeCodeLLM(backend="grpc")


def test_cli_backend_warns_on_api_only_params(caplog):
    with caplog.at_level("WARNING", logger="claude_code"):
        ClaudeCodeLLM(backend="cli", max_tokens=500, api_key="sk-x")
    assert "API-only parameter" in caplog.text
    assert "max_tokens" in caplog.text
    assert "api_key" in caplog.text


def test_cli_backend_no_warning_with_defaults(caplog):
    with caplog.at_level("WARNING", logger="claude_code"):
        ClaudeCodeLLM(backend="cli")
    assert "API-only parameter" not in caplog.text


def test_api_backend_no_warning_for_api_params(caplog):
    with caplog.at_level("WARNING", logger="claude_code"):
        ClaudeCodeLLM(backend="api", max_tokens=500, api_key="sk-x")
    assert "API-only parameter" not in caplog.text


def test_api_backend_warns_experimental(caplog):
    with caplog.at_level("WARNING", logger="claude_code"):
        ClaudeCodeLLM(backend="api")
    assert "experimental" in caplog.text


def test_cli_backend_no_experimental_warning(caplog):
    with caplog.at_level("WARNING", logger="claude_code"):
        ClaudeCodeLLM(backend="cli")
    assert "experimental" not in caplog.text


def test_mcp_result_to_text_joins_text_blocks():
    result = SimpleNamespace(content=[
        SimpleNamespace(text="hello"),
        SimpleNamespace(text="world"),
    ])
    assert ClaudeCodeLLM._mcp_result_to_text(result) == "hello\nworld"


def test_mcp_result_to_text_empty():
    assert ClaudeCodeLLM._mcp_result_to_text(SimpleNamespace(content=None)) == ""


def test_prompt_routes_to_api_backend(monkeypatch):
    llm = ClaudeCodeLLM(backend="api")
    sentinel = QueryResult(result="X", returncode=0, stderr="", stream_result="")
    monkeypatch.setattr(llm, "_run_api", lambda user, tools: sentinel)
    out = llm.prompt("hi", tools=[])
    assert out is sentinel
    assert out.success is True


def test_prompt_routes_to_cli_backend(monkeypatch):
    llm = ClaudeCodeLLM(backend="cli", log_stream=True)
    sentinel = QueryResult(result="Y", returncode=0, stderr="", stream_result="")
    monkeypatch.setattr(llm, "_run_claude_streaming", lambda user, tools: sentinel)
    out = llm.prompt("hi", tools=[])
    assert out is sentinel


# ---------------------------------------------------------------------------
# Mocked-loop tests (fake anthropic; no network, no SDK required)
# ---------------------------------------------------------------------------


def test_api_no_tools_request_shaping_and_result(monkeypatch):
    capture = {"calls": []}
    _install_fake_anthropic(
        monkeypatch,
        [_response([_text_block("PONG")], "end_turn", _usage(inp=10, out=5))],
        capture,
    )

    llm = ClaudeCodeLLM(backend="api", system_message="be terse", api_key="sk-test")
    cli = llm.prompt("ping", tools=[])

    assert cli.success is True
    assert cli.result == "PONG"
    assert cli.returncode == 0

    # The fake client received our explicit api_key.
    assert capture["api_key"] == "sk-test"

    # Exactly one turn (no tool calls), shaped as expected.
    assert len(capture["calls"]) == 1
    kw = capture["calls"][0]
    assert kw["model"] == "claude-sonnet-4-6"
    assert kw["max_tokens"] == 16000
    assert kw["thinking"] == {"type": "adaptive"}
    assert "tools" not in kw  # none passed
    # System prompt carries the prompt-cache breakpoint.
    assert kw["system"][0]["text"] == "be terse"
    assert kw["system"][0]["cache_control"] == {"type": "ephemeral"}

    # Token usage flowed into metadata.
    assert llm._last_metadata["input_tokens"] == 10
    assert llm._last_metadata["output_tokens"] == 5
    assert llm._last_metadata["num_turns"] == 1
    assert llm._last_metadata["model"] == "claude-sonnet-4-6"


def test_api_thinking_disabled_omits_param(monkeypatch):
    capture = {"calls": []}
    _install_fake_anthropic(
        monkeypatch, [_response([_text_block("ok")], "end_turn")], capture
    )
    llm = ClaudeCodeLLM(backend="api", thinking=None)
    llm.prompt("hi", tools=[])
    assert "thinking" not in capture["calls"][0]


def test_api_tool_loop_executes_mcp_and_feeds_results(monkeypatch):
    capture = {"calls": [], "urls": [], "tool_calls": []}
    _install_fake_anthropic(
        monkeypatch,
        [
            # Turn 1: think, then call the tool.
            _response(
                [
                    _thinking_block("let me compute"),
                    _tool_use_block("tu1", "calc__run", {"x": 21}),
                ],
                "tool_use",
                _usage(inp=10, out=5),
            ),
            # Turn 2: final answer.
            _response(
                [_text_block("the answer is 42")],
                "end_turn",
                _usage(inp=3, out=4, cr=8),
            ),
        ],
        capture,
    )
    _install_fake_mcp(monkeypatch, capture, tool_result_text="42")

    tool = SimpleNamespace(name="calc", hostname="localhost", port=9001)
    llm = ClaudeCodeLLM(backend="api")
    cli = llm.prompt("what is 21 doubled?", tools=[tool])

    assert cli.success is True
    assert cli.result == "the answer is 42"

    # Connected to the right MCP URL.
    assert capture["urls"] == ["http://localhost:9001/calc/mcp"]

    # The MCP tool was invoked with the MCP-side name and the model's input.
    assert capture["tool_calls"] == [("run", {"x": 21})]

    # Two model turns happened.
    assert len(capture["calls"]) == 2

    # First request advertised the namespaced tool, with a cache breakpoint
    # (no system prompt here, so the breakpoint lands on the last tool).
    tools_arg = capture["calls"][0]["tools"]
    assert tools_arg[0]["name"] == "calc__run"
    assert tools_arg[-1]["cache_control"] == {"type": "ephemeral"}

    # Second request carried the tool_result back to the model.
    second_msgs = capture["calls"][1]["messages"]
    tool_result_msg = second_msgs[-1]
    assert tool_result_msg["role"] == "user"
    block = tool_result_msg["content"][0]
    assert block["type"] == "tool_result"
    assert block["tool_use_id"] == "tu1"
    assert block["content"] == "42"
    assert block["is_error"] is False

    # Metadata accumulated across both turns.
    assert llm._last_metadata["num_turns"] == 2
    assert llm._last_metadata["input_tokens"] == 13
    assert llm._last_metadata["cache_read_input_tokens"] == 8


def test_api_unknown_tool_name_reports_error(monkeypatch):
    capture = {"calls": [], "urls": [], "tool_calls": []}
    _install_fake_anthropic(
        monkeypatch,
        [
            _response(
                [_tool_use_block("tu1", "calc__missing", {})], "tool_use"
            ),
            _response([_text_block("done")], "end_turn"),
        ],
        capture,
    )
    _install_fake_mcp(monkeypatch, capture)

    tool = SimpleNamespace(name="calc", hostname="localhost", port=9001)
    llm = ClaudeCodeLLM(backend="api")
    cli = llm.prompt("go", tools=[tool])

    assert cli.success is True
    # The real MCP tool was never called for the unknown name.
    assert capture["tool_calls"] == []
    # An error tool_result was fed back instead.
    block = capture["calls"][1]["messages"][-1]["content"][0]
    assert block["is_error"] is True
    assert "Unknown tool" in block["content"]


# ---------------------------------------------------------------------------
# Error translation (requires the real anthropic SDK + httpx to construct
# exception instances; skipped otherwise)
# ---------------------------------------------------------------------------


def test_translate_non_anthropic_error_returns_none():
    anthropic = pytest.importorskip("anthropic")  # noqa: F841
    llm = ClaudeCodeLLM(backend="api")
    assert llm._translate_api_error(ValueError("nope")) is None


def test_translate_status_errors():
    anthropic = pytest.importorskip("anthropic")
    httpx = pytest.importorskip("httpx")
    llm = ClaudeCodeLLM(backend="api")
    req = httpx.Request("POST", "https://api.anthropic.com/v1/messages")

    def make(exc_cls, status, headers=None):
        resp = httpx.Response(status, headers=headers or {}, request=req)
        return exc_cls("boom", response=resp, body=None)

    assert isinstance(
        llm._translate_api_error(make(anthropic.BadRequestError, 400)),
        InvalidRequestError,
    )
    assert isinstance(
        llm._translate_api_error(make(anthropic.AuthenticationError, 401)),
        AuthenticationError,
    )
    assert isinstance(
        llm._translate_api_error(make(anthropic.InternalServerError, 500)),
        ServerError,
    )

    rl = make(anthropic.RateLimitError, 429, headers={"retry-after": "30"})
    translated = llm._translate_api_error(rl)
    assert isinstance(translated, RateLimitError)
    assert translated.reset_time > datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Live API tests (real network; need ANTHROPIC_API_KEY + anthropic installed)
# ---------------------------------------------------------------------------

live = pytest.mark.skipif(
    not os.environ.get("ANTHROPIC_API_KEY"),
    reason="ANTHROPIC_API_KEY not set",
)


@live
def test_live_api_simple_prompt():
    pytest.importorskip("anthropic")
    llm = ClaudeCodeLLM(
        backend="api",
        model=CHEAP_MODEL,
        system_message="You answer with a single word and nothing else.",
        max_tokens=64,
        thinking=None,
    )
    cli = llm.prompt("Reply with exactly the word: PONG", tools=[])
    assert cli.success is True
    assert "PONG" in cli.result.upper()
    assert llm._last_metadata.get("output_tokens", 0) > 0


@pytest.fixture
def local_bash_tool():
    """Spin up a real BashTool MCP server on the local Ray instance.

    Skips (rather than errors) when the live prerequisites are missing, and
    always tears the tool down so the uvicorn server / Ray actor don't leak.
    """
    pytest.importorskip("anthropic")
    if not os.environ.get("ANTHROPIC_API_KEY"):
        pytest.skip("ANTHROPIC_API_KEY not set")

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
def test_live_api_with_bash_tool(local_bash_tool):
    """End-to-end: the API backend connects to a live MCP server, the model
    calls the bash tool, and the result flows back into the final answer."""
    llm = ClaudeCodeLLM(
        backend="api",
        model=CHEAP_MODEL,
        system_message=(
            "You have a bash tool available. To answer the user you MUST run "
            "the requested shell command with that tool and report its output "
            "verbatim. Never guess the output."
        ),
        max_tokens=2048,
        thinking=None,
    )
    cli = llm.prompt(
        "Run the shell command:  echo CHIA_TOOL_OK\n"
        "Then reply with exactly the line it printed.",
        tools=[local_bash_tool],
    )

    assert cli.success is True
    assert "CHIA_TOOL_OK" in cli.result
    # At least two turns means a tool round-trip actually happened
    # (turn 1 = tool_use, turn 2+ = final answer).
    assert llm._last_metadata.get("num_turns", 0) >= 2
    # The tool call + its result should appear in the event trace.
    assert "Tool Call" in cli.stream_result
    assert "Tool Result" in cli.stream_result


# ---------------------------------------------------------------------------
# Live tests: the full stream_result conversation log (API backend)
#
# The API backend's stream_result banner is "Prompt #N (api)" (the model is NOT
# in the banner, unlike the OpenAI-compat / Vertex / Bedrock backends), then the
# echoed [User Message], per-turn [Response] / [Thinking] / [Tool Call: name] +
# Args: / [Tool Result] sections, and a trailing rule. These assert the whole
# structure (presence + ordering) against the real API, not just a couple of
# markers. (thinking=None so no [Thinking] is expected here.)
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


def _assert_api_banner(stream: str) -> None:
    assert stream, "stream_result is empty"
    assert _BANNER_RULE in stream, "missing the '=' banner rule"
    assert "(api)" in stream and "Prompt #" in stream, (
        "banner missing the 'Prompt #N (api)' header\n" + stream
    )
    assert "[User Message]" in stream, "missing the [User Message] section"
    assert _TRAILING_RULE in stream, "missing the trailing '-' rule"


@live
def test_live_api_stream_log_simple():
    pytest.importorskip("anthropic")
    prompt = "Reply with exactly the word: PONG"
    llm = ClaudeCodeLLM(
        backend="api", model=CHEAP_MODEL,
        system_message="You answer with a single word and nothing else.",
        max_tokens=64, thinking=None,
    )
    stream = llm.prompt(prompt, tools=[]).stream_result

    _assert_api_banner(stream)
    assert prompt in stream, "the user message is not echoed into the log"
    assert "[Response]" in stream
    assert "[Tool Call:" not in stream
    assert "[Tool Result" not in stream
    assert stream.index("[User Message]") < stream.index("[Response]")


@live
def test_live_api_stream_log_tool_conversation(local_bash_tool):
    llm = ClaudeCodeLLM(
        backend="api", model=CHEAP_MODEL, system_message=_TOOL_SYSTEM,
        max_tokens=2048, thinking=None,
    )
    stream = llm.prompt(_TOOL_PROMPT, tools=[local_bash_tool]).stream_result

    _assert_api_banner(stream)
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


# ---------------------------------------------------------------------------
# CLI-backend session-transcript carry
#
# When ``resume_session=True`` the LLM captures the Claude Code CLI's
# ``<session_id>.jsonl`` transcript after every :meth:`prompt` call and
# re-pastes it before the next ``--resume`` run, so a session survives being
# scheduled on a different worker each call. (CLI backend only — the api
# backend keeps history in memory and writes no transcript.)
#
# * Offline tests exercise the capture/restore helpers against a temp dir.
# * The live cross-machine test is gated behind ``CHIA_LIVE_CLUSTER=1``:
#
#       CHIA_LIVE_CLUSTER=1 python -m pytest \
#           chia/models/tests/test_claude_api.py -k session -v -s
# ---------------------------------------------------------------------------


def test_transcript_path_none_without_resume(tmp_path):
    llm = ClaudeCodeLLM(backend="cli", projects_cwd=str(tmp_path))
    assert llm._session_id is None
    assert llm._transcript_path() is None


def test_transcript_path_uses_projects_cwd_and_session_id(tmp_path):
    llm = ClaudeCodeLLM(backend="cli", resume_session=True, projects_cwd=str(tmp_path))
    assert llm._session_id is not None
    assert llm._transcript_path() == os.path.join(
        str(tmp_path), f"{llm._session_id}.jsonl"
    )


def test_default_projects_cwd_is_llm_env_dir():
    llm = ClaudeCodeLLM(backend="cli", resume_session=True)
    assert llm._projects_cwd == "/home/ray/.claude/projects/-home-ray-llm-env"


def test_resolve_projects_dir_derives_from_cwd_when_unset(monkeypatch):
    # projects_cwd=None -> derive from CWD, escaping every non-alphanumeric to
    # "-" (matching the CLI: /home/ray/llm_env -> -home-ray-llm-env).
    llm = ClaudeCodeLLM(backend="cli", resume_session=True, projects_cwd=None)
    monkeypatch.setattr(os, "getcwd", lambda: "/home/ray/llm_env")
    expected = os.path.join(
        os.path.expanduser("~"), ".claude", "projects", "-home-ray-llm-env"
    )
    assert llm._resolve_projects_dir() == expected


def test_capture_reads_transcript_into_memory_and_cli(tmp_path):
    llm = ClaudeCodeLLM(backend="cli", resume_session=True, projects_cwd=str(tmp_path))
    sample = b'{"type":"user"}\n{"type":"assistant"}\n'
    path = os.path.join(str(tmp_path), f"{llm._session_id}.jsonl")
    with open(path, "wb") as f:
        f.write(sample)

    cli = ClaudeCodeQueryResult(result="", returncode=0, stderr="", stream_result="")
    llm._capture_transcript(cli)

    assert llm._session_transcript == sample
    assert llm._session_transcript_path == path
    assert cli.session_transcript == sample
    assert cli.session_transcript_path == path


def test_capture_noop_when_no_file(tmp_path):
    llm = ClaudeCodeLLM(backend="cli", resume_session=True, projects_cwd=str(tmp_path))
    cli = ClaudeCodeQueryResult(result="", returncode=0, stderr="", stream_result="")
    llm._capture_transcript(cli)
    assert llm._session_transcript is None
    assert cli.session_transcript is None


def test_restore_pastes_unconditionally_and_forces_resume(tmp_path):
    llm = ClaudeCodeLLM(backend="cli", resume_session=True, projects_cwd=str(tmp_path))
    path = os.path.join(str(tmp_path), f"{llm._session_id}.jsonl")
    # A stale file on this worker must be overwritten by the carried bytes.
    with open(path, "wb") as f:
        f.write(b"STALE")

    llm._session_transcript = b"FRESH"
    assert llm._call_counter == 0
    llm._restore_transcript()

    with open(path, "rb") as f:
        assert f.read() == b"FRESH"
    # A carried transcript means a prior conversation exists -> --resume.
    assert llm._call_counter == 1
    assert llm._session_transcript_path == path


def test_restore_noop_without_transcript(tmp_path):
    llm = ClaudeCodeLLM(backend="cli", resume_session=True, projects_cwd=str(tmp_path))
    llm._restore_transcript()  # no transcript captured yet
    assert not os.path.exists(os.path.join(str(tmp_path), f"{llm._session_id}.jsonl"))
    assert llm._call_counter == 0


def test_restore_noop_when_not_resuming(tmp_path):
    llm = ClaudeCodeLLM(backend="cli", projects_cwd=str(tmp_path))
    llm._session_transcript = b"FRESH"  # set, but resume_session was False
    llm._restore_transcript()
    # No session id -> nothing pasted.
    assert os.listdir(str(tmp_path)) == []


# --- Live cross-machine resume (needs a running Ray cluster) ---

live_cluster = pytest.mark.skipif(
    os.environ.get("CHIA_LIVE_CLUSTER") != "1",
    reason="set CHIA_LIVE_CLUSTER=1 to run against a live Ray cluster",
)

# The session dir baked into ClaudeCodeLLM's default projects_cwd only matches
# workers whose CWD is this path, so we target those workers explicitly.
_LLM_CWD = "/home/ray/llm_env"


def _select_worker_nodes():
    """Return node IDs and whether the cluster defines the claude_creds resource.

    Prefers ``claude_creds`` workers when the cluster defines that resource;
    otherwise falls back to any node whose CWD is ``_LLM_CWD`` (so the default
    ``projects_cwd`` lines up with where the CLI actually writes its transcript).
    """
    import ray
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    @ray.remote
    def _cwd():
        import os
        return os.getcwd()

    ids, has_creds = [], False
    for n in ray.nodes():
        if not n.get("Alive"):
            continue
        if n.get("Resources", {}).get("claude_creds", 0) >= 0.01:
            has_creds = True
            ids.append(n["NodeID"])
    if not ids:
        # No claude_creds resource on this cluster: pick nodes by CWD.
        for n in ray.nodes():
            if not n.get("Alive"):
                continue
            nid = n["NodeID"]
            cwd = ray.get(
                _cwd.options(
                    scheduling_strategy=NodeAffinitySchedulingStrategy(nid, soft=False)
                ).remote()
            )
            if cwd == _LLM_CWD:
                ids.append(nid)
    return ids, has_creds


def _init_ray_with_chia():
    """Connect to the cluster with this checkout's ``chia`` shipped to workers.

    Workers run the cluster image's (older) ``chia``, so we ship this checkout
    via ``runtime_env={"py_modules": [...]}`` to override it — otherwise the
    workers lack ``chia.models`` and run a stale ``ChiaFunction`` trampoline
    (wrong arity), which surfaces as ``No module named 'chia.models'`` or
    ``_hostname_i() takes 0 positional arguments but 1 was given``.

    Crucially, a ``runtime_env`` only attaches at *job* creation. If an earlier
    (offline) test already called ``ray.init`` in this process WITHOUT it,
    ``ray.init(runtime_env=...)`` is silently ignored — which is why this fails
    when the whole file runs but passes under an isolated ``-k``. So we tear
    down any existing connection first and start a fresh job. Override the
    shipped path with ``CHIA_SHIP_PY_MODULES`` (or ``""`` to skip shipping when
    the cluster already has this code deployed).
    """
    import ray
    import chia

    default_pkg = next(p for p in chia.__path__ if os.path.isdir(p))
    ship = os.environ.get("CHIA_SHIP_PY_MODULES", default_pkg)
    runtime_env = {"py_modules": [ship]} if ship else None
    if ray.is_initialized():
        ray.shutdown()
    ray.init(address="auto", runtime_env=runtime_env)


@live_cluster
def test_simple_call_cluster(capsys):
    """A remote ``prompt`` dispatch lands on the node it was pinned to.

    Pins a prompt to ``node_a`` (via ``NodeAffinitySchedulingStrategy``) and has
    the LLM run ``hostname -i``, then independently runs ``hostname -i`` on the
    same node through a tiny ``@ChiaFunction`` pinned the same way. The two
    addresses must match — proving the prompt's ``claude`` subprocess executed
    on ``node_a`` (the subprocess shares the worker's network namespace, so the
    two ``hostname -i`` results agree).

    ``resume_session=False`` here, so ``prompt.chia_remote`` returns a plain
    ObjectRef (no ``ObjectRefCallback`` wrapper, no transcript sync) — asserted
    inline. Gated by ``CHIA_LIVE_CLUSTER=1``; run with ``-s`` to see the printed
    node id and addresses.
    """
    import ray
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    from chia.base.ChiaFunction import get

    _init_ray_with_chia()

    node_ids, has_creds = _select_worker_nodes()
    if not node_ids:
        pytest.skip("no suitable claude workers found on this cluster")

    node_a = node_ids[0]

    @ChiaFunction()
    def _hostname_i():
        import subprocess
        return subprocess.run(
            ["hostname", "-i"], capture_output=True, text=True
        ).stdout.strip()

    opts = {"scheduling_strategy": NodeAffinitySchedulingStrategy(node_a, soft=False)}
    expected_ip = get(_hostname_i.options(**opts).chia_remote())

    llm = ClaudeCodeLLM(
        model=CHEAP_MODEL,
        resume_session=False,
        system_message="You can run shell commands. Answer concisely.",
        timeout_seconds=600,
    )

    def _run_on(node_id, message):
        strat = NodeAffinitySchedulingStrategy(node_id, soft=False)
        opts = {"scheduling_strategy": strat}
        if not has_creds:
            # Drop the default claude_creds pin this cluster doesn't define.
            opts["resources"] = {}
        # resume_session=False -> chia_remote returns a plain ObjectRef
        # (no ObjectRefCallback wrapper, no transcript sync).
        ref = llm.prompt.options(**opts).chia_remote(llm, message)
        assert not isinstance(ref, ObjectRefCallback)
        return get(ref)

    # Run `hostname -i` through the LLM on node_a and confirm it matches the
    # address we fetched directly from that node.
    cli1 = _run_on(
        node_a,
        "Run the shell command `hostname -i` and reply with ONLY its exact output.",
    )
    assert cli1.success, cli1.stderr

    with capsys.disabled():
        print("\n=== node A id:", node_a)
        print("expected (hostname -i on node_a):", expected_ip)
        print("--- call 1 (hostname -i via LLM) ---")
        print(cli1.result.strip())

    assert expected_ip, "could not resolve node_a's hostname -i"
    assert expected_ip in cli1.result, (
        f"LLM ran on the wrong node: hostname -i {cli1.result.strip()!r} "
        f"does not contain node_a IP {expected_ip!r}"
    )


@live_cluster
def test_session_resume_across_machines_cluster(capsys):
    import ray
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    from chia.base.ChiaFunction import get

    _init_ray_with_chia()

    node_ids, has_creds = _select_worker_nodes()
    if not node_ids:
        pytest.skip("no suitable claude workers found on this cluster")

    node_a = node_ids[0]
    node_b = node_ids[1] if len(node_ids) > 1 else node_ids[0]

    llm = ClaudeCodeLLM(
        model=CHEAP_MODEL,
        resume_session=True,
        system_message="You can run shell commands. Answer concisely.",
        timeout_seconds=600,
    )

    def cleanup():
        asyncio.sleep(5)

    def _run_on(node_id, message):
        strat = NodeAffinitySchedulingStrategy(node_id, soft=False)
        opts = {"scheduling_strategy": strat}
        if not has_creds:
            # Drop the default claude_creds pin this cluster doesn't define.
            opts["resources"] = {}
        # resume_session=True -> chia_remote returns an ObjectRefCallback
        # carrying _sync_transcript, so get() resolves it AND syncs the
        # transcript onto `llm` automatically — no callback=, no threading.
        ref = llm.prompt.options(**opts).chia_remote(llm, message, _chia_cleanup=cleanup)
        assert isinstance(ref, ObjectRefCallback)
        return ref, time.time()

    # Call 1 (node A): run hostname.
    cli1_ref, cli1_time = _run_on(
        node_a,
        "Run the shell command `hostname` and reply with ONLY its exact output.",
    )
    cli1b_ref, cli1b_time = _run_on(
        node_b,
        "Run the shell command `hostname` and reply with ONLY its exact output.",
    )
    print("cli1_time:", cli1_time, "cli1b_time:", cli1b_time)
    assert(abs(cli1_time - cli1b_time) < 5), "the two calls should be close together in time, indicating they are launched async"
    cli1 = get(cli1_ref)
    assert cli1.success, cli1.stderr
    assert cli1.session_transcript, "no transcript captured after the first call"

    # Call 2 (node B, ideally distinct): ask what the hostname was.
    cli2_ref, cli2_time = _run_on(
        node_b,
        "What was the exact hostname output from the previous command? "
        "Reply with ONLY that hostname.",
    )
    cli2 = get(cli2_ref)
    assert cli2.success, cli2.stderr

    with capsys.disabled():
        print("\n=== node A id:", node_a)
        print("=== node B id:", node_b, "(distinct:", node_a != node_b, ")")
        print("--- call 1 (hostname) ---")
        print(cli1.result.strip())
        print("--- call 2 (recalled hostname) ---")
        print(cli2.result.strip())

    # The resumed session should recall the earlier hostname.
    first_tokens = cli1.result.strip().split()
    if first_tokens:
        hostname = first_tokens[-1]
        assert hostname in cli2.result, (
            f"resumed model did not recall hostname {hostname!r}: {cli2.result!r}"
        )
