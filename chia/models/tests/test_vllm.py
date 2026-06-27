"""Tests for the vLLM self-hosted preset in vllm.py.

Offline tests check the endpoint/auth defaults and the ``vllm_creds`` resource on
the re-decorated ``prompt``. The live tests run against a real vLLM server and
are auto-skipped unless one is reachable — handy on the GPU machine where
``dockerfiles/VLLMDockerfile`` is deployed.
"""

from __future__ import annotations

import os
import urllib.error
import urllib.request
from urllib.parse import urlsplit

import pytest

from chia.models.openai_compat import QueryResult, OpenAICompatLLM
from chia.models.vllm import VLLMLLM

_DEFAULT_BASE_URL = "http://localhost:8200/v1"


# ---------------------------------------------------------------------------
# Offline tests
# ---------------------------------------------------------------------------


def test_is_openai_compat_subclass():
    assert issubclass(VLLMLLM, OpenAICompatLLM)


def test_applies_endpoint_and_logging_defaults():
    llm = VLLMLLM(model="Qwen/Qwen2.5-3B-Instruct")
    assert llm.model == "Qwen/Qwen2.5-3B-Instruct"
    assert llm.base_url == _DEFAULT_BASE_URL
    assert llm.logging_name == "vllm"


def test_defaults_dummy_api_key():
    # The openai SDK refuses an empty key; VLLMLLM injects a dummy.
    assert VLLMLLM(model="m").api_key == "vllm"


def test_explicit_api_key_not_clobbered():
    assert VLLMLLM(model="m", api_key="real").api_key == "real"


def test_explicit_base_url_overrides_default():
    llm = VLLMLLM(model="m", base_url="http://gpu-node:8000/v1")
    assert llm.base_url == "http://gpu-node:8000/v1"


def test_env_base_url_override(monkeypatch):
    monkeypatch.setenv("VLLM_BASE_URL", "http://remote:8000/v1")
    assert VLLMLLM(model="m").base_url == "http://remote:8000/v1"


def test_env_api_key_suppresses_dummy(monkeypatch):
    # If the environment already supplies a key, don't override it with "vllm".
    monkeypatch.setenv("OPENAI_API_KEY", "env-key")
    llm = VLLMLLM(model="m")
    assert llm.api_key is None  # left to the SDK to read OPENAI_API_KEY


def test_prompt_carries_vllm_resource():
    assert VLLMLLM.prompt._chia_options["resources"] == {"vllm_creds": 0.01}


def test_prompt_exposes_remote_surface():
    assert hasattr(VLLMLLM.prompt, "chia_remote")
    assert hasattr(VLLMLLM.prompt, "options")


def test_redecorates_prompt():
    assert VLLMLLM.prompt is not OpenAICompatLLM.prompt


def test_prompt_returns_base_result(monkeypatch):
    llm = VLLMLLM(model="Qwen/Qwen2.5-3B-Instruct")
    sentinel = QueryResult(result="X", returncode=0, stderr="", stream_result="")
    monkeypatch.setattr(llm, "_run_openai", lambda user, tools: sentinel)
    out = llm.prompt("hi", tools=[])
    assert out is sentinel
    assert out.success is True


# ---------------------------------------------------------------------------
# Live tests against a real vLLM server (no API key unless --api-key was set)
#
# Auto-skip unless a vLLM server is reachable. Point at a remote server with
# VLLM_BASE_URL (e.g. http://gpu-node:8000/v1); defaults to localhost. The model
# must match what the server was started with (VLLM_MODEL) — override the test
# model with VLLM_TEST_MODEL.
# ---------------------------------------------------------------------------

_VLLM_TEST_BASE_URL = os.environ.get("VLLM_BASE_URL", _DEFAULT_BASE_URL)
_VLLM_TEST_MODEL = os.environ.get("VLLM_TEST_MODEL", "Qwen/Qwen2.5-3B-Instruct")


def _vllm_reachable() -> bool:
    """True if the vLLM server's /health answers quickly."""
    split = urlsplit(_VLLM_TEST_BASE_URL)
    health_url = f"{split.scheme}://{split.netloc}/health"
    try:
        with urllib.request.urlopen(health_url, timeout=2) as resp:
            return resp.status == 200
    except (urllib.error.URLError, OSError, ValueError):
        return False


vllm_live = pytest.mark.skipif(
    not _vllm_reachable(),
    reason=f"no vLLM server reachable at {_VLLM_TEST_BASE_URL}",
)


@vllm_live
def test_live_vllm_simple_prompt():
    pytest.importorskip("openai")
    llm = VLLMLLM(
        model=_VLLM_TEST_MODEL,
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
    if not _vllm_reachable():
        pytest.skip(f"no vLLM server reachable at {_VLLM_TEST_BASE_URL}")

    import uuid

    import ray

    from chia.base.tools.BashTool import BashTool

    # chia is a namespace package run from the repo root (not pip-installed), so
    # Ray actor workers — which start in a different cwd — can't import chia.base
    # unless the repo root is on their PYTHONPATH. Put it there via runtime_env.
    chia_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    ray.init(
        ignore_reinit_error=True,
        runtime_env={"env_vars": {"PYTHONPATH": chia_root}},
    )
    tool = BashTool(name=f"echo_{uuid.uuid4().hex[:8]}", work_dir="/tmp")
    try:
        yield tool
    finally:
        tool.stop()


@vllm_live
def test_live_vllm_with_bash_tool(local_bash_tool):
    # Tool calling requires the server to have been started with
    # --enable-auto-tool-choice and a --tool-call-parser matching the model
    # (e.g. hermes for Qwen). If it wasn't, the model won't emit tool calls.
    llm = VLLMLLM(
        model=_VLLM_TEST_MODEL,
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
# stream_result is the human-readable transcript OpenAICompatLLM builds for every
# prompt: a banner with the model, the echoed [User Message], then per-turn
# [Response] / [Tool Call: ...] / [Tool Result] sections, and a trailing rule.
# These assert that whole structure (presence + ordering) against a real server,
# not just that a couple of markers happen to appear.
# ---------------------------------------------------------------------------

_BANNER_RULE = "=" * 80
_TRAILING_RULE = "-" * 80


def _assert_log_banner(stream: str, model: str) -> None:
    assert stream, "stream_result is empty"
    assert _BANNER_RULE in stream, "missing the '=' banner rule"
    assert f"chat.completions ({model})" in stream, "banner missing the model header"
    assert "[User Message]" in stream, "missing the [User Message] section"
    assert _TRAILING_RULE in stream, "missing the trailing '-' rule"


@vllm_live
def test_live_vllm_stream_log_simple():
    pytest.importorskip("openai")
    prompt = "Reply with exactly the word: PONG"
    llm = VLLMLLM(
        model=_VLLM_TEST_MODEL,
        system_message="You answer with a single word and nothing else.",
        max_tokens=64,
    )
    stream = llm.prompt(prompt, tools=[]).stream_result

    _assert_log_banner(stream, _VLLM_TEST_MODEL)
    assert prompt in stream, "the user message is not echoed into the log"
    # A no-tool prompt yields one [Response] and no tool sections.
    assert "[Response]" in stream
    assert "[Tool Call:" not in stream
    assert "[Tool Result" not in stream
    assert stream.index("[User Message]") < stream.index("[Response]")


@vllm_live
def test_live_vllm_stream_log_tool_conversation(local_bash_tool):
    llm = VLLMLLM(
        model=_VLLM_TEST_MODEL,
        system_message=(
            "You have a bash tool available. To answer the user you MUST run "
            "the requested shell command with that tool and report its output "
            "verbatim. Never guess the output."
        ),
        max_tokens=2048,
    )
    stream = llm.prompt(
        "Run the shell command:  echo CHIA_TOOL_OK\n"
        "Then reply with exactly the line it printed.",
        tools=[local_bash_tool],
    ).stream_result

    _assert_log_banner(stream, _VLLM_TEST_MODEL)
    # Every section of a tool round-trip is present and correctly named.
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
    # The echoed sentinel is captured in the tool-result portion of the log.
    assert "CHIA_TOOL_OK" in stream[result_idx:], (
        "tool-result section missing the echoed sentinel\n" + stream
    )
