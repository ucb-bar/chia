"""Tests for the Ollama self-hosted preset in ollama.py.

Offline tests check the endpoint/auth defaults and the ``ollama_creds`` resource
on the re-decorated ``prompt``. The live tests run against a real Ollama server
and are auto-skipped unless one is reachable — handy on the GPU machine where
``dockerfiles/OllamaDockerfile`` is deployed.
"""

from __future__ import annotations

import os
import urllib.error
import urllib.request
from urllib.parse import urlsplit

import pytest

from chia.models.ollama import OllamaLLM
from chia.models.openai_compat import QueryResult, OpenAICompatLLM

_DEFAULT_BASE_URL = "http://localhost:11434/v1"


# ---------------------------------------------------------------------------
# Offline tests
# ---------------------------------------------------------------------------


def test_is_openai_compat_subclass():
    assert issubclass(OllamaLLM, OpenAICompatLLM)


def test_applies_endpoint_and_logging_defaults():
    llm = OllamaLLM(model="llama3.1:8b")
    assert llm.model == "llama3.1:8b"
    assert llm.base_url == _DEFAULT_BASE_URL
    assert llm.logging_name == "ollama"


def test_defaults_dummy_api_key():
    # The openai SDK refuses an empty key; OllamaLLM injects a dummy.
    assert OllamaLLM(model="m").api_key == "ollama"


def test_explicit_api_key_not_clobbered():
    assert OllamaLLM(model="m", api_key="real").api_key == "real"


def test_explicit_base_url_overrides_default():
    llm = OllamaLLM(model="m", base_url="http://gpu-node:11434/v1")
    assert llm.base_url == "http://gpu-node:11434/v1"


def test_env_base_url_override(monkeypatch):
    monkeypatch.setenv("OLLAMA_BASE_URL", "http://remote:11434/v1")
    assert OllamaLLM(model="m").base_url == "http://remote:11434/v1"


def test_env_api_key_suppresses_dummy(monkeypatch):
    # If the environment already supplies a key, don't override it with "ollama".
    monkeypatch.setenv("OPENAI_API_KEY", "env-key")
    llm = OllamaLLM(model="m")
    assert llm.api_key is None  # left to the SDK to read OPENAI_API_KEY


def test_prompt_carries_ollama_resource():
    assert OllamaLLM.prompt._chia_options["resources"] == {"ollama_creds": 0.01}


def test_prompt_exposes_remote_surface():
    assert hasattr(OllamaLLM.prompt, "chia_remote")
    assert hasattr(OllamaLLM.prompt, "options")


def test_redecorates_prompt():
    assert OllamaLLM.prompt is not OpenAICompatLLM.prompt


def test_prompt_returns_base_result(monkeypatch):
    llm = OllamaLLM(model="llama3.1:8b")
    sentinel = QueryResult(result="X", returncode=0, stderr="", stream_result="")
    monkeypatch.setattr(llm, "_run_openai", lambda user, tools: sentinel)
    out = llm.prompt("hi", tools=[])
    assert out is sentinel
    assert out.success is True


# ---------------------------------------------------------------------------
# Live tests against a real Ollama server (no API key — auth-free)
#
# Auto-skip unless an Ollama server is reachable. Point at a remote server with
# OLLAMA_BASE_URL (e.g. http://gpu-node:11434/v1); defaults to localhost. The
# model must already be pulled on the server (`ollama pull llama3.1:8b`, or the
# OLLAMA_PULL knob in the Docker image). Override with OLLAMA_TEST_MODEL.
# ---------------------------------------------------------------------------

_OLLAMA_TEST_BASE_URL = os.environ.get("OLLAMA_BASE_URL", _DEFAULT_BASE_URL)
_OLLAMA_TEST_MODEL = os.environ.get("OLLAMA_TEST_MODEL", "llama3.1:8b")


def _ollama_reachable() -> bool:
    """True if the Ollama server's /api/version answers quickly."""
    split = urlsplit(_OLLAMA_TEST_BASE_URL)
    version_url = f"{split.scheme}://{split.netloc}/api/version"
    try:
        with urllib.request.urlopen(version_url, timeout=2) as resp:
            return resp.status == 200
    except (urllib.error.URLError, OSError, ValueError):
        return False


ollama_live = pytest.mark.skipif(
    not _ollama_reachable(),
    reason=f"no Ollama server reachable at {_OLLAMA_TEST_BASE_URL}",
)


@ollama_live
def test_live_ollama_simple_prompt():
    pytest.importorskip("openai")
    llm = OllamaLLM(
        model=_OLLAMA_TEST_MODEL,
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
    if not _ollama_reachable():
        pytest.skip(f"no Ollama server reachable at {_OLLAMA_TEST_BASE_URL}")

    import uuid

    import ray

    from chia.base.tools.BashTool import BashTool

    # chia is a namespace package run from the repo root (not pip-installed), so
    # Ray actor workers — which start in a different cwd — can't import chia.base
    # unless the repo root is on their PYTHONPATH. Put it there via runtime_env,
    # the same pattern pid_registry_tb.py uses.
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


@ollama_live
def test_live_ollama_with_bash_tool(local_bash_tool):
    # Tool calling requires a tool-capable model (e.g. llama3.1, qwen2.5,
    # mistral). Set OLLAMA_TEST_MODEL accordingly if the default isn't pulled.
    llm = OllamaLLM(
        model=_OLLAMA_TEST_MODEL,
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


@ollama_live
def test_live_ollama_stream_log_simple():
    pytest.importorskip("openai")
    prompt = "Reply with exactly the word: PONG"
    llm = OllamaLLM(
        model=_OLLAMA_TEST_MODEL,
        system_message="You answer with a single word and nothing else.",
        max_tokens=64,
    )
    stream = llm.prompt(prompt, tools=[]).stream_result

    _assert_log_banner(stream, _OLLAMA_TEST_MODEL)
    assert prompt in stream, "the user message is not echoed into the log"
    # A no-tool prompt yields one [Response] and no tool sections.
    assert "[Response]" in stream
    assert "[Tool Call:" not in stream
    assert "[Tool Result" not in stream
    assert stream.index("[User Message]") < stream.index("[Response]")


@ollama_live
def test_live_ollama_stream_log_tool_conversation(local_bash_tool):
    llm = OllamaLLM(
        model=_OLLAMA_TEST_MODEL,
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

    _assert_log_banner(stream, _OLLAMA_TEST_MODEL)
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
