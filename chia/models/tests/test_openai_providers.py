"""Tests for the lightweight per-provider presets in openai_providers.py.

Each provider sets endpoint/logging defaults and re-decorates ``prompt`` with
its own ``@ChiaFunction(resources={...})``. These tests check the config, the
per-provider resource on the decorator, and that the re-decorated prompt still
returns the base result.
"""

from __future__ import annotations

import os

import pytest

from chia.models.openai_compat import QueryResult, OpenAICompatLLM
from chia.models.openai_providers import (
    FireworksLLM,
    GroqLLM,
    NvidiaLLM,
    OpenAILLM,
    OpenRouterLLM,
)

# class -> (expected base_url, expected creds resource key)
PROVIDERS = {
    OpenAILLM: (None, "openai_creds"),
    FireworksLLM: ("https://api.fireworks.ai/inference/v1", "fireworks_creds"),
    GroqLLM: ("https://api.groq.com/openai/v1", "groq_creds"),
    OpenRouterLLM: ("https://openrouter.ai/api/v1", "openrouter_creds"),
    NvidiaLLM: ("https://integrate.api.nvidia.com/v1", "nvidia_nim_creds"),
}


@pytest.mark.parametrize("cls", list(PROVIDERS))
def test_provider_applies_defaults(cls):
    base_url, _ = PROVIDERS[cls]
    llm = cls(model="some-model")
    assert isinstance(llm, OpenAICompatLLM)
    assert llm.model == "some-model"
    assert llm.base_url == base_url


@pytest.mark.parametrize("cls", list(PROVIDERS))
def test_provider_prompt_carries_its_resource(cls):
    _, creds = PROVIDERS[cls]
    # The re-decorated prompt's ChiaFunction options carry this provider's
    # resource at 0.01 (gates on presence without throttling concurrency).
    assert cls.prompt._chia_options["resources"] == {creds: 0.01}


@pytest.mark.parametrize("cls", list(PROVIDERS))
def test_provider_prompt_exposes_remote_surface(cls):
    assert hasattr(cls.prompt, "chia_remote")
    assert hasattr(cls.prompt, "options")


@pytest.mark.parametrize("cls", list(PROVIDERS))
def test_provider_redecorates_prompt(cls):
    # Each provider re-decorates, so it does NOT share the base's prompt object.
    assert cls.prompt is not OpenAICompatLLM.prompt


def test_provider_prompt_returns_base_result(monkeypatch):
    # The re-decorated body must `return` the base call's QueryResult.
    llm = OpenAILLM(model="gpt-4o")
    sentinel = QueryResult(result="X", returncode=0, stderr="", stream_result="")
    monkeypatch.setattr(llm, "_run_openai", lambda user, tools: sentinel)
    out = llm.prompt("hi", tools=[])
    assert out is sentinel
    assert out.success is True


def test_openai_preset_uses_default_endpoint():
    assert OpenAILLM(model="gpt-4o").base_url is None  # -> OpenAI default / env


def test_explicit_base_url_overrides_default():
    llm = OpenRouterLLM(model="m", base_url="https://custom/v1")
    assert llm.base_url == "https://custom/v1"


def test_distinct_endpoints():
    urls = {url for url, _ in PROVIDERS.values()}
    assert len([u for u in urls if u]) == 4  # OpenAI is None; other four distinct


# ---------------------------------------------------------------------------
# Live tests against OpenRouter (real API; need a key + openai installed)
#
# Auth is env-driven, like every other backend: ``OpenRouterLLM`` bakes in the
# base_url, and the openai SDK reads the credential from ``OPENAI_API_KEY`` — so
# we pass NO api_key. Set ``OPENAI_API_KEY`` to your OpenRouter key (sk-or-...).
# Override the model with ``OPENROUTER_TEST_MODEL``; the default is a free model
# (the ``:free`` suffix => $0). Free-model availability rotates on OpenRouter, so
# if these fail with a model-not-found, pick a current free model from
# openrouter.ai/models.
# ---------------------------------------------------------------------------

openrouter_live = pytest.mark.skipif(
    not os.environ.get("OPENAI_API_KEY"),
    reason="OPENAI_API_KEY not set (set it to your OpenRouter key)",
)

_OPENROUTER_DEFAULT_MODEL = "openai/gpt-oss-120b:free"


@openrouter_live
def test_live_openrouter_simple_prompt():
    pytest.importorskip("openai")
    llm = OpenRouterLLM(
        model=os.environ.get("OPENROUTER_TEST_MODEL", _OPENROUTER_DEFAULT_MODEL),
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

    # chia is a namespace package run from the repo root (not pip-installed), so
    # Ray actor workers — which start in a different cwd — can't import chia.base
    # unless the repo root is on their PYTHONPATH. Put it there via runtime_env,
    # the same pattern pid_registry_tb.py uses. (Applied on first init only;
    # harmless if Ray is already up with it.)
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


@openrouter_live
def test_live_openrouter_with_bash_tool(local_bash_tool):
    llm = OpenRouterLLM(
        model=os.environ.get("OPENROUTER_TEST_MODEL", _OPENROUTER_DEFAULT_MODEL),
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
# Live tests against Groq (real API; need a key + openai installed)
#
# Same env-driven auth: ``GroqLLM`` bakes in the base_url and the openai SDK
# reads the credential from ``OPENAI_API_KEY`` — pass NO api_key. Set
# ``OPENAI_API_KEY`` to your Groq key (gsk_...). Groq has a no-card free tier;
# model IDs have no ``:free`` suffix (the free tier is account-level). Override
# the model with ``GROQ_TEST_MODEL``.
#
# NOTE: these gate on ``OPENAI_API_KEY`` just like the OpenRouter ones above, so
# point that key at ONE provider at a time and select with ``-k groq`` (or
# ``-k openrouter``). Running ``-k live`` with a single key would send the other
# provider's tests at the wrong endpoint and fail auth.
# ---------------------------------------------------------------------------

groq_live = pytest.mark.skipif(
    not os.environ.get("OPENAI_API_KEY"),
    reason="OPENAI_API_KEY not set (set it to your Groq key)",
)

_GROQ_DEFAULT_MODEL = "llama-3.3-70b-versatile"


@groq_live
def test_live_groq_simple_prompt():
    pytest.importorskip("openai")
    llm = GroqLLM(
        model=os.environ.get("GROQ_TEST_MODEL", _GROQ_DEFAULT_MODEL),
        system_message="You answer with a single word and nothing else.",
        max_tokens=64,
    )
    cli = llm.prompt("Reply with exactly the word: PONG", tools=[])
    assert cli.success is True
    assert "PONG" in cli.result.upper()
    assert llm._last_metadata.get("output_tokens", 0) > 0


@groq_live
def test_live_groq_with_bash_tool(local_bash_tool):
    llm = GroqLLM(
        model=os.environ.get("GROQ_TEST_MODEL", _GROQ_DEFAULT_MODEL),
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
# Live tests: the full stream_result conversation log (all providers)
#
# stream_result is the human-readable transcript OpenAICompatLLM builds for every
# prompt: a banner with the model, the echoed [User Message], then per-turn
# [Response] / [Tool Call: ...] / [Tool Result] sections, and a trailing rule.
# The per-provider stream-log tests below assert that whole structure (presence +
# ordering) against each real API, not just that a couple of markers appear. They
# all use this shared helper and live at the END of the file, below every
# provider's `*_live` mark / default-model definition they depend on. Select one
# provider at a time, e.g. `-k "fireworks and stream_log"`.
# ---------------------------------------------------------------------------

_BANNER_RULE = "=" * 80
_TRAILING_RULE = "-" * 80


def _assert_log_banner(stream: str, model: str) -> None:
    assert stream, "stream_result is empty"
    assert _BANNER_RULE in stream, "missing the '=' banner rule"
    assert f"chat.completions ({model})" in stream, "banner missing the model header"
    assert "[User Message]" in stream, "missing the [User Message] section"
    assert _TRAILING_RULE in stream, "missing the trailing '-' rule"


def _assert_simple_log(stream: str, model: str, prompt: str) -> None:
    """Assert a no-tool transcript: banner + echoed prompt + one [Response]."""
    _assert_log_banner(stream, model)
    assert prompt in stream, "the user message is not echoed into the log"
    assert "[Response]" in stream
    assert "[Tool Call:" not in stream
    assert "[Tool Result" not in stream
    assert stream.index("[User Message]") < stream.index("[Response]")


def _assert_tool_conversation_log(stream: str, tool_name: str) -> None:
    """Assert a full tool round-trip transcript: call -> result -> final answer."""
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
    # The echoed sentinel is captured in the tool-result portion of the log.
    assert "CHIA_TOOL_OK" in stream[result_idx:], (
        "tool-result section missing the echoed sentinel\n" + stream
    )


# ---------------------------------------------------------------------------
# Live tests against OpenAI itself (real API; need a key + openai installed)
#
# OpenAILLM uses the default endpoint, so make sure OPENAI_BASE_URL is unset
# (a leftover from testing another provider would redirect these). Set
# OPENAI_API_KEY to a real OpenAI key (sk-...). No free tier. Default model is a
# cheap tool-capable one; override with OPENAI_TEST_MODEL. Model ids rotate; list
# yours with:
#   curl -s https://api.openai.com/v1/models \
#     -H "Authorization: Bearer $OPENAI_API_KEY" | python3 -m json.tool
#
# Select with `-k live_openai`. (Plain `-k openai` over-matches: the FILENAME
# test_openai_providers.py contains "openai", so it would select every test in
# the file. "live_openai" appears only in these function names.)
# ---------------------------------------------------------------------------

openai_live = pytest.mark.skipif(
    not os.environ.get("OPENAI_API_KEY"),
    reason="OPENAI_API_KEY not set",
)

_OPENAI_DEFAULT_MODEL = "gpt-4o-mini"


@openai_live
def test_live_openai_simple_prompt():
    pytest.importorskip("openai")
    llm = OpenAILLM(
        model=os.environ.get("OPENAI_TEST_MODEL", _OPENAI_DEFAULT_MODEL),
        system_message="You answer with a single word and nothing else.",
        max_tokens=64,
    )
    cli = llm.prompt("Reply with exactly the word: PONG", tools=[])
    assert cli.success is True
    assert "PONG" in cli.result.upper()
    assert llm._last_metadata.get("output_tokens", 0) > 0


@openai_live
def test_live_openai_with_bash_tool(local_bash_tool):
    llm = OpenAILLM(
        model=os.environ.get("OPENAI_TEST_MODEL", _OPENAI_DEFAULT_MODEL),
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
# Live tests against Fireworks (real API; need a key + openai installed)
#
# Same env-driven auth: FireworksLLM bakes in the base_url and the openai SDK
# reads the credential from OPENAI_API_KEY — pass NO api_key. Set OPENAI_API_KEY
# to your Fireworks key (fw_...). No free tier (trial credits only); the default
# is a cheap tool-capable model. Override with FIREWORKS_TEST_MODEL —
# accounts/fireworks/models/llama-v3p3-70b-instruct is the more reliable caller.
# Model ids rotate; if the default 404s, list yours with:
#   curl -s https://api.fireworks.ai/inference/v1/models \
#     -H "Authorization: Bearer $OPENAI_API_KEY" | python3 -m json.tool
#
# Select with `-k fireworks` (gates on OPENAI_API_KEY like the others).
# ---------------------------------------------------------------------------

fireworks_live = pytest.mark.skipif(
    not os.environ.get("OPENAI_API_KEY"),
    reason="OPENAI_API_KEY not set (set it to your Fireworks key)",
)

_FIREWORKS_DEFAULT_MODEL = "accounts/fireworks/models/deepseek-v4-pro"


@fireworks_live
def test_live_fireworks_simple_prompt():
    pytest.importorskip("openai")
    llm = FireworksLLM(
        model=os.environ.get("FIREWORKS_TEST_MODEL", _FIREWORKS_DEFAULT_MODEL),
        system_message="You answer with a single word and nothing else.",
        max_tokens=64,
    )
    cli = llm.prompt("Reply with exactly the word: PONG", tools=[])
    assert cli.success is True
    assert "PONG" in cli.result.upper()
    assert llm._last_metadata.get("output_tokens", 0) > 0


@fireworks_live
def test_live_fireworks_with_bash_tool(local_bash_tool):
    llm = FireworksLLM(
        model=os.environ.get("FIREWORKS_TEST_MODEL", _FIREWORKS_DEFAULT_MODEL),
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
# Live tests against NVIDIA (real API; need a key + openai installed)
#
# Same env-driven auth: NvidiaLLM bakes in the base_url and the openai SDK
# reads the credential from OPENAI_API_KEY — pass NO api_key. Set
# OPENAI_API_KEY to your NVIDIA key (nvapi-..., from build.nvidia.com).
# Override the model with NVIDIA_TEST_MODEL; ids are the full "<vendor>/<name>"
# strings — list them (no auth needed) with:
#   curl -s https://integrate.api.nvidia.com/v1/models | python3 -m json.tool
#
# The default (Nemotron 3 Super) is a reasoning model: it spends output tokens
# thinking before it answers, so even the "single word" test gets a roomy
# max_tokens — 64 would truncate mid-reasoning.
#
# Select with `-k nvidia` (gates on OPENAI_API_KEY like the others).
# ---------------------------------------------------------------------------

nvidia_live = pytest.mark.skipif(
    not os.environ.get("OPENAI_API_KEY"),
    reason="OPENAI_API_KEY not set (set it to your NVIDIA key)",
)

_NVIDIA_DEFAULT_MODEL = "nvidia/nemotron-3-super-120b-a12b"


@nvidia_live
def test_live_nvidia_simple_prompt():
    pytest.importorskip("openai")
    llm = NvidiaLLM(
        model=os.environ.get("NVIDIA_TEST_MODEL", _NVIDIA_DEFAULT_MODEL),
        system_message="You answer with a single word and nothing else.",
        max_tokens=2048,
    )
    cli = llm.prompt("Reply with exactly the word: PONG", tools=[])
    assert cli.success is True
    assert "PONG" in cli.result.upper()
    assert llm._last_metadata.get("output_tokens", 0) > 0


@nvidia_live
def test_live_nvidia_with_bash_tool(local_bash_tool):
    llm = NvidiaLLM(
        model=os.environ.get("NVIDIA_TEST_MODEL", _NVIDIA_DEFAULT_MODEL),
        system_message=(
            "You have a bash tool available. To answer the user you MUST run "
            "the requested shell command with that tool and report its output "
            "verbatim. Never guess the output."
        ),
        max_tokens=4096,
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


_TOOL_PROMPT = (
    "Run the shell command:  echo CHIA_TOOL_OK\n"
    "Then reply with exactly the line it printed."
)
_TOOL_SYSTEM = (
    "You have a bash tool available. To answer the user you MUST run the "
    "requested shell command with that tool and report its output verbatim. "
    "Never guess the output."
)


@fireworks_live
def test_live_fireworks_stream_log_simple():
    pytest.importorskip("openai")
    prompt = "Reply with exactly the word: PONG"
    model = os.environ.get("FIREWORKS_TEST_MODEL", _FIREWORKS_DEFAULT_MODEL)
    llm = FireworksLLM(model=model, system_message="You answer with a single word and nothing else.", max_tokens=64)
    _assert_simple_log(llm.prompt(prompt, tools=[]).stream_result, model, prompt)


@fireworks_live
def test_live_fireworks_stream_log_tool_conversation(local_bash_tool):
    model = os.environ.get("FIREWORKS_TEST_MODEL", _FIREWORKS_DEFAULT_MODEL)
    llm = FireworksLLM(model=model, system_message=_TOOL_SYSTEM, max_tokens=2048)
    stream = llm.prompt(_TOOL_PROMPT, tools=[local_bash_tool]).stream_result
    _assert_log_banner(stream, model)
    _assert_tool_conversation_log(stream, local_bash_tool.name)


@openrouter_live
def test_live_openrouter_stream_log_simple():
    pytest.importorskip("openai")
    prompt = "Reply with exactly the word: PONG"
    model = os.environ.get("OPENROUTER_TEST_MODEL", _OPENROUTER_DEFAULT_MODEL)
    llm = OpenRouterLLM(model=model, system_message="You answer with a single word and nothing else.", max_tokens=64)
    _assert_simple_log(llm.prompt(prompt, tools=[]).stream_result, model, prompt)


@openrouter_live
def test_live_openrouter_stream_log_tool_conversation(local_bash_tool):
    model = os.environ.get("OPENROUTER_TEST_MODEL", _OPENROUTER_DEFAULT_MODEL)
    llm = OpenRouterLLM(model=model, system_message=_TOOL_SYSTEM, max_tokens=2048)
    stream = llm.prompt(_TOOL_PROMPT, tools=[local_bash_tool]).stream_result
    _assert_log_banner(stream, model)
    _assert_tool_conversation_log(stream, local_bash_tool.name)


@groq_live
def test_live_groq_stream_log_simple():
    pytest.importorskip("openai")
    prompt = "Reply with exactly the word: PONG"
    model = os.environ.get("GROQ_TEST_MODEL", _GROQ_DEFAULT_MODEL)
    llm = GroqLLM(model=model, system_message="You answer with a single word and nothing else.", max_tokens=64)
    _assert_simple_log(llm.prompt(prompt, tools=[]).stream_result, model, prompt)


@groq_live
def test_live_groq_stream_log_tool_conversation(local_bash_tool):
    model = os.environ.get("GROQ_TEST_MODEL", _GROQ_DEFAULT_MODEL)
    llm = GroqLLM(model=model, system_message=_TOOL_SYSTEM, max_tokens=2048)
    stream = llm.prompt(_TOOL_PROMPT, tools=[local_bash_tool]).stream_result
    _assert_log_banner(stream, model)
    _assert_tool_conversation_log(stream, local_bash_tool.name)


@openai_live
def test_live_openai_stream_log_simple():
    pytest.importorskip("openai")
    prompt = "Reply with exactly the word: PONG"
    model = os.environ.get("OPENAI_TEST_MODEL", _OPENAI_DEFAULT_MODEL)
    llm = OpenAILLM(model=model, system_message="You answer with a single word and nothing else.", max_tokens=64)
    _assert_simple_log(llm.prompt(prompt, tools=[]).stream_result, model, prompt)


@openai_live
def test_live_openai_stream_log_tool_conversation(local_bash_tool):
    model = os.environ.get("OPENAI_TEST_MODEL", _OPENAI_DEFAULT_MODEL)
    llm = OpenAILLM(model=model, system_message=_TOOL_SYSTEM, max_tokens=2048)
    stream = llm.prompt(_TOOL_PROMPT, tools=[local_bash_tool]).stream_result
    _assert_log_banner(stream, model)
    _assert_tool_conversation_log(stream, local_bash_tool.name)


@nvidia_live
def test_live_nvidia_stream_log_simple():
    pytest.importorskip("openai")
    prompt = "Reply with exactly the word: PONG"
    model = os.environ.get("NVIDIA_TEST_MODEL", _NVIDIA_DEFAULT_MODEL)
    llm = NvidiaLLM(model=model, system_message="You answer with a single word and nothing else.", max_tokens=2048)
    _assert_simple_log(llm.prompt(prompt, tools=[]).stream_result, model, prompt)


@nvidia_live
def test_live_nvidia_stream_log_tool_conversation(local_bash_tool):
    model = os.environ.get("NVIDIA_TEST_MODEL", _NVIDIA_DEFAULT_MODEL)
    llm = NvidiaLLM(model=model, system_message=_TOOL_SYSTEM, max_tokens=4096)
    stream = llm.prompt(_TOOL_PROMPT, tools=[local_bash_tool]).stream_result
    _assert_log_banner(stream, model)
    _assert_tool_conversation_log(stream, local_bash_tool.name)


# ---------------------------------------------------------------------------
# Permission controls (live): OpenAILLM is a raw-API backend with no permission
# gate. The args flow through **kwargs to OpenAICompatLLM, so the warning names
# OpenAILLM; this live test proves passing them does not break a real call.
# ---------------------------------------------------------------------------
import warnings as _warnings  # noqa: E402


@openai_live
def test_live_unsupported_permission_args_warn_but_run():
    pytest.importorskip("openai")
    with _warnings.catch_warnings(record=True) as rec:
        _warnings.simplefilter("always")
        llm = OpenAILLM(
            model=os.environ.get("OPENAI_TEST_MODEL", _OPENAI_DEFAULT_MODEL),
            system_message="You answer with a single word and nothing else.",
            max_tokens=64,
            dangerously_skip_permissions=True,
            config={"x": "y"},
        )
    msgs = " ".join(str(w.message) for w in rec)
    assert "does not support 'dangerously_skip_permissions'" in msgs
    assert "does not support a 'config'" in msgs
    cli = llm.prompt("Reply with exactly the word: PONG", tools=[])
    assert cli.success is True
    assert "PONG" in cli.result.upper()


# Worker for this test: `chia up chia/models/tests/cluster/all_models.yaml`
# (advertises openai_creds); the remote_prompt fixture skips if it's absent.
@pytest.mark.live_remote
def test_live_remote_unsupported_permission_args_warn_but_run(remote_prompt):
    pytest.importorskip("openai")
    with _warnings.catch_warnings(record=True) as rec:
        _warnings.simplefilter("always")
        llm = OpenAILLM(
            model=os.environ.get("OPENAI_TEST_MODEL", _OPENAI_DEFAULT_MODEL),
            system_message="You answer with a single word and nothing else.",
            max_tokens=64,
            dangerously_skip_permissions=True,
            config={"x": "y"},
        )
    msgs = " ".join(str(w.message) for w in rec)
    assert "does not support 'dangerously_skip_permissions'" in msgs
    assert "does not support a 'config'" in msgs
    cli = remote_prompt(llm, "Reply with exactly the word: PONG", "openai_creds")
    assert cli.success is True
    assert "PONG" in cli.result.upper()
