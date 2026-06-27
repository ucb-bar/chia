"""Offline tests for :class:`chia.models.antigravity.AntigravityLLM`.

Set ``ANTIGRAVITY_LIVE_TEST=1`` to run the opt-in live smoke test against an
authenticated local ``agy`` CLI.
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import timezone
from types import SimpleNamespace

import pytest

from chia.models import antigravity as agy_mod
from chia.models.antigravity import (
    AntigravityLLM,
    AuthenticationError,
    BillingError,
    InvalidRequestError,
    MaxOutputTokensError,
    QueryResult,
    RateLimitError,
    ServerError,
    UnknownAntigravityError,
    parse_rate_limit_reset,
)


def _cli(returncode=1, stderr="", result="", stream_result=""):
    return QueryResult(result, returncode, stderr, stream_result)


def _fake_subprocess(monkeypatch, capture, *, stdout="PONG", stderr="", returncode=0):
    def fake_run(cmd, **kwargs):
        capture.update(cmd=cmd, kwargs=kwargs)
        return SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)

    monkeypatch.setattr(agy_mod.subprocess, "run", fake_run)


def test_constructor_and_chia_surface(caplog):
    with caplog.at_level("INFO", logger="antigravity"):
        llm = AntigravityLLM()
    assert llm.model is None
    assert llm.agy_bin == "agy"
    assert "experimental" in caplog.text
    assert "default model" in caplog.text
    assert hasattr(AntigravityLLM.prompt, "chia_remote")
    assert AntigravityLLM.prompt._chia_options["resources"] == {"antigravity_creds": 0.01}


def test_prompt_formatting():
    assert AntigravityLLM()._format_prompt("hi") == "hi"
    formatted = AntigravityLLM(system_message="be terse")._format_prompt("say pong")
    assert "[System Instructions]" in formatted
    assert "be terse" in formatted
    assert "[User Request]" in formatted


def test_build_cmd_flags():
    llm = AntigravityLLM(
        model="gemini-2.5-pro",
        add_dirs=["/tmp/work"],
        timeout_seconds=120,
    )
    cmd = llm._build_cmd("say pong")
    assert cmd[0] == "agy"
    assert "--dangerously-skip-permissions" in cmd
    assert cmd[cmd.index("--model") + 1] == "gemini-2.5-pro"
    assert cmd[cmd.index("--add-dir") + 1] == "/tmp/work"
    assert cmd[cmd.index("--print-timeout") + 1] == "120s"
    # The prompt is the value of --print and comes last.
    assert cmd[-2] == "--print"
    assert cmd[-1] == "say pong"


def test_build_cmd_safe_defaults_omit_optional_flags():
    cmd = AntigravityLLM(dangerously_skip_permissions=False)._build_cmd("hi")
    assert "--dangerously-skip-permissions" not in cmd
    assert "--sandbox" not in cmd
    assert "--model" not in cmd


@pytest.mark.parametrize(
    ("tool_name", "expected_url"),
    [
        ("calc", "http://localhost:9001/calc/mcp"),
        ("calc.one", "http://localhost:9001/calc.one/mcp"),
    ],
)
def test_write_mcp_config(tmp_path, tool_name, expected_url):
    llm = AntigravityLLM(gemini_dir=str(tmp_path))
    tool = SimpleNamespace(name=tool_name, hostname="localhost", port=9001)
    llm._write_mcp_config([tool])
    with open(llm._mcp_config_path) as f:
        config = json.load(f)
    assert config["mcpServers"][tool_name]["httpUrl"] == expected_url


def test_write_mcp_config_merges_and_preserves_unmanaged(tmp_path):
    llm = AntigravityLLM(gemini_dir=str(tmp_path))
    os.makedirs(os.path.dirname(llm._mcp_config_path), exist_ok=True)
    with open(llm._mcp_config_path, "w") as f:
        json.dump({"mcpServers": {"keepme": {"httpUrl": "http://x/keepme/mcp"}}}, f)
    tool = SimpleNamespace(name="calc", hostname="localhost", port=9001)
    llm._write_mcp_config([tool])
    with open(llm._mcp_config_path) as f:
        config = json.load(f)
    assert "keepme" in config["mcpServers"]
    assert "calc" in config["mcpServers"]


def test_write_mcp_config_noop_without_tools(tmp_path):
    llm = AntigravityLLM(gemini_dir=str(tmp_path))
    llm._write_mcp_config([])
    assert not os.path.exists(llm._mcp_config_path)


def test_parse_rate_limit_reset():
    reset = parse_rate_limit_reset("usage limit - resets 4pm (America/Los_Angeles)")
    assert reset is not None
    assert reset.tzinfo == timezone.utc


def test_prompt_routes_to_run_antigravity(monkeypatch):
    llm = AntigravityLLM()
    sentinel = QueryResult("X", 0, "", "")
    monkeypatch.setattr(llm, "_run_antigravity", lambda user, tools: sentinel)
    out = llm.prompt("hi", tools=[])
    assert out is sentinel
    assert out.success is True
    assert llm._last_metadata["model"] == "antigravity-default"


def test_run_antigravity_subprocess_flow(monkeypatch, tmp_path):
    capture = {}
    _fake_subprocess(monkeypatch, capture, stdout="  PONG  ", stderr="")
    cli = AntigravityLLM(
        model="gemini-2.5-pro",
        system_message="be terse",
        work_dir="/tmp",
        gemini_dir=str(tmp_path),
        timeout_seconds=33,
    )._run_antigravity("say pong", tools=[])
    assert cli.result == "PONG"  # stdout is stripped
    assert "[Response]\nPONG" in cli.stream_result
    assert capture["cmd"][-1].startswith("[System Instructions]")
    assert capture["kwargs"]["timeout"] == 63  # timeout_seconds + 30
    assert capture["kwargs"]["cwd"] == "/tmp"


def test_run_antigravity_writes_mcp_config(monkeypatch, tmp_path):
    capture = {}
    _fake_subprocess(monkeypatch, capture, stdout="ok")
    tool = SimpleNamespace(name="calc", hostname="localhost", port=9001)
    llm = AntigravityLLM(gemini_dir=str(tmp_path))
    llm._run_antigravity("use calc", tools=[tool])
    with open(llm._mcp_config_path) as f:
        config = json.load(f)
    assert config["mcpServers"]["calc"]["httpUrl"] == "http://localhost:9001/calc/mcp"


def test_classify_clean_success_no_raise():
    AntigravityLLM()._classify_error(_cli(returncode=0, result="PONG"))


def test_classify_benign_prose_on_clean_exit_no_raise():
    # Generic words like "timeout"/"login" in a real answer must NOT be flagged
    # when agy exited cleanly — only the precise CLI signatures count at exit 0.
    AntigravityLLM()._classify_error(
        _cli(returncode=0, result="To fix a connection timeout, check your login settings.")
    )


def test_classify_auth_failure_on_exit_zero():
    # agy prints the OAuth prompt to stdout and exits 0 when unauthenticated;
    # the hard signature must still raise AuthenticationError.
    with pytest.raises(AuthenticationError):
        AntigravityLLM()._classify_error(
            _cli(returncode=0, result="Authentication required. Please visit the URL to log in:")
        )


@pytest.mark.parametrize(
    ("message", "error_cls", "returncode"),
    [
        ("429 rate limit", RateLimitError, 0),
        ("Authentication required. Please sign in", AuthenticationError, 0),
        ("You are not logged into Antigravity", AuthenticationError, 0),
        ("Error: authentication timed out.", AuthenticationError, 0),
        ("payment required: out of credit", BillingError, 1),
        ("invalid model: nope", InvalidRequestError, 1),
        ("503 service unavailable", ServerError, 1),
        ("maximum output token limit reached", MaxOutputTokensError, 1),
        ("something surprising", UnknownAntigravityError, 1),
    ],
)
def test_classify_errors(message, error_cls, returncode):
    with pytest.raises(error_cls):
        AntigravityLLM()._classify_error(_cli(returncode=returncode, stderr=message))


live = pytest.mark.skipif(
    os.environ.get("ANTIGRAVITY_LIVE_TEST") != "1" or not shutil.which("agy"),
    reason="set ANTIGRAVITY_LIVE_TEST=1 and authenticate agy to run live tests",
)


@live
def test_live_antigravity_simple_prompt():
    llm = AntigravityLLM(
        system_message="You answer with a single word and nothing else.",
        timeout_seconds=180,
    )
    cli = llm.prompt("Reply with exactly the word: PONG", tools=[])
    assert cli.success is True
    assert "PONG" in cli.result.upper()
