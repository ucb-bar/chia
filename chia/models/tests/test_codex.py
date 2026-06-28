"""Offline tests for :class:`chia.models.codex.CodexLLM`.

Set ``CODEX_LIVE_TEST=1`` to run the opt-in live smoke test against an
authenticated local Codex CLI.
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import timezone
from types import SimpleNamespace

import pytest

from chia.models import codex as codex_mod
from chia.models.codex import (
    AuthenticationError,
    BillingError,
    CodexQueryResult,
    QueryResult,
    CodexLLM,
    InvalidRequestError,
    MaxOutputTokensError,
    RateLimitError,
    ServerError,
    UnknownCodexError,
    parse_session_id,
    parse_rate_limit_reset,
)


def _event(event_type, **kwargs):
    return json.dumps({"type": event_type, **kwargs})


def _cli(returncode=1, stderr="", result="", stream_result=""):
    return QueryResult(result, returncode, stderr, stream_result)


def _fake_subprocess(monkeypatch, capture, *, stdout="", stderr="", returncode=0, final="PONG"):
    def fake_run(cmd, **kwargs):
        capture.update(cmd=cmd, kwargs=kwargs)
        path = cmd[cmd.index("--output-last-message") + 1]
        capture["output_last_message"] = path
        with open(path, "w") as f:
            f.write(final)
        return SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)

    monkeypatch.setattr(codex_mod.subprocess, "run", fake_run)


def _disable_profiler(monkeypatch):
    import chia.trace.profiler as profiler_mod

    monkeypatch.setattr(
        profiler_mod,
        "get_profiler",
        lambda: SimpleNamespace(enabled=False, add_info=lambda _info: None),
    )


def test_constructor_and_chia_surface(caplog):
    with caplog.at_level("INFO", logger="codex"):
        llm = CodexLLM()
    assert llm.model is None
    assert llm.codex_bin == "codex"
    assert "experimental" in caplog.text
    assert "default model" in caplog.text
    assert hasattr(CodexLLM.prompt, "chia_remote")
    assert CodexLLM.prompt._chia_options["resources"] == {"codex_creds": 0.01}


def test_prompt_formatting():
    assert CodexLLM()._format_prompt("hi") == "hi"
    formatted = CodexLLM(system_message="be terse")._format_prompt("say pong")
    assert "[System Instructions]" in formatted
    assert "be terse" in formatted
    assert "[User Request]" in formatted


@pytest.mark.parametrize(
    ("tool_name", "expected"),
    [
        ("calc", 'mcp_servers.calc.url="http://localhost:9001/calc/mcp"'),
        ("calc.one", 'mcp_servers."calc.one".url="http://localhost:9001/calc.one/mcp"'),
    ],
)
def test_mcp_config_args(tool_name, expected):
    tool = SimpleNamespace(name=tool_name, hostname="localhost", port=9001)
    assert CodexLLM()._mcp_config_args([tool]) == ["-c", expected]


def test_build_cmd_flags_and_reasoning_effort():
    llm = CodexLLM(
        model="gpt-test",
        work_dir="/tmp/work",
        ephemeral=True,
        reasoning_effort="xhigh",
    )
    cmd = llm._build_cmd(output_last_message_path="/tmp/out.txt")
    assert cmd[:4] == ["codex", "exec", "--json", "--color"]
    assert cmd[cmd.index("--model") + 1] == "gpt-test"
    assert cmd[cmd.index("--cd") + 1] == "/tmp/work"
    assert cmd[cmd.index("--output-last-message") + 1] == "/tmp/out.txt"
    assert "--skip-git-repo-check" in cmd
    assert "--ephemeral" in cmd
    assert "--dangerously-bypass-approvals-and-sandbox" in cmd
    assert 'model_reasoning_effort="xhigh"' in cmd
    assert cmd[-1] == "-"


def test_build_cmd_resume_flags_and_reasoning_effort():
    session_id = "123e4567-e89b-12d3-a456-426614174000"
    llm = CodexLLM(
        model="gpt-test",
        work_dir="/tmp/work",
        ephemeral=True,
        reasoning_effort="xhigh",
        resume_session=True,
    )
    cmd = llm._build_cmd(
        output_last_message_path="/tmp/out.txt",
        resume_session_id=session_id,
    )
    assert cmd[:4] == ["codex", "exec", "resume", "--json"]
    assert "--color" not in cmd
    assert "--cd" not in cmd
    assert cmd[cmd.index("--model") + 1] == "gpt-test"
    assert cmd[cmd.index("--output-last-message") + 1] == "/tmp/out.txt"
    assert "--skip-git-repo-check" in cmd
    assert "--ephemeral" in cmd
    assert "--dangerously-bypass-approvals-and-sandbox" in cmd
    assert 'model_reasoning_effort="xhigh"' in cmd
    assert cmd[-2:] == [session_id, "-"]


def test_build_cmd_safe_sandbox_flags():
    cmd = CodexLLM(
        dangerously_bypass_approvals_and_sandbox=False,
        sandbox="read-only",
        approval_policy="never",
    )._build_cmd()
    assert "--dangerously-bypass-approvals-and-sandbox" not in cmd
    assert cmd.index("--ask-for-approval") < cmd.index("exec")
    assert cmd[cmd.index("--sandbox") + 1] == "read-only"
    assert cmd[cmd.index("--ask-for-approval") + 1] == "never"


def test_parse_rate_limit_reset():
    reset = parse_rate_limit_reset("usage limit - resets 4pm (America/Los_Angeles)")
    assert reset is not None
    assert reset.tzinfo == timezone.utc


def test_parse_session_id_from_jsonl():
    session_id = "123e4567-e89b-12d3-a456-426614174000"
    stdout = "\n".join([
        _event("turn_start"),
        json.dumps({"type": "session_configured", "session_id": session_id}),
    ])
    assert parse_session_id(stdout) == session_id


def test_parse_session_id_nested_and_regex_fallback():
    session_id = "123e4567-e89b-12d3-a456-426614174000"
    assert parse_session_id(json.dumps({"payload": {"conversationId": session_id}})) == session_id
    assert parse_session_id(f"created session {session_id}") == session_id
    assert parse_session_id(f"plain uuid {session_id}") is None


def test_parse_jsonl_stream_response_tool_usage_and_stderr():
    stdout = "\n".join([
        _event("assistant_message", message={"content": "hello"}),
        json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": " pong"}}),
        _event("tool_call", name="calc", arguments={"x": 1}),
        _event("tool_result", output="2"),
        _event("turn_complete", usage={"input_tokens": 3, "output_tokens": 5}),
        "not-json",
    ])
    stream, meta, fallback = CodexLLM._parse_jsonl_stream(stdout, "stderr text")
    assert "[Response]\nhello" in stream
    assert "[Response]\n pong" in stream
    assert "[Tool Call: calc]" in stream
    assert 'Args: {"x": 1}' in stream
    assert "[Tool Result]\n2" in stream
    assert "[UNPARSED]" in stream
    assert "[stderr]\nstderr text" in stream
    assert meta == {"num_turns": 1, "input_tokens": 3, "output_tokens": 5}
    assert fallback == "hello pong"


def test_prompt_routes_to_run_codex(monkeypatch):
    _disable_profiler(monkeypatch)
    llm = CodexLLM()
    sentinel = QueryResult("X", 0, "", "")
    monkeypatch.setattr(llm, "_run_codex", lambda user, tools: sentinel)
    out = llm.prompt("hi", tools=[])
    assert out is sentinel
    assert out.success is True
    assert llm._last_metadata["model"] == "codex-default"


def test_sync_session_copies_state_to_local_instance():
    session_id = "123e4567-e89b-12d3-a456-426614174000"
    llm = CodexLLM(resume_session=True)
    cli = CodexQueryResult(
        result="ok",
        returncode=0,
        stderr="",
        stream_result="",
        session_id=session_id,
        session_state={"state_5.sqlite": b"STATE"},
        session_state_paths=("state_5.sqlite",),
    )

    assert llm._sync_session(cli) is cli
    assert llm._session_id == session_id
    assert llm._session_state == {"state_5.sqlite": b"STATE"}
    assert llm._session_state_paths == ("state_5.sqlite",)


def test_resume_session_first_call_captures_and_second_call_restores(monkeypatch, tmp_path):
    _disable_profiler(monkeypatch)
    session_id = "123e4567-e89b-12d3-a456-426614174000"
    state_path = tmp_path / "state_5.sqlite"
    rollout_rel = f"sessions/2026/06/16/rollout-test-{session_id}.jsonl"
    rollout_path = tmp_path / rollout_rel
    captures = []

    def write_codex_state(rollout_bytes):
        rollout_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_bytes(b"OPAQUE_STATE")
        rollout_path.write_bytes(rollout_bytes)

    def fake_run(cmd, **kwargs):
        captures.append(cmd)
        path = cmd[cmd.index("--output-last-message") + 1]
        with open(path, "w") as f:
            f.write(f"OK{len(captures)}")
        if len(captures) == 1:
            write_codex_state(b"ROLLOUT1")
            stdout = json.dumps({"type": "session_configured", "session_id": session_id})
        else:
            assert state_path.exists()
            assert rollout_path.read_bytes() == b"ROLLOUT1"
            write_codex_state(b"ROLLOUT2")
            stdout = _event("turn_complete")
        return SimpleNamespace(returncode=0, stdout=stdout, stderr="")

    monkeypatch.setenv("CODEX_HOME", str(tmp_path))
    monkeypatch.setattr(codex_mod.subprocess, "run", fake_run)

    llm = CodexLLM(resume_session=True)
    cli1 = llm.prompt("first", tools=[])
    assert cli1.success is True
    assert cli1.session_id == session_id
    assert cli1.session_state is not None
    assert "state_5.sqlite" in cli1.session_state
    assert cli1.session_state[rollout_rel] == b"ROLLOUT1"
    assert captures[0][:3] == ["codex", "exec", "--json"]

    state_path.unlink()
    rollout_path.unlink()
    cli2 = llm.prompt("second", tools=[])
    assert cli2.success is True
    assert cli2.session_id == session_id
    assert cli2.session_state is not None
    assert "state_5.sqlite" in cli2.session_state
    assert cli2.session_state[rollout_rel] == b"ROLLOUT2"
    assert captures[1][:4] == ["codex", "exec", "resume", "--json"]
    assert captures[1][-2:] == [session_id, "-"]


def test_run_codex_subprocess_flow(monkeypatch):
    capture = {}
    _fake_subprocess(
        monkeypatch,
        capture,
        stdout=_event("assistant_message", message={"content": "fallback"}),
        final="PONG",
    )
    cli = CodexLLM(
        model="gpt-test",
        system_message="be terse",
        work_dir="/tmp",
        timeout_seconds=33,
    )._run_codex("say pong", tools=[])
    assert cli.result == "PONG"
    assert "[Response]\nfallback" in cli.stream_result
    assert capture["kwargs"]["input"].startswith("[System Instructions]")
    assert capture["kwargs"]["timeout"] == 33
    assert capture["kwargs"]["cwd"] == "/tmp"
    assert not os.path.exists(capture["output_last_message"])


def test_run_codex_fallback_and_mcp_config(monkeypatch):
    capture = {}
    _fake_subprocess(
        monkeypatch,
        capture,
        stdout=_event("assistant_message", message={"content": "fallback"}),
        final="",
    )
    tool = SimpleNamespace(name="calc", hostname="localhost", port=9001)
    cli = CodexLLM()._run_codex("use calc", tools=[tool])
    assert cli.result == "fallback"
    assert 'mcp_servers.calc.url="http://localhost:9001/calc/mcp"' in capture["cmd"]


def test_classify_clean_success_no_raise():
    CodexLLM()._classify_error(_cli(returncode=0, result="PONG"))


@pytest.mark.parametrize(
    "message",
    [
        "HTTP 429 Too Many Requests",
        "statusCode: 429",
        "APIError 429",
    ],
)
def test_classify_real_429_rate_limit(message, monkeypatch):
    monkeypatch.setattr(CodexLLM, "_get_node_id", lambda self: "test-node")
    with pytest.raises(RateLimitError):
        CodexLLM()._classify_error(_cli(returncode=1, stderr=message))


@pytest.mark.parametrize(
    "message",
    [
        "80000460:\t429000ef\tjal 80001088 <keyu>",
        "lw t1,1440(gp) # 80004298 <__global_pointer$+0x5a0>",
        "[Metadata]\nInput tokens: 436037 | Total tokens: 442985",
    ],
)
def test_classify_success_with_incidental_429_text(message):
    CodexLLM()._classify_error(_cli(returncode=0, stream_result=message))


@pytest.mark.parametrize(
    ("message", "error_cls", "returncode"),
    [
        ("429 rate limit", RateLimitError, 0),
        ("not logged in: run codex login", AuthenticationError, 1),
        ("payment required: add credit", BillingError, 1),
        ("invalid model: nope", InvalidRequestError, 1),
        ("503 service unavailable", ServerError, 1),
        ("max output token limit reached", MaxOutputTokensError, 1),
        ("something surprising", UnknownCodexError, 1),
    ],
)
def test_classify_errors(message, error_cls, returncode, monkeypatch):
    monkeypatch.setattr(CodexLLM, "_get_node_id", lambda self: "test-node")
    with pytest.raises(error_cls):
        CodexLLM()._classify_error(_cli(returncode=returncode, stderr=message))


def test_prompt_preserves_final_retry_error(monkeypatch):
    _disable_profiler(monkeypatch)
    monkeypatch.setattr(CodexLLM, "_get_node_id", lambda self: "test-node")
    calls = 0

    def fake_run_codex(self, user_message, tools):
        nonlocal calls
        calls += 1
        return _cli(returncode=1, stderr="something surprising")

    monkeypatch.setattr(CodexLLM, "_run_codex", fake_run_codex)
    cli = CodexLLM(retries=2).prompt("hello", tools=[])

    assert calls == 2
    assert cli.success is False
    assert cli.returncode == -1
    assert "UnknownCodexError" in cli.stderr
    assert "something surprising" in cli.stderr


live = pytest.mark.skipif(
    os.environ.get("CODEX_LIVE_TEST") != "1" or not shutil.which("codex"),
    reason="set CODEX_LIVE_TEST=1 and authenticate codex to run live tests",
)


@live
def test_live_codex_simple_prompt():
    llm = CodexLLM(
        system_message="You answer with a single word and nothing else.",
        timeout_seconds=180,
        dangerously_bypass_approvals_and_sandbox=False,
        sandbox="read-only",
        approval_policy="never",
        ephemeral=True,
    )
    cli = llm.prompt("Reply with exactly the word: PONG", tools=[])
    assert cli.success is True
    assert "PONG" in cli.result.upper()
