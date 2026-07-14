"""Offline tests for :class:`chia.models.copilot.CopilotLLM`.

Set ``COPILOT_LIVE_TEST=1`` to run the opt-in live smoke test against an
authenticated local Copilot CLI.
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import timezone
from types import SimpleNamespace

import pytest

from chia.models import copilot as copilot_mod
from chia.models.copilot import (
    AuthenticationError,
    BillingError,
    CopilotQueryResult,
    QueryResult,
    CopilotLLM,
    InvalidRequestError,
    MaxOutputTokensError,
    RateLimitError,
    ServerError,
    UnknownCopilotError,
    parse_session_id,
    parse_rate_limit_reset,
)


def _event(event_type, **data):
    return json.dumps({"type": event_type, "data": data})


def _ephemeral_event(event_type, **data):
    return json.dumps({"type": event_type, "data": data, "ephemeral": True})


def _result_event(session_id, exit_code=0, **usage):
    return json.dumps({"type": "result", "sessionId": session_id,
                       "exitCode": exit_code, "usage": usage})


def _cli(returncode=1, stderr="", result="", stream_result=""):
    return QueryResult(result, returncode, stderr, stream_result)


def _fake_subprocess(monkeypatch, capture, *, stdout="", stderr="", returncode=0):
    def fake_run(cmd, **kwargs):
        capture.update(cmd=cmd, kwargs=kwargs)
        return SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)

    monkeypatch.setattr(copilot_mod.subprocess, "run", fake_run)


def _disable_profiler(monkeypatch):
    import chia.trace.profiler as profiler_mod

    monkeypatch.setattr(
        profiler_mod,
        "get_profiler",
        lambda: SimpleNamespace(enabled=False, add_info=lambda _info: None),
    )


def test_constructor_and_chia_surface(caplog):
    with caplog.at_level("INFO", logger="copilot"):
        llm = CopilotLLM()
    assert llm.model is None
    assert llm.copilot_bin == "copilot"
    assert "experimental" in caplog.text
    assert "default model" in caplog.text
    assert hasattr(CopilotLLM.prompt, "chia_remote")
    assert CopilotLLM.prompt._chia_options["resources"] == {"copilot_creds": 0.01}


def test_prompt_formatting():
    assert CopilotLLM()._format_prompt("hi") == "hi"
    formatted = CopilotLLM(system_message="be terse")._format_prompt("say pong")
    assert "[System Instructions]" in formatted
    assert "be terse" in formatted
    assert "[User Request]" in formatted


def test_mcp_config_args():
    tool = SimpleNamespace(name="calc", hostname="localhost", port=9001)
    args = CopilotLLM()._mcp_config_args([tool])
    assert args[0] == "--additional-mcp-config"
    assert json.loads(args[1]) == {
        "mcpServers": {
            "calc": {
                "type": "http",
                "url": "http://localhost:9001/calc/mcp",
                "tools": ["*"],
            }
        }
    }


def test_build_cmd_flags_and_reasoning_effort():
    llm = CopilotLLM(
        model="gpt-test",
        work_dir="/tmp/work",
        reasoning_effort="xhigh",
    )
    cmd = llm._build_cmd("say pong")
    assert cmd[:5] == ["copilot", "--output-format", "json", "--no-color", "--no-auto-update"]
    assert cmd[cmd.index("--model") + 1] == "gpt-test"
    assert cmd[cmd.index("-C") + 1] == "/tmp/work"
    assert "--allow-all" in cmd
    assert cmd[cmd.index("--effort") + 1] == "xhigh"
    assert cmd[-2:] == ["--prompt", "say pong"]


def test_build_cmd_resume_flags():
    session_id = "123e4567-e89b-12d3-a456-426614174000"
    llm = CopilotLLM(
        model="gpt-test",
        work_dir="/tmp/work",
        reasoning_effort="xhigh",
        resume_session=True,
    )
    cmd = llm._build_cmd("say pong", resume_session_id=session_id)
    # --resume takes an optional value, so it must be a single =-joined token.
    assert f"--resume={session_id}" in cmd
    assert cmd[cmd.index("--model") + 1] == "gpt-test"
    # Unlike codex's --cd, copilot's global -C composes with --resume.
    assert cmd[cmd.index("-C") + 1] == "/tmp/work"
    assert "--allow-all" in cmd
    assert cmd[cmd.index("--effort") + 1] == "xhigh"
    assert cmd[-2:] == ["--prompt", "say pong"]


def test_build_cmd_safe_permission_flags():
    llm = CopilotLLM(
        allow_all=False,
        allow_tools=["shell(git:*)"],
        deny_tools=["shell(git push)"],
    )
    assert llm.dangerously_skip_permissions is False  # mirrored from allow_all
    cmd = llm._build_cmd("say pong")
    assert "--allow-all" not in cmd
    assert "--allow-tool=shell(git:*)" in cmd
    assert "--deny-tool=shell(git push)" in cmd


def test_build_cmd_safe_permissions_allow_prompt_mcp_tools():
    # When allow_all=False, MCP servers passed to prompt() must be explicitly
    # allowed, otherwise copilot disallows their tool calls.
    tools = [
        SimpleNamespace(name="calc", hostname="localhost", port=9001),
        SimpleNamespace(name="shell(git:*)", hostname="localhost", port=9002),
    ]
    llm = CopilotLLM(allow_all=False, allow_tools=["shell(git:*)"])
    cmd = llm._build_cmd("say pong", tools=tools)
    assert "--allow-tool=calc" in cmd
    # Already-allowed names are not duplicated.
    assert cmd.count("--allow-tool=shell(git:*)") == 1
    # The instance list is not mutated across calls.
    assert llm.allow_tools == ["shell(git:*)"]

    allow_all_cmd = CopilotLLM(allow_all=True)._build_cmd("say pong", tools=tools)
    assert not any(arg.startswith("--allow-tool") for arg in allow_all_cmd)


def test_parse_rate_limit_reset():
    reset = parse_rate_limit_reset("usage limit - resets 4pm (America/Los_Angeles)")
    assert reset is not None
    assert reset.tzinfo == timezone.utc


def test_parse_session_id_from_jsonl():
    session_id = "123e4567-e89b-12d3-a456-426614174000"
    stdout = "\n".join([
        _event("assistant.turn_start", turnId="0"),
        _result_event(session_id),
    ])
    assert parse_session_id(stdout) == session_id


def test_parse_session_id_ignores_event_envelope_ids():
    # Every copilot event envelope carries unrelated id/parentId UUIDs; only
    # explicitly session-named keys (e.g. the result event's sessionId) count.
    envelope = json.dumps({
        "type": "session.mcp_server_status_changed",
        "data": {"serverName": "github-mcp-server", "status": "connected"},
        "id": "bd83dd84-4a8f-433f-a0aa-246c962e1f65",
        "parentId": "8f4884f3-dc8f-4e47-9943-5dd18c2257b0",
    })
    assert parse_session_id(envelope) is None


def test_parse_session_id_regex_fallback():
    session_id = "123e4567-e89b-12d3-a456-426614174000"
    assert parse_session_id(f"created session {session_id}") == session_id
    assert parse_session_id(f"plain uuid {session_id}") is None


def test_parse_jsonl_stream_response_tool_usage_and_stderr():
    session_id = "123e4567-e89b-12d3-a456-426614174000"
    stdout = "\n".join([
        _event("user.message", content="say hello"),
        _event("assistant.message", content="hello", outputTokens=3),
        _ephemeral_event("assistant.message_delta", deltaContent="hel"),
        _event("tool.execution_start", toolName="calc", arguments={"x": 1}),
        _event("tool.execution_complete", success=True, result={"content": "2"}),
        _event("assistant.turn_end", turnId="0"),
        _event("assistant.message", content=" pong", outputTokens=5),
        _event("assistant.turn_end", turnId="1"),
        _result_event(session_id, premiumRequests=1),
        "not-json",
    ])
    stream, meta, result_text, result_code = CopilotLLM._parse_jsonl_stream(stdout, "stderr text")
    assert "[Response]\nhello" in stream
    assert "[Response]\n pong" in stream
    # The echoed user message and the ephemeral streaming delta are not responses.
    assert stream.count("[Response]") == 2
    assert "[Tool Call: calc]" in stream
    assert 'Args: {"x": 1}' in stream
    assert "[Tool Result]\n2" in stream
    assert "[UNPARSED]" in stream
    assert "[stderr]\nstderr text" in stream
    assert meta == {"num_turns": 2, "output_tokens": 8, "premium_requests": 1}
    assert result_text == "hello pong"
    assert result_code == 0


def test_prompt_routes_to_run_copilot(monkeypatch):
    _disable_profiler(monkeypatch)
    llm = CopilotLLM()
    sentinel = QueryResult("X", 0, "", "")
    monkeypatch.setattr(llm, "_run_copilot", lambda user, tools: sentinel)
    out = llm.prompt("hi", tools=[])
    assert out is sentinel
    assert out.success is True
    assert llm._last_metadata["model"] == "copilot-default"


def test_sync_session_copies_state_to_local_instance():
    session_id = "123e4567-e89b-12d3-a456-426614174000"
    llm = CopilotLLM(resume_session=True)
    cli = CopilotQueryResult(
        result="ok",
        returncode=0,
        stderr="",
        stream_result="",
        session_id=session_id,
        session_state={"session-store.db": b"STATE"},
        session_state_paths=("session-store.db",),
    )

    assert llm._sync_session(cli) is cli
    assert llm._session_id == session_id
    assert llm._session_state == {"session-store.db": b"STATE"}
    assert llm._session_state_paths == ("session-store.db",)


def test_resume_session_first_call_captures_and_second_call_restores(monkeypatch, tmp_path):
    _disable_profiler(monkeypatch)
    session_id = "123e4567-e89b-12d3-a456-426614174000"
    store_path = tmp_path / "session-store.db"
    events_rel = f"session-state/{session_id}/events.jsonl"
    events_path = tmp_path / events_rel
    captures = []

    def write_copilot_state(events_bytes):
        events_path.parent.mkdir(parents=True, exist_ok=True)
        store_path.write_bytes(b"OPAQUE_STATE")
        events_path.write_bytes(events_bytes)

    def fake_run(cmd, **kwargs):
        captures.append(cmd)
        if len(captures) == 1:
            write_copilot_state(b"EVENTS1")
        else:
            assert store_path.exists()
            assert events_path.read_bytes() == b"EVENTS1"
            write_copilot_state(b"EVENTS2")
        stdout = "\n".join([
            _event("assistant.message", content=f"OK{len(captures)}"),
            _result_event(session_id),
        ])
        return SimpleNamespace(returncode=0, stdout=stdout, stderr="")

    monkeypatch.setenv("COPILOT_HOME", str(tmp_path))
    monkeypatch.setattr(copilot_mod.subprocess, "run", fake_run)

    llm = CopilotLLM(resume_session=True)
    cli1 = llm.prompt("first", tools=[])
    assert cli1.success is True
    assert cli1.session_id == session_id
    assert cli1.session_state is not None
    assert "session-store.db" in cli1.session_state
    assert cli1.session_state[events_rel] == b"EVENTS1"
    assert not any(arg.startswith("--resume") for arg in captures[0])

    store_path.unlink()
    events_path.unlink()
    cli2 = llm.prompt("second", tools=[])
    assert cli2.success is True
    assert cli2.session_id == session_id
    assert cli2.session_state is not None
    assert "session-store.db" in cli2.session_state
    assert cli2.session_state[events_rel] == b"EVENTS2"
    assert f"--resume={session_id}" in captures[1]


def test_run_copilot_subprocess_flow(monkeypatch):
    capture = {}
    _fake_subprocess(
        monkeypatch,
        capture,
        stdout="\n".join([
            _event("assistant.message", content="PONG"),
            _result_event("123e4567-e89b-12d3-a456-426614174000"),
        ]),
    )
    cli = CopilotLLM(
        model="gpt-test",
        system_message="be terse",
        work_dir="/tmp",
        timeout_seconds=33,
    )._run_copilot("say pong", tools=[])
    assert cli.result == "PONG"
    assert cli.returncode == 0
    assert "[Response]\nPONG" in cli.stream_result
    assert capture["cmd"][-2] == "--prompt"
    assert capture["cmd"][-1].startswith("[System Instructions]")
    assert capture["kwargs"]["timeout"] == 33
    assert capture["kwargs"]["cwd"] == "/tmp"


def test_run_copilot_exit_zero_without_result_event_is_failure(monkeypatch):
    # copilot exits 0 even when the CLI fails outright (e.g. an unknown
    # --model); the missing trailing `result` event marks the failure.
    capture = {}
    _fake_subprocess(
        monkeypatch,
        capture,
        stderr='Error: Model "bogus" from --model flag is not available.',
    )
    llm = CopilotLLM(model="bogus")
    monkeypatch.setattr(CopilotLLM, "_get_node_id", lambda self: "test-node")
    cli = llm._run_copilot("say pong", tools=[])
    assert cli.returncode == 1
    with pytest.raises(InvalidRequestError):
        llm._classify_error(cli)


def test_run_copilot_mcp_config(monkeypatch):
    capture = {}
    _fake_subprocess(
        monkeypatch,
        capture,
        stdout="\n".join([
            _event("assistant.message", content="4"),
            _result_event("123e4567-e89b-12d3-a456-426614174000"),
        ]),
    )
    tool = SimpleNamespace(name="calc", hostname="localhost", port=9001)
    cli = CopilotLLM()._run_copilot("use calc", tools=[tool])
    assert cli.result == "4"
    config = capture["cmd"][capture["cmd"].index("--additional-mcp-config") + 1]
    assert json.loads(config)["mcpServers"]["calc"]["url"] == "http://localhost:9001/calc/mcp"


def test_run_copilot_e2big_raises_invalid_request(monkeypatch):
    import errno

    def fake_run(cmd, **kwargs):
        raise OSError(errno.E2BIG, "Argument list too long")

    monkeypatch.setattr(copilot_mod.subprocess, "run", fake_run)
    monkeypatch.setattr(CopilotLLM, "_get_node_id", lambda self: "test-node")
    long_prompt = "x" * 500000
    with pytest.raises(InvalidRequestError, match="argument length limit"):
        CopilotLLM()._run_copilot(long_prompt, tools=[])


def test_run_copilot_other_oserror_propagates(monkeypatch):
    def fake_run(cmd, **kwargs):
        raise OSError(2, "No such file or directory")

    monkeypatch.setattr(copilot_mod.subprocess, "run", fake_run)
    with pytest.raises(OSError, match="No such file"):
        CopilotLLM()._run_copilot("hi", tools=[])


def test_classify_clean_success_no_raise():
    CopilotLLM()._classify_error(_cli(returncode=0, result="PONG"))


@pytest.mark.parametrize(
    "message",
    [
        "HTTP 429 Too Many Requests",
        "statusCode: 429",
        "APIError 429",
    ],
)
def test_classify_real_429_rate_limit(message, monkeypatch):
    monkeypatch.setattr(CopilotLLM, "_get_node_id", lambda self: "test-node")
    with pytest.raises(RateLimitError):
        CopilotLLM()._classify_error(_cli(returncode=1, stderr=message))


@pytest.mark.parametrize(
    "message",
    [
        "80000460:\t429000ef\tjal 80001088 <keyu>",
        "lw t1,1440(gp) # 80004298 <__global_pointer$+0x5a0>",
        "[Metadata]\nInput tokens: 436037 | Total tokens: 442985",
    ],
)
def test_classify_success_with_incidental_429_text(message):
    CopilotLLM()._classify_error(_cli(returncode=0, stream_result=message))


@pytest.mark.parametrize(
    ("message", "error_cls", "returncode"),
    [
        ("429 rate limit", RateLimitError, 0),
        ("not logged in: run copilot login", AuthenticationError, 1),
        ("payment required: add credit", BillingError, 1),
        ('Model "nope" from --model flag is not available.', InvalidRequestError, 1),
        ("503 service unavailable", ServerError, 1),
        ("max output token limit reached", MaxOutputTokensError, 1),
        ("something surprising", UnknownCopilotError, 1),
    ],
)
def test_classify_errors(message, error_cls, returncode, monkeypatch):
    monkeypatch.setattr(CopilotLLM, "_get_node_id", lambda self: "test-node")
    with pytest.raises(error_cls):
        CopilotLLM()._classify_error(_cli(returncode=returncode, stderr=message))


def test_prompt_preserves_final_retry_error(monkeypatch):
    _disable_profiler(monkeypatch)
    monkeypatch.setattr(CopilotLLM, "_get_node_id", lambda self: "test-node")
    calls = 0

    def fake_run_copilot(self, user_message, tools):
        nonlocal calls
        calls += 1
        return _cli(returncode=1, stderr="something surprising")

    monkeypatch.setattr(CopilotLLM, "_run_copilot", fake_run_copilot)
    cli = CopilotLLM(retries=2).prompt("hello", tools=[])

    assert calls == 2
    assert cli.success is False
    assert cli.returncode == -1
    assert "UnknownCopilotError" in cli.stderr
    assert "something surprising" in cli.stderr


live = pytest.mark.skipif(
    os.environ.get("COPILOT_LIVE_TEST") != "1" or not shutil.which("copilot"),
    reason="set COPILOT_LIVE_TEST=1 and authenticate copilot to run live tests",
)


@live
def test_live_copilot_simple_prompt():
    llm = CopilotLLM(
        system_message="You answer with a single word and nothing else.",
        timeout_seconds=180,
        allow_all=False,
    )
    cli = llm.prompt("Reply with exactly the word: PONG", tools=[])
    assert cli.success is True
    assert "PONG" in cli.result.upper()


# ---------------------------------------------------------------------------
# Permission controls (live): copilot's allow_all is mirrored onto the
# canonical dangerously_skip_permissions flag; copilot has no opencode-style
# `permission` block. Gated by COPILOT_LIVE_TEST=1 + the binary.
# ---------------------------------------------------------------------------


@live
def test_live_copilot_allow_all_mirrors_skip_permissions_and_runs():
    llm = CopilotLLM(
        system_message="You answer with a single word and nothing else.",
        timeout_seconds=180,
        allow_all=True,
    )
    assert llm.dangerously_skip_permissions is True  # mirrored from allow_all
    cli = llm.prompt("Reply with exactly the word: PONG", tools=[])
    assert cli.success is True
    assert "PONG" in cli.result.upper()


@live
def test_live_copilot_permission_arg_warns_but_still_runs():
    with pytest.warns(UserWarning, match="does not support a 'config'"):
        llm = CopilotLLM(
            system_message="You answer with a single word and nothing else.",
            timeout_seconds=180,
            allow_all=False,
            config={"bash": "allow"},
        )
    cli = llm.prompt("Reply with exactly the word: PONG", tools=[])
    assert cli.success is True
    assert "PONG" in cli.result.upper()


# ---------------------------------------------------------------------------
# Permission controls (live_remote): dispatch onto a real copilot_creds worker
# so the allow-all flag applies inside the worker container.
# ---------------------------------------------------------------------------


# Worker for this test: `chia up chia/models/tests/cluster/all_models.yaml`
# (advertises copilot_creds); the remote_prompt fixture skips if it's absent.
@pytest.mark.live_remote
def test_live_remote_copilot_allow_all_runs(remote_prompt):
    llm = CopilotLLM(
        system_message="You answer with a single word and nothing else.",
        timeout_seconds=180,
        allow_all=True,
    )
    assert llm.dangerously_skip_permissions is True  # mirrored from allow_all
    cli = remote_prompt(llm, "Reply with exactly the word: PONG", "copilot_creds")
    assert cli.success is True
    assert "PONG" in cli.result.upper()
