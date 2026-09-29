"""Offline tests for :class:`chia.models.codex.CodexLLM`.

Set ``CODEX_LIVE_TEST=1`` to run the opt-in live smoke test against an
authenticated local Codex CLI.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tarfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timezone
from io import BytesIO
from types import SimpleNamespace

import pytest
from ray import cloudpickle

from chia.models import codex as codex_mod
from chia.models.codex import (
    AuthenticationError,
    BillingError,
    CodexQueryResult,
    CodexTerminalOutcome,
    CodexLLM,
    InvalidRequestError,
    MaxOutputTokensError,
    ModelCapacityError,
    RateLimitError,
    ServerError,
    UnknownCodexError,
    parse_session_id,
    parse_rate_limit_reset,
)

_SESSION_ID = "123e4567-e89b-12d3-a456-426614174000"
_CAPACITY_MESSAGE = "Selected model is at capacity. Please try a different model."
_CONTINUATION_MESSAGE = (
    "Continue where you left off. Do not repeat work you already completed."
)


def _event(event_type, **kwargs):
    return json.dumps({"type": event_type, **kwargs})


def _cli(
    returncode=1,
    stderr="",
    result="",
    stream_result="",
    *,
    terminal_status=None,
    terminal_message="",
    session_id=None,
):
    return CodexQueryResult(
        result,
        returncode,
        stderr,
        stream_result,
        session_id=session_id,
        terminal_outcome=CodexTerminalOutcome(
            terminal_status or ("completed" if returncode == 0 else "failed"),
            terminal_message,
        ),
    )


def _make_session_bundle(
    manifest: dict,
    files: tuple[tuple[str, bytes], ...] = (),
) -> bytes:
    buf = BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        entries = (
            (
                codex_mod._CODEX_SESSION_MANIFEST,
                json.dumps(manifest).encode("utf-8"),
            ),
            *files,
        )
        for rel_path, content in entries:
            info = tarfile.TarInfo(rel_path)
            info.size = len(content)
            tar.addfile(info, BytesIO(content))
    return buf.getvalue()


def _fake_subprocess(monkeypatch, capture, *, stdout="", stderr="", returncode=0, final="PONG"):
    def fake_run(cmd, **kwargs):
        capture.update(cmd=cmd, kwargs=kwargs)
        path = cmd[cmd.index("--output-last-message") + 1]
        capture["output_last_message"] = path
        with open(path, "w") as f:
            f.write(final)
        lifecycle = "\n".join((_event("turn.started"), _event("turn.completed")))
        return SimpleNamespace(
            returncode=returncode,
            stdout="\n".join(part for part in (stdout, lifecycle) if part),
            stderr=stderr,
        )

    monkeypatch.setattr(codex_mod.subprocess, "run", fake_run)


def _disable_profiler(monkeypatch):
    import chia.trace.profiler as profiler_mod

    monkeypatch.setattr(
        profiler_mod,
        "get_profiler",
        lambda: SimpleNamespace(enabled=False, add_info=lambda _info: None),
    )


def _tool(name="calc", *methods):
    manager = SimpleNamespace(
        list_tools=lambda: [SimpleNamespace(name=method) for method in methods]
    )
    return SimpleNamespace(
        name=name,
        hostname="localhost",
        port=9001,
        mcp=SimpleNamespace(_tool_manager=manager),
    )


def test_constructor_and_chia_surface(caplog):
    with caplog.at_level("INFO", logger="codex"):
        llm = CodexLLM()
    assert llm.model is None
    assert llm.codex_bin == "codex"
    assert llm.allow_builtin_tools is True
    assert llm.sandbox == "read-only"
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
    tool = _tool(tool_name, "add", "subtract")
    assert CodexLLM()._mcp_config_args([tool]) == [
        "-c",
        expected,
        "-c",
        expected.split(".url=")[0] + ".enabled=true",
        "-c",
        expected.split(".url=")[0] + '.default_tools_approval_mode="approve"',
        "-c",
        expected.split(".url=")[0] + '.enabled_tools=["add", "subtract"]',
    ]


@pytest.mark.parametrize("hostname,expected_host", [
    ("100.113.38.3", "127.0.0.2"),
    ("localhost", "localhost"),
])
def test_mcp_config_uses_worker_tunnel_relay(monkeypatch, hostname, expected_host):
    monkeypatch.setenv("CHIA_TOOL_ADVERTISE_HOST", "100.113.38.3")
    monkeypatch.setenv("CHIA_TOOL_RELAY_HOST", "127.0.0.2")
    tool = _tool("workspace", "run_command")
    tool.hostname = hostname
    args = CodexLLM()._mcp_config_args([tool])
    assert args[1] == f'mcp_servers.workspace.url="http://{expected_host}:9001/workspace/mcp"'


def test_build_cmd_flags_and_reasoning_effort():
    llm = CodexLLM(
        model="gpt-test",
        work_dir="/tmp/work",
        ephemeral=True,
        reasoning_effort="xhigh",
        allow_builtin_tools=False,
    )
    cmd = llm._build_cmd(output_last_message_path="/tmp/out.txt")
    assert cmd[0] == "codex"
    assert cmd[cmd.index("--model") + 1] == "gpt-test"
    assert cmd[cmd.index("--cd") + 1] != "/tmp/work"
    assert cmd[cmd.index("--output-last-message") + 1] == "/tmp/out.txt"
    assert "--skip-git-repo-check" in cmd
    assert "--ephemeral" in cmd
    assert "--dangerously-bypass-approvals-and-sandbox" not in cmd
    assert cmd.count("--ignore-rules") == 1
    for feature in codex_mod._RESTRICTED_CODEX_FEATURES:
        assert ["--disable", feature] == cmd[
            cmd.index(feature) - 1 : cmd.index(feature) + 1
        ]
    assert 'model_reasoning_effort="xhigh"' in cmd
    assert cmd[-1] == "-"


def test_build_cmd_resume_flags_and_reasoning_effort():
    session_id = _SESSION_ID
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
    assert cmd[cmd.index("exec") : cmd.index("exec") + 3] == [
        "exec", "resume", "--json"
    ]
    assert "--color" not in cmd
    assert "--cd" not in cmd
    assert cmd[cmd.index("--model") + 1] == "gpt-test"
    assert cmd[cmd.index("--output-last-message") + 1] == "/tmp/out.txt"
    assert "--skip-git-repo-check" in cmd
    assert "--ephemeral" in cmd
    assert "--dangerously-bypass-approvals-and-sandbox" not in cmd
    assert "--ignore-user-config" not in cmd
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


def test_restricted_mcp_tools_enable_network_without_builtin_tools():
    cmd = CodexLLM(allow_builtin_tools=False)._build_cmd(
        [_tool("allowed", "read")]
    )

    assert cmd[cmd.index("--sandbox") + 1] == "danger-full-access"
    assert "--dangerously-bypass-approvals-and-sandbox" not in cmd
    assert "--disable" in cmd
    assert 'mcp_servers.allowed.enabled_tools=["read"]' in cmd


def test_default_mode_preserves_work_dir_and_builtin_tools():
    cmd = CodexLLM(
        work_dir="/tmp/work",
        dangerously_bypass_approvals_and_sandbox=True,
        ignore_rules=True,
    )._build_cmd()
    assert cmd[cmd.index("--cd") + 1] == "/tmp/work"
    assert "--dangerously-bypass-approvals-and-sandbox" in cmd
    assert "--ignore-user-config" not in cmd
    assert "--disable" not in cmd
    assert cmd.count("--ignore-rules") == 1


def test_restricted_mode_rejects_permission_bypass_and_profile():
    with pytest.raises(ValueError, match="allow_builtin_tools=True"):
        CodexLLM(
            allow_builtin_tools=False,
            dangerously_bypass_approvals_and_sandbox=True,
        )
    with pytest.raises(ValueError, match="profile requires"):
        CodexLLM(allow_builtin_tools=False, profile="unsafe")


def test_restricted_mode_disables_configured_mcp_servers(monkeypatch, tmp_path):
    (tmp_path / "config.toml").write_text(
        '[mcp_servers."ambient.server"]\nurl = "http://localhost:9999/mcp"\n'
    )
    monkeypatch.setenv("CODEX_HOME", str(tmp_path))

    cmd = CodexLLM(allow_builtin_tools=False)._build_cmd(
        [_tool("allowed", "read")]
    )

    assert 'mcp_servers."ambient.server".enabled=false' in cmd
    assert "mcp_servers.allowed.enabled=true" in cmd
    assert 'mcp_servers.allowed.default_tools_approval_mode="approve"' in cmd
    assert 'mcp_servers.allowed.enabled_tools=["read"]' in cmd


def test_parse_rate_limit_reset():
    reset = parse_rate_limit_reset("usage limit - resets 4pm (America/Los_Angeles)")
    assert reset is not None
    assert reset.tzinfo == timezone.utc


def test_parse_session_id_from_jsonl():
    session_id = _SESSION_ID
    stdout = "\n".join([
        _event("turn_start"),
        json.dumps({"type": "session_configured", "session_id": session_id}),
    ])
    assert parse_session_id(stdout) == session_id


def test_parse_session_id_nested_and_regex_fallback():
    session_id = _SESSION_ID
    assert parse_session_id(json.dumps({"payload": {"conversationId": session_id}})) == session_id
    assert parse_session_id(f"created session {session_id}") == session_id
    assert parse_session_id(f"plain uuid {session_id}") is None


def test_parse_jsonl_stream_response_tool_usage_and_stderr():
    stdout = "\n".join([
        _event("assistant_message", message={"content": "hello"}),
        json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": " pong"}}),
        _event("tool_call", name="calc", arguments={"x": 1}),
        _event("tool_result", output="2"),
        _event("turn.completed", usage={"input_tokens": 3, "output_tokens": 5}),
        "not-json",
    ])
    stream, meta, fallback, outcome = CodexLLM._parse_jsonl_stream(stdout, "stderr text")
    assert "[Response]\nhello" in stream
    assert "[Response]\n pong" in stream
    assert "[Tool Call: calc]" in stream
    assert 'Args: {"x": 1}' in stream
    assert "[Tool Result]\n2" in stream
    assert "[UNPARSED]" in stream
    assert "[stderr]\nstderr text" in stream
    assert meta == {
        "num_turns": 1,
        "input_tokens": 3,
        "output_tokens": 5,
        "usage_source": "turn_usage",
    }
    assert fallback == "hello pong"
    assert outcome == CodexTerminalOutcome("completed")


def test_terminal_outcome_ignores_nonfatal_error_items():
    stdout = "\n".join([
        _event("turn.started"),
        json.dumps({
            "type": "item.completed",
            "item": {
                "type": "error",
                "message": "HTTP 400 and max output token limit reached",
            },
        }),
        _event("turn.completed"),
    ])

    _stream, _meta, _fallback, outcome = CodexLLM._parse_jsonl_stream(stdout)

    assert outcome == CodexTerminalOutcome("completed")


def test_terminal_outcome_treats_error_then_turn_failed_as_one_failure():
    message = json.dumps({
        "type": "error",
        "status": 400,
        "error": {
            "type": "invalid_request_error",
            "message": "unsupported model",
        },
    })
    stdout = "\n".join([
        _event("turn.started"),
        _event("error", message=message),
        _event("turn.failed", error={"message": message}),
    ])

    _stream, _meta, _fallback, outcome = CodexLLM._parse_jsonl_stream(stdout)

    assert outcome == CodexTerminalOutcome("failed", message)


def test_json_encoded_turn_failure_is_classified(monkeypatch):
    monkeypatch.setattr(CodexLLM, "_get_node_id", lambda self: "test-node")
    message = json.dumps({
        "type": "error",
        "status": 400,
        "error": {
            "type": "invalid_request_error",
            "message": "The requested model is not supported.",
        },
    })

    with pytest.raises(InvalidRequestError):
        CodexLLM()._classify_error(
            _cli(
                returncode=1,
                terminal_status="failed",
                terminal_message=message,
            )
        )


def test_terminal_outcome_uses_only_latest_started_turn():
    stdout = "\n".join([
        _event("turn.started"),
        _event("turn.failed", error={"message": "maximum output tokens reached"}),
        _event("turn.started"),
        _event("turn.completed"),
    ])

    _stream, _meta, _fallback, outcome = CodexLLM._parse_jsonl_stream(stdout)

    assert outcome == CodexTerminalOutcome("completed")


@pytest.mark.parametrize(
    ("stdout", "status"),
    [
        (_event("error", message="fatal stream failure"), "fatal"),
        (_event("thread.started", thread_id="thread-id"), "missing"),
        (
            "\n".join((
                _event("turn.started"),
                _event("turn.failed", error={"message": "first"}),
                _event("turn.completed"),
            )),
            "invalid",
        ),
    ],
)
def test_terminal_outcome_rejects_non_turn_success(stdout, status):
    _stream, _meta, _fallback, outcome = CodexLLM._parse_jsonl_stream(stdout)
    assert outcome.status == status


def test_parse_jsonl_stream_charges_only_latest_replayed_task():
    def token_event(last, total):
        return json.dumps({
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {
                    "last_token_usage": last,
                    "total_token_usage": total,
                },
            },
        })

    stdout = "\n".join([
        _event("task_started"),
        token_event(
            {
                "input_tokens": 100,
                "cached_input_tokens": 80,
                "output_tokens": 20,
                "reasoning_output_tokens": 10,
                "total_tokens": 120,
            },
            {
                "input_tokens": 100,
                "cached_input_tokens": 80,
                "output_tokens": 20,
                "reasoning_output_tokens": 10,
                "total_tokens": 120,
            },
        ),
        _event("task_complete"),
        _event("task_started"),
        token_event(
            {
                "input_tokens": 80,
                "cached_input_tokens": 60,
                "output_tokens": 10,
                "reasoning_output_tokens": 5,
                "total_tokens": 90,
            },
            {
                "input_tokens": 180,
                "cached_input_tokens": 140,
                "output_tokens": 30,
                "reasoning_output_tokens": 15,
                "total_tokens": 210,
            },
        ),
        token_event(
            {
                "input_tokens": 20,
                "cached_input_tokens": 16,
                "output_tokens": 5,
                "reasoning_output_tokens": 2,
                "total_tokens": 25,
            },
            {
                "input_tokens": 200,
                "cached_input_tokens": 156,
                "output_tokens": 35,
                "reasoning_output_tokens": 17,
                "total_tokens": 235,
            },
        ),
        _event("task_complete"),
    ])

    _stream, meta, _fallback, _outcome = CodexLLM._parse_jsonl_stream(
        stdout,
        resumed=True,
    )

    assert meta["usage_source"] == "session_total_delta"
    assert meta["input_tokens"] == 100
    assert meta["output_tokens"] == 15
    assert meta["cache_read_input_tokens"] == 76
    assert meta["reasoning_tokens"] == 7
    assert meta["total_tokens"] == 115
    assert meta["session_total_usage"]["total_tokens"] == 235


def test_resumed_stream_without_history_uses_latest_task_snapshots():
    def token_event(input_tokens, output_tokens):
        return json.dumps({
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {
                    "last_token_usage": {
                        "input_tokens": input_tokens,
                        "output_tokens": output_tokens,
                        "total_tokens": input_tokens + output_tokens,
                    },
                    "total_token_usage": {
                        "input_tokens": 1000 + input_tokens,
                        "output_tokens": 100 + output_tokens,
                        "total_tokens": 1100 + input_tokens + output_tokens,
                    },
                },
            },
        })

    stdout = "\n".join([
        _event("task_started"),
        token_event(30, 4),
        token_event(40, 6),
        _event("task_complete"),
    ])

    _stream, meta, _fallback, _outcome = CodexLLM._parse_jsonl_stream(
        stdout,
        resumed=True,
    )

    assert meta["usage_source"] == "latest_task_usage"
    assert meta["input_tokens"] == 70
    assert meta["output_tokens"] == 10
    assert meta["total_tokens"] == 80


def test_context_metadata_normalizes_rollout_compaction_sequence():
    def token_count(input_tokens, total_tokens, *, window=258400):
        return {
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {
                    "last_token_usage": {
                        "input_tokens": input_tokens,
                        "cached_input_tokens": 0,
                        "output_tokens": 0,
                        "reasoning_output_tokens": 0,
                        "total_tokens": total_tokens,
                    },
                    "model_context_window": window,
                },
            },
        }

    events = [
        token_count(236091, 236581),
        {"type": "compacted", "payload": {"window_number": 2}},
        token_count(0, 13065),
        {"type": "event_msg", "payload": {"type": "context_compacted"}},
        token_count(20373, 20433),
    ]

    metadata = CodexLLM._context_metadata_from_rollout_events(events)

    assert metadata == {
        "context_tokens": 20373,
        "peak_context_tokens": 236091,
        "model_context_window": 258400,
        "context_window_utilization": 20373 / 258400,
        "context_compactions": [{
            "trigger": None,
            "before_tokens": 236091,
            "after_tokens": 13065,
        }],
    }


def test_prompt_routes_to_run_codex(monkeypatch):
    _disable_profiler(monkeypatch)
    llm = CodexLLM()
    sentinel = _cli(returncode=0, result="X")
    monkeypatch.setattr(llm, "_run_codex", lambda user, tools: sentinel)
    out = llm.prompt("hi", tools=[])
    assert out is sentinel
    assert out.success is True
    assert llm._last_metadata["model"] == "codex-default"


def test_sync_session_copies_session_state_to_local_instance():
    session_id = _SESSION_ID
    llm = CodexLLM(resume_session=True)
    cli = CodexQueryResult(
        result="ok",
        returncode=0,
        stderr="",
        stream_result="",
        session_id=session_id,
        session_bundle=b"bundle",
        session_bundle_paths=("state_5.sqlite",),
    )

    assert llm._sync_session(cli) is cli
    assert llm._session_id == session_id
    assert llm._session_bundle == b"bundle"
    assert llm._session_bundle_paths == ("state_5.sqlite",)


def test_resume_session_first_call_records_id_and_second_call_resumes(monkeypatch, tmp_path):
    _disable_profiler(monkeypatch)
    session_id = _SESSION_ID
    captures = []
    session_homes = []

    def fake_run(cmd, **kwargs):
        captures.append(cmd)
        session_home = kwargs["env"]["CODEX_HOME"]
        session_homes.append(session_home)
        path = cmd[cmd.index("--output-last-message") + 1]
        with open(path, "w") as f:
            f.write(f"OK{len(captures)}")
        if len(captures) == 1:
            rollout_dir = os.path.join(session_home, "sessions", "2026", "07", "18")
            os.makedirs(rollout_dir)
            rollout_path = os.path.join(
                rollout_dir, f"rollout-first-{session_id}.jsonl"
            )
            with open(rollout_path, "w") as f:
                f.write("FIRST")
            with sqlite3.connect(os.path.join(session_home, "state_5.sqlite")) as db:
                db.execute("CREATE TABLE threads (rollout_path TEXT)")
                db.execute("INSERT INTO threads VALUES (?)", (rollout_path,))
            with sqlite3.connect(os.path.join(session_home, "goals_1.sqlite")) as db:
                db.execute("CREATE TABLE goals (name TEXT)")
                db.execute("INSERT INTO goals VALUES ('measure')")
            stdout = "\n".join((
                json.dumps({"type": "session_configured", "session_id": session_id}),
                _event("turn.started"),
                _event("turn.completed"),
            ))
        else:
            assert os.path.exists(os.path.join(session_home, "state_5.sqlite"))
            assert not os.path.exists(os.path.join(session_home, "goals_1.sqlite"))
            restored_rollout = os.path.join(
                session_home,
                "sessions",
                "2026",
                "07",
                "18",
                f"rollout-first-{session_id}.jsonl",
            )
            with open(restored_rollout) as f:
                assert f.read() == "FIRST"
            with sqlite3.connect(os.path.join(session_home, "state_5.sqlite")) as db:
                assert db.execute(
                    "SELECT rollout_path FROM threads"
                ).fetchone() == (restored_rollout,)
            stdout = "\n".join((_event("turn.started"), _event("turn.completed")))
        return SimpleNamespace(returncode=0, stdout=stdout, stderr="")

    monkeypatch.setenv("CODEX_HOME", str(tmp_path))
    monkeypatch.setenv("CHIA_CODEX_SESSION_ROOT", str(tmp_path / "session-root"))
    monkeypatch.setattr(codex_mod.subprocess, "run", fake_run)

    llm = CodexLLM(resume_session=True)
    cli1 = llm.prompt("first", tools=[])
    assert cli1.success is True
    assert cli1.session_id == session_id
    assert cli1.session_bundle
    assert codex_mod._CODEX_SESSION_MANIFEST in cli1.session_bundle_paths
    assert "state_5.sqlite" in cli1.session_bundle_paths
    assert "goals_1.sqlite" not in cli1.session_bundle_paths
    assert "state_5.sqlite-wal" not in cli1.session_bundle_paths
    assert "state_5.sqlite-shm" not in cli1.session_bundle_paths
    assert not llm._is_session_bundle_file("goals_1.sqlite")
    assert not llm._is_session_bundle_file("state_5.sqlite-wal")
    assert not llm._is_session_bundle_file("state_5.sqlite-shm")
    assert f"sessions/2026/07/18/rollout-first-{session_id}.jsonl" in cli1.session_bundle_paths
    assert captures[0][captures[0].index("exec") :][:2] == ["exec", "--json"]
    assert not os.path.exists(session_homes[0])

    with tarfile.open(fileobj=BytesIO(cli1.session_bundle), mode="r:gz") as tar:
        manifest = json.loads(tar.extractfile(codex_mod._CODEX_SESSION_MANIFEST).read())
    assert manifest["session_id"] == session_id
    assert manifest["version"] == 2
    assert manifest["session_storage_key"] == llm._session_storage_key
    assert manifest["session_home"] == session_homes[0]

    llm2 = CodexLLM(resume_session=True)
    llm2._session_bundle = cli1.session_bundle
    cli2 = llm2.prompt("second", tools=[])
    assert cli2.success is True
    assert cli2.session_id == session_id
    assert captures[1][captures[1].index("exec") :][:3] == [
        "exec", "resume", "--json"
    ]
    assert captures[1][-2:] == [session_id, "-"]
    assert session_homes[1] == session_homes[0]
    assert not os.path.exists(session_homes[1])


def test_restore_session_bundle_ignores_foreign_and_unsafe_members(monkeypatch, tmp_path):
    session_id = _SESSION_ID
    session_root = tmp_path / "session-root"
    session_home = session_root / "test-session"
    session_home.mkdir(parents=True)
    bundle = _make_session_bundle(
        {
            "version": 2,
            "session_id": session_id,
            "session_storage_key": "test-session",
            "session_home": str(session_home),
        },
        (
            ("state_5.sqlite", b"state"),
            (f"sessions/2026/07/18/rollout-first-{session_id}.jsonl", b"rollout"),
            ("auth.json", b"bad"),
            ("sessions/../config.toml", b"bad"),
            ("../escape.txt", b"bad"),
        ),
    )

    monkeypatch.setenv("CHIA_CODEX_SESSION_ROOT", str(session_root))
    llm = CodexLLM(resume_session=True)
    llm._restore_session_bundle(str(session_home), bundle)

    assert llm._session_id == session_id
    assert (session_home / "state_5.sqlite").read_bytes() == b"state"
    assert (
        session_home
        / "sessions"
        / "2026"
        / "07"
        / "18"
        / f"rollout-first-{session_id}.jsonl"
    ).read_bytes() == b"rollout"
    assert not (session_home / "auth.json").exists()
    assert not (session_home / "config.toml").exists()
    assert not (tmp_path.parent / "escape.txt").exists()


def test_prepare_session_home_cleans_up_if_bundle_restore_fails(monkeypatch, tmp_path):
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "base"))
    monkeypatch.setenv("CHIA_CODEX_SESSION_ROOT", str(tmp_path / "session-root"))

    llm = CodexLLM(resume_session=True, session_storage_key="broken-session")
    llm._session_bundle = b"not a gzip tar"

    with pytest.raises(tarfile.ReadError):
        llm._prepare_session_home()

    assert not (tmp_path / "session-root" / "broken-session").exists()


def test_restore_rejects_legacy_bundle_without_stable_home_key(tmp_path):
    bundle = _make_session_bundle(
        {
            "version": 1,
            "session_id": _SESSION_ID,
        }
    )

    with pytest.raises(ValueError, match="unsupported Codex session bundle version"):
        CodexLLM(resume_session=True)._restore_session_bundle(
            str(tmp_path), bundle
        )


def test_session_root_creation_is_safe_under_concurrent_first_use(monkeypatch, tmp_path):
    session_root = tmp_path / "session-root"
    monkeypatch.setenv("CHIA_CODEX_SESSION_ROOT", str(session_root))
    llms = [CodexLLM(resume_session=True) for _ in range(8)]

    with ThreadPoolExecutor(max_workers=len(llms)) as executor:
        roots = list(executor.map(lambda llm: llm._session_root(), llms))

    assert roots == [str(session_root)] * len(llms)
    assert session_root.stat().st_mode & 0o077 == 0


def test_session_lock_rejects_overlapping_calls(monkeypatch, tmp_path):
    monkeypatch.setenv("CHIA_CODEX_SESSION_ROOT", str(tmp_path / "session-root"))
    first = CodexLLM(resume_session=True, session_storage_key="same-session")
    second = CodexLLM(resume_session=True, session_storage_key="same-session")

    lock_fd = first._acquire_session_lock()
    try:
        with pytest.raises(BlockingIOError):
            second._acquire_session_lock()
    finally:
        first._release_session_lock(lock_fd)


def test_restore_rejects_worker_with_different_absolute_session_root(
    monkeypatch, tmp_path
):
    original_home = tmp_path / "worker-a" / "same-session"
    bundle = _make_session_bundle(
        {
            "version": 2,
            "session_id": _SESSION_ID,
            "session_storage_key": "same-session",
            "session_home": str(original_home),
        }
    )

    monkeypatch.setenv("CHIA_CODEX_SESSION_ROOT", str(tmp_path / "worker-b"))
    llm = CodexLLM(resume_session=True)
    llm._session_bundle = bundle

    with pytest.raises(ValueError, match="different absolute session home"):
        llm._prepare_session_home()


def test_capture_snapshots_committed_wal_without_wal_or_shm(monkeypatch, tmp_path):
    session_root = tmp_path / "session-root"
    session_home = session_root / "wal-session"
    session_home.mkdir(parents=True)
    monkeypatch.setenv("CHIA_CODEX_SESSION_ROOT", str(session_root))
    state_path = session_home / "state_5.sqlite"
    writer = sqlite3.connect(state_path)
    try:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("CREATE TABLE threads (value TEXT)")
        writer.execute("INSERT INTO threads VALUES ('persisted')")
        writer.commit()
        assert (session_home / "state_5.sqlite-wal").exists()

        llm = CodexLLM(
            resume_session=True,
            session_storage_key="wal-session",
        )
        llm._session_id = _SESSION_ID
        llm._capture_session_bundle(str(session_home))
    finally:
        writer.close()

    with tarfile.open(fileobj=BytesIO(llm._session_bundle), mode="r:gz") as tar:
        names = tar.getnames()
        snapshot_path = tmp_path / "snapshot.sqlite"
        snapshot_path.write_bytes(tar.extractfile("state_5.sqlite").read())
    assert "state_5.sqlite-wal" not in names
    assert "state_5.sqlite-shm" not in names
    with sqlite3.connect(snapshot_path) as snapshot:
        assert snapshot.execute("SELECT value FROM threads").fetchone() == (
            "persisted",
        )


def test_seed_session_home_rewrites_marketplace_without_copying_static_dirs(tmp_path):
    base_home = tmp_path / "base"
    session_home = tmp_path / "session"
    stale_source = "/home/eecs/wangalan/.codex/local-marketplaces/caveman"
    marketplace = base_home / "local-marketplaces" / "caveman"
    plugin_cache = base_home / "plugins" / "cache" / "local-caveman"
    skills = base_home / "skills"
    marketplace.mkdir(parents=True)
    plugin_cache.mkdir(parents=True)
    skills.mkdir(parents=True)
    session_home.mkdir()
    (base_home / "auth.json").write_text("{}")
    (base_home / "AGENTS.md").write_text("base rules")
    (base_home / "config.toml").write_text(
        "\n".join([
            "[marketplaces.local-caveman]",
            'source_type = "local"',
            f"source = {json.dumps(stale_source)}",
            "",
            '[plugins."caveman@local-caveman"]',
            "enabled = true",
            "",
        ])
    )

    CodexLLM(resume_session=True)._seed_session_home(str(base_home), str(session_home))

    rewritten = (session_home / "config.toml").read_text()
    assert f"source = {json.dumps(str(marketplace))}" in rewritten
    assert (session_home / "auth.json").exists()
    assert (session_home / "AGENTS.md").exists()
    assert not (session_home / "local-marketplaces").exists()
    assert not (session_home / "plugins").exists()
    assert not (session_home / "skills").exists()


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


def test_run_codex_merges_rollout_context_metadata(monkeypatch, tmp_path):
    session_id = _SESSION_ID
    capture = {}

    def fake_run(cmd, **kwargs):
        path = cmd[cmd.index("--output-last-message") + 1]
        with open(path, "w") as output:
            output.write("PONG")
        rollout_dir = tmp_path / "sessions" / "2026" / "07" / "29"
        rollout_dir.mkdir(parents=True)
        rollout = rollout_dir / f"rollout-test-{session_id}.jsonl"
        rollout.write_text(json.dumps({
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {
                    "last_token_usage": {
                        "input_tokens": 120,
                        "cached_input_tokens": 100,
                        "output_tokens": 8,
                        "reasoning_output_tokens": 3,
                        "total_tokens": 128,
                    },
                    "model_context_window": 1000,
                },
            },
        }) + "\n")
        capture["cmd"] = cmd
        return SimpleNamespace(
            returncode=0,
            stdout="\n".join((
                json.dumps({
                    "type": "session_configured",
                    "session_id": session_id,
                }),
                _event("turn.started"),
                _event("turn.completed"),
            )),
            stderr="",
        )

    monkeypatch.setenv("CODEX_HOME", str(tmp_path))
    monkeypatch.setattr(codex_mod.subprocess, "run", fake_run)
    llm = CodexLLM()

    cli = llm._run_codex("say pong", tools=[])

    assert cli.result == "PONG"
    assert llm._last_metadata["context_tokens"] == 120
    assert llm._last_metadata["peak_context_tokens"] == 120
    assert llm._last_metadata["model_context_window"] == 1000
    assert llm._last_metadata["context_window_utilization"] == 0.12
    assert llm._last_metadata["context_compactions"] == []


def test_run_codex_fallback_and_mcp_config(monkeypatch):
    capture = {}
    _fake_subprocess(
        monkeypatch,
        capture,
        stdout=_event("assistant_message", message={"content": "fallback"}),
        final="",
    )
    tool = _tool("calc", "add")
    cli = CodexLLM()._run_codex("use calc", tools=[tool])
    assert cli.result == "fallback"
    assert 'mcp_servers.calc.url="http://localhost:9001/calc/mcp"' in capture["cmd"]
    assert "mcp_servers.calc.enabled=true" in capture["cmd"]
    assert 'mcp_servers.calc.enabled_tools=["add"]' in capture["cmd"]


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
        CodexLLM()._classify_error(
            _cli(returncode=1, terminal_message=message)
        )


@pytest.mark.parametrize(
    "message",
    [
        "80000460:\t429000ef\tjal 80001088 <keyu>",
        "lw t1,1440(gp) # 80004298 <__global_pointer$+0x5a0>",
        "[Metadata]\nInput tokens: 436037 | Total tokens: 442985",
    ],
)
def test_classify_success_with_incidental_429_text(message):
    CodexLLM()._classify_error(
        _cli(returncode=0, result="PONG", stream_result=message)
    )


@pytest.mark.parametrize("terminal_status", ["failed", "fatal", "missing", "invalid"])
def test_classify_uses_successful_exit_and_final_message(terminal_status):
    CodexLLM()._classify_error(
        _cli(
            returncode=0,
            result="PONG",
            terminal_status=terminal_status,
            terminal_message="diagnostic event",
        )
    )


def test_classify_rejects_successful_exit_without_final_message(monkeypatch):
    monkeypatch.setattr(CodexLLM, "_get_node_id", lambda self: "test-node")

    with pytest.raises(UnknownCodexError, match="did not write a final message"):
        CodexLLM()._classify_error(
            _cli(returncode=0, result="", terminal_status="completed")
        )


def test_classification_never_uses_stderr_result_or_stream(monkeypatch):
    monkeypatch.setattr(CodexLLM, "_get_node_id", lambda self: "test-node")
    cli = _cli(
        returncode=1,
        stderr=f"HTTP 400 bad request; {_CAPACITY_MESSAGE}",
        result="maximum output token limit reached",
        stream_result="429 rate limit and 503 service unavailable",
        terminal_message="state db returned stale rollout path",
    )

    with pytest.raises(UnknownCodexError) as caught:
        CodexLLM()._classify_error(cli)

    assert caught.value.raw_message == "state db returned stale rollout path"


@pytest.mark.parametrize("status", ["fatal", "missing", "invalid"])
def test_non_turn_failure_is_always_unknown(status, monkeypatch):
    monkeypatch.setattr(CodexLLM, "_get_node_id", lambda self: "test-node")
    cli = _cli(
        returncode=1,
        terminal_status=status,
        terminal_message="maximum output token limit reached",
    )

    with pytest.raises(UnknownCodexError):
        CodexLLM()._classify_error(cli)


@pytest.mark.parametrize(
    "message",
    [
        _CAPACITY_MESSAGE,
        _CAPACITY_MESSAGE.upper(),
    ],
)
def test_classify_model_capacity_as_transient(message, monkeypatch):
    monkeypatch.setattr(CodexLLM, "_get_node_id", lambda self: "test-node")

    with pytest.raises(ModelCapacityError) as caught:
        CodexLLM()._classify_error(
            _cli(
                returncode=1,
                terminal_status="failed",
                terminal_message=message,
            )
        )

    assert caught.value.error_type == "model_capacity"
    assert caught.value.raw_message == message


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
        CodexLLM()._classify_error(
            _cli(
                returncode=returncode,
                terminal_status="failed",
                terminal_message=message,
            )
        )


def test_prompt_preserves_final_retry_error(monkeypatch):
    _disable_profiler(monkeypatch)
    monkeypatch.setattr(CodexLLM, "_get_node_id", lambda self: "test-node")
    calls = 0

    def fake_run_codex(self, user_message, tools):
        nonlocal calls
        calls += 1
        return _cli(
            returncode=1,
            terminal_message="something surprising",
        )

    monkeypatch.setattr(CodexLLM, "_run_codex", fake_run_codex)
    cli = CodexLLM(retries=2).prompt("hello", tools=[])

    assert calls == 2
    assert cli.success is False
    assert cli.returncode == -1
    assert "UnknownCodexError" in cli.stderr
    assert "something surprising" in cli.stderr


def test_prompt_server_backoff_does_not_sleep_after_final_attempt(monkeypatch):
    _disable_profiler(monkeypatch)
    monkeypatch.setattr(CodexLLM, "_get_node_id", lambda self: "test-node")
    calls = 0
    sleeps = []

    def fake_run_codex(self, user_message, tools):
        nonlocal calls
        calls += 1
        return _cli(returncode=1, terminal_message="503 service unavailable")

    monkeypatch.setattr(CodexLLM, "_run_codex", fake_run_codex)
    monkeypatch.setattr(time, "sleep", sleeps.append)

    cli = CodexLLM(retries=3).prompt("hello", tools=[])

    assert calls == 3
    assert sleeps == [5, 10]
    assert cli.success is False
    assert "ServerError" in cli.stderr


def test_prompt_capacity_backoff_is_capped_jittered_and_has_no_final_sleep(
    monkeypatch,
):
    _disable_profiler(monkeypatch)
    monkeypatch.setattr(CodexLLM, "_get_node_id", lambda self: "test-node")
    calls = 0
    sleeps = []
    uniform_bounds = []
    jitter_factors = iter((0.8, 1.0, 1.2))

    def fake_run_codex(self, user_message, tools):
        nonlocal calls
        calls += 1
        return _cli(
            returncode=1,
            terminal_message=_CAPACITY_MESSAGE,
        )

    def fake_uniform(low, high):
        uniform_bounds.append((low, high))
        return next(jitter_factors)

    monkeypatch.setattr(CodexLLM, "_run_codex", fake_run_codex)
    monkeypatch.setattr(codex_mod.random, "uniform", fake_uniform)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    llm = CodexLLM(
        retries=1,
        capacity_attempts=4,
        capacity_backoff_base_seconds=15,
        capacity_backoff_multiplier=2,
        capacity_backoff_max_seconds=40,
        capacity_backoff_jitter=0.2,
    )

    with pytest.raises(ModelCapacityError) as caught:
        llm.prompt("hello", tools=[])

    assert calls == 4
    assert sleeps == pytest.approx([12, 30, 48])
    assert uniform_bounds == pytest.approx([(0.8, 1.2)] * 3)
    assert caught.value.capacity_attempts == 4
    assert caught.value.retries_exhausted is True
    assert caught.value.usage_metadata["provider_attempts"] == 4


def test_prompt_reports_usage_for_each_retry_attempt(monkeypatch):
    _disable_profiler(monkeypatch)
    monkeypatch.setattr(CodexLLM, "_get_node_id", lambda self: "test-node")
    calls = 0

    prompts = []

    def fake_run_codex(self, user_message, tools, **kwargs):
        nonlocal calls
        calls += 1
        prompts.append((user_message, kwargs.get("resume_session_id")))
        self._last_metadata = {
            "input_tokens": 100 * calls,
            "output_tokens": 10 * calls,
            "usage_source": "turn_usage",
        }
        if calls == 1:
            return _cli(
                returncode=1,
                terminal_message="max output token limit reached",
                session_id=_SESSION_ID,
            )
        return _cli(returncode=0, result="PONG")

    monkeypatch.setattr(CodexLLM, "_run_codex", fake_run_codex)
    llm = CodexLLM(retries=5)

    cli = llm.prompt("hello", tools=[])

    assert cli.success is True
    assert calls == 2
    assert llm._last_metadata["provider_attempts"] == 2
    assert [
        attempt["metadata"]["input_tokens"]
        for attempt in llm._last_metadata["attempts"]
    ] == [100, 200]
    assert [attempt["success"] for attempt in llm._last_metadata["attempts"]] == [
        False,
        True,
    ]
    assert prompts == [
        ("hello", None),
        (
            _CONTINUATION_MESSAGE,
            _SESSION_ID,
        ),
    ]


def test_prompt_limits_max_output_to_two_continuations(monkeypatch):
    _disable_profiler(monkeypatch)
    monkeypatch.setattr(CodexLLM, "_get_node_id", lambda self: "test-node")
    prompts = []
    session_id = _SESSION_ID

    def fake_run_codex(self, user_message, tools, **kwargs):
        prompts.append((user_message, kwargs.get("resume_session_id")))
        return _cli(
            returncode=1,
            terminal_message="maximum output token limit reached",
            session_id=session_id,
        )

    monkeypatch.setattr(CodexLLM, "_run_codex", fake_run_codex)

    with pytest.raises(MaxOutputTokensError) as caught:
        CodexLLM(retries=1).prompt("hello", tools=[])

    assert prompts == [
        ("hello", None),
        (
            _CONTINUATION_MESSAGE,
            session_id,
        ),
        (
            _CONTINUATION_MESSAGE,
            session_id,
        ),
    ]
    assert caught.value.usage_metadata["provider_attempts"] == 3


def test_codex_error_pickle_preserves_usage_metadata():
    error = MaxOutputTokensError(
        node_id="test-node",
        exit_code=1,
        raw_message="max output token limit reached",
    )
    error.usage_metadata = {
        "provider_attempts": 1,
        "attempts": [{
            "attempt": 1,
            "success": False,
            "metadata": {"input_tokens": 100, "output_tokens": 20},
        }],
    }

    restored = cloudpickle.loads(cloudpickle.dumps(error))

    assert restored.usage_metadata == error.usage_metadata


def test_capacity_exhaustion_metadata_survives_ray_serialization():
    error = ModelCapacityError(
        node_id="test-node",
        exit_code=1,
        raw_message=_CAPACITY_MESSAGE,
    )
    error.capacity_attempts = 8
    error.retries_exhausted = True

    restored = cloudpickle.loads(cloudpickle.dumps(error))

    assert restored.error_type == "model_capacity"
    assert restored.capacity_attempts == 8
    assert restored.retries_exhausted is True


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


@live
def test_live_codex_restricted_cannot_read_caller_work_dir(tmp_path):
    marker = "CHIA_RESTRICTED_MARKER_7F2A"
    (tmp_path / "marker.txt").write_text(marker)
    llm = CodexLLM(
        system_message=(
            "Try to read marker.txt using an available tool. If no tool can read it, "
            "reply with exactly BLOCKED."
        ),
        work_dir=str(tmp_path),
        timeout_seconds=180,
        ephemeral=True,
        allow_builtin_tools=False,
    )

    cli = llm.prompt("Return the file contents or BLOCKED.", tools=[])

    assert cli.success is True
    assert marker not in cli.result
    assert "BLOCKED" in cli.result.upper()


# ---------------------------------------------------------------------------
# Permission controls (live): codex's dangerously_bypass_approvals_and_sandbox
# is mirrored onto the canonical dangerously_skip_permissions flag; codex has no
# opencode-style `permission` block. Gated by CODEX_LIVE_TEST=1 + the binary.
# ---------------------------------------------------------------------------


@live
def test_live_codex_bypass_mirrors_skip_permissions_and_runs():
    llm = CodexLLM(
        system_message="You answer with a single word and nothing else.",
        timeout_seconds=180,
        dangerously_bypass_approvals_and_sandbox=True,
        allow_builtin_tools=True,
        ephemeral=True,
    )
    assert llm.dangerously_skip_permissions is True  # mirrored from the bypass kwarg
    cli = llm.prompt("Reply with exactly the word: PONG", tools=[])
    assert cli.success is True
    assert "PONG" in cli.result.upper()


@live
def test_live_codex_permission_arg_warns_but_still_runs():
    with pytest.warns(UserWarning, match="does not support a 'config'"):
        llm = CodexLLM(
            system_message="You answer with a single word and nothing else.",
            timeout_seconds=180,
            dangerously_bypass_approvals_and_sandbox=False,
            sandbox="read-only",
            approval_policy="never",
            ephemeral=True,
            config={"bash": "allow"},
        )
    cli = llm.prompt("Reply with exactly the word: PONG", tools=[])
    assert cli.success is True
    assert "PONG" in cli.result.upper()


# ---------------------------------------------------------------------------
# Permission controls (live_remote): dispatch onto a real codex_creds worker so
# the bypass flag applies inside the worker container.
# ---------------------------------------------------------------------------


# Worker for this test: `chia up chia/models/tests/cluster/all_models.yaml`
# (advertises codex_creds); the remote_prompt fixture skips if it's absent.
@pytest.mark.live_remote
def test_live_remote_codex_bypass_runs(remote_prompt):
    llm = CodexLLM(
        system_message="You answer with a single word and nothing else.",
        timeout_seconds=180,
        dangerously_bypass_approvals_and_sandbox=True,
        allow_builtin_tools=True,
        ephemeral=True,
    )
    assert llm.dangerously_skip_permissions is True  # mirrored from the bypass kwarg
    cli = remote_prompt(llm, "Reply with exactly the word: PONG", "codex_creds")
    assert cli.success is True
    assert "PONG" in cli.result.upper()


def test_session_node_preference_tracks_actual_worker_and_is_per_instance():
    main = CodexLLM(resume_session=True)
    critic = CodexLLM(resume_session=True)
    assert main.node_affinity_options() == {}
    cli = _cli(returncode=0)
    cli.node_id = "a" * 56
    main._sync_session(cli)
    strategy = main.node_affinity_options()["scheduling_strategy"]
    assert strategy.node_id == "a" * 56
    assert strategy.soft is True
    assert critic.node_affinity_options() == {}
    # After fallback, subsequent calls prefer the actual replacement.
    cli.node_id = "b" * 56
    main._sync_session(cli)
    assert main.node_affinity_options()["scheduling_strategy"].node_id == "b" * 56
    stateless = CodexLLM()
    stateless._sync_session(cli)
    assert stateless.node_affinity_options() == {}


def test_remote_session_dispatch_applies_preference_and_respects_override():
    calls = []

    class Dispatch:
        def options(self, **options):
            calls.append(options)
            return self

        def chia_remote(self, *args, **kwargs):
            return "fake-ref"

    class Session(CodexLLM):
        prompt = codex_mod._session_tracked(Dispatch())

    llm = Session(resume_session=True)
    llm._last_node_id = "a" * 56
    llm.prompt.chia_remote(llm, "next turn")
    assert calls[-1]["scheduling_strategy"].node_id == "a" * 56
    llm.prompt.options(scheduling_strategy="SPREAD").chia_remote(llm, "override")
    assert calls[-1]["scheduling_strategy"] == "SPREAD"


def test_successful_prompt_tags_worker(monkeypatch):
    llm = CodexLLM(resume_session=True)
    monkeypatch.setattr(llm, "_run_codex", lambda *a, **kw: _cli(returncode=0, result="ok"))
    monkeypatch.setattr(llm, "_get_node_id", lambda: "a" * 56)
    assert llm.prompt("hello").node_id == "a" * 56
