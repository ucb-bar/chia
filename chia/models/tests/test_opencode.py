"""Tests for :class:`chia.models.opencode.OpenCodeLLM`.

Run from the repo root with the chia venv active::

    pytest chia/models/tests/test_opencode.py -v

Three layers, matching the other backends' tests:

* **Offline unit tests** — construction/warning, config building (agent prompt +
  remote MCP servers), run-command shaping, session-id parsing, export
  extraction, and error classification.
* **Mocked flow tests** — monkeypatch ``subprocess.run`` so the full
  run -> export flow executes offline: the ``run`` call returns a ``step_start``
  line carrying a session id, the ``export`` call returns a session JSON, and we
  assert the assembled :class:`QueryResult` + metadata + the actual commands/config.
* **Live tests** — skipped unless the ``opencode`` binary is resolvable (set
  ``OPENCODE_BIN`` to your install, or have ``opencode`` on PATH). They actually
  shell out to opencode, so they need opencode auth configured. The model
  defaults to ``opencode/big-pickle``; override with ``OPENCODE_TEST_MODEL``.
  E.g.::

      OPENCODE_BIN=/path/to/opencode \
          pytest chia/models/tests/test_opencode.py -k live -v
"""

from __future__ import annotations

import json
import os
import pathlib
import shutil
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from chia.models import opencode as oc_mod
from chia.models.opencode import (
    AuthenticationError,
    BillingError,
    QueryResult,
    InvalidRequestError,
    MaxOutputTokensError,
    OpenCodeError,
    OpenCodeLLM,
    RateLimitError,
    ServerError,
    UnknownOpenCodeError,
    parse_run_error,
    parse_session_id,
)

FIXTURE_DIR = pathlib.Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Fakes — a stand-in subprocess.run distinguishing `run` from `export`
# ---------------------------------------------------------------------------


def _step_start(session_id="ses_test123"):
    return json.dumps({
        "type": "step_start",
        "sessionID": session_id,
        "part": {"id": "prt_x", "messageID": "msg_x",
                 "sessionID": session_id, "type": "step-start"},
    })


def _export_obj(text="PONG", tokens=None, cost=0.0026, tool=None):
    tokens = tokens or {"input": 3, "output": 6, "reasoning": 0,
                        "cache": {"read": 8280, "write": 0}}
    parts = [{"type": "step-start"}]
    if tool is not None:
        parts.append({"type": "tool", "tool": tool,
                      "state": {"status": "completed",
                                "input": {"command": "echo hi"},
                                "output": "hi"}})
    parts.append({"type": "text", "text": text})
    parts.append({"type": "step-finish", "tokens": tokens, "cost": cost})
    return {
        "info": {"id": "ses_test123"},
        "messages": [
            {"info": {"role": "user"}, "parts": [{"type": "text", "text": "hi"}]},
            {"info": {"role": "assistant", "tokens": tokens, "cost": cost},
             "parts": parts},
        ],
    }


def _export_with_error(name="APIError", message="", status_code=None,
                       is_retryable=False, headers=None, text="", tokens=None,
                       cost=0.0):
    """Build an export dict whose assistant message carries a structured error.

    Mirrors opencode's ``messages[].info.error`` shape ({name, data}). ``text``
    lets a partial response ride along (the exit-0 truncation case).
    """
    tokens = tokens or {"input": 0, "output": 0}
    parts = [{"type": "step-start"}]
    if text:
        parts.append({"type": "text", "text": text})
    data = {"message": message}
    if status_code is not None:
        data["statusCode"] = status_code
    if is_retryable is not None:
        data["isRetryable"] = is_retryable
    if headers is not None:
        data["responseHeaders"] = headers
    return {
        "info": {"id": "ses_test_err"},
        "messages": [
            {"info": {"role": "user"},
             "parts": [{"type": "text", "text": "hi"}]},
            {"info": {"role": "assistant", "tokens": tokens, "cost": cost,
                      "error": {"name": name, "data": data}},
             "parts": parts},
        ],
    }


def _install_fake_subprocess(monkeypatch, *, run_stdout, export_obj,
                             run_rc=0, run_stderr="", capture):
    """Fake ``subprocess.run`` returning different results for run vs export.

    Captures each call's command + (for the run call) the temp config content
    read back from the OPENCODE_CONFIG env var before it's unlinked.

    The code redirects stdout to a real file (``_capture``) to dodge opencode's
    64 KiB pipe-truncation bug, so this fake mirrors that: when handed a writable
    ``stdout`` file object it writes the payload there and reports ``stdout=None``
    (exactly as ``subprocess.run`` does with a redirected stdout); otherwise it
    returns the payload directly, keeping any direct callers working.
    """
    def fake_run(cmd, **kwargs):
        sub = cmd[1] if len(cmd) > 1 else ""
        rec = {"cmd": cmd, "sub": sub, "stdin": kwargs.get("stdin"),
               "stdout": kwargs.get("stdout")}
        env = kwargs.get("env") or {}
        if sub == "run":
            cfg_path = env.get("OPENCODE_CONFIG")
            if cfg_path and os.path.exists(cfg_path):
                with open(cfg_path) as f:
                    rec["config"] = json.load(f)
            rec["disable_project"] = env.get("OPENCODE_DISABLE_PROJECT_CONFIG")
            payload, rc, err = run_stdout, run_rc, run_stderr
        elif sub == "export":
            payload, rc, err = json.dumps(export_obj), 0, ""
        else:
            payload, rc, err = "", 0, ""
        capture["calls"].append(rec)

        out = kwargs.get("stdout")
        if hasattr(out, "write"):
            out.write(payload)
            return SimpleNamespace(returncode=rc, stdout=None, stderr=err)
        return SimpleNamespace(returncode=rc, stdout=payload, stderr=err)

    monkeypatch.setattr(oc_mod.subprocess, "run", fake_run)


# ---------------------------------------------------------------------------
# Offline unit tests
# ---------------------------------------------------------------------------


def test_constructor_defaults():
    llm = OpenCodeLLM()
    assert llm.model is None  # optional; opencode uses its default
    assert llm.opencode_bin == "opencode"
    assert llm.agent_name == "chia"


def test_no_model_logs_informative_message(caplog):
    with caplog.at_level("INFO", logger="opencode"):
        OpenCodeLLM()
    assert "default model" in caplog.text


def test_no_model_omits_model_flag():
    cmd = OpenCodeLLM()._build_run_cmd("hi")
    assert "--model" not in cmd
    assert cmd[-1] == "hi"


def test_experimental_warning(caplog):
    with caplog.at_level("WARNING", logger="opencode"):
        OpenCodeLLM()
    assert "experimental" in caplog.text


def test_prompt_exposes_chia_remote_surface():
    assert hasattr(OpenCodeLLM.prompt, "chia_remote")
    assert hasattr(OpenCodeLLM.prompt, "options")


def test_prompt_resource_gate():
    assert OpenCodeLLM.prompt._chia_options["resources"] == {"opencode_creds": 0.01}


def test_build_config_agent_prompt():
    llm = OpenCodeLLM(system_message="be terse")
    cfg = llm._build_config([])
    assert cfg["agent"]["chia"]["prompt"] == "be terse"
    assert cfg["agent"]["chia"]["mode"] == "primary"
    assert "mcp" not in cfg  # no tools


def test_build_config_default_prompt_when_no_system_message():
    cfg = OpenCodeLLM()._build_config([])
    assert cfg["agent"]["chia"]["prompt"]  # non-empty default


def test_build_config_mcp_servers():
    llm = OpenCodeLLM()
    tool = SimpleNamespace(name="calc", hostname="localhost", port=9001)
    cfg = llm._build_config([tool])
    assert cfg["mcp"]["calc"] == {
        "type": "remote",
        "url": "http://localhost:9001/calc/mcp",
        "enabled": True,
    }


def test_build_run_cmd_flags():
    llm = OpenCodeLLM(model="anthropic/claude-sonnet-4-6", work_dir="/tmp/x")
    cmd = llm._build_run_cmd("hello world")
    assert cmd[0] == "opencode" and cmd[1] == "run"
    assert "--format" in cmd and cmd[cmd.index("--format") + 1] == "json"
    assert "--agent" in cmd and cmd[cmd.index("--agent") + 1] == "chia"
    assert "--dangerously-skip-permissions" in cmd
    assert cmd[cmd.index("--model") + 1] == "anthropic/claude-sonnet-4-6"
    assert cmd[cmd.index("--dir") + 1] == "/tmp/x"
    assert cmd[-1] == "hello world"  # message is the trailing positional


def test_parse_session_id_from_json():
    assert parse_session_id(_step_start("ses_abc")) == "ses_abc"


def test_parse_session_id_regex_fallback():
    assert parse_session_id("noise\nsome ses_XYZ789 here\n") == "ses_XYZ789"


def test_parse_session_id_none():
    assert parse_session_id("nothing here") is None


def test_extract_from_export_text_and_usage():
    text, meta, stream, err = OpenCodeLLM()._extract_from_export(_export_obj(text="PONG"))
    assert text == "PONG"
    assert meta["output_tokens"] == 6
    assert meta["input_tokens"] == 3
    assert meta["cache_read"] == 8280
    assert meta["num_turns"] == 1
    assert round(meta["cost_usd"], 4) == 0.0026
    assert "[Response]\nPONG" in stream
    assert err is None


def test_extract_from_export_tool_trace():
    _, _, stream, err = OpenCodeLLM()._extract_from_export(_export_obj(tool="calc_run"))
    assert "[Tool Call: calc_run]" in stream
    assert "[Tool Result]" in stream
    assert err is None


def test_extract_from_export_empty():
    text, meta, stream, err = OpenCodeLLM()._extract_from_export({})
    assert text == ""
    assert stream == ""
    assert err is None


def test_extract_from_export_captures_error():
    _, _, _, err = OpenCodeLLM()._extract_from_export(
        _export_with_error(name="APIError", message="Rate Limited",
                           status_code=429, is_retryable=True),
    )
    assert err is not None
    assert err["name"] == "APIError"
    assert err["data"]["statusCode"] == 429
    assert err["data"]["isRetryable"] is True


# ---- error classification ----

def _cli(returncode=1, stderr="", result=""):
    return QueryResult(result=result, returncode=returncode, stderr=stderr, stream_result="")


def test_classify_clean_success_no_raise():
    OpenCodeLLM()._classify_error(_cli(returncode=0, result="PONG"))  # no raise


# Without a structured export_error, classification does NOT keyword-match
# stderr (opencode emits its real errors as JSON, handled by the export path);
# every such failure is reported as UnknownOpenCodeError with the stderr attached.
@pytest.mark.parametrize("stderr", [
    "Error: 429 rate limit exceeded",
    "not authenticated: missing api key",
    "payment required: add credit",
    "Configuration is invalid at cfg.json",
    "503 service unavailable",
    "hit maximum output length",
    "something weird",
])
def test_classify_no_structured_error_is_unknown(stderr):
    with pytest.raises(UnknownOpenCodeError):
        OpenCodeLLM()._classify_error(_cli(stderr=stderr))


def test_classify_empty_result_is_unknown():
    # clean exit but empty text, no structured error -> unknown failure
    with pytest.raises(UnknownOpenCodeError):
        OpenCodeLLM()._classify_error(_cli(returncode=0, result=""))


# ---- structured export-error classification (the reliable path) ----
#
# opencode run exits 0 even on failure (bug #14551), so these pass returncode=0
# and rely on the structured export_error — exactly the production shape.

def test_classify_export_rate_limit():
    with pytest.raises(RateLimitError) as exc:
        OpenCodeLLM()._classify_error(
            _cli(returncode=0, result=""),
            export_error={"name": "APIError",
                          "data": {"message": "Rate Limited",
                                   "statusCode": 429, "isRetryable": True}},
        )
    assert exc.value.error_type == "rate_limit"


def test_classify_export_rate_limit_honors_retry_after():
    """reset_time reflects the Retry-After header, not the hardcoded 60s."""
    before = datetime.now(timezone.utc)
    with pytest.raises(RateLimitError) as exc:
        OpenCodeLLM()._classify_error(
            _cli(returncode=0, result=""),
            export_error={"name": "APIError",
                          "data": {"message": "slow down", "statusCode": 429,
                                   "responseHeaders": {"retry-after": "300"}}},
        )
    delta = (exc.value.reset_time - before).total_seconds()
    assert 290 <= delta <= 320  # ~300s, clearly not the 60s default


def test_classify_export_auth():
    with pytest.raises(AuthenticationError):
        OpenCodeLLM()._classify_error(
            _cli(),
            export_error={"name": "ProviderAuthError",
                          "data": {"providerID": "anthropic",
                                   "message": "invalid api key"}},
        )


def test_classify_export_billing():
    with pytest.raises(BillingError):
        OpenCodeLLM()._classify_error(
            _cli(),
            export_error={"name": "APIError",
                          "data": {"message": "Quota exceeded. Check plan.",
                                   "statusCode": 402, "isRetryable": False}},
        )


def test_classify_export_context_overflow():
    with pytest.raises(MaxOutputTokensError) as exc:
        OpenCodeLLM()._classify_error(
            _cli(returncode=0, result="partial response..."),
            export_error={"name": "ContextOverflowError",
                          "data": {"message": "prompt is too long"}},
        )
    assert exc.value.partial_text == "partial response..."


def test_classify_export_output_length():
    with pytest.raises(MaxOutputTokensError):
        OpenCodeLLM()._classify_error(
            _cli(),
            export_error={"name": "MessageOutputLengthError", "data": {}},
        )


def test_classify_export_server_5xx():
    with pytest.raises(ServerError):
        OpenCodeLLM()._classify_error(
            _cli(),
            export_error={"name": "APIError",
                          "data": {"message": "Internal server error",
                                   "statusCode": 500, "isRetryable": True}},
        )


def test_classify_export_server_retryable_no_status():
    with pytest.raises(ServerError):
        OpenCodeLLM()._classify_error(
            _cli(),
            export_error={"name": "APIError",
                          "data": {"message": "Overloaded", "isRetryable": True}},
        )


def test_classify_export_invalid_request():
    with pytest.raises(InvalidRequestError):
        OpenCodeLLM()._classify_error(
            _cli(),
            export_error={"name": "APIError",
                          "data": {"message": "Bad model name",
                                   "statusCode": 400, "isRetryable": False}},
        )


def test_classify_export_unknown():
    with pytest.raises(UnknownOpenCodeError):
        OpenCodeLLM()._classify_error(
            _cli(),
            export_error={"name": "MessageAbortedError",
                          "data": {"message": "user cancelled"}},
        )


def test_classify_clean_success_with_no_export_error():
    """Exit 0 + result present + export_error=None -> success (no raise)."""
    OpenCodeLLM()._classify_error(_cli(returncode=0, result="PONG"), export_error=None)


def test_classify_export_error_overrides_exit_0_success():
    """Even exit 0 + partial text raises when a structured error is present
    (the core bug: run lies about success)."""
    with pytest.raises(RateLimitError):
        OpenCodeLLM()._classify_error(
            _cli(returncode=0, result="partial response..."),
            export_error={"name": "APIError",
                          "data": {"message": "Rate Limited",
                                   "statusCode": 429, "isRetryable": True}},
        )


# ---- fixture-based classification against REAL/realistic opencode payloads ----
#
# These pin opencode's actual error schema (captured against 1.15.13; see
# fixtures/README.md). The export fixtures carry the error in
# messages[].info.error; the run-stream fixture carries it as a `type:"error"`
# event in stdout (the unknown-model case, which never reaches the export).

EXPORT_ERROR_FIXTURES = [
    ("opencode_export_apierror_401.json", AuthenticationError),       # REAL
    ("opencode_export_rate_limit_429.json", RateLimitError),
    ("opencode_export_billing_402.json", BillingError),
    ("opencode_export_context_overflow.json", MaxOutputTokensError),
    ("opencode_export_output_length.json", MaxOutputTokensError),
    ("opencode_export_server_503.json", ServerError),
    ("opencode_export_invalid_request_400.json", InvalidRequestError),
    ("opencode_export_aborted.json", UnknownOpenCodeError),
]


@pytest.mark.parametrize("fixture_name,expected_exc", EXPORT_ERROR_FIXTURES)
def test_classify_on_export_fixture(fixture_name, expected_exc):
    """_extract_from_export pulls the error from a real/realistic export, and
    _classify_error maps it to the right chia exception. Exit 0 is simulated
    because opencode run lies about success (bug #14551)."""
    path = FIXTURE_DIR / fixture_name
    if not path.exists():
        pytest.skip(f"fixture not found: {path}")

    export = json.loads(path.read_text())
    llm = OpenCodeLLM()
    _, _, _, export_error = llm._extract_from_export(export)
    assert export_error is not None, f"{fixture_name}: no info.error extracted"

    cli = QueryResult(result="", returncode=0, stderr="", stream_result="")
    with pytest.raises(expected_exc):
        llm._classify_error(cli, export_error=export_error)


RUN_STREAM_ERROR_FIXTURES = [
    # Unknown model -> UnknownError in the run stream, no assistant export msg.
    ("opencode_run_unknownerror_badmodel.jsonl", UnknownOpenCodeError),
]


@pytest.mark.parametrize("fixture_name,expected_exc", RUN_STREAM_ERROR_FIXTURES)
def test_classify_on_run_stream_fixture(fixture_name, expected_exc):
    """parse_run_error pulls the error from a real `run` stdout stream (where
    pre-request failures live), and _classify_error maps it."""
    path = FIXTURE_DIR / fixture_name
    if not path.exists():
        pytest.skip(f"fixture not found: {path}")

    run_error = parse_run_error(path.read_text())
    assert run_error is not None, f"{fixture_name}: no run-stream error parsed"
    # The real unknown-model error carries the provider message.
    assert "model not found" in run_error.get("data", {}).get("message", "").lower()

    cli = QueryResult(result="", returncode=0, stderr="", stream_result="")
    with pytest.raises(expected_exc):
        llm = OpenCodeLLM()
        llm._classify_error(cli, export_error=run_error)


# REAL opencode 1.15.13 stderr captures. opencode emits its structured errors as
# JSON on stdout (the export path); only CLI/process-level failures reach stderr,
# and with no structured error those are surfaced as UnknownOpenCodeError (we no
# longer keyword-match stderr). These confirm such failures classify as unknown
# while preserving the raw stderr.
STDERR_FIXTURES = [
    "opencode_stderr_badconfig.txt",  # malformed config: "...is not valid JSON(C)"
    "opencode_stderr_badflag.txt",    # bad CLI flag: usage/help text
]


@pytest.mark.parametrize("fixture_name", STDERR_FIXTURES)
def test_classify_real_stderr_is_unknown(fixture_name):
    """A real CLI/process-level failure (stderr, no structured error) -> always
    UnknownOpenCodeError, with the stderr preserved on the exception."""
    path = FIXTURE_DIR / fixture_name
    if not path.exists():
        pytest.skip(f"fixture not found: {path}")
    stderr = path.read_text()
    cli = QueryResult(result="", returncode=1, stderr=stderr, stream_result="")
    with pytest.raises(UnknownOpenCodeError) as exc:
        OpenCodeLLM()._classify_error(cli, export_error=None)
    assert exc.value.stderr == stderr  # raw stderr is not lost


# ---------------------------------------------------------------------------
# stdout capture: file, not pipe (opencode's 64 KiB pipe-truncation bug)
# ---------------------------------------------------------------------------


def test_capture_redirects_stdout_to_file(monkeypatch):
    """_capture must redirect stdout to a real file (NOT capture_output/a pipe)
    and read the full payload back. opencode cuts piped stdout at 64 KiB and
    exits 0, so a pipe would truncate large exports; a file does not."""
    big = '{"x":"' + "y" * 200_000 + '"}'   # ~200 KiB, far past the 64 KiB limit
    seen = {}

    def fake_run(cmd, **kwargs):
        out = kwargs.get("stdout")
        seen["has_file"] = hasattr(out, "write")
        seen["capture_output"] = kwargs.get("capture_output")
        seen["stdin"] = kwargs.get("stdin")
        if hasattr(out, "write"):
            out.write(big)
        return SimpleNamespace(returncode=0, stdout=None, stderr="")

    monkeypatch.setattr(oc_mod.subprocess, "run", fake_run)
    res = OpenCodeLLM()._capture(["opencode", "export", "ses_x"], env={})

    assert seen["has_file"] is True                      # wrote to a file...
    assert seen.get("capture_output") in (None, False)   # ...not a pipe
    import subprocess as _sp
    assert seen["stdin"] == _sp.DEVNULL
    assert res.stdout == big and len(res.stdout) > 65536  # full payload, untruncated


def test_prompt_handles_large_export(monkeypatch):
    """Regression: a >64 KiB export must round-trip through prompt intact instead
    of being truncated into an 'empty response' (the original bug)."""
    big_text = "Z" * 100_000
    capture = {"calls": []}
    _install_fake_subprocess(
        monkeypatch,
        run_stdout=_step_start("ses_big"),
        export_obj=_export_obj(text=big_text),
        capture=capture,
    )
    cli = OpenCodeLLM().prompt("do a big thing", tools=[])
    assert cli.success is True
    assert cli.result == big_text
    # export stdout was redirected to a file (the fix), not a pipe.
    export_call = [c for c in capture["calls"] if c["sub"] == "export"][0]
    assert hasattr(export_call["stdout"], "write")


# ---------------------------------------------------------------------------
# Mocked flow tests (fake subprocess; full run -> export path, no opencode)
# ---------------------------------------------------------------------------


def test_prompt_run_then_export_success(monkeypatch):
    capture = {"calls": []}
    _install_fake_subprocess(
        monkeypatch,
        run_stdout=_step_start("ses_flow1"),
        export_obj=_export_obj(text="PONG"),
        capture=capture,
    )
    llm = OpenCodeLLM(model="anthropic/claude-sonnet-4-6", system_message="be terse")
    cli = llm.prompt("Reply with PONG", tools=[])

    assert cli.success is True
    assert cli.result == "PONG"
    assert cli.returncode == 0
    assert "[Response]\nPONG" in cli.stream_result

    # Two subprocess calls: run, then export with the parsed session id.
    assert [c["sub"] for c in capture["calls"]] == ["run", "export"]
    assert capture["calls"][1]["cmd"][-1] == "ses_flow1"

    # run carried our config (agent prompt) + disabled project config.
    run_call = capture["calls"][0]
    assert run_call["config"]["agent"]["chia"]["prompt"] == "be terse"
    assert run_call["disable_project"] == "1"
    # stdin must be closed or `run` hangs on the pipe.
    import subprocess as _sp
    assert run_call["stdin"] == _sp.DEVNULL

    # metadata surfaced
    assert llm._last_metadata["output_tokens"] == 6
    assert llm._last_metadata["model"] == "anthropic/claude-sonnet-4-6"
    assert llm._last_metadata["num_turns"] == 1


def test_prompt_export_error_raises_despite_exit_0(monkeypatch):
    """Core fix end-to-end: run exits 0 but the export carries a structured
    error, so prompt must classify + raise (not return a bogus success)."""
    capture = {"calls": []}
    _install_fake_subprocess(
        monkeypatch,
        run_stdout=_step_start("ses_err1"),
        export_obj=_export_with_error(name="ProviderAuthError",
                                      message="invalid api key"),
        run_rc=0,  # the lie: exit 0 despite the failure
        capture=capture,
    )
    llm = OpenCodeLLM(model="x/y")
    with pytest.raises(AuthenticationError):
        llm.prompt("hi", tools=[])
    # it actually ran the export to discover the error.
    assert [c["sub"] for c in capture["calls"]] == ["run", "export"]


def test_prompt_resets_stale_export_error(monkeypatch):
    """Fix #5: a prior attempt's export error must not leak into a later attempt
    whose run fails *before* producing an export. Here the run fails with no
    session id (so _run_opencode returns early and never sets _last_export_error);
    the reset at the top of the attempt must wipe the pre-seeded stale 429. If it
    didn't, prompt would raise RateLimitError instead of returning a clean
    failure."""
    capture = {"calls": []}
    _install_fake_subprocess(
        monkeypatch,
        run_stdout="",  # no session id -> early return, export never runs
        export_obj={},
        run_rc=1, run_stderr="boom: mysterious failure",
        capture=capture,
    )
    llm = OpenCodeLLM(model="x/y", retries=1)
    llm._last_export_error = {"name": "APIError",
                              "data": {"statusCode": 429, "message": "stale"}}
    cli = llm.prompt("hi", tools=[])
    assert cli.success is False
    assert llm._last_export_error is None


def test_prompt_run_stream_error_fallback(monkeypatch):
    """Pre-request errors (unknown model) appear only in the run stream and the
    export has no assistant message — _run_opencode must surface the run-stream
    error via the fallback so prompt classifies it instead of reporting success
    or losing it as 'empty response'. Driven by the REAL bad-model run stream."""
    capture = {"calls": []}
    run_stdout = (FIXTURE_DIR / "opencode_run_unknownerror_badmodel.jsonl").read_text()
    export_no_error = {  # opencode's real export for this case: user msg only
        "info": {"id": "ses_x"},
        "messages": [{"info": {"role": "user"},
                      "parts": [{"type": "text", "text": "hi"}]}],
    }
    _install_fake_subprocess(
        monkeypatch,
        run_stdout=run_stdout,
        export_obj=export_no_error,
        run_rc=0,  # opencode exits 0 even though the model was unknown
        capture=capture,
    )
    llm = OpenCodeLLM(model="nonexistent/fake", retries=1)
    cli = llm.prompt("hi", tools=[])

    assert cli.success is False
    err = llm._last_export_error
    assert err is not None and err["name"] == "UnknownError"
    assert "model not found" in err["data"]["message"].lower()


def test_prompt_passes_mcp_servers(monkeypatch):
    capture = {"calls": []}
    _install_fake_subprocess(
        monkeypatch,
        run_stdout=_step_start(),
        export_obj=_export_obj(text="done", tool="calc"),
        capture=capture,
    )
    tool = SimpleNamespace(name="calc", hostname="localhost", port=9001)
    llm = OpenCodeLLM()
    cli = llm.prompt("use calc", tools=[tool])

    assert cli.success is True
    mcp = capture["calls"][0]["config"]["mcp"]
    assert mcp["calc"]["url"] == "http://localhost:9001/calc/mcp"
    assert mcp["calc"]["type"] == "remote"
    assert "[Tool Call: calc]" in cli.stream_result


def test_prompt_run_failure_no_structured_error_is_unknown(monkeypatch):
    """A run failure whose only signal is stderr text (no structured JSON error)
    is NOT keyword-classified — it surfaces as UnknownOpenCodeError, which is
    retried and then returned as a failure QueryResult."""
    capture = {"calls": []}
    _install_fake_subprocess(
        monkeypatch,
        run_stdout="",                      # no session id, no JSON error event
        run_rc=1,
        run_stderr="not authenticated for provider anthropic",
        export_obj={},
        capture=capture,
    )
    cli = OpenCodeLLM(retries=1).prompt("hi", tools=[])
    assert cli.success is False
    # export must NOT be attempted without a session id
    assert [c["sub"] for c in capture["calls"]] == ["run"]


def test_prompt_no_session_id_is_failure(monkeypatch):
    capture = {"calls": []}
    _install_fake_subprocess(
        monkeypatch,
        run_stdout="garbage with no session",
        run_rc=0,
        export_obj={},
        capture=capture,
    )
    # clean exit but no session id -> unknown error, retried then failure QueryResult
    cli = OpenCodeLLM(retries=2).prompt("hi", tools=[])
    assert cli.success is False
    assert cli.returncode == -1


# ---------------------------------------------------------------------------
# Live tests (real opencode; need OPENCODE_TEST_MODEL + opencode auth)
# ---------------------------------------------------------------------------

_OC_BIN = os.environ.get("OPENCODE_BIN", "opencode")
_OC_AVAILABLE = bool(shutil.which(_OC_BIN) or os.path.exists(_OC_BIN))
# Default the live-test model to opencode/big-pickle; override with the env var.
_OC_TEST_MODEL = os.environ.get("OPENCODE_TEST_MODEL", "opencode/big-pickle")

live = pytest.mark.skipif(
    not _OC_AVAILABLE,
    reason="opencode binary not found (set OPENCODE_BIN to your install to run live)",
)


@live
def test_live_opencode_simple_prompt(tmp_path):
    llm = OpenCodeLLM(
        model=_OC_TEST_MODEL,  # defaults to opencode/big-pickle
        opencode_bin=_OC_BIN,
        system_message="You answer with a single word and nothing else.",
        work_dir=str(tmp_path),
    )
    cli = llm.prompt("Reply with exactly: PONG", tools=[])
    assert cli.success is True
    assert "PONG" in cli.result.upper()
    assert llm._last_metadata.get("output_tokens", 0) > 0
    # The full result trace is reconstructed into stream_result.
    assert "[Response]" in cli.stream_result


@live
def test_live_opencode_bad_model_surfaces_structured_error(tmp_path):
    """Validate the REAL opencode error schema against our extractor/classifier.

    Because ``opencode run`` exits 0 even on failure (bug #14551), a bogus model
    id should still create a session whose export carries ``info.error``. This
    forces that path and checks two things our unit tests can only *assume*:
      1. a bogus model does NOT look like a success (prompt raises a typed
         OpenCodeError, or returns success=False), and
      2. if the failure was surfaced via the export, the captured
         ``_last_export_error`` matches the ``{name: str, data: dict}`` contract
         the classifier relies on — so schema drift in opencode is caught here
         rather than silently degrading every error to UnknownOpenCodeError.
    The captured shape is printed (``-s``) so drift is visible even when it still
    technically satisfies the contract.
    """
    llm = OpenCodeLLM(
        model="nonexistent/totally-bogus-model-xyzzy",
        opencode_bin=_OC_BIN,
        work_dir=str(tmp_path),
        retries=1,  # a deterministic failure — don't retry/back off
    )
    raised = None
    cli = None
    try:
        cli = llm.prompt("Reply with exactly: PONG", tools=[])
    except OpenCodeError as e:
        raised = e

    assert raised is not None or (cli is not None and cli.success is False), (
        "a bogus model must not be reported as a success"
    )

    err = llm._last_export_error
    print(f"\n[opencode] raised={type(raised).__name__ if raised else None} "
          f"_last_export_error={err!r}")
    if err is not None:
        assert isinstance(err, dict), f"export error is not a dict: {err!r}"
        assert isinstance(err.get("name"), str) and err["name"], (
            f"export error missing a string 'name': {err!r}"
        )
        assert isinstance(err.get("data", {}), dict), (
            f"export error 'data' is not a dict: {err!r}"
        )


@live
def test_live_opencode_auth_error_caught(tmp_path, monkeypatch):
    """Live error-catching for the GENUINE provider-error path (the case the fix
    targets): a github-copilot model with a deliberately bogus GITHUB_TOKEN makes
    opencode return APIError 401 in the export's ``info.error``, which our
    classifier must map to AuthenticationError end-to-end. The bogus token is
    scoped to this subprocess (monkeypatch) — the real token is untouched. Skips
    if github-copilot isn't the configured provider here (you'd get a
    'model not found' UnknownError instead); override with OPENCODE_AUTHERR_MODEL.
    """
    model = os.environ.get("OPENCODE_AUTHERR_MODEL", "github-copilot/gpt-4o")
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_bogusinvalidtoken000000000000000000")
    llm = OpenCodeLLM(model=model, opencode_bin=_OC_BIN,
                      work_dir=str(tmp_path), retries=1)
    raised = None
    try:
        llm.prompt("Reply with exactly: PONG", tools=[])
    except OpenCodeError as e:
        raised = e

    err = llm._last_export_error
    print(f"\n[opencode] auth-test raised={type(raised).__name__ if raised else None} "
          f"err={err!r}")
    if err and err.get("name") == "UnknownError":
        pytest.skip("github-copilot not configured here (got 'model not found'); "
                    "set OPENCODE_AUTHERR_MODEL to a configured provider's model")

    assert isinstance(raised, AuthenticationError), (
        f"expected AuthenticationError, got {type(raised).__name__}; err={err!r}"
    )
    assert err and err.get("name") == "APIError"
    assert err["data"].get("statusCode") in (401, 403)


@pytest.fixture
def local_bash_tool():
    if not _OC_AVAILABLE:
        pytest.skip("opencode binary not found (set OPENCODE_BIN to run live)")

    import uuid

    import ray

    from chia.base.tools.BashTool import BashTool

    # chia is a namespace package run from the repo root, so Ray actor workers
    # need the repo root on PYTHONPATH to import chia.base.
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


@live
def test_live_opencode_with_bash_tool(local_bash_tool, tmp_path):
    llm = OpenCodeLLM(
        model=_OC_TEST_MODEL,  # defaults to opencode/big-pickle
        opencode_bin=_OC_BIN,
        system_message=(
            "You have a bash tool available. To answer the user you MUST run "
            "the requested shell command with that tool and report its output "
            "verbatim. Never guess the output."
        ),
        work_dir=str(tmp_path),
    )
    cli = llm.prompt(
        "Run the shell command:  echo CHIA_TOOL_OK\n"
        "Then reply with exactly the line it printed.",
        tools=[local_bash_tool],
    )
    assert cli.success is True
    assert "CHIA_TOOL_OK" in cli.result
    # The full trace (tool call + result) is captured in stream_result.
    assert "[Tool Call:" in cli.stream_result
    assert "[Tool Result]" in cli.stream_result


# ---------------------------------------------------------------------------
# Live tests: the full stream_result conversation log
#
# Unlike the OpenAI-compat / Vertex backends, opencode's RETURNED stream_result
# is the assistant transcript ONLY — the `===` banner, the [User Message], and
# the trailing `---` rule are written to the .log file (see _run_opencode), NOT
# into stream_result. So these assert opencode's actual shape: per-turn
# [Response] (and [Thinking] for reasoning models), and for a tool run a
# [Tool Call: <opencode tool id>] + Args: + [Tool Result] block, in order, with
# the sentinel round-tripping through the tool. We assert section ORDER + the
# command-in-args + sentinel-in-result rather than the exact tool name, because
# opencode names the call by its own id, not chia's `{tool.name}__{fn}`.
# ---------------------------------------------------------------------------


@live
def test_live_opencode_stream_log_simple(tmp_path):
    prompt = "Reply with exactly: PONG"
    llm = OpenCodeLLM(
        model=_OC_TEST_MODEL,
        opencode_bin=_OC_BIN,
        system_message="You answer with a single word and nothing else.",
        work_dir=str(tmp_path),
    )
    cli = llm.prompt(prompt, tools=[])
    stream = cli.stream_result

    assert cli.success is True
    assert stream, "stream_result is empty"
    # A no-tool answer: one [Response] (reasoning models may add [Thinking]),
    # and no tool sections. No banner/[User Message] in opencode's stream_result.
    assert "[Response]" in stream, "missing [Response] section\n" + stream
    assert "PONG" in stream.upper(), "the answer is not in the [Response]\n" + stream
    assert "[Tool Call:" not in stream
    assert "[Tool Result" not in stream


@live
def test_live_opencode_stream_log_tool_conversation(local_bash_tool, tmp_path):
    llm = OpenCodeLLM(
        model=_OC_TEST_MODEL,
        opencode_bin=_OC_BIN,
        system_message=(
            "You have a bash tool available. To answer the user you MUST run "
            "the requested shell command with that tool and report its output "
            "verbatim. Never guess the output."
        ),
        work_dir=str(tmp_path),
    )
    cli = llm.prompt(
        "Run the shell command:  echo CHIA_TOOL_OK\n"
        "Then reply with exactly the line it printed.",
        tools=[local_bash_tool],
    )
    stream = cli.stream_result

    assert cli.success is True
    # Every section of a tool round-trip is present.
    assert "[Tool Call:" in stream, "missing [Tool Call:] section\n" + stream
    assert "Args:" in stream, "tool call missing its Args: line\n" + stream
    assert "[Tool Result]" in stream, "missing [Tool Result] section\n" + stream
    assert "[Response]" in stream

    call_idx = stream.index("[Tool Call:")
    result_idx = stream.index("[Tool Result]")
    final_response_idx = stream.rindex("[Response]")

    # Order: tool call -> tool result -> final answer.
    assert call_idx < result_idx, "tool call should precede its result\n" + stream
    assert final_response_idx > result_idx, (
        "the final [Response] should follow the [Tool Result]\n" + stream
    )
    # The command went through the tool, and its echoed output came back.
    assert "echo CHIA_TOOL_OK" in stream[call_idx:result_idx], (
        "the command is not in the tool-call Args\n" + stream
    )
    assert "CHIA_TOOL_OK" in stream[result_idx:], (
        "tool-result section missing the echoed sentinel\n" + stream
    )
