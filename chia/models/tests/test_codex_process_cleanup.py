"""Offline timeout regression; can also run with --basetemp on an NFS mount."""

import os
from pathlib import Path
import signal
import subprocess
import sys
from types import SimpleNamespace

import pytest

from chia.models.codex import CodexLLM


@pytest.mark.skipif(sys.platform != "linux", reason="inspects Linux process state")
def test_timeout_kills_session_file_holder_and_retry_succeeds(monkeypatch, tmp_path):
    import chia.trace.profiler as profiler

    monkeypatch.setattr(profiler, "get_profiler", lambda: SimpleNamespace(enabled=False))
    base = tmp_path / "base"
    base.mkdir()
    monkeypatch.setenv("CODEX_HOME", str(base))
    monkeypatch.setenv("CHIA_CODEX_SESSION_ROOT", str(tmp_path / "sessions"))
    monkeypatch.setenv("CODEX_CLEANUP_TEST", str(tmp_path))
    child = tmp_path / "holder.py"
    child.write_text('''import os, time
from pathlib import Path
with open(Path(os.environ["CODEX_HOME"]) / "held.log", "w") as held:
    held.write("isolated test"); held.flush()
    Path(os.environ["CODEX_CLEANUP_TEST"], "child.pid").write_text(str(os.getpid()))
    time.sleep(60)
''')
    launcher = tmp_path / "launcher.py"
    launcher.write_text('''import json, os, subprocess, sys, time
from pathlib import Path
root = Path(os.environ["CODEX_CLEANUP_TEST"])
sys.stdin.read()
if not (root / "child.pid").exists():
    subprocess.Popen([sys.executable, str(root / "holder.py")])
    time.sleep(60)
else:
    Path(sys.argv[sys.argv.index("--output-last-message") + 1]).write_text("OK")
    print(json.dumps({"type": "turn.started"}))
    print(json.dumps({"type": "turn.completed"}))
''')
    llm = CodexLLM(timeout_seconds=2, retries=2, resume_session=True,
                   session_storage_key="isolated-timeout", work_dir=str(tmp_path))
    build_cmd = llm._build_cmd
    monkeypatch.setattr(llm, "_build_cmd", lambda *a, **kw:
                        [sys.executable, str(launcher), *build_cmd(*a, **kw)[1:]])
    monkeypatch.setattr(llm, "_get_node_id", lambda: "isolated-test")
    # An unrelated process must survive the timeout cleanup.
    unrelated = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        result = CodexLLM.prompt._chia_original(llm, "test", [])
        pid = int((tmp_path / "child.pid").read_text())
        state = Path(f"/proc/{pid}/stat")
        assert not state.exists() or state.read_text().rsplit(")", 1)[1].split()[0] == "Z"
        assert unrelated.poll() is None
        assert result.success and result.result == "OK", result.stderr
        assert [a["error"] for a in llm._last_metadata["attempts"]] == ["TimeoutExpired", ""]
        assert not list((tmp_path / "sessions").rglob(".nfs*"))
        assert not (tmp_path / "sessions" / "isolated-timeout").exists()
    finally:
        unrelated.kill()
        unrelated.wait()
        marker = tmp_path / "child.pid"
        if marker.exists():
            try:
                os.kill(int(marker.read_text()), signal.SIGKILL)
            except ProcessLookupError:
                pass
