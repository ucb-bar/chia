from pathlib import Path
import shutil
import sys
import time

import pytest

from chia.base.bounded_subprocess import run_bounded
from chia.chipyard.riscv_build_node import RiscvBuildNode


def test_small_messages_and_exit_status_are_unchanged(tmp_path):
    command = [sys.executable, '-c', "import sys;print('build log');print('probe.c:4: error: bad syntax',file=sys.stderr);sys.exit(3)"]
    assert run_bounded(command, tmp_path, 5) == ('build log\n', 'probe.c:4: error: bad syntax\n', 3)


def test_drain_both_streams_and_keep_initial_error_and_final_linker_error(tmp_path):
    script = """import os
for fd in (1, 2):
 os.write(fd,b'probe.c:18: error: invalid call\\n')
 for _ in range(128):os.write(fd,b'x'*65536)
 os.write(fd,b'\\nld: undefined reference to finish\\n')
"""
    stdout, stderr, rc = run_bounded([sys.executable, '-c', script], tmp_path, 10)
    assert rc == 0
    for stream in (stdout, stderr):
        assert stream.startswith('probe.c:18: error: invalid call\n')
        assert stream.endswith('ld: undefined reference to finish\n')
        assert 'compiler output truncated' in stream
        assert len(stream.encode()) <= 1024 * 1024


def test_timeout_kills_descendant_holding_pipes_after_parent_exits(tmp_path):
    script = "import subprocess,sys; p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)']);print(p.pid,flush=True)"
    stdout, stderr, rc = run_bounded([sys.executable, '-c', script], tmp_path, .5)
    assert rc == -1 and 'timed out' in stderr
    status = Path(f'/proc/{int(stdout.strip())}/stat')
    deadline = time.monotonic() + 2
    while status.exists() and status.read_text().split()[2] != 'Z' and time.monotonic() < deadline:
        time.sleep(.01)
    assert not status.exists() or status.read_text().split()[2] == 'Z'


def test_boom_build_logs_are_bounded_and_shell_commands_still_work(tmp_path):
    node = RiscvBuildNode()
    out, err, rc = node._run(f'{sys.executable} -c "import sys;sys.stderr.write(\'x\'*4000000)"', str(tmp_path))
    assert rc == 0 and out == ''
    assert len(err.encode()) <= 1024 * 1024 and 'truncated' in err


def test_compiler_flag_keeps_actual_errors_without_giant_source_echo(tmp_path):
    gcc = shutil.which('gcc')
    if gcc is None:
        pytest.skip('host GCC unavailable')
    source = tmp_path/'probe.c'
    source.write_text(''.join(f'int g{i}(void){{ return missing_{i}; }}' for i in range(200)))
    out, err, rc = run_bounded([gcc, '-fno-diagnostics-show-caret', '-fsyntax-only', str(source)], tmp_path, 10)
    assert rc != 0 and 'error:' in err and 'missing_0' in err and 'missing_199' in err
    assert 'return missing_0;' not in err
