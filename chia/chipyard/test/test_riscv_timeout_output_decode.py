import subprocess
import sys

from chia.chipyard import riscv_build_node, riscv_objdump_node


def test_riscv_build_timeout_preserves_partial_output(tmp_path):
    stdout, stderr, returncode = riscv_build_node.RiscvBuildNode(
        timeout_seconds=.5
    )._run([sys.executable, '-u', '-c', "import sys,time;print('partial stdout');print('partial stderr',file=sys.stderr);time.sleep(60)"], cwd=str(tmp_path))

    assert stdout == "partial stdout\n"
    assert "partial stderr" in stderr
    assert "timeout after 0.5s" in stderr
    assert returncode == -1


def test_riscv_objdump_timeout_decodes_byte_output(monkeypatch):
    def fake_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(
            cmd=args[0],
            timeout=kwargs["timeout"],
            output=b"partial stdout",
            stderr=b"partial stderr",
        )

    monkeypatch.setattr(riscv_objdump_node.subprocess, "run", fake_run)

    stdout, stderr, returncode = riscv_objdump_node.RiscvObjdumpNode(
        timeout_seconds=11
    )._run(["objdump"], cwd="/work")

    assert stdout == "partial stdout"
    assert "partial stderr" in stderr
    assert "[RiscvObjdumpNode] timeout after 11s" in stderr
    assert returncode == -1
