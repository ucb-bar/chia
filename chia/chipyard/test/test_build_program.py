"""Unit tests for RiscvBuildNode.build_program.

Exercise the general build path with plain shell commands — no cluster, Docker
image, or RISC-V toolchain needed (build()'s real cross-compilation is covered by
the cluster tests here). Run: pytest chia/chipyard/test/test_build_program.py
"""

from chia.chipyard.riscv_build_node import RiscvBuildNode


def test_collects_outputs_from_nested_inputs(tmp_path):
    art = RiscvBuildNode().build_program(
        input_files={"src/a.txt": b"AAA", "src/b.txt": b"BBB"},
        command=["bash", "-c", "cat src/a.txt src/b.txt > out.bin"],
        work_dir=str(tmp_path),
        outputs=["out.bin"],
    )
    assert art.success and art.returncode == 0
    assert art.files == {"out.bin": b"AAABBB"}


def test_string_command_and_directory_glob(tmp_path):
    # str command runs under a shell; a directory in `outputs` is walked.
    art = RiscvBuildNode().build_program(
        input_files={},
        command="mkdir -p build && echo hi > build/x.o && echo yo > build/y.o",
        work_dir=str(tmp_path),
        outputs=["build"],
    )
    assert art.success
    assert art.files == {"build/x.o": b"hi\n", "build/y.o": b"yo\n"}


def test_failure_reports_returncode_and_no_files(tmp_path):
    art = RiscvBuildNode().build_program(
        input_files={},
        command="echo boom >&2; exit 3",
        work_dir=str(tmp_path),
        outputs=["never.bin"],
    )
    assert not art.success and art.returncode == 3
    assert art.files == {}
    assert "boom" in art.stderr
