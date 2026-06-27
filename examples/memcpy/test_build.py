"""Test-build node: cross-compile memcpy.c inside the chipyard tests folder.

This node runs ON the chipyard (chisel-build) container, where the RISC-V
baremetal toolchain, the chipyard ``tests/`` suite, its ``rocc.h`` header, and
CMake all live. It:

  1. drops ``memcpy.c`` into ``$chipyard/tests/`` (so the committed ``rocc.h``
     and linker collateral resolve),
  2. registers a CMake executable + disassembly target for it (idempotently —
     the chipyard CMakeLists drives the build via per-target ``add_executable``
     / ``add_dump_target``; see tests/README.md),
  3. runs the configure / build / dump make commands, and
  4. reads ``build/memcpy.riscv`` (ELF) and ``build/memcpy.dump`` (objdump)
     back so the head node can stage them for the verilator run + debugger.

The output naming is fixed by the chipyard CMakeLists: ``CMAKE_EXECUTABLE_SUFFIX
= ".riscv"`` makes the ``memcpy`` target emit ``build/memcpy.riscv``, and
``add_dump_target(memcpy)`` emits ``build/memcpy.dump`` via the ``memcpy-dump``
target.

The node never raises on a failed compile — callers branch on
``TestBuildResult.success`` (mirrors the never-raise contract of the chia
build/run nodes).
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass

from chia.base.ChiaFunction import ChiaFunction

# Marker guarding the lines we append to the chipyard CMakeLists so a repeated
# run (or a debug rebuild) does not register the target twice.
_CMAKE_MARKER = "# --- chia memcpy example (auto-added) ---"


@dataclass
class TestBuildResult:
    """Result of building the bare-metal test ELF in the chipyard tests tree."""

    success: bool
    riscv_name: str           # e.g. "memcpy.riscv"
    riscv_content: bytes      # raw ELF bytes (b"" on failure)
    dump_name: str            # e.g. "memcpy.dump"
    dump: str                 # objdump -D disassembly ("" on failure)
    stdout: str
    stderr: str
    returncode: int


def _run(cmd: list[str], cwd: str, timeout_seconds: int) -> tuple[str, str, int]:
    """Run *cmd*, returning (stdout, stderr, rc). rc=-1 on timeout (never raises)."""
    try:
        proc = subprocess.run(
            cmd, cwd=cwd, capture_output=True, text=True, timeout=timeout_seconds
        )
        return proc.stdout, proc.stderr, proc.returncode
    except subprocess.TimeoutExpired as e:
        out = e.stdout.decode(errors="replace") if isinstance(e.stdout, bytes) else (e.stdout or "")
        err = e.stderr.decode(errors="replace") if isinstance(e.stderr, bytes) else (e.stderr or "")
        return out, err + f"\n[test_build] timed out after {timeout_seconds}s", -1


@ChiaFunction(resources={"chipyard": 0.05})
def build_test(
    source_content: bytes,
    tests_dir: str,
    test_name: str,
    timeout_seconds: int = 900,
) -> TestBuildResult:
    """Build ``<test_name>.riscv`` + ``<test_name>.dump`` in *tests_dir*.

    Runs the chipyard tests CMake flow:

        cmake -S ./ -B ./build/ -D CMAKE_BUILD_TYPE=Debug
        cmake --build ./build/ --target <test_name>-dump

    The ``-dump`` target depends on the executable target, so building it alone
    produces both ``<test_name>.riscv`` and ``<test_name>.dump``. Reads the
    resulting ELF + disassembly from ``build/`` and returns them.
    """
    source_filename = f"{test_name}.c"
    build_dir = os.path.join(tests_dir, "build")
    cmake_lists = os.path.join(tests_dir, "CMakeLists.txt")

    stdout_parts: list[str] = []
    stderr_parts: list[str] = []

    # 1. Drop the source into the tests tree (alongside rocc.h etc.).
    with open(os.path.join(tests_dir, source_filename), "wb") as f:
        f.write(source_content)

    # 2. Register the CMake target idempotently. Mirrors the per-test pattern in
    # the chipyard tests CMakeLists: add_executable(<name> <name>.c) followed by
    # add_dump_target(<name>).
    with open(cmake_lists, "r", errors="replace") as f:
        cmake_text = f.read()
    if _CMAKE_MARKER not in cmake_text:
        with open(cmake_lists, "a") as f:
            f.write(
                f"\n{_CMAKE_MARKER}\n"
                f"add_executable({test_name} {source_filename})\n"
                f"add_dump_target({test_name})\n"
            )

    # 3a. Configure (safe to re-run; only does real work when build/ is fresh).
    out, err, rc = _run(
        ["cmake", "-S", "./", "-B", "./build/", "-D", "CMAKE_BUILD_TYPE=Debug"],
        cwd=tests_dir, timeout_seconds=timeout_seconds,
    )
    stdout_parts.append(f"$ cmake configure\n{out}")
    if err:
        stderr_parts.append(f"$ cmake configure\n{err}")
    if rc != 0:
        return TestBuildResult(
            success=False, riscv_name=f"{test_name}.riscv", riscv_content=b"",
            dump_name=f"{test_name}.dump", dump="",
            stdout="\n".join(stdout_parts), stderr="\n".join(stderr_parts), returncode=rc,
        )

    # 3b. Build the disassembly target. It DEPENDS on the executable target
    # (add_dump_target in the chipyard tests CMakeLists), so this builds
    # <test_name>.riscv first and then objdumps it to <test_name>.dump.
    dump_target = f"{test_name}-dump"
    out, err, rc = _run(
        ["cmake", "--build", "./build/", "--target", dump_target],
        cwd=tests_dir, timeout_seconds=timeout_seconds,
    )
    stdout_parts.append(f"$ cmake --build --target {dump_target}\n{out}")
    if err:
        stderr_parts.append(f"$ cmake --build --target {dump_target}\n{err}")
    if rc != 0:
        return TestBuildResult(
            success=False, riscv_name=f"{test_name}.riscv", riscv_content=b"",
            dump_name=f"{test_name}.dump", dump="",
            stdout="\n".join(stdout_parts), stderr="\n".join(stderr_parts), returncode=rc,
        )

    # 4. Read back the collateral.
    riscv_path = os.path.join(build_dir, f"{test_name}.riscv")
    dump_path = os.path.join(build_dir, f"{test_name}.dump")
    try:
        with open(riscv_path, "rb") as f:
            riscv_content = f.read()
    except FileNotFoundError:
        riscv_content = b""
    try:
        with open(dump_path, "r", errors="replace") as f:
            dump = f.read()
    except FileNotFoundError:
        dump = ""

    success = bool(riscv_content)
    if not success:
        stderr_parts.append(f"[test_build] expected ELF not found at {riscv_path}")

    return TestBuildResult(
        success=success,
        riscv_name=f"{test_name}.riscv",
        riscv_content=riscv_content,
        dump_name=f"{test_name}.dump",
        dump=dump,
        stdout="\n".join(stdout_parts),
        stderr="\n".join(stderr_parts),
        returncode=0 if success else 1,
    )
