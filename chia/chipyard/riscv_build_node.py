"""Build RISC-V programs inside the ``chia-riscv-cross`` image.

``build_program`` runs an arbitrary build command (bash/make/cmake) over
caller-supplied files and collects requested outputs as bytes. ``build`` is a
thin single-source-C/asm-to-ELF wrapper over it using the harness Makefile at
``/opt/riscv-harness/Makefile``. Both run on ``riscv_build`` workers and never
raise on build failure — callers branch on the artifact's ``success``.
"""

import logging
import os
import shutil
import subprocess
import uuid
from pathlib import Path
from typing import Literal

from chia.base.ChiaFunction import ChiaFunction
from chia.chipyard.state_def import ProgramBuildArtifact, RiscvBuildArtifact


HARNESS_MAKEFILE = "/opt/riscv-harness/Makefile"

BuildTarget = Literal["verilator", "linux"]

# Output filename per target; mirrors the harness Makefile's OUTPUT.
_OUTPUT_NAME: dict[str, "callable[[str], str]"] = {
    "verilator": lambda program: f"{program}.riscv",
    "linux":     lambda program: program,
}

# Source language -> file extension (the harness Makefile has rules for both).
SourceLang = Literal["c", "asm"]
_LANG_EXT: dict[str, str] = {"c": ".c", "asm": ".S"}


class RiscvBuildNode:
    """Cross-compiles RISC-V programs via the ``chia-riscv-cross`` toolchain."""

    logging_name = "RiscvBuildNode"

    def __init__(self, timeout_seconds: int = 300, logging_level: int = logging.DEBUG):
        """
        Args:
            timeout_seconds: Wall-clock limit per build command; on expiry the
                build returns ``returncode=-1`` (never raises).
            logging_level: Logging level for this node's logger.
        """
        self.timeout_seconds = timeout_seconds
        self.logger = logging.getLogger(self.logging_name)
        self.logger.setLevel(logging_level)

    @ChiaFunction(resources={"riscv_build": 1})
    def build_program(
        self,
        input_files: dict[str, bytes],
        command: "list[str] | str",
        work_dir: str,
        outputs: "list[str] | None" = None,
        cleanup_task_dir: bool = True,
    ) -> ProgramBuildArtifact:
        """Run ``command`` over ``input_files`` in a task dir and collect outputs.

        Args:
            input_files: Files to drop into the task dir, keyed by relative path
                (nested paths allowed; parent dirs are created).
            command: Build command — a ``list`` is exec'd; a ``str`` runs via a shell.
            work_dir: Base dir; a uuid task subdir is created under it per call.
            outputs: Path/glob patterns (relative to the task dir) to read back as
                bytes; a directory pattern is walked recursively.
            cleanup_task_dir: Remove the task dir after collecting outputs.

        Returns:
            ProgramBuildArtifact with the collected ``files`` and the command's
            ``success``/``stdout``/``stderr``/``returncode`` (``-1`` on timeout).
        """
        task_dir = self._setup(input_files, work_dir)
        self.logger.info(f"Running: {command!r} (cwd={task_dir})")
        stdout, stderr, returncode = self._run(command, cwd=task_dir)
        files = self._collect(task_dir, outputs or [])
        if returncode != 0:
            self.logger.warning(
                f"build_program failed (rc={returncode}); stderr tail: {stderr[-500:]}"
            )
        if cleanup_task_dir:
            shutil.rmtree(task_dir, ignore_errors=True)
        return ProgramBuildArtifact(
            files=files, success=returncode == 0,
            stdout=stdout, stderr=stderr, returncode=returncode,
        )

    @ChiaFunction(resources={"riscv_build": 1})
    def build(
        self,
        source_content: bytes,
        program_name: str,
        work_dir: str,
        target: BuildTarget = "verilator",
        extra_cflags: str = "",
        extra_ldflags: str = "",
        include_dump: bool = False,
        cleanup_task_dir: bool = True,
        lang: SourceLang = "c",
    ) -> RiscvBuildArtifact:
        """Cross-compile one C/asm source into a RISC-V ELF via the harness Makefile.

        Args:
            source_content: Raw source bytes.
            program_name: Base name for the source, ``PROGRAM=``, and output binary.
            work_dir: Base dir; a uuid task subdir is created under it per call.
            target: ``"verilator"`` (baremetal ``<name>.riscv``) or ``"linux"``
                (userspace ``<name>``); selects the toolchain prefix.
            extra_cflags: Forwarded as ``EXTRA_CFLAGS=`` to the harness Makefile.
            extra_ldflags: Forwarded as ``EXTRA_LDFLAGS=``.
            include_dump: Also build the ``dump`` target and return the disassembly.
            cleanup_task_dir: Remove the task dir after the build.
            lang: ``"c"`` -> ``.c``, ``"asm"`` -> ``.S``.

        Returns:
            RiscvBuildArtifact with the ELF bytes, ``binary_name``, ``target``,
            optional ``dump``, and ``success``/std streams/``returncode``.

        Raises:
            ValueError: If ``target`` or ``lang`` is unrecognized.
        """
        if target not in _OUTPUT_NAME:
            raise ValueError(f"target must be one of {sorted(_OUTPUT_NAME)} (got {target!r})")
        if lang not in _LANG_EXT:
            raise ValueError(f"lang must be one of {sorted(_LANG_EXT)} (got {lang!r})")

        source_filename = f"{program_name}{_LANG_EXT[lang]}"
        binary_name = _OUTPUT_NAME[target](program_name)
        dump_name = f"{program_name}.dump"

        cmd = [
            "make", "-f", HARNESS_MAKEFILE,
            f"TARGET={target}", f"PROGRAM={program_name}", f"SRCS={source_filename}",
            f"EXTRA_CFLAGS={extra_cflags}", f"EXTRA_LDFLAGS={extra_ldflags}",
        ]
        outputs = [binary_name]
        if include_dump:
            cmd.append("dump")
            outputs.append(dump_name)

        art = self.build_program(
            input_files={source_filename: source_content}, command=cmd,
            work_dir=work_dir, outputs=outputs, cleanup_task_dir=cleanup_task_dir,
        )

        binary_content = art.files.get(binary_name, b"") if art.returncode == 0 else b""
        success = art.returncode == 0 and binary_content != b""
        dump = ""
        if include_dump and success:
            dump = art.files.get(dump_name, b"").decode("utf-8", errors="replace")

        return RiscvBuildArtifact(
            binary_name=binary_name, binary_content=binary_content, target=target,
            success=success, stdout=art.stdout, stderr=art.stderr,
            returncode=art.returncode, dump=dump,
        )

    @staticmethod
    def _setup(input_files: dict[str, bytes], work_dir: str) -> str:
        """Create a uuid task dir under ``work_dir`` and write ``input_files`` into it."""
        task_dir = os.path.join(work_dir, uuid.uuid4().hex[:8])
        os.makedirs(task_dir, exist_ok=True)
        for rel_path, content in input_files.items():
            dest = os.path.join(task_dir, rel_path)
            os.makedirs(os.path.dirname(dest) or task_dir, exist_ok=True)
            with open(dest, "wb") as f:
                f.write(content)
        return task_dir

    def _run(self, command: "list[str] | str", cwd: str) -> tuple[str, str, int]:
        """Run ``command`` (str -> shell) with the node's timeout; rc=-1 on timeout."""
        try:
            proc = subprocess.run(
                command, cwd=cwd, capture_output=True, text=True,
                timeout=self.timeout_seconds, shell=isinstance(command, str),
            )
            return proc.stdout, proc.stderr, proc.returncode
        except subprocess.TimeoutExpired as e:
            stdout = self._to_text(e.stdout)
            stderr = self._to_text(e.stderr) + \
                f"\n[RiscvBuildNode] timeout after {self.timeout_seconds}s"
            return stdout, stderr, -1

    @staticmethod
    def _collect(task_dir: str, outputs: list[str]) -> dict[str, bytes]:
        """Read files matching ``outputs`` (globs relative to task_dir; dirs walked)."""
        base = Path(task_dir)
        collected: dict[str, bytes] = {}
        for pattern in outputs:
            for match in base.glob(pattern):
                for p in (match.rglob("*") if match.is_dir() else [match]):
                    if p.is_file():
                        collected[str(p.relative_to(base))] = p.read_bytes()
        return collected

    @staticmethod
    def _to_text(value: "str | bytes | None") -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return value or ""
