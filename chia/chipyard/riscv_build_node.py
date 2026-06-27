"""Cross-compile a C/asm source into a RISC-V ELF.

Environment this node needs:
    * `riscv64-unknown-elf-*` baremetal toolchain on PATH (target=verilator).
    * `riscv64-unknown-linux-gnu-*` Linux toolchain on PATH (target=linux).
    * The harness Makefile at `/opt/riscv-harness/Makefile` plus its
      `include/` headers (rocc.h, mmio.h, marchid.h).

All three are provisioned by the `chia-riscv-cross` Docker image
(`dockerfiles/RiscvCrossDockerfile`).

Where it sits in the pipeline:

    Source bytes gets passed into RiscvBuildNode.build(target="verilator" | "linux"), which calls
    `make -f /opt/riscv-harness/Makefile ...`

    RiscvBuildNode.build returns RiscvBuildArtifact (binary_content + target + success + std{out,err} + rc)
"""

import logging
import os
import shutil
import subprocess
import uuid
from typing import Literal

from chia.base.ChiaFunction import ChiaFunction
from chia.chipyard.state_def import RiscvBuildArtifact


HARNESS_MAKEFILE = "/opt/riscv-harness/Makefile"

BuildTarget = Literal["verilator", "linux"]

# Single source of truth for output naming. Mirrors the Makefile's per-TARGET
# OUTPUT setting; bump both together if a new target is added.
_OUTPUT_NAME: dict[str, "callable[[str], str]"] = {
    "verilator": lambda program: f"{program}.riscv",
    "linux":     lambda program: program,
}

# Source-language -> file extension. The harness Makefile already has both
# `%.o: %.c` and `%.o: %.S` rules and derives OBJS from $(basename $(SRCS)),
# so picking a language is just naming the source file and passing SRCS= to
# make — no Makefile change (and no image rebuild) required.
SourceLang = Literal["c", "asm"]
_LANG_EXT: dict[str, str] = {"c": ".c", "asm": ".S"}


# TODO: this class currently assumes the harness Makefile baked into the
# `chia-riscv-cross` image at /opt/riscv-harness/Makefile is the build
# recipe. We may want callers to supply their own Makefile — either as an
# extra `makefile_path` kwarg on build(), or via a separate
# `build_with_make(makefile, ...)` entry point. Revisit when a workload
# needs flags or rules the baked-in harness doesn't cover.
class RiscvBuildNode:
    """Cross-compiles a C/asm source into a RISC-V ELF via the harness Makefile.

    Each :meth:`build` writes its source into an
    isolated per-call task dir and shells out to the baked-in harness Makefile.
    Runs inside the ``chia-riscv-cross`` image on workers tagged with the
    ``riscv_build`` resource (see module docstring for the toolchain/Makefile
    it depends on).
    """

    logging_name = "RiscvBuildNode"

    def __init__(
        self,
        timeout_seconds: int = 300,
        logging_level: int = logging.DEBUG,
    ):
        """
        Args:
            timeout_seconds: Wall-clock limit applied to each ``make`` invocation
                in :meth:`build`. On expiry the build returns ``returncode=-1``
                (never raises); defaults to 300s.
            logging_level: Python logging level for this node's logger.
        """
        self.timeout_seconds = timeout_seconds
        self.logger = logging.getLogger(self.logging_name)
        self.logger.setLevel(logging_level)

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
        """Cross-compile `source_content` into a RISC-V ELF.

        Args:
            source_content: Raw bytes of the source file to compile.
            program_name: Base name of the program. Names the source file
                (``<program_name>.c`` / ``.S``), the ``PROGRAM=`` Make variable,
                and the output binary.
            work_dir: Base directory; a uuid-namespaced task subdir is created
                under it so concurrent builds on one worker don't collide.
            target: ``"verilator"`` for a baremetal ELF (output
                ``<program_name>.riscv``) or ``"linux"`` for a userspace ELF
                (output ``<program_name>``). Selects the toolchain prefix.
            extra_cflags: Forwarded verbatim as ``EXTRA_CFLAGS=`` to the harness
                Makefile (e.g. ``"-march=rv64gc_zba_zbb"`` for extension ISAs).
            extra_ldflags: Forwarded verbatim as ``EXTRA_LDFLAGS=``.
            include_dump: If True, also run the Makefile's ``dump`` target and
                read the ``objdump -D`` output back into the artifact's ``dump``.
            cleanup_task_dir: If True (default), remove the task dir after the
                build (and after reading back the binary/dump).
            lang: ``"c"`` writes ``<program_name>.c``; ``"asm"`` writes
                ``<program_name>.S`` (preprocessed assembly). Both compile via
                the same Makefile rules.

        Returns:
            RiscvBuildArtifact: Carries the compiled ELF bytes, output
            ``binary_name``, ``target``, and (if ``include_dump``) the
            disassembly. On compile failure/timeout, ``success=False`` with
            empty ``binary_content`` and the captured stdout/stderr/returncode.

        Raises:
            ValueError: If ``target`` or ``lang`` is not a recognized value.
        """
        if target not in _OUTPUT_NAME:
            raise ValueError(
                f"target must be one of {sorted(_OUTPUT_NAME)} (got {target!r})"
            )
        if lang not in _LANG_EXT:
            raise ValueError(
                f"lang must be one of {sorted(_LANG_EXT)} (got {lang!r})"
            )

        source_filename = f"{program_name}{_LANG_EXT[lang]}"
        task_dir = self._setup(source_content, source_filename, work_dir)
        binary_name = _OUTPUT_NAME[target](program_name)
        binary_path = os.path.join(task_dir, binary_name)

        cmd = [
            "make", "-f", HARNESS_MAKEFILE,
            f"TARGET={target}",
            f"PROGRAM={program_name}",
            f"SRCS={source_filename}",
            f"EXTRA_CFLAGS={extra_cflags}",
            f"EXTRA_LDFLAGS={extra_ldflags}",
        ]
        if include_dump:
            cmd.append("dump")
        self.logger.info(f"Running: {cmd} (cwd={task_dir})")
        stdout, stderr, returncode = self._run(cmd, cwd=task_dir)

        binary_content = self._read(binary_path) if returncode == 0 else b""
        success = returncode == 0 and binary_content != b""
        if not success:
            self.logger.warning(
                f"Build failed (target={target}, returncode={returncode}); "
                f"stderr tail: {stderr[-500:]}"
            )

        dump = ""
        if include_dump and success:
            dump_path = os.path.join(task_dir, f"{program_name}.dump")
            dump = self._read(dump_path).decode("utf-8", errors="replace")

        if cleanup_task_dir:
            shutil.rmtree(task_dir, ignore_errors=True)

        return RiscvBuildArtifact(
            binary_name=binary_name,
            binary_content=binary_content,
            target=target,
            success=success,
            stdout=stdout,
            stderr=stderr,
            returncode=returncode,
            dump=dump,
        )

    @staticmethod
    def _setup(source_content: bytes, source_filename: str, work_dir: str) -> str:
        """Create a uuid-namespaced task dir under `work_dir`, drop the source
        file (named `source_filename`, e.g. prog.c or prog.S) into it, and
        return the task dir path. The uuid keeps concurrent builds on one
        worker from clobbering each other."""
        os.makedirs(work_dir, exist_ok=True)
        task_dir = os.path.join(work_dir, uuid.uuid4().hex[:8])
        os.makedirs(task_dir, exist_ok=True)
        with open(os.path.join(task_dir, source_filename), "wb") as f:
            f.write(source_content)
        return task_dir

    def _run(self, cmd: list[str], cwd: str) -> tuple[str, str, int]:
        """Run `cmd` with the node's configured timeout. On timeout, return
        rc=-1 with a tagged stderr instead of raising — keeps the never-raise
        contract that callers branch on `artifact.success`."""
        try:
            proc = subprocess.run(
                cmd, cwd=cwd, capture_output=True, text=True,
                timeout=self.timeout_seconds,
            )
            return proc.stdout, proc.stderr, proc.returncode
        except subprocess.TimeoutExpired as e:
            stdout = self._to_text(e.stdout)
            stderr = self._to_text(e.stderr) + \
                     f"\n[RiscvBuildNode] timeout after {self.timeout_seconds}s"
            return stdout, stderr, -1

    @staticmethod
    def _to_text(value: str | bytes | None) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return value or ""

    @staticmethod
    def _read(path: str) -> bytes:
        """Read the expected output ELF; return b'' if the build never
        produced it (compile error, missing rule, etc.)."""
        try:
            with open(path, "rb") as f:
                return f.read()
        except FileNotFoundError:
            return b""
