"""Disassemble a RISC-V ELF with `objdump`.

Environment this node needs:

* ``riscv64-unknown-elf-objdump`` on PATH (target=verilator).
* ``riscv64-unknown-linux-gnu-objdump`` on PATH (target=linux).

Both are provisioned by the `chia-riscv-cross` Docker image
(`dockerfiles/RiscvCrossDockerfile`).

Where it sits in the pipeline::

    Binary bytes (from RiscvBuildNode, S3, or anywhere else) get passed into RiscvObjdumpNode.dump(target="verilator" | "linux"), which
    shells out to `<TOOL_PREFIX>objdump <flags> <binary>`.

    RiscvObjdumpNode.dump returns RiscvObjdumpArtifact (dump + target + success + std{out,err} + rc)

This is the standalone counterpart to RiscvBuildNode.build(include_dump=True):
use the build flag when you're compiling and want both outputs in one step,
use this node when you already have an ELF and just want its disassembly.
"""

import logging
import os
import shutil
import subprocess
import uuid
from typing import Literal

from chia.base.ChiaFunction import ChiaFunction
from chia.chipyard.state_def import RiscvObjdumpArtifact


ObjdumpTarget = Literal["verilator", "linux"]

# Single source of truth for objdump toolchain prefix. Mirrors the Makefile's
# per-TARGET TOOL_PREFIX; bump both together if a new target is added.
_TOOL_PREFIX: dict[str, str] = {
    "verilator": "riscv64-unknown-elf-",
    "linux":     "riscv64-unknown-linux-gnu-",
}


class RiscvObjdumpNode:
    """Disassembles a RISC-V ELF with ``objdump``.`
    """

    logging_name = "RiscvObjdumpNode"

    def __init__(
        self,
        timeout_seconds: int = 120,
        logging_level: int = logging.DEBUG,
    ):
        """
        Args:
            timeout_seconds: Wall-clock limit applied to each ``objdump``
                invocation in :meth:`dump`. On expiry the dump returns
                ``returncode=-1`` (never raises); defaults to 120s.
            logging_level: Python logging level for this node's logger.
        """
        self.timeout_seconds = timeout_seconds
        self.logger = logging.getLogger(self.logging_name)
        self.logger.setLevel(logging_level)

    @ChiaFunction(resources={"riscv_build": 1})
    def dump(
        self,
        binary_content: bytes,
        binary_name: str,
        work_dir: str,
        target: ObjdumpTarget = "verilator",
        objdump_flags: str = "-D",
        cleanup_task_dir: bool = True,
    ) -> RiscvObjdumpArtifact:
        """Disassemble `binary_content` with `<TOOL_PREFIX>objdump`.

        Args:
            binary_content: Raw bytes of the RISC-V ELF to disassemble.
            binary_name: Filename to give the ELF on disk (echoed into the
                returned artifact for traceability).
            work_dir: Base directory; a uuid-namespaced task subdir is created
                under it so concurrent runs on one worker don't collide.
            target: ``"verilator"`` selects the ``riscv64-unknown-elf-`` prefix
                (baremetal ELF); ``"linux"`` selects ``riscv64-unknown-linux-gnu-``
                (userspace ELF).
            objdump_flags: Flags forwarded verbatim to ``objdump`` (split on
                whitespace); defaults to ``"-D"`` (disassemble all sections).
            cleanup_task_dir: If True (default), remove the task dir after the
                disassembly is captured.

        Returns:
            RiscvObjdumpArtifact: Carries the disassembly (``dump``), the echoed
            ``binary_name`` and ``target``, and stdout/stderr/returncode.
            ``success=False`` (with empty ``dump``) on failure or timeout.

        Raises:
            ValueError: If ``target`` is not a recognized value.
        """
        if target not in _TOOL_PREFIX:
            raise ValueError(
                f"target must be one of {sorted(_TOOL_PREFIX)} (got {target!r})"
            )

        task_dir = self._setup(binary_content, binary_name, work_dir)
        binary_path = os.path.join(task_dir, binary_name)

        cmd = [f"{_TOOL_PREFIX[target]}objdump", *objdump_flags.split(), binary_path]
        self.logger.info(f"Running: {cmd} (cwd={task_dir})")
        stdout, stderr, returncode = self._run(cmd, cwd=task_dir)

        success = returncode == 0 and stdout != ""
        if not success:
            self.logger.warning(
                f"Objdump failed (target={target}, returncode={returncode}); "
                f"stderr tail: {stderr[-500:]}"
            )

        if cleanup_task_dir:
            shutil.rmtree(task_dir, ignore_errors=True)

        return RiscvObjdumpArtifact(
            binary_name=binary_name,
            dump=stdout,
            target=target,
            success=success,
            stdout=stdout,
            stderr=stderr,
            returncode=returncode,
        )

    @staticmethod
    def _setup(binary_content: bytes, binary_name: str, work_dir: str) -> str:
        """Create a uuid-namespaced task dir under `work_dir`, drop the binary
        into it, and return the task dir path. The uuid keeps concurrent
        objdump runs on one worker from clobbering each other."""
        os.makedirs(work_dir, exist_ok=True)
        task_dir = os.path.join(work_dir, uuid.uuid4().hex[:8])
        os.makedirs(task_dir, exist_ok=True)
        with open(os.path.join(task_dir, binary_name), "wb") as f:
            f.write(binary_content)
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
                     f"\n[RiscvObjdumpNode] timeout after {self.timeout_seconds}s"
            return stdout, stderr, -1

    @staticmethod
    def _to_text(value: str | bytes | None) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return value or ""
