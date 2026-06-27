"""Spike<->Verilator lockstep co-simulation of one ELF via chipyard's Cospike.

Spike is compiled into the simulator (WithCospike + WithTraceIO) and checks every
committed instruction in-process; the run aborts at the first architectural
divergence. The embedded spike takes its ISA from the DUT's isaDTS, so this node
needs no isa argument. Cosim requires a zero-initialized DUT (a RANDOM=0 build
predefine) so unspecified reset state matches spike's zeros; Cospike forwards the
legitimately non-deterministic reads (counters, IDs, LR/SC, device loads).

On a mismatch the run is repeated with +verbose to capture the interleaved
DUT/spike commit logs around the abort (the ``ctx``-line debug window). Composes
VerilatorRunNode for the simulation, adding the cospike plusargs, divergence
parsing, and the verbose re-run.
"""
import gzip
import logging
import re

from chia.base.ChiaFunction import ChiaFunction
from chia.chipyard.state_def import CosimResult
from chia.chipyard.verilator_run_node import VerilatorRunNode

COSIM_PLUSARGS = {"+cospike-enable=1": "", "+cospike-printf=0": ""}

_MISMATCH = re.compile(r"^.*\b(?:PC|wdata) mismatch.*$", re.M)
_PC = re.compile(r"PC mismatch spike ([0-9a-fA-F]+) != DUT ([0-9a-fA-F]+)")
_WDATA = re.compile(r"wdata mismatch reg (\d+) ([0-9a-fA-F]+) != ([0-9a-fA-F]+)")
_SPIKE_COMMIT = re.compile(r"^core\s+\d+:", re.M)
# riscv-dv programs end with a tohost write that fesvr reports as a bad
# syscall; riscv-tests exit cleanly. Either marks completion.
_COMPLETED = ("bad syscall", "*** PASSED ***", "*** FAILED ***")


def _divergence(line: str) -> dict:
    d = {"line": line.strip()}
    if m := _PC.search(line):
        d["spike"], d["dut"] = f"pc 0x{m[1]}", f"pc 0x{m[2]}"
    elif m := _WDATA.search(line):
        d["spike"], d["dut"] = f"x{m[1]} = 0x{m[2]}", f"x{m[1]} = 0x{m[3]}"
    return d


def _cycles(out: str | None):
    m = re.search(r"after\s+(\d+)\s+simulation cycles", out or "")
    return int(m.group(1)) if m else None


class CosimNode:

    logging_name = "CosimNode"

    def __init__(self, ctx: int = 64, logging_level: int = logging.DEBUG):
        self.ctx = ctx  # trace lines kept around the abort point in the window
        self.logger = logging.getLogger(self.logging_name)
        self.logger.setLevel(logging_level)

    @ChiaFunction(resources={"verilator_run": 1})
    def run(self, sim_artifact, elf_content: bytes, elf_name: str,
            work_dir: str, timeout_cycles: int,
            timeout_seconds: int = 4 * 3600) -> CosimResult:
        dut = self._run(sim_artifact, elf_content, elf_name, work_dir,
                        timeout_cycles, timeout_seconds, verbose=False)
        text = (dut.log or "") + "\n" + (dut.out or "")
        mismatch = _MISMATCH.search(text)
        matched = len(_SPIKE_COMMIT.findall(text))
        completed = any(s in text for s in _COMPLETED)
        # Cospike prints-but-continues on tolerated reads (tval); a divergence
        # is a mismatch that actually aborted the run.
        match = matched > 0 and (mismatch is None or completed)
        if match:
            return CosimResult(elf_name, True, matched, completed, None,
                               _cycles(dut.out), None)

        # Re-run with +verbose for the debug window: DUT commit log + spike's,
        # interleaved up to the abort (deterministic thanks to zero-init).
        rerun = self._run(sim_artifact, elf_content, elf_name, work_dir,
                          timeout_cycles, timeout_seconds, verbose=True)
        tail = ((rerun.log or "") + "\n" + (rerun.out or "")).splitlines()[-self.ctx * 4:]
        window = ("=== cospike lockstep abort tail ('core 0:' = spike golden; "
                  "DASM lines = DUT; the mismatch line is the divergence) ===\n"
                  + "\n".join(tail))
        return CosimResult(elf_name, False, matched, completed,
                           _divergence(mismatch.group(0)) if mismatch else None,
                           _cycles(dut.out), gzip.compress(window.encode("utf-8", "replace")))

    def _run(self, sim_artifact, elf_content, elf_name, work_dir,
             timeout_cycles, timeout_seconds, verbose):
        return VerilatorRunNode().run(
            artifact=sim_artifact, test_binary_content=elf_content,
            test_binary_name=elf_name, work_dir=work_dir,
            plusargs={"+loadmem": elf_name, **COSIM_PLUSARGS},
            timeout_cycles=timeout_cycles, timeout_seconds=timeout_seconds,
            verbose=verbose)
