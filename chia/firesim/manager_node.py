"""Run one FireSim job on the FPGA attached to this worker.

This node runs inside the FireSim container on an F2 instance, which Chia
started with ``--net=host --privileged``. The manager therefore sees the host's
network namespace, and its run farm is the single local FPGA reached over
``localhost`` — FireSim's own ``ExternallyProvisioned`` mode. Nothing launches
or terminates run hosts here; :class:`~chia.firesim.sim_splitter.SimSplitter`
owns that.
"""

from __future__ import annotations

import logging
import os
import subprocess
import time

from chia.base.ChiaFunction import ChiaFunction
from chia.firesim.fs_bitstream import FSBitstream
from chia.firesim.render import render_hwdb, render_runtime_config, stage_workload
from chia.firesim.state_def import SimJob, SimJobResult

FIRESIM_DIR = "/home/ray/firesim"
FPGA_RESOURCE = "firesim_fpga"

# `firesim` exits unless sourceme-manager.sh has run (it sets FIRESIM_SOURCED
# and puts deploy/ on PATH). --skip-ssh-setup because the container's entrypoint
# already loaded firesim.pem into an ssh-agent.
_RUN = r"""set -e
cd "$1"
source sourceme-manager.sh --skip-ssh-setup
cd deploy
./firesim "$2"
"""


class FireSimManagerNode:
    """Drives ``firesim infrasetup`` and ``firesim runworkload`` for one FPGA."""

    logging_name = "FireSimManagerNode"

    def __init__(
        self,
        firesim_dir: str = FIRESIM_DIR,
        timeout_seconds: int = 14400,
        logging_level: int = logging.DEBUG,
    ):
        """
        Args:
            firesim_dir: FireSim checkout inside the container.
            timeout_seconds: Wall-clock limit per manager step; on expiry the
                step returns ``returncode=-1`` (never raises).
            logging_level: Logging level for this node's logger.
        """
        self.firesim_dir = firesim_dir
        self.deploy_dir = os.path.join(firesim_dir, "deploy")
        self.timeout_seconds = timeout_seconds
        self.logger = logging.getLogger(self.logging_name)
        self.logger.setLevel(logging_level)

    @ChiaFunction(resources={FPGA_RESOURCE: 1})
    def run_job(
        self,
        job: SimJob,
        bitstream: FSBitstream,
        plusarg_passthrough: str = "",
    ) -> SimJobResult:
        """Stage ``job``, flash the FPGA, run it, and collect the results.

        Infrasetup and runworkload are one task on purpose: as separate Ray
        tasks the scheduler could flash one FPGA and run on another.

        Args:
            job: The workload to run.
            bitstream: FPGA image + the driver built against it. With its
                ``driver_tar`` set FireSim skips ``make driver``, so this
                container needs no chipyard.
            plusarg_passthrough: Extra ``+plusargs`` for the simulator.

        Returns:
            :class:`SimJobResult` with the uartlog, the collected outputs, and
            the manager log tail on failure.
        """
        t0 = time.monotonic()
        render_runtime_config(self.deploy_dir, job, plusarg_passthrough)
        render_hwdb(self.deploy_dir, bitstream)
        stage_workload(self.deploy_dir, job)

        log = ""
        for task in ("infrasetup", "runworkload"):
            self.logger.info(f"firesim {task} for {job.benchmark_name}")
            stdout, stderr, rc = self._firesim(task)
            log += f"=== {task} (rc={rc}) ===\n{stdout[-4000:]}\n{stderr[-4000:]}\n"
            if rc != 0:
                self.logger.warning(
                    f"firesim {task} failed (rc={rc}) for {job.benchmark_name}")
                return SimJobResult(
                    benchmark_name=job.benchmark_name, success=False, log=log,
                    duration_seconds=time.monotonic() - t0)

        uartlog, outputs = self._collect(job)
        return SimJobResult(
            benchmark_name=job.benchmark_name, success=True, uartlog=uartlog,
            outputs=outputs, duration_seconds=time.monotonic() - t0, log=log)

    def _firesim(self, task: str) -> tuple[str, str, int]:
        """Run one manager task; rc=-1 on timeout (never raises)."""
        try:
            proc = subprocess.run(
                ["bash", "-c", _RUN, "_", self.firesim_dir, task],
                capture_output=True, text=True, timeout=self.timeout_seconds)
            return proc.stdout, proc.stderr, proc.returncode
        except subprocess.TimeoutExpired as e:
            stdout = e.stdout.decode(errors="replace") if isinstance(e.stdout, bytes) else (e.stdout or "")
            stderr = e.stderr.decode(errors="replace") if isinstance(e.stderr, bytes) else (e.stderr or "")
            return stdout, stderr + f"\n[FireSimManagerNode] timeout after {self.timeout_seconds}s", -1

    def _collect(self, job: SimJob) -> tuple[str, dict[str, str]]:
        """Read back the newest ``results-workload`` directory for this job."""
        results_root = os.path.join(self.deploy_dir, "results-workload")
        runs = [d for d in _listdir(results_root) if d.endswith(job.benchmark_name)]
        if not runs:
            self.logger.warning(f"No results directory under {results_root}")
            return "", {}
        run_dir = os.path.join(results_root, sorted(runs)[-1])

        uartlog, outputs = "", {}
        for root, _dirs, files in os.walk(run_dir):
            for name in files:
                path = os.path.join(root, name)
                rel = os.path.relpath(path, run_dir)
                try:
                    if os.path.getsize(path) > 10_000_000:
                        self.logger.info(f"Skipping large output {rel}")
                        continue
                    with open(path) as f:
                        content = f.read()
                except (OSError, UnicodeDecodeError):
                    continue  # binary or unreadable
                if name == "uartlog":
                    uartlog = content
                else:
                    outputs[rel] = content
        return uartlog, outputs


def _listdir(path: str) -> list[str]:
    return sorted(os.listdir(path)) if os.path.isdir(path) else []
