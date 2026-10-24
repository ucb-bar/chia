"""Run a FireSim workload on the F2 FPGAs of a farm, one job per FPGA at a time.

:meth:`FireSimManagerNode.run_workload` runs in the calling process: it sends
each job to an FPGA as a :meth:`FireSimManagerNode.run_job` task, and takes down
the FPGAs with no job left through their farm. ``run_job`` runs in the FireSim
container on an F2 instance (``chia.firesim.specs.F2_SIM``). The manager's run
farm is the instance itself (FireSim's ``ExternallyProvisioned`` mode), and
``_MANAGER`` runs FireSim's commands for it on the instance, with no ssh.
"""

from __future__ import annotations

import logging
import os
import socket
import subprocess
import time
from dataclasses import replace
from urllib.request import Request, urlopen

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from chia.aws.manager import Farm
from chia.base.ChiaFunction import ChiaFunction, get
from chia.chipyard.state_def import FireMarshalArtifact
from chia.firesim.fs_bitstream import FSBitstream
from chia.firesim.render import RUN_FARM_USER, render_hwdb, render_runtime_config, stage_workload
from chia.firesim.specs import FPGA_RESOURCE
from chia.firesim.state_def import RunConfig, SimJobResult
from chia.trace.status import report, watch

FIRESIM_DIR = "/home/ray/firesim"
DEPLOY_DIR = f"{FIRESIM_DIR}/deploy"

# `firesim` exits unless sourceme-manager.sh has run: it sets FIRESIM_SOURCED,
# which check_env() requires.
_RUN = r"""set -e
cd "$1"
task="$2"
source sourceme-manager.sh --skip-ssh-setup
cd deploy
python - "$task"
"""

# FireSim's manager with its run farm host commands run on this instance, not
# over ssh: run() runs on the instance through nsenter, as $USER in a login
# shell in its home, as over ssh; put/get/rsync_project copy in the instance's
# home, which the container mounts at the same path.
_MANAGER = r"""
import os, runpy, shlex, sys
import fabric.api, fabric.contrib.project
from fabric.api import abort, env, local, settings
from fabric.operations import _prefix_commands, _prefix_env_vars
from fabric.state import output


def run_on_host(command, **kw):
    command = _prefix_env_vars(_prefix_commands(command, "remote"))
    with settings(command_prefixes=[], warn_only=True):
        out = local(f"sudo nsenter -t 1 -a -- sudo -H -u {os.environ['USER']} /bin/bash -l -c "
                    + shlex.quote(f"cd && {command}"), shell="/bin/bash", capture=True)
    if output.stdout and out:
        print(out, flush=True)
    if output.stderr and out.stderr:
        print(out.stderr, file=sys.stderr, flush=True)
    if out.failed and not env.warn_only:
        abort(f"run() failed (rc={out.return_code}): {command}")
    return out


def put_in_home(local_path, remote_path, **kw):
    return local(f"cp -r {local_path} {remote_path}")


def get_from_home(remote_path, local_path, **kw):
    return local(f"cp -r {remote_path} {local_path}")


def rsync_in_home(remote_dir, local_dir, exclude=(), delete=False, extra_opts="",
                  capture=False, upload=True, default_opts="-pthrvz", **kw):
    exclude = [exclude] if isinstance(exclude, str) else exclude
    options = ("--delete" if delete else "") + "".join(f' --exclude "{e}"' for e in exclude)
    src, dst = (local_dir, remote_dir) if upload else (remote_dir, local_dir)
    return local(f"rsync {options} {default_opts} {extra_opts} {src} {dst}", capture=capture)


fabric.api.run = run_on_host
fabric.api.put = put_in_home
fabric.api.get = get_from_home
fabric.contrib.project.rsync_project = rsync_in_home
sys.argv = [os.path.abspath("firesim"), sys.argv[1]]
runpy.run_path(sys.argv[0], run_name="__main__")
"""


class FireSimManagerNode:
    """Runs ``firesim infrasetup`` and ``firesim runworkload`` on the local FPGA."""

    def __init__(self, timeout_seconds: int | None = None):
        """
        Args:
            timeout_seconds: Wall-clock limit per manager step; None for no limit.
        """
        self.timeout_seconds = timeout_seconds
        self.logger = logging.getLogger("FireSimManagerNode")
        watch("FPGA")          # the process that creates the node prints the FPGA table

    def run_workload(self, farm: Farm, workload: FireMarshalArtifact,
                     bitstream: FSBitstream, config: RunConfig | None = None,
                     teardown: bool = True) -> list[SimJobResult]:
        """Run each job of ``workload`` on an FPGA of ``farm``, one job at a time per
        FPGA, and return the results in the order they finish. Runs in the calling
        process.

        With ``teardown`` and a farm that has its manager, an FPGA with no job left
        goes down at once, and at the end so does the whole farm.
        """
        release = teardown and farm.manager is not None
        pending, results = workload.split(), []
        try:
            running = [self._start(pending.pop(0), bitstream, config)
                       for _ in farm.ips[:len(pending)]]
            while running:
                [ref], running = ray.wait(running)
                results.append(result := get(ref))
                if pending:    # the next job goes to the FPGA that just finished
                    running.append(self._start(pending.pop(0), bitstream, config,
                                               result.node_id))
                elif release:
                    replace(farm, ips=[result.ip]).teardown()
        finally:
            if release:
                farm.teardown()
        return results

    def _start(self, job: FireMarshalArtifact, bitstream: FSBitstream,
               config: RunConfig | None, node_id: str | None = None):
        """Start ``run_job`` as a task; with ``node_id``, on that Ray node."""
        strategy = NodeAffinitySchedulingStrategy(node_id, soft=False) if node_id else None
        return self.run_job.options(scheduling_strategy=strategy).chia_remote(
            self, job=job, bitstream=bitstream, config=config)

    @ChiaFunction(resources={FPGA_RESOURCE: 1})
    def run_job(self, job: FireMarshalArtifact, bitstream: FSBitstream,
                config: RunConfig | None = None) -> SimJobResult:
        """Run the single-job workload ``job`` with ``bitstream`` on this worker's
        FPGA, and collect its results.

        ``config`` holds the FireSim runtime settings (``config_runtime.yaml``).
        Infrasetup and runworkload are one task so both use the same FPGA. Each
        phase is reported on the ``FPGA`` status board.
        """
        host = socket.gethostname()
        report("FPGA", host, job=os.path.splitext(job.json_name)[0], state="staging")
        try:
            result = self._run(host, job, bitstream, config)
        except Exception:
            report("FPGA", host, state="failed")
            raise
        report("FPGA", host, state="done" if result.success else "failed")
        return replace(result, node_id=ray.get_runtime_context().get_node_id(), ip=_public_ip())

    def _run(self, host: str, workload: FireMarshalArtifact, bitstream: FSBitstream,
             config: RunConfig | None) -> SimJobResult:
        t0 = time.monotonic()
        name = stage_workload(DEPLOY_DIR, workload)
        render_runtime_config(DEPLOY_DIR, name, config)
        render_hwdb(DEPLOY_DIR, bitstream)

        log = ""
        for task in ("infrasetup", "runworkload"):
            report("FPGA", host, job=name, state=task)
            self.logger.info(f"firesim {task} for {name}")
            rc, out = self._firesim(task)
            log += f"=== {task} (rc={rc}) ===\n{out[-4000:]}\n"
            if rc != 0:
                report("FPGA", host, detail=f"{task} rc={rc}")
                return SimJobResult(name, success=False, log=log,
                                    duration_seconds=time.monotonic() - t0)

        outputs = self._collect(name)
        uartlog = next((v.decode(errors="replace") for k, v in outputs.items()
                        if os.path.basename(k) == "uartlog"), "")
        return SimJobResult(name, success=True, uartlog=uartlog, outputs=outputs,
                            duration_seconds=time.monotonic() - t0, log=log)

    def _firesim(self, task: str) -> tuple[int, str]:
        """Run one manager task; rc=-1 on timeout."""
        # FireSim names the run farm host's paths after $USER (/home/$USER/aws-fpga),
        # as if the manager ran as that host's user.
        env = {**os.environ, "USER": RUN_FARM_USER}
        try:
            p = subprocess.run(["bash", "-c", _RUN, "_", FIRESIM_DIR, task], env=env,
                               input=_MANAGER, capture_output=True, text=True,
                               timeout=self.timeout_seconds)
            return p.returncode, p.stdout + p.stderr
        except subprocess.TimeoutExpired:
            return -1, f"timeout after {self.timeout_seconds}s"

    @staticmethod
    def _collect(name: str) -> dict[str, bytes]:
        """Files of the newest ``results-workload`` run of ``name``."""
        root = os.path.join(DEPLOY_DIR, "results-workload")
        runs = sorted(d for d in os.listdir(root) if d.endswith(name))
        if not runs:
            return {}
        run_dir = os.path.join(root, runs[-1])
        outputs = {}
        for dirpath, _, files in os.walk(run_dir):
            for f in files:
                path = os.path.join(dirpath, f)
                with open(path, "rb") as fh:
                    outputs[os.path.relpath(path, run_dir)] = fh.read()
        return outputs


def _public_ip() -> str:
    """This machine's public IP, from EC2's instance metadata (IMDSv2)."""
    imds = "http://169.254.169.254/latest"
    token = urlopen(Request(f"{imds}/api/token", method="PUT", headers={
        "X-aws-ec2-metadata-token-ttl-seconds": "60"})).read().decode()
    return urlopen(Request(f"{imds}/meta-data/public-ipv4", headers={
        "X-aws-ec2-metadata-token": token})).read().decode()
