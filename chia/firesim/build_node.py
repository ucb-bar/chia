"""Bitstream build orchestrator — manages the full lifecycle of building an
FPGA bitstream on an ephemeral EC2 instance.

Adapted from FireSim's F2BitBuilder.build_bitstream() and
AWSEC2.request_build_host().

Flow:
1. Launch EC2 build instance (z1d.2xlarge, F2 AMI with Vivado)
2. Wait for SSH readiness
3. Pull and start chia-chisel-build Docker container
4. Rsync modified Chisel sources into the container (overlay on baked-in chipyard)
5. Run make replace-rtl inside Docker (Chisel -> FIRRTL -> GoldenGate -> Verilog)
6. Run make driver inside Docker (C++ driver build)
7. Copy generated RTL from Docker container to host filesystem
8. Run build-bitstream.sh natively on host (Vivado synthesis, 24h timeout)
9. Upload tarball to S3, create AGFI (on EC2 instance via aws CLI)
10. Return result to head node, terminate instance
"""

from __future__ import annotations

import base64
import json
import os
import random
import shlex
import string
import subprocess
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from chia.aws.config import AWSConfig, EC2InstanceConfig
from chia.aws.ec2 import launch_ec2_instances, wait_for_instances
from chia.aws.host import EphemeralEC2Host
from chia.cluster.log import get_logger
from chia.cluster.ssh import SSHError
from chia.firesim.config import FireSimBuildConfig
from chia.firesim.state_def import BitstreamBuildResult

logger = get_logger("firesim.build")

# Path to chipyard inside the Docker container (baked into chia-chisel-build)
CONTAINER_CHIPYARD = "/home/ray/chipyard"
CONTAINER_NAME = "chisel-build"


# ---------------------------------------------------------------------------
# Platform dispatch table
# ---------------------------------------------------------------------------
# AWS f2 expects an aws-fpga HDK clone on the build host with the cl_ directory
# placed at hdk/cl/developer_designs/cl_<quintuplet>/, build-bitstream.sh sourced
# after `source hdk_setup.sh`, and `aws ec2 create-fpga-image` to mint an AGFI.
#
# corigine_xb10 (and similar local-PCIe FPGA targets) keep everything inside
# the firesim platforms/<platform>/ tree: cl_firesim/ next to a thin
# build-bitstream.sh that calls Vivado directly, expects --board on the
# command line, and emits a raw .bit file. There's no AGFI step.

@dataclass(frozen=True)
class PlatformBuildSpec:
    """Per-platform knobs for the post-RTL-generation portion of the build.

    Each FireSim platform (f2, corigine_xb10, xilinx_alveo_u250, ...) lays out
    its custom-logic (cl_) directory, build-bitstream.sh script, and bitstream
    packaging differently. One frozen instance per supported platform captures
    those differences so :class:`BitstreamBuilder` can stay platform-agnostic
    and dispatch through ``_PLATFORM_SPECS``.

    Attributes:
        name: FireSim platform name; must match ``FireSimBuildConfig.platform``
            and the ``sims/firesim/platforms/<name>/`` directory.
        sim_output_subdir: Sub-directory under
            ``sims/firesim/sim/output/<here>/`` where ``make driver`` drops the
            host driver binary + shared libs.
        driver_binary: Driver binary name produced by ``make driver``.
        builds_driver: If False, both ``make driver`` and the driver-bundle step
            are skipped (platform ships no deployable host driver).
        needs_aws_fpga: If True, a separate aws-fpga repo is cloned on the host
            and the cl_ directory is placed inside its HDK tree. f2-only.
        aws_fpga_repo_url: Git URL of the aws-fpga HDK repo to clone when
            ``needs_aws_fpga`` is True; None otherwise.
        container_cl_root: Absolute path inside the container to the directory
            that *contains* the cl_ subdir. f2's HDK layout is nested under
            ``platforms/f2/aws-fpga-firesim-f2``; xb10 puts cl_firesim directly
            under ``platforms/corigine_xb10``.
        container_cl_subpath: Sub-path under ``container_cl_root`` that is the
            cl_ directory itself, with ``{quintuplet}`` substituted at build
            time. f2 uses an HDK-flavored path that includes the chisel
            quintuplet; the self-contained platforms use ``cl_{quintuplet}``.
        container_build_script: Absolute path inside the container to
            build-bitstream.sh.
        needs_hdk_setup: Whether to ``cd $aws-fpga && source hdk_setup.sh``
            before invoking Vivado (f2 HDK flow).
        extra_build_script_args: Extra args appended to build-bitstream.sh
            (e.g. ``("--board", "xb10")``).
        creates_agfi: If True, run ``aws ec2 create-fpga-image`` after Vivado to
            mint an AGFI. If False, the .bit file at
            ``<host_cl_dir>/<bitstream_artifact_relpath>`` is uploaded to S3
            directly and surfaced via ``BitstreamBuildResult.bitstream_path``.
        bitstream_artifact_relpath: Path (relative to the cl_ directory) to the
            raw bitstream artifact for non-AGFI platforms; None when
            ``creates_agfi`` is True.
    """
    name: str
    # Sub-directory under sims/firesim/sim/output/<here>/ where `make driver`
    # drops the host driver binary + shared libs.
    sim_output_subdir: str
    # Driver binary name produced by `make driver`.
    driver_binary: str
    # If False, both `make driver` and the driver-bundle step are skipped.
    builds_driver: bool
    # If True, a separate aws-fpga repo is cloned on the host and the cl_
    # directory is placed inside its HDK tree. f2-only.
    needs_aws_fpga: bool
    aws_fpga_repo_url: str | None
    # Path inside the container (relative to nothing — full absolute path)
    # to the directory that *contains* the cl_ subdir. f2's HDK layout is
    # nested under platforms/f2/aws-fpga-firesim-f2; xb10 puts cl_firesim
    # directly under platforms/corigine_xb10.
    container_cl_root: str
    # Sub-path under container_cl_root that is the cl_ directory itself.
    # f2 uses an HDK-flavored path that includes the chisel quintuplet; xb10
    # is just "cl_firesim".
    container_cl_subpath: str
    # Path inside the container to build-bitstream.sh.
    container_build_script: str
    # Whether to `cd $aws-fpga && source hdk_setup.sh` before invoking Vivado.
    needs_hdk_setup: bool
    # Extra args appended to build-bitstream.sh (e.g. ("--board", "xb10")).
    extra_build_script_args: tuple[str, ...]
    # If True, run `aws ec2 create-fpga-image` after Vivado to mint an AGFI.
    # If False, the .bit file at <host_cl_dir>/<bitstream_artifact_relpath>
    # is uploaded to S3 directly and surfaced via BitstreamBuildResult.bitstream_path.
    creates_agfi: bool
    bitstream_artifact_relpath: str | None


F2_SPEC = PlatformBuildSpec(
    name="f2",
    sim_output_subdir="f2",
    driver_binary="FireSim-f2",
    builds_driver=True,
    needs_aws_fpga=True,
    aws_fpga_repo_url="https://github.com/firesim/aws-fpga-firesim-f2.git",
    container_cl_root=f"{CONTAINER_CHIPYARD}/sims/firesim/platforms/f2/aws-fpga-firesim-f2",
    container_cl_subpath="hdk/cl/developer_designs/cl_{quintuplet}",
    container_build_script=f"{CONTAINER_CHIPYARD}/sims/firesim/platforms/f2/build-bitstream.sh",
    needs_hdk_setup=True,
    extra_build_script_args=(),
    creates_agfi=True,
    bitstream_artifact_relpath=None,
)

XB10_SPEC = PlatformBuildSpec(
    name="corigine_xb10",
    sim_output_subdir="corigine_xb10",
    driver_binary="FireSim-corigine_xb10",
    builds_driver=False,
    needs_aws_fpga=False,
    aws_fpga_repo_url=None,
    container_cl_root=f"{CONTAINER_CHIPYARD}/sims/firesim/platforms/corigine_xb10",
    # `make replace-rtl PLATFORM=corigine_xb10` copies cl_firesim → cl_<quintuplet>
    # via fpga.mk's $(fpga_work_dir) rule, so the materialized cl_dir is keyed
    # by the chisel quintuplet (not the literal "cl_firesim" template name).
    container_cl_subpath="cl_{quintuplet}",
    container_build_script=f"{CONTAINER_CHIPYARD}/sims/firesim/platforms/corigine_xb10/build-bitstream.sh",
    needs_hdk_setup=False,
    extra_build_script_args=("--board", "xb10"),
    creates_agfi=False,
    bitstream_artifact_relpath="vivado_proj/firesim.bit",
)

# Xilinx Alveo U250: same self-contained pattern as xb10. The build script lives
# under platforms/xilinx_alveo_u250/, takes --board au250, and emits firesim.bit
# at vivado_proj/firesim.bit. No host driver, no aws-fpga, no AGFI.
U250_SPEC = PlatformBuildSpec(
    name="xilinx_alveo_u250",
    sim_output_subdir="xilinx_alveo_u250",
    driver_binary="FireSim-xilinx_alveo_u250",  # unused (builds_driver=False)
    builds_driver=False,
    needs_aws_fpga=False,
    aws_fpga_repo_url=None,
    container_cl_root=f"{CONTAINER_CHIPYARD}/sims/firesim/platforms/xilinx_alveo_u250",
    container_cl_subpath="cl_{quintuplet}",
    container_build_script=f"{CONTAINER_CHIPYARD}/sims/firesim/platforms/xilinx_alveo_u250/build-bitstream.sh",
    needs_hdk_setup=False,
    extra_build_script_args=("--board", "au250"),
    creates_agfi=False,
    bitstream_artifact_relpath="vivado_proj/firesim.bit",
)

_PLATFORM_SPECS: dict[str, PlatformBuildSpec] = {
    F2_SPEC.name: F2_SPEC,
    XB10_SPEC.name: XB10_SPEC,
    U250_SPEC.name: U250_SPEC,
}


class BitstreamBuilder:
    """Orchestrates a full FPGA bitstream build on an ephemeral EC2 host.

    One instance drives a single build recipe end-to-end. It runs on the
    FireSim manager / head node, but the head node does NOT need a chipyard
    installation: all heavy build steps execute remotely on a freshly launched
    ephemeral EC2 build host. Chisel elaboration (``make replace-rtl``) and the
    host driver build (``make driver``) run inside a Docker container
    (chia-chisel-build) on that host, while Vivado synthesis runs natively on
    the host. The instance is terminated when :meth:`build` returns.
    """

    def __init__(
        self,
        build_config: FireSimBuildConfig,
        aws_config: AWSConfig,
        s3_bucket: str,
        source_overlay_dir: str | None = None,
        setup_fn: callable | None = None,
        docker_image: str = "chia-chisel-build",
        instance_type: str = "z1d.2xlarge",
        cluster_name: str = "chia",
        results_dir: str | None = None,
        local_log_dir: str | None = None,
        ami_id: str | None = None,
        vivado_stack_kb: int | None = None,
        volume_size_gb: int = 200,
        bitstream_local_dir: str | None = None,
        copy_all_artifacts: bool = True,
    ) -> None:
        """Configure a builder for a single FireSim build recipe.

        Args:
            build_config: The FireSim build recipe to build (the build
                quintuplet: platform, target_project, design, target_config,
                platform_config, plus build mode / incremental / PR knobs).
            aws_config: AWS credentials, region, and networking config used to
                launch and reach the ephemeral build host.
            s3_bucket: S3 bucket for build artifacts — the DCP/AFI tarball,
                logs, driver bundle, and (for non-AGFI platforms) the raw .bit.
            source_overlay_dir: Local dir with modified Chisel sources to overlay
                onto the container's chipyard. Files are rsynced into
                ``/home/ray/chipyard/`` inside the container, on top of the
                baked-in chipyard. If None, the baked-in chipyard in the Docker
                image is used as-is.
            setup_fn: Optional setup callable retained on the instance (the
                per-build setup hook is passed separately to :meth:`build`).
            docker_image: Docker image used for Chisel elaboration and the
                driver build (the chia-chisel-build image with chipyard baked in).
            instance_type: EC2 instance type for the build host (default
                ``z1d.2xlarge``, an F2-class host with Vivado on the AMI).
            cluster_name: Chia cluster name, applied as the ``chia-cluster`` tag
                on the launched build instance.
            results_dir: Local directory to store build results. If None,
                results are not downloaded to the head node (the AGFI / S3
                artifacts are still created).
            local_log_dir: Local directory to mirror the build host's logs
                (vivado.log and optionally the full cl_dir) into, under
                ``<local_log_dir>/<build_id>/``. If None, logs are not copied back.
            ami_id: Override AMI for the build host; None uses the launcher's
                default FPGA Developer AMI.
            vivado_stack_kb: If set, build-bitstream.sh is patched in place to
                inject ``-stack <kb>`` into every standalone ``vivado`` call,
                raising Vivado's per-thread Tcl stack size for deeply-recursive
                scripts in larger designs. None leaves the script unmodified.
            volume_size_gb: EBS root volume size (GB) for the build host.
                Default 200 GB suits f2 / Rocket-class designs; larger designs
                (e.g. MegaBoom) need more headroom for Vivado synth intermediates.
            bitstream_local_dir: If set, after a successful build the final
                artifact (.bit for non-AGFI platforms, driver-bundle.tar.gz for
                f2) plus build-info.json are copied from S3 to
                ``<bitstream_local_dir>/<build_id>/`` on the head node. The
                directory is created up front. None skips this local copy.
            copy_all_artifacts: If True, the whole cl_dir (Vivado reports,
                checkpoints, runme logs) is rsync'd to
                ``<local_log_dir>/<build_id>/cl/`` on both success and failure.
                False copies only vivado.log (lighter on local disk).
        """
        self.build_config = build_config
        self.aws_config = aws_config
        self.s3_bucket = s3_bucket
        self.source_overlay_dir = source_overlay_dir
        self.setup_fn = setup_fn
        self.docker_image = docker_image
        self.instance_type = instance_type
        self.cluster_name = cluster_name
        self.results_dir = results_dir
        self.local_log_dir = local_log_dir
        self.ami_id = ami_id
        # When set, after a successful build the final artifact (.bit for non-
        # AGFI platforms, driver-bundle.tar.gz for f2) plus build-info.json are
        # copied from S3 to {bitstream_local_dir}/{build_id}/ on the head node.
        # Lets downstream tooling consume bitstreams without an S3 round-trip.
        self.bitstream_local_dir = bitstream_local_dir
        # When True, the whole cl_dir (Vivado reports, checkpoints, runme logs)
        # is rsync'd to local_log_dir/<build_id>/cl/ on both success and
        # failure. False = only vivado.log is copied (lighter on local disk).
        self.copy_all_artifacts = copy_all_artifacts
        # Pre-create local output dirs so they exist even when builds fail
        # before reaching the per-build mkdir paths. Without this, a totally
        # failing run leaves no /scratch/.../bitstreams/ at all.
        if self.bitstream_local_dir:
            os.makedirs(self.bitstream_local_dir, exist_ok=True)
        if self.local_log_dir:
            os.makedirs(self.local_log_dir, exist_ok=True)
        # When set, build-bitstream.sh is patched in place before invocation
        # to inject `-stack <kb>` into every standalone `vivado` call. This
        # raises Vivado's per-thread Tcl stack size; the default is too small
        # for some deeply-recursive scripts in larger designs.
        self.vivado_stack_kb = vivado_stack_kb
        # EBS root volume size for the build host. The default 200 GB is
        # enough for f2 / Rocket-class designs but too small for MegaBoom
        # Vivado synth: Vivado writes massive intermediates to .Xil/ and
        # firesim.runs/ during synth_design (10s of GB), and the disk fills
        # up before route_design even starts. 1000 GB is generous headroom
        # at ~$2.50/day extra cost (gp3 $0.08/GB-month).
        self.volume_size_gb = volume_size_gb
        self.aws_creds_dir = aws_config.aws_creds_dir or os.path.expanduser("~/.aws")
        # Platform-specific dispatch table (paths, build-script args, AGFI vs .bit).
        # Raises KeyError fast if the recipe targets an unsupported platform.
        self._spec: PlatformBuildSpec = _PLATFORM_SPECS[build_config.platform]

    def _chisel_quintuplet(self) -> str:
        bc = self.build_config
        parts = [bc.platform, bc.target_project, bc.design, bc.target_config, bc.platform_config]
        return "-".join(parts)

    def _target_project_makefrag_arg(self) -> str:
        """Resolve TARGET_PROJECT_MAKEFRAG for the make command."""
        bc = self.build_config
        if bc.target_project_makefrag:
            return f"TARGET_PROJECT_MAKEFRAG={bc.target_project_makefrag}"
        if bc.target_project == "firesim":
            return (
                f"TARGET_PROJECT_MAKEFRAG="
                f"{CONTAINER_CHIPYARD}/generators/firechip/chip/src/main/makefrag/firesim"
            )
        if bc.target_project == "bridges":
            return (
                f"TARGET_PROJECT_MAKEFRAG="
                f"{CONTAINER_CHIPYARD}/generators/firechip/bridgestubs/src/main/makefrag/bridges"
            )
        return ""

    def _make_cmd(self, target: str) -> str:
        """Build the full make command string for execution inside Docker."""
        bc = self.build_config
        makefrag = self._target_project_makefrag_arg()
        makefrag_part = f" {makefrag}" if makefrag else ""
        # Use most of the instance's RAM for the JVM heap (default 8G is
        # too small for large designs like Boom).  The value can be
        # overridden via JAVA_HEAP_SIZE in the build config environment.
        java_heap = getattr(bc, "java_heap_size", None) or "48G"
        return (
            f"cd {CONTAINER_CHIPYARD}/sims/firesim && "
            f"source sourceme-manager.sh --skip-ssh-setup && "
            f"cd sim && "
            f"make JAVA_HEAP_SIZE={java_heap}"
            f" PLATFORM={bc.platform}"
            f" TARGET_PROJECT={bc.target_project}"
            f"{makefrag_part}"
            f" DESIGN={bc.design}"
            f" TARGET_CONFIG={bc.target_config}"
            f" PLATFORM_CONFIG={bc.platform_config}"
            f" {target}"
        )

    def _docker_exec(self, host: EphemeralEC2Host, cmd: str,
                     timeout: int = 300, check: bool = True):
        """Run a command inside the Docker container on the remote host."""
        # bash -lc ensures .bashrc is sourced (env.sh for chipyard/firesim)
        escaped = cmd.replace("'", "'\\''")
        return host.run(
            f"sudo docker exec {CONTAINER_NAME} bash -lc '{escaped}'",
            timeout=timeout, check=check,
        )

    def _run_long(self, host: EphemeralEC2Host, cmd: str,
                  timeout: int = 7200, label: str = "command",
                  poll_interval: int = 60,
                  max_ssh_failure_seconds: int = 1800,
                  ) -> subprocess.CompletedProcess:
        """Run a long-running command via nohup to survive SSH drops.

        Writes the command to a script, runs it in the background,
        and polls for completion.

        Files live under ``/home/ubuntu/`` (on the EBS root) rather than
        ``/tmp/`` because Ubuntu's ``systemd-tmpfiles`` clears ``/tmp`` on
        boot — if the instance hangs and we have to reboot to recover,
        a ``/tmp``-resident log gets wiped, defeating post-mortem analysis.

        ``max_ssh_failure_seconds`` bounds how long we'll tolerate
        unreachable SSH before giving up. The historical pattern: the
        EC2 instance's network stack hangs (ENA driver wedge, AWS
        networking event, etc.), the kernel keeps running but is
        unreachable, and polls quietly time out forever while burning
        EC2 hours. After this many seconds of *continuous* failure we
        exit the poll loop with a synthetic non-zero return code so the
        caller can treat it as a build failure and tear down the instance.
        We track wall-clock time rather than failure count because callers
        use very different ``poll_interval`` values (60s for setup, 600s
        for Vivado), and a streak count is meaningless without it.
        """
        import subprocess as _sp

        # Persistent paths on EBS — survive reboot so we can post-mortem
        # the log even after a hung-instance reboot.
        base = "/home/ubuntu/chia-long-cmd"
        script_path = f"{base}.sh"
        log_path = f"{base}.log"
        pid_path = f"{base}.pid"
        rc_path = f"{base}.rc"

        # Write the command as a script
        host.run(
            f"cat > {script_path} << 'CHIA_CMD_EOF'\n"
            f"#!/bin/bash\n{cmd}\n"
            f"echo $? > {rc_path}\n"
            f"CHIA_CMD_EOF",
            timeout=30,
        )
        host.run(f"chmod +x {script_path}", timeout=10)
        host.run(f"rm -f {rc_path}", timeout=10)

        # Launch in background
        host.run(
            f"nohup bash {script_path} > {log_path} 2>&1 & echo $! > {pid_path}",
            timeout=30,
        )
        logger.info(f"  [{label}] Started in background, polling...")

        deadline = time.monotonic() + timeout
        ssh_first_fail_t: float | None = None  # None when SSH is healthy
        ssh_dead = False
        while time.monotonic() < deadline:
            time.sleep(poll_interval)
            ssh_failed = False
            ssh_err = ""
            try:
                check = host.run(
                    f"if [ -f {rc_path} ]; then echo done; "
                    f"elif kill -0 $(cat {pid_path}) 2>/dev/null; then echo running; "
                    f"else echo done; fi",
                    timeout=60, check=False,
                )
                # host.run(check=False) does NOT raise on SSH-level errors —
                # it returns CompletedProcess with rc=255 and stderr set
                # (e.g. "Connection timed out during banner exchange"). The
                # only reliable SSH-success signal is that stdout contains
                # the literal token the remote bash always echoes.
                if "done" in check.stdout:
                    ssh_first_fail_t = None
                    break
                elif "running" in check.stdout:
                    ssh_first_fail_t = None
                    elapsed = time.monotonic() - (deadline - timeout)
                    logger.info(f"  [{label}] Still running ({elapsed/60:.0f}min)...")
                else:
                    ssh_failed = True
                    ssh_err = (check.stderr or check.stdout).strip()[:200] or \
                              f"empty stdout (rc={check.returncode})"
            except Exception as e:
                ssh_failed = True
                ssh_err = repr(e)[:200]

            if ssh_failed:
                now = time.monotonic()
                if ssh_first_fail_t is None:
                    ssh_first_fail_t = now
                ssh_dead_for = now - ssh_first_fail_t
                logger.warning(
                    f"  [{label}] SSH poll failed "
                    f"(unreachable {ssh_dead_for/60:.1f}min of "
                    f"{max_ssh_failure_seconds/60:.0f}min budget): {ssh_err}"
                )
                if ssh_dead_for >= max_ssh_failure_seconds:
                    logger.error(
                        f"  [{label}] SSH unreachable for "
                        f"{ssh_dead_for/60:.1f}min — instance appears hung. "
                        f"Aborting poll loop. Caller should treat as build failure."
                    )
                    ssh_dead = True
                    break

        if ssh_dead:
            # SSH is permanently down. Don't try to read the log/rc — those
            # will just hang or time out. Return a synthetic CompletedProcess
            # so the caller's failure-handling path runs cleanly.
            ssh_dead_for = time.monotonic() - (ssh_first_fail_t or time.monotonic())
            return _sp.CompletedProcess(
                args=cmd, returncode=124,  # 124 = timeout (gnu coreutils convention)
                stdout=(
                    f"chia _run_long: aborted after {ssh_dead_for/60:.0f}min "
                    f"of unreachable SSH. Instance hung; log may be readable "
                    f"on the EBS volume after reboot at {log_path}.\n"
                ),
                stderr="",
            )

        # Get return code and log
        rc_result = host.run(f"cat {rc_path} 2>/dev/null || echo 1", timeout=60, check=False)
        returncode = int(rc_result.stdout.strip() or "1")
        log_result = host.run(f"tail -200 {log_path}", timeout=60, check=False)

        return _sp.CompletedProcess(
            args=cmd, returncode=returncode,
            stdout=log_result.stdout, stderr=log_result.stderr,
        )

    def _setup_docker(self, host: EphemeralEC2Host) -> None:
        """Install Docker if needed, pull the image, and start the build container."""
        # Install Docker if not present (FPGA Developer AMI doesn't ship it)
        # Wait for cloud-init (user-data installs Docker at boot)
        logger.info("Waiting for cloud-init to finish...")
        host.run("sudo cloud-init status --wait", timeout=600, check=False)
        check = host.run("which docker", timeout=10, check=False)
        if check.returncode != 0:
            logger.warning("Docker not installed by cloud-init — installing manually")
            # Wait for unattended-upgrades (or any other apt consumer) to release
            # the dpkg lock on freshly-booted Ubuntu AMIs before running apt.
            # Without this, apt-get fails instantly with exit 100 when it
            # collides with the default at-boot security-upgrades service.
            host.run(
                "while sudo fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1; "
                "do sleep 5; done && "
                "sudo apt-get update -qq && "
                "sudo apt-get install -y -qq docker.io > /dev/null",
                timeout=600,
            )
            host.run("sudo usermod -aG docker ubuntu", timeout=10)
            host.run("sudo systemctl start docker", timeout=30)

        # Authenticate to ghcr.io if a GitHub token is available
        # Check GITHUB_TOKEN env var first, then fall back to token file
        gh_token = os.environ.get("GITHUB_TOKEN", "").strip()
        token_path = os.path.expanduser("~/.config/chia/github-token")
        if not gh_token and os.path.isfile(token_path):
            with open(token_path) as f:
                gh_token = f.read().strip()

        if gh_token:
            logger.info("Logging into ghcr.io on build host")
            import urllib.request
            req = urllib.request.Request(
                "https://api.github.com/user",
                headers={"Authorization": f"Bearer {gh_token}"},
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                gh_user = json.loads(resp.read()).get("login", "token")
            logger.info(f"Resolved GitHub user: {gh_user}")

            # Write token to a temp file on the remote host
            host.run(
                f"echo {shlex.quote(gh_token)} > /tmp/.gh-token",
                timeout=10,
            )
            result = host.run(
                f"cat /tmp/.gh-token | sudo docker login ghcr.io -u {gh_user} --password-stdin",
                timeout=30, check=False,
            )
            host.run("rm -f /tmp/.gh-token", timeout=10)
            if result.returncode != 0:
                logger.warning(f"ghcr.io login failed: {result.stderr.strip()}")
            else:
                logger.info("ghcr.io login succeeded")

        # All docker commands via sudo to avoid newgrp issues. Retry around
        # `docker pull` because GHCR (and other registries) returns transient
        # `error from registry: retry-after: <ms>` indications under burst load
        # (e.g. 30+ concurrent pulls of the same image). Docker exits non-zero
        # on these even though they're just throttle hints; an exponential
        # backoff retry absorbs them in practice. Real errors (auth,
        # manifest-not-found, real disk-full) still surface on the last attempt.
        logger.info(f"Pulling Docker image: {self.docker_image}")
        TRANSIENT = (
            "retry-after", "error from registry",
            "toomanyrequests", "TOOMANYREQUESTS",
            "TLS handshake timeout", "i/o timeout",
            "connection reset", "connection refused",
            "net/http: TLS handshake",
        )
        max_attempts = 5
        for attempt in range(1, max_attempts + 1):
            pull_result = self._run_long(
                host, f"sudo docker pull {self.docker_image}",
                timeout=1800, label=f"docker-pull (attempt {attempt}/{max_attempts})",
                poll_interval=30,
            )
            if pull_result.returncode == 0:
                break
            is_transient = any(p in pull_result.stdout for p in TRANSIENT)
            if not is_transient or attempt == max_attempts:
                raise SSHError(
                    f"Docker pull failed (attempt {attempt}/{max_attempts}): "
                    f"{pull_result.stdout[-500:]}"
                )
            backoff = min(60, 5 * (2 ** (attempt - 1)))   # 5, 10, 20, 40, 60 s
            logger.warning(
                f"[{host.instance_id}] Pull attempt {attempt} hit transient "
                f"registry error; sleeping {backoff}s before retry"
            )
            time.sleep(backoff)

        logger.info("Starting build container")
        host.run(
            f"sudo docker run -d --name {CONTAINER_NAME} "
            f"{self.docker_image} sleep infinity",
            timeout=60,
        )

    def _install_u250_board_files_if_needed(self, host: EphemeralEC2Host) -> None:
        """Make Vivado on the host find Alveo U250 board files.

        Vivado doesn't ship Alveo board files (per AMD UG1289). Two steps
        are needed on Vivado 2025.1 — placing files in the xhub dir is
        NOT sufficient because xhub auto-scan is gated on a per-user
        opt-in we don't set (confirmed empirically: a prior run had the
        files at the canonical path and Vivado still errored with
        ``[Board 49-71] The board_part definition was not found for
        xilinx.com:au250:part0:1.3``):

        1. Sparse-clone au250/ from Xilinx/open-nic-shell (NOT
           XilinxBoardStore — that repo only carries Kintex/Versal/Zynq
           eval boards) into Vivado's default xhub path.
        2. Per-user ``Vivado_init.tcl`` with
           ``set_param board.repoPaths <path>`` so Vivado actually scans
           that directory at startup.

        Idempotent. No-op for non-U250 platforms.
        """
        if self._spec.name != "xilinx_alveo_u250":
            return
        res = host.run(
            "ls -d /opt/Xilinx/Vivado/*/ /tools/Xilinx/Vivado/*/ 2>/dev/null "
            "| sort -V | tail -1 | sed 's:/$::'",
            timeout=10, check=False,
        )
        vivado_dir = res.stdout.strip()
        if not vivado_dir:
            raise SSHError(
                "Could not locate Vivado install on build host "
                "(searched /opt/Xilinx/Vivado/* and /tools/Xilinx/Vivado/*) — "
                "is this AMI a Vivado/FPGA Developer AMI?"
            )
        target_parent = (
            f"{vivado_dir}/data/xhub/boards/XilinxBoardStore/boards/Xilinx"
        )
        # Step 1: sparse-clone au250 board files into the xhub dir.
        #
        # TODO: GitHub-hosted source is convenient but not durable — every U250
        # build depends on Xilinx/open-nic-shell being reachable and unchanged.
        # Better long-term: mirror board_files/Xilinx/au250/ once to
        # s3://firesim-chia-builds/board_files/au250-<rev>.tar.gz and aws s3 cp
        # from there. Same install location, removes the public-Internet
        # dependency, and pins to a known revision.
        check = host.run(
            f"sudo test -d {target_parent}/au250 && echo present || echo missing",
            timeout=10, check=False,
        )
        if "present" in check.stdout:
            logger.info(
                f"au250 board files already present at {target_parent}/au250"
            )
        else:
            logger.info(f"Installing au250 board files into {target_parent}")
            host.run(
                "set -e && rm -rf /tmp/au250-bf && "
                "git clone --depth=1 --filter=blob:none --sparse "
                "https://github.com/Xilinx/open-nic-shell.git /tmp/au250-bf && "
                "cd /tmp/au250-bf && "
                "git sparse-checkout set board_files/Xilinx/au250 && "
                f"sudo mkdir -p {target_parent} && "
                f"sudo cp -r board_files/Xilinx/au250 {target_parent}/au250 && "
                "rm -rf /tmp/au250-bf",
                timeout=180,
            )
            logger.info(f"au250 board files installed at {target_parent}/au250")
        # Step 2: per-user Vivado_init.tcl with set_param board.repoPaths.
        # Without this Vivado 2025.1 does not scan the xhub dir at all
        # (gated on a per-user opt-in we don't set).
        vivado_version = vivado_dir.rsplit("/", 1)[-1]
        repo_path = f"{vivado_dir}/data/xhub/boards/XilinxBoardStore"
        init_dir = f"/home/ubuntu/.Xilinx/Vivado/{vivado_version}"
        init_file = f"{init_dir}/Vivado_init.tcl"
        set_param_line = f"set_param board.repoPaths {repo_path}"
        host.run(
            f"mkdir -p {init_dir} && "
            f"grep -qxF {shlex.quote(set_param_line)} {init_file} 2>/dev/null || "
            f"echo {shlex.quote(set_param_line)} >> {init_file}",
            timeout=10,
        )
        logger.info(f"Vivado_init.tcl at {init_file} contains: {set_param_line}")

    def _overlay_sources(self, host: EphemeralEC2Host) -> None:
        """Rsync modified Chisel sources into the Docker container."""
        if not self.source_overlay_dir:
            return

        # rsync to a staging dir on the EC2 host, then docker cp into container
        staging = "/tmp/chipyard-overlay"
        host.run(f"rm -rf {staging} && mkdir -p {staging}")
        host.rsync_up(
            f"{self.source_overlay_dir}/",
            f"{staging}/",
            exclude=[".git", "*.o", "*.a"],
        )
        host.run(
            f"sudo docker cp {staging}/. {CONTAINER_NAME}:{CONTAINER_CHIPYARD}/",
            timeout=120,
        )
        host.run(f"rm -rf {staging}")
        logger.info("Overlaid modified sources into container")

    # ---- Incremental / PR reference resolution ----

    def _resolve_incremental_ref(
        self, host: EphemeralEC2Host, cl_dir: str, base_build_id: str,
    ) -> tuple[str | None, str | None]:
        """Download DCPs from a previous build on S3 for incremental compile.

        base_build_id can be a build_ref (e.g. "group/build_id" or just "build_id").

        Returns (incr_synth_ref, incr_impl_ref) paths on the build host.
        """
        ref_dir = f"{cl_dir}/build/incremental_ref"
        host.run(f"mkdir -p {ref_dir}", timeout=10)

        s3_ckpt = (
            f"s3://{self.s3_bucket}/builds/"
            f"{base_build_id}/cl/build/checkpoints"
        )
        # Also try legacy path without recipe name prefix
        # (older builds used builds/{recipe}/{build_id}/ format)

        # Find the CL OOC synthesis DCP (*.CL.post_synth.dcp) which contains
        # incremental reuse metadata from -incremental_synth.
        # Falls back to the linked post_synth if OOC not found.
        ls = host.run(
            f"aws s3 ls {s3_ckpt}/ 2>/dev/null | grep 'CL.post_synth.dcp' | head -1 | awk '{{print $4}}'",
            timeout=60, check=False,
        )
        if not ls.stdout.strip():
            ls = host.run(
                f"aws s3 ls {s3_ckpt}/ 2>/dev/null | grep 'cl_.*post_synth.dcp' | head -1 | awk '{{print $4}}'",
                timeout=60, check=False,
            )
        synth_name = ls.stdout.strip()
        incr_synth = None
        if synth_name:
            host.run(f"aws s3 cp {s3_ckpt}/{synth_name} {ref_dir}/post_synth.dcp", timeout=300)
            incr_synth = f"{ref_dir}/post_synth.dcp"
            logger.info(f"Incremental synth ref: {synth_name}")

        # Find post_route.dcp (or .VIOLATED.dcp or full_routed.dcp)
        ls = host.run(
            f"aws s3 ls {s3_ckpt}/ 2>/dev/null | grep -E 'post_route|full_routed' | head -1 | awk '{{print $4}}'",
            timeout=60, check=False,
        )
        impl_name = ls.stdout.strip()
        incr_impl = None
        if impl_name:
            host.run(f"aws s3 cp {s3_ckpt}/{impl_name} {ref_dir}/post_route.dcp", timeout=300)
            incr_impl = f"{ref_dir}/post_route.dcp"
            logger.info(f"Incremental impl ref: {impl_name}")

        if not incr_synth and not incr_impl:
            logger.warning(f"No incremental reference DCPs found for build {base_build_id}")
        return incr_synth, incr_impl

    def _resolve_pr_base(
        self, host: EphemeralEC2Host, cl_dir: str, base_build_id: str,
    ) -> dict[str, str | None]:
        """Download PR base artifacts from a previous build on S3.

        Returns dict with keys: full_routed_dcp, abstract_shell_dcp,
        base_synth_dcp, partition_cell, restore_pblock_tcl,
        post_recombine_dcp, post_opt_dcp.
        """
        pr_dir = f"{cl_dir}/build/pr_base"
        host.run(f"mkdir -p {pr_dir}", timeout=10)

        s3_pr = (
            f"s3://{self.s3_bucket}/builds/"
            f"{base_build_id}/pr-base"
        )

        files = {
            "full_routed.dcp": True,        # required
            "abstract_shell.dcp": False,
            "post_synth.dcp": True,         # required
            "pr_metadata.json": True,       # required
            "post_recombine.dcp": False,
            "post_opt.dcp": False,
            "restore_pr_pblock.tcl": False,
        }

        paths: dict[str, str | None] = {}
        for filename, required in files.items():
            dl = host.run(
                f"aws s3 cp {s3_pr}/{filename} {pr_dir}/{filename}",
                timeout=300, check=False,
            )
            if dl.returncode == 0:
                paths[filename] = f"{pr_dir}/{filename}"
                logger.info(f"PR base artifact: {filename}")
            elif required:
                raise SSHError(f"Required PR base artifact not found: {s3_pr}/{filename}")

        # Extract partition_cell from pr_metadata.json
        partition_cell = None
        meta = host.run(f"cat {pr_dir}/pr_metadata.json", timeout=10, check=False)
        if meta.returncode == 0:
            try:
                data = json.loads(meta.stdout)
                pr_modules = data.get("pr_modules", [])
                if pr_modules:
                    partition_cell = pr_modules[0].get("partition_cell")
                    logger.info(f"PR partition cell: {partition_cell}")
            except json.JSONDecodeError:
                logger.warning("Could not parse pr_metadata.json")

        return {
            "full_routed_dcp": paths.get("full_routed.dcp"),
            "abstract_shell_dcp": paths.get("abstract_shell.dcp"),
            "base_synth_dcp": paths.get("post_synth.dcp"),
            "partition_cell": partition_cell,
            "restore_pblock_tcl": paths.get("restore_pr_pblock.tcl"),
            "post_recombine_dcp": paths.get("post_recombine.dcp"),
            "post_opt_dcp": paths.get("post_opt.dcp"),
        }

    def _upload_pr_base(
        self, host: EphemeralEC2Host, cl_dir: str, s3_prefix: str,
    ) -> None:
        """Upload PR base artifacts to S3 after a successful PR base build."""
        s3_dest = f"s3://{self.s3_bucket}/{s3_prefix}/pr-base"
        ckpt = f"{cl_dir}/build/checkpoints"
        reports = f"{cl_dir}/build/reports"

        upload_map = {
            f"{ckpt}/*.full_routed.dcp": "full_routed.dcp",
            f"{ckpt}/*.abstract_shell.dcp": "abstract_shell.dcp",
            f"{ckpt}/*.post_synth.dcp": "post_synth.dcp",
            f"{ckpt}/*.post_route.dcp": "post_recombine.dcp",
            f"{ckpt}/*.post_opt.dcp": "post_opt.dcp",
            f"{ckpt}/pr_metadata.json": "pr_metadata.json",
            f"{reports}/restore_pr_pblock.tcl": "restore_pr_pblock.tcl",
        }

        logger.info(f"Uploading PR base artifacts to {s3_dest}/")
        for src_glob, dest_name in upload_map.items():
            host.run(
                f'for f in {src_glob}; do '
                f'[ -f "$f" ] && aws s3 cp "$f" {s3_dest}/{dest_name} && '
                f'echo "Uploaded {dest_name}"; done',
                timeout=300, check=False,
            )

    def _build_extra_flags(self, host: EphemeralEC2Host, cl_dir: str,
                           build_log_lines: list[str]) -> str:
        """Resolve incremental/PR references and return extra build-bitstream.sh flags."""
        bc = self.build_config
        flags = ""

        # Incremental compile
        if bc.incremental_base_build_id:
            build_log_lines.append("=== Resolving incremental references ===")
            incr_synth, incr_impl = self._resolve_incremental_ref(
                host, cl_dir, bc.incremental_base_build_id,
            )
            if incr_synth:
                flags += f" --incr_synth_ref {incr_synth}"
            if incr_impl:
                flags += f" --incr_impl_ref {incr_impl}"
            build_log_lines.append(
                f"Incremental refs: synth={'yes' if incr_synth else 'no'}, "
                f"impl={'yes' if incr_impl else 'no'}"
            )

        # Partial reconfiguration
        if bc.enable_pr:
            flags += " --enable_pr true"
            if bc.pr_module_name:
                flags += f" --pr_module_name {bc.pr_module_name}"

            if bc.pr_base_build_id:
                # RM build — resolve base artifacts
                build_log_lines.append("=== Resolving PR base references ===")
                pr = self._resolve_pr_base(host, cl_dir, bc.pr_base_build_id)
                for key, flag in [
                    ("full_routed_dcp", "--pr_full_routed_dcp"),
                    ("abstract_shell_dcp", "--pr_abstract_shell_dcp"),
                    ("base_synth_dcp", "--pr_base_synth_dcp"),
                    ("restore_pblock_tcl", "--pr_restore_pblock_tcl"),
                    ("post_recombine_dcp", "--pr_post_recombine_dcp"),
                    ("post_opt_dcp", "--pr_post_opt_dcp"),
                ]:
                    if pr.get(key):
                        flags += f" {flag} {pr[key]}"
                cell = pr.get("partition_cell") or bc.pr_partition_cell
                if cell:
                    flags += f" --pr_partition_cell {cell}"
                build_log_lines.append(f"PR RM build using base: {bc.pr_base_build_id}")
            else:
                build_log_lines.append("PR base build (will create abstract shell)")

        return flags

    def _post_build_uploads(self, host: EphemeralEC2Host, cl_dir: str,
                            s3_prefix: str, build_log_lines: list[str]) -> None:
        """Upload PR base artifacts and record build mode after a successful build."""
        bc = self.build_config
        # Upload PR base artifacts if this was a PR base build
        if bc.enable_pr and not bc.pr_base_build_id:
            build_log_lines.append("=== Uploading PR base artifacts ===")
            self._upload_pr_base(host, cl_dir, s3_prefix)
            build_log_lines.append("PR base artifacts uploaded")

    def _build_mode(self) -> str:
        bc = self.build_config
        if bc.enable_pr and bc.pr_base_build_id:
            return "pr_rm"
        elif bc.enable_pr:
            return "pr_base"
        elif bc.incremental_base_build_id:
            return "incremental"
        return "standard"

    def _create_agfi_remote(self, host: EphemeralEC2Host,
                            cl_dir: str,
                            s3_prefix: str) -> tuple[str | None, str | None, str]:
        """Create AGFI from build results on the EC2 instance.

        Uploads the DCP tarball to S3 and creates the FPGA image, all via
        aws CLI on the EC2 instance.

        Args:
            host: SSH connection to the build instance.
            cl_dir: Path to the cl_ directory on the build host.
            s3_prefix: S3 key prefix for this build (e.g. builds/Recipe/20260402-091500-a1b2c3d4).

        Returns:
            (agfi, afi, hwdb_entry) or (None, None, error_msg)
        """
        checkpoint_dir = f"{cl_dir}/build/checkpoints"

        # Find .tar file
        result = host.run(f"ls {checkpoint_dir}/*.tar 2>/dev/null || true",
                          timeout=30, check=False)
        tar_files = [l.strip() for l in result.stdout.strip().split("\n") if l.strip()]
        if not tar_files:
            return None, None, f"No .tar files found in {checkpoint_dir}"

        tarpath = tar_files[-1]
        tarname = os.path.basename(tarpath)
        s3_key = f"{s3_prefix}/dcp/{tarname}"
        logs_key = f"{s3_prefix}/logs/"

        # Upload to S3
        logger.info(f"Uploading {tarname} to s3://{self.s3_bucket}/{s3_key}")
        host.run(
            f"aws s3 cp {tarpath} s3://{self.s3_bucket}/{s3_key}",
            timeout=600,
        )

        # Create FPGA image
        logger.info("Creating FPGA image...")
        result = host.run(
            f'aws ec2 create-fpga-image '
            f'--input-storage-location Bucket={self.s3_bucket},Key={s3_key} '
            f'--logs-storage-location Bucket={self.s3_bucket},Key={logs_key} '
            f'--name "{self.build_config.name}"',
            timeout=120,
        )
        ids = json.loads(result.stdout)
        agfi = ids["FpgaImageGlobalId"]
        afi = ids["FpgaImageId"]
        logger.info(f"AGFI: {agfi}, AFI: {afi}")

        # Poll for completion
        logger.info("Waiting for FPGA image to become available...")
        state = "pending"
        while state == "pending":
            time.sleep(10)
            check_result = host.run(
                f"aws ec2 describe-fpga-images --fpga-image-id {afi}",
                timeout=60, check=False,
            )
            if check_result.returncode == 0:
                desc = json.loads(check_result.stdout)
                state = desc["FpgaImages"][0]["State"]["Code"]
                logger.info(f"  FPGA image state: {state}")

        if state != "available":
            return None, None, f"FPGA image entered unexpected state: {state}"

        hwdb_entry = (
            f"{self.build_config.name}:\n"
            f"    agfi: {agfi}\n"
            f"    deploy_quintuplet_override: null\n"
            f"    custom_runtime_config: null\n"
        )
        return agfi, afi, hwdb_entry

    def build(self,
              setup: Callable[..., Any] | None = None,
              setup_args: tuple[Any, ...] = (),
              instance_name: str = "chia-firesim",
              vivado_poll_interval: int = 120,
              instance_prefix: str | None = None,
              vivado_timeout: int = 86400, # 24 hours
              ) -> BitstreamBuildResult:
        """Execute the full build pipeline.

        Launches the ephemeral EC2 build host, runs Chisel elaboration and
        (where applicable) the driver build inside Docker, runs Vivado synthesis
        natively on the host, then either mints an AGFI or uploads the raw .bit
        to S3, and finally terminates the instance.

        Args:
            setup: Optional callable executed between Step 4 (overlay sources) and Step 5 (replace-rtl).
                Called as ``setup(host, docker_exec, *setup_args)`` where *host* is the
                :class:`EphemeralEC2Host` and *docker_exec* is :meth:`_docker_exec`.
            setup_args: Extra positional arguments forwarded to *setup* after *host* and *docker_exec*.
            instance_name: Base name for the launched build instance; the
                build_id is appended to form the full EC2 ``Name`` tag.
            vivado_poll_interval: Seconds between status polls while waiting on
                the long-running Vivado synthesis step.
            instance_prefix: Optional prefix prepended to *instance_name*
                (``<prefix>_<instance_name>``); None uses *instance_name* as-is.
            vivado_timeout: Maximum wall-clock seconds to allow Vivado synthesis
                before treating the build as failed (default 86400 = 24 hours).

        Returns:
            BitstreamBuildResult with success/failure info, the build log, and
            the AGFI/AFI + hwdb_entry (for AGFI platforms) or bitstream_path
            (for non-AGFI platforms) when successful.

        Raises:
            KeyError: If ``build_config.platform`` is not a supported platform
                (raised earlier, at construction, via the ``_PLATFORM_SPECS``
                lookup).
            SSHError: If a remote build step fails — e.g. the Docker pull, Chisel
                ``replace-rtl`` elaboration, the driver build, or required
                incremental/PR base artifacts cannot be resolved.
        """
        if self.aws_config.aws_creds_dir:
            os.environ["AWS_SHARED_CREDENTIALS_FILE"] = os.path.join(self.aws_creds_dir, "credentials")
            os.environ["AWS_CONFIG_FILE"] = os.path.join(self.aws_creds_dir, "config")

        build_log_lines: list[str] = []
        instance_ids: list[str] = []
        quintuplet = self._chisel_quintuplet()

        # Up-front diagnostics: a misaligned platform_config (e.g. an f2 config
        # with PLATFORM=corigine_xb10) silently mangles the chisel quintuplet
        # and `make replace-rtl` either elaborates the wrong design or fails
        # with a hard-to-trace Scala error. Log the full build_config + the
        # selected platform spec so the cause is visible in the build log
        # without having to grep through 4000 lines of Vivado output.
        bc_summary = (
            f"name={self.build_config.name} "
            f"platform={self.build_config.platform} "
            f"target_project={self.build_config.target_project} "
            f"design={self.build_config.design} "
            f"target_config={self.build_config.target_config} "
            f"platform_config={self.build_config.platform_config} "
            f"fpga_frequency={self.build_config.fpga_frequency} "
            f"build_strategy={self.build_config.build_strategy}"
        )
        build_log_lines.append(f"FireSimBuildConfig: {bc_summary}")
        logger.info(f"FireSimBuildConfig: {bc_summary}")
        build_log_lines.append(f"Chisel quintuplet: {quintuplet}")
        logger.info(f"Chisel quintuplet: {quintuplet}")
        spec_summary = (
            f"name={self._spec.name} "
            f"needs_aws_fpga={self._spec.needs_aws_fpga} "
            f"needs_hdk_setup={self._spec.needs_hdk_setup} "
            f"creates_agfi={self._spec.creates_agfi} "
            f"builds_driver={self._spec.builds_driver} "
            f"extra_build_script_args={self._spec.extra_build_script_args} "
            f"bitstream_artifact_relpath={self._spec.bitstream_artifact_relpath}"
        )
        build_log_lines.append(f"PlatformBuildSpec: {spec_summary}")
        logger.info(f"PlatformBuildSpec: {spec_summary}")

        # Build ID: user-specified or auto-generated {recipe}-{timestamp}-{uuid}
        ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        if self.build_config.build_id:
            build_id = self.build_config.build_id
        else:
            short_id = uuid.uuid4().hex[:8]
            build_id = f"{self.build_config.name}-{ts}-{short_id}"
        # S3 path: builds/{group}/{build_id}/ or builds/{build_id}/
        group = self.build_config.build_group
        build_ref = f"{group}/{build_id}" if group else build_id
        s3_prefix = f"builds/{build_ref}"
        build_log_lines.append(f"Build ID: {build_id}")
        logger.info(f"Build ID: {build_id}")
        build_log_lines.append(f"Build ref: {build_ref}")
        logger.info(f"Build ref: {build_ref}")

        try:
            # Step 1: Launch EC2 build instance
            build_log_lines.append("=== Step 1: Launch EC2 build instance ===")
            logger.info("=== Step 1: Launch EC2 build instance ===")
            # User-data script runs as root at boot, before SSH is ready.
            # Waits for unattended-upgrades to finish, then installs Docker.
            user_data = (
                "#!/bin/bash\n"
                "while fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1; do sleep 5; done\n"
                # awscli is needed for the post-Vivado S3 sync (cl_dir, vivado.log,
                # build-info.json, .bit file). Some Ubuntu-based FPGA Developer
                # AMIs ship without it — install unconditionally so we never
                # silently lose post-build artifacts to a missing CLI.
                "apt-get update -qq && apt-get install -y -qq docker.io awscli > /dev/null\n"
                "systemctl start docker\n"
                "usermod -aG docker ubuntu\n"
            )
            inst_config = EC2InstanceConfig(
                instance_type=self.instance_type,
                volume_size_gb=self.volume_size_gb,
                market=self.build_config.market,
                ami_id=self.ami_id,
                tags={
                    "chia-cluster": self.cluster_name,
                    "chia-op": "build",
                    "chia-recipe": self.build_config.name,
                },
                user_data=user_data,
            )
            effective_name = f"{instance_prefix}_{instance_name}" if instance_prefix else instance_name
            instances = launch_ec2_instances(self.aws_config, inst_config, count=1, instance_name=f"{effective_name}-{build_id}")
            instance_ids = [i.instance_id for i in instances]
            build_log_lines.append(f"Launched instance: {instance_ids[0]}")
            logger.info(f"Launched instance: {instance_ids[0]}")

            # Step 2: Wait for readiness
            ready_instances = wait_for_instances(instance_ids, region=self.aws_config.region)
            ec2_inst = ready_instances[0]
            build_log_lines.append(f"Instance ready: {ec2_inst.private_ip}")
            logger.info(f"Instance ready: {ec2_inst.private_ip}")

            with EphemeralEC2Host(ec2_inst, self.aws_config) as host:
                # Replace instance_ids so finally block doesn't double-terminate
                instance_ids = []

                host.wait_ready(timeout=300)

                # Push AWS credentials so the host can use aws CLI (S3, FPGA)
                if os.path.isdir(self.aws_creds_dir):
                    host.run("mkdir -p ~/.aws", timeout=10)
                    host.rsync_up(f"{self.aws_creds_dir}/", "/home/ubuntu/.aws/")
                    logger.info("Pushed AWS credentials to build host")

                # Step 3: Start Docker container
                build_log_lines.append("=== Step 3: Start Docker container ===")
                logger.info("=== Step 3: Start Docker container ===")
                self._setup_docker(host)
                build_log_lines.append(f"Container started: {self.docker_image}")
                logger.info(f"Container started: {self.docker_image}")

                # Step 4: Overlay modified Chisel sources
                if self.source_overlay_dir:
                    build_log_lines.append("=== Step 4: Overlay modified sources ===")
                    logger.info("=== Step 4: Overlay modified sources ===")
                    self._overlay_sources(host)
                    build_log_lines.append("Sources overlaid")
                    logger.info("Sources overlaid")

                # Step 4b: Run optional setup callable inside Docker
                if setup is not None:
                    setup(host, self._docker_exec, *setup_args)

                # Step 5: Run replace-rtl inside Docker
                build_log_lines.append("=== Step 5: Replace RTL (Chisel elaboration) ===")
                logger.info("=== Step 5: Replace RTL (Chisel elaboration) ===")
                logger.info(f"Running replace-rtl for {quintuplet}")
                replace_rtl_cmd = self._make_cmd("replace-rtl")
                # Log the full make command — when Chisel elaboration fails,
                # confirming the exact PLATFORM / PLATFORM_CONFIG / TARGET_CONFIG
                # passed to make is the first thing we want to check.
                build_log_lines.append(f"replace-rtl command: {replace_rtl_cmd}")
                logger.info(f"replace-rtl command: {replace_rtl_cmd}")
                escaped_rtl = replace_rtl_cmd.replace("'", "'\\''")
                rtl_result = self._run_long(
                    host,
                    f"sudo docker exec {CONTAINER_NAME} bash -lc '{escaped_rtl}'",
                    timeout=7200, label="replace-rtl",
                )
                if rtl_result.returncode != 0:
                    # Capture more context on Chisel failures — the actual Scala
                    # error tends to be 200-500 lines deep in the log.
                    raise SSHError(
                        f"replace-rtl failed (exit={rtl_result.returncode}). "
                        f"Last 4000 chars of stdout:\n{rtl_result.stdout[-4000:]}"
                    )
                build_log_lines.append("RTL generation complete")
                logger.info("RTL generation complete")

                # Step 6 + 6b: Build host driver and bundle it.
                # Skipped on platforms that don't ship a deployable driver
                # (e.g. corigine_xb10 — somebody else owns deployment there).
                driver_s3_path = None
                host_driver: str | None = None
                if self._spec.builds_driver:
                    build_log_lines.append("=== Step 6: Build driver ===")
                    logger.info("=== Step 6: Build driver ===")
                    logger.info(f"Building driver for {quintuplet}")
                    driver_cmd = self._make_cmd("driver")
                    escaped_drv = driver_cmd.replace("'", "'\\''")
                    drv_result = self._run_long(
                        host,
                        f"sudo docker exec {CONTAINER_NAME} bash -lc '{escaped_drv}'",
                        timeout=3600, label="driver",
                    )
                    if drv_result.returncode != 0:
                        raise SSHError(f"driver build failed: {drv_result.stdout[-1000:]}")
                    build_log_lines.append("Driver build complete")
                    logger.info("Driver build complete")

                    # Step 6b: Bundle driver outputs and upload to S3
                    # make driver produces a binary + shared libs, not a tarball.
                    # Create the tarball inside the container from the output dir.
                    container_output_dir = (
                        f"{CONTAINER_CHIPYARD}/sims/firesim/sim/output/{self._spec.sim_output_subdir}"
                        f"/{quintuplet}"
                    )
                    container_driver_tarball = f"{container_output_dir}/driver-bundle.tar.gz"
                    host_driver = "/tmp/driver-bundle.tar.gz"

                    # List what make driver produced (for debugging)
                    ls_result = self._docker_exec(
                        host, f"ls -la {container_output_dir}/",
                        timeout=30, check=False,
                    )
                    if ls_result.stdout.strip():
                        build_log_lines.append(f"Driver output dir:\n{ls_result.stdout.strip()}")
                        logger.info(f"Driver output dir contents:\n{ls_result.stdout.strip()}")

                    # Create tarball from driver binary + all non-trivial shared libs.
                    # Use ldd to discover deps, exclude only core system libs that
                    # are guaranteed to exist on any Ubuntu AMI (libc, libstdc++,
                    # libm, libpthread, libz, etc.). Include everything else
                    # (libriscv.so, libdwarf.so, libgmp.so, etc.).
                    driver_bin = self._spec.driver_binary
                    self._docker_exec(
                        host,
                        f"cd {container_output_dir} && "
                        f"ldd {driver_bin} | grep '=>' | awk '{{print $3}}' | "
                        f"grep -v -E '(libc\\.so|libstdc\\+\\+|libm\\.so|libpthread|libdl\\.so|librt\\.so|libgcc_s|libz\\.so|libzstd|libelf)' | "
                        f"while read lib; do cp -L \"$lib\" . 2>/dev/null; done; "
                        f"tar -czf driver-bundle.tar.gz {driver_bin} *.so *.so.* 2>/dev/null || "
                        f"tar -czf driver-bundle.tar.gz {driver_bin}",
                        timeout=120,
                    )
                    host.run(
                        f"sudo docker cp {CONTAINER_NAME}:{container_driver_tarball} {host_driver}",
                        timeout=120,
                    )
                    # Driver tarball is at host_driver; upload deferred to after
                    # AGFI creation so we can key the S3 path by AGFI.
                    build_log_lines.append("Driver bundle ready for upload")
                    logger.info("Driver bundle ready for upload")
                else:
                    build_log_lines.append(
                        f"=== Step 6: Skipped (platform {self._spec.name} does not build a driver) ==="
                    )
                    logger.info(
                        f"Step 6 (driver build) skipped for platform {self._spec.name}"
                    )

                # Step 7: Stage cl_ directory on the host (and aws-fpga, if needed)
                build_log_lines.append("=== Step 7: Stage cl_ directory on host ===")
                logger.info("=== Step 7: Stage cl_ directory on host ===")

                spec = self._spec
                cl_subpath = spec.container_cl_subpath.format(quintuplet=quintuplet)
                # host_awsfpga is only meaningful for platforms that need the
                # aws-fpga HDK clone; xb10-style platforms leave it as None.
                host_awsfpga: str | None = None

                if spec.needs_aws_fpga:
                    # f2: clone aws-fpga so hdk_setup.sh can resolve encrypted IP,
                    # then drop the cl_ subtree into developer_designs/.
                    host_awsfpga = "/home/ubuntu/aws-fpga"
                    host_cl_dir = f"{host_awsfpga}/{cl_subpath}"

                    clone_check = host.run(
                        "test -d /home/ubuntu/aws-fpga/.git && echo exists || echo missing",
                        timeout=10, check=False,
                    )
                    if "missing" in clone_check.stdout:
                        clone_result = self._run_long(
                            host,
                            f"git clone --recurse-submodules "
                            f"{spec.aws_fpga_repo_url} /home/ubuntu/aws-fpga",
                            timeout=1800, label="aws-fpga-clone", poll_interval=30,
                        )
                        if clone_result.returncode != 0:
                            raise SSHError(f"aws-fpga clone failed: {clone_result.stdout[-500:]}")

                    # Copy generated cl_ directory from Docker via tar
                    # (docker cp rejects relative symlinks that cross the copy boundary)
                    host.run(f"mkdir -p {host_cl_dir}")
                    self._docker_exec(
                        host,
                        f"cd {spec.container_cl_root} && tar cf /tmp/cl_dir.tar {cl_subpath}",
                        timeout=120,
                    )
                    host.run(
                        f"sudo docker cp {CONTAINER_NAME}:/tmp/cl_dir.tar /tmp/cl_dir.tar",
                        timeout=120,
                    )
                    host.run(
                        f"cd {host_awsfpga} && tar xf /tmp/cl_dir.tar",
                        timeout=120,
                    )

                    # Copy build-bitstream.sh from Docker (fallback in case the
                    # cl_ directory shipped without one).
                    host.run(
                        f"sudo docker cp {CONTAINER_NAME}:{spec.container_build_script} {host_cl_dir}/ "
                        f"2>/dev/null || true",
                        timeout=30, check=False,
                    )

                    # Overlay modified build scripts (incremental/PR support) from S3.
                    # These scripts may not be in the Docker image or upstream aws-fpga
                    # yet, so we maintain them as a separate overlay tarball on S3.
                    scripts_s3 = f"s3://{self.s3_bucket}/build-scripts/firesim-build-scripts.tar"
                    overlay_result = host.run(
                        f"aws s3 cp {scripts_s3} /tmp/firesim-build-scripts.tar",
                        timeout=60, check=False,
                    )
                    if overlay_result.returncode == 0:
                        # Extract tarball and overlay all modified files onto the
                        # host's aws-fpga clone and cl_ directory.
                        host.run(
                            f"cd /tmp && tar xf firesim-build-scripts.tar && "
                            # build-bitstream.sh → cl_dir
                            f"cp -f build-bitstream.sh {host_cl_dir}/build-bitstream.sh && "
                            # HDK common scripts (aws_build_dcp_from_cl.py, build_all.tcl, TCLs)
                            f"cp -rf aws-fpga-firesim-f2/hdk/common/shell_stable/build/scripts/* "
                            f"  {host_awsfpga}/hdk/common/shell_stable/build/scripts/ && "
                            # CL-level scripts (synth, PR, metadata, etc.)
                            f"mkdir -p {host_cl_dir}/build/scripts && "
                            f"cp -rf aws-fpga-firesim-f2/hdk/cl/developer_designs/cl_firesim/build/scripts/* "
                            f"  {host_cl_dir}/build/scripts/ && "
                            # CL-level constraints (SLR pblocks for PR)
                            f"mkdir -p {host_cl_dir}/build/constraints && "
                            f"cp -rf aws-fpga-firesim-f2/hdk/cl/developer_designs/cl_firesim/build/constraints/* "
                            f"  {host_cl_dir}/build/constraints/",
                            timeout=60, check=False,
                        )
                        # Verify overlay applied
                        verify = host.run(
                            f"grep -c 'incremental_synth\\|INCR_SYNTH_REF\\|enable_pr' "
                            f"{host_cl_dir}/build/scripts/synth_cl_firesim.tcl "
                            f"{host_cl_dir}/build-bitstream.sh 2>/dev/null",
                            timeout=10, check=False,
                        )
                        logger.info(f"Overlaid modified build scripts from S3 (verify: {verify.stdout.strip()})")
                        build_log_lines.append("Build scripts overlaid from S3")
                    else:
                        logger.info("No build script overlay found on S3, using defaults")

                    # Fix ownership on all extracted files
                    host.run(f"sudo chown -R ubuntu:ubuntu {host_cl_dir}", timeout=60)
                    host.run(
                        f"sudo chown -R ubuntu:ubuntu "
                        f"{host_awsfpga}/hdk/common/shell_stable/build/scripts",
                        timeout=60, check=False,
                    )
                else:
                    # xb10 (and any future local-PCIe platforms): no aws-fpga clone,
                    # no HDK script overlay. Tar+untar the materialized cl_<quintuplet>
                    # straight to /home/ubuntu/<cl_subpath>/, then copy build-bitstream.sh
                    # next to it so the vivado_patch_part can sed it in place.
                    host_cl_dir = f"/home/ubuntu/{cl_subpath}"
                    build_log_lines.append(
                        f"xb10 staging: container_cl_root={spec.container_cl_root}, "
                        f"cl_subpath={cl_subpath}, host_cl_dir={host_cl_dir}"
                    )
                    logger.info(
                        f"xb10 staging: container_cl_root={spec.container_cl_root}, "
                        f"cl_subpath={cl_subpath}, host_cl_dir={host_cl_dir}"
                    )

                    # Diagnostic: show what cl_* dirs actually exist inside the
                    # platform_root. If make replace-rtl produced a different
                    # quintuplet (because platform_config disagreed with platform)
                    # the cl_<expected> won't be there and we want to see what is.
                    ls_root = self._docker_exec(
                        host,
                        f"ls -la {spec.container_cl_root}/ 2>&1 | head -40",
                        timeout=30, check=False,
                    )
                    if ls_root.stdout.strip():
                        build_log_lines.append(
                            f"container_cl_root contents:\n{ls_root.stdout.strip()}"
                        )
                        logger.info(
                            f"container_cl_root contents:\n{ls_root.stdout.strip()}"
                        )

                    # Hard-fail with a clear message if the expected cl_dir is
                    # missing — the alternative is a cryptic tar error.
                    cl_check = self._docker_exec(
                        host,
                        f"test -d {spec.container_cl_root}/{cl_subpath} && echo found || echo missing",
                        timeout=10, check=False,
                    )
                    if "missing" in cl_check.stdout:
                        raise SSHError(
                            f"Expected cl_dir not found in container at "
                            f"{spec.container_cl_root}/{cl_subpath}. "
                            f"This usually means `make replace-rtl` produced a "
                            f"different quintuplet (check platform / platform_config / "
                            f"target_config alignment). "
                            f"container_cl_root listing was: {ls_root.stdout.strip()}"
                        )

                    self._docker_exec(
                        host,
                        f"cd {spec.container_cl_root} && tar cf /tmp/cl_dir.tar {cl_subpath}",
                        timeout=120,
                    )
                    host.run(
                        f"sudo docker cp {CONTAINER_NAME}:/tmp/cl_dir.tar /tmp/cl_dir.tar",
                        timeout=120,
                    )
                    host.run(
                        f"cd /home/ubuntu && tar xf /tmp/cl_dir.tar",
                        timeout=120,
                    )

                    # build-bitstream.sh lives next to cl_firesim in the firesim
                    # tree, not inside it. Drop a copy inside host_cl_dir so the
                    # vivado_stack patch + invocation logic below stays uniform.
                    host.run(
                        f"sudo docker cp {CONTAINER_NAME}:{spec.container_build_script} "
                        f"{host_cl_dir}/build-bitstream.sh",
                        timeout=30,
                    )
                    host.run(f"sudo chmod +x {host_cl_dir}/build-bitstream.sh", timeout=10)

                    host.run(f"sudo chown -R ubuntu:ubuntu {host_cl_dir}", timeout=60)

                    # Verify the staged tree on the host — top-level directory
                    # listing answers "does cl_<quintuplet>/{scripts,design,...} look right?"
                    ls_staged = host.run(
                        f"ls -la {host_cl_dir}/ 2>&1 | head -40",
                        timeout=30, check=False,
                    )
                    if ls_staged.stdout.strip():
                        build_log_lines.append(
                            f"host cl_dir contents:\n{ls_staged.stdout.strip()}"
                        )
                        logger.info(
                            f"host cl_dir contents:\n{ls_staged.stdout.strip()}"
                        )

                build_log_lines.append("RTL and build scripts extracted to host")
                logger.info("RTL and build scripts extracted to host")

                # Step 8: Stop Docker container (no longer needed)
                # The cl_dir is already extracted to the host filesystem, so
                # whatever happens to the container from here is purely cleanup
                # — it MUST NOT kill the build. Two failure modes we've seen:
                #   1. `docker stop` SIGTERM → 10s grace → SIGKILL on a
                #      memory-saturated host (48 GB Chisel JVM heap still
                #      pinned in the container) can stretch past 60s, blowing
                #      the SSH timeout.
                #   2. The instance can transiently be slow right after Chisel
                #      finishes — docker daemon contention on shutdown.
                # Bump timeout to 300s and swallow any exception (including
                # subprocess.TimeoutExpired which `check=False` doesn't catch).
                # The instance gets terminated anyway in the finally block, so
                # leaving the container running until then is fine.
                try:
                    host.run(
                        f"sudo docker stop {CONTAINER_NAME} && sudo docker rm {CONTAINER_NAME}",
                        timeout=300, check=False,
                    )
                except Exception as e:
                    logger.warning(
                        f"docker stop/rm of {CONTAINER_NAME} failed (best-effort, ignoring): "
                        f"{type(e).__name__}: {e}"
                    )
                    build_log_lines.append(
                        f"docker stop/rm failed (ignored): {type(e).__name__}: {e}"
                    )

                # Vivado doesn't ship Alveo board files — install before launch.
                self._install_u250_board_files_if_needed(host)

                # Step 9: Resolve incremental/PR references, then run Vivado
                extra_flags = self._build_extra_flags(host, host_cl_dir, build_log_lines)

                build_log_lines.append("=== Step 9: Run Vivado build ===")
                logger.info("=== Step 9: Run Vivado build ===")
                if extra_flags:
                    logger.info(f"Extra build flags: {extra_flags}")
                    build_log_lines.append(f"Extra build flags: {extra_flags}")
                # Bump the OS thread stack via `ulimit -s` before launching
                # Vivado. Vivado's `-mode batch -source main.tcl` does deep
                # Tcl recursion during XDC processing on MegaBoom-class
                # designs and segfaults at the default 8 MB Linux stack.
                #
                # We tried two earlier approaches that DON'T work:
                #   1. Sed-patching build-bitstream.sh to add `-stack <kb>`:
                #      catches only the outer vivado, not the synth_1 child.
                #   2. PATH-shim that rewrites every vivado call to add
                #      `-stack <kb>`: turns out Vivado 2025.1 silently exits
                #      0 with no output for ANY non-default `-stack` value
                #      (2000 is default; 4000/8000/16000 all silently fail
                #      under `-mode batch -source <tcl>`). Confirmed by
                #      direct experiment on the FPGA Dev AMI on 2026-05-02.
                #
                # `ulimit -s <kb>` operates at the OS level — it sets the
                # thread stack soft-limit for the bash shell and any
                # processes it exec's, including all of Vivado's children.
                # Default Linux is 8192 KB; bumping to 32768 KB gives 4×
                # headroom and fits within the unprivileged hard limit
                # (`unlimited` requires root). The `-stack` flag becomes
                # unnecessary — it doesn't actually map to OS stack anyway.
                vivado_shim_part = ""
                if self.vivado_stack_kb is not None:
                    kb = self.vivado_stack_kb
                    vivado_shim_part = (
                        f"ulimit -s {kb} && "
                        f"echo \"chia: bumped OS thread stack to {kb} KB (was $(ulimit -s) default 8192)\" && "
                    )
                    build_log_lines.append(
                        f"Will set ulimit -s {kb} before invoking Vivado "
                        f"(replaces broken -stack approach)"
                    )
                    logger.info(
                        f"Will set ulimit -s {kb} before invoking Vivado "
                        f"(replaces broken -stack approach)"
                    )
                # Vivado env setup: f2's build-bitstream.sh expects to be invoked
                # from inside the aws-fpga clone after `source hdk_setup.sh`
                # so encrypted shell IP resolves. xb10 just calls `vivado`
                # directly (FPGA Developer AMI puts it on PATH), no preamble.
                if spec.needs_hdk_setup:
                    env_prefix = f"cd {host_awsfpga} && source hdk_setup.sh && "
                else:
                    env_prefix = ""
                extra_script_args = "".join(
                    f" {a}" for a in spec.extra_build_script_args
                )
                vivado_cmd = (
                    f"{env_prefix}"
                    f"{vivado_shim_part}"
                    f"{host_cl_dir}/build-bitstream.sh "
                    f"--cl_dir {host_cl_dir} "
                    f"--frequency {self.build_config.fpga_frequency} "
                    f"--strategy {self.build_config.build_strategy}"
                    f"{extra_script_args}"
                    f"{extra_flags}"
                )
                build_log_lines.append(f"Vivado command: {vivado_cmd}")
                logger.info(f"Vivado command: {vivado_cmd}")
                vivado_result = self._run_long(
                    host, vivado_cmd,
                    timeout=vivado_timeout, label="vivado",
                    poll_interval=vivado_poll_interval,
                )
                build_log_lines.append(f"Vivado exit code: {vivado_result.returncode}")
                logger.info(f"Vivado exit code: {vivado_result.returncode}")

                # Upload the full Vivado log to S3 (always, regardless of build
                # success — log is useful for both passing and failing builds).
                # `check=False` so a transient S3 issue doesn't kill the build
                # before we get to the cl_dir sync, but we record the actual
                # upload result so silent failures (e.g. missing aws CLI) are
                # visible in the build log instead of falsely reporting success.
                vivado_log_s3 = f"s3://{self.s3_bucket}/{s3_prefix}/vivado.log"
                vivado_log_upload = host.run(
                    f"aws s3 cp /home/ubuntu/chia-long-cmd.log {vivado_log_s3}",
                    timeout=120, check=False,
                )
                if vivado_log_upload.returncode == 0:
                    build_log_lines.append(f"Vivado log uploaded: {vivado_log_s3}")
                    logger.info(f"Vivado log uploaded: {vivado_log_s3}")
                else:
                    build_log_lines.append(
                        f"Vivado log upload FAILED (rc={vivado_log_upload.returncode}): "
                        f"{vivado_log_upload.stderr.strip()[-300:]}"
                    )
                    logger.warning(
                        f"Vivado log upload FAILED (rc={vivado_log_upload.returncode}): "
                        f"{vivado_log_upload.stderr.strip()[-300:]}"
                    )

                # Extract per-phase timing from the Vivado log
                phase_timing = host.run(
                    "grep -E 'synth_design:.*Time|opt_design:.*Time|place_design:.*Time|"
                    "phys_opt_design:.*Time|route_design:.*Time|"
                    "AWS FPGA.*Start|Synth Design complete' "
                    "/home/ubuntu/chia-long-cmd.log 2>/dev/null",
                    timeout=30, check=False,
                )
                if phase_timing.stdout.strip():
                    build_log_lines.append(f"Vivado phase timing:\n{phase_timing.stdout.strip()}")
                    logger.info(f"Vivado phase timing:\n{phase_timing.stdout.strip()}")
                    
                # Step 9b: Upload full cl_ directory (Vivado reports, timing, etc.).
                # Best-effort: if S3 perms are misconfigured (or any other
                # transient AWS issue), we still want Step 9c's local rsync to
                # run so the head node ends up with the collateral. Without
                # this guard, a 403 from s3:PutObject silently destroyed the
                # build's local cl/ directory because BitstreamBuilder.build
                # tore down the EC2 host before the local rsync got a chance.
                s3_cl_prefix = f"s3://{self.s3_bucket}/{s3_prefix}/cl/"
                logger.info(f"Uploading cl_ directory to {s3_cl_prefix}")
                try:
                    host.run(
                        f"aws s3 sync {host_cl_dir}/ {s3_cl_prefix} "
                        f"--exclude '*.tar' --exclude '.git/*'",
                        timeout=600,
                    )
                    build_log_lines.append(f"cl_ directory uploaded: {s3_cl_prefix}")
                except Exception as e:
                    msg = f"cl_ S3 upload failed (best-effort, local rsync still runs): {type(e).__name__}: {str(e)[:300]}"
                    build_log_lines.append(msg)
                    logger.warning(msg)

                # Step 9c: Always copy vivado.log to local_log_dir/<build_id>/
                # so failed builds leave a grep-able log behind without an
                # S3 round-trip. With copy_all_artifacts=True, also rsync
                # the full cl_dir (reports, checkpoints, runme logs).
                if self.local_log_dir:
                    local_dir = os.path.join(self.local_log_dir, build_id)
                    os.makedirs(local_dir, exist_ok=True)
                    try:
                        host.rsync_down(
                            "/home/ubuntu/chia-long-cmd.log",
                            os.path.join(local_dir, "vivado.log"),
                        )
                        build_log_lines.append(f"vivado.log copied to {local_dir}/vivado.log")
                        if self.copy_all_artifacts:
                            host.rsync_down(f"{host_cl_dir}/", os.path.join(local_dir, "cl") + "/")
                            build_log_lines.append(f"cl_ directory copied to {local_dir}/cl/")
                    except Exception as e:
                        msg = f"local artifact copy failed (best-effort): {type(e).__name__}: {e}"
                        build_log_lines.append(msg)
                        logger.warning(msg)

                if vivado_result.returncode != 0:
                    build_log_lines.append(
                        f"Vivado log (last 2000 chars): {vivado_result.stdout[-2000:]}"
                    )
                    logger.info(f"Vivado log (last 2000 chars): {vivado_result.stdout[-2000:]}")
                    # Surface what Vivado managed to produce before dying — DCP
                    # files and report files indicate which synthesis/impl phase
                    # the run died in, which is usually the fastest debug signal.
                    diag = host.run(
                        f"echo '== reports ==' && ls -la {host_cl_dir}/build/reports/ 2>&1 | head -40 && "
                        f"echo '== checkpoints ==' && ls -la {host_cl_dir}/build/checkpoints/ 2>&1 | head -40 && "
                        f"echo '== vivado_proj ==' && ls -la {host_cl_dir}/vivado_proj/ 2>&1 | head -40",
                        timeout=30, check=False,
                    )
                    if diag.stdout.strip():
                        build_log_lines.append(
                            f"Vivado partial outputs:\n{diag.stdout.strip()}"
                        )
                        logger.info(
                            f"Vivado partial outputs:\n{diag.stdout.strip()}"
                        )
                    return BitstreamBuildResult(
                        recipe_name=self.build_config.name,
                        agfi=None, afi=None,
                        success=False,
                        build_log="\n".join(build_log_lines),
                        hwdb_entry="",
                        build_id=build_id,
                        build_ref=build_ref,
                    )

                # Step 10: Finalize bitstream artifact.
                # f2: mint an AGFI via `aws ec2 create-fpga-image`.
                # xb10 (and other local-PCIe platforms): upload the raw .bit
                # straight to S3 and surface its URI on BitstreamBuildResult.
                bitstream_path: str | None = None
                if spec.creates_agfi:
                    build_log_lines.append("=== Step 10: Create AGFI ===")
                    logger.info("=== Step 10: Create AGFI ===")
                    agfi, afi, hwdb_or_err = self._create_agfi_remote(
                        host, host_cl_dir, s3_prefix,
                    )
                    if agfi is None:
                        build_log_lines.append(f"AGFI creation failed: {hwdb_or_err}")
                        logger.info(f"AGFI creation failed: {hwdb_or_err}")
                        return BitstreamBuildResult(
                            recipe_name=self.build_config.name,
                            agfi=None, afi=None,
                            success=False,
                            build_log="\n".join(build_log_lines),
                            hwdb_entry="",
                            build_id=build_id,
                            build_ref=build_ref,
                        )
                else:
                    build_log_lines.append("=== Step 10: Upload .bit artifact ===")
                    logger.info("=== Step 10: Upload .bit artifact ===")
                    agfi, afi, hwdb_or_err = None, None, ""
                    bit_local = f"{host_cl_dir}/{spec.bitstream_artifact_relpath}"

                    # Diagnostic: list the directory the .bit is supposed to land
                    # in. If Vivado renamed it (e.g. xb10's main.tcl emits
                    # `${design}.bit` for some configs) we want to see what's
                    # actually there before failing.
                    bit_dir = os.path.dirname(bit_local)
                    ls_bit_dir = host.run(
                        f"ls -la {bit_dir}/ 2>&1 | head -40",
                        timeout=30, check=False,
                    )
                    if ls_bit_dir.stdout.strip():
                        build_log_lines.append(
                            f"bitstream dir contents ({bit_dir}):\n"
                            f"{ls_bit_dir.stdout.strip()}"
                        )
                        logger.info(
                            f"bitstream dir contents ({bit_dir}):\n"
                            f"{ls_bit_dir.stdout.strip()}"
                        )
                    # Also do a wide find for any *.bit under cl_dir, in case
                    # main.tcl wrote the bitstream somewhere we didn't expect.
                    find_bits = host.run(
                        f"find {host_cl_dir} -name '*.bit' -size +1k 2>/dev/null",
                        timeout=60, check=False,
                    )
                    if find_bits.stdout.strip():
                        build_log_lines.append(
                            f"all *.bit files found under host_cl_dir:\n"
                            f"{find_bits.stdout.strip()}"
                        )
                        logger.info(
                            f"all *.bit files found under host_cl_dir:\n"
                            f"{find_bits.stdout.strip()}"
                        )

                    bit_check = host.run(
                        f"test -f {bit_local} && echo found || echo missing",
                        timeout=10, check=False,
                    )
                    if "missing" in bit_check.stdout:
                        msg = (
                            f"Expected bitstream not found at {bit_local}. "
                            f"Found bitstreams: "
                            f"{find_bits.stdout.strip() or '(none)'}"
                        )
                        build_log_lines.append(msg)
                        logger.info(msg)
                        return BitstreamBuildResult(
                            recipe_name=self.build_config.name,
                            agfi=None, afi=None,
                            success=False,
                            build_log="\n".join(build_log_lines),
                            hwdb_entry="",
                            build_id=build_id,
                            build_ref=build_ref,
                        )

                    # Log the .bit size — sanity check that Vivado actually
                    # produced a real bitstream and not a stub.
                    bit_size = host.run(
                        f"stat -c %s {bit_local} 2>/dev/null",
                        timeout=10, check=False,
                    )
                    build_log_lines.append(
                        f"bitstream local size: {bit_size.stdout.strip()} bytes"
                    )
                    logger.info(
                        f"bitstream local size: {bit_size.stdout.strip()} bytes"
                    )

                    bitstream_path = f"s3://{self.s3_bucket}/{s3_prefix}/firesim.bit"
                    host.run(
                        f"aws s3 cp {bit_local} {bitstream_path}",
                        timeout=300,
                    )
                    build_log_lines.append(f"Bitstream uploaded: {bitstream_path}")
                    logger.info(f"Bitstream uploaded: {bitstream_path}")

                # Step 10b: Upload driver bundle (skipped when step 6 didn't
                # produce one — e.g. xb10).
                driver_s3_uri: str | None = None
                if self._spec.builds_driver and host_driver is not None:
                    s3_driver_key = f"{s3_prefix}/driver-bundle.tar.gz"
                    driver_s3_uri = f"s3://{self.s3_bucket}/{s3_driver_key}"
                    logger.info(f"Uploading driver bundle to {driver_s3_uri}")
                    host.run(
                        f"aws s3 cp {host_driver} {driver_s3_uri}",
                        timeout=300,
                    )
                    driver_s3_path = driver_s3_uri
                    build_log_lines.append(f"Driver bundle uploaded: {driver_s3_uri}")

                # Step 10c: Upload PR base artifacts if applicable
                self._post_build_uploads(host, host_cl_dir, s3_prefix, build_log_lines)

                # Step 10d: Write build-info.json with AGFI and metadata
                bc = self.build_config

                # Parse per-phase wall times from the Vivado log
                phase_times = {}
                for phase in ["synth_design", "opt_design", "place_design",
                              "phys_opt_design", "route_design"]:
                    pt = host.run(
                        f"grep '{phase}:.*elapsed' /home/ubuntu/chia-long-cmd.log 2>/dev/null | "
                        f"tail -1 | grep -oP 'elapsed = \\K[^.]*'",
                        timeout=10, check=False,
                    )
                    if pt.stdout.strip():
                        phase_times[phase] = pt.stdout.strip()

                build_info = json.dumps({
                    "build_id": build_id,
                    "build_ref": build_ref,
                    "recipe_name": bc.name,
                    "agfi": agfi,
                    "afi": afi,
                    "bitstream_path": bitstream_path,
                    "driver_s3_path": driver_s3_uri,
                    "timestamp": ts,
                    # Build config
                    "build_config": {
                        "platform": bc.platform,
                        "target_project": bc.target_project,
                        "design": bc.design,
                        "target_config": bc.target_config,
                        "platform_config": bc.platform_config,
                        "fpga_frequency": bc.fpga_frequency,
                        "build_strategy": bc.build_strategy,
                        "build_group": bc.build_group,
                    },
                    "quintuplet": quintuplet,
                    "build_mode": self._build_mode(),
                    "pr_module_name": bc.pr_module_name,
                    "incremental_base_build_id": bc.incremental_base_build_id,
                    "pr_base_build_id": bc.pr_base_build_id,
                    "vivado_phase_times": phase_times,
                    "vivado_log_s3": vivado_log_s3,
                }, indent=2)
                host.run(
                    f"echo '{build_info}' | aws s3 cp - "
                    f"s3://{self.s3_bucket}/{s3_prefix}/build-info.json",
                    timeout=60,
                )

                # Write latest pointer so users can reference "latest" or "{group}/latest"
                latest_prefix = f"builds/{group}/latest" if group else "builds/latest"
                host.run(
                    f"echo '{build_info}' | aws s3 cp - "
                    f"s3://{self.s3_bucket}/{latest_prefix}/build-info.json",
                    timeout=60, check=False,
                )

                # Step 10e: Cache the final artifact + build-info.json on the
                # head node. boto3 (not the aws CLI): Ray workers don't inherit
                # the interactive PATH that has aws.
                if self.bitstream_local_dir is not None:
                    import boto3
                    s3 = boto3.client("s3")
                    local_dir = os.path.join(self.bitstream_local_dir, build_id)
                    os.makedirs(local_dir, exist_ok=True)
                    artifact_uri = bitstream_path if not spec.creates_agfi else driver_s3_uri
                    if artifact_uri is not None:
                        artifact_local = os.path.join(local_dir, os.path.basename(artifact_uri))
                        key = artifact_uri[len(f"s3://{self.s3_bucket}/"):]
                        s3.download_file(self.s3_bucket, key, artifact_local)
                        build_log_lines.append(f"Artifact cached locally: {artifact_local}")
                        logger.info(f"Artifact cached locally: {artifact_local}")
                    info_local = os.path.join(local_dir, "build-info.json")
                    s3.download_file(self.s3_bucket, f"{s3_prefix}/build-info.json", info_local)

                # Step 11: Optionally download results to an explicit
                # results_dir on the head node. The local_log_dir fallback
                # is now covered by Step 9c above (runs for both success
                # and failure), so this only fires when results_dir is
                # explicitly set — preserving the existing escape hatch
                # without duplicating the rsync.
                effective_results_dir = self.results_dir
                if effective_results_dir:
                    build_log_lines.append("=== Step 11: Download results ===")
                    logger.info("=== Step 11: Download results ===")
                    os.makedirs(effective_results_dir, exist_ok=True)
                    host.rsync_down(f"{host_cl_dir}/", f"{effective_results_dir}/")
                    # Save hwdb entry locally (only meaningful for AGFI platforms;
                    # xb10's hwdb_or_err is the empty string).
                    if hwdb_or_err:
                        hwdb_path = os.path.join(effective_results_dir, "hwdb_entry.yaml")
                        with open(hwdb_path, "w") as f:
                            f.write(hwdb_or_err)
                    build_log_lines.append(f"Results saved to {effective_results_dir}")
                    logger.info(f"Results saved to {effective_results_dir}")

                if spec.creates_agfi:
                    build_log_lines.append(f"AGFI ready: {agfi}")
                    logger.info(f"AGFI ready: {agfi}")
                else:
                    build_log_lines.append(f"Bitstream ready: {bitstream_path}")
                    logger.info(f"Bitstream ready: {bitstream_path}")
                build_log_lines.append(f"Build ref for runs: {build_ref}")
                logger.info(f"Build ref for runs: {build_ref}")
                return BitstreamBuildResult(
                    recipe_name=self.build_config.name,
                    agfi=agfi, afi=afi,
                    success=True,
                    build_log="\n".join(build_log_lines),
                    hwdb_entry=hwdb_or_err,
                    driver_s3_path=driver_s3_path,
                    build_id=build_id,
                    build_ref=build_ref,
                    bitstream_path=bitstream_path,
                )

        except Exception as e:
            build_log_lines.append(f"ERROR: {type(e).__name__}: {e}")
            logger.error(f"Build failed: {type(e).__name__}: {e}")
            return BitstreamBuildResult(
                recipe_name=self.build_config.name,
                agfi=None, afi=None,
                success=False,
                build_log="\n".join(build_log_lines),
                hwdb_entry="",
                build_id=build_id,
                build_ref=build_ref,
            )
        finally:
            # Terminate any instances that weren't cleaned up by the context manager
            if instance_ids:
                logger.warning(f"Cleaning up orphaned instances: {instance_ids}")
                from chia.aws.ec2 import terminate_ec2_instances
                terminate_ec2_instances(instance_ids, region=self.aws_config.region)
