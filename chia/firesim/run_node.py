"""FPGA simulation orchestrator — manages the full lifecycle of launching
F2 instances, flashing FPGAs, running simulations, and collecting results.

Adapted from FireSim's EC2InstanceDeployManager.infrasetup_instance(),
InstanceDeployManager.start_sim_slot(), and
FireSimTopologyWithPasses.run_workload_passes().

Flow::

    1. Launch F2 EC2 instances (f2.6xlarge/12xlarge/48xlarge based on num_sims)
    2. Wait for SSH readiness on all hosts
    3. Infrasetup (parallel across hosts):
       - Rsync driver tarball to each sim slot
       - Install AWS FPGA SDK (firesim fork)
       - Clear all FPGAs (with wait-for-clear)
       - Flash FPGAs with AGFI (with wait-for-loaded)
    4. Boot simulations via screen
    5. Monitor loop (10s poll) checking for simulation completion
    6. Collect results: rsync uartlogs and output files back
    7. Terminate all instances (in finally block)
"""

from __future__ import annotations

import os
import re
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from chia.aws.config import AWSConfig, EC2InstanceConfig
from chia.aws.ec2 import EC2Instance, launch_ec2_instances, wait_for_instances
from chia.aws.host import EphemeralEC2Host
from chia.cluster.log import get_logger
from chia.firesim.config import FireSimRunConfig
from chia.firesim.state_def import FireSimRunResult

logger = get_logger("firesim.run")

SIM_TIMEOUT_DEFAULT = 14400  # 4 hours
MONITOR_POLL_INTERVAL = 10   # seconds
SSH_WAIT_TIMEOUT = 300       # 5 minutes
DEADLOCK_MAX_RETRIES = 3     # retries on cycle-0 deadlock

# FPGAs per F2 instance type
FPGAS_PER_INSTANCE = {
    "f2.6xlarge": 1,
    "f2.12xlarge": 2,
    "f2.48xlarge": 8,
}

# Max FPGA slots that could exist on any F2 instance (f2.48xlarge)
MAX_FPGA_SLOTS = 8


class SimulationRunner:
    """Orchestrates FPGA simulations on ephemeral F2 EC2 instances.

    Drives the full FireSim run lifecycle: it launches one or more F2
    instances, runs ``infrasetup`` on each (deploy driver/rootfs/boot binary,
    clear and flash the FPGAs with the build's AGFI), boots simulation slots,
    monitors them to completion, and collects the uartlogs and output
    artifacts before terminating the instances. It is typically invoked on a
    Chia ``firesim_manager`` worker node via
    :func:`chia.firesim.chia_functions.firesim_run_workload`, but can also be
    constructed and driven directly for standalone runs.
    """

    def __init__(
        self,
        run_config: FireSimRunConfig,
        aws_config: AWSConfig,
        sim_timeout: int = SIM_TIMEOUT_DEFAULT,
        cluster_name: str = "chia",
        results_dir: str | None = None,
        local_log_dir: str | None = None,
        log_prefix: str = "firesim-run",
    ) -> None:
        """Initialize a simulation runner.

        Args:
            run_config: FireSim run configuration. Carries everything needed to
                launch the simulation: the FPGA image (``agfi``, or a
                ``build_ref`` already resolved to one), the driver/rootfs/boot
                binary artifacts (S3 paths preferred, local paths as fallback),
                the F2 ``instance_type``, the number of simulation slots
                (``num_sims``), the EC2 ``market``/``ami_id``, the
                ``workload_name`` used for tagging and result paths, and any
                ``plusarg_passthrough`` appended to the driver's plusargs.
            aws_config: AWS credentials and networking configuration (region,
                S3 bucket, key pair, security group, and ``aws_creds_dir``).
                The credentials directory is rsynced to each host so the
                simulation can pull artifacts from S3; ``region`` is used when
                waiting on and terminating instances.
            sim_timeout: Maximum wall-clock runtime, in seconds, for the
                monitor loop before simulations are forcibly killed and the run
                is treated as timed out (default 4 hours).
            cluster_name: Chia cluster name applied as the ``chia-cluster`` tag
                on every launched instance (for ownership/tracking).
            results_dir: Local directory under which collected results
                (uartlogs and output files) are written. If None, a temporary
                directory is created (optionally inside ``local_log_dir``).
            local_log_dir: Parent directory in which to create the temporary
                results directory when ``results_dir`` is None. If None, the
                system default temp location is used.
            log_prefix: Filename prefix for the temporary results directory
                created when ``results_dir`` is None (default
                ``"firesim-run"``).
        """
        self.run_config = run_config
        self.aws_config = aws_config
        self.sim_timeout = sim_timeout
        self.cluster_name = cluster_name
        self.results_dir = results_dir
        self.local_log_dir = local_log_dir
        self.log_prefix = log_prefix
        self.aws_creds_dir = aws_config.aws_creds_dir or os.path.expanduser("~/.aws")

    def _get_rootfs_name(self) -> str | None:
        """Derive the rootfs filename as it appears on the remote host."""
        rc = self.run_config
        if rc.rootfs_s3_path:
            return rc.rootfs_s3_path.rsplit("/", 1)[-1]
        elif rc.rootfs_path:
            return os.path.basename(rc.rootfs_path)
        return None

    def _determine_instance_count(self) -> int:
        """Determine how many F2 instances to launch based on sim count and type."""
        fpgas = FPGAS_PER_INSTANCE.get(self.run_config.instance_type, 1)
        return max(1, -(-self.run_config.num_sims // fpgas))  # ceiling division

    def _infrasetup_host(self, host: EphemeralEC2Host, sim_slots: list[int]) -> None:
        """Set up a single F2 host for FPGA simulation.

        Follows the FireSim EC2InstanceDeployManager.infrasetup_instance() flow:
        1. Create sim slot dirs and deploy driver + workload artifacts
        2. Install AWS FPGA SDK
        3. Clear all FPGA slots (with wait loop)
        4. Flash assigned slots with AGFI (with wait loop)

        Artifacts are resolved from S3 paths (preferred) or local paths (fallback).
        """
        logger.info(f"[{host.instance_id}] Running infrasetup for slots {sim_slots}")
        rc = self.run_config

        sim_dir = "/home/ubuntu/sim"
        host.run(f"mkdir -p {sim_dir}")

        # Create sim slot directories and deploy driver + workload to each
        for slot in sim_slots:
            slot_dir = f"{sim_dir}/sim_slot_{slot}"
            host.run(f"mkdir -p {slot_dir}")

            # Driver tarball: S3 or local
            if rc.driver_s3_path:
                host.run(
                    f"aws s3 cp {rc.driver_s3_path} {slot_dir}/driver-bundle.tar.gz",
                    timeout=300,
                )
            elif rc.driver_tarball_path:
                host.rsync_up(rc.driver_tarball_path, f"{slot_dir}/driver-bundle.tar.gz")
            else:
                raise ValueError("No driver source: set driver_s3_path or driver_tarball_path")
            host.run(f"cd {slot_dir} && tar -xf driver-bundle.tar.gz", timeout=60)

            # Boot binary: S3 or local
            if rc.bootbinary_s3_path:
                boot_name = rc.bootbinary_s3_path.rsplit("/", 1)[-1]
                host.run(
                    f"aws s3 cp {rc.bootbinary_s3_path} {slot_dir}/{boot_name}",
                    timeout=300,
                )
            elif rc.bootbinary_path:
                boot_name = os.path.basename(rc.bootbinary_path)
                host.rsync_up(rc.bootbinary_path, f"{slot_dir}/{boot_name}")

            # Rootfs: S3 or local
            if rc.rootfs_s3_path:
                rootfs_name = rc.rootfs_s3_path.rsplit("/", 1)[-1]
                host.run(
                    f"aws s3 cp {rc.rootfs_s3_path} {slot_dir}/{rootfs_name}",
                    timeout=600,
                )
            elif rc.rootfs_path:
                rootfs_name = os.path.basename(rc.rootfs_path)
                host.rsync_up(rc.rootfs_path, f"{slot_dir}/{rootfs_name}")

        # Install runtime libraries needed by the FireSim driver binary.
        # libdwarf-dev pulls in the runtime .so (libdwarf.so.1) as a dependency.
        # This is a fallback in case the driver-bundle tarball is missing the .so.
        # host.run(
        #     "sudo apt-get update -qq && sudo apt-get install -y -qq libdwarf-dev libelf-dev",
        #     timeout=300,
        # )

        # Install AWS FPGA SDK (firesim fork)
        logger.info(f"[{host.instance_id}] Installing AWS FPGA SDK")
        host.run_script([
            "cd /home/ubuntu",
            "if [ ! -d aws-fpga ]; then "
            "git clone https://github.com/firesim/aws-fpga-firesim-f2.git aws-fpga; "
            "fi",
            "cd aws-fpga && source sdk_setup.sh",
        ], timeout=600, check=False)

        # Clear ALL FPGA slots (FireSim clears all, not just assigned ones)
        fpgas_on_instance = FPGAS_PER_INSTANCE.get(self.run_config.instance_type, 1)
        logger.info(f"[{host.instance_id}] Clearing {fpgas_on_instance} FPGA slot(s)")
        for s in range(fpgas_on_instance):
            host.run(f"sudo fpga-clear-local-image -S {s} -A", timeout=60, check=False)

        # Wait for all clears to complete
        for s in range(fpgas_on_instance):
            host.run(
                f'until sudo fpga-describe-local-image -S {s} -R -H | grep -q "cleared"; '
                f"do sleep 1; done",
                timeout=120, check=False,
            )

        # Flash FPGAs with AGFI
        if self.run_config.agfi:
            agfi = self.run_config.agfi
            logger.info(f"[{host.instance_id}] Flashing FPGAs with {agfi}")

            # Flash assigned slots
            for slot in sim_slots:
                host.run(
                    f"sudo fpga-load-local-image -S {slot} -I {agfi} -A",
                    timeout=120,
                )

            # Flash unused slots with same AGFI (FireSim requirement:
            # XDMA hangs if some FPGAs are left cleared)
            for s in range(len(sim_slots), fpgas_on_instance):
                host.run(
                    f"sudo fpga-load-local-image -S {s} -I {agfi} -A",
                    timeout=120,
                )

            # Wait for all flashes to complete
            for s in range(fpgas_on_instance):
                host.run(
                    f'until sudo fpga-describe-local-image -S {s} -R -H | grep -q "loaded"; '
                    f"do sleep 1; done",
                    timeout=120,
                )

            # Verify final state
            for slot in sim_slots:
                result = host.run(
                    f"sudo fpga-describe-local-image -S {slot} -R -H",
                    timeout=30, check=False,
                )
                logger.info(f"[{host.instance_id}] Slot {slot}: {result.stdout.strip()[:200]}")

    def _start_sim_slot(self, host: EphemeralEC2Host, slot: int) -> None:
        """Start a simulation in a screen session on the remote host.

        Matches FireSim's native launch: screen wraps ``script -f`` which
        provides a pty and captures combined stdout+stderr to ``uartlog``.
        Rootfs is passed as +blkdev0= plusarg; boot binary is passed as a
        positional argument after +permissive-off (firesim_tsi convention).
        """
        sim_dir = f"/home/ubuntu/sim/sim_slot_{slot}"
        rc = self.run_config

        # Build permissive plusargs (between +permissive ... +permissive-off)
        permissive_args = [f"+slotid={slot}"]

        # Rootfs as +blkdev0= (FireSim convention)
        if rc.rootfs_s3_path:
            rootfs_name = rc.rootfs_s3_path.rsplit("/", 1)[-1]
            permissive_args.append(f"+blkdev0={sim_dir}/{rootfs_name}")
        elif rc.rootfs_path:
            rootfs_name = os.path.basename(rc.rootfs_path)
            permissive_args.append(f"+blkdev0={sim_dir}/{rootfs_name}")

        # User-specified plusargs
        if rc.plusarg_passthrough:
            permissive_args.append(rc.plusarg_passthrough)

        permissive_str = " ".join(permissive_args)

        # Boot binary: passed as positional arg after +permissive-off
        # (firesim_tsi expects: DRIVER +permissive ... +permissive-off BINARY)
        boot_name = None
        if rc.bootbinary_s3_path:
            boot_name = rc.bootbinary_s3_path.rsplit("/", 1)[-1]
        elif rc.bootbinary_path:
            boot_name = os.path.basename(rc.bootbinary_path)
        boot_arg = f" {sim_dir}/{boot_name}" if boot_name else ""

        # Driver call: matches FireSim's format exactly.
        # LD_LIBRARY_PATH set for bundled shared libs (libriscv.so, libdwarf.so).
        driver_call = (
            f"sudo env LD_LIBRARY_PATH={sim_dir}:$LD_LIBRARY_PATH "
            f"{sim_dir}/FireSim-f2 +permissive {permissive_str} +permissive-off{boot_arg}"
        )

        # Wrap in script(1) for pty + combined output capture, same as FireSim:
        #   script -f -c 'stty intr ^] && <driver> && stty intr ^c' uartlog
        # Then wrap in screen for background execution.
        screen_name = f"fsim{slot}"
        base_command = (
            f"cd {sim_dir} && export LD_LIBRARY_PATH={sim_dir}:$LD_LIBRARY_PATH && "
            f"script -f -c 'stty intr ^] && {driver_call} && stty intr ^c' uartlog"
        )
        screen_cmd = (
            f'screen -S {screen_name} -d -m bash -c "{base_command}"; sleep 1'
        )

        logger.info(f"[{host.instance_id}] Starting sim slot {slot}")
        host.run(screen_cmd, timeout=30)

    def _check_deadlock_at_zero(self, host: EphemeralEC2Host, slot: int) -> bool:
        """Check if a simulation deadlocked at target cycle 0."""
        sim_dir = f"/home/ubuntu/sim/sim_slot_{slot}"
        result = host.run(
            f"grep -q 'deadlock detected at target cycle 0' {sim_dir}/uartlog 2>/dev/null "
            f"&& echo deadlock || echo ok",
            timeout=120, check=False,
        )
        return "deadlock" in result.stdout

    def _reset_and_reflash(self, host: EphemeralEC2Host, sim_slots: list[int]) -> None:
        """Clear all FPGAs, re-flash with AGFI, and clean up sim slot files."""
        fpgas_on_instance = FPGAS_PER_INSTANCE.get(self.run_config.instance_type, 1)

        # Clear all FPGA slots
        for s in range(fpgas_on_instance):
            host.run(f"sudo fpga-clear-local-image -S {s} -A", timeout=60, check=False)
        for s in range(fpgas_on_instance):
            host.run(
                f'until sudo fpga-describe-local-image -S {s} -R -H | grep -q "cleared"; '
                f"do sleep 1; done",
                timeout=120, check=False,
            )

        # Re-flash all slots
        agfi = self.run_config.agfi
        if agfi:
            for s in range(fpgas_on_instance):
                host.run(f"sudo fpga-load-local-image -S {s} -I {agfi} -A", timeout=120)
            for s in range(fpgas_on_instance):
                host.run(
                    f'until sudo fpga-describe-local-image -S {s} -R -H | grep -q "loaded"; '
                    f"do sleep 1; done",
                    timeout=120,
                )

        # Clean up old sim output files
        for slot in sim_slots:
            sim_dir = f"/home/ubuntu/sim/sim_slot_{slot}"
            host.run(
                f"rm -f {sim_dir}/uartlog {sim_dir}/sim.out {sim_dir}/sim.exitcode",
                timeout=10, check=False,
            )

    def _check_sim_complete(self, host: EphemeralEC2Host, slot: int) -> bool:
        """Check if a simulation slot has completed.

        Uses the same approach as FireSim: check if the screen session
        for this slot is still running. When the driver exits (for any
        reason), the screen session terminates.

        On any SSH error or timeout, treats the state as unknown and
        returns False (keep polling), mirroring FireSim's warn_only=True.
        """
        try:
            result = host.run("screen -ls", timeout=300, check=False)
        except Exception as e:
            logger.warning(
                f"[{host.instance_id}] screen -ls failed for slot {slot}: {e}; "
                f"treating as unknown and continuing to poll"
            )
            return False

        for line in result.stdout.splitlines():
            if "(Detached)" not in line and "(Attached)" not in line:
                continue
            m = re.search(r"fsim([0-9]+)", line.strip())
            if m and int(m.group(1)) == slot:
                return False  # still running
        return True  # screen session gone -> completed

    def _collect_results(self, host: EphemeralEC2Host, slot: int,
                         local_results_dir: str) -> tuple[str, str]:
        """Download all simulation outputs from a slot directory.

        Downloads the entire sim_slot_X directory, excluding large input
        artifacts (driver binary, rootfs image, boot binary, driver bundle)
        that we uploaded during infrasetup. This future-proofs collection
        against new FireSim output types (autocounters, traces, memory
        stats, etc.).

        Returns:
            (uartlog_content, exitcode_str)
        """
        sim_dir = f"/home/ubuntu/sim/sim_slot_{slot}"
        slot_results = os.path.join(local_results_dir, f"sim_slot_{slot}")
        os.makedirs(slot_results, exist_ok=True)

        # Download everything except large input artifacts we uploaded
        try:
            host.rsync_down(
                f"{sim_dir}/",
                f"{slot_results}/",
                exclude=[
                    "FireSim-f2",           # driver binary
                    "driver-bundle.tar.gz",  # driver tarball
                    "*.img",                 # rootfs images
                    "*.so",                  # shared libraries
                    "*.so.*",
                    "rootfs_mount/",         # transient mount point
                    "rootfs_output/",        # transient staging dir
                ],
            )
        except Exception as e:
            logger.warning(f"[{host.instance_id}] Could not download sim_slot_{slot}: {e}")

        uartlog = ""
        uartlog_path = os.path.join(slot_results, "uartlog")
        if os.path.exists(uartlog_path):
            with open(uartlog_path) as f:
                uartlog = f.read()

        # Infer exit code from uartlog content (script doesn't write exitcode)
        if "*** PASSED ***" in uartlog:
            exitcode = "0"
        elif "*** FAILED ***" in uartlog or "deadlock" in uartlog:
            exitcode = "1"
        else:
            exitcode = ""  # unknown / timed out

        return uartlog, exitcode

    def _read_sim_outputs(self, slot: int,
                          local_results_dir: str) -> dict[str, str]:
        """Read all downloaded sim artifacts (excluding uartlog) into a dict.

        Scans the locally downloaded sim_slot_X directory for text files
        produced by the simulation (memory_stats, autocounters, traces, etc.).
        Skips uartlog (already in uartlogs field) and binary/large files.

        Returns:
            Dict mapping relative file path -> file content.
        """
        slot_results = os.path.join(local_results_dir, f"sim_slot_{slot}")
        if not os.path.isdir(slot_results):
            return {}

        outputs: dict[str, str] = {}
        for root, dirs, files in os.walk(slot_results):
            # Skip output/ subdirectory — already captured in rootfs_outputs
            if "output" in dirs:
                dirs.remove("output")
            for fname in files:
                if fname == "uartlog":
                    continue  # already captured separately
                fpath = os.path.join(root, fname)
                relpath = os.path.relpath(fpath, slot_results)
                try:
                    size = os.path.getsize(fpath)
                    if size > 10_000_000:  # skip files > 10MB
                        logger.info(f"Skipping large sim output: {relpath} ({size} bytes)")
                        continue
                    with open(fpath) as f:
                        outputs[relpath] = f.read()
                except (UnicodeDecodeError, OSError):
                    pass  # skip binary files
        return outputs

    def _extract_rootfs_outputs(self, host: EphemeralEC2Host, slot: int,
                                local_results_dir: str) -> dict[str, str]:
        """Mount the rootfs image on the remote host and extract /output files.

        Returns:
            Dict mapping relative file path -> file content for small text files.
        """
        rootfs_name = self._get_rootfs_name()
        if not rootfs_name:
            return {}

        sim_dir = f"/home/ubuntu/sim/sim_slot_{slot}"
        rootfs_path = f"{sim_dir}/{rootfs_name}"
        mountpoint = f"{sim_dir}/rootfs_mount"
        staging_dir = f"{sim_dir}/rootfs_output"

        try:
            host.run(f"mkdir -p {mountpoint} {staging_dir}", timeout=30, check=False)

            # Mount the rootfs (ext2/ext4 image, read-only)
            host.run(f"sudo mount -o loop,ro {rootfs_path} {mountpoint}", timeout=30)

            # Check if /output exists inside the rootfs
            check = host.run(
                f"test -d {mountpoint}/output && echo exists || echo missing",
                timeout=10, check=False,
            )
            if "exists" not in check.stdout:
                logger.info(f"[{host.instance_id}] No /output directory in rootfs for slot {slot}")
                host.run(f"sudo umount {mountpoint}", timeout=30, check=False)
                return {}

            # Copy /output contents to staging dir (as ubuntu user)
            host.run(
                f"sudo cp -rL {mountpoint}/output/* {staging_dir}/ && "
                f"sudo chown -R ubuntu:ubuntu {staging_dir}",
                timeout=60, check=False,
            )

            # Unmount
            host.run(f"sudo umount {mountpoint}", timeout=30, check=False)

        except Exception as e:
            logger.warning(f"[{host.instance_id}] Rootfs extraction failed for slot {slot}: {e}")
            try:
                host.run(f"sudo umount {mountpoint}", timeout=10, check=False)
            except Exception:
                pass
            return {}

        # Download the extracted output directory to local results
        slot_results = os.path.join(local_results_dir, f"sim_slot_{slot}")
        local_output_dir = os.path.join(slot_results, "output")
        os.makedirs(local_output_dir, exist_ok=True)

        try:
            host.rsync_down(f"{staging_dir}/", f"{local_output_dir}/")
        except Exception as e:
            logger.warning(f"[{host.instance_id}] Could not download rootfs outputs: {e}")
            return {}

        # Read small text files into a dict for the result dataclass
        output_files: dict[str, str] = {}
        for root, dirs, files in os.walk(local_output_dir):
            for fname in files:
                fpath = os.path.join(root, fname)
                relpath = os.path.relpath(fpath, local_output_dir)
                try:
                    size = os.path.getsize(fpath)
                    if size < 1_000_000:  # Only read files < 1MB
                        with open(fpath) as f:
                            output_files[relpath] = f.read()
                except Exception:
                    pass

        logger.info(
            f"[{host.instance_id}] Extracted {len(output_files)} output file(s) "
            f"from rootfs for slot {slot}"
        )
        return output_files

    def run(self,
            instance_name: str = "chia-firesim-run",
            instance_prefix: str | None = None,
            terminate_on_failure: bool = True) -> FireSimRunResult:
        """Execute the full FPGA simulation pipeline end to end.

        Launches the F2 instance(s), waits for SSH, runs infrasetup in parallel
        across hosts (deploy artifacts, clear and flash FPGAs with the AGFI),
        boots one simulation per slot, monitors them to completion (retrying on
        a cycle-0 deadlock with a reset-and-reflash, up to
        ``DEADLOCK_MAX_RETRIES``), then collects uartlogs and output files. All
        launched instances are terminated in the ``finally`` block.

        Args:
            instance_name: EC2 Name tag for the launched instance(s); a UTC
                timestamp is appended to keep names unique.
            instance_prefix: Optional prefix prepended to ``instance_name``
                (as ``{instance_prefix}_{instance_name}``); None leaves the
                name unprefixed.
            terminate_on_failure: If False, leave instances running when the
                pipeline raises — useful for post-mortem SSH debugging.
                On a clean completion, instances are always terminated.

        Returns:
            FireSimRunResult (see :mod:`chia.firesim.state_def`) with the
            per-slot uartlogs, rootfs/sim output files, overall ``success``
            (true only if every slot exited 0 and all completed before the
            timeout), and the wall-clock ``duration_seconds``. Internal
            failures are caught and surfaced as a result with ``success=False``
            and empty uartlogs rather than propagated.
        """
        if self.aws_config.aws_creds_dir:
            os.environ["AWS_SHARED_CREDENTIALS_FILE"] = os.path.join(self.aws_creds_dir, "credentials")
            os.environ["AWS_CONFIG_FILE"] = os.path.join(self.aws_creds_dir, "config")

        instance_ids: list[str] = []
        start_time = time.monotonic()
        had_exception = False

        try:
            # Step 1: Launch F2 instances
            num_instances = self._determine_instance_count()
            inst_config = EC2InstanceConfig(
                instance_type=self.run_config.instance_type,
                volume_size_gb=300,
                ami_id=self.run_config.ami_id,
                market=self.run_config.market,
                tags={
                    "chia-cluster": self.cluster_name,
                    "chia-op": "run",
                    "chia-workload": self.run_config.workload_name,
                },
            )
            logger.info(f"Launching {num_instances}x {self.run_config.instance_type} instances")
            ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
            effective_name = f"{instance_prefix}_{instance_name}" if instance_prefix else instance_name
            instances = launch_ec2_instances(self.aws_config, inst_config, count=num_instances, instance_name=f"{effective_name}-{ts}")
            instance_ids = [i.instance_id for i in instances]

            # Step 2: Wait for SSH readiness
            ready_instances = wait_for_instances(instance_ids, region=self.aws_config.region)

            hosts: list[EphemeralEC2Host] = []
            for inst in ready_instances:
                host = EphemeralEC2Host(inst, self.aws_config)
                host.wait_ready(timeout=SSH_WAIT_TIMEOUT)
                # Push AWS credentials for S3 access
                if os.path.isdir(self.aws_creds_dir):
                    host.run("mkdir -p ~/.aws", timeout=10)
                    host.rsync_up(f"{self.aws_creds_dir}/", "/home/ubuntu/.aws/")
                hosts.append(host)

            # Assign sim slots to hosts
            fpgas = FPGAS_PER_INSTANCE.get(self.run_config.instance_type, 1)
            host_slots: list[list[int]] = [[] for _ in hosts]
            for slot in range(self.run_config.num_sims):
                host_idx = slot // fpgas
                if host_idx >= len(hosts):
                    host_idx = len(hosts) - 1
                host_slots[host_idx].append(slot)

            # Step 3: Infrasetup in parallel
            logger.info("Running infrasetup across all hosts...")
            with ThreadPoolExecutor(max_workers=len(hosts)) as executor:
                futures = {
                    executor.submit(self._infrasetup_host, host, slots): i
                    for i, (host, slots) in enumerate(zip(hosts, host_slots))
                }
                for future in as_completed(futures):
                    idx = futures[future]
                    future.result()  # raises if infrasetup failed
                    logger.info(f"  Host {idx} infrasetup complete")

            # Step 4+5: Boot simulations with deadlock retry
            all_complete = False
            for attempt in range(1, DEADLOCK_MAX_RETRIES + 1):
                logger.info(f"Starting simulations (attempt {attempt}/{DEADLOCK_MAX_RETRIES})...")
                for host, slots in zip(hosts, host_slots):
                    for slot in slots:
                        self._start_sim_slot(host, slot)

                # Monitor loop
                logger.info("Monitoring simulations...")
                deadline = time.monotonic() + self.sim_timeout
                all_complete = False

                while not all_complete and time.monotonic() < deadline:
                    time.sleep(MONITOR_POLL_INTERVAL)
                    all_complete = True
                    for host, slots in zip(hosts, host_slots):
                        for slot in slots:
                            if not self._check_sim_complete(host, slot):
                                all_complete = False
                                break
                        if not all_complete:
                            break
                    elapsed = time.monotonic() - start_time
                    if not all_complete and int(elapsed) % 60 < MONITOR_POLL_INTERVAL:
                        logger.info(f"  Still running... ({elapsed:.0f}s elapsed)")

                # Check for cycle-0 deadlock — retry with re-flash
                any_deadlock = False
                for host, slots in zip(hosts, host_slots):
                    for slot in slots:
                        if self._check_deadlock_at_zero(host, slot):
                            any_deadlock = True
                            break
                    if any_deadlock:
                        break

                if any_deadlock and attempt < DEADLOCK_MAX_RETRIES:
                    logger.warning(
                        f"Deadlock at cycle 0 detected (attempt {attempt}/{DEADLOCK_MAX_RETRIES}). "
                        f"Re-clearing and re-flashing FPGAs..."
                    )
                    # Kill any lingering screen sessions before retry
                    for host, slots in zip(hosts, host_slots):
                        for slot in slots:
                            host.run(f"screen -S fsim{slot} -X quit 2>/dev/null",
                                     timeout=10, check=False)
                        time.sleep(2)
                        self._reset_and_reflash(host, slots)
                    logger.info(f"Retrying simulation (attempt {attempt + 1}/{DEADLOCK_MAX_RETRIES})...")
                    continue  # retry
                elif any_deadlock:
                    logger.error(f"Deadlock at cycle 0 persisted after {DEADLOCK_MAX_RETRIES} attempts")
                    break
                else:
                    break  # succeeded

            if not all_complete:
                logger.warning("Simulation timed out, killing simulations")
                for host, slots in zip(hosts, host_slots):
                    for slot in slots:
                        sim_dir = f"/home/ubuntu/sim/sim_slot_{slot}"
                        try:
                            host.run(
                                f"screen -S fsim{slot} -X quit 2>/dev/null; "
                                f"sleep 2; echo 1 > {sim_dir}/sim.exitcode",
                                timeout=30, check=False,
                            )
                        except Exception:
                            pass

            # Step 6: Collect results
            logger.info("Collecting simulation results...")
            if self.results_dir:
                base_dir = self.results_dir
            else:
                mkdtemp_dir = self.local_log_dir
                if mkdtemp_dir:
                    os.makedirs(mkdtemp_dir, exist_ok=True)
                workload = self.run_config.workload_name or "unnamed"
                prefix = f"{self.log_prefix}-{workload}-"
                base_dir = tempfile.mkdtemp(prefix=prefix, dir=mkdtemp_dir)
            local_results_dir = os.path.join(
                base_dir, "results-run",
                self.run_config.workload_name or "unnamed",
            )
            os.makedirs(local_results_dir, exist_ok=True)

            uartlogs: dict[str, str] = {}
            all_rootfs_outputs: dict[str, dict[str, str]] = {}
            all_sim_outputs: dict[str, dict[str, str]] = {}
            all_success = True

            for host, slots in zip(hosts, host_slots):
                for slot in slots:
                    uartlog, exitcode = self._collect_results(host, slot, local_results_dir)
                    rootfs_outputs = self._extract_rootfs_outputs(host, slot, local_results_dir)
                    sim_outputs = self._read_sim_outputs(slot, local_results_dir)

                    uartlogs[f"sim_slot_{slot}"] = uartlog
                    if rootfs_outputs:
                        all_rootfs_outputs[f"sim_slot_{slot}"] = rootfs_outputs
                    if sim_outputs:
                        all_sim_outputs[f"sim_slot_{slot}"] = sim_outputs
                    if exitcode != "0":
                        all_success = False

            duration = time.monotonic() - start_time
            return FireSimRunResult(
                workload_name=self.run_config.workload_name,
                success=all_success and all_complete,
                uartlogs=uartlogs,
                rootfs_outputs=all_rootfs_outputs,
                sim_outputs=all_sim_outputs,
                duration_seconds=duration,
            )

        except Exception as e:
            logger.error(f"Simulation failed: {e}")
            had_exception = True
            duration = time.monotonic() - start_time
            return FireSimRunResult(
                workload_name=self.run_config.workload_name,
                success=False,
                uartlogs={},
                duration_seconds=duration,
            )
        finally:
            # Step 7: Terminate all instances, unless the caller asked to
            # preserve them for post-mortem debugging after a failure.
            if instance_ids:
                if had_exception and not terminate_on_failure:
                    logger.warning(
                        f"Leaving {len(instance_ids)} instance(s) running for "
                        f"post-mortem (terminate_on_failure=False): {instance_ids}"
                    )
                else:
                    logger.info(f"Terminating {len(instance_ids)} instances")
                    from chia.aws.ec2 import terminate_ec2_instances
                    terminate_ec2_instances(instance_ids, region=self.aws_config.region)
