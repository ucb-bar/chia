import fcntl
import os
import shutil
import stat
import subprocess
import threading
import logging
import uuid
from urllib.parse import urlparse
from chia.chipyard.state_def import BuildArtifact, RunResult, WaveWindow
from chia.trace.profiler import get_profiler
from chia.chipyard.numa_prefix import get_numa_prefix
from chia.base.ChiaFunction import ChiaFunction


# Maximum number of wave windows supported (matches the binder's compile-time
# WithSelectiveWaveform(maxWindows=64) cap).
_MAX_WAVE_WINDOWS = 64

class VerilatorRunNode:
    """Runs one test ELF on a prebuilt chipyard Verilator simulator.
    """

    logging_name = "VerilatorRunNode"

    def __init__(self, logging_level: int = logging.DEBUG):
        """Construct a run node.

        Args:
            logging_level: Python logging level for this node's logger. The
                node is otherwise stateless at construction time; per-run state
                (task dir, binary path) is created in ``_setup`` when ``run``
                is invoked.
        """
        self._binary_path = None
        self.logger = logging.getLogger(self.logging_name)
        self.logger.setLevel(logging_level)

    def _write_binary_once(self, path: str, content: bytes) -> None:
        """Write a binary to disk exactly once per new content.

        Uses flock to coordinate parallel tasks in the same container.
        A SHA-256 hash stored in the lock file tracks what was last
        written: the first task to see a mismatch writes the binary
        and updates the hash; subsequent tasks skip.
        """
        import hashlib
        content_hash = hashlib.sha256(content).hexdigest()

        lock_path = path + ".lock"
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR)
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        try:
            os.lseek(lock_fd, 0, os.SEEK_SET)
            stored_hash = os.read(lock_fd, 64).decode().strip()

            if stored_hash == content_hash:
                return

            with open(path, "wb") as f:
                f.write(content)
            os.chmod(path, os.stat(path).st_mode
                     | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

            os.ftruncate(lock_fd, 0)
            os.lseek(lock_fd, 0, os.SEEK_SET)
            os.write(lock_fd, content_hash.encode())
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)

    def _setup(self, artifact: BuildArtifact, test_binary_content: bytes, test_binary_name: str, work_dir: str, dramsim_ini_files: dict[str, bytes]) -> str:
        os.makedirs(work_dir, exist_ok=True)

        # Create a unique task-level subdirectory to isolate concurrent runs
        # sharing the same Docker container / work_dir.
        self._task_dir = os.path.join(work_dir, uuid.uuid4().hex[:8])
        os.makedirs(self._task_dir, exist_ok=True)

        binary_path = os.path.join(self._task_dir, artifact.simulator_binary_name)
        with open(binary_path, "wb") as f:
            f.write(artifact.simulator_binary_content)
        os.chmod(binary_path, os.stat(binary_path).st_mode
                 | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        self._binary_path = binary_path

        test_binary_path = os.path.join(self._task_dir, test_binary_name)
        with open(test_binary_path, "wb") as f:
            f.write(test_binary_content)
        os.chmod(test_binary_path, os.stat(test_binary_path).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

        if dramsim_ini_files:
            dramsim_dir = os.path.join(self._task_dir, "dramsim_ini")
            os.makedirs(dramsim_dir, exist_ok=True)
            for filename, content in dramsim_ini_files.items():
                with open(os.path.join(dramsim_dir, filename), "wb") as f:
                    f.write(content)
            self._dramsim_ini_dir = dramsim_dir
        else:
            self._dramsim_ini_dir = ""

        self.logger.info(f"Setup complete. Task dir: {self._task_dir}, Simulator: {self._binary_path}, test binary: {test_binary_path}")
        return test_binary_path

    def _cleanup_task_dir(self):
        """Remove the per-task directory to avoid disk bloat.

        Ideally we'd share the simulator binary across tasks from the same
        optimization and only isolate logs/outputs, but per-task directories
        are the simplest fix for the concurrent work_dir race condition.
        """
        if hasattr(self, '_task_dir') and os.path.isdir(self._task_dir):
            shutil.rmtree(self._task_dir, ignore_errors=True)

    def _upload_file_to_s3(
        self,
        local_path: str,
        s3_path: str,
        aws_access_key_id: str = "",
        aws_secret_access_key: str = "",
        aws_session_token: str = "",
        aws_region: str = "",
    ) -> tuple[str, int]:
        """Upload a local file to S3 under ``s3_path/<basename>``.

        If `aws_access_key_id` and `aws_secret_access_key` are both non-empty,
        they're passed explicitly to ``boto3.client("s3", ...)`` (local-dev
        path). Otherwise boto3 walks its default credential chain
        (env vars / ``~/.aws/credentials`` / EC2 instance role / etc.).

        Returns ``(uploaded_s3_uri, local_size_bytes)``. On any failure logs a
        warning and returns ``("", size)`` so the caller's run is not failed
        by an upload glitch. A missing local file is also a soft failure
        (returns ``("", 0)``) — callers may pass paths that only exist
        conditionally (e.g. a VCD that wasn't generated).
        """
        if not local_path or not os.path.exists(local_path):
            self.logger.warning(f"File not found for upload at {local_path}; skipping")
            return "", 0
        if not s3_path.startswith("s3://"):
            raise ValueError(f"s3_path must start with 's3://', got: {s3_path!r}")

        size = os.path.getsize(local_path)
        parsed = urlparse(s3_path)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/").rstrip("/")
        filename = os.path.basename(local_path)
        key = f"{prefix}/{filename}" if prefix else filename

        client_kwargs: dict = {}
        if aws_access_key_id and aws_secret_access_key:
            client_kwargs["aws_access_key_id"] = aws_access_key_id
            client_kwargs["aws_secret_access_key"] = aws_secret_access_key
            if aws_session_token:
                client_kwargs["aws_session_token"] = aws_session_token
            self.logger.debug("Using explicit AWS credentials for S3 upload")
        elif aws_access_key_id or aws_secret_access_key:
            # Both must be set together — having only one is almost always a bug.
            raise ValueError(
                "aws_access_key_id and aws_secret_access_key must both be set "
                "(got only one). Leave both empty to use the default credential chain."
            )
        if aws_region:
            client_kwargs["region_name"] = aws_region

        try:
            import boto3
            boto3.client("s3", **client_kwargs).upload_file(local_path, bucket, key)
            uri = f"s3://{bucket}/{key}"
            self.logger.info(f"Uploaded {filename} ({size} bytes) -> {uri}")
            return uri, size
        except Exception as e:
            self.logger.warning(f"S3 upload of {filename} failed: {e}; s3_path will be empty")
            return "", size

    def _execute(
        self,
        argv: list[str],
        test_binary_name: str,
        test_binary_path: str,
        timeout_seconds: int | None = None,
        cleanup_task_dir: bool = True,
        capture_waveform: bool = False,
        wave_windows: list[WaveWindow] = (),
        upload_to_s3: bool = False,
        s3_path: str = "",
        aws_access_key_id: str = "",
        aws_secret_access_key: str = "",
        aws_session_token: str = "",
        aws_region: str = "",
    ) -> RunResult:
        """Run the simulator and pipe stderr through spike-dasm.

        Both `run` and `run_metasim` delegate here after building their
        respective argv lists.  The caller supplies everything up to (but
        not including) the ``+permissive-off <binary>`` trailer — this
        method appends that trailer, executes the process, and returns
        the RunResult.
        """
        argv += ["+permissive-off", test_binary_path]

        basename = os.path.splitext(test_binary_name)[0]
        log_path = os.path.join(self._task_dir, f"{basename}.log")
        out_path = os.path.join(self._task_dir, f"{basename}.out")

        # clear log/out files
        with open(log_path, "w") as f:
            f.write("")
        logfile = open(log_path, "ab")
        with open(out_path, "w") as f:
            f.write("")
        outfile = open(out_path, "ab")

        # sim_proc.stdout -> log_path
        # sim_proc.stderr -> dasm_proc.std_in -> dasm_proc.std_out -> out_path
        sim_proc = subprocess.Popen(
            argv,
            cwd=self._task_dir,
            stdout=logfile,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL,
        )

        dasm_proc = subprocess.Popen(
            ["spike-dasm"],
            stdin=sim_proc.stderr,
            stdout=outfile,
        )

        try:
            sim_proc.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            logfile.close()
            sim_proc.kill()
            sim_proc.wait()
        finally:
            logfile.close()

        dasm_proc.wait()
        outfile.close()

        # Optionally upload .vcd / .out / .log to S3 BEFORE we wipe task_dir.
        # The .vcd only exists when capture_waveform was on; _upload_file_to_s3
        # treats a missing local file as a soft skip.
        vcd_s3_path = ""
        vcd_size_bytes = 0
        out_s3_path = ""
        log_s3_path = ""
        if upload_to_s3 and s3_path:
            aws_kwargs = dict(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                aws_region=aws_region,
            )
            vcd_local = os.path.join(self._task_dir, f"{basename}.vcd")
            vcd_s3_path, vcd_size_bytes = self._upload_file_to_s3(
                vcd_local, s3_path, **aws_kwargs)
            out_s3_path, _ = self._upload_file_to_s3(out_path, s3_path, **aws_kwargs)
            log_s3_path, _ = self._upload_file_to_s3(log_path, s3_path, **aws_kwargs)

        result = RunResult(
            test_binary_name=test_binary_name,
            log= (lambda: open(log_path, "r", errors="replace").read())(),
            out= (lambda: open(out_path, "r", errors="replace").read())(),
            returncode=sim_proc.returncode,
            success=sim_proc.returncode == 0,
            vcd_s3_path=vcd_s3_path,
            vcd_size_bytes=vcd_size_bytes,
            out_s3_path=out_s3_path,
            log_s3_path=log_s3_path,
            wave_windows=list(wave_windows),
        )

        # Parse simulation cycles from log output
        import re
        cycles_match = re.search(r'after\s+(\d+)\s+simulation cycles', result.out)
        profiler = get_profiler()
        if cycles_match:
            profiler.add_info({"simulation_cycles": int(cycles_match.group(1))})
        if capture_waveform:
            profiler.add_info({
                "wf_windows_count": len(wave_windows),
                "vcd_size_bytes": vcd_size_bytes,
                "vcd_s3_path": vcd_s3_path,
            })

        if (cleanup_task_dir):
            self._cleanup_task_dir()
        return result

    @ChiaFunction(resources={"verilator_run": 1})
    def run(
        self,
        artifact: BuildArtifact,
        test_binary_content: bytes,
        test_binary_name: str,
        work_dir: str,
        plusargs: dict = {},
        timeout_cycles: int | None = None,
        timeout_seconds: int | None = None,
        dramsim_ini_files: dict[str, bytes] = {},
        capture_waveform: bool = False,
        verbose: bool = True,
        cleanup_task_dir: bool = True,
        numactl: bool = False,
        wave_windows: list[WaveWindow] = (),
        dump_all_waveform: bool = False,
        upload_to_s3: bool = False,
        s3_path: str = "",
        aws_access_key_id: str = "",
        aws_secret_access_key: str = "",
        aws_session_token: str = "",
        aws_region: str = "",
    ) -> RunResult:
        """Run one test ELF on the prebuilt Chipyard Verilator simulator.

        Writes the simulator and test binaries into an isolated per-task
        directory, assembles the simulator's ``+plusarg`` command line, executes
        it (piping the committed-instruction stderr through ``spike-dasm``), and
        returns the captured output. By default, runs on nodes tagged with the
        ``verilator_run`` resource.

        Args:
            artifact: The :class:`BuildArtifact` produced by
                :meth:`ChiselBuildNode.build`. Supplies the simulator ELF bytes
                and its name; the binary is materialized into the task dir and
                marked executable.
            test_binary_content: Raw bytes of the RISC-V test ELF to run (the
                program the simulator loads over the front-end server / HTIF).
            test_binary_name: Filename to give that ELF on disk; its stem also
                names the ``.log`` and ``.out`` output files.
            work_dir: Base working directory. A unique 8-hex-char subdirectory
                is created under it per run so concurrent runs sharing the same
                container/work_dir don't collide.
            plusargs: Extra simulator ``+plusargs`` as a dict. Each entry is
                emitted as ``+key`` when the value is falsy, else ``+key=value``
                (e.g. ``{"+loadmem": path}`` or ``{"+verbose": ""}``).
            timeout_cycles: Simulated-cycle budget; passed as
                ``+max-cycles=<n>``. The simulator self-terminates when reached.
                ``None`` omits the plusarg (no cycle limit).
            timeout_seconds: Wall-clock limit. On expiry the simulator process
                is killed and whatever was captured so far is returned.
            dramsim_ini_files: DRAMSim2 model config as ``{filename: bytes}``.
                When non-empty they're written to a ``dramsim_ini`` dir and the
                sim is launched with ``+dramsim +dramsim_ini_dir=<dir>`` to use
                the cycle-accurate DRAM model instead of the simple memory.
            capture_waveform: If True, emit a VCD via ``+vcdfile=<task>/<stem>.vcd``.
                Requires the simulator to have been built with
                ``target=VERILATOR_DEBUG``. Auto-enabled if ``wave_windows`` or
                ``dump_all_waveform`` is set.
            verbose: If True, append ``+verbose`` (commit-log / verbose sim
                output).
            cleanup_task_dir: If True (default), delete the per-task directory
                after the run (and after any S3 upload) to bound disk usage.
            numactl: If True, prefix the simulator argv with the platform's
                ``numactl`` binding (from :func:`get_numa_prefix`) to pin it to
                a NUMA node.
            wave_windows: chia_artifact-specific *temporal* waveform filter — a
                list of :class:`WaveWindow` PC-triggered dump windows. Each is
                emitted as ``+wf_pc_<i>=<hex> +wf_n_<i> +wf_cyc_<i>``: dump for
                ``cyc`` testbench cycles starting at the ``n``-th retired commit of
                ``pc``. At most ``_MAX_WAVE_WINDOWS`` (64); each is validated for
                ``pc>0, n>=1, cyc>0``.
            dump_all_waveform: If True, append ``+wf_dump_all=1`` to bypass the
                window filter and dump the entire run (combine with
                ``wf_scopes`` at build time to still bound it spatially).
            upload_to_s3: If True (and ``s3_path`` is set), upload the produced
                ``.vcd`` (if any), ``.out`` and ``.log`` to S3 before the task
                dir is cleaned up. A missing VCD is a soft skip.
            s3_path: Destination ``s3://bucket/prefix`` for uploads. Files land
                under ``<s3_path>/<basename>``. Required when
                ``upload_to_s3=True``.
            aws_access_key_id: Explicit AWS access key for the upload. If set,
                ``aws_secret_access_key`` must also be set; otherwise boto3's
                default credential chain (env vars / profile / instance role)
                is used.
            aws_secret_access_key: Explicit AWS secret key (see above).
            aws_session_token: Optional session token for temporary
                (STS) credentials; only used when explicit keys are given.
            aws_region: Optional AWS region name for the S3 client.

        Returns:
            RunResult: Captured ``log`` (simulator stdout) and ``out``
            (spike-dasm disassembly of stderr), the process returncode/success,
            any S3 URIs for uploaded artifacts, and an echo of the configured
            ``wave_windows``.
        """
        profiler = get_profiler()
        profiler.add_info({"test_binary": test_binary_name})
        test_binary_path = self._setup(artifact, test_binary_content, test_binary_name, work_dir, dramsim_ini_files)

        # Validate wave-window args before launching the simulator.
        wave_windows = list(wave_windows)
        if len(wave_windows) > _MAX_WAVE_WINDOWS:
            raise ValueError(
                f"max {_MAX_WAVE_WINDOWS} wave_windows (got {len(wave_windows)})"
            )
        for i, w in enumerate(wave_windows):
            if not isinstance(w, WaveWindow):
                raise TypeError(f"wave_windows[{i}] must be WaveWindow, got {type(w).__name__}")
            if w.pc <= 0 or w.cyc <= 0 or w.n < 1:
                raise ValueError(
                    f"WaveWindow[{i}] invalid: pc=0x{w.pc:x}, n={w.n}, cyc={w.cyc} "
                    "(require pc>0, n>=1, cyc>0)"
                )

        # If the caller asked for windows or a full-trace bypass, they need a
        # destination — auto-enable VCD output. Without this the windows fire
        # but no +vcdfile= plusarg is set so nothing reaches disk.
        if (wave_windows or dump_all_waveform) and not capture_waveform:
            self.logger.info(
                "wave_windows/dump_all_waveform requested without capture_waveform; "
                "auto-enabling VCD output"
            )
            capture_waveform = True

        basename = os.path.splitext(test_binary_name)[0]
        argv = [self._binary_path, "+permissive"]

        if self._dramsim_ini_dir:
            argv += ["+dramsim", f"+dramsim_ini_dir={self._dramsim_ini_dir}"]

        if timeout_cycles is not None:
            argv += [f"+max-cycles={timeout_cycles}"]
        if verbose:
            argv.append("+verbose")

        if capture_waveform:
            vcd_path = os.path.join(self._task_dir, f"{basename}.vcd")
            argv.append(f"+vcdfile={vcd_path}")

        # Selective waveform plusargs: +wf_pc_<i>=<hex> +wf_n_<i> +wf_cyc_<i>
        # Hex is bare (no 0x) — matches the harness's plusarg_reader FORMAT="%h".
        for i, w in enumerate(wave_windows):
            argv += [
                f"+wf_pc_{i}={w.pc:x}",
                f"+wf_n_{i}={w.n}",
                f"+wf_cyc_{i}={w.cyc}",
            ]
        if dump_all_waveform:
            argv.append("+wf_dump_all=1")

        for k, v in plusargs.items():
            argv.append(k if not v else f"{k}={v}")

        if numactl:
            argv = get_numa_prefix().split() + argv

        return self._execute(
            argv,
            test_binary_name,
            test_binary_path,
            timeout_seconds,
            cleanup_task_dir=cleanup_task_dir,
            capture_waveform=capture_waveform,
            wave_windows=wave_windows,
            upload_to_s3=upload_to_s3,
            s3_path=s3_path,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            aws_region=aws_region,
        )

    def run_metasim(
        self,
        artifact: BuildArtifact,
        test_binary_content: bytes,
        test_binary_name: str,
        work_dir: str,
        fesvr_step_size: int = 128,
        plusargs: dict = {},
        timeout_cycles: int | None = None,
        timeout_seconds: int | None = None,
        verbose: bool = True,
        cleanup_task_dir: bool = True,
        numactl: bool = False
    ) -> RunResult:
        """Run a FireSim metasim (VFireSim) binary.

        Unlike ``run``, this method:

        - omits ``+loadmem``, ``+dramsim``, and ``+dramsim_ini_dir``
          (metasim uses FASED memory modeling)
        - omits waveform capture (metasim uses ``+waveformfile`` with a
          ``-debug`` build variant, not ``+vcdfile``)
        - always passes ``+fesvr-step-size`` (required for metasim)
        """
        profiler = get_profiler()
        profiler.add_info({"test_binary": test_binary_name})
        test_binary_path = self._setup(artifact, test_binary_content, test_binary_name, work_dir, dramsim_ini_files={})

        argv = [self._binary_path, "+permissive"]

        argv.append(f"+fesvr-step-size={fesvr_step_size}")

        if timeout_cycles is not None:
            argv += [f"+max-cycles={timeout_cycles}"]
        if verbose:
            argv.append("+verbose")

        for k, v in plusargs.items():
            argv.append(k if not v else f"{k}={v}")

        if numactl:
            argv = get_numa_prefix().split() + argv

        return self._execute(argv, test_binary_name, test_binary_path, timeout_seconds, cleanup_task_dir=cleanup_task_dir)
