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

# Sibling of the per-task dirs under work_dir where run(keep_waveform=True)
# parks VCDs for later collect_waveform() pickup. Also the confinement root
# for read_waveform_chunk / remove_waveform.
_WAVEFORMS_SUBDIR = "waveforms"

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

        # Shared libraries the artifact carries (libriscv.so above all, which is
        # the golden model for a cospike build). Staged beside the binary rather
        # than installed anywhere shared: two runs on one worker can then use
        # different golden models, and nothing outlives the task dir. Empty for
        # artifacts built the historical way, where the image provides them.
        for lib_name, lib_content in artifact.runtime_libs:
            with open(os.path.join(self._task_dir, os.path.basename(lib_name)), "wb") as f:
                f.write(lib_content)
        if artifact.runtime_libs:
            self.logger.info(
                f"Staged {len(artifact.runtime_libs)} runtime lib(s) in "
                f"{self._task_dir}: {[n for n, _ in artifact.runtime_libs]}"
                + (f" (golden model {artifact.golden_model_digest[:12]})"
                   if artifact.golden_model_digest else ""))

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

    def _env(self) -> dict:
        """The simulator's environment, with the task dir first on the library path.

        Only meaningful for an artifact that carries its own libraries; harmless
        otherwise. Note this alone is not enough on a worker whose image happens
        to have a chipyard tree at the path chipyard baked into the binary:
        that is a DT_RPATH, which the loader consults *before* LD_LIBRARY_PATH.
        ChiselBuildNode builds bundling artifacts with --enable-new-dtags so the
        tag is DT_RUNPATH instead, which this overrides.
        """
        env = dict(os.environ)
        existing = env.get("LD_LIBRARY_PATH", "")
        env["LD_LIBRARY_PATH"] = (f"{self._task_dir}:{existing}" if existing
                                  else self._task_dir)
        return env

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
        keep_waveform: bool = False,
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
            env=self._env(),
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

        # Keep the VCD on this worker's disk for later collect_waveform()
        # pickup (the S3-free transfer path). Moved OUT of the task dir so
        # cleanup_task_dir below doesn't take it; a uuid prefix keeps
        # concurrent runs of the same test from colliding. The claim ticket
        # (path + this ray node's id) travels back in the RunResult.
        vcd_kept_path = ""
        vcd_node_id = ""
        if keep_waveform:
            vcd_local = os.path.join(self._task_dir, f"{basename}.vcd")
            if os.path.isfile(vcd_local):
                import ray
                keep_dir = os.path.join(
                    os.path.dirname(self._task_dir), _WAVEFORMS_SUBDIR)
                os.makedirs(keep_dir, exist_ok=True)
                vcd_kept_path = os.path.join(
                    keep_dir, f"{uuid.uuid4().hex[:8]}-{basename}.vcd")
                shutil.move(vcd_local, vcd_kept_path)
                vcd_node_id = ray.get_runtime_context().get_node_id()
                vcd_size_bytes = os.path.getsize(vcd_kept_path)
                self.logger.info(
                    f"Kept waveform at {vcd_kept_path} ({vcd_size_bytes} bytes)")
            else:
                self.logger.warning(
                    f"keep_waveform set but no VCD at {vcd_local} "
                    f"(was capture_waveform on and did the sim start?)")

        result = RunResult(
            test_binary_name=test_binary_name,
            log= (lambda: open(log_path, "r", errors="replace").read())(),
            out= (lambda: open(out_path, "r", errors="replace").read())(),
            returncode=sim_proc.returncode,
            success=sim_proc.returncode == 0,
            vcd_s3_path=vcd_s3_path,
            vcd_size_bytes=vcd_size_bytes,
            vcd_path=vcd_kept_path,
            vcd_node_id=vcd_node_id,
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
        keep_waveform: bool = False,
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
            keep_waveform: If True, keep the produced ``.vcd`` on this worker's
                disk (moved to ``<work_dir>/waveforms/``, surviving
                ``cleanup_task_dir``) and record its path + this worker's ray
                node id in the RunResult — the claim ticket for
                :func:`collect_waveform`, the S3-free transfer path.
                Auto-enables ``capture_waveform``. Independent of
                ``upload_to_s3`` (both may be set).

        Returns:
            RunResult: Captured ``log`` (simulator stdout) and ``out``
            (spike-dasm disassembly of stderr), the process returncode/success,
            any S3 URIs for uploaded artifacts, the kept-waveform claim ticket
            (``vcd_path``/``vcd_node_id``, when ``keep_waveform``), and an echo
            of the configured ``wave_windows``.
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

        # If the caller asked for windows, a full-trace bypass, or a kept
        # waveform, they need a destination — auto-enable VCD output. Without
        # this the windows fire but no +vcdfile= plusarg is set so nothing
        # reaches disk.
        if (wave_windows or dump_all_waveform or keep_waveform) and not capture_waveform:
            self.logger.info(
                "wave_windows/dump_all_waveform/keep_waveform requested without "
                "capture_waveform; auto-enabling VCD output"
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
            keep_waveform=keep_waveform,
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


# ---------------------------------------------------------------------------
# Kept-waveform collection — the S3-free transfer path
# ---------------------------------------------------------------------------
#
# run(keep_waveform=True) parks the VCD under <work_dir>/waveforms/ on the
# worker and returns a claim ticket (RunResult.vcd_path + .vcd_node_id).
# collect_waveform() redeems it from any machine in the cluster by driving
# read_waveform_chunk tasks pinned to that node — the same chunked-streaming
# shape as HammerNode.read_chunk / collect_fs, with node-affinity scheduling
# (as in chia.trace.profiler) instead of a placement group.
#
# The chunk/remove tasks request a tiny fractional "verilator_run" slot so
# they land on verilator workers without competing with sims for whole slots.


def _check_waveform_path(path: str) -> str:
    """Confine *path* to a ``waveforms/`` dir (the keep_waveform park)."""
    path = os.path.abspath(path)
    if os.path.basename(os.path.dirname(path)) != _WAVEFORMS_SUBDIR:
        raise ValueError(
            f"{path!r} is not under a {_WAVEFORMS_SUBDIR}/ dir; only kept "
            f"waveforms may be read/removed remotely")
    return path


@ChiaFunction(resources={"verilator_run": 0.01})
def read_waveform_chunk(path: str, offset: int, length: int) -> bytes:
    """Read ``length`` bytes at ``offset`` from a kept waveform on this worker.

    The transfer primitive behind :func:`collect_waveform`; returns ``b""`` at
    or past EOF. Dispatch pinned (node affinity) to the node in the claim
    ticket — an unpinned call may land on a worker without the file.
    """
    with open(_check_waveform_path(path), "rb") as f:
        f.seek(offset)
        return f.read(length)


@ChiaFunction(resources={"verilator_run": 0.01})
def remove_waveform(path: str) -> bool:
    """Delete a kept waveform on this worker; False if already gone.

    Idempotent — safe to call from best-effort cleanup sweeps.
    """
    try:
        os.remove(_check_waveform_path(path))
        return True
    except OSError:
        return False


def collect_waveform(
    dest_path: str,
    run_result: RunResult,
    chunk_bytes: int = 64 * 1024 * 1024,
    remove_source: bool = False,
) -> int:
    """Stream a kept waveform from its verilator worker to THIS machine's disk.

    Caller-side orchestrator, not a ChiaFunction (mirrors
    ``HammerNode.collect_fs``): it runs wherever you call it — another worker
    (e.g. the power node) or the driver — writes ``dest_path`` on that
    machine's filesystem, and pulls bytes via :func:`read_waveform_chunk`
    tasks pinned to the node recorded in *run_result*. Peak memory is
    ~``chunk_bytes``, not the file size; transfers are peer-to-peer through
    the object store.

    Args:
        dest_path: Where to write on the calling machine (parent dirs are
            created).
        run_result: A :class:`RunResult` from ``run(keep_waveform=True)`` —
            its ``vcd_path``/``vcd_node_id`` are the claim ticket.
        chunk_bytes: Transfer chunk size.
        remove_source: If True, delete the worker-side VCD after a complete,
            size-verified copy.

    Returns:
        Bytes written to ``dest_path``.

    Raises:
        ValueError: *run_result* carries no claim ticket.
        RuntimeError: the copied size disagrees with the size recorded at
            keep time (truncated/clobbered source).
    """
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
    from chia.base.ChiaFunction import get

    if not (run_result.vcd_path and run_result.vcd_node_id):
        raise ValueError(
            f"RunResult for {run_result.test_binary_name!r} has no kept "
            f"waveform — was the run dispatched with keep_waveform=True?")
    pin = {"scheduling_strategy": NodeAffinitySchedulingStrategy(
        node_id=run_result.vcd_node_id, soft=False)}

    dest_path = os.path.abspath(dest_path)
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    written = 0
    with open(dest_path, "wb") as out:
        while True:
            chunk = get(read_waveform_chunk.options(**pin).chia_remote(
                run_result.vcd_path, written, chunk_bytes))
            if not chunk:
                break
            out.write(chunk)
            written += len(chunk)

    if run_result.vcd_size_bytes and written != run_result.vcd_size_bytes:
        raise RuntimeError(
            f"collected {written} bytes of {run_result.vcd_path} but "
            f"{run_result.vcd_size_bytes} were recorded at keep time — "
            f"source truncated or clobbered?")

    if remove_source:
        get(remove_waveform.options(**pin).chia_remote(run_result.vcd_path))
    return written
