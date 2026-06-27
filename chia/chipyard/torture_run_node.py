"""Run RISC-V Torture against a compiled Chipyard/BOOM RTL simulator and diff the
Spike vs RTL architectural signatures."""
import fcntl
import logging
import os
import re
import stat
import subprocess
import uuid

from chia.chipyard.chisel_build_node import ChiselBuildNode
from chia.chipyard.state_def import (
    BuildArtifact,
    TortureMode,
    TortureResult,
    TortureTestRun,
)
from chia.base.ChiaFunction import ChiaFunction

# Drops -XX:MaxPermSize=128M (Unrecognized VM option on JDK 9+) and adds
# -Djava.security.manager=allow so the bundled sbt-launch.jar (1.4.4) can
# still call System.setSecurityManager on JDK 17+ without UnsupportedOperationException.
SBT_OVERRIDE = "java -Djava.security.manager=allow -Xmx2G -Xss8M -jar sbt-launch.jar"


class TortureRunNode:
    """Runs RISC-V Torture against a compiled Chipyard/BOOM RTL simulator.

    `Torture <https://github.com/ucb-bar/riscv-torture>`_ randomly generates
    RISC-V assembly tests, runs each on both Spike (the ISA reference model)
    and the RTL simulator under test (the DUT), and diffs their architectural
    signatures. This node drives
    the ``make`` flow in ``<chipyard_path>/tools/torture``: it writes the
    simulator binary from a :class:`BuildArtifact` to disk, passes it as the
    Torture ``R_SIM``, invokes the appropriate target for the requested
    :class:`TortureMode`, and collects the generated test, disassembly, and
    Spike/RTL signatures for each run.

    Because Torture writes into a shared ``tools/torture/output`` directory,
    concurrent runs on the same checkout are serialized with a file lock.
    """

    logging_name = "TortureRunNode"

    def __init__(
        self,
        chipyard_path: str,
        sbt_override: str = SBT_OVERRIDE,
        timeout_seconds: int = 1800,
        logging_level: int = logging.DEBUG,
    ):
        """Configure a Torture runner against a Chipyard checkout.

        Args:
            chipyard_path: Absolute path to the Chipyard checkout on the build
                node. Torture is driven out of ``<chipyard_path>/tools/torture``
                (with its shared ``output/`` subdirectory). Path for the CHIA chipyard container is ``/home/ray/chipyard``
            sbt_override: The ``SBT=`` command handed to ``make``, overriding the
                Makefile's default launcher.
            timeout_seconds: Wall-clock timeout for the ``make`` subprocess
                (default 30 min). For :attr:`TortureMode.OVERNIGHT` the effective
                timeout is raised to at least ``overnight_minutes * 60 + 600`` so
                the make process outlives the overnight loop.
            logging_level: Logging level for this node's logger.
        """
        self.chipyard_path = chipyard_path
        self.torture_path = os.path.join(chipyard_path, "tools", "torture")
        self.output_path = os.path.join(self.torture_path, "output")
        self.sbt_override = sbt_override
        self.timeout_seconds = timeout_seconds
        self.logger = logging.getLogger(self.logging_name)
        self.logger.setLevel(logging_level)

    def _setup(self, artifact: BuildArtifact, work_dir: str) -> tuple[str, str]:
        os.makedirs(work_dir, exist_ok=True)
        task_dir = os.path.join(work_dir, uuid.uuid4().hex[:8])
        os.makedirs(task_dir, exist_ok=True)
        sim_path = os.path.join(task_dir, artifact.simulator_binary_name)
        with open(sim_path, "wb") as f:
            f.write(artifact.simulator_binary_content)
        os.chmod(sim_path, os.stat(sim_path).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        self.logger.info(f"Setup complete. task_dir={task_dir}, sim_path={sim_path}")
        return task_dir, sim_path

    def _flock_torture(self) -> int:
        lock_path = os.path.join(self.torture_path, ".chia-torture.lock")
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR)
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        return lock_fd

    def _release(self, lock_fd: int) -> None:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)

    def _clean_output(self) -> None:
        if not os.path.isdir(self.output_path):
            return
        try:
            subprocess.run(
                ["make", "-C", self.output_path, "clean-all"],
                capture_output=True, text=True, timeout=60,
            )
        except subprocess.TimeoutExpired:
            self.logger.warning("torture output clean-all timed out")

    def _run_make(self, args: list[str], timeout_seconds: int) -> subprocess.CompletedProcess:
        cmd = ["make", "-C", self.torture_path] + args + [f"SBT={self.sbt_override}"]
        self.logger.info(f"Running: {cmd}")
        return subprocess.run(
            cmd,
            capture_output=True, text=True, timeout=timeout_seconds,
        )

    def _slurp(self, path: str) -> str:
        try:
            with open(path, "r", errors="replace") as f:
                return f.read()
        except (FileNotFoundError, IsADirectoryError):
            return ""

    def _parse_single_stdout(self, stdout: str) -> tuple[bool, list[str]]:
        """testrun emits '// All signatures match for <bin>' on success and
        '// Simulation failed for <bin>:' / '// Mismatched sigs for <bin>:' on failure."""
        fails = re.findall(r"//\s+Simulation failed for (\S+):", stdout)
        mism = re.findall(r"//\s+Mismatched sigs for (\S+):", stdout)
        if fails or mism:
            # dedupe preserving order
            return False, list(dict.fromkeys(fails + mism))
        if "All signatures match" in stdout:
            return True, []
        # Neither marker — testrun did not reach the diff phase (compile error, generator crash).
        return False, []

    def _build_test_run(self, abs_bin: str, success: bool) -> TortureTestRun:
        """Slurp .S/.dump/.spike.sig/.rtlsim.sig (and any narrowed pseg) for one test."""
        base = os.path.basename(abs_bin)
        test_s = self._slurp(abs_bin + ".S")
        test_dump = self._slurp(abs_bin + ".dump")
        spike_sig = self._slurp(abs_bin + ".spike.sig")
        rtlsim_sig = self._slurp(abs_bin + ".rtlsim.sig")

        pseg_s: str | None = None
        if not success:
            output_dir = os.path.dirname(abs_bin)
            if os.path.isdir(output_dir):
                for fname in sorted(os.listdir(output_dir)):
                    if fname.startswith(base + "_pseg_") and fname.endswith(".S"):
                        pseg_s = self._slurp(os.path.join(output_dir, fname))
                        break
        return TortureTestRun(
            name=base, success=success,
            test_s=test_s, test_dump=test_dump,
            spike_sig=spike_sig, rtlsim_sig=rtlsim_sig,
            pseg_test_s=pseg_s,
        )

    def _persist_test(self, test: TortureTestRun, task_dir: str) -> None:
        """Copy a test's artifacts into <task_dir>/tests/<name>/ so they survive cross-run cleanup."""
        persist_dir = os.path.join(task_dir, "tests", test.name)
        os.makedirs(persist_dir, exist_ok=True)
        for ext, content in [
            (".S", test.test_s), (".dump", test.test_dump),
            (".spike.sig", test.spike_sig), (".rtlsim.sig", test.rtlsim_sig),
        ]:
            if content:
                with open(os.path.join(persist_dir, test.name + ext), "w") as f:
                    f.write(content)
        if test.pseg_test_s is not None:
            with open(os.path.join(persist_dir, test.name + "_pseg.S"), "w") as f:
                f.write(test.pseg_test_s)

    def _gather_single(self, stdout: str, all_match: bool, failing_binaries: list[str], task_dir: str) -> list[TortureTestRun]:
        """SINGLE/REPLAY: one test ran. Pull its artifacts whether it passed or failed."""
        tests: list[TortureTestRun] = []
        if all_match:
            # On success, testrun emits the binary path in "All signatures match for <bin>".
            success_bins = re.findall(r"//\s+All signatures match for (\S+)", stdout)
            for bin_path in dict.fromkeys(success_bins):
                abs_bin = bin_path if os.path.isabs(bin_path) else os.path.join(self.torture_path, bin_path)
                t = self._build_test_run(abs_bin, success=True)
                self._persist_test(t, task_dir)
                tests.append(t)
        else:
            for bin_path in failing_binaries:
                abs_bin = bin_path if os.path.isabs(bin_path) else os.path.join(self.torture_path, bin_path)
                t = self._build_test_run(abs_bin, success=False)
                self._persist_test(t, task_dir)
                tests.append(t)
        return tests

    def _gather_overnight(self, stdout: str, failed_dir: str, task_dir: str) -> tuple[int, int, list[TortureTestRun]]:
        """OVERNIGHT: count passes from stdout (overnight deletes passing artifacts);
        slurp every failure from failedtests/.  Returns (num_tests, num_failures, tests)."""
        passes = len(re.findall(r"All signatures match", stdout))
        tests: list[TortureTestRun] = []
        if os.path.isdir(failed_dir):
            seen: set[str] = set()
            for fname in sorted(os.listdir(failed_dir)):
                base, _ = os.path.splitext(fname)
                if base.endswith((".spike", ".rtlsim", ".csim")):
                    base = base.rsplit(".", 1)[0]
                if base in seen:
                    continue
                seen.add(base)
                fbase = os.path.join(failed_dir, base)
                t = self._build_test_run(fbase, success=False)
                self._persist_test(t, task_dir)
                tests.append(t)
        return passes + len(tests), len(tests), tests

    @ChiaFunction(resources={"chipyard": 1})
    def torture(
        self,
        artifact: BuildArtifact,
        mode: TortureMode = TortureMode.SINGLE,
        work_dir: str = "/tmp/chia-torture",
        torture_config_file: str | None = None,
        overnight_minutes: int = 30,
        overnight_max_failures: int = 1,
        replay_test_s: str | None = None,
    ) -> TortureResult:
        """Run Torture against a pre-built simulator and collect the results.

        The simulator binary from ``artifact`` is written to a per-run task
        directory under ``work_dir`` and passed to Torture as ``R_SIM``. The
        make target invoked depends on ``mode``:

        * :attr:`TortureMode.SINGLE` → ``make rgentest`` — generate one test,
          run it on the DUT and Spike, and diff signatures.
        * :attr:`TortureMode.OVERNIGHT` → ``make rnight`` — loop generating and
          running tests until ``overnight_max_failures`` failures accumulate or
          ``overnight_minutes`` elapse; failing tests are saved under
          ``failedtests/``.
        * :attr:`TortureMode.REPLAY` → ``make rtest`` — run a caller-supplied
          assembly test (``replay_test_s``) instead of generating one.

        If ``artifact.success`` is False the run is skipped and an unsuccessful
        :class:`TortureResult` is returned immediately. Runs are serialized on a
        per-checkout file lock.

        Args:
            artifact: Compiled RTL simulator to test, from a
                :class:`ChiselBuildNode` build. Its ELF bytes become the Torture
                ``R_SIM`` (the DUT); Spike is the implicit reference model.
            mode: Which Torture flow to run — see :class:`TortureMode`.
            work_dir: Base directory under which a fresh per-run task directory
                (random 8-hex name) is created to hold the simulator binary and
                collected artifacts.
            torture_config_file: Optional Torture generator config, passed as
                ``-C <file>`` (Torture's ``--config``). Controls the generated
                instruction mix / sequences. ``None`` uses Torture's default.
            overnight_minutes: OVERNIGHT only — how long the generate-and-test
                loop runs, passed as ``-m <minutes>``. Also extends the make
                subprocess timeout. Ignored in other modes.
            overnight_max_failures: OVERNIGHT only — stop after this many failing
                tests, passed as Torture's ``-t <count>`` threshold. Ignored in
                other modes.
            replay_test_s: REPLAY only — the assembly source to replay. Written
                to ``replay.S`` and passed as ``TEST=`` (Torture's ``-a``).
                Required for REPLAY; raises :class:`ValueError` if missing.

        Returns:
            A :class:`TortureResult` with overall ``success``, the test/failure
            counts, per-test artifacts (:class:`TortureTestRun`), and the raw
            make ``stdout``/``stderr``/``returncode``. ``build_artifact`` is left
            ``None`` here; :meth:`torture_from_config` populates it.
        """
        if not artifact.success:
            return TortureResult(
                name="torture",
                config=artifact.config,
                config_package=artifact.config_package,
                mode=mode,
                success=False,
                num_tests=0,
                num_failures=0,
                tests=[],
                stdout=artifact.stdout,
                stderr=artifact.stderr + "\nTorture skipped: build artifact was unsuccessful.",
                returncode=artifact.returncode,
                build_artifact=None,
            )

        task_dir, sim_path = self._setup(artifact, work_dir)
        lock_fd = self._flock_torture()
        try:
            self._clean_output()

            opts: list[str] = []
            if torture_config_file:
                opts += ["-C", torture_config_file]

            run_timeout = self.timeout_seconds
            if mode == TortureMode.SINGLE:
                make_args = ["rgentest", f"R_SIM={sim_path}"]
                if opts:
                    make_args.append(f"OPTIONS={' '.join(opts)}")

            elif mode == TortureMode.OVERNIGHT:
                failed_dir = os.path.join(task_dir, "failedtests")
                os.makedirs(failed_dir, exist_ok=True)
                onight_opts = opts + [
                    "-m", str(overnight_minutes),
                    "-t", str(overnight_max_failures),
                    "-p", failed_dir,
                ]
                make_args = ["rnight", f"R_SIM={sim_path}", f"OPTIONS={' '.join(onight_opts)}"]
                # Ensure the make subprocess doesn't time out before the overnight loop completes.
                run_timeout = max(self.timeout_seconds, overnight_minutes * 60 + 600)

            elif mode == TortureMode.REPLAY:
                if not replay_test_s:
                    raise ValueError("REPLAY mode requires replay_test_s")
                replay_path = os.path.join(task_dir, "replay.S")
                with open(replay_path, "w") as f:
                    f.write(replay_test_s)
                make_args = ["rtest", f"R_SIM={sim_path}", f"TEST={replay_path}"]
                if opts:
                    make_args.append(f"OPTIONS={' '.join(opts)}")

            else:
                raise ValueError(f"Unsupported TortureMode: {mode}")

            try:
                proc = self._run_make(make_args, run_timeout)
                stdout, stderr, rc = proc.stdout, proc.stderr, proc.returncode
            except subprocess.TimeoutExpired as e:
                stdout = e.stdout.decode(errors="replace") if isinstance(e.stdout, bytes) else (e.stdout or "")
                stderr = e.stderr.decode(errors="replace") if isinstance(e.stderr, bytes) else (e.stderr or "")
                stderr += f"\nTorture run timed out after {run_timeout}s"
                rc = -1

            if mode == TortureMode.OVERNIGHT:
                num_tests, num_failures, tests = self._gather_overnight(
                    stdout, os.path.join(task_dir, "failedtests"), task_dir)
                # overnight/run does System.exit(2) on errors; that's a *normal* completion for us.
                run_completed = rc in (0, 2)
                success = run_completed and num_failures == 0
            else:
                all_match, failing_bins = self._parse_single_stdout(stdout)
                tests = self._gather_single(stdout, all_match, failing_bins, task_dir)
                num_failures = sum(1 for t in tests if not t.success)
                num_tests = max(len(tests), 1)
                success = (rc == 0) and all_match
        finally:
            self._release(lock_fd)

        return TortureResult(
            name="torture",
            config=artifact.config,
            config_package=artifact.config_package,
            mode=mode,
            success=success,
            num_tests=num_tests,
            num_failures=num_failures,
            tests=tests,
            stdout=stdout,
            stderr=stderr,
            returncode=rc,
        )

    @ChiaFunction(resources={"chipyard": 1})
    def torture_from_config(
        self,
        config: str,
        config_package: str = "chipyard",
        mode: TortureMode = TortureMode.SINGLE,
        work_dir: str = "/tmp/chia-torture",
        build_kwargs: dict | None = None,
        torture_config_file: str | None = None,
        overnight_minutes: int = 30,
        overnight_max_failures: int = 1,
        replay_test_s: str | None = None,
    ) -> TortureResult:
        builder = ChiselBuildNode(
            chipyard_path=self.chipyard_path,
            config=config,
            config_package=config_package,
            **(build_kwargs or {}),
        )
        artifact = builder.build()
        if not artifact.success:
            return TortureResult(
                name="torture",
                config=config,
                config_package=config_package,
                mode=mode,
                success=False,
                num_tests=0,
                num_failures=0,
                tests=[],
                stdout=artifact.stdout,
                stderr=artifact.stderr + "\nTorture skipped: ChiselBuildNode failed.",
                returncode=artifact.returncode,
                build_artifact=artifact,
            )
        result = self.torture(
            artifact=artifact,
            mode=mode,
            work_dir=work_dir,
            torture_config_file=torture_config_file,
            overnight_minutes=overnight_minutes,
            overnight_max_failures=overnight_max_failures,
            replay_test_s=replay_test_s,
        )
        result.build_artifact = artifact
        return result
