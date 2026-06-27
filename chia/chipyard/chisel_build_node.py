import os
import shlex
import subprocess
import logging
from chia.chipyard.state_def import BuildArtifact, BuildTarget
from chia.trace.profiler import get_profiler
from chia.base.ChiaFunction import ChiaFunction

# Maximum number of WF_SCOPES paths supported by the Makefile and TestDriver.v
# (the .v file has WF_SCOPE_1 ... WF_SCOPE_8 ifdef branches).
_MAX_WF_SCOPES = 8


class ChiselBuildNode:
    """Elaborates a Chipyard config into a Verilator simulator binary.

    Wraps the Chipyard ``sims/verilator`` (and FireSim ``sims/firesim/sim``)
    Make flow: it shells out to ``make`` inside the appropriate sims directory
    to run the Chisel generator and Verilator, then reads the resulting
    simulator ELF back into a :class:`BuildArtifact` so it can be shipped to a
    :class:`~chia.chipyard.verilator_run_node.VerilatorRunNode` for execution.
    By default runs on cluster nodes tagged with ``chipyard=1`` resource.
    """

    logging_name = "ChiselBuildNode"

    def __init__(
        self,
        chipyard_path: str,
        config: str,
        config_package: str = "chipyard",
        target: BuildTarget = BuildTarget.VERILATOR,
        make_jobs: int = 32,
        timeout_seconds: int = 600,
        extra_make_args: dict = {},
        collect_generated_src: bool = False,
        clean_sim: bool = False,
        clean: bool = True,
        wf_scopes: list[str] | tuple[str, ...] = (),
        clean_wf_stamp: bool = True,
        logging_level: int = logging.DEBUG,
        name: str = "chipyard",
    ):
        """Configure a single Chipyard Verilator build.

        Args:
            chipyard_path: Absolute path to the Chipyard checkout (the
                ``chia_artifact`` branch) on the build node. 
                Set to ``/home/ray/chipyard`` when using the CHIA chipyard container.
                The Make flow runs
                inside ``<chipyard_path>/sims/verilator`` (or, for FireSim
                metasims, ``<chipyard_path>/sims/firesim/sim``).
            config: Chipyard ``CONFIG``: the Chisel ``Config`` class name that
                parametrizes the SoC to elaborate (e.g. ``"RocketConfig"``,
                ``"MegaBoomV3Config"``). Passed to Make as ``CONFIG=<config>``
                and becomes the suffix of the produced binary's name.
            config_package: Chipyard ``CONFIG_PACKAGE``: the Scala package the
                ``Config`` class lives in (passed as ``CONFIG_PACKAGE=``).
                Defaults to ``"chipyard"``; the generated binary is named
                ``simulator-<config_package>.harness-<config>``.
            target: Which simulator to build (see :class:`BuildTarget`):
                ``VERILATOR`` (fast, no waveforms), ``VERILATOR_DEBUG``
                (``make debug``; VCD/waveform-capable, required for
                ``wf_scopes`` and runtime wave windows), or
                ``FIRESIM_METASIM_VERILATOR`` (a FireSim metasim ``VFireSim``
                binary built under ``sims/firesim/sim``).
            make_jobs: Value passed to ``make -j`` controlling build
                parallelism (number of concurrent compile jobs).
            timeout_seconds: Wall-clock limit for the main ``make`` build. On
                expiry the build is killed and a failed ``BuildArtifact``
                (``returncode=-1``) is returned. The preceding ``clean`` steps
                have their own fixed 600s timeouts.
            extra_make_args: Additional ``KEY=value`` Make variables to append
                to the build command (e.g. ``{"VERILATOR_THREADS": 4}``). For ``FIRESIM_METASIM_VERILATOR`` these
                may include ``DESIGN`` / ``PLATFORM`` / ``PLATFORM_CONFIG`` and
                are merged over the FireSim defaults. Note: passing
                ``WF_SCOPES`` here conflicts with ``wf_scopes`` (below).
            collect_generated_src: If True, after a successful build read every
                generated ``.v``/``.sv`` file (skipping ``TestDriver.v`` and any
                DPI-C files) plus the ``.top.mems.conf`` from the
                ``gen-collateral`` directory into
                ``BuildArtifact.generated_src_files``.
            clean_sim: If True, run ``make clean-sim`` before building. This
                removes the simulator binaries but leaves the cached
                ``chipyard.jar`` and generated FIRRTL/Verilog (``gen_dir``) in
                place: so it can relink against stale Verilog.
            clean: If True (default), run ``make clean`` before building. This
                also clears the Chisel-generator cache (``chipyard.jar``) and
                ``gen_dir``, guaranteeing the build reflects the current Scala
                sources. Slower but correct.
            wf_scopes: chia_artifact-specific *spatial* waveform filter: a list
                of module-hierarchy scope paths (e.g.
                ``"TestHarness.chiptop0.system..."``) restricting which modules
                Verilator traces into the VCD. Joined with spaces into the
                ``WF_SCOPES`` Make variable. At most ``_MAX_WF_SCOPES`` (8)
                paths, and only valid with ``target=VERILATOR_DEBUG`` (the
                Makefile wires the filter only into the debug model).
            clean_wf_stamp: If True and ``wf_scopes`` is set, delete
                ``sims/verilator/.wf_scopes.stamp`` before building so Make
                re-applies the scope filter even when it would otherwise
                consider nothing changed (mirrors ``run_wave.sh``).
            logging_level: Python logging level for this node's logger.
            name: Label copied into the resulting ``BuildArtifact.name``;
                purely for downstream identification/traceability.
        """
        self.chipyard_path = chipyard_path
        self.config = config
        self.config_package = config_package
        self.target = target
        self.make_jobs = make_jobs
        self.timeout_seconds = timeout_seconds
        self.extra_make_args = dict(extra_make_args)
        self.collect_generated_src = collect_generated_src
        self.clean_sim = clean_sim
        self.clean = clean
        self.wf_scopes = list(wf_scopes)
        self.clean_wf_stamp = clean_wf_stamp
        self.logging_level = logging_level
        self.name = name
        self.logger = logging.getLogger(self.logging_name)
        self.logger.setLevel(self.logging_level)

        # WF_SCOPES validation. The Makefile only wires the spatial filter
        # into model_mk_debug, so it's only meaningful for the debug target.
        if len(self.wf_scopes) > _MAX_WF_SCOPES:
            raise ValueError(
                f"WF_SCOPES supports at most {_MAX_WF_SCOPES} paths "
                f"(got {len(self.wf_scopes)})"
            )
        if self.wf_scopes and self.target != BuildTarget.VERILATOR_DEBUG:
            raise ValueError(
                "wf_scopes requires target=BuildTarget.VERILATOR_DEBUG "
                f"(got target={self.target!r})"
            )
        if self.wf_scopes:
            if "WF_SCOPES" in self.extra_make_args:
                raise ValueError(
                    "wf_scopes and extra_make_args['WF_SCOPES'] both set; "
                    "use one or the other"
                )
            # Space-join paths for the Makefile's WF_SCOPES variable; shell
            # quoting happens later via shlex.quote() in build().
            self.extra_make_args["WF_SCOPES"] = " ".join(self.wf_scopes)

    @staticmethod
    def _format_make_args(args: dict) -> str:
        """Format a dict of make variables into a shell-safe argument string.

        Uses shlex.quote() on each value so that spaces, quotes, and other
        shell metacharacters survive ``subprocess.run(..., shell=True)``.
        Critical for multi-token values like ``WF_SCOPES="path1 path2"``.
        """
        return " ".join(f"{k}={shlex.quote(str(v))}" for k, v in args.items())

    @ChiaFunction(resources={"chipyard": 1})
    def build(self) -> BuildArtifact:
        """Run the Chipyard Make flow and return the compiled simulator.

        Takes no arguments: every input comes from the attributes set in
        ``__init__``. Selects the sims directory, binary name and ``make``
        command line from ``self.target``/``self.config``/``self.config_package``
        (plus ``self.extra_make_args``), optionally clears the WF_SCOPES stamp
        and runs ``make clean``/``clean-sim``, then invokes the build under
        ``self.timeout_seconds``.

        Returns:
            BuildArtifact: On success, carries the simulator ELF bytes plus
            ``config``/``config_package``/``target`` and (if
            ``collect_generated_src``) the generated source files. On a build
            failure or timeout, ``success=False`` with empty binary content and
            the captured stdout/stderr and returncode (``-1`` on timeout).
        """
        if self.target == BuildTarget.VERILATOR:
            sims_dir = os.path.join(self.chipyard_path, "sims/verilator")
            binary_name = f"simulator-{self.config_package}.harness-{self.config}"
            binary_path = os.path.join(sims_dir, binary_name)
            extra_args = self._format_make_args(self.extra_make_args)
            cmd = f"make -j {self.make_jobs} CONFIG={self.config} CONFIG_PACKAGE={self.config_package} {extra_args}".strip()

        elif self.target == BuildTarget.VERILATOR_DEBUG:
            sims_dir = os.path.join(self.chipyard_path, "sims/verilator")
            binary_name = f"simulator-{self.config_package}.harness-{self.config}-debug"
            binary_path = os.path.join(sims_dir, binary_name)
            extra_args = self._format_make_args(self.extra_make_args)
            cmd = f"make -j {self.make_jobs} CONFIG={self.config} CONFIG_PACKAGE={self.config_package} debug {extra_args}".strip()

        elif self.target == BuildTarget.FIRESIM_METASIM_VERILATOR:
            sims_dir = os.path.join(self.chipyard_path, "sims/firesim/sim")

            design = self.extra_make_args.get("DESIGN", "FireSim")
            platform = self.extra_make_args.get("PLATFORM", "f2")
            platform_config = self.extra_make_args.get("PLATFORM_CONFIG", "BaseF2Config")

            binary_name = f"V{design}"
            quintuplet = f"{platform}-firesim-{design}-{self.config}-{platform_config}"
            binary_path = os.path.join(sims_dir, "generated-src", platform, quintuplet, binary_name)

            makefrag_path = os.path.join(
                self.chipyard_path, "generators/firechip/chip/src/main/makefrag/firesim"
            )
            firesim_args = {
                "TARGET_PROJECT": "firesim",
                "TARGET_PROJECT_MAKEFRAG": makefrag_path,
                "TARGET_CONFIG": self.config,
                "TARGET_CONFIG_PACKAGE": self.config_package,
                "PLATFORM": platform,
                "PLATFORM_CONFIG": platform_config
            }
            all_args = {**firesim_args, **self.extra_make_args}
            extra_args = self._format_make_args(all_args)
            cmd = f"make -j {self.make_jobs} verilator {extra_args}".strip()
            print(cmd)

        else:
            raise ValueError(f"Unsupported build target: {self.target}")

        # Force re-evaluation of WF_SCOPES even when Make would otherwise
        # think nothing changed. Mirrors `rm -f .wf_scopes.stamp` from
        # sims/verilator/run_wave.sh:13. Cheap; only runs when scopes are set.
        if self.clean_wf_stamp and self.wf_scopes:
            stamp = os.path.join(self.chipyard_path, "sims/verilator/.wf_scopes.stamp")
            try:
                os.remove(stamp)
                self.logger.debug(f"Removed WF_SCOPES stamp: {stamp}")
            except FileNotFoundError:
                pass
            except OSError as e:
                self.logger.warning(f"Could not remove WF_SCOPES stamp {stamp}: {e}")

        # `make clean` removes $(CLASSPATH_CACHE) (the cached chipyard.jar),
        # $(gen_dir) (generated FIRRTL/Verilog) and the simulator binaries:
        # the only target that clears the Chisel-generator cache, so it's the
        # one that guarantees the build reflects current Scala sources. Both
        # `clean-sim` and `clean-sim-debug` leave the jar and gen_dir in place
        # and can therefore link a simulator against stale Verilog.
        if self.clean_sim:
            clean_cmd = f"make clean-sim CONFIG={self.config} CONFIG_PACKAGE={self.config_package}"
            self.logger.info(f"Running clean-sim: {clean_cmd} in directory: {sims_dir}")
            try:
                clean_result = subprocess.run(clean_cmd, shell=True, cwd=sims_dir, capture_output=True, text=True, timeout=600)
                if clean_result.returncode != 0:
                    self.logger.warning(f"clean-sim returned non-zero exit code {clean_result.returncode}: {clean_result.stderr[:500]}")
            except subprocess.TimeoutExpired:
                self.logger.warning("clean-sim timed out after 600s, proceeding with build")
            except Exception as e:
                self.logger.warning(f"clean-sim failed: {e}, proceeding with build")
        if self.clean:
            clean_cmd = f"make clean"
            self.logger.info(f"Running clean: {clean_cmd} in directory: {sims_dir}")
            try:
                clean_result = subprocess.run(clean_cmd, shell=True, cwd=sims_dir, capture_output=True, text=True, timeout=600)
                if clean_result.returncode != 0:
                    self.logger.warning(f"clean returned non-zero exit code {clean_result.returncode}: {clean_result.stderr[:500]}")
            except subprocess.TimeoutExpired:
                self.logger.warning("clean timed out after 600s, proceeding with build")
            except Exception as e:
                self.logger.warning(f"clean failed: {e}, proceeding with build")

        profiler = get_profiler()
        profiler.add_info({
            "verilator_threads": self.extra_make_args.get("VERILATOR_THREADS", "1"),
            "config": self.config,
            "clean_sim": self.clean_sim,
            "clean": self.clean,
            "wf_scopes_count": len(self.wf_scopes),
            "wf_scopes": list(self.wf_scopes),
        })

        self.logger.info(f"Running build command: {cmd} in directory: {sims_dir}")
        try:
            result = subprocess.run(
                cmd,
                shell=True,
                cwd=sims_dir,
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
            )
        except subprocess.TimeoutExpired as e:
            self.logger.info(f"Build timed out after {self.timeout_seconds} seconds")
            stdout = e.stdout.decode(errors="replace") if e.stdout else ""
            stderr = e.stderr.decode(errors="replace") if e.stderr else ""
            return BuildArtifact(
                name=self.name,
                simulator_binary_content=b"",
                simulator_binary_name=binary_name,
                config=self.config,
                config_package=self.config_package,
                target=self.target,
                success=False,
                stdout=stdout,
                stderr=stderr + "\nBuild timed out",
                returncode=-1,
            )

        if result.returncode != 0:
            self.logger.info(f"Build failed stderr: {result.stderr[-500:]}")
            self.logger.info(f"Build failed stdout: {result.stdout[-500:]}")
            return BuildArtifact(
                name=self.name,
                simulator_binary_content=b"",
                simulator_binary_name=binary_name,
                config=self.config,
                config_package=self.config_package,
                target=self.target,
                success=False,
                stdout=result.stdout,
                stderr=result.stderr,
                returncode=result.returncode,
            )

        with open(binary_path, "rb") as f:
            binary_content = f.read()

        generated_src_files = []
        if self.collect_generated_src:
            generated_src_files = self._collect_generated_src(sims_dir)

        return BuildArtifact(
            name=self.name,
            simulator_binary_content=binary_content,
            simulator_binary_name=binary_name,
            config=self.config,
            config_package=self.config_package,
            target=self.target,
            success=True,
            stdout=result.stdout,
            stderr=result.stderr,
            returncode=result.returncode,
            generated_src_files=generated_src_files,
        )

    def _collect_generated_src(self, sims_dir: str) -> list[tuple[str, str]]:
        """Read all .v/.sv files from the generated-src directory.

        Also collects ``.top.mems.conf`` from the build directory (one level
        above gen-collateral) for SRAM macro remapping.
        """
        gen_src_parent = os.path.join(sims_dir, "generated-src")
        if os.path.isdir(gen_src_parent):
            self.logger.info(f"Contents of generated-src/: {os.listdir(gen_src_parent)}")
        else:
            self.logger.warning(f"generated-src dir itself not found: {gen_src_parent}")
        config_dir = os.path.join(
            gen_src_parent,
            f"{self.config_package}.harness.TestHarness.{self.config}",
        )
        gen_src_dir = os.path.join(config_dir, "gen-collateral")
        if not os.path.isdir(gen_src_dir):
            self.logger.warning(f"generated-src dir not found: {gen_src_dir}")
            return []
        files = []
        for root, _dirs, filenames in os.walk(gen_src_dir):
            for fname in filenames:
                if not fname.endswith((".v", ".sv")):
                    continue
                if fname == "TestDriver.v":
                    continue
                path = os.path.join(root, fname)
                with open(path, "r", errors="replace") as f:
                    contents = f.read()
                if 'import "DPI-C"' in contents:
                    continue
                files.append((fname, contents))
        self.logger.info(f"Collected {len(files)} generated src files from {gen_src_dir}")

        # Also collect .top.mems.conf for SRAM macro remapping
        if os.path.isdir(config_dir):
            for fname in os.listdir(config_dir):
                if fname.endswith(".top.mems.conf"):
                    path = os.path.join(config_dir, fname)
                    with open(path, "r") as f:
                        files.append((fname, f.read()))
                    self.logger.info(f"Collected mems conf: {fname}")

        return files

