import hashlib
import os
import re
import shlex
import subprocess
import logging
from chia.chipyard.state_def import BuildArtifact, BuildTarget, SpikeBuildArtifact
from chia.chipyard.spike_build_node import STATIC_ARCHIVES, stage_golden_model
from chia.trace.profiler import get_profiler
from chia.base.ChiaFunction import ChiaFunction

# Maximum number of WF_SCOPES paths supported by the Makefile and TestDriver.v
# (the .v file has WF_SCOPE_1 ... WF_SCOPE_8 ifdef branches).
_MAX_WF_SCOPES = 8

# Libraries every image provides; everything else ldd reports is bundled.
# Mirrors the FireSim driver bundle filter (chia/firesim/build_node.py).
_SYSTEM_LIBS = re.compile(
    r"(libc\.so|libstdc\+\+|libm\.so|libpthread|libdl\.so|librt\.so|libgcc_s"
    r"|libz\.so|libzstd|libelf|ld-linux)")


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
        golden_model: SpikeBuildArtifact | None = None,
        static_golden_model: bool = False,
        bundle_runtime_libs: bool | None = None,
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
            golden_model: A :class:`SpikeBuildArtifact` to link against; staged
                into ``$RISCV`` before make, recorded as ``golden_model_digest``
                and, when the link is dynamic, bundled into ``runtime_libs``.
                ``None`` links whatever the image installed.
            static_golden_model: Compile libriscv into the simulator for one
                hermetic binary. Pass ``clean_sim=True`` with it, or make will
                not relink for a new library.
            bundle_runtime_libs: Carry non-system shared libraries in
                ``BuildArtifact.runtime_libs`` for the run node to stage.
                Defaults to True only for a dynamic ``golden_model`` build.
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
        self.golden_model = golden_model
        self.static_golden_model = static_golden_model
        self.bundle_runtime_libs = (
            bundle_runtime_libs if bundle_runtime_libs is not None
            else (golden_model is not None and not static_golden_model))
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

    def _riscv_path(self) -> str:
        """``$RISCV``: the toolchain prefix chipyard links libriscv from."""
        return os.environ.get("RISCV") or os.path.join(
            self.chipyard_path, ".conda-env", "riscv-tools")

    def _stage_golden_model(self) -> None:
        """Ensure ``$RISCV/lib`` holds the given model, so the link cannot
        silently use whatever libriscv the image shipped."""
        lib_dir = os.path.join(self._riscv_path(), "lib")
        installed = os.path.join(lib_dir, self.golden_model.lib_name)
        # A static link also needs the archives, not just the shared library.
        missing = [name for name in STATIC_ARCHIVES
                   if not os.path.exists(os.path.join(lib_dir, name))]
        archive_ok = not self.static_golden_model or not missing
        if os.path.exists(installed) and archive_ok:
            with open(installed, "rb") as handle:
                if hashlib.sha256(handle.read()).hexdigest() == self.golden_model.digest:
                    self.logger.debug(
                        f"golden model {self.golden_model.digest[:12]} already installed")
                    return
        if not archive_ok and not self.golden_model.static_archives:
            raise RuntimeError(
                f"static_golden_model needs {missing}, neither installed in "
                f"{lib_dir} nor carried by the golden model; build it with "
                "SpikeBuildNode(build_static=True), plus collect_static=True "
                "when the build runs in a different container")
        self.logger.info(f"Staging golden model {self.golden_model.digest[:12]} "
                         f"into {self._riscv_path()}")
        stage_golden_model(self.golden_model, self._riscv_path(), self.logger)

    def _effective_make_args(self) -> dict:
        """``extra_make_args`` plus what the golden-model options imply, all
        through make variables chipyard already exposes."""
        args = dict(self.extra_make_args)
        if self.static_golden_model:
            # Name the archives outright and in dependency order: -lriscv
            # resolves to the .so, and libriscv.a alone leaves ~900 undefined
            # softfloat symbols.
            lib_dir = os.path.join(self._riscv_path(), "lib")
            args["LRISCV"] = " ".join(os.path.join(lib_dir, name)
                                      for name in STATIC_ARCHIVES)
            existing_ld = args.get("EXTRA_SIM_LDFLAGS", "")
            args["EXTRA_SIM_LDFLAGS"] = (
                existing_ld + " -Wl,--allow-multiple-definition").strip()
            if not self.clean_sim and not self.clean:
                self.logger.warning(
                    "static_golden_model without clean_sim/clean: make has no "
                    "prerequisite on libriscv, so it will not relink and the "
                    "simulator may keep an older golden model compiled in")
        if self.bundle_runtime_libs:
            # new-dtags turns chipyard's baked-in rpath into DT_RUNPATH, which
            # LD_LIBRARY_PATH overrides - so the staged library wins over the
            # image's.
            existing = args.get("EXTRA_SIM_LDFLAGS", "")
            args["EXTRA_SIM_LDFLAGS"] = (
                f"{existing} -Wl,--enable-new-dtags".strip())
        return args

    def _collect_runtime_libs(self, binary_path: str) -> list[tuple[str, bytes]]:
        """The simulator's non-system shared libraries, by value (ldd, minus
        the libraries every image has)."""
        try:
            result = subprocess.run(["ldd", binary_path], capture_output=True,
                                    text=True, timeout=60)
            needed = self._parse_ldd(result.stdout)
        except Exception as error:                                  # noqa: BLE001
            self.logger.warning(f"ldd failed on {binary_path}: {error}")
            needed = []

        libs: list[tuple[str, bytes]] = []
        unresolved: list[str] = []
        for name, path in needed:
            if _SYSTEM_LIBS.search(name):
                continue
            # Carry the given model's own bytes, even when ldd resolved a
            # different file or none: the artifact must ship what its digest
            # names.
            if (self.golden_model is not None
                    and name == self.golden_model.lib_name):
                libs.append((name, self.golden_model.lib_content))
                continue
            if path and os.path.exists(path):
                with open(path, "rb") as handle:
                    libs.append((name, handle.read()))
            else:
                unresolved.append(name)

        if unresolved:
            # Not fatal here - the run image may well provide these - but it is
            # the one thing worth saying out loud, because the symptom on the
            # far side is the simulator dying on an unresolved symbol rather
            # than anything that names the missing library.
            self.logger.warning(
                f"could not bundle {unresolved}: not resolvable in this "
                "container, so the run worker's image must provide them")
        if self.golden_model is not None and not any(
                name == self.golden_model.lib_name for name, _ in libs):
            self.logger.warning(
                f"{self.golden_model.lib_name} is not among the simulator's "
                "dynamic dependencies; the golden model is linked statically "
                "or not linked at all")
        self.logger.info(f"Bundled {len(libs)} runtime libs: "
                         f"{[name for name, _ in libs]}")
        return libs

    @staticmethod
    def _parse_ldd(output: str) -> list[tuple[str, str | None]]:
        """``(soname, resolved path or None)`` per ldd line; unresolved entries
        are kept, they are the ones that matter."""
        needed: list[tuple[str, str | None]] = []
        for line in output.splitlines():
            if "=>" not in line:
                continue
            left, right = line.split("=>", 1)
            name = left.strip()
            if not name:
                continue
            path = right.strip().split(" (")[0].strip()
            needed.append((name, path if path and path != "not found" else None))
        return needed

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
        # Golden model first: staging and the link flags both have to be settled
        # before the make command line is assembled.
        if self.golden_model is not None:
            self._stage_golden_model()
        make_args = self._effective_make_args()

        if self.target == BuildTarget.VERILATOR:
            sims_dir = os.path.join(self.chipyard_path, "sims/verilator")
            binary_name = f"simulator-{self.config_package}.harness-{self.config}"
            binary_path = os.path.join(sims_dir, binary_name)
            extra_args = self._format_make_args(make_args)
            cmd = f"make -j {self.make_jobs} CONFIG={self.config} CONFIG_PACKAGE={self.config_package} {extra_args}".strip()

        elif self.target == BuildTarget.VERILATOR_DEBUG:
            sims_dir = os.path.join(self.chipyard_path, "sims/verilator")
            binary_name = f"simulator-{self.config_package}.harness-{self.config}-debug"
            binary_path = os.path.join(sims_dir, binary_name)
            extra_args = self._format_make_args(make_args)
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
            all_args = {**firesim_args, **make_args}
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
            "golden_model_digest": (self.golden_model.digest
                                    if self.golden_model is not None else ""),
            "static_golden_model": self.static_golden_model,
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

        runtime_libs = (self._collect_runtime_libs(binary_path)
                        if self.bundle_runtime_libs else [])

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
            runtime_libs=runtime_libs,
            golden_model_digest=(self.golden_model.digest
                                 if self.golden_model is not None else ""),
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

