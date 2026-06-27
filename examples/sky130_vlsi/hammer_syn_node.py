"""Sky130 + Cadence Genus hammer synthesis node
"""

import logging
import os
import re
import shutil
import sys
import tempfile

from chia.base.ChiaFunction import ChiaFunction
from chia.vlsi.hammer import HammerNode
from sky130_vlsi.state_def import SynthesisResult

logger = logging.getLogger(__name__)

# Directory containing bundled YAML configs shipped with this package
_PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))

# Custom Hammer CLI driver that replaces Genus's generate_reports step with
# one that requests 500 critical paths per view (default is 50).
_HAMMER_DRIVER = os.path.join(_PACKAGE_DIR, "hammer_driver.py")


def _hammer_python_interpreter() -> str:
    """Resolve the Python interpreter used by the installed ``hammer-vlsi``.

    The custom driver imports the ``hammer`` package, so it must run under the
    same interpreter that the ``hammer-vlsi`` console script uses. That path is
    encoded in the script's shebang.
    """
    hammer_vlsi_bin = shutil.which("hammer-vlsi")
    if hammer_vlsi_bin is not None:
        try:
            with open(hammer_vlsi_bin, "r") as f:
                shebang = f.readline().rstrip()
            if shebang.startswith("#!"):
                return shebang[2:].split()[0]
        except OSError:
            pass
    return sys.executable


def _populate_hvl_lef_cache(obj_dir: str, tech_yml: str) -> None:
    """Copy sky130_fd_sc_hvl LEFs into the tech cache expected by Hammer.

    The Sky130 Hammer plugin auto-generates cache entries for the HD standard
    cells but not for the HVL level-shifter.  The tech YAML references
    ``cache/fd_sc_hvl__lef/sky130_fd_sc_hvl__lsbufhv2lv_1.lef`` which Hammer
    resolves under ``<obj_dir>/tech-sky130-cache/``.  We extract the PDK base
    path from the tech YAML and copy the LEF into place if it's missing.
    """
    cache_dir = os.path.join(obj_dir, "tech-sky130-cache", "fd_sc_hvl__lef")
    dest = os.path.join(cache_dir, "sky130_fd_sc_hvl__lsbufhv2lv_1.lef")
    if os.path.isfile(dest):
        return

    # Find the PDK sky130A path from the tech YAML basepath
    try:
        with open(tech_yml, "r") as f:
            content = f.read()
        # Look for the resolved basepath value
        m = re.search(r'basepath:\s*"([^"]+)"', content)
        if not m:
            logger.warning("Could not find basepath in tech YAML; skipping HVL LEF cache")
            return
        basepath = m.group(1)
        src = os.path.join(
            basepath, "sky130_col", "open_pdks-2022.10", "share", "pdk", "sky130A",
            "libs.ref", "sky130_fd_sc_hvl", "lef", "sky130_fd_sc_hvl__lsbufhv2lv_1.lef",
        )
        if not os.path.isfile(src):
            logger.warning(f"HVL LEF source not found: {src}")
            return
        os.makedirs(cache_dir, exist_ok=True)
        shutil.copy2(src, dest)
        logger.info(f"Populated HVL LEF cache: {dest}")
    except Exception as e:
        logger.warning(f"Failed to populate HVL LEF cache: {e}")


def hammer_syn(
    tech_yml: str,
    tools_yml: str,
    design_yml: str,
    input_files: list[tuple[str, str]] | None = None,
    vlsi_top: str | None = "ChipTop",
    obj_dir: str = "build",
    extra_args: list[str] | None = None,
    timeout_seconds: int = 259200,
) -> SynthesisResult:
    """Run hammer-vlsi synthesis.

    Args:
        tech_yml: Path to technology YAML config.
        tools_yml: Path to tools YAML config.
        design_yml: Path to design YAML config.
        input_files: List of (filename, contents) tuples for SV/Verilog sources.
        vlsi_top: Top-level module name for synthesis.
        obj_dir: Build output directory.
        extra_args: Additional CLI args passed to hammer-vlsi.
        timeout_seconds: Subprocess timeout.
    """
    obj_dir = os.path.abspath(obj_dir)
    os.makedirs(obj_dir, exist_ok=True)

    # Pre-populate the HVL LEF cache that the Sky130 plugin doesn't generate.
    # tech-sky130.yml references cache/fd_sc_hvl__lef/ which Hammer resolves
    # inside obj_dir/tech-sky130-cache/.
    _populate_hvl_lef_cache(obj_dir, tech_yml)

    logger.info(f"hammer_syn called: input_files={'None' if input_files is None else len(input_files)}, vlsi_top={vlsi_top}")

    # Write input files to a staging directory and generate override YAML
    override_args = []
    if input_files or vlsi_top:
        override_yml = os.path.join(obj_dir, "inputs_override.yml")
        with open(override_yml, "w") as f:
            if input_files:
                src_dir = os.path.join(obj_dir, "input_src")
                os.makedirs(src_dir, exist_ok=True)
                f.write("synthesis.inputs:\n")
                f.write("  input_files:\n")
                for filename, contents in input_files:
                    if not filename.endswith((".v", ".sv")):
                        continue
                    src_path = os.path.join(src_dir, filename)
                    os.makedirs(os.path.dirname(src_path), exist_ok=True)
                    with open(src_path, "w") as sf:
                        sf.write(contents)
                    f.write(f'    - "{src_path}"\n')
            if vlsi_top:
                if not input_files:
                    f.write("synthesis.inputs:\n")
                f.write(f'  top_module: "{vlsi_top}"\n')
        logger.info(f"Override YAML at {override_yml}:")
        with open(override_yml, "r") as f:
            logger.info(f.read())
        override_args = ["-p", override_yml]

    # Run the hammer-vlsi 'syn' action through the canonical
    # chia.vlsi.hammer.HammerNode instead of a hand-rolled subprocess: the node
    # owns the command build, the start_new_session process group (tracked by
    # chia's pid_registry, so chia_cancel kills the whole Genus tree), the
    # timeout/SIGKILL, and stdout/stderr capture. Called in-process via the
    # static member (not .chia_remote) because we are already on the synthesis
    # worker that owns obj_dir — a local call requests no resources and does not
    # re-dispatch.
    #
    # HammerNode.run takes a single ``hammer_bin``, but our custom 500-critical-
    # path driver must run as ``<hammer-python> hammer_driver.py`` (it has no
    # shebang and imports the ``hammer`` package). Bridge that two-token command
    # with a tiny exec wrapper in obj_dir and pass it as hammer_bin.
    wrapper = os.path.join(obj_dir, "hammer_vlsi_driver.sh")
    with open(wrapper, "w") as f:
        f.write(f'#!/bin/sh\nexec "{_hammer_python_interpreter()}" '
                f'"{_HAMMER_DRIVER}" "$@"\n')
    os.chmod(wrapper, 0o755)

    # tech/tools/design (+ the inputs override, when present) go in as ordered
    # -p configs; extra_args (e.g. the CACTI SRAM-lib -p override added by
    # Sky130SynNode.syn) are appended after them so they win on conflicts.
    configs = [os.path.abspath(tech_yml), os.path.abspath(tools_yml),
               os.path.abspath(design_yml)]
    if override_args:                    # ["-p", override_yml] -> the path only
        configs.append(override_args[1])

    result = HammerNode.run(
        "syn",
        configs=configs,
        obj_dir=obj_dir,
        extra_args=extra_args,
        hammer_bin=wrapper,
        timeout_seconds=timeout_seconds,
    )

    if not result.success:
        logger.error(f"Synthesis failed (rc={result.returncode})")
        logger.debug(f"stderr tail: "
                     f"{result.stderr[-500:] if result.stderr else '(empty)'}")

    return SynthesisResult(
        success=result.success,
        stdout=result.stdout,
        stderr=result.stderr,
        returncode=result.returncode,
        reports=_collect_reports(obj_dir),
    )


def _resolve_tech_yaml(tech_src: str, basepath: str) -> str:
    """Read a tech YAML template and resolve all basepath references.

    Sets the ``technology.sky130.basepath`` value and also expands every
    ``${technology.sky130.basepath}`` reference to the absolute path so that
    hammer's Sky130 plugin can find files without relying on lazy substitution.
    """
    with open(tech_src, "r") as f:
        content = f.read()
    # Set the basepath value
    content = re.sub(
        r'(basepath:\s*)"[^"]*"',
        f'\\1"{basepath}"',
        content,
    )
    # Expand all ${technology.sky130.basepath} references to absolute paths
    content = content.replace("${technology.sky130.basepath}", basepath)
    return content


def _collect_reports(obj_dir: str) -> dict[str, str]:
    """Collect report files from obj_dir.

    Searches both the ``reports/`` directory (standard Hammer output) and
    ``syn-rundir/`` (Genus working directory) for text report files, logs,
    and JSON outputs that may contain area/timing data.
    """
    reports = {}

    # Standard Hammer reports directory
    reports_dir = os.path.join(obj_dir, "reports")
    if os.path.isdir(reports_dir):
        for root, _dirs, filenames in os.walk(reports_dir):
            for fname in filenames:
                path = os.path.join(root, fname)
                rel_path = os.path.relpath(path, reports_dir)
                with open(path, "r", errors="replace") as f:
                    reports[rel_path] = f.read()

    # Genus syn-rundir — collect logs, JSON, and report files
    syn_rundir = os.path.join(obj_dir, "syn-rundir")
    if os.path.isdir(syn_rundir):
        collect_extensions = (".log", ".json", ".rpt", ".txt", ".sdc", ".tcl")
        # Top-level files in syn-rundir
        for fname in os.listdir(syn_rundir):
            if not any(fname.endswith(ext) for ext in collect_extensions):
                continue
            path = os.path.join(syn_rundir, fname)
            if not os.path.isfile(path):
                continue
            rel_path = os.path.join("syn-rundir", fname)
            with open(path, "r", errors="replace") as f:
                reports[rel_path] = f.read()
        # Genus writes area/timing reports to syn-rundir/reports/ via
        # ``write_reports -directory reports`` (relative to its cwd)
        syn_reports_dir = os.path.join(syn_rundir, "reports")
        if os.path.isdir(syn_reports_dir):
            for root, _dirs, filenames in os.walk(syn_reports_dir):
                for fname in filenames:
                    path = os.path.join(root, fname)
                    rel_path = os.path.join(
                        "syn-rundir", "reports",
                        os.path.relpath(path, syn_reports_dir),
                    )
                    with open(path, "r", errors="replace") as f:
                        reports[rel_path] = f.read()

    if not reports:
        logger.warning(f"No reports found in {obj_dir}")
    else:
        logger.info(f"Collected {len(reports)} report files from {obj_dir}")
    return reports


class Sky130SynNode:
    """
    Reads bundled YAML templates from this package directory, creates working
    copies with technology.sky130.basepath set from sky130_col_path (an input,
    since it varies per machine), and runs hammer synthesis
    """

    def __init__(
        self,
        sky130_col_path: str,
        input_files: list[tuple[str, str]] | None = None,
        vlsi_top: str | None = None,
        obj_dir: str = "build",
        extra_args: list[str] | None = None,
        timeout_seconds: int = 259200,
        cacti_sram_libs: list[dict[str, str]] | None = None,
    ):
        self.sky130_col_path = sky130_col_path
        self.input_files = input_files
        self.vlsi_top = vlsi_top
        self.obj_dir = obj_dir
        self.extra_args = extra_args
        self.timeout_seconds = timeout_seconds
        self.cacti_sram_libs = cacti_sram_libs
        self._work_dir = None

    def _prepare_configs(self) -> tuple[str, str, str]:
        """Create working copies of bundled YAMLs with basepath filled in.

        Returns:
            (tech_yml, tools_yml, design_yml) paths in a temp directory.
        """
        self._work_dir = tempfile.mkdtemp(prefix="hammer_sky130_")
        basepath = os.path.dirname(self.sky130_col_path)

        # Tech YAML: resolve basepath and all ${technology.sky130.basepath} refs
        tech_content = _resolve_tech_yaml(
            os.path.join(_PACKAGE_DIR, "tech-sky130.yml"), basepath,
        )
        tech_dst = os.path.join(self._work_dir, "tech-sky130.yml")
        with open(tech_dst, "w") as f:
            f.write(tech_content)

        # Tools YAML: use chia tools config
        tools_dst = os.path.join(self._work_dir, "tools-chia.yml")
        shutil.copy2(os.path.join(_PACKAGE_DIR, "tools-chia.yml"), tools_dst)

        # Design YAML: copy from package
        design_dst = os.path.join(self._work_dir, "design.yml")
        shutil.copy2(os.path.join(_PACKAGE_DIR, "design.yml"), design_dst)

        return tech_dst, tools_dst, design_dst

    def _generate_cacti_libs_yaml(self) -> str | None:
        """Generate a Hammer YAML override registering CACTI-generated SRAM libs.

        Each SRAM has three corners (ff, ss, tt) with Liberty files written
        to local files on this node before being referenced in the YAML.
        """
        if not self.cacti_sram_libs:
            return None

        # Corner metadata matching Sky130 SRAM22 characterization
        corner_info = {
            "ff_n40C_1v95": {"nmos": "fast", "pmos": "fast", "temperature": "-40 C",
                             "VDD": "1.95 V"},
            "ss_100C_1v60": {"nmos": "slow", "pmos": "slow", "temperature": "100 C",
                             "VDD": "1.60 V"},
            "tt_025C_1v80": {"nmos": "typical", "pmos": "typical", "temperature": "25 C",
                             "VDD": "1.80 V"},
        }

        lib_dir = os.path.join(self._work_dir, "cacti_libs")
        os.makedirs(lib_dir, exist_ok=True)

        lines = [
            'vlsi.technology.extra_libraries_meta: ["append", "lazydeepsubst"]',
            "vlsi.technology.extra_libraries:",
        ]
        for lp in self.cacti_sram_libs:
            lib_contents = lp.get("lib_contents", {})
            # Backwards compat: if old single-corner format, wrap it
            if "lib_content" in lp and not lib_contents:
                lib_contents = {"tt_025C_1v80": lp["lib_content"]}

            # Write LEF once per SRAM (geometry is not corner-dependent)
            lef_path = None
            lef_content = lp.get("lef_content")
            if lef_content:
                lef_path = os.path.join(lib_dir, f"cacti_{lp['name']}.lef")
                with open(lef_path, "w") as f:
                    f.write(lef_content)

            for corner_suffix, lib_content in lib_contents.items():
                info = corner_info.get(corner_suffix, corner_info["tt_025C_1v80"])
                lib_path = os.path.join(lib_dir, f"{lp['name']}_{corner_suffix}.lib")
                with open(lib_path, "w") as f:
                    f.write(lib_content)
                lines.append("  - library:")
                lines.append(f'      nldm_liberty_file: "{lib_path}"')
                if lef_path:
                    lines.append(f'      lef_file: "{lef_path}"')
                lines.append("      corner:")
                lines.append(f'        nmos: "{info["nmos"]}"')
                lines.append(f'        pmos: "{info["pmos"]}"')
                lines.append(f'        temperature: "{info["temperature"]}"')
                lines.append("      supplies:")
                lines.append(f'        VDD: "{info["VDD"]}"')
                lines.append('        GND: "0 V"')

        yaml_path = os.path.join(self._work_dir, "cacti_sram_libs.yml")
        with open(yaml_path, "w") as f:
            f.write("\n".join(lines) + "\n")
        logger.info(f"Generated CACTI SRAM libs YAML with {len(self.cacti_sram_libs)} "
                     f"macros (3 corners each) at {yaml_path}")
        return yaml_path

    def _cleanup(self) -> None:
        """Remove the temporary config directory created by _prepare_configs."""
        if self._work_dir and os.path.isdir(self._work_dir):
            try:
                shutil.rmtree(self._work_dir)
                logger.info(f"Cleaned up config dir {self._work_dir}")
            except Exception as e:
                logger.warning(f"Failed to clean up config dir {self._work_dir}: {e}")

    @ChiaFunction(resources={"VLSI": 1, "Syn": 0})
    def syn(self) -> SynthesisResult:
        try:
            tech_yml, tools_yml, design_yml = self._prepare_configs()
            logger.info(f"Prepared Syn Sky130 configs in {self._work_dir}")

            extra_args = list(self.extra_args) if self.extra_args else []
            cacti_yml = self._generate_cacti_libs_yaml()
            if cacti_yml:
                extra_args.extend(["-p", cacti_yml])
                logger.info(f"Added CACTI SRAM libs override: {cacti_yml}")

            return hammer_syn(
                tech_yml=tech_yml,
                tools_yml=tools_yml,
                design_yml=design_yml,
                input_files=self.input_files,
                vlsi_top=self.vlsi_top,
                obj_dir=self.obj_dir,
                extra_args=extra_args or None,
                timeout_seconds=self.timeout_seconds,
            )
        finally:
            self._cleanup()
