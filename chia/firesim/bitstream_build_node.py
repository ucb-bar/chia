"""Build a bitstream with FireSim's own build code."""

from __future__ import annotations

import logging
import os
import subprocess
import threading

import yaml

from chia.base.ChiaFunction import ChiaFunction
from chia.firesim.fs_bitstream import DRIVER_TAR_NAME, FSBitstream
from chia.firesim.state_def import BuildRecipe, EcadBuildResult

CHIPYARD = "/home/ray/chipyard"
FIRESIM = f"{CHIPYARD}/sims/firesim"
DEPLOY = f"{FIRESIM}/deploy"
BUILD_DIR = "/home/ray/firesim-build"


class BitstreamBuildNode:
    """Applies a chipyard diff and runs ``firesim buildbitstream``."""

    def __init__(self, timeout_seconds: int = 86400):
        """
        Args:
            timeout_seconds: Wall-clock limit for the whole build, AGFI included.
        """
        self.timeout_seconds = timeout_seconds
        self.logger = logging.getLogger("BitstreamBuildNode")

    @ChiaFunction(resources={"VIVADO": 1})
    def build_bitstream(self, recipe: BuildRecipe,
                        diffs: "list[str] | None" = None) -> EcadBuildResult:
        """Builds ``recipe`` with ``diffs`` applied to chipyard, in order, on a machine with
        the ``VIVADO`` resource. To build on other machines, call it with their resource,
        for example ``build_bitstream.options(resources={"F2_VIVADO": 1})``."""
        log = []
        out = f"{FIRESIM}/sim/output/{recipe.platform}/{recipe.quintuplet()}"
        bundle = f"{out}/{DRIVER_TAR_NAME}"
        steps = [
            ("git reset", f"cd {CHIPYARD} && git reset --hard HEAD && git clean -fd"
                          if diffs else "true", ""),
            *((f"git apply {i}", f"cd {CHIPYARD} && git apply -", diff)
              for i, diff in enumerate(diffs or [], 1)),
            ("build", f"source {CHIPYARD}/env.sh && cd {FIRESIM} && "
                      f"source sourceme-manager.sh --skip-ssh-setup && "
                      f"JAVA_HEAP_SIZE={recipe.java_heap_size} firesim buildbitstream", ""),
            # The driver and the libraries it loads from the conda env, which the run
            # host lacks. Not FireSim's get_local_shared_libraries: in this image it
            # also takes glibc, which crashes on the host.
            ("driver bundle", f"source {CHIPYARD}/env.sh && cd {out} && rm -rf bundle && "
                              f"mkdir bundle && cp {recipe.design}-{recipe.platform} bundle && "
                              f"ldd {recipe.design}-{recipe.platform} | awk -v p=$CONDA_PREFIX/ "
                              f"'index($3, p) == 1 {{print $3, $1}}' | "
                              f"while read lib name; do cp -L $lib bundle/$name; done && "
                              f"cd bundle && tar -czf {bundle} *", ""),
        ]
        self._write_configs(recipe)
        for name, cmd, stdin in steps:
            print(f"[build] {name}", flush=True)
            rc, out = self._sh(cmd, stdin)
            log.append(f"=== {name} (rc={rc}) ===\n{out[-4000:]}")
            if rc != 0:
                return EcadBuildResult(recipe.name, success=False, log="\n".join(log))

        with open(f"{DEPLOY}/built-hwdb-entries/{recipe.name}") as f:
            entry = yaml.safe_load(f)[recipe.name]
        with open(bundle, "rb") as f:
            driver = f.read()
        # F2 builds an AGFI; the other platforms build a bitstream tar.
        tar = None
        if "bitstream_tar" in entry:
            with open(entry["bitstream_tar"].removeprefix("file://"), "rb") as f:
                tar = f.read()
        return EcadBuildResult(
            recipe.name, success=True, log="\n".join(log),
            bitstream=FSBitstream(recipe.quintuplet(), agfi=entry.get("agfi"),
                                  bitstream_bytes=tar, driver_bytes=driver))

    @staticmethod
    def _write_configs(recipe: BuildRecipe) -> None:
        """The files FireSim's build code reads; everything else is FireSim's."""
        stale = f"{DEPLOY}/built-hwdb-entries/{recipe.name}"
        if os.path.exists(stale):
            os.remove(stale)
        build = {
            "build_farm": {
                "base_recipe": "build-farm-recipes/externally_provisioned.yaml",
                "recipe_arg_overrides": {
                    "default_build_dir": BUILD_DIR,
                    "build_farm_hosts": ["localhost"],
                },
            },
            "builds_to_run": [recipe.name],
            "agfis_to_share": [],
            "share_with_accounts": {},
        }
        recipes = {recipe.name: {
            "PLATFORM": recipe.platform,
            "TARGET_PROJECT": recipe.target_project,
            "TARGET_PROJECT_MAKEFRAG":
                f"{CHIPYARD}/generators/firechip/chip/src/main/makefrag/firesim",
            "DESIGN": recipe.design,
            "TARGET_CONFIG": recipe.target_config,
            "PLATFORM_CONFIG": recipe.platform_config,
            "deploy_quintuplet": None,
            "platform_config_args": {"fpga_frequency": recipe.fpga_frequency,
                                     "build_strategy": recipe.build_strategy},
            "post_build_hook": None,
            "metasim_customruntimeconfig": None,
            "bit_builder_recipe": f"bit-builder-recipes/{recipe.platform}.yaml",
        }}
        # BuildConfigFile also opens the hwdb, and fails on an empty file.
        for name, config in (("config_build.yaml", build),
                             ("config_build_recipes.yaml", recipes),
                             ("config_hwdb.yaml", {})):
            with open(f"{DEPLOY}/{name}", "w") as f:
                yaml.safe_dump(config, f, sort_keys=False)

    def _sh(self, cmd: str, stdin: str = "") -> tuple[int, str]:
        """Run in this container and return its output. Never raises."""
        self.logger.info(f"$ {cmd[:200]}")
        p = subprocess.Popen(["bash", "-lc", cmd], stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        timer = threading.Timer(self.timeout_seconds, p.kill)
        timer.start()
        out, _ = p.communicate(stdin)
        timer.cancel()
        return p.returncode, out
