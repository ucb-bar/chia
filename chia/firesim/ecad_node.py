"""Build an f2 bitstream with FireSim's own build code.

Runs inside the chisel-build container on an ECAD instance (see
``chia.firesim.specs.ECAD``). The node applies the diff, writes the build
configs, and runs the steps of ``firesim buildbitstream`` through FireSim's own
functions (``_BUILD``). The CLI would ssh to ``localhost`` for Chisel, which
under ``--net=host`` is the instance rather than this container, so those two
steps run here directly; Vivado and the AGFI still go to the instance over ssh.
"""

from __future__ import annotations

import logging
import os
import subprocess

import yaml

from chia.base.ChiaFunction import ChiaFunction
from chia.firesim.fs_bitstream import FSBitstream
from chia.firesim.specs import ECAD_RESOURCE
from chia.firesim.state_def import BuildRecipe, EcadBuildResult

CHIPYARD = "/home/ray/chipyard"
FIRESIM = f"{CHIPYARD}/sims/firesim"
DEPLOY = f"{FIRESIM}/deploy"

# The `buildbitstream` task of deploy/firesim, minus its ssh to localhost.
# Runs from DEPLOY in chipyard's env, so FireSim's modules import as they do
# for the CLI.
_BUILD = r"""
import argparse, os, sys
sys.path.insert(0, os.getcwd())
from fabric.api import env, execute, local
import buildtools.bitbuilder as bitbuilder
from buildtools.buildconfigfile import BuildConfigFile

env.key_filename = os.path.expanduser("~/firesim.pem")   # as deploy/firesim sets
env.disable_known_hosts = True
env.timeout, env.connection_attempts = 100, 10

config = BuildConfigFile(argparse.Namespace(
    launchtime=None, forceterminate=True, buildconfigfile="config_build.yaml",
    buildrecipesconfigfile="config_build_recipes.yaml",
    hwdbconfigfile="config_hwdb.yaml"))

# Chisel and the driver: the CLI runs these over ssh to localhost; run them here.
remote_run = bitbuilder.run
bitbuilder.run = lambda cmd, **kw: local(cmd, shell="/bin/bash")
for build in config.builds_list:
    build.bitbuilder.replace_rtl()
    build.bitbuilder.build_driver()
bitbuilder.run = remote_run

# Vivado and the AGFI: on the build host, over ssh, exactly as the CLI does.
config.request_build_hosts()
config.wait_on_build_host_initializations()
done = execute(lambda: config.get_build_by_ip(env.host_string).bitbuilder.build_bitstream(),
               hosts=config.build_ip_set)
sys.exit(0 if all(done.values()) else 1)
"""


class BitstreamBuildNode:
    """Applies a chipyard diff and runs ``firesim buildbitstream``."""

    def __init__(self, timeout_seconds: int = 86400):
        """
        Args:
            timeout_seconds: Wall-clock limit for the whole build, AGFI included.
        """
        self.timeout_seconds = timeout_seconds
        self.logger = logging.getLogger("BitstreamBuildNode")

    @ChiaFunction(resources={ECAD_RESOURCE: 1})
    def build_bitstream(self, recipe: BuildRecipe, diff: str = "") -> EcadBuildResult:
        """Build ``recipe`` with ``diff`` applied; return its AGFI and driver."""
        log = []
        steps = [
            ("git apply", f"cd {CHIPYARD} && git apply -" if diff else "true", diff),
            # deploy/firesim imports fabric 1.x, which FireSim's conda lock omits.
            ("fabric", f"source {CHIPYARD}/env.sh && "
                       "(python -c 'import fabric.api' 2>/dev/null || "
                       "pip install -q 'Fabric3==1.14.post1')", ""),
            ("build", f"source {CHIPYARD}/env.sh && cd {DEPLOY} && python -", _BUILD),
        ]
        self._write_configs(recipe)
        for name, cmd, stdin in steps:
            rc, out = self._sh(cmd, stdin)
            log.append(f"=== {name} (rc={rc}) ===\n{out[-4000:]}")
            if rc != 0:
                return EcadBuildResult(recipe.name, success=False, log="\n".join(log))

        with open(f"{DEPLOY}/built-hwdb-entries/{recipe.name}") as f:
            agfi = yaml.safe_load(f)[recipe.name]["agfi"]
        driver_dir = f"{FIRESIM}/sim/output/{recipe.platform}/{recipe.quintuplet()}"
        driver = subprocess.run(["tar", "-czf", "-", "-C", driver_dir, "."],
                                capture_output=True, check=True).stdout
        return EcadBuildResult(
            recipe.name, success=True, log="\n".join(log),
            bitstream=FSBitstream(recipe.quintuplet(), agfi=agfi, driver_bytes=driver))

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
                    "default_build_dir": "/home/ubuntu/firesim-build",
                    # --net=host: localhost is the instance, where Vivado is.
                    "build_farm_hosts": ["ubuntu@localhost"],
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
        """Run in this container; never raises."""
        self.logger.info(f"$ {cmd[:200]}")
        try:
            p = subprocess.run(["bash", "-lc", cmd], input=stdin, text=True,
                               capture_output=True, timeout=self.timeout_seconds)
            return p.returncode, p.stdout + p.stderr
        except subprocess.TimeoutExpired:
            return -1, f"timeout after {self.timeout_seconds}s"
