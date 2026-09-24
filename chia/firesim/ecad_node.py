"""Build an f2 bitstream by running FireSim's own ``firesim buildbitstream``.

Runs inside the chisel-build container on an ECAD instance (see
``chia.firesim.specs.ECAD``). FireSim does all the work — Chisel on
``localhost`` (this container), Vivado on the build-farm host (the instance,
at 127.0.0.2), then ``create-fpga-image``. This node only applies the diff,
writes the two build configs, and reads the results back.
"""

from __future__ import annotations

import logging
import os
import subprocess

import yaml

from chia.base.ChiaFunction import ChiaFunction
from chia.firesim.fs_bitstream import FSBitstream
from chia.firesim.specs import ECAD_RESOURCE, HOST_LOOPBACK
from chia.firesim.state_def import BuildRecipe, EcadBuildResult

CHIPYARD = "/home/ray/chipyard"
FIRESIM = f"{CHIPYARD}/sims/firesim"
DEPLOY = f"{FIRESIM}/deploy"


class EcadBuildNode:
    """Applies a chipyard diff and runs ``firesim buildbitstream``."""

    def __init__(self, timeout_seconds: int = 86400):
        """
        Args:
            timeout_seconds: Wall-clock limit for the whole build, AGFI included.
        """
        self.timeout_seconds = timeout_seconds
        self.logger = logging.getLogger("EcadBuildNode")

    @ChiaFunction(resources={ECAD_RESOURCE: 1})
    def build_bitstream(self, recipe: BuildRecipe, diff: str = "") -> EcadBuildResult:
        """Build ``recipe`` with ``diff`` applied; return its AGFI and driver."""
        log = []
        steps = [
            ("git apply", f"cd {CHIPYARD} && git apply -" if diff else "true", diff),
            # FireSim's `localhost`: an sshd in this container on 127.0.0.1
            # (the host's gave that address up at boot), trusting the key
            # AWSManager generated and authorized on the host.
            ("sshd", "test -x /usr/sbin/sshd || "
                     "(sudo apt-get update -qq && sudo apt-get install -y -qq openssh-server); "
                     "echo 'ListenAddress 127.0.0.1' | sudo tee /etc/ssh/sshd_config.d/chia.conf >/dev/null; "
                     "sudo mkdir -p /run/sshd; "
                     "pgrep -x sshd >/dev/null || sudo /usr/sbin/sshd; "
                     "mkdir -p ~/.ssh && chmod 700 ~/.ssh && "
                     "grep -qxFf ~/firesim.pem.pub ~/.ssh/authorized_keys 2>/dev/null || "
                     "cat ~/firesim.pem.pub >> ~/.ssh/authorized_keys; "
                     "chmod 600 ~/.ssh/authorized_keys", ""),
            # deploy/firesim imports fabric 1.x, which FireSim's conda lock omits.
            ("fabric", f"source {CHIPYARD}/env.sh && "
                       "(python -c 'import fabric.api' 2>/dev/null || "
                       "pip install -q 'Fabric3==1.14.post1')", ""),
            ("buildbitstream", f"source {CHIPYARD}/env.sh && cd {FIRESIM} && "
                               f"source sourceme-manager.sh && cd deploy && "
                               f"./firesim buildbitstream", ""),
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
        """The two files ``buildbitstream`` reads; everything else is FireSim's."""
        stale = f"{DEPLOY}/built-hwdb-entries/{recipe.name}"
        if os.path.exists(stale):
            os.remove(stale)
        build = {
            "build_farm": {
                "base_recipe": "build-farm-recipes/externally_provisioned.yaml",
                "recipe_arg_overrides": {
                    "default_build_dir": "/home/ubuntu/firesim-build",
                    "build_farm_hosts": [f"ubuntu@{HOST_LOOPBACK}"],
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
        for name, config in (("config_build.yaml", build),
                             ("config_build_recipes.yaml", recipes)):
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
