"""Smoke test: an LLM edits chipyard, and an ECAD machine builds the bitstream.

    ChiselBuildNode worker (head)        ECAD worker (EC2, launched here)
    ────────────────────────────         ───────────────────────────────
    LLM makes a small RTL change
    git diff  ─────────── diff ───────>  git apply
                                         firesim buildbitstream
              <──────── FSBitstream ───  agfi + driver

Small on purpose: Rocket at 75 MHz, the cheapest real f2 build there is. Budget
1-3 hours for Vivado plus up to an hour for AWS to register the AGFI. Everything
before the ECAD launch is minutes, so run that part alone first with --diff-only.

Run (after `chia up cluster.local.yaml -y`; the cluster's dashboard is on 8285):
    RAY_ADDRESS=http://127.0.0.1:8285 chia job submit --working-dir . -- python ecad_build_loop.py --diff-only
    RAY_ADDRESS=http://127.0.0.1:8285 chia job submit --working-dir . -- python ecad_build_loop.py
"""

import argparse
import os
import sys

import ray

import chia.firesim

from chia.aws.config import AWSConfig
from chia.aws.manager import AWSManager
from chia.base.ChiaFunction import ChiaFunction, get
from chia.cluster.config import load_config
from chia.firesim.ecad_node import EcadBuildNode
from chia.firesim.specs import ECAD
from chia.firesim.state_def import BuildRecipe

CLUSTER_YAML = os.environ.get("ECAD_CLUSTER", "cluster.local.yaml")
CHIPYARD = "/home/ray/chipyard"

# One line, in a file every FireSim target elaborates, so the diff provably
# reaches the RTL without changing what the design does.
PROMPT = f"""In {CHIPYARD}, open
generators/chipyard/src/main/scala/config/AbstractConfig.scala
and add a single Scala comment line at the top of the file that reads:
// chia ecad smoke test
Change nothing else. Do not reformat. Do not touch any other file."""

RECIPE = BuildRecipe(name="rocket-smoke",
                     target_config="FireSimRocketConfig",
                     platform_config="BaseF2Config",
                     fpga_frequency=75)


@ChiaFunction(resources={"chipyard": 1})
def edit_and_diff(prompt: str, chipyard: str = CHIPYARD) -> str:
    """Let the LLM edit the chipyard checkout, then return the diff it made."""
    import subprocess

    from chia.models.claude import ClaudeCodeLLM

    llm = ClaudeCodeLLM(system_message="You edit RTL. Make the smallest change asked for.",
                        timeout_seconds=600)
    llm.prompt(prompt)
    return subprocess.run(["git", "-C", chipyard, "diff"],
                          capture_output=True, text=True).stdout


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--diff-only", action="store_true",
                        help="Stop after the diff; do not launch the ECAD machine")
    args = parser.parse_args()

    # TODO: drop once the chisel-build image ships a chia that has chia.firesim.ecad_node.
    ray.init(address="auto",
             runtime_env={"py_modules": [os.path.dirname(chia.firesim.__path__[0])]})

    diff = get(edit_and_diff.chia_remote(prompt=PROMPT))
    if not diff.strip():
        print("FAIL: the LLM produced no diff")
        return 1
    print(f"--- diff ({len(diff)} bytes) ---\n{diff}")
    if args.diff_only:
        return 0

    # One source of truth for the key: the cluster's aws block, not a literal
    # here, so the EC2 key pair and the ssh key cannot drift apart.
    cluster = load_config(CLUSTER_YAML)
    manager = AWSManager(cluster_config=cluster,
                         aws_config=AWSConfig(
                             region=cluster.aws_config.region,
                             key_name=cluster.aws_config.key_name,
                             vpc_name=cluster.aws_config.vpc_name,
                             security_group_name=cluster.aws_config.security_group_name,
                             ssh_user=cluster.aws_config.ssh_user,
                             ssh_private_key=cluster.aws_config.ssh_private_key,
                             subnet_id=cluster.aws_config.subnet_id,
                             use_public_ip=cluster.aws_config.use_public_ip))
    farm = manager.launch(ECAD, count=1)
    try:
        node = EcadBuildNode()
        result = get(node.build_bitstream.chia_remote(
            node, recipe=RECIPE, diff=diff))
    finally:
        manager.teardown(farm)

    if not result.success:
        print(f"FAIL: build failed\n{result.log[-4000:]}")
        return 1

    bitstream = result.bitstream
    print(f"PASS: {bitstream.quintuplet}")
    print(f"  agfi   {bitstream.agfi}")
    print(f"  driver {len(bitstream.driver_bytes) / 1e6:.1f} MB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
