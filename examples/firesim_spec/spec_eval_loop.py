"""Run SPEC on a FireSim design with spec_eval.py, and print the SPEC ratios.

Run from examples/firesim_spec:
    RAY_ADDRESS=http://127.0.0.1:8265 chia job submit --working-dir . -- \\
        python spec_eval_loop.py spec06-int-test --cluster /path/to/cluster.yaml
"""

import argparse
import sys

import ray

from chia.aws.config import AWSConfig
from chia.aws.manager import start_aws_manager
from chia.base.ChiaFunction import get
from chia.cluster.config import load_config
from chia.firesim.fs_bitstream import FSBitstream
from chia.firesim.state_def import BuildRecipe, RunConfig
from spec_eval import spec_eval


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("spec", help="SPEC suite and input size, e.g. spec06-int-test or "
                                     "spec17-intspeed-train")
    parser.add_argument("--cluster", required=True, help="The cluster file of the running cluster")
    parser.add_argument("--recipe", default="megaboom_fcfs16",
                        help="A recipe of config_build_recipes.yaml")
    parser.add_argument("--agfi", help="Skip the bitstream build: an AGFI built from the recipe")
    parser.add_argument("--driver", help="The driver bundle (driver-bundle.tar.gz) of --agfi")
    args = parser.parse_args()

    ray.init(address="auto")
    cluster = load_config(args.cluster)
    aws = start_aws_manager(cluster, AWSConfig(
        region=cluster.aws_config.region,
        key_name=cluster.aws_config.key_name,
        ssh_user=cluster.aws_config.ssh_user,
        ssh_private_key=cluster.aws_config.ssh_private_key))

    recipe = BuildRecipe.from_yaml("config_build_recipes.yaml", args.recipe)
    bitstream = None
    if args.agfi:
        with open(args.driver, "rb") as f:
            bitstream = FSBitstream(recipe.quintuplet(), agfi=args.agfi, driver_bytes=f.read())

    result = get(spec_eval.chia_remote(aws, args.spec, recipe,
                                       RunConfig.from_yaml("config_runtime.yaml"),
                                       bitstream=bitstream))
    print(f"bitstream: {result.bitstream.agfi}")
    for benchmark, seconds in sorted(result.seconds.items()):
        print(f"{benchmark}: {seconds:.0f} s, ratio {result.ratios.get(benchmark)}")
    print(f"score: {result.score}")
    return 0 if result.score is not None else 1


if __name__ == "__main__":
    sys.exit(main())
