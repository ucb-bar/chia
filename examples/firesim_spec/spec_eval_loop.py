"""Evaluate a FireSim design on SPEC with spec_eval.py, next to this script.

    spec_eval(aws, spec, recipe, run_config) -> bitstream, workload, jobs, ratios, score

The recipe's bitstream builds on an F2_ECAD machine while SPEC compiles into a
FireMarshal workload; then each benchmark runs on its own F2 FPGA, and the loop
prints SPEC's ratios. Other loops call spec_eval the same way, and pass the
bitstream or the workload of an earlier call (or from S3) to skip their builds;
--agfi and --driver do this for the bitstream.

The recipe and the run settings are FireSim's own YAML files next to this script.
spec_eval.py lists what the cluster needs.

Run from examples/firesim_spec:
    RAY_ADDRESS=http://127.0.0.1:8265 chia job submit --working-dir . -- \\
        python spec_eval_loop.py spec06-int-test --cluster /path/to/cluster.yaml
"""

import argparse
import os
import sys

import ray

import chia.firesim
from chia.aws.config import AWSConfig
from chia.aws.manager import start_aws_manager
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

    # TODO: drop once the images ship a chia with the FireSim build and run nodes.
    ray.init(address="auto",
             runtime_env={"py_modules": [os.path.dirname(chia.firesim.__path__[0])]})
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

    result = spec_eval(aws, args.spec, recipe, RunConfig.from_yaml("config_runtime.yaml"),
                       bitstream=bitstream)
    print(f"bitstream: {result.bitstream.agfi}")
    for benchmark, seconds in sorted(result.seconds.items()):
        print(f"{benchmark}: {seconds:.0f} s, ratio {result.ratios.get(benchmark)}")
    print(f"score: {result.score}")
    return 0 if result.score is not None else 1


if __name__ == "__main__":
    sys.exit(main())
