"""Build all 6 MegaBoom bitstream variants in parallel via Ray.

Each recipe runs on its own z1d.2xlarge EC2 instance.

Usage:
    python test/test_firesim_build_megaboom_all.py
"""

import sys
import time
from dataclasses import asdict

import ray

from chia.aws.config import AWSConfig
from chia.base.ChiaFunction import get
from chia.cluster.log import setup_logging
from chia.firesim.chia_functions import firesim_build_bitstream
from chia.firesim.config import FireSimBuildConfig

# --- Hardcoded config ---
S3_BUCKET = "firesim-chia-builds"
SSH_KEY = "/home/ubuntu/firesim.pem"
DOCKER_IMAGE = "ghcr.io/ucb-bar/chia-chisel-build:latest"
INSTANCE_TYPE = "z1d.2xlarge"
MAKEFRAG = "../../../generators/firechip/chip/src/main/makefrag/firesim"

BUILD_CONFIGS = {
    "firesim_megaboom_chia": FireSimBuildConfig(
        name="firesim_megaboom_chia",
        platform="f2",
        target_project="firesim",
        target_project_makefrag=MAKEFRAG,
        design="FireSim",
        target_config="FireSimMegaBoomChiaConfig",
        platform_config="WithSynthAsserts_DefaultF2FRFCFS16GBQuadRankConfig",
        fpga_frequency=50,
        build_strategy="TIMING",
        bit_builder_recipe="bit-builder-recipes/f2.yaml",
    ),
}


def main():
    setup_logging()

    aws_config = AWSConfig(ssh_private_key=SSH_KEY)
    aws_config_dict = asdict(aws_config)

    print(f"=== Building {len(BUILD_CONFIGS)} MegaBoom variants in parallel ===")
    print(f"  Docker image:  {DOCKER_IMAGE}")
    print(f"  Instance type: {INSTANCE_TYPE}")
    print(f"  S3 bucket:     {S3_BUCKET}")
    for name in BUILD_CONFIGS:
        print(f"    - {name}")

    t0 = time.time()

    # Submit all builds in parallel
    pending = {}
    for name, config in BUILD_CONFIGS.items():
        print(f"  Submitting: {name}")
        ref = firesim_build_bitstream.chia_remote(
            build_recipe_name=name,
            aws_config_dict=aws_config_dict,
            s3_bucket=S3_BUCKET,
            docker_image=DOCKER_IMAGE,
            instance_type=INSTANCE_TYPE,
            build_config_dict=asdict(config),
        )
        pending[ref] = name

    # Collect results
    results = {}
    while pending:
        ready, _ = ray.wait(list(pending.keys()), num_returns=1)
        for ref in ready:
            name = pending.pop(ref)
            try:
                result = get(ref)
                results[name] = result
                status = "SUCCESS" if result.success else "FAILED"
                print(f"\n  {name}: {status}")
                print(f"    AGFI: {result.agfi}")
                print(f"    Driver: {result.driver_s3_path}")
            except Exception as e:
                print(f"\n  {name}: EXCEPTION: {e}")
                results[name] = None

    total_time = time.time() - t0

    # Summary
    print(f"\n{'='*70}")
    print(f"Build complete in {total_time:.0f}s")
    print(f"{'='*70}")

    succeeded = 0
    for name in BUILD_CONFIGS:
        r = results.get(name)
        if r and r.success:
            succeeded += 1
            print(f"  PASS  {name}: agfi={r.agfi}")
        elif r:
            print(f"  FAIL  {name}: {r.build_log[-200:]}")
        else:
            print(f"  ERR   {name}: no result")

    print(f"\n{succeeded}/{len(BUILD_CONFIGS)} succeeded")

    if succeeded < len(BUILD_CONFIGS):
        sys.exit(1)


if __name__ == "__main__":
    main()
