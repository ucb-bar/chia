"""End-to-end test for FireSim FPGA bitstream build.

Runs the full build pipeline:
  Docker elaboration (replace-rtl + driver) → Vivado synthesis → AGFI creation

Requires:
  - AWS credentials with EC2 + S3 + FPGA image permissions
  - ghcr.io/ucb-bar/chia-chisel-build:latest Docker image accessible
  - Sufficient EC2 quota for z1d.2xlarge

Usage:
    python test/test_firesim_build_e2e.py

Environment variables:
    CHIA_S3_BUCKET       S3 bucket for build artifacts (default: firesim-chia-builds)
    CHIA_BUILD_RECIPE    Recipe name for logging
    CHIA_DOCKER_IMAGE    Docker image (default: ghcr.io/ucb-bar/chia-chisel-build:latest)
    CHIA_INSTANCE_TYPE   Build instance type (default: z1d.2xlarge)
    CHIA_SSH_KEY         Path to SSH private key (default: ~/.ssh/firesim)
"""

import os
import sys

from chia.aws.config import AWSConfig
from chia.cluster.log import setup_logging
from chia.firesim.build_node import BitstreamBuilder
from chia.firesim.config import FireSimBuildConfig


def make_rocket_config() -> FireSimBuildConfig:
    """FireSimRocket build recipe — small config for fast E2E testing."""
    return FireSimBuildConfig(
        name=os.environ.get("CHIA_BUILD_RECIPE", "FireSimRocket"),
        platform="f2",
        target_project="firesim",
        target_project_makefrag=None,
        design="FireSim",
        target_config="FireSimRocketConfig",
        platform_config="BaseF2Config",
        fpga_frequency=75,
        build_strategy="TIMING",
        bit_builder_recipe="",
    )


def main():
    setup_logging()
    s3_bucket = os.environ.get("CHIA_S3_BUCKET", "firesim-chia-builds")
    docker_image = os.environ.get(
        "CHIA_DOCKER_IMAGE", "ghcr.io/ucb-bar/chia-chisel-build:latest"
    )
    instance_type = os.environ.get("CHIA_INSTANCE_TYPE", "f2.6xlarge")
    ssh_key = os.environ.get("CHIA_SSH_KEY", os.path.expanduser("~/firesim.pem"))

    aws_config = AWSConfig(ssh_private_key=ssh_key, security_group_name="firesim", use_public_ip=True)
    
    build_config = make_rocket_config()

    print(f"=== E2E Test: FireSim bitstream build ({build_config.name}) ===")
    print(f"  Docker image: {docker_image}")
    print(f"  Instance type: {instance_type}")
    print(f"  S3 bucket: {s3_bucket}")

    builder = BitstreamBuilder(
        build_config=build_config,
        aws_config=aws_config,
        s3_bucket=s3_bucket,
        docker_image=docker_image,
        instance_type=instance_type,
    )

    result = builder.build()

    print(f"\n  success: {result.success}")
    print(f"  agfi: {result.agfi}")
    print(f"  afi: {result.afi}")
    print(f"  driver_s3_path: {result.driver_s3_path}")
    print(f"  build_log (last 1000 chars):\n{result.build_log[-1000:]}")

    if not result.success:
        print("\n  FAIL: build did not succeed")
        sys.exit(1)
    if not result.agfi or not result.agfi.startswith("agfi-"):
        print(f"\n  FAIL: unexpected AGFI value: {result.agfi}")
        sys.exit(1)
    if not result.driver_s3_path:
        print("\n  FAIL: driver_s3_path not set")
        sys.exit(1)

    print("\n  PASS")
    print(f"\n  HWDB entry:\n{result.hwdb_entry}")


if __name__ == "__main__":
    main()
