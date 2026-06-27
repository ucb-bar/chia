"""Ray-scheduled E2E test for FireSim FPGA bitstream build.

Same build pipeline as test_firesim_build_e2e.py, but dispatched through
Ray via the @ChiaFunction-decorated firesim_build_bitstream().  This
validates that:
  1. The function serializes cleanly across Ray object boundaries
  2. The firesim_manager resource scheduling works
  3. The BitstreamBuildResult returns correctly through ray.get()

Requires:
  - A running Ray cluster (ray start --head, or connect via RAY_ADDRESS)
  - The head node must advertise a ``firesim_manager`` custom resource:
        ray start --head --resources='{"firesim_manager": 1}'
  - AWS credentials with EC2 + S3 + FPGA image permissions
  - ghcr.io/ucb-bar/chia-firesim-build:latest accessible (or override)

Usage:
    # Start Ray head with firesim_manager resource, then:
    python test/test_firesim_build_ray.py

    # Or submit as a Ray job:
    ray job submit --working-dir . -- python test/test_firesim_build_ray.py

Environment variables:
    RAY_ADDRESS          Ray cluster address (default: auto)
    CHIA_S3_BUCKET       S3 bucket for build artifacts (default: firesim-chia-builds)
    CHIA_BUILD_RECIPE    Recipe name (default: FireSimRocket)
    CHIA_DOCKER_IMAGE    Docker image (default: ghcr.io/ucb-bar/chia-firesim-build:latest)
    CHIA_INSTANCE_TYPE   Build instance type (default: z1d.2xlarge)
    CHIA_SSH_KEY         Path to SSH private key (default: ~/firesim.pem)
"""

import dataclasses
import os
import sys

import ray

from chia.firesim.chia_functions import firesim_build_bitstream
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
    ray_address = os.environ.get("RAY_ADDRESS", "auto")
    s3_bucket = os.environ.get("CHIA_S3_BUCKET", "firesim-chia-builds")
    docker_image = os.environ.get(
        "CHIA_DOCKER_IMAGE", "ghcr.io/ucb-bar/chia-chisel-build:latest"
    )
    instance_type = os.environ.get("CHIA_INSTANCE_TYPE", "z1d.2xlarge")
    ssh_key = os.environ.get("CHIA_SSH_KEY", os.path.expanduser("~/firesim.pem"))

    build_config = make_rocket_config()

    # Serialize configs as plain dicts for Ray
    aws_config_dict = {
        "ssh_private_key": ssh_key,
    }
    build_config_dict = dataclasses.asdict(build_config)

    print(f"=== Ray Test: FireSim bitstream build ({build_config.name}) ===")
    print(f"  Ray address: {ray_address}")
    print(f"  Docker image: {docker_image}")
    print(f"  Instance type: {instance_type}")
    print(f"  S3 bucket: {s3_bucket}")

    # Connect to Ray cluster
    ray.init(address=ray_address)

    # Check that firesim_manager resource is available
    resources = ray.cluster_resources()
    if "firesim_manager" not in resources:
        print("\n  FAIL: No 'firesim_manager' resource in cluster.")
        print("  Start Ray with: ray start --head --resources='{\"firesim_manager\": 1}'")
        sys.exit(1)
    print(f"  firesim_manager resource: {resources['firesim_manager']}")

    # Dispatch via @ChiaFunction (schedules on firesim_manager node)
    print("\n  Submitting build via firesim_build_bitstream.chia_remote()...")
    ref = firesim_build_bitstream.chia_remote(
        build_recipe_name=build_config.name,
        aws_config_dict=aws_config_dict,
        s3_bucket=s3_bucket,
        docker_image=docker_image,
        instance_type=instance_type,
        build_config_dict=build_config_dict,
    )

    print("  Waiting for result (this may take 1-3 hours)...")
    result = ray.get(ref)

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
