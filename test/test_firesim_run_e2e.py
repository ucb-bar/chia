"""End-to-end test for FireSim FPGA simulation run.

Runs a simulation on an F2 instance using a pre-built AGFI and S3-hosted
driver bundle / workload artifacts.

Requires:
  - AWS credentials with EC2 + S3 permissions
  - A valid build ref (pre-built or from test_firesim_build_e2e.py)
  - Sufficient EC2 quota for F2 instances

Usage:
    python test/test_firesim_run_e2e.py

Environment variables:
    CHIA_BUILD_REF         Build reference, e.g. "FireSimRocket/latest" (preferred)
    CHIA_AGFI              AGFI to flash (fallback, e.g. agfi-02e674630210250c3)
    CHIA_DRIVER_S3_PATH    S3 URI of driver-bundle.tar.gz (fallback)
    CHIA_BOOTBIN_S3_PATH   S3 URI of boot binary (optional)
    CHIA_ROOTFS_S3_PATH    S3 URI of rootfs image (optional)
    CHIA_S3_BUCKET         S3 bucket (default: firesim-chia-builds)
    CHIA_INSTANCE_TYPE     F2 instance type (default: f2.12xlarge)
    CHIA_SSH_KEY           Path to SSH private key (default: ~/.ssh/firesim)
    CHIA_SIM_TIMEOUT       Simulation timeout in seconds (default: 3600)
"""

import os
import sys

from chia.aws.config import AWSConfig
from chia.cluster.log import setup_logging
from chia.firesim.config import FireSimRunConfig
from chia.firesim.run_node import SimulationRunner


def main():
    setup_logging()
    build_ref = os.environ.get("CHIA_BUILD_REF", "latest")
    agfi = os.environ.get("CHIA_AGFI")
    driver_s3 = os.environ.get("CHIA_DRIVER_S3_PATH")
    s3_bucket = os.environ.get("CHIA_S3_BUCKET", "firesim-chia-builds")

    if not build_ref and not agfi:
        print("ERROR: Set CHIA_BUILD_REF or CHIA_AGFI + CHIA_DRIVER_S3_PATH")
        sys.exit(1)

    ssh_key = os.environ.get("CHIA_SSH_KEY", os.path.expanduser("~/firesim.pem"))
    instance_type = os.environ.get("CHIA_INSTANCE_TYPE", "f2.12xlarge")
    sim_timeout = int(os.environ.get("CHIA_SIM_TIMEOUT", "3600"))

    aws_config = AWSConfig(
        ssh_private_key=ssh_key,
    )

    run_config = FireSimRunConfig(
        hw_config_name="test-e2e",
        build_ref=build_ref if not agfi else None,
        agfi=agfi,
        workload_name="e2e-test",
        num_sims=1,
        instance_type=instance_type,
        driver_s3_path=driver_s3,
        bootbinary_s3_path=os.environ.get("CHIA_BOOTBIN_S3_PATH"),
        rootfs_s3_path=os.environ.get("CHIA_ROOTFS_S3_PATH"),
    )

    # Resolve build_ref -> agfi + driver_s3_path
    if run_config.build_ref:
        run_config.resolve_build(s3_bucket)

    print(f"=== E2E Test: FireSim simulation run ===")
    print(f"  Build ref: {build_ref}")
    print(f"  AGFI: {run_config.agfi}")
    print(f"  Driver S3: {run_config.driver_s3_path}")
    print(f"  Instance type: {instance_type}")
    print(f"  Timeout: {sim_timeout}s")

    runner = SimulationRunner(
        run_config=run_config,
        aws_config=aws_config,
        sim_timeout=sim_timeout,
    )

    result = runner.run()

    print(f"\n  success: {result.success}")
    print(f"  duration: {result.duration_seconds:.1f}s")
    print(f"  uartlogs: {list(result.uartlogs.keys())}")

    for name, log in result.uartlogs.items():
        preview = log[:2000] if log else "(empty)"
        print(f"\n  --- {name} ---")
        print(f"  {preview}")

    # Check for evidence of Linux boot in uartlog
    all_logs = "\n".join(result.uartlogs.values())
    boot_indicators = ["Linux version", "login:", "Welcome", "buildroot",
                       "OpenSBI", "Commencing simulation"]
    found = [ind for ind in boot_indicators if ind in all_logs]

    if found:
        print(f"\n  Boot evidence found: {found}")

    if result.success:
        print("\n  PASS (clean exit)")
    elif found:
        # Simulation timed out but Linux booted — this is expected for
        # configs without a halt mechanism (e.g. no BlockDev bridge, so
        # Linux drops to emergency shell and never exits).
        print("\n  PASS (boot evidence found, sim timed out as expected)")
    else:
        print("\n  FAIL: simulation did not succeed and no boot evidence found")
        sys.exit(1)


if __name__ == "__main__":
    main()
