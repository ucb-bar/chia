"""End-to-end test for FireSim workload suite run.

Runs one or more benchmarks from an S3-hosted workload suite through the
SuiteRunner, which fans out independent SimulationRunner runs via Ray.

Requires:
  - AWS credentials with EC2 + S3 permissions
  - Sufficient EC2 quota for F2 instances

Usage:
    python test/test_firesim_suite_e2e.py
"""

import sys

from chia.aws.config import AWSConfig
from chia.cluster.log import setup_logging
from chia.firesim.config import FireSimRunConfig
from chia.firesim.suite_runner import SuiteRunner
from chia.firesim.workloads import load_manifest

# --- Hardcoded test config ---
S3_BUCKET = "firesim-chia-builds"
BUILD_REF = "latest"  # derives agfi + driver_s3_path from build-info.json
SSH_KEY = "~/firesim.pem"
SUITE_NAME = "spec17-intspeed-test"
WORKLOAD_NAMES = ["602.gcc_s"]
PARALLELISM = 1
INSTANCE_TYPE = "f2.6xlarge"
SIM_TIMEOUT = 7200


def main():
    setup_logging()

    aws_config = AWSConfig(ssh_private_key=SSH_KEY, security_group_name="firesim", use_public_ip=True)
    manifest = load_manifest(S3_BUCKET, f"workloads/{SUITE_NAME}")

    base_config = FireSimRunConfig(
        hw_config_name=SUITE_NAME,
        build_ref=BUILD_REF,
        instance_type=INSTANCE_TYPE,
        market="spot",
    )

    print(f"=== E2E Test: FireSim suite run ===")
    print(f"  Build ref:   {BUILD_REF}")
    print(f"  Suite:       {SUITE_NAME}")
    print(f"  Workloads:   {WORKLOAD_NAMES}")
    print(f"  Parallelism: {PARALLELISM}")
    print(f"  Timeout:     {SIM_TIMEOUT}s")

    runner = SuiteRunner(
        manifest=manifest,
        workload_names=WORKLOAD_NAMES,
        base_run_config=base_config,
        aws_config=aws_config,
        s3_bucket=S3_BUCKET,
        parallelism=PARALLELISM,
        sim_timeout=SIM_TIMEOUT,
    )

    result = runner.run()

    print(f"\n{'='*60}")
    print(f"Suite: {result.suite_name}")
    print(f"Total time: {result.total_duration_seconds:.1f}s")
    print(f"Overall: {'PASS' if result.all_success else 'FAIL'}")
    print(f"{'='*60}")

    for name, res in sorted(result.workload_results.items()):
        status = "PASS" if res.success else "FAIL"
        print(f"\n  {name}: {status} ({res.duration_seconds:.1f}s)")
        for log_name, log in res.uartlogs.items():
            preview = log[:2000] if log else "(empty)"
            print(f"    --- {log_name} ---")
            print(f"    {preview}")
        if res.rootfs_outputs:
            for slot, outputs in res.rootfs_outputs.items():
                print(f"    --- rootfs outputs ({slot}) ---")
                for relpath, content in sorted(outputs.items()):
                    print(f"    {relpath} ({len(content)} bytes)")
                    if relpath.endswith(".csv"):
                        print(f"      {content.strip()}")

    if result.scores:
        from chia.firesim.spec_parser import compute_aggregate_score
        print(f"\n--- Scores ---")
        for name, sc in sorted(result.scores.items()):
            print(f"  {name}: {sc['score']:.3f} (RealTime={sc['RealTime']:.1f}s)")
        agg = compute_aggregate_score(result.scores)
        print(f"  Aggregate (geomean): {agg:.3f}")

    if not result.all_success:
        sys.exit(1)


if __name__ == "__main__":
    main()
