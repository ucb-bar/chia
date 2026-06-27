"""CLI subcommand implementations for standalone FireSim operations."""

from __future__ import annotations

import signal
import sys

import yaml

from chia.aws.config import AWSConfig
from chia.cluster.log import get_logger, setup_logging

logger = get_logger("cli.firesim")

# Track running instance IDs for SIGINT cleanup
_active_instance_ids: list[str] = []
_aws_region: str = "us-east-1"


def _sigint_handler(signum, frame):
    """Terminate any running EC2 instances on Ctrl+C."""
    if _active_instance_ids:
        logger.warning(f"SIGINT received — terminating {len(_active_instance_ids)} instances")
        from chia.aws.ec2 import terminate_ec2_instances
        terminate_ec2_instances(_active_instance_ids, region=_aws_region, wait=False)
    sys.exit(1)


def _load_aws_config(raw: dict) -> AWSConfig:
    """Parse an AWSConfig from the 'aws' section of a YAML config."""
    aws_raw = raw.get("aws", {})
    return AWSConfig(
        region=aws_raw.get("region", "us-east-1"),
        key_name=aws_raw.get("key_name", "firesim"),
        vpc_name=aws_raw.get("vpc_name", "firesim"),
        security_group_name=aws_raw.get("security_group_name", "for-farms-only-firesim"),
        ssh_user=aws_raw.get("ssh_user", "ubuntu"),
        ssh_private_key=aws_raw.get("ssh_private_key"),
        use_public_ip=aws_raw.get("use_public_ip", False),
        s3_bucket=aws_raw.get("s3_bucket", "firesim-chia-builds"),
    )


def cmd_firesim_build(args):
    """Execute a standalone bitstream build."""
    setup_logging(verbose=args.verbose)
    signal.signal(signal.SIGINT, _sigint_handler)

    with open(args.config_file) as f:
        raw = yaml.safe_load(f)

    aws_config = _load_aws_config(raw)
    global _aws_region
    _aws_region = aws_config.region

    firesim_raw = raw.get("firesim", {})
    s3_bucket = raw.get("aws", {}).get("s3_bucket", "firesim-chia-builds")
    source_overlay_dir = firesim_raw.get("source_overlay_dir")
    docker_image = firesim_raw.get("docker_image", "chia-firesim-build")
    results_dir = firesim_raw.get("results_dir")

    # Build config can come from a recipes file or inline in the YAML
    build_config_dict = firesim_raw.get("build_config")
    build_recipes_file = firesim_raw.get("build_recipes_file")

    from chia.firesim.build_node import BitstreamBuilder
    from chia.firesim.config import FireSimBuildConfig, load_build_recipes

    if build_config_dict:
        build_config = FireSimBuildConfig(**build_config_dict)
    elif build_recipes_file:
        recipes = load_build_recipes(build_recipes_file)
        if args.recipe not in recipes:
            logger.error(f"Recipe '{args.recipe}' not found. Available: {list(recipes.keys())}")
            sys.exit(1)
        build_config = recipes[args.recipe]
    else:
        logger.error("firesim.build_config or firesim.build_recipes_file is required")
        sys.exit(1)

    builder = BitstreamBuilder(
        build_config=build_config,
        aws_config=aws_config,
        s3_bucket=s3_bucket,
        source_overlay_dir=source_overlay_dir,
        docker_image=docker_image,
        instance_type=getattr(args, "instance_type", "z1d.2xlarge"),
        results_dir=results_dir,
    )

    result = builder.build()
    if result.success:
        logger.info(f"Build succeeded! AGFI: {result.agfi}")
        logger.info(f"HWDB entry:\n{result.hwdb_entry}")
    else:
        logger.error(f"Build failed. Log:\n{result.build_log}")
        sys.exit(1)


def cmd_firesim_run(args):
    """Execute a standalone FPGA simulation run, or a workload suite."""
    setup_logging(verbose=args.verbose)
    signal.signal(signal.SIGINT, _sigint_handler)

    with open(args.config_file) as f:
        raw = yaml.safe_load(f)

    aws_config = _load_aws_config(raw)
    global _aws_region
    _aws_region = aws_config.region

    firesim_raw = raw.get("firesim", {})
    results_dir = firesim_raw.get("results_dir")

    if args.suite:
        # --- Suite mode: fan out multiple workloads ---
        from chia.firesim.config import FireSimRunConfig
        from chia.firesim.suite_runner import SuiteRunner
        from chia.firesim.workloads import load_manifest

        s3_bucket = getattr(args, "workload_bucket", None) or raw.get("aws", {}).get("s3_bucket", "firesim-chia-builds")
        suite_s3_prefix = f"workloads/{args.suite}"
        manifest = load_manifest(s3_bucket, suite_s3_prefix)

        workload_names = None
        if args.workloads:
            workload_names = [w.strip() for w in args.workloads.split(",")]

        hw_config = args.hw_config if args.hw_config else args.suite

        base_run_config = FireSimRunConfig(
            hw_config_name=hw_config,
            build_ref=getattr(args, "build", None) or firesim_raw.get("build_ref"),
            agfi=getattr(args, "agfi", None),
            instance_type=getattr(args, "instance_type", "f2.12xlarge"),
            plusarg_passthrough=getattr(args, "plusargs", ""),
            driver_s3_path=firesim_raw.get("driver_s3_path"),
            driver_tarball_path=firesim_raw.get("driver_tarball_path"),
        )

        runner = SuiteRunner(
            manifest=manifest,
            workload_names=workload_names,
            base_run_config=base_run_config,
            aws_config=aws_config,
            s3_bucket=s3_bucket,
            parallelism=getattr(args, "parallelism", 4),
            sim_timeout=getattr(args, "timeout", 14400),
            results_dir=results_dir,
        )

        result = runner.run()

        logger.info(f"\n{'='*60}")
        logger.info(f"Suite: {result.suite_name}")
        logger.info(f"Total time: {result.total_duration_seconds:.1f}s")
        logger.info(f"Overall: {'PASS' if result.all_success else 'FAIL'}")
        logger.info(f"{'='*60}")
        for name, res in sorted(result.workload_results.items()):
            status = "PASS" if res.success else "FAIL"
            logger.info(f"  {name}: {status} ({res.duration_seconds:.1f}s)")

        if result.scores:
            from chia.firesim.spec_parser import compute_aggregate_score
            logger.info(f"\n--- Scores ---")
            for name, sc in sorted(result.scores.items()):
                logger.info(f"  {name}: {sc['score']:.3f} (RealTime={sc['RealTime']:.1f}s)")
            agg = compute_aggregate_score(result.scores)
            logger.info(f"  Aggregate (geomean): {agg:.3f}")

        if not result.all_success:
            sys.exit(1)
    else:
        # --- Single workload mode ---
        if not args.hw_config:
            logger.error("--hw-config is required when --suite is not specified")
            sys.exit(1)
        if not args.workload:
            logger.error("--workload is required when --suite is not specified")
            sys.exit(1)

        from chia.firesim.config import FireSimRunConfig
        from chia.firesim.run_node import SimulationRunner

        run_config = FireSimRunConfig(
            hw_config_name=args.hw_config,
            build_ref=getattr(args, "build", None) or firesim_raw.get("build_ref"),
            agfi=getattr(args, "agfi", None),
            workload_name=args.workload,
            num_sims=getattr(args, "num_sims", 1),
            instance_type=getattr(args, "instance_type", "f2.12xlarge"),
            plusarg_passthrough=getattr(args, "plusargs", ""),
            driver_s3_path=firesim_raw.get("driver_s3_path"),
            driver_tarball_path=firesim_raw.get("driver_tarball_path"),
            bootbinary_s3_path=firesim_raw.get("bootbinary_s3_path"),
            bootbinary_path=firesim_raw.get("bootbinary_path"),
            rootfs_s3_path=firesim_raw.get("rootfs_s3_path"),
            rootfs_path=firesim_raw.get("rootfs_path"),
        )

        # Auto-derive agfi + driver_s3_path from build_ref if provided
        if run_config.build_ref:
            s3_bucket = raw.get("aws", {}).get("s3_bucket", "firesim-chia-builds")
            run_config.resolve_build(s3_bucket)

        runner = SimulationRunner(
            run_config=run_config,
            aws_config=aws_config,
            sim_timeout=getattr(args, "timeout", 14400),
            results_dir=results_dir,
        )

        result = runner.run()
        if result.success:
            logger.info(f"Simulation completed in {result.duration_seconds:.1f}s")
            for name, log in result.uartlogs.items():
                logger.info(f"--- {name} ---")
                logger.info(log[:2000] if log else "(empty)")
        else:
            logger.error("Simulation failed")
            sys.exit(1)


def cmd_firesim_upload_workload(args):
    """Upload FireMarshal workload images to S3."""
    setup_logging(verbose=args.verbose)

    if args.dest_bucket:
        s3_bucket = args.dest_bucket
    elif args.config_file and args.config_file.strip():
        with open(args.config_file) as f:
            raw = yaml.safe_load(f)
        s3_bucket = raw.get("aws", {}).get("s3_bucket", "firesim-chia-builds")
    else:
        s3_bucket = "firesim-chia-builds"

    from chia.firesim.workloads import upload_workload_images

    manifest = upload_workload_images(
        marshal_config_path=args.marshal_config,
        images_dir=args.images_dir,
        s3_bucket=s3_bucket,
        suite_name=args.suite_name,
        dataset=getattr(args, "dataset", ""),
    )

    logger.info(f"Uploaded suite '{manifest.suite}' with {len(manifest.workloads)} workloads")
    logger.info(f"Workloads: {manifest.workload_names()}")


def cmd_firesim_cleanup(args):
    """Find and terminate orphaned chia-tagged EC2 instances."""
    setup_logging(verbose=args.verbose)

    with open(args.config_file) as f:
        raw = yaml.safe_load(f)

    aws_config = _load_aws_config(raw)

    import boto3
    client = boto3.client("ec2", region_name=aws_config.region)

    # Find instances tagged with chia-cluster
    response = client.describe_instances(
        Filters=[
            {"Name": "tag-key", "Values": ["chia-cluster"]},
            {"Name": "instance-state-name", "Values": ["running", "pending"]},
        ]
    )

    instance_ids = []
    for reservation in response.get("Reservations", []):
        for inst in reservation.get("Instances", []):
            iid = inst["InstanceId"]
            tags = {t["Key"]: t["Value"] for t in inst.get("Tags", [])}
            logger.info(
                f"  {iid}: type={inst['InstanceType']}, "
                f"cluster={tags.get('chia-cluster', '?')}, "
                f"op={tags.get('chia-op', '?')}"
            )
            instance_ids.append(iid)

    if not instance_ids:
        logger.info("No orphaned chia instances found")
        return

    logger.info(f"Found {len(instance_ids)} chia instance(s)")

    if not args.yes:
        confirm = input("Terminate these instances? [y/N] ")
        if confirm.lower() != "y":
            logger.info("Aborted")
            return

    from chia.aws.ec2 import terminate_ec2_instances
    terminate_ec2_instances(instance_ids, region=aws_config.region)
    logger.info(f"Terminated {len(instance_ids)} instance(s)")
