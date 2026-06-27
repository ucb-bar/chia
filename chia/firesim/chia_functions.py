"""@ChiaFunction wrappers for FireSim build and run operations.

These functions can be scheduled onto the firesim_manager node,
or called directly for standalone use.
"""

from __future__ import annotations

from chia.aws.config import AWSConfig
from chia.base.ChiaFunction import ChiaFunction, get
from chia.cluster.log import setup_logging
from chia.firesim.config import (
    FireSimBuildConfig,
    FireSimRunConfig,
    load_build_recipes,
)
from chia.firesim.state_def import BitstreamBuildResult, FireSimRunResult


@ChiaFunction(resources={"firesim_manager": 1.0})
def firesim_build_bitstream(
    build_recipe_name: str,
    aws_config_dict: dict,
    s3_bucket: str,
    source_overlay_dir: str | None = None,
    docker_image: str = "chia-chisel-build",
    instance_type: str = "z1d.2xlarge",
    build_recipes_file: str | None = None,
    build_config_dict: dict | None = None,
    results_dir: str | None = None,
    local_log_dir: str | None = None,
) -> BitstreamBuildResult:
    """Build an FPGA bitstream using an ephemeral EC2 build host.

    All build steps run on the EC2 instance — the head node does NOT need
    a chipyard installation. Chisel elaboration + driver build happen inside
    a Docker container (chia-chisel-build); Vivado runs natively on the host.

    Args:
        build_recipe_name: Name of the build recipe (used for tagging/logging).
        aws_config_dict: Dict representation of AWSConfig fields.
        s3_bucket: S3 bucket for AFI tarball storage.
        source_overlay_dir: Local dir with modified Chisel sources to overlay
            onto the container's chipyard. If None, uses the Docker image as-is.
        docker_image: Docker image for Chisel elaboration.
        instance_type: EC2 instance type for the build host.
        build_recipes_file: Path to build recipes YAML (to look up recipe by name).
            Mutually exclusive with build_config_dict.
        build_config_dict: Dict representation of FireSimBuildConfig fields.
            If provided, used directly instead of looking up from recipes file.
        results_dir: Local directory to store build results. Optional.
        local_log_dir: Local directory to mirror the build host's logs into.
            Optional; if None, logs are not copied back to the head node.

    Returns:
        BitstreamBuildResult with the AGFI/AFI (or raw bitstream path), the
        ``hwdb_entry`` and ``build_ref`` needed to launch runs, and the build
        log. On a recipe-resolution error (missing recipe, or neither
        ``build_config_dict`` nor ``build_recipes_file`` given) returns early
        with ``success=False`` and the reason in ``build_log``.
    """
    setup_logging(verbose=True)
    from chia.firesim.build_node import BitstreamBuilder

    aws_config = AWSConfig(**aws_config_dict)

    # Resolve build config: either from dict or by looking up recipe name
    if build_config_dict:
        build_config = FireSimBuildConfig(**build_config_dict)
    elif build_recipes_file:
        recipes = load_build_recipes(build_recipes_file)
        if build_recipe_name not in recipes:
            return BitstreamBuildResult(
                recipe_name=build_recipe_name,
                agfi=None, afi=None,
                success=False,
                build_log=f"Recipe '{build_recipe_name}' not found in {build_recipes_file}. "
                           f"Available: {list(recipes.keys())}",
                hwdb_entry="",
            )
        build_config = recipes[build_recipe_name]
    else:
        return BitstreamBuildResult(
            recipe_name=build_recipe_name,
            agfi=None, afi=None,
            success=False,
            build_log="Either build_config_dict or build_recipes_file must be provided.",
            hwdb_entry="",
        )

    builder = BitstreamBuilder(
        build_config=build_config,
        aws_config=aws_config,
        s3_bucket=s3_bucket,
        source_overlay_dir=source_overlay_dir,
        docker_image=docker_image,
        instance_type=instance_type,
        results_dir=results_dir,
        local_log_dir=local_log_dir,
    )
    return builder.build()


@ChiaFunction(resources={"firesim_manager": 0.1})
def firesim_run_workload(
    run_config_dict: dict,
    aws_config_dict: dict,
    sim_timeout: int = 14400,
    results_dir: str | None = None,
    name: str = "",
    instance_prefix: str | None = None,
    terminate_on_failure: bool = True,
    local_log_dir: str | None = None,
    log_prefix: str = "firesim-run",
) -> FireSimRunResult:
    """Run an FPGA simulation workload using ephemeral F2 EC2 instances.

    Args:
        run_config_dict: Dict representation of FireSimRunConfig fields. If it
            carries a ``build_ref``, the AGFI and driver S3 path are auto-derived
            from it via ``resolve_build`` against the AWS config's S3 bucket
            (falling back to ``"firesim-chia-builds"``).
        aws_config_dict: Dict representation of AWSConfig fields.
        sim_timeout: Maximum simulation runtime in seconds (default 14400 = 4h).
        results_dir: Local directory to store results. Optional.
        name: Name of the simulation instance. Optional.
        instance_prefix: Optional prefix applied to the launched EC2 instance
            name(s); None uses the runner's default naming.
        terminate_on_failure: If False, leave instances running when the
            simulation raises — for post-mortem SSH debugging.
        local_log_dir: Local directory to mirror the run's logs into. Optional.
        log_prefix: Filename/tag prefix for emitted log files
            (default ``"firesim-run"``).

    Returns:
        FireSimRunResult with per-slot uartlogs, rootfs/sim outputs, the overall
        success status, and the wall-clock duration.
    """
    setup_logging(verbose=True)
    from chia.firesim.run_node import SimulationRunner

    aws_config = AWSConfig(**aws_config_dict)
    run_config = FireSimRunConfig(**run_config_dict)

    # Auto-derive agfi + driver_s3_path from build_ref if provided
    if run_config.build_ref:
        s3_bucket = aws_config.s3_bucket or "firesim-chia-builds"
        run_config.resolve_build(s3_bucket)

    runner = SimulationRunner(
        run_config=run_config,
        aws_config=aws_config,
        sim_timeout=sim_timeout,
        results_dir=results_dir,
        local_log_dir=local_log_dir,
        log_prefix=log_prefix,
    )
    return runner.run(
        instance_name=name,
        instance_prefix=instance_prefix,
        terminate_on_failure=terminate_on_failure,
    )
