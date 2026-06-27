"""FireSim-specific configuration dataclasses and YAML parsers."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from chia.cluster.log import get_logger

logger = get_logger("firesim.config")


@dataclass
class FireSimBuildConfig:
    """A single bitstream build recipe, parsed from config_build_recipes.yaml.

    Mirrors one stanza of a FireSim ``config_build_recipes.yaml`` (the recipe
    name is the YAML key; the remaining fields come from that entry). Drives an
    FPGA bitstream build for a given target design + FPGA platform.

    Attributes:
        name: Recipe name (the YAML key); used as the build's base name
            (e.g. ``"midasexamples_gcd"``).
        platform: FireSim platform (e.g. ``"f1"``, ``"f2"``);
            from the recipe's ``PLATFORM``.
        target_project: Chipyard target project (``TARGET_PROJECT``,
            e.g. ``"firesim"``, or ``"midasexamples"`` for MIDAS example designs).
        target_project_makefrag: Optional makefrag override for the target
            project (``TARGET_PROJECT_MAKEFRAG``, e.g.
            ``"../../../generators/firechip/chip/src/main/makefrag/firesim"``);
            ``None`` to use the default.
        design: Top-level design/harness module to build (``DESIGN``, e.g.
            ``"FireSim"``, or ``"GCD"`` for the MIDAS example).
        target_config: Chipyard target Scala config (``TARGET_CONFIG``) selecting
            the SoC configuration (e.g. ``"FireSimMegaBoomChiaBigCacheConfig"``,
            ``"FireSimRocketConfig"``).
        platform_config: FireSim platform config (``PLATFORM_CONFIG``) selecting
            host-side simulation options (e.g. ``"DefaultF2Config"``).
        fpga_frequency: Target FPGA clock frequency in MHz
            (from ``platform_config_args.fpga_frequency``, e.g. ``20``).
        build_strategy: Vivado build strategy (from
            ``platform_config_args.build_strategy``, e.g. ``"TIMING"``).
        bit_builder_recipe: Path to the FireSim bit-builder recipe YAML
            (``bit_builder_recipe``) selecting the platform's build flow (e.g.
            ``"bit-builder-recipes/f2.yaml"``).
        deploy_quintuplet: Optional override for the FireSim deploy quintuplet
            (``PLATFORM-TARGET_PROJECT-DESIGN-TARGET_CONFIG-PLATFORM_CONFIG``,
            e.g. ``"f2-firesim-FireSim-FireSimMegaBoomChiaBigCacheConfig-DefaultF2Config"``);
            ``None`` to let FireSim derive it.
        post_build_hook: Optional path to a script run after the build completes.
        metasim_customruntimeconfig: Optional custom runtime config for metasim
            (software-simulation) builds.
        build_id: User-specified build ID (e.g. ``"megaboom-baseline"``). If
            omitted, an auto-generated ``{name}-{timestamp}-{short_uuid}`` is used.
        build_group: Optional subdirectory under ``builds/`` for organizing builds
            (e.g. ``builds/{build_group}/{build_id}/``). If omitted, builds are stored flat under
            ``builds/{build_id}/``.
        incremental_base_build_id: ``build_id`` of a previous build whose DCPs to
            use as Vivado incremental synthesis/implementation references
            (~50% faster); ``None`` for a full build.
        market: AWS instance market for the build host (``"ondemand"`` or
            ``"spot"``).
        enable_pr: Whether to enable partial reconfiguration (PR) for this build.
        pr_module_name: Reconfigurable module name when ``enable_pr`` is set.
        pr_base_build_id: ``build_id`` of the PR base build (its presence triggers
            the reconfigurable-module flow).
        pr_partition_cell: PR partition cell; auto-discovered if omitted.
    """
    name: str
    platform: str
    target_project: str
    target_project_makefrag: str | None
    design: str
    target_config: str
    platform_config: str
    fpga_frequency: float
    build_strategy: str
    bit_builder_recipe: str
    deploy_quintuplet: str | None = None
    post_build_hook: str | None = None
    metasim_customruntimeconfig: str | None = None
    # User-specified build ID (e.g. "megaboom-baseline"). If omitted, an
    # auto-generated {name}-{timestamp}-{short_uuid} is used.
    build_id: str | None = None
    # Optional subdirectory under builds/ for organizing builds
    # (e.g. "prefetcher-experiments" → builds/prefetcher-experiments/{build_id}/).
    # If omitted, builds are stored flat under builds/{build_id}/.
    build_group: str | None = None
    # Incremental compile: build_id of a previous build whose DCPs to use as
    # Vivado incremental synthesis/implementation references (~50% faster).
    incremental_base_build_id: str | None = None
    # Partial reconfiguration (PR)
    market: str = "ondemand"
    enable_pr: bool = False
    pr_module_name: str | None = None       # reconfigurable module (e.g. "BestOffsetPrefetcher")
    pr_base_build_id: str | None = None     # build_id of the PR base build (triggers RM flow)
    pr_partition_cell: str | None = None    # auto-discovered if omitted


@dataclass
class FireSimRunConfig:
    """Runtime configuration for an FPGA simulation run.

    Describes a single FireSim run: which bitstream (AGFI) and simulation driver
    to deploy, which workload to run, and on what instances. The build artifacts
    (``agfi``, ``driver_s3_path``) are typically auto-derived from ``build_ref``
    via :meth:`resolve_build`.

    Attributes:
        hw_config_name: Name of the hardware-database (hwdb) entry / hardware
            config to run (the ``config_hwdb.yaml`` key, e.g.
            ``"firesim_megaboom_chia_bigcache"``).
        build_ref: Build reference — path under ``builds/`` on S3 identifying a
            build, of the form ``"{recipe}/{build_id}"``. Examples:
            ``"FireSimRocket-20260409-a1b2c3d4"`` (flat) or
            ``"experiments/FireSimRocket-20260409-a1b2c3d4"`` (grouped).
            When set, ``agfi`` and ``driver_s3_path`` are auto-derived via
            :meth:`resolve_build`.
        agfi: AWS Global FPGA Image ID (e.g. ``"agfi-01234abcde"``) to
            program onto the FPGA; auto-derived from ``build_ref`` if unset.
        workload_name: Name of the workload to run (e.g. ``"spec17-intrate.json"``).
        num_sims: Number of parallel simulation slots to launch.
        instance_type: AWS instance type for the run host
            (e.g. ``"f2.12xlarge"``).
        plusarg_passthrough: Extra ``+plusarg`` flags passed through to the
            simulator.
        terminate_on_completion: Whether to terminate the run instance when the
            run finishes.
        driver_tarball_path: Local path to the simulation driver tarball
            (legacy/fallback; prefer ``driver_s3_path``).
        ami_id: Optional AMI ID for the run instance.
        market: AWS instance market for the run host (``"ondemand"`` or
            ``"spot"``).
        bootbinary_path: Local path to the boot binary (legacy/fallback; prefer
            ``bootbinary_s3_path``).
        rootfs_path: Local path to the root filesystem image (legacy/fallback;
            prefer ``rootfs_s3_path``).
        result_id: Optional subdirectory under ``results/`` for organizing run
            results (e.g. ``results/{result_id}/{suite_name}/``). If omitted, results are stored under
            ``results/{suite_name}/{timestamp}/``.
        driver_s3_path: S3 URI of the simulation driver tarball (preferred — any
            worker node can use it), e.g.
            ``"s3://firesim-chia-builds/builds/design/driver-bundle.tar.gz"``;
            auto-derived from ``build_ref`` when :meth:`resolve_build` is called.
        bootbinary_s3_path: S3 URI of the boot binary (preferred over the local
            path).
        rootfs_s3_path: S3 URI of the root filesystem image (preferred over the
            local path).
    """
    hw_config_name: str
    # Build reference — path under builds/ on S3 that identifies a build.
    # Examples: "FireSimRocket-20260409-a1b2c3d4" (flat) or
    #           "prefetcher-experiments/FireSimRocket-20260409-a1b2c3d4" (grouped).
    # When set, agfi and driver_s3_path are auto-derived via resolve_build().
    build_ref: str | None = None
    agfi: str | None = None
    workload_name: str = ""
    num_sims: int = 1
    instance_type: str = "f2.12xlarge"
    plusarg_passthrough: str = ""
    terminate_on_completion: bool = True
    # Local paths (legacy/fallback)
    driver_tarball_path: str | None = None
    ami_id: str | None = None
    market: str = "ondemand"
    bootbinary_path: str | None = None
    rootfs_path: str | None = None
    # Optional subdirectory under results/ for organizing run results
    # (e.g. "opt-branch-20260409" → results/{result_id}/{suite_name}/).
    # If omitted, results are stored under results/{suite_name}/{timestamp}/.
    result_id: str | None = None
    # S3 paths (preferred — any worker node can use these)
    # Auto-derived from build_ref when resolve_build() is called.
    driver_s3_path: str | None = None
    bootbinary_s3_path: str | None = None
    rootfs_s3_path: str | None = None

    def resolve_build(self, s3_bucket: str) -> FireSimRunConfig:
        """Populate agfi and driver_s3_path from build_ref if not already set.

        Fetches ``build-info.json`` from S3 for the referenced build and uses it
        to fill in ``agfi`` and ``driver_s3_path``. Explicit values for either
        field take precedence (they are not overwritten). A no-op if ``build_ref``
        is unset or both derived fields are already populated.

        Args:
            s3_bucket: S3 bucket name holding the build's ``build-info.json``.

        Returns:
            ``self``, with ``agfi`` and ``driver_s3_path`` populated from the
            build referenced by ``build_ref`` where they were previously unset.
            Returned for chaining.
        """
        if not self.build_ref:
            return self
        if self.agfi and self.driver_s3_path:
            return self  # both already set, nothing to resolve

        info = fetch_build_info(s3_bucket, self.build_ref)
        if not self.agfi:
            self.agfi = info.get("agfi")
        if not self.driver_s3_path:
            self.driver_s3_path = info.get("driver_s3_path")
        return self


@dataclass
class FireSimConfig:
    """Top-level FireSim configuration, parsed from the Chia cluster YAML.

    Attributes:
        chipyard_path: Filesystem path to the Chipyard checkout on the host.
        deploy_path: Filesystem path to the FireSim deploy directory
            (``sim/firesim`` workspace where builds/runs are launched).
        build_recipes_file: Filename of the FireSim build-recipes YAML to load
            (defaults to ``"config_build_recipes.yaml"``).
    """
    chipyard_path: str
    deploy_path: str
    build_recipes_file: str = "config_build_recipes.yaml"


def load_build_recipes(yaml_path: str) -> dict[str, FireSimBuildConfig]:
    """Parse a config_build_recipes.yaml file into FireSimBuildConfig objects.

    Args:
        yaml_path: Path to config_build_recipes.yaml.

    Returns:
        Dict mapping recipe name to FireSimBuildConfig. Empty if the YAML is
        empty.

    Raises:
        FileNotFoundError: If ``yaml_path`` does not exist.
    """
    path = Path(yaml_path)
    if not path.exists():
        raise FileNotFoundError(f"Build recipes file not found: {yaml_path}")

    with open(path) as f:
        raw = yaml.safe_load(f)

    if not raw:
        return {}

    recipes: dict[str, FireSimBuildConfig] = {}
    for name, cfg in raw.items():
        if not isinstance(cfg, dict):
            continue
        platform_args = cfg.get("platform_config_args", {})
        recipes[name] = FireSimBuildConfig(
            name=name,
            platform=cfg.get("PLATFORM", ""),
            target_project=cfg.get("TARGET_PROJECT", ""),
            target_project_makefrag=cfg.get("TARGET_PROJECT_MAKEFRAG"),
            design=cfg.get("DESIGN", ""),
            target_config=cfg.get("TARGET_CONFIG", ""),
            platform_config=cfg.get("PLATFORM_CONFIG", ""),
            fpga_frequency=platform_args.get("fpga_frequency", 75),
            build_strategy=platform_args.get("build_strategy", "TIMING"),
            bit_builder_recipe=cfg.get("bit_builder_recipe", ""),
            deploy_quintuplet=cfg.get("deploy_quintuplet"),
            post_build_hook=cfg.get("post_build_hook"),
            metasim_customruntimeconfig=cfg.get("metasim_customruntimeconfig"),
            build_id=cfg.get("build_id"),
            build_group=cfg.get("build_group"),
        )
        logger.debug(f"Loaded build recipe: {name} ({recipes[name].platform})")

    return recipes


def load_hwdb(yaml_path: str) -> dict[str, dict[str, Any]]:
    """Parse config_hwdb.yaml into a dict of hw config name -> properties.

    Args:
        yaml_path: Path to config_hwdb.yaml.

    Returns:
        Dict mapping hw config name to its hwdb properties (agfi, etc.); empty
        if the YAML does not parse to a mapping.

    Raises:
        FileNotFoundError: If ``yaml_path`` does not exist.
    """
    path = Path(yaml_path)
    if not path.exists():
        raise FileNotFoundError(f"HWDB file not found: {yaml_path}")

    with open(path) as f:
        raw = yaml.safe_load(f)

    return raw if isinstance(raw, dict) else {}


def fetch_build_info(s3_bucket: str, build_ref: str) -> dict:
    """Fetch build-info.json from S3 for a build reference.

    Args:
        s3_bucket: S3 bucket name.
        build_ref: Path under builds/ identifying a build.
            Examples: "FireSimRocket-20260409-a1b2c3d4" (flat),
            "prefetcher-experiments/FireSimRocket-20260409-a1b2c3d4" (grouped),
            or "latest" / "mygroup/latest" for the most recent build.

    Returns:
        Parsed build-info dict with keys: agfi, driver_s3_path, build_id, etc.

    Raises:
        FileNotFoundError: If build-info.json does not exist at the expected path.
    """
    import boto3

    s3_key = f"builds/{build_ref}/build-info.json"
    logger.info(f"Fetching build info from s3://{s3_bucket}/{s3_key}")

    s3 = boto3.client("s3")
    try:
        response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
        return json.loads(response["Body"].read().decode())
    except s3.exceptions.NoSuchKey:
        raise FileNotFoundError(
            f"Build info not found: s3://{s3_bucket}/{s3_key}. "
            f"Check that build_ref '{build_ref}' is correct."
        )
    except Exception as e:
        raise FileNotFoundError(
            f"Cannot fetch build info from s3://{s3_bucket}/{s3_key}: {e}. "
            f"Check bucket name, credentials, and network."
        )
