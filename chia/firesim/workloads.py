"""Workload manifest management and S3 upload/download utilities.

Handles uploading FireMarshal-built workload images to S3,
parsing manifest.json catalogs, and resolving per-workload FireSimRunConfigs.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from chia.cluster.log import get_logger
from chia.firesim.config import FireSimRunConfig

logger = get_logger("firesim.workloads")


@dataclass
class WorkloadManifest:
    """Parsed manifest.json catalog for a single FireSim workload suite.

    A manifest describes one FireMarshal-built workload suite uploaded to S3:
    the shared boot binary and the per-workload rootfs images, keyed by their
    S3 locations.

    Attributes:
        suite: Name of the workload suite; also the S3 prefix segment under
            ``workloads/`` where the suite's images and manifest live.
        dataset: Dataset label for the suite (e.g. ``"test"``, ``"ref"``);
            empty string when unspecified.
        bootbinary_s3_key: S3 key (relative to the bucket) of the shared
            FireSim boot binary used by every workload in the suite.
        workloads: Per-workload image catalog mapping ``workload_name`` ->
            ``{"rootfs_s3_key": "..."}``, where ``rootfs_s3_key`` is the S3
            key of that workload's rootfs image.
    """

    suite: str
    dataset: str
    bootbinary_s3_key: str
    workloads: dict[str, dict[str, str]] = field(default_factory=dict)
    # workloads maps workload_name -> {"rootfs_s3_key": "..."}

    def workload_names(self) -> list[str]:
        """Return the suite's workload names in sorted order.

        Returns:
            Sorted list of workload names present in this manifest.
        """
        return sorted(self.workloads.keys())

    def to_dict(self) -> dict:
        """Serialize this manifest to a plain dict for manifest.json.

        Returns:
            Dict with ``suite``, ``dataset``, ``bootbinary_s3_key``, and
            ``workloads`` fields, suitable for JSON serialization.
        """
        return {
            "suite": self.suite,
            "dataset": self.dataset,
            "bootbinary_s3_key": self.bootbinary_s3_key,
            "workloads": self.workloads,
        }

    @classmethod
    def from_dict(cls, data: dict) -> WorkloadManifest:
        """Construct a WorkloadManifest from a parsed manifest.json dict.

        Accepts both the current ``"workloads"`` field and the legacy
        ``"benchmarks"`` field for backward compatibility, and tolerates a
        missing ``"dataset"`` (defaulting to an empty string).

        Args:
            data: Parsed manifest.json contents. Must contain ``"suite"`` and
                ``"bootbinary_s3_key"``; ``"dataset"`` and the workload catalog
                (``"workloads"`` or ``"benchmarks"``) are optional.

        Returns:
            A WorkloadManifest populated from the dict.
        """
        return cls(
            suite=data["suite"],
            dataset=data.get("dataset", ""),
            bootbinary_s3_key=data["bootbinary_s3_key"],
            # Accept both "workloads" (new) and "benchmarks" (old manifests)
            workloads=data.get("workloads", data.get("benchmarks", {})),
        )


def upload_workload_images(
    marshal_config_path: str,
    images_dir: str,
    s3_bucket: str,
    suite_name: str,
    dataset: str = "",
) -> WorkloadManifest:
    """Upload FireMarshal workload images to S3 and generate manifest.json.

    Parses the FireMarshal config JSON to discover job names, uploads the
    shared bootbinary and per-job rootfs images, then uploads the manifest.

    Args:
        marshal_config_path: Path to FireMarshal config JSON.
        images_dir: Directory containing built images (bootbinary + rootfs).
        s3_bucket: S3 bucket to upload to.
        suite_name: Name for the suite (used as S3 prefix).
        dataset: Dataset label (e.g. "test", "ref").

    Returns:
        The generated WorkloadManifest, also uploaded to
        ``s3://{s3_bucket}/workloads/{suite_name}/manifest.json``.

    Raises:
        FileNotFoundError: If the marshal config, images directory, or the
            expected boot binary cannot be found.
        ValueError: If the marshal config defines no ``jobs``.
        RuntimeError: If no workload rootfs images were found to upload.
    """
    import boto3

    images_path = Path(images_dir)
    marshal_path = Path(marshal_config_path)

    if not marshal_path.exists():
        raise FileNotFoundError(f"Marshal config not found: {marshal_config_path}")
    if not images_path.is_dir():
        raise FileNotFoundError(f"Images directory not found: {images_dir}")

    with open(marshal_path) as f:
        marshal_config = json.load(f)

    # Extract job names from FireMarshal config
    # jobs can be a list of {"name": ..., "command": ...} or a dict of name -> config
    jobs = marshal_config.get("jobs", [])
    if not jobs:
        raise ValueError(
            f"No jobs found in marshal config {marshal_config_path}. "
            "Expected a 'jobs' field with workload definitions."
        )

    if isinstance(jobs, list):
        job_names = sorted(j["name"] for j in jobs)
    else:
        job_names = sorted(jobs.keys())
    logger.info(f"Found {len(job_names)} workload jobs: {job_names}")

    s3 = boto3.client("s3")
    s3_prefix = f"workloads/{suite_name}"

    # Upload shared bootbinary
    # FireMarshal layout: images_dir/{base_name}/{base_name}-bin
    # or flat: images_dir/{base_name}-bin
    base_name = marshal_config.get("name", suite_name)
    bootbinary_filename = f"{base_name}-bin"
    candidates = [
        images_path / base_name / bootbinary_filename,          # subdir layout
        images_path / bootbinary_filename,                      # flat layout
        images_path / base_name / f"{base_name}.img",
    ]
    bootbinary_local = None
    for c in candidates:
        if c.exists():
            bootbinary_local = c
            break
    if bootbinary_local is None:
        raise FileNotFoundError(
            f"Boot binary not found. Tried: {[str(c) for c in candidates]}. "
            f"Check images_dir contents."
        )

    bootbinary_s3_key = f"{s3_prefix}/bootbinary/{bootbinary_filename}"
    logger.info(f"Uploading bootbinary: {bootbinary_local} -> s3://{s3_bucket}/{bootbinary_s3_key}")
    s3.upload_file(str(bootbinary_local), s3_bucket, bootbinary_s3_key)

    # Upload per-workload rootfs images
    # FireMarshal layout: images_dir/{base_name}-{job_name}/{base_name}-{job_name}.img
    # or flat: images_dir/{job_name}.img or images_dir/{base_name}-{job_name}.img
    workloads: dict[str, dict[str, str]] = {}
    for job_name in job_names:
        prefixed_name = f"{base_name}-{job_name}"
        candidates = [
            images_path / prefixed_name / f"{prefixed_name}.img",   # subdir layout
            images_path / f"{prefixed_name}.img",                   # flat with prefix
            images_path / f"{job_name}.img",                        # flat bare name
        ]
        rootfs_local = None
        for c in candidates:
            if c.exists():
                rootfs_local = c
                break
        if rootfs_local is None:
            logger.warning(f"Rootfs not found for {job_name}, skipping. Tried: {[str(c) for c in candidates]}")
            continue

        rootfs_s3_key = f"{s3_prefix}/rootfs/{job_name}.img"
        logger.info(f"Uploading rootfs: {rootfs_local} -> s3://{s3_bucket}/{rootfs_s3_key}")
        s3.upload_file(str(rootfs_local), s3_bucket, rootfs_s3_key)
        workloads[job_name] = {"rootfs_s3_key": rootfs_s3_key}

    if not workloads:
        raise RuntimeError("No workload rootfs images were found to upload")

    # Generate and upload manifest
    manifest = WorkloadManifest(
        suite=suite_name,
        dataset=dataset,
        bootbinary_s3_key=bootbinary_s3_key,
        workloads=workloads,
    )

    manifest_json = json.dumps(manifest.to_dict(), indent=2)
    manifest_s3_key = f"{s3_prefix}/manifest.json"
    logger.info(f"Uploading manifest: s3://{s3_bucket}/{manifest_s3_key}")
    s3.put_object(
        Bucket=s3_bucket,
        Key=manifest_s3_key,
        Body=manifest_json.encode(),
        ContentType="application/json",
    )

    logger.info(
        f"Upload complete: {len(workloads)} workloads in suite '{suite_name}'"
    )
    return manifest


def load_manifest(s3_bucket: str, suite_s3_prefix: str) -> WorkloadManifest:
    """Download and parse a manifest.json from S3.

    Args:
        s3_bucket: S3 bucket name.
        suite_s3_prefix: S3 prefix for the suite (e.g. "workloads/spec17-intspeed-test").

    Returns:
        Parsed WorkloadManifest.
    """
    import boto3

    s3 = boto3.client("s3")
    manifest_key = f"{suite_s3_prefix}/manifest.json"
    logger.info(f"Loading manifest from s3://{s3_bucket}/{manifest_key}")

    response = s3.get_object(Bucket=s3_bucket, Key=manifest_key)
    data = json.loads(response["Body"].read().decode())
    return WorkloadManifest.from_dict(data)


def resolve_run_configs(
    manifest: WorkloadManifest,
    workload_names: list[str] | None,
    base_run_config: FireSimRunConfig,
    s3_bucket: str,
) -> list[FireSimRunConfig]:
    """Resolve per-workload FireSimRunConfigs from a manifest.

    Args:
        manifest: Parsed workload manifest.
        workload_names: List of workload names to run, or None for all.
        base_run_config: Base config with agfi, driver_s3_path, instance_type, etc.
        s3_bucket: S3 bucket containing the workload images.

    Returns:
        List of FireSimRunConfig, one per workload, each pointing at the
        suite's shared bootbinary and that workload's rootfs in S3.

    Raises:
        ValueError: If any name in ``workload_names`` is not present in the
            manifest.
    """
    if workload_names is None:
        names = manifest.workload_names()
    else:
        # Validate requested workloads exist in manifest
        available = set(manifest.workloads.keys())
        missing = [n for n in workload_names if n not in available]
        if missing:
            raise ValueError(
                f"Workloads not found in manifest: {missing}. "
                f"Available: {sorted(available)}"
            )
        names = workload_names

    bootbinary_s3 = f"s3://{s3_bucket}/{manifest.bootbinary_s3_key}"

    configs = []
    for name in names:
        wl_info = manifest.workloads[name]
        rootfs_s3 = f"s3://{s3_bucket}/{wl_info['rootfs_s3_key']}"

        config = FireSimRunConfig(
            hw_config_name=base_run_config.hw_config_name,
            agfi=base_run_config.agfi,
            workload_name=name,
            num_sims=1,
            instance_type=base_run_config.instance_type,
            plusarg_passthrough=base_run_config.plusarg_passthrough,
            terminate_on_completion=base_run_config.terminate_on_completion,
            driver_s3_path=base_run_config.driver_s3_path,
            driver_tarball_path=base_run_config.driver_tarball_path,
            bootbinary_s3_path=bootbinary_s3,
            rootfs_s3_path=rootfs_s3,
            result_id=base_run_config.result_id,
            market=base_run_config.market,
        )
        configs.append(config)

    logger.info(f"Resolved {len(configs)} workload run configs from manifest '{manifest.suite}'")
    return configs
