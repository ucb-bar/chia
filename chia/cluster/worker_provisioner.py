"""On-demand worker provisioner for Chia evaluation pipelines.

Manages two separate worker pools — build workers (heavy, chipyard image) and
sim workers (lightweight, verilator-run image) — as ephemeral Docker containers
or EC2 instances that join the Ray cluster.

Usage::

    provisioner = WorkerProvisioner(
        build_image="ghcr.io/ucb-bar/chia-chipyard-base-prefetcher:latest",
        sim_image="ghcr.io/ucb-bar/chia-verilator-run:0.1",
        ray_address="...",
    )
    provisioner.provision(num_build_workers=1, num_sim_workers=4, backend="docker")
    try:
        # ... run evaluation ...
    finally:
        provisioner.teardown()
"""

from __future__ import annotations

import json
import logging
import math
import os
import shlex
import subprocess
import time
from dataclasses import dataclass, field

logger = logging.getLogger("chia.worker_provisioner")


# Known vCPU counts for common EC2 instance types
_EC2_VCPUS = {
    "c5.xlarge": 4, "c5.2xlarge": 8, "c5.4xlarge": 16,
    "c5.9xlarge": 36, "c5.12xlarge": 48, "c5.18xlarge": 72,
    "c5.24xlarge": 96,
    "c5a.xlarge": 4, "c5a.2xlarge": 8, "c5a.4xlarge": 16,
    "c5a.8xlarge": 32, "c5a.12xlarge": 48, "c5a.16xlarge": 64,
    "c5a.24xlarge": 96,
    "m5.xlarge": 4, "m5.2xlarge": 8, "m5.4xlarge": 16,
    "m5.8xlarge": 32, "m5.12xlarge": 48, "m5.16xlarge": 64,
    "m5.24xlarge": 96,
    "z1d.2xlarge": 8, "z1d.3xlarge": 12, "z1d.6xlarge": 24,
    "z1d.12xlarge": 48,
}


@dataclass
class WorkerPoolConfig:
    """Configuration for a single worker pool (build or sim)."""
    image: str
    resources: dict[str, int | float]
    chipyard_env_script: str = "/home/ray/chipyard/env.sh"
    needs_chipyard_env: bool = True  # False for lightweight sim containers
    extra_mounts: list[tuple[str, str]] = field(default_factory=list)  # [(host_path, container_path)]


class WorkerProvisioner:
    """Provisions separate build and sim worker pools.

    Build workers use the heavy chipyard image (~29GB) with {"chipyard": 1}.
    Sim workers use the lightweight verilator-run image (~2GB) with {"verilator_run": 1}.
    One build produces a binary that fans out to many sim workers via Ray.
    """

    def __init__(
        self,
        build_image: str = "ghcr.io/ucb-bar/chia-chipyard-base-prefetcher:latest",
        sim_image: str = "ghcr.io/ucb-bar/chia-verilator-run:0.1",
        ray_address: str = "auto",
        chia_source_path: str | None = None,
        conda_env: str = "py3_12_12",
        shm_size: str = "8g",
        build_resources: dict[str, int | float] | None = None,
        sim_resources: dict[str, int | float] | None = None,
        build_extra_mounts: list[tuple[str, str]] | None = None,
    ):
        self.build_pool = WorkerPoolConfig(
            image=build_image,
            resources=build_resources or {"chipyard": 1},
            chipyard_env_script="/home/ray/chipyard/env.sh",
            needs_chipyard_env=True,
            extra_mounts=build_extra_mounts or [],
        )
        # Sim workers need chipyard env if using the full evolve-worker image
        # (for spike-dasm, libriscv.so). Lightweight images don't have it.
        sim_needs_chipyard = "evolve-worker" in sim_image or "chipyard" in sim_image
        self.sim_pool = WorkerPoolConfig(
            image=sim_image,
            resources=sim_resources or {"verilator_run": 1},
            chipyard_env_script="/home/ray/chipyard/env.sh",
            needs_chipyard_env=sim_needs_chipyard,
        )
        self.ray_address = ray_address
        self.chia_source_path = chia_source_path or self._detect_chia_source()
        self.conda_env = conda_env
        self.shm_size = shm_size

        # Unique session ID to prevent container name collisions
        import uuid
        self._session_id = uuid.uuid4().hex[:6]

        # Per-pool tracking for independent lifecycle management
        self._pool_containers: dict[str, list[str]] = {}   # pool_name -> container names
        self._pool_instances: dict[str, list[str]] = {}     # pool_name -> EC2 instance IDs
        self._pool_configs: dict[str, WorkerPoolConfig] = {
            "build": self.build_pool,
            "sim": self.sim_pool,
        }

        # Legacy flat lists (updated by pool operations for backward compat)
        self._container_names: list[str] = []
        self._ec2_instance_ids: list[str] = []
        self._aws_region: str = "us-east-1"
        self._baseline_resources: dict[str, float] = {}
        self._successful_subnets: set[str] = set()  # cache subnets that worked

    @staticmethod
    def _snapshot_resources() -> dict[str, float]:
        try:
            import ray
            if not ray.is_initialized():
                ray.init(address="auto", ignore_reinit_error=True)
            return dict(ray.cluster_resources())
        except Exception:
            return {}

    @staticmethod
    def _detect_chia_source() -> str:
        return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def provision(
        self,
        num_build_workers: int = 1,
        num_sim_workers: int = 1,
        backend: str = "docker",
        instance_type: str = "c5.4xlarge",
        instance_types: list[str] | None = None,
        instance_family: str | None = None,
        workers_per_instance: int | None = None,
        cpus_per_worker: int = 8,
        build_cpus: int | None = None,
        sim_cpus: int | None = None,
        max_cost_per_hour: float | None = None,
        max_vcpus_per_instance: int = 96,
        aws_config_dict: dict | None = None,
    ) -> None:
        """Provision build and sim worker pools.

        Instance selection priority (highest first):
          1. instance_type — use exactly this type, no fallback.
          2. instance_types — try each in order, with multi-AZ fallback per type.
          3. instance_family — auto-discover types in family, sort by cost/vCPU,
             apply caps, try in order (full fallback chain).

        Args:
            num_build_workers: Number of build workers (chipyard image).
            num_sim_workers: Number of sim workers (verilator-run image).
            backend: "docker" (local containers) or "ec2" (AWS instances).
            instance_type: Exact EC2 instance type (ec2 only).
            instance_types: Prioritized list of instance types to try in order.
            instance_family: EC2 instance family (e.g. "c5"). Auto-selects
                cheapest per-vCPU type with fallback.
            workers_per_instance: Containers per EC2 instance (ec2 only).
            cpus_per_worker: Default CPUs per worker (used if build_cpus/sim_cpus
                not set). Also used as min_vcpus for instance selection.
            build_cpus: CPUs per build worker (make_jobs). Defaults to cpus_per_worker.
            sim_cpus: CPUs per sim worker (VERILATOR_THREADS). Defaults to cpus_per_worker.
            max_cost_per_hour: Cap spot price per instance (applies to family
                auto-selection). None = no cap.
            max_vcpus_per_instance: Max vCPUs per instance (applies to family
                auto-selection). Default 96.
            aws_config_dict: AWS config for EC2 backend.
        """
        total = num_build_workers + num_sim_workers
        if total <= 0:
            return

        t_provision_start = time.time()
        self._baseline_resources = self._snapshot_resources()

        logger.info(
            f"Provisioning {num_build_workers} build + {num_sim_workers} sim "
            f"workers via {backend}"
        )

        if backend == "docker":
            self._provision_docker_pool(
                self.build_pool, num_build_workers, prefix="chia-build-worker",
            )
            self._provision_docker_pool(
                self.sim_pool, num_sim_workers, prefix="chia-sim-worker",
            )
        elif backend == "ec2":
            self._provision_ec2_pools(
                num_build_workers, num_sim_workers,
                instance_type, instance_types, instance_family,
                workers_per_instance, cpus_per_worker,
                build_cpus or cpus_per_worker,
                sim_cpus or cpus_per_worker,
                max_cost_per_hour, max_vcpus_per_instance,
                aws_config_dict or {},
            )
        else:
            raise ValueError(f"Unknown backend: {backend}")

        # Wait for all resources to appear
        target = {}
        for key, val in self.build_pool.resources.items():
            target[key] = target.get(key, 0) + num_build_workers * val
        for key, val in self.sim_pool.resources.items():
            target[key] = target.get(key, 0) + num_sim_workers * val
        self._wait_for_resources(target)

        logger.info(
            "TIMING: provision total=%.1fs backend=%s workers=%d",
            time.time() - t_provision_start, backend, total,
        )

    def teardown(self) -> None:
        """Remove all provisioned workers across all pools."""
        for pool_name in list(self._pool_containers.keys()):
            self.teardown_pool(pool_name)
        # Clean up any legacy tracked resources not in pools
        for name in list(self._container_names):
            _docker_run(f"docker stop {shlex.quote(name)}", check=False)
            _docker_run(f"docker rm -f {shlex.quote(name)}", check=False)
        self._container_names.clear()
        if self._ec2_instance_ids:
            from chia.aws.ec2 import terminate_ec2_instances
            terminate_ec2_instances(self._ec2_instance_ids, region=self._aws_region)
            self._ec2_instance_ids.clear()

    # ------------------------------------------------------------------
    # Pool-level lifecycle
    # ------------------------------------------------------------------

    def provision_pool(
        self,
        pool_name: str,
        num_workers: int,
        backend: str = "docker",
        cpus_per_worker: int = 8,
        instance_type: str = "c5.4xlarge",
        instance_types: list[str] | None = None,
        instance_family: str | None = None,
        max_cost_per_hour: float | None = None,
        max_vcpus_per_instance: int = 96,
        aws_config_dict: dict | None = None,
    ) -> dict:
        """Provision a single worker pool independently.

        Args:
            pool_name: "build" or "sim" (must be in _pool_configs).
            num_workers: Number of workers to provision.
            backend: "docker" or "ec2".
            cpus_per_worker: CPUs per worker for instance planning.
            Other args: same as provision().

        Returns:
            Dict with pool status info.
        """
        if num_workers <= 0:
            return {"pool_name": pool_name, "num_workers": 0}

        if pool_name not in self._pool_configs:
            raise ValueError(f"Unknown pool: {pool_name}. Known: {list(self._pool_configs.keys())}")

        pool_cfg = self._pool_configs[pool_name]
        prefix = f"chia-{pool_name}-worker"

        t0 = time.time()
        self._baseline_resources = self._snapshot_resources()

        logger.info(f"Provisioning pool '{pool_name}': {num_workers} workers via {backend}")

        if backend == "docker":
            self._provision_docker_pool(pool_cfg, num_workers, prefix=prefix)
        elif backend == "ec2":
            self._provision_ec2_single_pool(
                pool_name, pool_cfg, num_workers, prefix,
                cpus_per_worker, instance_type, instance_types, instance_family,
                max_cost_per_hour, max_vcpus_per_instance, aws_config_dict or {},
            )
        else:
            raise ValueError(f"Unknown backend: {backend}")

        # Wait for this pool's resources
        target = {}
        for key, val in pool_cfg.resources.items():
            target[key] = target.get(key, 0) + num_workers * val
        self._wait_for_resources(target)

        elapsed = time.time() - t0
        logger.info(f"Pool '{pool_name}' ready: {num_workers} workers in {elapsed:.1f}s")

        return {
            "pool_name": pool_name,
            "num_workers": num_workers,
            "containers": list(self._pool_containers.get(pool_name, [])),
            "ec2_instances": list(self._pool_instances.get(pool_name, [])),
            "provision_time": elapsed,
        }

    def teardown_pool(self, pool_name: str) -> None:
        """Tear down a single worker pool, leaving others running."""
        containers = self._pool_containers.pop(pool_name, [])
        instances = self._pool_instances.pop(pool_name, [])

        if containers:
            logger.info(f"Tearing down pool '{pool_name}': {len(containers)} containers")
            for name in containers:
                _docker_run(f"docker stop {shlex.quote(name)}", check=False)
                _docker_run(f"docker rm -f {shlex.quote(name)}", check=False)
                if name in self._container_names:
                    self._container_names.remove(name)

        if instances:
            logger.info(f"Terminating pool '{pool_name}': {len(instances)} EC2 instances")
            from chia.aws.ec2 import terminate_ec2_instances
            terminate_ec2_instances(instances, region=self._aws_region)
            for iid in instances:
                if iid in self._ec2_instance_ids:
                    self._ec2_instance_ids.remove(iid)

    def pool_status(self, pool_name: str) -> dict:
        """Get status of a single pool."""
        containers = self._pool_containers.get(pool_name, [])
        instances = self._pool_instances.get(pool_name, [])
        pool_cfg = self._pool_configs.get(pool_name)

        # Check Ray resources for this pool's resource type
        ray_resources = {}
        try:
            import ray
            if ray.is_initialized():
                current = ray.cluster_resources()
                if pool_cfg:
                    for key in pool_cfg.resources:
                        ray_resources[key] = current.get(key, 0)
        except Exception:
            pass

        return {
            "pool_name": pool_name,
            "num_containers": len(containers),
            "num_instances": len(instances),
            "containers": list(containers),
            "ec2_instances": list(instances),
            "ray_resources": ray_resources,
            "active": bool(containers or instances),
        }

    # ------------------------------------------------------------------
    # Docker backend
    # ------------------------------------------------------------------

    def _provision_docker_pool(
        self, pool: WorkerPoolConfig, count: int, prefix: str,
    ) -> None:
        """Create local Docker containers for one worker pool."""
        if count <= 0:
            return

        resources_json = json.dumps(pool.resources)

        for i in range(count):
            # Use unique suffix to avoid collisions with concurrent provisioners
            name = f"{prefix}-{self._session_id}-{i}"
            _docker_run(f"docker rm -f {shlex.quote(name)}", check=False)

            mount_flags = ""
            if self.chia_source_path:
                mount_flags = f"-v {self.chia_source_path}:{self.chia_source_path}:ro"
            for host_path, container_path in pool.extra_mounts:
                mount_flags += f" -v {host_path}:{container_path}"

            _docker_run(
                f"docker run -d --name {shlex.quote(name)} "
                f"--net=host --shm-size={self.shm_size} "
                f"--ulimit nofile=65536:65536 "
                f"{mount_flags} "
                f"{shlex.quote(pool.image)} sleep infinity"
            )
            # Track in both pool-level and flat lists
            pool_name = next((k for k, v in self._pool_configs.items() if v is pool), "unknown")
            self._pool_containers.setdefault(pool_name, []).append(name)
            self._container_names.append(name)

            try:
                script = self._build_ray_start_script(pool, resources_json, worker_index=i)
                _docker_exec(name, script, timeout=300)
            except Exception as e:
                logger.error(f"Failed to start Ray in {name}: {e}")
                _docker_run(f"docker rm -f {shlex.quote(name)}", check=False)
                self._pool_containers[pool_name].remove(name)
                self._container_names.remove(name)
                raise

            logger.info(f"Started {name} (image: {pool.image}, resources: {pool.resources})")

    def _build_ray_start_script(self, pool: WorkerPoolConfig, resources_json: str, worker_index: int = 0) -> str:
        """Build the shell script that starts a Ray worker inside a container."""
        lines = ["set -e"]

        if pool.needs_chipyard_env:
            lines.append(f"source {pool.chipyard_env_script} 2>/dev/null || true")

        # Find or create a Python env with Ray
        lines.append(
            f"if [ -d /home/ray/anaconda3/envs/py_worker/bin ]; then "
            f"  export PATH=/home/ray/anaconda3/envs/py_worker/bin:$PATH; "
            f"elif [ -d /home/ray/anaconda3/envs/{self.conda_env}/bin ]; then "
            f"  export PATH=/home/ray/anaconda3/envs/{self.conda_env}/bin:$PATH; "
            f"elif command -v conda &>/dev/null; then "
            f"  conda create -n {self.conda_env} python={self._host_python_version()} -y -q && "
            f"  /home/ray/anaconda3/envs/{self.conda_env}/bin/pip install -q "
            f"    'ray[default]=={self._host_ray_version()}' pyyaml && "
            f"  export PATH=/home/ray/anaconda3/envs/{self.conda_env}/bin:$PATH; "
            f"elif command -v ray &>/dev/null; then "
            f"  true; "  # ray already on PATH (e.g. verilator-run image with ray pre-installed)
            f"else "
            f"  pip install -q 'ray[default]=={self._host_ray_version()}' pyyaml; "
            f"fi"
        )

        if self.chia_source_path:
            lines.append(f"export PYTHONPATH={self.chia_source_path}:${{PYTHONPATH:-}}")

        # Unique worker port ranges per container to avoid conflicts on --net=host
        # Start at 20000+ to avoid Ray's auto-picked management ports (~10000-10100)
        min_worker_port = 20000 + worker_index * 1000
        max_worker_port = min_worker_port + 999
        lines.append(
            f"ray start --address={self.ray_address} "
            f"--min-worker-port={min_worker_port} "
            f"--max-worker-port={max_worker_port} "
            f"--resources='{resources_json}'"
        )

        return "\n".join(lines)

    @staticmethod
    def _host_python_version() -> str:
        import sys
        return f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"

    @staticmethod
    def _host_ray_version() -> str:
        import ray
        return ray.__version__

    # ------------------------------------------------------------------
    # EC2 backend
    # ------------------------------------------------------------------

    @staticmethod
    def _find_instance_type_chain(
        family: str,
        min_vcpus: int,
        region: str = "us-east-1",
        max_vcpus: int = 96,
        max_cost_per_hour: float | None = None,
    ) -> list[tuple[str, int, float]]:
        """Build a ranked fallback chain of instance types in a family.

        Returns types sorted by cost-per-vCPU (cheapest first), with
        tie-break on total vCPUs descending (prefer fewer, bigger instances).
        Filters out metal instances and those outside [min_vcpus, max_vcpus].

        Args:
            family: Instance family (e.g. "c5", "m5", "c5a").
            min_vcpus: Minimum vCPUs needed per instance.
            max_vcpus: Maximum vCPUs (cap to avoid overly large instances).
            region: AWS region.
            max_cost_per_hour: If set, exclude types with spot price above this.

        Returns:
            List of (instance_type, vcpus, spot_price) sorted by cost-per-vCPU.
        """
        import boto3
        ec2 = boto3.client("ec2", region_name=region)

        paginator = ec2.get_paginator("describe_instance_types")
        candidates = []
        for page in paginator.paginate(
            Filters=[{"Name": "instance-type", "Values": [f"{family}.*"]}],
        ):
            for it in page["InstanceTypes"]:
                vcpus = it["VCpuInfo"]["DefaultVCpus"]
                name = it["InstanceType"]
                if vcpus >= min_vcpus and vcpus <= max_vcpus and "metal" not in name:
                    candidates.append((name, vcpus))

        if not candidates:
            raise RuntimeError(f"No {family}.* instance types found with {min_vcpus}-{max_vcpus} vCPUs")

        # Get spot prices for candidates
        spot_resp = ec2.describe_spot_price_history(
            InstanceTypes=[c[0] for c in candidates],
            ProductDescriptions=["Linux/UNIX"],
            MaxResults=len(candidates) * 6,  # up to 6 AZs
        )

        # Find cheapest spot price per instance type (min across AZs)
        prices: dict[str, float] = {}
        for sp in spot_resp["SpotPriceHistory"]:
            it = sp["InstanceType"]
            price = float(sp["SpotPrice"])
            if it not in prices or price < prices[it]:
                prices[it] = price

        vcpu_map = dict(candidates)

        if not prices:
            # No spot data — sort by vCPU descending (prefer larger)
            candidates.sort(key=lambda x: -x[1])
            return [(name, vcpus, 0.0) for name, vcpus in candidates]

        # Apply cost cap
        if max_cost_per_hour is not None:
            prices = {it: p for it, p in prices.items() if p <= max_cost_per_hour}
            if not prices:
                raise RuntimeError(
                    f"All {family}.* types exceed ${max_cost_per_hour}/hr spot cap"
                )

        # Sort by cost-per-vCPU, tie-break on vCPUs descending
        ranked = sorted(
            prices.keys(),
            key=lambda it: (prices[it] / vcpu_map.get(it, 1), -vcpu_map.get(it, 0)),
        )

        chain = [(it, vcpu_map[it], prices[it]) for it in ranked if it in vcpu_map]
        for it, vc, pr in chain:
            logger.info(f"  {it:20s}  {vc:3d} vCPUs  ${pr:.4f}/hr  (${pr/vc:.5f}/vCPU/hr)")
        logger.info(f"Preferred: {chain[0][0]} ({chain[0][1]} vCPUs, ~${chain[0][2]:.3f}/hr)")
        return chain

    def _resolve_instance_chain(
        self,
        instance_type: str,
        instance_types: list[str] | None,
        instance_family: str | None,
        cpus_per_worker: int,
        max_cost_per_hour: float | None,
        max_vcpus: int,
        region: str,
    ) -> list[tuple[str, int, float]]:
        """Resolve the instance selection config into a ranked fallback chain.

        Priority:
          1. instance_types list — user-ordered, try each in sequence
          2. instance_family — auto-discover from AWS, sort by cost/vCPU
          3. instance_type (exact) — single type, no fallback

        Returns:
            List of (instance_type, vcpus, spot_price) to try in order.
        """
        if instance_types:
            # User-provided prioritized list — look up vCPUs for each
            chain = []
            for it in instance_types:
                vcpus = _EC2_VCPUS.get(it, 0)
                if vcpus == 0:
                    # Try to look up dynamically
                    try:
                        import boto3
                        ec2 = boto3.client("ec2", region_name=region)
                        resp = ec2.describe_instance_types(InstanceTypes=[it])
                        if resp["InstanceTypes"]:
                            vcpus = resp["InstanceTypes"][0]["VCpuInfo"]["DefaultVCpus"]
                    except Exception:
                        vcpus = 16  # safe fallback
                chain.append((it, vcpus, 0.0))
            logger.info(f"Using user-provided instance type list: {[c[0] for c in chain]}")
            return chain

        if instance_family:
            min_vcpus = cpus_per_worker  # at least 1 worker per instance
            chain = self._find_instance_type_chain(
                instance_family, min_vcpus, region,
                max_vcpus=max_vcpus, max_cost_per_hour=max_cost_per_hour,
            )
            logger.info(f"Auto-selected fallback chain from {instance_family}: {[c[0] for c in chain]}")
            return chain

        # Single exact type, no fallback
        vcpus = _EC2_VCPUS.get(instance_type, 16)
        return [(instance_type, vcpus, 0.0)]

    @staticmethod
    def _compute_instance_plan(
        num_build: int, num_sim: int,
        vcpus_per_instance: int, sim_cpus: int = 8,
    ) -> tuple[int, int, int]:
        """Compute how many instances are needed for build + sim workers.

        Build workers get dedicated instances (use all cores for make -jN).
        Sim workers are packed onto shared instances.

        Returns:
            (num_build_instances, num_sim_instances, sims_per_instance)
        """
        sims_per_instance = max(1, vcpus_per_instance // sim_cpus)
        num_build_instances = num_build  # 1:1
        num_sim_instances = math.ceil(num_sim / sims_per_instance) if num_sim > 0 else 0
        return num_build_instances, num_sim_instances, sims_per_instance

    def _provision_ec2_single_pool(
        self,
        pool_name: str,
        pool_cfg: WorkerPoolConfig,
        num_workers: int,
        prefix: str,
        cpus_per_worker: int,
        instance_type: str,
        instance_types: list[str] | None,
        instance_family: str | None,
        max_cost_per_hour: float | None,
        max_vcpus_per_instance: int,
        aws_config_dict: dict,
    ) -> None:
        """Launch EC2 instances for a single worker pool.

        Delegates to _provision_ec2_pools with the other pool count set to 0.
        """
        if pool_name == "build":
            self._provision_ec2_pools(
                num_workers, 0,
                instance_type, instance_types, instance_family,
                None, cpus_per_worker,
                cpus_per_worker, cpus_per_worker,
                max_cost_per_hour, max_vcpus_per_instance,
                aws_config_dict,
            )
        else:
            self._provision_ec2_pools(
                0, num_workers,
                instance_type, instance_types, instance_family,
                None, cpus_per_worker,
                cpus_per_worker, cpus_per_worker,
                max_cost_per_hour, max_vcpus_per_instance,
                aws_config_dict,
            )

    def _provision_ec2_pools(
        self,
        num_build_workers: int,
        num_sim_workers: int,
        instance_type: str,
        instance_types: list[str] | None,
        instance_family: str | None,
        workers_per_instance: int | None,
        cpus_per_worker: int,
        build_cpus: int,
        sim_cpus: int,
        max_cost_per_hour: float | None,
        max_vcpus_per_instance: int,
        aws_config_dict: dict,
    ) -> None:
        """Launch EC2 instances with both build and sim containers.

        Tries each instance type in the fallback chain. For each type, tries
        all AZs before moving to the next type. This maximizes the chance of
        getting capacity while preferring the most cost-efficient option.
        """
        from chia.aws.config import AWSConfig, EC2InstanceConfig
        from chia.aws.ec2 import launch_ec2_instances, wait_for_instances
        from chia.aws.host import EphemeralEC2Host

        aws_config = AWSConfig(**aws_config_dict)
        self._aws_region = aws_config.region

        # Build the instance type fallback chain
        type_chain = self._resolve_instance_chain(
            instance_type, instance_types, instance_family,
            cpus_per_worker, max_cost_per_hour, max_vcpus_per_instance,
            aws_config.region,
        )

        # Use the preferred type's vCPUs for planning
        chosen_type, chosen_vcpus, chosen_price = type_chain[0]

        # Compute total vCPUs needed, then how many instances
        build_vcpus_needed = num_build_workers * build_cpus
        sim_vcpus_needed = num_sim_workers * sim_cpus
        total_vcpus_needed = build_vcpus_needed + sim_vcpus_needed

        if workers_per_instance is not None:
            # User override — pack exactly this many per instance
            total_workers = num_build_workers + num_sim_workers
            num_instances = math.ceil(total_workers / workers_per_instance)
        else:
            num_instances = max(1, math.ceil(total_vcpus_needed / chosen_vcpus))

        logger.info(
            f"Instance plan: {num_instances}x {chosen_type} ({chosen_vcpus} vCPUs) — "
            f"{num_build_workers} build workers ({build_cpus} cpus each) + "
            f"{num_sim_workers} sim workers ({sim_cpus} cpus each) = "
            f"{total_vcpus_needed} vCPUs needed"
        )
        if chosen_price > 0:
            est_cost = chosen_price * num_instances
            logger.info(f"Estimated cost: ~${est_cost:.2f}/hr ({chosen_type} x{num_instances} @ ${chosen_price:.3f}/hr)")

        # Discover all subnets in the VPC for AZ fallback
        fallback_subnets = self._discover_vpc_subnets(aws_config)

        # Launch instances one at a time with type + AZ fallback.
        t_launch_start = time.time()
        all_instances = []
        for i in range(num_instances):
            inst = self._launch_one_instance_with_fallback(
                type_chain, fallback_subnets, aws_config, aws_config_dict,
                label=f"{i+1}/{num_instances}",
            )
            if inst is None:
                logger.error(f"Could not launch instance {i+1}/{num_instances}")
                break
            all_instances.append(inst)

        t_launched = time.time()
        logger.info("TIMING: ec2 launch_instances=%.1fs (%d instances)", t_launched - t_launch_start, len(all_instances))

        self._ec2_instance_ids = [i.instance_id for i in all_instances]
        # Track instances per pool (approximation: build instances first, then sim)
        num_build_instances_actual = min(num_build_workers, len(all_instances))
        for inst in all_instances[:num_build_instances_actual]:
            self._pool_instances.setdefault("build", []).append(inst.instance_id)
        for inst in all_instances[num_build_instances_actual:]:
            self._pool_instances.setdefault("sim", []).append(inst.instance_id)
        if len(all_instances) < num_instances:
            logger.error(f"Only launched {len(all_instances)}/{num_instances} instances")
            self.teardown()
            raise RuntimeError(f"Insufficient EC2 capacity: only {len(all_instances)}/{num_instances}")

        try:
            ready_instances = wait_for_instances(self._ec2_instance_ids, region=aws_config.region)
        except Exception:
            logger.error("EC2 instances failed to start, terminating")
            self.teardown()
            raise
        t_ready = time.time()
        logger.info("TIMING: ec2 wait_for_instances=%.1fs", t_ready - t_launched)

        # Bin-pack workers onto instances by CPU count.
        # Track remaining vCPUs per instance, assign greedily.
        worker_assignments: list[list[tuple[WorkerPoolConfig, str]]] = [[] for _ in ready_instances]
        remaining_vcpus = [chosen_vcpus] * len(ready_instances)

        def _assign(pool, name, cpus):
            for idx in range(len(remaining_vcpus)):
                if remaining_vcpus[idx] >= cpus:
                    worker_assignments[idx].append((pool, name))
                    remaining_vcpus[idx] -= cpus
                    return
            # Shouldn't happen if instance count is correct, but fall back to least-loaded
            idx = max(range(len(remaining_vcpus)), key=lambda i: remaining_vcpus[i])
            worker_assignments[idx].append((pool, name))
            remaining_vcpus[idx] -= cpus

        for i in range(num_build_workers):
            _assign(self.build_pool, f"chia-build-worker-{self._session_id}-{i}", build_cpus)
        for i in range(num_sim_workers):
            _assign(self.sim_pool, f"chia-sim-worker-{self._session_id}-{i}", sim_cpus)

        for idx, assigns in enumerate(worker_assignments):
            if assigns:
                names = [n for _, n in assigns]
                logger.info(f"  Instance {idx}: {names} ({chosen_vcpus - remaining_vcpus[idx]}/{chosen_vcpus} vCPUs used)")

        # Set up all hosts in parallel — each host's setup is independent
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _setup_one_host(idx, inst):
            t_host_start = time.time()
            host = EphemeralEC2Host(inst, aws_config)
            host.wait_ready(timeout=600)
            t_ssh = time.time()
            logger.info("TIMING: ec2 [%s] ssh_ready=%.1fs", inst.instance_id, t_ssh - t_host_start)

            # Push credentials
            aws_creds_dir = os.path.expanduser("~/.aws")
            if os.path.isdir(aws_creds_dir):
                host.run("mkdir -p ~/.aws", timeout=10)
                host.rsync_up(f"{aws_creds_dir}/", "/home/ubuntu/.aws/")

            # Install Docker — wait for unattended-upgrades to release apt lock first
            logger.info(f"[{inst.instance_id}] Ensuring Docker is installed")
            host.run(
                "which docker || ("
                "echo 'Waiting for apt lock...'; "
                "for i in $(seq 1 30); do "
                "  sudo fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1 || break; "
                "  sleep 5; "
                "done && "
                "sudo apt-get update -qq && "
                "sudo apt-get install -y -qq docker.io && "
                "sudo systemctl start docker && "
                "sudo usermod -aG docker ubuntu"
                ")",
                timeout=300,
            )
            host.run("sudo docker info > /dev/null", timeout=30)
            t_docker = time.time()
            logger.info("TIMING: ec2 [%s] docker_install=%.1fs", inst.instance_id, t_docker - t_ssh)

            # Authenticate to ghcr.io
            github_token_path = os.path.expanduser("~/.config/chia/github-token")
            if os.path.exists(github_token_path):
                host.run("mkdir -p ~/.config/chia", timeout=10)
                host.rsync_up(github_token_path, "~/.config/chia/github-token")
                host.run(
                    "cat ~/.config/chia/github-token | "
                    "sudo docker login ghcr.io -u chia --password-stdin",
                    timeout=30, check=False,
                )

            # Pull all unique images needed on this instance
            images_needed = set(pool.image for pool, _ in worker_assignments[idx])
            for image in images_needed:
                logger.info(f"[{inst.instance_id}] Pulling {image}")
                host.run(f"sudo docker pull {shlex.quote(image)}", timeout=1200)
            t_pull = time.time()
            logger.info("TIMING: ec2 [%s] image_pull=%.1fs", inst.instance_id, t_pull - t_docker)

            # Rsync chia source
            if self.chia_source_path:
                host.run(f"mkdir -p {self.chia_source_path}", timeout=10)
                host.rsync_up(
                    f"{self.chia_source_path}/",
                    f"{self.chia_source_path}/",
                    exclude=[".git", "__pycache__", "*.pyc"],
                )

            # Start containers
            for wi, (pool, name) in enumerate(worker_assignments[idx]):
                t_container_start = time.time()
                mount_flags = ""
                if self.chia_source_path:
                    mount_flags = f"-v {self.chia_source_path}:{self.chia_source_path}:ro"
                if pool.extra_mounts:
                    logger.warning(
                        f"extra_mounts are not supported on EC2 (files don't exist on remote). "
                        f"Use a Docker image with the files baked in instead."
                    )

                host.run(
                    f"sudo docker run -d --name {shlex.quote(name)} "
                    f"--net=host --shm-size={self.shm_size} "
                    f"--ulimit nofile=65536:65536 "
                    f"{mount_flags} "
                    f"{shlex.quote(pool.image)} sleep infinity",
                    timeout=60,
                )

                resources_json = json.dumps(pool.resources)
                self._setup_worker_in_container_ec2(host, inst.instance_id, name, pool, resources_json, worker_index_on_host=wi)
                logger.info("TIMING: ec2 [%s] container_%s=%.1fs", inst.instance_id, name, time.time() - t_container_start)

            t_host_done = time.time()
            logger.info(
                "TIMING: ec2 [%s] total_host_setup=%.1fs (ssh=%.1f docker=%.1f pull=%.1f containers=%.1f)",
                inst.instance_id, t_host_done - t_host_start,
                t_ssh - t_host_start, t_docker - t_ssh, t_pull - t_docker, t_host_done - t_pull,
            )

        errors = []
        with ThreadPoolExecutor(max_workers=len(ready_instances)) as pool:
            futures = {
                pool.submit(_setup_one_host, idx, inst): inst.instance_id
                for idx, inst in enumerate(ready_instances)
            }
            for future in as_completed(futures):
                iid = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Host setup failed for {iid}: {e}")
                    errors.append((iid, e))

        if errors:
            succeeded = len(ready_instances) - len(errors)
            logger.warning(
                f"{len(errors)}/{len(ready_instances)} host setups failed, "
                f"{succeeded} succeeded"
            )
            if succeeded == 0:
                self.teardown()
                raise RuntimeError(f"All host setups failed: {errors[0][1]}")
            # Continue with partial success — some workers are better than none

    def _launch_one_instance_with_fallback(
        self,
        type_chain: list[tuple[str, int, float]],
        fallback_subnets: list[str],
        aws_config,
        aws_config_dict: dict,
        label: str = "",
    ):
        """Try to launch 1 instance, walking the type chain and all AZs.

        For each instance type in the chain, tries every AZ. If all AZs fail
        for that type (capacity or unsupported), moves to the next type.

        Subnets that have previously succeeded are tried first to minimize
        wasted API calls on known-bad AZs.

        Returns:
            An EC2 instance object, or None if all types exhausted.
        """
        from chia.aws.config import AWSConfig, EC2InstanceConfig
        from chia.aws.ec2 import launch_ec2_instances

        base_subnets = [aws_config.subnet_id] if aws_config.subnet_id else []
        base_subnets.extend(s for s in fallback_subnets if s not in base_subnets)

        # Prioritize subnets that previously succeeded
        good = [s for s in self._successful_subnets if s in base_subnets]
        rest = [s for s in base_subnets if s not in self._successful_subnets]
        subnets_to_try = good + rest

        for it_type, it_vcpus, it_price in type_chain:
            inst_config = EC2InstanceConfig(
                instance_type=it_type,
                volume_size_gb=200,
                ami_id=aws_config_dict.get("ami_id"),
                market="spot",
                tags={"chia-cluster": "evolve", "chia-op": "worker"},
            )

            for subnet in subnets_to_try:
                try:
                    trial_config = AWSConfig(
                        region=aws_config.region,
                        key_name=aws_config.key_name,
                        vpc_name=aws_config.vpc_name,
                        security_group_name=aws_config.security_group_name,
                        ssh_user=aws_config.ssh_user,
                        ssh_private_key=aws_config.ssh_private_key,
                        use_public_ip=aws_config.use_public_ip,
                        s3_bucket=aws_config.s3_bucket,
                        subnet_id=subnet,
                    )
                    insts = launch_ec2_instances(trial_config, inst_config, count=1)
                    logger.info(f"Instance {label} launched: {it_type} in {subnet}")
                    self._successful_subnets.add(subnet)
                    return insts[0]
                except Exception as e:
                    err = str(e)
                    if "InsufficientInstanceCapacity" in err or "Unsupported" in err or "no Spot capacity" in err.lower():
                        logger.warning(f"Cannot launch {it_type} in {subnet}: {err.split(':')[-1].strip()[:100]}")
                        continue
                    raise

            logger.warning(f"Exhausted all AZs for {it_type}, trying next type in chain")

        logger.error(f"All instance types exhausted for instance {label}")
        return None

    def _setup_worker_in_container_ec2(
        self, host, instance_id: str, container_name: str,
        pool: WorkerPoolConfig, resources_json: str,
        worker_index_on_host: int = 0,
    ) -> None:
        """Set up conda env + Ray inside a container on an EC2 instance."""
        def _dexec(cmd: str, timeout: int = 300) -> None:
            host.run(
                f"sudo docker exec {shlex.quote(container_name)} bash -lc {shlex.quote(cmd)}",
                timeout=timeout,
            )

        if pool.needs_chipyard_env:
            _dexec(f"source {pool.chipyard_env_script} 2>/dev/null || true", timeout=30)

        # Check for pre-baked env
        check = host.run(
            f"sudo docker exec {shlex.quote(container_name)} "
            f"test -x /home/ray/anaconda3/envs/py_worker/bin/ray && echo yes || echo no",
            timeout=15, check=False,
        )
        if "yes" in check.stdout:
            env_bin = "/home/ray/anaconda3/envs/py_worker/bin"
        else:
            check2 = host.run(
                f"sudo docker exec {shlex.quote(container_name)} "
                f"bash -lc 'command -v ray && echo yes || echo no'",
                timeout=15, check=False,
            )
            if "yes" in check2.stdout:
                # Ray already on PATH (e.g. verilator-run image with ray pre-installed)
                env_bin = None
            else:
                # Create conda env from scratch
                logger.info(f"[{instance_id}] Creating conda env in {container_name}")
                _dexec(
                    f"conda create -n {self.conda_env} python={self._host_python_version()} -y -q",
                    timeout=300,
                )
                _dexec(
                    f"/home/ray/anaconda3/envs/{self.conda_env}/bin/pip install -q "
                    f"'ray[default]=={self._host_ray_version()}' pyyaml",
                    timeout=600,
                )
                env_bin = f"/home/ray/anaconda3/envs/{self.conda_env}/bin"

        # Start Ray worker
        path_export = f"export PATH={env_bin}:$PATH && " if env_bin else ""
        pythonpath_export = ""
        if self.chia_source_path:
            pythonpath_export = f"export PYTHONPATH={self.chia_source_path}:${{PYTHONPATH:-}} && "
        env_source = f"source {pool.chipyard_env_script} 2>/dev/null || true && " if pool.needs_chipyard_env else ""
        ray_bin = f"{env_bin}/ray" if env_bin else "ray"

        # Each worker on the same host needs unique worker port ranges to avoid
        # conflicts when using --net=host (shared network namespace).
        # Start at 20000+ to avoid Ray's auto-picked management ports (~10000-10100).
        min_worker_port = 20000 + worker_index_on_host * 1000
        max_worker_port = min_worker_port + 999

        logger.info(f"[{instance_id}] Starting Ray worker in {container_name} (worker ports {min_worker_port}-{max_worker_port})")
        _dexec(
            f"{env_source}{path_export}{pythonpath_export}"
            f"{ray_bin} start --address={self.ray_address} "
            f"--min-worker-port={min_worker_port} "
            f"--max-worker-port={max_worker_port} "
            f"--resources='{resources_json}'",
            timeout=60,
        )
        logger.info(f"[{instance_id}] Started worker {container_name}")

    # ------------------------------------------------------------------
    # Readiness check
    # ------------------------------------------------------------------

    def _wait_for_resources(self, target: dict[str, float], timeout: int = 300) -> None:
        """Block until Ray cluster has the expected resource delta."""
        import ray

        logger.info(f"Waiting for Ray resources (delta): {target}")
        deadline = time.time() + timeout
        baseline = self._baseline_resources

        while time.time() < deadline:
            try:
                if not ray.is_initialized():
                    ray.init(address="auto", ignore_reinit_error=True)
                current = ray.cluster_resources()
                all_met = True
                for key, needed in target.items():
                    delta = current.get(key, 0) - baseline.get(key, 0)
                    if delta < needed - 0.5:
                        all_met = False
                        break
                if all_met:
                    logger.info("All workers registered with Ray")
                    return
            except Exception:
                pass
            time.sleep(2)

        logger.warning(f"Timed out waiting for worker resources after {timeout}s")

    # ------------------------------------------------------------------
    # Auto-detect worker count
    # ------------------------------------------------------------------

    @staticmethod
    def _discover_vpc_subnets(aws_config) -> list[str]:
        """Auto-discover all subnets in the VPC for AZ fallback."""
        try:
            import boto3
            ec2 = boto3.client("ec2", region_name=aws_config.region)

            # Find the VPC
            if aws_config.subnet_id:
                resp = ec2.describe_subnets(SubnetIds=[aws_config.subnet_id])
                vpc_id = resp["Subnets"][0]["VpcId"]
            else:
                vpcs = ec2.describe_vpcs(Filters=[{"Name": "tag:Name", "Values": [aws_config.vpc_name]}])
                if vpcs["Vpcs"]:
                    vpc_id = vpcs["Vpcs"][0]["VpcId"]
                else:
                    return []

            # Get all subnets in this VPC
            resp = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
            subnets = [s["SubnetId"] for s in resp["Subnets"]]
            logger.info(f"Discovered {len(subnets)} subnets in VPC {vpc_id} for AZ fallback")
            return subnets
        except Exception as e:
            logger.warning(f"Could not discover VPC subnets: {e}")
            return []

    @staticmethod
    def auto_sim_worker_count(cpus_per_worker: int = 8, max_workers: int = 8) -> int:
        """Suggest sim worker count based on available CPUs."""
        try:
            total_cpus = os.cpu_count() or 4
        except Exception:
            total_cpus = 4
        # Reserve CPUs for 1 build worker + head node overhead
        usable = max(1, total_cpus - cpus_per_worker - 2)
        count = usable // cpus_per_worker
        return max(1, min(count, max_workers))


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _docker_run(cmd: str, check: bool = True, timeout: int = 120) -> subprocess.CompletedProcess:
    result = subprocess.run(
        ["sudo", "bash", "-c", cmd],
        capture_output=True, text=True, timeout=timeout,
    )
    if check and result.returncode != 0:
        raise RuntimeError(f"Docker command failed: {cmd}\n{result.stderr}")
    return result


def _docker_exec(container_name: str, script: str, timeout: int = 120) -> None:
    cmd = f"docker exec {shlex.quote(container_name)} bash -lc {shlex.quote(script)}"
    result = subprocess.run(
        ["sudo", "bash", "-c", cmd],
        capture_output=True, text=True, timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Docker exec failed in {container_name}:\n"
            f"stdout: {result.stdout[-500:]}\n"
            f"stderr: {result.stderr[-500:]}"
        )
