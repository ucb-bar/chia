"""Write FireSim's manager config files and stage a workload for one run.

The manager is driven through the config files it already reads, so FireSim's
own code is unchanged. The run image ships FireSim's sample configs; these
functions patch them.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile

import yaml

from chia.chipyard.state_def import FireMarshalArtifact
from chia.cluster.log import get_logger
from chia.firesim.fs_bitstream import FSBitstream
from chia.firesim.state_def import RunConfig

logger = get_logger("firesim.render")

HW_CONFIG_NAME = "chia_hwdb"
# The F2 instance itself is the run farm host. FireSim names it `user@host`, and
# _MANAGER (manager_node.py) runs its commands there as that user: the AMI's
# `ubuntu`, which owns the FPGA tooling.
RUN_FARM_USER = "ubuntu"
RUN_FARM_HOST = f"{RUN_FARM_USER}@localhost"

# The one FPGA of this instance, already up: no run farm to launch or terminate.
_RUN_FARM = {
    "base_recipe": "run-farm-recipes/externally_provisioned.yaml",
    "recipe_arg_overrides": {
        "default_platform": "EC2InstanceDeployManager",
        "default_simulation_dir": f"/home/{RUN_FARM_USER}",
        "run_farm_hosts_to_use": [{RUN_FARM_HOST: "one_fpga_spec"}],
    },
}


def render_runtime_config(deploy_dir: str, workload_name: str,
                          config: RunConfig | None = None) -> str:
    """Set ``config`` in ``config_runtime.yaml``, then point it at the local FPGA
    and ``workload_name``; return its path. Keys not set keep their value in the
    file."""
    path = os.path.join(deploy_dir, "config_runtime.yaml")
    with open(path) as f:
        runtime = yaml.safe_load(f)
    for section, values in (config.sections if config else {}).items():
        runtime.setdefault(section, {}).update(values)
    runtime["run_farm"] = _RUN_FARM
    runtime["target_config"]["default_hw_config"] = HW_CONFIG_NAME
    runtime["workload"]["workload_name"] = f"{workload_name}.json"
    return _dump(path, runtime)


def render_hwdb(deploy_dir: str, bitstream: FSBitstream) -> str:
    """Write ``config_hwdb.yaml`` for the bitstream."""
    return _dump(os.path.join(deploy_dir, "config_hwdb.yaml"),
                 bitstream.to_hwdb(HW_CONFIG_NAME, deploy_dir))


def stage_workload(deploy_dir: str, workload: FireMarshalArtifact) -> str:
    """Unpack a single-job workload into ``deploy/workloads/``; return its name.

    FireSim reads the descriptor at ``workloads/<name>.json`` and the rootfs and
    boot binary it names from ``workloads/<name>/``, as plain files.
    """
    workloads = os.path.join(deploy_dir, "workloads")
    os.makedirs(workloads, exist_ok=True)
    staging = tempfile.mkdtemp(dir=workloads)
    descriptor = workload.unpack(staging)
    name = descriptor["benchmark_name"]
    with open(os.path.join(workloads, f"{name}.json"), "w") as f:
        json.dump(descriptor, f)
    shutil.rmtree(os.path.join(workloads, name), ignore_errors=True)
    os.rename(staging, os.path.join(workloads, name))
    logger.info(f"Staged workload {name} in {workloads}")
    return name


def _dump(path: str, config: dict) -> str:
    with open(path, "w") as f:
        yaml.safe_dump(config, f, sort_keys=False)
    return path
