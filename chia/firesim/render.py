"""Render FireSim manager config files and stage a job's workload files.

The manager is driven entirely through the config files it already reads, so
none of FireSim's own code is modified. ``firesim managerinit`` is deliberately
not used: on ``f2`` it calls ``awsinit()``, which loops on ``aws configure`` and
prompts for an email address, and it only copies sample configs anyway.
"""

from __future__ import annotations

import copy
import json
import os
import re
import shutil

import yaml

from chia.cluster.log import get_logger
from chia.firesim.fs_bitstream import FSBitstream
from chia.firesim.state_def import RunConfig, SimJob

logger = get_logger("firesim.render")

_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")

HW_CONFIG_NAME = "chia_hwdb"
# --net=host puts the container in the host's network namespace, so "localhost"
# is the F2 instance, and the account that owns the FPGA tooling there is the
# AMI's `ubuntu`. Fabric parses `user@host` out of the run farm host string.
RUN_FARM_HOST = "ubuntu@localhost"

# One FPGA that is already up and reachable over localhost: no run farm to
# launch, no hosts to terminate.
_RUN_FARM = {
    "base_recipe": "run-farm-recipes/externally_provisioned.yaml",
    "recipe_arg_overrides": {
        "default_platform": "EC2InstanceDeployManager",
        "default_simulation_dir": "/home/ubuntu",
        "run_farm_hosts_to_use": [{RUN_FARM_HOST: "one_fpga_spec"}],
    },
}

_METASIM = {
    "metasimulation_enabled": False,
    "metasimulation_host_simulator": "verilator",
    "metasimulation_only_plusargs": "+fesvr-step-size=128 +max-cycles=100000000",
    "metasimulation_only_vcs_plusargs": "+vcs+initreg+0 +vcs+initmem+0",
}

# Used only when the node has no config_runtime.yaml yet. The FireSim image
# ships one (the sample managerinit would have copied), so normally the file on
# the node is the baseline and we only patch it.
_BASELINE = {
    "target_config": {
        "topology": "no_net_config",
        "no_net_num_nodes": 1,
        "link_latency": 6405,
        "switching_latency": 10,
        "net_bandwidth": 200,
        "profile_interval": -1,
        "plusarg_passthrough": "",
    },
    "tracing": {"enable": False, "output_format": 0, "selector": 1,
                "start": 0, "end": -1},
    "autocounter": {"read_rate": 0},
    "host_debug": {"zero_out_dram": False, "disable_synth_asserts": False},
    "synth_print": {"start": 0, "end": -1, "cycle_prefix": True},
}

# RunConfig field -> (config_runtime.yaml section, key).
_KNOBS = {
    "plusarg_passthrough": ("target_config", "plusarg_passthrough"),
    "profile_interval": ("target_config", "profile_interval"),
    "trace_enable": ("tracing", "enable"),
    "trace_output_format": ("tracing", "output_format"),
    "trace_selector": ("tracing", "selector"),
    "trace_start": ("tracing", "start"),
    "trace_end": ("tracing", "end"),
    "autocounter_read_rate": ("autocounter", "read_rate"),
    "zero_out_dram": ("host_debug", "zero_out_dram"),
    "disable_synth_asserts": ("host_debug", "disable_synth_asserts"),
    "print_start": ("synth_print", "start"),
    "print_end": ("synth_print", "end"),
    "print_cycle_prefix": ("synth_print", "cycle_prefix"),
}


def render_runtime_config(deploy_dir: str, job: SimJob,
                          config: RunConfig | None = None) -> str:
    """Patch ``config_runtime.yaml`` for this job and return its path.

    Read-modify-write, not generate: the file on the node is the baseline, so a
    hand-edit there survives. Only the fields this design owns and the
    :class:`RunConfig` fields that are set get rewritten.
    """
    path = os.path.join(deploy_dir, "config_runtime.yaml")
    if os.path.isfile(path):
        with open(path) as f:
            runtime = yaml.safe_load(f) or {}
    else:
        runtime = copy.deepcopy(_BASELINE)

    # Ours: these hold the one-local-FPGA design together.
    runtime["run_farm"] = _RUN_FARM
    runtime["metasimulation"] = _METASIM
    runtime.setdefault("target_config", {})["default_hw_config"] = HW_CONFIG_NAME
    runtime.setdefault("workload", {}).update({
        "workload_name": f"{job.benchmark_name}.json",
        # The run farm host is SimSplitter's, not FireSim's — it must not try
        # to tear down a machine it does not own.
        "terminate_on_completion": False,
    })

    for field, (section, key) in _KNOBS.items():
        value = getattr(config, field, None)
        if value is not None:
            runtime.setdefault(section, {})[key] = value

    return _dump(path, runtime)


def render_hwdb(deploy_dir: str, bitstream: FSBitstream) -> str:
    """Write ``config_hwdb.yaml`` for the bitstream."""
    return _dump(os.path.join(deploy_dir, "config_hwdb.yaml"),
                 bitstream.to_hwdb(HW_CONFIG_NAME, deploy_dir))


def stage_workload(deploy_dir: str, job: SimJob) -> str:
    """Fetch the job's rootfs and boot binary and write its workload JSON.

    FireSim reads workload images from ``deploy/workloads/<benchmark_name>/`` as
    plain local files; only ``driver_tar``/``bitstream_tar`` go through its URI
    machinery, so these are fetched here.
    """
    workloads = os.path.join(deploy_dir, "workloads")
    job_dir = os.path.join(workloads, job.benchmark_name)
    os.makedirs(job_dir, exist_ok=True)

    rootfs_name = os.path.basename(job.rootfs_uri)
    bootbinary_name = os.path.basename(job.bootbinary_uri)
    _fetch(job.rootfs_uri, os.path.join(job_dir, rootfs_name))
    _fetch(job.bootbinary_uri, os.path.join(job_dir, bootbinary_name))

    # Uniform form (no "workloads" list): one job, one rootfs, one binary.
    descriptor = {
        "benchmark_name": job.benchmark_name,
        "common_bootbinary": bootbinary_name,
        "common_rootfs": rootfs_name,
        "common_outputs": job.outputs,
        "common_simulation_outputs": job.simulation_outputs,
    }
    path = os.path.join(workloads, f"{job.benchmark_name}.json")
    with open(path, "w") as f:
        json.dump(descriptor, f, indent=2)
    logger.info(f"Staged workload {job.benchmark_name} in {job_dir}")
    return path


def _fetch(uri: str, dest: str) -> None:
    """Copy ``uri`` (any fsspec URI, or a plain path) to ``dest``, if absent."""
    if os.path.isfile(dest):
        logger.debug(f"Reusing {dest}")
        return
    logger.info(f"Fetching {uri} -> {dest}")
    if not _SCHEME.match(uri):
        shutil.copyfile(uri, dest)
        return
    # Only remote URIs need fsspec, which ships with FireSim, not with chia.
    from fsspec.core import url_to_fs

    fs, path = url_to_fs(uri)
    fs.get_file(path, dest)


def _dump(path: str, config: dict) -> str:
    with open(path, "w") as f:
        yaml.safe_dump(config, f, sort_keys=False)
    return path
