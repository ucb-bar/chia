"""Unit tests for FSBitstream and the FireSim config renderer.

No AWS, no FPGA: these check the files handed to the FireSim manager, which is
where a mistake is otherwise only visible after an F2 instance is running.
"""

import json
import os

import pytest
import yaml

from chia.firesim.fs_bitstream import FSBitstream
from chia.firesim.render import (
    HW_CONFIG_NAME,
    RUN_FARM_HOST,
    render_hwdb,
    render_runtime_config,
    stage_workload,
)
from chia.firesim.state_def import SimJob


def _bitstream(**kwargs) -> FSBitstream:
    args = {"quintuplet": "f2-firesim-FireSim-Rocket-DefaultF2Config",
            "agfi": "agfi-0123", "driver_uri": "s3://b/driver-bundle.tar.gz"}
    args.update(kwargs)
    return FSBitstream(**args)


def test_bitstream_rejects_agfi_and_tar_together():
    with pytest.raises(ValueError):
        _bitstream(bitstream_uri="s3://b/firesim.tar.gz")


def test_bitstream_rejects_neither_image():
    with pytest.raises(ValueError):
        _bitstream(agfi=None)


def test_hwdb_entry_uses_agfi_and_driver_tar(tmp_path):
    entry = _bitstream().to_hwdb(HW_CONFIG_NAME, str(tmp_path))[HW_CONFIG_NAME]
    assert entry["agfi"] == "agfi-0123"
    assert entry["driver_tar"] == "s3://b/driver-bundle.tar.gz"
    assert "bitstream_tar" not in entry
    assert entry["custom_runtime_config"] is None


def test_hwdb_materializes_bytes_to_files(tmp_path):
    bits = FSBitstream(quintuplet="q", bitstream_bytes=b"bits",
                       driver_bytes=b"driver")
    entry = bits.to_hwdb(HW_CONFIG_NAME, str(tmp_path))[HW_CONFIG_NAME]
    for key, expected in (("bitstream_tar", b"bits"), ("driver_tar", b"driver")):
        assert os.path.isfile(entry[key])
        with open(entry[key], "rb") as f:
            assert f.read() == expected


def test_runtime_config_targets_one_local_fpga(tmp_path):
    job = SimJob(benchmark_name="gcc", rootfs_uri="s3://b/gcc.img",
                 bootbinary_uri="s3://b/br-base-bin")
    config = yaml.safe_load(open(render_runtime_config(str(tmp_path), job)))

    overrides = config["run_farm"]["recipe_arg_overrides"]
    assert config["run_farm"]["base_recipe"].endswith("externally_provisioned.yaml")
    assert overrides["run_farm_hosts_to_use"] == [{RUN_FARM_HOST: "one_fpga_spec"}]
    assert config["target_config"]["no_net_num_nodes"] == 1
    assert config["target_config"]["default_hw_config"] == HW_CONFIG_NAME
    assert config["workload"]["workload_name"] == "gcc.json"
    # The run host belongs to SimSplitter, so the manager must not terminate it.
    assert config["workload"]["terminate_on_completion"] is False
    assert config["metasimulation"]["metasimulation_enabled"] is False


def test_render_hwdb_writes_named_entry(tmp_path):
    hwdb = yaml.safe_load(open(render_hwdb(str(tmp_path), _bitstream())))
    assert hwdb[HW_CONFIG_NAME]["agfi"] == "agfi-0123"


def test_stage_workload_fetches_images_and_writes_descriptor(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    (src / "gcc.img").write_bytes(b"rootfs")
    (src / "br-base-bin").write_bytes(b"bootbin")
    deploy = tmp_path / "deploy"
    deploy.mkdir()

    job = SimJob(benchmark_name="gcc",
                 rootfs_uri=str(src / "gcc.img"),
                 bootbinary_uri=str(src / "br-base-bin"),
                 outputs=["/output"])
    descriptor = json.load(open(stage_workload(str(deploy), job)))

    job_dir = deploy / "workloads" / "gcc"
    assert (job_dir / "gcc.img").read_bytes() == b"rootfs"
    assert (job_dir / "br-base-bin").read_bytes() == b"bootbin"
    # Uniform form: FireSim derives the single job from the common_* fields.
    assert descriptor["benchmark_name"] == "gcc"
    assert descriptor["common_rootfs"] == "gcc.img"
    assert descriptor["common_bootbinary"] == "br-base-bin"
    assert descriptor["common_outputs"] == ["/output"]
    assert "workloads" not in descriptor
