"""What chia hands the FireSim manager: the hwdb entry, the runtime config and
the staged workload. A mistake in any of them shows only on a running F2
instance.
"""

import io
import json
import tarfile

import yaml

from chia.chipyard.state_def import FireMarshalArtifact
from chia.firesim.fs_bitstream import FSBitstream
from chia.firesim.render import (HW_CONFIG_NAME, RUN_FARM_HOST, render_runtime_config,
                                 stage_workload)
from chia.firesim.state_def import RunConfig


def test_hwdb_entry_for_an_f2_bitstream(tmp_path):
    bitstream = FSBitstream("f2-firesim-FireSim-FireSimRocketConfig-BaseF2Config",
                            agfi="agfi-0123", driver_bytes=b"driver")

    entry = bitstream.to_hwdb(HW_CONFIG_NAME, str(tmp_path))[HW_CONFIG_NAME]

    assert entry["agfi"] == "agfi-0123"
    assert "bitstream_tar" not in entry
    assert entry["deploy_quintuplet_override"] == bitstream.quintuplet
    with open(entry["driver_tar"], "rb") as f:
        assert f.read() == b"driver"


def test_runtime_config_keeps_chias_fields_over_the_callers(tmp_path):
    (tmp_path / "config_runtime.yaml").write_text(yaml.safe_dump({
        "run_farm": {"base_recipe": "run-farm-recipes/aws_ec2.yaml"},
        "target_config": {"default_hw_config": "midasexamples_gcd"},
        "tracing": {"enable": False, "selector": 1},
        "workload": {"workload_name": "null.json"},
    }))
    config = RunConfig({"tracing": {"enable": True},
                        "run_farm": {"base_recipe": "run-farm-recipes/aws_ec2.yaml"}})

    runtime = yaml.safe_load(open(render_runtime_config(str(tmp_path), "gcc", config)))

    assert runtime["tracing"] == {"enable": True, "selector": 1}
    assert runtime["run_farm"]["base_recipe"].endswith("externally_provisioned.yaml")
    assert (runtime["run_farm"]["recipe_arg_overrides"]["run_farm_hosts_to_use"]
            == [{RUN_FARM_HOST: "one_fpga_spec"}])
    assert runtime["target_config"]["default_hw_config"] == HW_CONFIG_NAME
    assert runtime["workload"]["workload_name"] == "gcc.json"


def test_stage_workload_uses_firesims_layout(tmp_path):
    descriptor = {"benchmark_name": "gcc", "common_rootfs": "gcc.img",
                  "common_bootbinary": "gcc-bin"}
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name, data in (("gcc.json", json.dumps(descriptor).encode()),
                           ("gcc.img", b"rootfs"), ("gcc-bin", b"boot")):
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

    assert stage_workload(str(tmp_path), FireMarshalArtifact(archive=buf.getvalue())) == "gcc"

    workloads = tmp_path / "workloads"
    assert json.loads((workloads / "gcc.json").read_text()) == descriptor
    assert (workloads / "gcc" / "gcc.img").read_bytes() == b"rootfs"
    assert (workloads / "gcc" / "gcc-bin").read_bytes() == b"boot"
