"""Splitting a FireMarshal workload into one FireSim workload per job."""

import io
import json
import os
import tarfile

from chia.chipyard.state_def import FireMarshalArtifact


def test_split_gives_each_job_only_its_own_files(tmp_path):
    descriptor = {
        "benchmark_name": "spec",
        "common_simulation_outputs": ["uartlog"],
        "workloads": [
            {"name": "spec-gcc", "bootbinary": "spec-gcc-bin", "rootfs": "spec-gcc.img",
             "outputs": ["/output"]},
            {"name": "spec-mcf", "bootbinary": "spec-mcf-bin", "rootfs": "spec-mcf.img",
             "outputs": ["/output"]},
        ],
    }
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name, data in [("spec.json", json.dumps(descriptor).encode())] + [
                (f, f.encode()) for f in ("spec-gcc.img", "spec-gcc-bin",
                                          "spec-mcf.img", "spec-mcf-bin")]:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

    jobs = FireMarshalArtifact(archive=buf.getvalue()).split()

    assert [j.json_name for j in jobs] == ["spec-gcc.json", "spec-mcf.json"]
    assert jobs[0].unpack(str(tmp_path)) == {
        "benchmark_name": "spec-gcc",
        "common_rootfs": "spec-gcc.img",
        "common_bootbinary": "spec-gcc-bin",
        "common_outputs": ["/output"],
        "common_simulation_outputs": ["uartlog"],
    }
    assert sorted(os.listdir(tmp_path)) == ["spec-gcc-bin", "spec-gcc.img", "spec-gcc.json"]
