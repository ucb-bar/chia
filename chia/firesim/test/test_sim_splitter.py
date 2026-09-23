"""Unit tests for SimSplitter's workload splitting.

Only the split is covered here: everything else in SimSplitter needs real EC2.
"""

import io
import json
import tarfile

import pytest

from chia.chipyard.state_def import FireMarshalArtifact
from chia.firesim.sim_splitter import SimSplitter


class _FakeS3:
    """Records uploads instead of calling S3."""

    def __init__(self, *args, **kwargs):
        self.uploaded: list[str] = []

    def upload_file(self, local_path, key):
        self.uploaded.append(key)


@pytest.fixture(autouse=True)
def fake_s3(monkeypatch):
    fake = _FakeS3()
    monkeypatch.setattr("chia.firesim.sim_splitter.S3Node", lambda *a, **k: fake)
    return fake


def _artifact(descriptor: dict, members: list[str]) -> FireMarshalArtifact:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name in members:
            data = name.encode()
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
        blob = json.dumps(descriptor).encode()
        info = tarfile.TarInfo("wl.json")
        info.size = len(blob)
        tar.addfile(info, io.BytesIO(blob))
    return FireMarshalArtifact(
        archive=buf.getvalue(), img_name="", bin_name="", json_name="wl.json",
        success=True, stdout="", stderr="", returncode=0)


def _splitter() -> SimSplitter:
    from chia.aws.config import AWSConfig

    return SimSplitter(aws_config=AWSConfig(), ray_address="127.0.0.1:6379",
                       s3_bucket="bucket", chia_source_path="/chia")


def test_split_multi_job_workload(fake_s3):
    artifact = _artifact(
        {
            "benchmark_name": "spec",
            "common_simulation_outputs": ["uartlog"],
            "workloads": [
                {"name": "spec-gcc", "bootbinary": "spec-gcc-bin",
                 "rootfs": "spec-gcc.img", "outputs": ["/output"]},
                {"name": "spec-mcf", "bootbinary": "spec-mcf-bin",
                 "rootfs": "spec-mcf.img", "outputs": ["/output"]},
            ],
        },
        ["spec-gcc.img", "spec-gcc-bin", "spec-mcf.img", "spec-mcf-bin"],
    )

    jobs = _splitter().split_workload(artifact)

    assert [j.benchmark_name for j in jobs] == ["spec-gcc", "spec-mcf"]
    assert jobs[0].rootfs_uri == "s3://bucket/workloads/spec/spec-gcc.img"
    assert jobs[0].bootbinary_uri == "s3://bucket/workloads/spec/spec-gcc-bin"
    assert jobs[0].outputs == ["/output"]
    assert jobs[0].simulation_outputs == ["uartlog"]
    assert len(fake_s3.uploaded) == 4


def test_split_uniform_workload_yields_one_job():
    artifact = _artifact(
        {
            "benchmark_name": "hello",
            "common_bootbinary": "hello-bin",
            "common_rootfs": "hello.img",
            "common_outputs": ["/out"],
            "common_simulation_outputs": ["uartlog"],
        },
        ["hello.img", "hello-bin"],
    )

    jobs = _splitter().split_workload(artifact)

    assert len(jobs) == 1
    assert jobs[0].benchmark_name == "hello"
    assert jobs[0].outputs == ["/out"]


def test_split_rejects_failed_artifact():
    artifact = FireMarshalArtifact(
        archive=b"", img_name="", bin_name="", json_name="wl.json",
        success=False, stdout="", stderr="boom", returncode=1)
    with pytest.raises(ValueError):
        _splitter().split_workload(artifact)
