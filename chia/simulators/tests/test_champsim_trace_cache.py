"""Offline behavioral tests for ChampSim trace resolution and S3 caching."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from chia.simulators import champsim


@pytest.fixture
def s3_cache(monkeypatch, tmp_path):
    """Use an isolated cache and fake S3 downloads without AWS credentials."""
    def download_file(bucket, key, filename):
        Path(filename).write_bytes(f"{bucket}/{key}".encode())

    client = SimpleNamespace(download_file=Mock(side_effect=download_file))
    client_factory = Mock(return_value=client)
    monkeypatch.setitem(sys.modules, "boto3", SimpleNamespace(client=client_factory))
    monkeypatch.setattr(champsim.tempfile, "gettempdir", lambda: str(tmp_path))
    return client, client_factory


def test_s3_cache_separates_buckets_with_the_same_key(s3_cache):
    client, _ = s3_cache
    key = "traces/workload.champsimtrace.xz"

    first = champsim._resolve_trace(f"s3://bucket-a/{key}")
    second = champsim._resolve_trace(f"s3://bucket-b/{key}")

    assert Path(second).read_bytes() == f"bucket-b/{key}".encode()
    assert Path(first).read_bytes() == f"bucket-a/{key}".encode()
    assert first != second
    assert client.download_file.call_count == 2


def test_s3_cache_reuses_the_same_object(s3_cache):
    client, client_factory = s3_cache
    trace = "s3://bucket-a/traces/workload.champsimtrace.gz"

    first = champsim._resolve_trace(trace)
    second = champsim._resolve_trace(trace)

    assert first == second
    assert Path(second).read_bytes() == b"bucket-a/traces/workload.champsimtrace.gz"
    client_factory.assert_called_once_with("s3")
    client.download_file.assert_called_once_with(
        "bucket-a", "traces/workload.champsimtrace.gz", first,
    )


def test_s3_cache_separates_prefixes_with_the_same_basename(s3_cache):
    client, _ = s3_cache
    first = champsim._resolve_trace("s3://bucket-a/first/workload.champsimtrace.gz")
    second = champsim._resolve_trace("s3://bucket-a/second/workload.champsimtrace.gz")

    assert first != second
    assert Path(first).read_bytes() == b"bucket-a/first/workload.champsimtrace.gz"
    assert Path(second).read_bytes() == b"bucket-a/second/workload.champsimtrace.gz"
    assert client.download_file.call_count == 2


def test_s3_cache_ignores_legacy_key_only_entries(s3_cache, tmp_path):
    client, _ = s3_cache
    key = "traces/workload.champsimtrace.xz"
    key_hash = hashlib.sha256(key.encode()).hexdigest()[:12]
    legacy = tmp_path / f"{key_hash}_{Path(key).name}"
    legacy.write_bytes(b"trace from an unknown bucket")

    resolved = champsim._resolve_trace(f"s3://bucket-a/{key}")

    assert Path(resolved).read_bytes() == f"bucket-a/{key}".encode()
    assert Path(resolved) != legacy
    assert legacy.read_bytes() == b"trace from an unknown bucket"
    client.download_file.assert_called_once_with("bucket-a", key, resolved)


@pytest.mark.parametrize("suffix", [".champsimtrace.gz", ".champsimtrace.xz"])
def test_s3_cache_preserves_the_trace_basename(s3_cache, suffix):
    basename = f"workload{suffix}"

    resolved = champsim._resolve_trace(f"s3://bucket-a/traces/{basename}")

    assert Path(resolved).name.endswith(f"_{basename}")


def test_local_trace_is_returned_without_s3(s3_cache, tmp_path):
    _, client_factory = s3_cache
    trace = tmp_path / "local.champsimtrace.gz"
    trace.write_bytes(b"local trace")

    assert champsim._resolve_trace(str(trace)) == str(trace)
    client_factory.assert_not_called()


def test_missing_local_trace_raises(tmp_path):
    with pytest.raises(FileNotFoundError, match="Trace not found"):
        champsim._resolve_trace(str(tmp_path / "missing.champsimtrace.gz"))


def test_gcs_trace_remains_unsupported():
    with pytest.raises(NotImplementedError, match="GCS trace URIs not yet supported"):
        champsim._resolve_trace("gs://bucket-a/traces/workload.champsimtrace.gz")
