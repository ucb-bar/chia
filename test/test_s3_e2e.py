"""End-to-end test for S3Node.

Hits real AWS S3 (no mocks) against an existing bucket. Skipped entirely
unless ``CHIA_S3_BUCKET`` is set. Never creates buckets; all objects are
written under a unique ``chia-s3-e2e/<uuid>/`` prefix and deleted afterwards.

Usage:
    CHIA_S3_BUCKET=<bucket> python test/test_s3_e2e.py

Environment variables:
    CHIA_S3_BUCKET   Existing bucket to test against (required)
    CHIA_S3_REGION   Optional region override (default: boto3 default chain)
"""

import logging
import os
import sys
import tempfile
import uuid
from pathlib import Path

import boto3
import pytest

from chia.aws.s3 import S3AuthError, S3Node, S3NotFoundError

BUCKET = os.environ.get("CHIA_S3_BUCKET")
REGION = os.environ.get("CHIA_S3_REGION")
PREFIX = f"chia-s3-e2e/{uuid.uuid4().hex}/"

pytestmark = pytest.mark.skipif(
    not BUCKET, reason="CHIA_S3_BUCKET not set; skipping live S3 e2e")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_s3_e2e")


def _node() -> S3Node:
    return S3Node(BUCKET, region=REGION)


def test_bytes_roundtrip():
    node = _node()
    key = PREFIX + "blob.bin"
    try:
        node.put_bytes(key, b"hello e2e")
        assert node.exists(key) is True
        assert node.get_bytes(key) == b"hello e2e"
        infos = node.list(PREFIX)
        assert any(i.key == key and i.size == len(b"hello e2e") for i in infos), \
            f"key not in listing: {[i.key for i in infos]}"
    finally:
        node.delete(key)
    assert node.exists(key) is False


def test_file_roundtrip():
    node = _node()
    key = PREFIX + "file.bin"
    payload = b"file payload " * 1024
    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "src.bin"
        src.write_bytes(payload)
        dest = Path(tmp) / "nested" / "dir" / "dst.bin"
        try:
            node.upload_file(src, key)
            assert node.exists(key) is True
            node.download_file(key, dest)
            assert dest.read_bytes() == payload
        finally:
            node.delete(key)


def test_missing_key_raises_not_found():
    node = _node()
    try:
        node.get_bytes(PREFIX + "definitely-not-there.bin")
    except S3NotFoundError:
        return
    raise AssertionError("expected S3NotFoundError")


def test_delete_missing_key_is_idempotent():
    _node().delete(PREFIX + "never-existed.bin")


def test_ensure_bucket_on_existing():
    assert _node().ensure_bucket() is False


def test_explicit_credentials_roundtrip():
    """Re-pass the ambient credentials explicitly — the ship-creds-by-value
    pattern used for docker workers without a mounted ~/.aws."""
    creds = boto3.session.Session().get_credentials()
    assert creds is not None, "no ambient AWS credentials to re-pass"
    frozen = creds.get_frozen_credentials()
    node = S3Node(BUCKET, region=REGION,
                  aws_access_key_id=frozen.access_key,
                  aws_secret_access_key=frozen.secret_key,
                  aws_session_token=frozen.token or None)
    key = PREFIX + "explicit-creds.bin"
    try:
        node.put_bytes(key, b"explicit")
        assert node.get_bytes(key) == b"explicit"
    finally:
        node.delete(key)


def test_bogus_explicit_credentials_raise_auth_error():
    node = S3Node(BUCKET, region=REGION,
                  aws_access_key_id="AKIAINVALIDINVALID00",
                  aws_secret_access_key="bogus-secret-bogus-secret-0000000000")
    try:
        node.exists(PREFIX + "anything")
    except S3AuthError:
        return
    raise AssertionError("expected S3AuthError")


TESTS = [
    test_bytes_roundtrip,
    test_file_roundtrip,
    test_missing_key_raises_not_found,
    test_delete_missing_key_is_idempotent,
    test_ensure_bucket_on_existing,
    test_explicit_credentials_roundtrip,
    test_bogus_explicit_credentials_raise_auth_error,
]


def main():
    if not BUCKET:
        print("CHIA_S3_BUCKET not set; skipping live S3 e2e")
        return
    failed: list[str] = []
    for t in TESTS:
        name = t.__name__
        logger.info("RUN  %s", name)
        try:
            t()
        except Exception as exc:
            failed.append(name)
            logger.error("FAIL %s: %s", name, exc)
        else:
            logger.info("PASS %s", name)
    if failed:
        print(f"\n{len(failed)}/{len(TESTS)} tests failed: {failed}")
        sys.exit(1)
    print(f"\nAll {len(TESTS)} tests passed against s3://{BUCKET}/{PREFIX}")


if __name__ == "__main__":
    main()
