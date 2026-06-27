"""Network-free unit tests for chia.aws.s3.S3Node using botocore Stubber.

Run with::

    python -m pytest chia/aws/test/test_s3.py -v
"""

from __future__ import annotations

import io
from datetime import datetime, timezone

import pytest
from botocore.stub import ANY, Stubber

from chia.aws.s3 import (
    S3AuthError,
    S3Node,
    S3NotFoundError,
    S3RequestError,
    S3ServerError,
)

BUCKET = "test-bucket"


@pytest.fixture(autouse=True)
def _isolate_aws(monkeypatch):
    """Fake env credentials (signing happens before Stubber intercepts) and
    no-op the retry sleep."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    monkeypatch.setattr("chia.aws.s3.time.sleep", lambda _s: None)


def _stubbed(region: str = "us-east-1") -> tuple[S3Node, Stubber]:
    node = S3Node(BUCKET, region=region)
    stubber = Stubber(node._client)
    stubber.activate()
    return node, stubber


# ---------------------------------------------------------------------------
# Constructor credential handling
# ---------------------------------------------------------------------------

def test_explicit_creds_are_used(monkeypatch):
    # Explicit keys must work with nothing else available: clear the fixture's
    # fake env creds and point the file chain at nowhere.
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")
    monkeypatch.setenv("AWS_CONFIG_FILE", "/nonexistent")
    node = S3Node(BUCKET, region="us-east-1",
                  aws_access_key_id="AKIAEXPLICIT",
                  aws_secret_access_key="secret",
                  aws_session_token="token")
    with Stubber(node._client) as stub:
        stub.add_response("head_object", {"ContentLength": 1},
                          {"Bucket": BUCKET, "Key": "k"})
        assert node.exists("k") is True


def test_empty_creds_fall_through_to_default_chain():
    # A blank load_aws_creds-style tuple must behave like no creds at all
    # (the fixture's fake env creds carry the request).
    node = S3Node(BUCKET, region="us-east-1",
                  aws_access_key_id="", aws_secret_access_key="",
                  aws_session_token="")
    with Stubber(node._client) as stub:
        stub.add_response("head_object", {"ContentLength": 1},
                          {"Bucket": BUCKET, "Key": "k"})
        assert node.exists("k") is True


def test_key_without_secret_raises():
    with pytest.raises(ValueError):
        S3Node(BUCKET, aws_access_key_id="AKIA...")


def test_token_without_keys_raises():
    with pytest.raises(ValueError):
        S3Node(BUCKET, aws_session_token="token")


def test_profile_and_explicit_keys_raises():
    with pytest.raises(ValueError):
        S3Node(BUCKET, profile="prod",
               aws_access_key_id="AKIA...", aws_secret_access_key="secret")


# ---------------------------------------------------------------------------
# put_bytes / get_bytes
# ---------------------------------------------------------------------------

def test_put_get_bytes_roundtrip():
    node, stub = _stubbed()
    stub.add_response(
        "put_object", {"ETag": '"abc"'},
        {"Bucket": BUCKET, "Key": "a/b.json", "Body": b"hello"})
    stub.add_response(
        "get_object", {"Body": io.BytesIO(b"hello")},
        {"Bucket": BUCKET, "Key": "a/b.json"})

    node.put_bytes("a/b.json", b"hello")
    assert node.get_bytes("a/b.json") == b"hello"
    stub.assert_no_pending_responses()


def test_put_bytes_content_type():
    node, stub = _stubbed()
    stub.add_response(
        "put_object", {"ETag": '"abc"'},
        {"Bucket": BUCKET, "Key": "k", "Body": b"{}",
         "ContentType": "application/json"})
    node.put_bytes("k", b"{}", content_type="application/json")
    stub.assert_no_pending_responses()


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------

_TS = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _obj(key: str, size: int = 1) -> dict:
    return {"Key": key, "Size": size, "LastModified": _TS, "ETag": '"e-tag"'}


def test_list_paginates():
    node, stub = _stubbed()
    stub.add_response(
        "list_objects_v2",
        {"IsTruncated": True, "NextContinuationToken": "tok",
         "Contents": [_obj("p/a", 3)]},
        {"Bucket": BUCKET, "Prefix": "p/"})
    stub.add_response(
        "list_objects_v2",
        {"IsTruncated": False, "Contents": [_obj("p/b", 7)]},
        {"Bucket": BUCKET, "Prefix": "p/", "ContinuationToken": "tok"})

    infos = node.list("p/")
    assert [i.key for i in infos] == ["p/a", "p/b"]
    assert [i.size for i in infos] == [3, 7]
    assert infos[0].last_modified == _TS.isoformat()
    assert infos[0].etag == "e-tag"  # quotes stripped
    stub.assert_no_pending_responses()


def test_list_max_keys_stops_early():
    node, stub = _stubbed()
    stub.add_response(
        "list_objects_v2",
        {"IsTruncated": False,
         "Contents": [_obj("p/a"), _obj("p/b"), _obj("p/c")]},
        {"Bucket": BUCKET, "Prefix": "p/"})
    infos = node.list("p/", max_keys=2)
    assert [i.key for i in infos] == ["p/a", "p/b"]


def test_list_empty():
    node, stub = _stubbed()
    stub.add_response(
        "list_objects_v2", {"IsTruncated": False},
        {"Bucket": BUCKET, "Prefix": "none/"})
    assert node.list("none/") == []


# ---------------------------------------------------------------------------
# exists / delete
# ---------------------------------------------------------------------------

def test_exists_true():
    node, stub = _stubbed()
    stub.add_response("head_object", {"ContentLength": 5},
                      {"Bucket": BUCKET, "Key": "k"})
    assert node.exists("k") is True


def test_exists_false_on_404():
    node, stub = _stubbed()
    stub.add_client_error("head_object", service_error_code="404",
                          service_message="Not Found", http_status_code=404)
    assert node.exists("k") is False


def test_exists_raises_on_403():
    node, stub = _stubbed()
    stub.add_client_error("head_object", service_error_code="403",
                          service_message="Forbidden", http_status_code=403)
    with pytest.raises(S3AuthError):
        node.exists("k")


def test_delete():
    node, stub = _stubbed()
    stub.add_response("delete_object", {}, {"Bucket": BUCKET, "Key": "k"})
    assert node.delete("k") is None
    stub.assert_no_pending_responses()


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------

def test_get_bytes_no_such_key():
    node, stub = _stubbed()
    stub.add_client_error("get_object", service_error_code="NoSuchKey",
                          service_message="missing", http_status_code=404)
    with pytest.raises(S3NotFoundError):
        node.get_bytes("nope")


def test_access_denied_maps_to_auth_error():
    node, stub = _stubbed()
    stub.add_client_error("get_object", service_error_code="AccessDenied",
                          service_message="denied", http_status_code=403)
    with pytest.raises(S3AuthError):
        node.get_bytes("k")


def test_other_4xx_maps_to_request_error():
    node, stub = _stubbed()
    stub.add_client_error("get_object", service_error_code="InvalidBucketName",
                          service_message="bad name", http_status_code=400)
    with pytest.raises(S3RequestError) as excinfo:
        node.get_bytes("k")
    assert excinfo.value.code == "InvalidBucketName"


# ---------------------------------------------------------------------------
# Retry on transient failures
# ---------------------------------------------------------------------------

def test_transient_error_retried_once_then_succeeds():
    node, stub = _stubbed()
    stub.add_client_error("get_object", service_error_code="SlowDown",
                          service_message="slow down", http_status_code=503)
    stub.add_response("get_object", {"Body": io.BytesIO(b"ok")},
                      {"Bucket": BUCKET, "Key": "k"})
    assert node.get_bytes("k") == b"ok"
    stub.assert_no_pending_responses()


def test_persistent_transient_error_raises_server_error():
    node, stub = _stubbed()
    for _ in range(2):  # initial attempt + one retry
        stub.add_client_error("get_object", service_error_code="InternalError",
                              service_message="oops", http_status_code=500)
    with pytest.raises(S3ServerError):
        node.get_bytes("k")
    stub.assert_no_pending_responses()


def test_not_found_is_not_retried():
    node, stub = _stubbed()
    stub.add_client_error("get_object", service_error_code="NoSuchKey",
                          service_message="missing", http_status_code=404)
    with pytest.raises(S3NotFoundError):
        node.get_bytes("k")
    stub.assert_no_pending_responses()  # exactly one request made


# ---------------------------------------------------------------------------
# ensure_bucket
# ---------------------------------------------------------------------------

def test_ensure_bucket_already_exists():
    node, stub = _stubbed()
    stub.add_response("head_bucket", {}, {"Bucket": BUCKET})
    assert node.ensure_bucket() is False
    stub.assert_no_pending_responses()  # no create_bucket issued


def test_ensure_bucket_creates_us_east_1():
    node, stub = _stubbed()
    stub.add_client_error("head_bucket", service_error_code="404",
                          service_message="Not Found", http_status_code=404)
    # us-east-1 must NOT send CreateBucketConfiguration
    stub.add_response("create_bucket", {"Location": f"/{BUCKET}"},
                      {"Bucket": BUCKET})
    assert node.ensure_bucket() is True
    stub.assert_no_pending_responses()


def test_ensure_bucket_creates_with_location_constraint():
    node, stub = _stubbed(region="us-west-2")
    stub.add_client_error("head_bucket", service_error_code="404",
                          service_message="Not Found", http_status_code=404)
    stub.add_response(
        "create_bucket", {"Location": f"/{BUCKET}"},
        {"Bucket": BUCKET,
         "CreateBucketConfiguration": {"LocationConstraint": "us-west-2"}})
    assert node.ensure_bucket() is True
    stub.assert_no_pending_responses()


def test_ensure_bucket_forbidden_raises():
    node, stub = _stubbed()
    stub.add_client_error("head_bucket", service_error_code="403",
                          service_message="Forbidden", http_status_code=403)
    with pytest.raises(S3AuthError):
        node.ensure_bucket()


def test_ensure_bucket_creation_race():
    node, stub = _stubbed()
    stub.add_client_error("head_bucket", service_error_code="404",
                          service_message="Not Found", http_status_code=404)
    stub.add_client_error("create_bucket",
                          service_error_code="BucketAlreadyOwnedByYou",
                          service_message="already yours",
                          http_status_code=409)
    assert node.ensure_bucket() is False
    stub.assert_no_pending_responses()


# ---------------------------------------------------------------------------
# upload_file / download_file
# ---------------------------------------------------------------------------

def test_upload_file_small(tmp_path):
    src = tmp_path / "src.bin"
    src.write_bytes(b"payload")
    node, stub = _stubbed()
    # Under the 8 MB multipart threshold the transfer manager degenerates to
    # a single put_object.
    stub.add_response("put_object", {"ETag": '"x"'},
                      {"Bucket": BUCKET, "Key": "dst.bin", "Body": ANY,
                       "ChecksumAlgorithm": ANY})
    node.upload_file(src, "dst.bin")
    stub.assert_no_pending_responses()


def test_upload_file_unwraps_client_error():
    node, stub = _stubbed()
    stub.add_client_error("put_object", service_error_code="AccessDenied",
                          service_message="denied", http_status_code=403)
    with pytest.raises(S3AuthError):
        node.upload_file(__file__, "dst.bin")


def test_download_file_small(tmp_path):
    dest = tmp_path / "sub" / "dir" / "out.bin"
    node, stub = _stubbed()
    stub.add_response("head_object", {"ContentLength": 5, "ETag": '"x"'},
                      {"Bucket": BUCKET, "Key": "k"})
    stub.add_response("get_object",
                      {"Body": io.BytesIO(b"hello"), "ContentLength": 5,
                       "ETag": '"x"'},
                      {"Bucket": BUCKET, "Key": "k"})
    node.download_file("k", dest)
    assert dest.read_bytes() == b"hello"  # parent dirs auto-created
    stub.assert_no_pending_responses()


def test_download_file_not_found():
    node, stub = _stubbed()
    stub.add_client_error("head_object", service_error_code="404",
                          service_message="Not Found", http_status_code=404)
    with pytest.raises(S3NotFoundError):
        node.download_file("nope", "/tmp/never-written.bin")
