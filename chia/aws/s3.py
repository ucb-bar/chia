"""S3 object-store client bound to one bucket, exposed as a chia service-pattern node.

binds to one
bucket at construction and exposes a small, synchronous API. Errors are raised
as typed exceptions (no in-band ``success: bool``); see ``S3Error`` and its
subclasses. Transient failures (5xx, throttling, network errors) are retried
once before raising.

Usage::

    from chia.aws.s3 import S3Node, S3NotFoundError

    node = S3Node("my-bucket", region="us-west-2")
    node.put_bytes("results/run1.json", b"{}")
    data = node.get_bytes("results/run1.json")
    node.upload_file("/tmp/waves.vcd", "waves/run1.vcd")

For anything beyond this surface (presigned URLs, multipart tuning, ...),
drop down to the raw boto3 client via ``node._client``.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import boto3
from boto3.exceptions import RetriesExceededError, S3UploadFailedError
from botocore.config import Config as BotoConfig
from botocore.exceptions import (
    ClientError,
    ConnectionError as BotoConnectionError,
    HTTPClientError,
    NoCredentialsError,
)

from chia.cluster.log import get_logger


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class S3Error(Exception):
    """Base class for all S3 node errors. ``code`` is the AWS error code, if any."""

    def __init__(self, message: str = "", code: str = ""):
        self.code = code
        super().__init__(message)


class S3AuthError(S3Error):
    """Missing/invalid credentials, or access denied (401/403/AccessDenied)."""


class S3NotFoundError(S3Error):
    """Object or bucket does not exist (404/NoSuchKey/NoSuchBucket)."""


class S3RequestError(S3Error):
    """Non-transient client-side error (other 4xx, e.g. BucketAlreadyExists)."""


class S3ServerError(S3Error):
    """5xx/throttling after one retry, or a network/timeout failure."""


# HEAD requests (head_object/head_bucket) have no error body, so botocore
# reports bare numeric codes ("404", "403") — match those alongside the
# named codes.
_NOT_FOUND_CODES = frozenset({"NoSuchKey", "NoSuchBucket", "404", "NotFound"})
_AUTH_CODES = frozenset({
    "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch",
    "ExpiredToken", "TokenRefreshRequired", "403", "Forbidden",
})
_TRANSIENT_CODES = frozenset({
    "InternalError", "ServiceUnavailable", "SlowDown", "RequestTimeout",
    "Throttling", "ThrottlingException", "RequestLimitExceeded",
    "RequestTimeTooSkewed",
})


# ---------------------------------------------------------------------------
# Node
# ---------------------------------------------------------------------------

@dataclass
class S3ObjectInfo:
    """Serializable metadata for one S3 object (one ``list`` result row)."""

    key: str
    size: int
    last_modified: str  # ISO-8601 string
    etag: str  # surrounding quotes stripped


class S3Node:
    """Client for one S3 bucket.

    Service-pattern node (head-node only, not a Ray task). Bind to one bucket
    at construction; all methods are synchronous and raise typed exceptions on
    failure. Credentials come from the default boto3 chain unless overridden —
    see ``__init__`` for the resolution order.
    """

    logging_name = "S3Node"

    def __init__(
        self,
        bucket: str,
        region: str | None = None,
        profile: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        timeout_seconds: int = 60,
        logging_level: int = logging.DEBUG,
    ):
        """Bind to ``bucket``. Credential resolution, in order of precedence:

        1. Explicit keys — pass ``aws_access_key_id`` and
           ``aws_secret_access_key`` together (plus ``aws_session_token`` for
           temporary/STS credentials). Use this to ship credentials by value
           into an environment with no ``~/.aws`` or instance role, e.g. a
           docker worker::

               key, secret, token = load_aws_creds(creds_dir)  # on the head node
               node = S3Node(bucket,                           # on the worker
                             aws_access_key_id=key,
                             aws_secret_access_key=secret,
                             aws_session_token=token or None)

           Empty strings are treated as "not provided" (so a blank
           ``load_aws_creds`` tuple falls through to the default chain), but
           passing only one of key/secret, a token without both keys, or
           explicit keys together with ``profile`` raises ``ValueError``.
        2. ``profile`` — a named profile from ``~/.aws``.
        3. Neither — boto3's default chain (env vars, ``~/.aws``, instance
           metadata / IAM role). The right choice on the head node and on EC2.

        Credentials are resolved lazily by boto3: nothing is validated here,
        and missing/invalid credentials surface as ``S3AuthError`` on the
        first call.
        """
        aws_access_key_id = aws_access_key_id or None
        aws_secret_access_key = aws_secret_access_key or None
        aws_session_token = aws_session_token or None
        if (aws_access_key_id is None) != (aws_secret_access_key is None):
            raise ValueError(
                "aws_access_key_id and aws_secret_access_key must be "
                "provided together")
        if aws_session_token and aws_access_key_id is None:
            raise ValueError(
                "aws_session_token requires aws_access_key_id and "
                "aws_secret_access_key")
        if profile and aws_access_key_id:
            raise ValueError(
                "pass either profile or explicit aws_* keys, not both")

        self.bucket = bucket
        self._session = boto3.session.Session(
            profile_name=profile,
            region_name=region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )
        self.region = self._session.region_name or "us-east-1"

        self.logger = get_logger("aws.s3")
        self.logger.setLevel(logging_level)

        # botocore's built-in retries are disabled so the node's explicit
        # retry-once below is the only retry policy in play.
        self._client = self._session.client(
            "s3",
            config=BotoConfig(
                connect_timeout=timeout_seconds,
                read_timeout=timeout_seconds,
                retries={"max_attempts": 1, "mode": "standard"},
            ),
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def upload_file(self, local_path: str | Path, key: str) -> None:
        """Upload a local file to ``key`` (multipart for large files)."""
        local = Path(local_path)
        self._call(
            f"upload_file {local} -> s3://{self.bucket}/{key}",
            self._client.upload_file, str(local), self.bucket, key,
        )

    def download_file(self, key: str, local_path: str | Path) -> None:
        """Download ``key`` to a local path, creating parent directories."""
        local = Path(local_path)
        local.parent.mkdir(parents=True, exist_ok=True)
        self._call(
            f"download_file s3://{self.bucket}/{key} -> {local}",
            self._client.download_file, self.bucket, key, str(local),
        )

    def put_bytes(self, key: str, data: bytes, content_type: str | None = None) -> None:
        """Write ``data`` to ``key``."""
        kwargs: dict[str, Any] = {"Bucket": self.bucket, "Key": key, "Body": data}
        if content_type is not None:
            kwargs["ContentType"] = content_type
        self._call(f"put_object s3://{self.bucket}/{key}",
                   self._client.put_object, **kwargs)

    def get_bytes(self, key: str) -> bytes:
        """Return the contents of ``key``."""
        def _get() -> bytes:
            resp = self._client.get_object(Bucket=self.bucket, Key=key)
            return resp["Body"].read()

        return self._call(f"get_object s3://{self.bucket}/{key}", _get)

    def list(self, prefix: str = "", max_keys: int | None = None) -> list[S3ObjectInfo]:
        """List objects under ``prefix``, up to ``max_keys`` (None = all).

        Includes zero-byte ``.../`` folder-marker keys if present; callers
        that don't want them should filter on ``key.endswith("/")``.
        """
        def _list() -> list[S3ObjectInfo]:
            paginator = self._client.get_paginator("list_objects_v2")
            out: list[S3ObjectInfo] = []
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    out.append(S3ObjectInfo(
                        key=obj["Key"],
                        size=obj["Size"],
                        last_modified=obj["LastModified"].isoformat(),
                        etag=obj["ETag"].strip('"'),
                    ))
                    if max_keys is not None and len(out) >= max_keys:
                        return out
            return out

        return self._call(f"list_objects_v2 s3://{self.bucket}/{prefix}", _list)

    def exists(self, key: str) -> bool:
        """Return whether ``key`` exists. A 403 raises ``S3AuthError`` rather
        than reporting ``False``."""
        try:
            self._call(f"head_object s3://{self.bucket}/{key}",
                       self._client.head_object, Bucket=self.bucket, Key=key)
        except S3NotFoundError:
            return False
        return True

    def delete(self, key: str) -> None:
        """Delete ``key``. Idempotent — S3 does not error on a missing key."""
        self._call(f"delete_object s3://{self.bucket}/{key}",
                   self._client.delete_object, Bucket=self.bucket, Key=key)

    def ensure_bucket(self) -> bool:
        """Create the bound bucket if it does not exist.

        Returns True if the bucket was created, False if it already existed.
        Raises ``S3AuthError`` if the bucket exists but is not accessible.
        """
        try:
            self._call(f"head_bucket s3://{self.bucket}",
                       self._client.head_bucket, Bucket=self.bucket)
            return False
        except S3NotFoundError:
            pass

        self.logger.info("Creating S3 bucket: %s", self.bucket)
        create_kwargs: dict[str, Any] = {"Bucket": self.bucket}
        if self.region != "us-east-1":
            create_kwargs["CreateBucketConfiguration"] = {
                "LocationConstraint": self.region
            }
        try:
            self._call(f"create_bucket s3://{self.bucket}",
                       self._client.create_bucket, **create_kwargs)
        except S3RequestError as exc:
            if exc.code == "BucketAlreadyOwnedByYou":
                return False  # lost a creation race; the bucket exists
            raise
        return True

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _call(self, op: str, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
        """Invoke a boto3 call, retrying once on transient failures and
        raising typed errors."""
        attempts = 2  # one retry, mirroring GithubIssuesNode._request
        for attempt in range(attempts):
            try:
                return fn(*args, **kwargs)
            except NoCredentialsError as exc:
                raise S3AuthError(f"no AWS credentials found for {op}") from exc
            except S3UploadFailedError as exc:
                # The transfer manager wraps the original ClientError
                # (implicitly chained, so check __context__ too).
                cause = exc.__cause__ or exc.__context__
                if isinstance(cause, ClientError):
                    mapped = self._map_client_error(op, cause)
                else:
                    mapped = S3ServerError(f"upload failed on {op}: {exc}")
                if isinstance(mapped, S3ServerError) and attempt + 1 < attempts:
                    self.logger.warning(
                        "Transient failure on %s (attempt %d), retrying: %s",
                        op, attempt + 1, exc)
                    time.sleep(2)
                    continue
                raise mapped from exc
            except (BotoConnectionError, HTTPClientError, RetriesExceededError) as exc:
                if attempt + 1 < attempts:
                    self.logger.warning(
                        "Network error on %s (attempt %d), retrying: %s",
                        op, attempt + 1, exc)
                    time.sleep(2)
                    continue
                raise S3ServerError(f"network error on {op}: {exc}") from exc
            except ClientError as exc:
                mapped = self._map_client_error(op, exc)
                if isinstance(mapped, S3ServerError) and attempt + 1 < attempts:
                    self.logger.warning(
                        "Transient %s on %s (attempt %d), retrying",
                        mapped.code or "error", op, attempt + 1)
                    time.sleep(2)
                    continue
                raise mapped from exc
        raise S3ServerError(f"exhausted retries for {op}")  # unreachable

    def _map_client_error(self, op: str, exc: ClientError) -> S3Error:
        """Map a botocore ClientError to the matching typed exception."""
        err = exc.response.get("Error", {})
        code = err.get("Code", "")
        message = err.get("Message", "")
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        detail = f"{code or status} on {op}: {message or exc}"
        if code in _NOT_FOUND_CODES or status == 404:
            return S3NotFoundError(detail, code=code)
        if code in _AUTH_CODES or status in (401, 403):
            return S3AuthError(detail, code=code)
        if code in _TRANSIENT_CODES or 500 <= status < 600:
            return S3ServerError(detail, code=code)
        return S3RequestError(detail, code=code)
