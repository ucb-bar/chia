from __future__ import annotations

from pathlib import Path

from chia.base.ChiaFunction import ChiaFunction, get

HEAD = "node:__internal_head__"


@ChiaFunction(num_cpus=0)
def _append(data: bytes, path: str) -> str:
    if path.startswith("s3://"):
        from chia.aws.s3 import S3Node
        bucket, key = path.removeprefix("s3://").split("/", 1)
        s3 = S3Node(bucket)
        s3.put_bytes(key, (s3.get_bytes(key) if s3.exists(key) else b"") + data)
    else:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "ab") as f:
            f.write(data)
    return path


def chia_log(data: bytes | str, path: str, resource: str | None = None) -> str:
    """Add ``data`` to the end of ``path`` on a machine with ``resource``, the head when
    ``None``, and return ``path``. A path that starts with ``s3://bucket/key`` is an S3
    object, written with that machine's AWS credentials.

    Do not pass a resource that the calling task holds: the write would wait for that task
    to end. Writing on the caller's own machine needs a local option, not supported yet.
    """
    if isinstance(data, str):
        data = data.encode()
    return get(_append.options(resources={resource or HEAD: 0.001}).chia_remote(data, path))
