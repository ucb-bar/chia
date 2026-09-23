"""An FPGA bitstream paired with the driver built against it."""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass

from chia.aws.s3 import S3Node
from chia.cluster.log import get_logger

logger = get_logger("firesim.fs_bitstream")

# FireSim's fixed filenames inside a sim slot (RuntimeHWConfig).
DRIVER_TAR_NAME = "driver-bundle.tar.gz"
BITSTREAM_TAR_NAME = "firesim.tar.gz"


@dataclass
class FSBitstream:
    """An FPGA image plus the simulation driver built against it.

    The two always travel together: a driver built from different RTL than the
    bitstream fails in confusing ways. Either half is carried by value
    (``*_bytes``, straight out of a build) or by reference (``*_uri``, any
    fsspec URI). :meth:`publish` turns value into reference so a fan-out over
    many FPGAs pulls from S3 instead of replicating bytes through Ray.

    TODO: f2 names its image with an AGFI while the Alveo/xb10 platforms use a
    bitstream tar, and config_hwdb.yaml rejects an entry carrying both. This
    split may need to change once a non-f2 platform runs.
    """

    quintuplet: str
    agfi: str | None = None
    bitstream_uri: str | None = None
    bitstream_bytes: bytes | None = None
    driver_uri: str | None = None
    driver_bytes: bytes | None = None

    def __post_init__(self) -> None:
        if bool(self.agfi) == bool(self.bitstream_uri or self.bitstream_bytes):
            raise ValueError(
                "FSBitstream needs exactly one of agfi or bitstream_uri/bytes")

    def publish(self, bucket: str, prefix: str) -> FSBitstream:
        """Upload any by-value half to S3 and return a reference-only copy."""
        if not self.bitstream_bytes and not self.driver_bytes:
            return self
        s3 = S3Node(bucket)
        return FSBitstream(
            quintuplet=self.quintuplet,
            agfi=self.agfi,
            bitstream_uri=self._upload(
                s3, bucket, prefix, self.bitstream_uri, self.bitstream_bytes,
                BITSTREAM_TAR_NAME),
            driver_uri=self._upload(
                s3, bucket, prefix, self.driver_uri, self.driver_bytes,
                DRIVER_TAR_NAME),
        )

    def to_hwdb(self, name: str, deploy_dir: str) -> dict[str, dict]:
        """Render the ``config_hwdb.yaml`` stanza, writing out any bytes.

        FireSim resolves a bare path relative to ``firesim/deploy`` and fetches
        URIs itself, so both cases end up as a plain string here.
        """
        entry: dict[str, object] = {
            "deploy_quintuplet_override": None,
            "deploy_makefrag_override": None,
            "custom_runtime_config": None,
        }
        if self.agfi:
            entry["agfi"] = self.agfi
        else:
            entry["bitstream_tar"] = self._materialize(
                deploy_dir, self.bitstream_uri, self.bitstream_bytes,
                BITSTREAM_TAR_NAME)
        driver = self._materialize(
            deploy_dir, self.driver_uri, self.driver_bytes, DRIVER_TAR_NAME)
        if driver:
            # With driver_tar set FireSim skips `make driver`, so the manager
            # never needs chipyard or the sources the image came from.
            entry["driver_tar"] = driver
        else:
            logger.warning(f"{name} has no driver; FireSim will build one, "
                           f"which needs chipyard in the manager image")
        return {name: entry}

    @staticmethod
    def _upload(s3: S3Node, bucket: str, prefix: str, uri: str | None,
                data: bytes | None, filename: str) -> str | None:
        if uri or not data:
            return uri
        key = f"{prefix.strip('/')}/{filename}"
        logger.info(f"Publishing {filename} to s3://{bucket}/{key}")
        s3.put_bytes(key, data)
        return f"s3://{bucket}/{key}"

    @staticmethod
    def _materialize(deploy_dir: str, uri: str | None, data: bytes | None,
                     filename: str) -> str | None:
        if uri or not data:
            return uri
        # Content-addressed, so a rerun reuses the file instead of rewriting
        # hundreds of MB.
        dest_dir = os.path.join(deploy_dir, "fsbit",
                                hashlib.sha256(data).hexdigest()[:16])
        dest = os.path.join(dest_dir, filename)
        if not os.path.isfile(dest):
            os.makedirs(dest_dir, exist_ok=True)
            with open(dest, "wb") as f:
                f.write(data)
        return dest
