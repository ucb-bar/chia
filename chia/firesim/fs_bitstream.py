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
    bitstream produces a simulation that fails in confusing ways. Either half
    may be carried by value (``*_bytes``, straight out of a build) or by
    reference (``*_uri``, any fsspec URI — ``s3://``, ``https://``, ``file://``,
    or a plain path). :meth:`publish` turns value into reference so a fan-out
    over many FPGAs pulls from S3 instead of replicating bytes through Ray.

    TODO: f2 names its image with an AGFI (an AWS id, not a URI) while the
    Alveo/xb10 platforms use a bitstream tar, and ``config_hwdb.yaml`` rejects
    an entry carrying both. This split may need to change once a non-f2
    platform actually runs.

    Attributes:
        quintuplet: FireSim deploy quintuplet the image was built for
            (``PLATFORM-TARGET_PROJECT-DESIGN-TARGET_CONFIG-PLATFORM_CONFIG``).
        agfi: AWS Global FPGA Image id, for ``f2``. Mutually exclusive with the
            ``bitstream_*`` fields.
        bitstream_uri: URI of the bitstream tar, for non-AGFI platforms.
        bitstream_bytes: Bitstream tar carried by value.
        driver_uri: URI of the driver bundle tar.
        driver_bytes: Driver bundle tar carried by value.
    """

    quintuplet: str
    agfi: str | None = None
    bitstream_uri: str | None = None
    bitstream_bytes: bytes | None = None
    driver_uri: str | None = None
    driver_bytes: bytes | None = None

    def __post_init__(self) -> None:
        has_bitstream = bool(self.bitstream_uri or self.bitstream_bytes)
        if bool(self.agfi) == has_bitstream:
            raise ValueError(
                "FSBitstream needs exactly one of agfi or bitstream_uri/bytes "
                "(config_hwdb.yaml rejects an entry carrying both)")

    def publish(self, bucket: str, prefix: str) -> FSBitstream:
        """Upload any by-value half to S3 and return a reference-only copy.

        Args:
            bucket: Destination S3 bucket.
            prefix: Key prefix; files land at ``<prefix>/<filename>``.

        Returns:
            A copy with ``*_bytes`` dropped and ``*_uri`` pointing at S3. Halves
            that are already URIs are passed through untouched.
        """
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
        """Render the ``config_hwdb.yaml`` stanza for this bitstream.

        By-value halves are written under ``<deploy_dir>/fsbit/`` and referenced
        by path; FireSim resolves a bare path relative to ``firesim/deploy``
        and fetches URIs itself, so both cases end up as a plain string here.

        Args:
            name: hwdb entry name, referenced by ``default_hw_config``.
            deploy_dir: The manager's ``firesim/deploy`` directory.

        Returns:
            ``{name: {...}}``, ready to dump into ``config_hwdb.yaml``.
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
            # With driver_tar set FireSim skips `make driver` entirely, so the
            # manager never needs chipyard or the sources the image came from.
            entry["driver_tar"] = driver
        else:
            logger.warning(
                f"{name} has no driver; FireSim will build one, which needs "
                f"chipyard in the manager image")
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
        # Content-addressed so re-running a job reuses the file instead of
        # rewriting hundreds of MB.
        digest = hashlib.sha256(data).hexdigest()[:16]
        dest_dir = os.path.join(deploy_dir, "fsbit", digest)
        dest = os.path.join(dest_dir, filename)
        if not os.path.isfile(dest):
            os.makedirs(dest_dir, exist_ok=True)
            with open(dest, "wb") as f:
                f.write(data)
        return dest
