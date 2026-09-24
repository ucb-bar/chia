"""Build an FPGA bitstream on an ECAD worker.

Runs inside the chipyard container on an ECAD instance. The work splits the way
FireSim's own ``buildbitstream`` splits it, for the same reason: Chisel
elaboration and the driver build need chipyard, which is in the container;
Vivado needs the FPGA Developer AMI, which is on the host. ``--net=host`` makes
``localhost`` the host, so the Vivado steps go over ssh to there.

The result is an :class:`~chia.firesim.fs_bitstream.FSBitstream` holding the
AGFI and the driver built against it. The AGFI step mirrors FireSim's own
``F2BitBuilder.aws_create_afi``: only AWS can turn a design checkpoint into a
flashable image, and its API reads the checkpoint from an S3 location.
"""

from __future__ import annotations

import logging
import os
import shlex
import subprocess
import time
from datetime import datetime, timezone

from chia.base.ChiaFunction import ChiaFunction
from chia.firesim.fs_bitstream import FSBitstream
from chia.firesim.specs import ECAD_RESOURCE
from chia.firesim.state_def import BuildRecipe, EcadBuildResult

CHIPYARD = "/home/ray/chipyard"
HOST = "ubuntu@localhost"
AWS_FPGA = "https://github.com/firesim/aws-fpga-firesim-f2.git"


class EcadBuildNode:
    """Builds one bitstream from a chipyard diff."""

    logging_name = "EcadBuildNode"

    def __init__(self, chipyard: str = CHIPYARD, timeout_seconds: int = 86400,
                 agfi_timeout_seconds: int = 7200):
        """
        Args:
            chipyard: Chipyard checkout inside the container.
            timeout_seconds: Wall-clock limit for the Vivado step.
            agfi_timeout_seconds: How long to wait for AWS to finish the image.
        """
        self.chipyard = chipyard
        self.timeout_seconds = timeout_seconds
        self.agfi_timeout_seconds = agfi_timeout_seconds
        self.logger = logging.getLogger(self.logging_name)

    @ChiaFunction(resources={ECAD_RESOURCE: 1})
    def build_bitstream(self, recipe: BuildRecipe, s3_bucket: str,
                        diff: str = "") -> EcadBuildResult:
        """Apply ``diff`` to chipyard, build the bitstream, and mint the AGFI.

        Args:
            recipe: What to build — the FireSim quintuplet plus frequency and
                Vivado strategy.
            s3_bucket: Where the design checkpoint is staged for
                ``create-fpga-image``. AWS reads the checkpoint from S3; there
                is no other way to register an f2 image.
            diff: Unified diff applied to the chipyard checkout before
                elaboration. Empty builds the image's chipyard unchanged.

        Returns:
            :class:`EcadBuildResult`. On success its ``bitstream`` holds the
            AGFI and the driver built against it.
        """
        quintuplet = recipe.quintuplet()
        log: list[str] = [f"quintuplet: {quintuplet}"]

        if diff:
            rc, out = self._sh(f"cd {self.chipyard} && git apply -", stdin=diff)
            log.append(f"git apply (rc={rc})\n{out[-1000:]}")
            if rc != 0:
                return self._failed(recipe, log)

        for target in ("replace-rtl", "driver"):
            rc, out = self._sh(self._make(recipe, target))
            log.append(f"make {target} (rc={rc})\n{out[-2000:]}")
            if rc != 0:
                return self._failed(recipe, log)

        rc, out = self._vivado(recipe, quintuplet)
        log.append(f"vivado (rc={rc})\n{out[-2000:]}")
        if rc != 0:
            return self._failed(recipe, log)

        checkpoint = self._host_file(self._dcp_tar(quintuplet))
        if not checkpoint:
            log.append("Vivado produced no tarball under build/checkpoints")
            return self._failed(recipe, log)

        agfi, err = self._create_agfi(recipe, checkpoint, s3_bucket)
        log.append(f"agfi: {agfi or err}")
        if not agfi:
            return self._failed(recipe, log)

        return EcadBuildResult(
            recipe_name=recipe.name, success=True, log="\n".join(log),
            bitstream=FSBitstream(
                quintuplet=quintuplet, agfi=agfi,
                driver_bytes=self._driver_bundle(recipe, quintuplet)))

    def _create_agfi(self, recipe: BuildRecipe, checkpoint: bytes,
                     s3_bucket: str) -> tuple[str, str]:
        """Register the checkpoint with AWS and wait for the image.

        Mirrors FireSim's ``F2BitBuilder.aws_create_afi``. Returns
        ``(agfi, "")`` or ``("", reason)``.
        """
        import boto3

        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        key = f"dcp/{recipe.name}-{stamp}.tar"
        s3, ec2 = boto3.client("s3"), boto3.client("ec2")
        try:
            s3.put_object(Bucket=s3_bucket, Key=key, Body=checkpoint)
            image = ec2.create_fpga_image(
                InputStorageLocation={"Bucket": s3_bucket, "Key": key},
                LogsStorageLocation={"Bucket": s3_bucket, "Key": "logs/"},
                Name=f"{recipe.name}-{stamp}")
        except Exception as e:
            return "", f"{type(e).__name__}: {e}"

        agfi, afi = image["FpgaImageGlobalId"], image["FpgaImageId"]
        self.logger.info(f"Created {afi}; waiting for it to become available")
        # AWS finishes its own place-and-route and validation here; tens of
        # minutes is normal.
        deadline = time.monotonic() + self.agfi_timeout_seconds
        while time.monotonic() < deadline:
            state = ec2.describe_fpga_images(
                FpgaImageIds=[afi])["FpgaImages"][0]["State"]["Code"]
            if state == "available":
                return agfi, ""
            if state != "pending":
                return "", f"{afi} entered state {state}"
            time.sleep(30)
        return "", f"{afi} still pending after {self.agfi_timeout_seconds}s"

    # ---- container-side steps ----------------------------------------------

    def _make(self, recipe: BuildRecipe, target: str) -> str:
        """The make invocation FireSim's own bitbuilder uses for this target."""
        makefrag = (f" TARGET_PROJECT_MAKEFRAG={self.chipyard}/generators/firechip"
                    f"/chip/src/main/makefrag/firesim"
                    if recipe.target_project == "firesim" else "")
        return (f"cd {self.chipyard}/sims/firesim && "
                f"source sourceme-manager.sh --skip-ssh-setup && cd sim && "
                f"make JAVA_HEAP_SIZE={recipe.java_heap_size}"
                f" PLATFORM={recipe.platform}"
                f" TARGET_PROJECT={recipe.target_project}{makefrag}"
                f" DESIGN={recipe.design}"
                f" TARGET_CONFIG={recipe.target_config}"
                f" PLATFORM_CONFIG={recipe.platform_config} {target}")

    def _driver_bundle(self, recipe: BuildRecipe, quintuplet: str) -> bytes:
        """Tar the driver binary with the shared libraries it actually needs."""
        out_dir = (f"{self.chipyard}/sims/firesim/sim/output/"
                   f"{recipe.platform}/{quintuplet}")
        driver = f"FireSim-{recipe.platform}"
        # Bundle every non-system .so ldd reports, so the run host needs none
        # of chipyard's toolchain installed.
        self._sh(
            f"cd {out_dir} && "
            f"ldd {driver} | grep '=>' | awk '{{print $3}}' | "
            f"grep -vE '(libc\\.so|libstdc\\+\\+|libm\\.so|libpthread|libdl\\.so"
            f"|librt\\.so|libgcc_s|libz\\.so|libzstd|libelf|ld-linux)' | "
            f"while read l; do cp -L \"$l\" . 2>/dev/null; done; "
            f"tar -czf driver-bundle.tar.gz {driver} *.so *.so.* 2>/dev/null || "
            f"tar -czf driver-bundle.tar.gz {driver}")
        return self._read(f"{out_dir}/driver-bundle.tar.gz")

    # ---- host-side steps ----------------------------------------------------

    def _vivado(self, recipe: BuildRecipe, quintuplet: str) -> tuple[int, str]:
        """Ship the generated cl_ dir to the host and run Vivado there."""
        cl_root = (f"{self.chipyard}/sims/firesim/platforms/f2/"
                   f"aws-fpga-firesim-f2/hdk/cl/developer_designs")
        cl_sub = f"cl_{quintuplet}"
        script = f"{self.chipyard}/sims/firesim/platforms/f2/build-bitstream.sh"

        rc, out = self._ssh(
            f"test -d ~/aws-fpga/.git || "
            f"git clone --recurse-submodules {AWS_FPGA} ~/aws-fpga", timeout=1800)
        if rc != 0:
            return rc, out

        # The container and the host share a network namespace, not a
        # filesystem, so the cl_ dir goes over the wire.
        rc, out = self._sh(
            f"cd {cl_root} && tar cf - {cl_sub} | "
            f"{self._ssh_cmd()} {shlex.quote(HOST)} "
            f"'mkdir -p ~/aws-fpga/hdk/cl/developer_designs && "
            f"cd ~/aws-fpga/hdk/cl/developer_designs && tar xf -'", timeout=1800)
        if rc != 0:
            return rc, out

        cl_dir = f"~/aws-fpga/hdk/cl/developer_designs/{cl_sub}"
        rc, out = self._sh(
            f"{self._ssh_cmd()} {shlex.quote(HOST)} "
            f"'cat > {cl_dir}/build-bitstream.sh' < {script} && "
            f"{self._ssh_cmd()} {shlex.quote(HOST)} "
            f"'chmod +x {cl_dir}/build-bitstream.sh'")
        if rc != 0:
            return rc, out

        # ulimit -s: Vivado's XDC processing recurses deeply enough to blow the
        # default 8 MB stack on larger designs.
        return self._ssh(
            f"cd ~/aws-fpga && source hdk_setup.sh && ulimit -s 32768 && "
            f"{cl_dir}/build-bitstream.sh --cl_dir {cl_dir} "
            f"--frequency {recipe.fpga_frequency} "
            f"--strategy {recipe.build_strategy}",
            timeout=self.timeout_seconds)

    @staticmethod
    def _dcp_tar(quintuplet: str) -> str:
        return (f"~/aws-fpga/hdk/cl/developer_designs/cl_{quintuplet}"
                f"/build/checkpoints/*.tar")

    def _host_file(self, remote_glob: str) -> bytes:
        """Read a file off the host, by value."""
        rc, _ = self._sh(
            f"{self._ssh_cmd()} {shlex.quote(HOST)} "
            f"'cat $(ls -t {remote_glob} | head -1)' > /tmp/artifact.bin")
        return self._read("/tmp/artifact.bin") if rc == 0 else b""

    # ---- plumbing -----------------------------------------------------------

    @staticmethod
    def _ssh_cmd() -> str:
        return "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

    def _ssh(self, cmd: str, timeout: int = 600) -> tuple[int, str]:
        return self._sh(f"{self._ssh_cmd()} {shlex.quote(HOST)} {shlex.quote(cmd)}",
                        timeout=timeout)

    def _sh(self, cmd: str, stdin: str = "",
            timeout: int = 7200) -> tuple[int, str]:
        """Run a command in this container; never raises."""
        self.logger.info(f"$ {cmd[:200]}")
        try:
            p = subprocess.run(["bash", "-lc", cmd], input=stdin, text=True,
                               capture_output=True, timeout=timeout)
            return p.returncode, p.stdout + p.stderr
        except subprocess.TimeoutExpired:
            return -1, f"[EcadBuildNode] timeout after {timeout}s"

    @staticmethod
    def _read(path: str) -> bytes:
        if not os.path.isfile(path):
            return b""
        with open(path, "rb") as f:
            return f.read()

    @staticmethod
    def _failed(recipe: BuildRecipe, log: list[str]) -> EcadBuildResult:
        return EcadBuildResult(recipe_name=recipe.name, success=False,
                               log="\n".join(log))
