"""Build and compose FireMarshal RISC-V workload images.

Images live in a name-keyed store (:data:`IMAGE_STORE`, default ``/images``):
``<store>/<name>/<name>.img`` + ``<name>-bin`` (+ ``<name>.json`` FireSim
descriptor for composed workloads). Mount it **writable**
(``-v /host/fm-images:/images``) to supply prebuilt images by name and persist
built ones; both methods are reuse-if-present, so a rerun just repackages the
stored image instead of rebuilding.

Usage::

    fm = FireMarshalNode()
    base = get(fm.build_base.chia_remote(fm, name="br-base"))            # built once, then reused
    wl = get(fm.compose.chia_remote(fm, base_name="br-base",
                                    overlay_files={"root/x": b"..."}, name="my-wl"))
    # wl.archive: sparse-tar bytes (rootfs + boot bin + descriptor) for the run node

  * :meth:`build_base` builds a configurable base (rootfs + kernel + boot binary)
    via FireMarshal from a plain options dict — slow (~4-24 min), once.
  * :meth:`compose` copies a stored base by name and drops a workload overlay into
    its rootfs — fast (loop-mount + ``cp``), no FireMarshal.

Both return the image as sparse-tar bytes so it moves to a run node over Ray with
no shared FS. The worker must be ``--privileged`` (loop-mount) and, for
``build_base``, carry a Buildroot-capable FireMarshal (``dockerfiles/FireMarshalDockerfile``).
"""

import json
import logging
import os
import shutil
import subprocess
import uuid

from chia.base.ChiaFunction import ChiaFunction
from chia.chipyard.state_def import FireMarshalArtifact, FireMarshalBaseArtifact

FIREMARSHAL_DIR = "/home/ray/chipyard/software/firemarshal"
IMAGE_STORE = "/images"

# Copy the base image/binary, optionally grow the rootfs, loop-mount it, drop the
# overlay in, then fsck. `trap` guarantees the mount is released on any error.
_COMPOSE = r"""set -e
IMG="$1"; MNT="$2"; OVERLAY="$3"; BASE_IMG="$4"; BASE_BIN="$5"; BIN_OUT="$6"; SIZE_MIB="$7"
cp "$BASE_IMG" "$IMG"
cp "$BASE_BIN" "$BIN_OUT"
if [ "$SIZE_MIB" != "0" ]; then
    e2fsck -f -p "$IMG" || true
    truncate -s "${SIZE_MIB}M" "$IMG"
    resize2fs "$IMG"
fi
mkdir -p "$MNT"
trap 'sudo umount "$MNT" 2>/dev/null || true' EXIT
sudo mount -o loop "$IMG" "$MNT"
sudo cp -a "$OVERLAY"/. "$MNT"/
sync
sudo umount "$MNT"
trap - EXIT
e2fsck -f -p "$IMG" >/dev/null
"""

# Run `marshal build <config>`. env.sh leaves an empty element in LD_LIBRARY_PATH
# that trips Buildroot's "CWD in LD_LIBRARY_PATH" check, so strip empties first.
_MARSHAL_BUILD = r"""set -e
source "$2"
export LD_LIBRARY_PATH=$(echo "$LD_LIBRARY_PATH" | sed -e 's/::*/:/g' -e 's/^://' -e 's/:$//')
git config --global --add safe.directory '*'
cd "$1"
./marshal build "$3"
"""


class FireMarshalNode:
    """Builds/composes FireMarshal images into a name-keyed store."""

    logging_name = "FireMarshalNode"

    def __init__(
        self,
        firemarshal_dir: str = FIREMARSHAL_DIR,
        image_store: str = IMAGE_STORE,
        timeout_seconds: int = 3600,
        logging_level: int = logging.DEBUG,
    ):
        """
        Args:
            firemarshal_dir: FireMarshal checkout used by :meth:`build_base`.
            image_store: Name-keyed image dir; mount it (``-v host:/images``) to
                persist/reuse images across runs and to supply prebuilt ones.
            timeout_seconds: Wall-clock limit per build/compose/package step; on
                expiry the step returns ``returncode=-1`` (never raises). Default
                1h to accommodate a from-scratch base build.
            logging_level: Logging level for this node's logger.
        """
        self.firemarshal_dir = firemarshal_dir
        self.image_store = image_store
        self.timeout_seconds = timeout_seconds
        self.logger = logging.getLogger(self.logging_name)
        self.logger.setLevel(logging_level)

    @ChiaFunction(resources={"chipyard": 1})
    def build_base(
        self,
        name: str,
        options: "dict | None" = None,
        work_dir: str = "/tmp/fm",
    ) -> FireMarshalBaseArtifact:
        """Build a configurable base image and return it as sparse-tar bytes.

        Reused from the store if ``<image_store>/<name>/`` already exists
        (prebuilt or previously built) — otherwise built once via FireMarshal.

        Args:
            name: Store key / tag for the image (``<store>/<name>/``).
            options: Plain option dict, translated internally into a FireMarshal
                config (no marshal JSON exposed). Supported keys: ``base`` (parent
                config, default ``"br-base.json"``), ``distro`` (``"br"``/``"fedora"``),
                ``rootfs_size_mib`` (int), ``kernel_config`` (kernel config fragment
                text). Empty/omitted with ``name="br-base"`` builds the stock base.
            work_dir: Scratch dir for packaging.
        """
        stdout, stderr, returncode = "", "", 0
        if not self._have(name):
            returncode, stdout, stderr = self._marshal_build(name, options or {})
        archive = b""
        if returncode == 0 and self._have(name):
            archive, out, err, returncode = self._package(name, work_dir)
            stdout, stderr = stdout + out, stderr + err

        success = returncode == 0 and archive != b""
        if not success:
            self.logger.warning(
                f"build_base({name}) failed (rc={returncode}); stderr tail: {stderr[-500:]}")
        return FireMarshalBaseArtifact(
            archive=archive, img_name=f"{name}.img", bin_name=f"{name}-bin",
            success=success, stdout=stdout, stderr=stderr, returncode=returncode)

    @ChiaFunction(resources={"chipyard": 1})
    def compose(
        self,
        base_name: str,
        overlay_files: dict[str, bytes],
        name: str,
        work_dir: str = "/tmp/fm",
        config: "dict | None" = None,
        rootfs_size_mib: int = 0,
    ) -> FireMarshalArtifact:
        """Compose ``overlay_files`` onto the stored base ``base_name`` and return
        the workload image(s) + FireSim descriptor as sparse-tar bytes.

        Reused from the store if ``<store>/<name>/<name>.json`` already exists —
        otherwise composed (copy base + loop-mount + ``cp`` the overlay). With
        ``jobs`` it emits one image per job (each with its own baked run command).

        Args:
            base_name: Store key of the base to copy (from :meth:`build_base` or a
                prebuilt image in the store).
            overlay_files: rootfs-relative path -> bytes (mirrors the rootfs layout).
            name: Store key / tag for the produced workload.
            work_dir: Scratch dir for the overlay/mount/packaging.
            config: Workload options as a dict (the marshal-config run knobs):
                ``command`` (str, baked as the boot run-script), ``jobs`` (list of
                ``{"name","command","outputs"}`` — one image per job, emitted as the
                FireSim descriptor's ``workloads``), ``outputs`` (default guest
                paths), ``post_run_hook`` (str), ``simulation_outputs`` (default
                ``["uartlog"]``).
            rootfs_size_mib: Grow the rootfs before composing (``0`` keeps base size).
        """
        config = config or {}
        json_path = os.path.join(self._dir(name), f"{name}.json")
        stdout, stderr, returncode = "", "", 0
        if not os.path.isfile(json_path):
            if not self._have(base_name):
                return FireMarshalArtifact(
                    b"", "", "", f"{name}.json", success=False, stdout="",
                    stderr=f"base '{base_name}' not found in {self.image_store}", returncode=1)
            returncode, stdout, stderr = self._compose_workload(
                base_name, name, overlay_files, config, rootfs_size_mib, work_dir)

        archive = b""
        if returncode == 0 and os.path.isfile(json_path):
            archive, out, err, returncode = self._package(name, work_dir)
            stdout, stderr = stdout + out, stderr + err

        success = returncode == 0 and archive != b""
        if not success:
            self.logger.warning(
                f"compose({name}) failed (rc={returncode}); stderr tail: {stderr[-500:]}")
        single = not config.get("jobs")
        return FireMarshalArtifact(
            archive=archive, img_name=f"{name}.img" if single else "",
            bin_name=f"{name}-bin" if single else "", json_name=f"{name}.json",
            success=success, stdout=stdout, stderr=stderr, returncode=returncode)

    # ---- store layout -------------------------------------------------------

    def _dir(self, name: str) -> str:
        return os.path.join(self.image_store, name)

    def _have(self, name: str) -> bool:
        d = self._dir(name)
        return os.path.isfile(os.path.join(d, f"{name}.img")) and \
            os.path.isfile(os.path.join(d, f"{name}-bin"))

    def _package(self, name: str, work_dir: str) -> "tuple[bytes, str, str, int]":
        """Sparse-tar the stored image dir into bytes."""
        os.makedirs(work_dir, exist_ok=True)
        arc = os.path.join(work_dir, f".pkg-{uuid.uuid4().hex[:8]}.tar.gz")
        out, err, rc = self._run(["tar", "--sparse", "-czf", arc, "-C", self._dir(name), "."])
        archive = b""
        if rc == 0 and os.path.isfile(arc):
            with open(arc, "rb") as f:
                archive = f.read()
            os.remove(arc)
        return archive, out, err, rc

    # ---- build_base internals ----------------------------------------------

    def _marshal_build(self, name: str, options: dict) -> "tuple[int, str, str]":
        """Build image `name` via FireMarshal and copy it into the store."""
        config = self._gen_base_config(name, options)
        env_sh = os.path.normpath(os.path.join(self.firemarshal_dir, "..", "..", "env.sh"))
        self.logger.info(f"marshal build {config} -> {self._dir(name)}")
        stdout, stderr, rc = self._run(
            ["bash", "-c", _MARSHAL_BUILD, "_", self.firemarshal_dir, env_sh, config])
        if rc != 0:
            return rc, stdout, stderr
        src = os.path.join(self.firemarshal_dir, "images", "firechip", name)
        dst = self._dir(name)
        os.makedirs(dst, exist_ok=True)
        try:
            shutil.copy(os.path.join(src, f"{name}.img"), os.path.join(dst, f"{name}.img"))
            shutil.copy(os.path.join(src, f"{name}-bin"), os.path.join(dst, f"{name}-bin"))
        except FileNotFoundError as e:
            return 1, stdout, stderr + f"\nmarshal produced no {name} image: {e}"
        return 0, stdout, stderr

    def _gen_base_config(self, name: str, options: dict) -> str:
        """Translate the options dict into a FireMarshal config; return its filename.

        The stock base (``name="br-base"``, no options) uses the shipped
        ``br-base.json``; anything else writes ``<name>.json`` inheriting a parent.
        """
        if name == "br-base" and not options:
            return "br-base.json"
        config = {"name": name, "base": options.get("base", "br-base.json")}
        if "distro" in options:
            config["distro"] = options["distro"]
        if "rootfs_size_mib" in options:
            config["rootfs-size"] = f"{options['rootfs_size_mib']}MiB"
        if "kernel_config" in options:
            frag = f"{name}.kfrag"
            with open(os.path.join(self.firemarshal_dir, frag), "w") as f:
                f.write(options["kernel_config"])
            config["linux"] = {"config": frag}
        with open(os.path.join(self.firemarshal_dir, f"{name}.json"), "w") as f:
            json.dump(config, f, indent=2)
        return f"{name}.json"

    # ---- compose internals --------------------------------------------------

    def _compose_workload(self, base_name, name, overlay_files, config,
                          rootfs_size_mib, work_dir) -> "tuple[int, str, str]":
        os.makedirs(work_dir, exist_ok=True)
        work = os.path.join(work_dir, f".fm-{uuid.uuid4().hex[:8]}")
        overlay_dir = os.path.join(work, "overlay")
        self._write_overlay(overlay_files, overlay_dir)
        dst = self._dir(name)
        os.makedirs(dst, exist_ok=True)
        base_img = os.path.join(self._dir(base_name), f"{base_name}.img")
        base_bin = os.path.join(self._dir(base_name), f"{base_name}-bin")

        # One image per job (with its own baked command); else a single image.
        jobs = config.get("jobs")
        targets = ([(f"{name}-{j['name']}", j.get("command"), j.get("outputs", config.get("outputs", [])))
                    for j in jobs] if jobs
                   else [(name, config.get("command"), config.get("outputs", []))])

        stdout, stderr = "", ""
        for target, command, _outs in targets:
            self._set_run_script(overlay_dir, command)
            self.logger.info(f"Composing {target} from base {base_name}")
            out, err, rc = self._run(
                ["bash", "-c", _COMPOSE, "_", os.path.join(dst, f"{target}.img"),
                 os.path.join(work, "mnt"), overlay_dir, base_img, base_bin,
                 os.path.join(dst, f"{target}-bin"), str(rootfs_size_mib)])
            stdout, stderr = stdout + out, stderr + err
            if rc != 0:
                shutil.rmtree(work, ignore_errors=True)
                return rc, stdout, stderr

        self._write_descriptor(os.path.join(dst, f"{name}.json"), name, config, targets)
        shutil.rmtree(work, ignore_errors=True)
        return 0, stdout, stderr

    @staticmethod
    def _set_run_script(overlay_dir: str, command: "str | None") -> None:
        """Bake `command` as the boot run-script (``/firemarshal.sh``), or remove it.

        br-base's init runs ``/firemarshal.sh`` at boot; the script ends in poweroff
        so the sim exits (mirrors FireMarshal's ``genRunScript``)."""
        path = os.path.join(overlay_dir, "firemarshal.sh")
        if command:
            with open(path, "w") as f:
                f.write(f"#!/bin/sh\n{command}\nsync; poweroff -f\n")
            os.chmod(path, 0o755)
        elif os.path.exists(path):
            os.remove(path)

    @staticmethod
    def _write_overlay(overlay_files: dict[str, bytes], overlay_dir: str) -> None:
        """Materialize overlay_files into overlay_dir, mirroring the rootfs layout."""
        os.makedirs(overlay_dir, exist_ok=True)
        for rel_path, content in overlay_files.items():
            dest = os.path.join(overlay_dir, rel_path)
            os.makedirs(os.path.dirname(dest) or overlay_dir, exist_ok=True)
            with open(dest, "wb") as f:
                f.write(content)

    @staticmethod
    def _write_descriptor(path: str, name: str, config: dict, targets: list) -> None:
        """Write the FireSim workload descriptor: uniform form for a single command,
        or a ``workloads`` list (one entry per job) when config has ``jobs``."""
        sim_outputs = config.get("simulation_outputs", ["uartlog"])
        if config.get("jobs"):
            descriptor = {
                "benchmark_name": name,
                "common_simulation_outputs": sim_outputs,
                "workloads": [
                    {"name": t, "bootbinary": f"{t}-bin", "rootfs": f"{t}.img",
                     "outputs": list(outs)}
                    for (t, _cmd, outs) in targets
                ],
            }
        else:
            descriptor = {
                "benchmark_name": name,
                "common_bootbinary": f"{name}-bin",
                "common_rootfs": f"{name}.img",
                "common_outputs": list(config.get("outputs", [])),
                "common_simulation_outputs": sim_outputs,
            }
        if config.get("post_run_hook"):
            descriptor["post_run_hook"] = config["post_run_hook"]
        with open(path, "w") as f:
            json.dump(descriptor, f, indent=2)

    def _run(self, cmd: list[str]) -> "tuple[str, str, int]":
        """Run cmd with the node's timeout; rc=-1 on timeout (never raises)."""
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True,
                                  timeout=self.timeout_seconds)
            return proc.stdout, proc.stderr, proc.returncode
        except subprocess.TimeoutExpired as e:
            stdout = e.stdout.decode(errors="replace") if isinstance(e.stdout, bytes) else (e.stdout or "")
            stderr = (e.stderr.decode(errors="replace") if isinstance(e.stderr, bytes) else (e.stderr or "")) + \
                f"\n[FireMarshalNode] timeout after {self.timeout_seconds}s"
            return stdout, stderr, -1
