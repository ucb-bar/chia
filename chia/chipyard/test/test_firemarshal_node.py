"""Tests for FireMarshalNode.

Two layers, because ``build_base``/``compose`` loop-mount a disk image (needs a
``--privileged`` container) and run ``marshal`` (needs the Buildroot FireMarshal
image), which a plain test runner can't do:

  * **pytest (local, no privilege)** — the store/reuse/error contract that runs
    anywhere: a missing base fails cleanly, and an image already in the store is
    repackaged instead of rebuilt. Run: ``pytest chia/chipyard/test/test_firemarshal_node.py``.

  * **e2e (privileged image)** — the real ``build_base`` + ``compose`` (command
    and jobs), asserting the produced images/descriptor/run-scripts. Run inside
    the FireMarshal image::

        docker run --rm --privileged -v <repo>:/chia1:ro -e PYTHONPATH=/chia1 \\
            chia-firemarshal:local bash -lc \\
            "sudo mkdir -p /images && sudo chown ray:users /images && \\
             python3 /chia1/chia/chipyard/test/test_firemarshal_node.py"
"""

import io
import json
import sys
import tarfile

from chia.chipyard.firemarshal_node import FireMarshalNode


def _unpack(archive: bytes) -> dict:
    out = {}
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as t:
        for m in t.getmembers():
            if m.isfile():
                out[m.name.lstrip("./")] = t.extractfile(m).read()
    return out


# --- pytest: interface contract that needs no privilege ---------------------

def test_compose_missing_base_fails(tmp_path):
    """compose reports a clean failure when the base isn't in the store."""
    node = FireMarshalNode(image_store=str(tmp_path))
    art = node.compose(base_name="absent", overlay_files={"root/a": b"x"},
                       name="wl", work_dir=str(tmp_path / "w"))
    assert not art.success and art.returncode == 1
    assert art.archive == b"" and "not found" in art.stderr


def test_build_base_reuses_store(tmp_path):
    """A base already in the store is repackaged (no rebuild)."""
    d = tmp_path / "br-base"
    d.mkdir()
    (d / "br-base.img").write_bytes(b"IMG")
    (d / "br-base-bin").write_bytes(b"BIN")
    art = FireMarshalNode(image_store=str(tmp_path)).build_base(
        name="br-base", work_dir=str(tmp_path / "w"))
    assert art.success and art.returncode == 0
    files = _unpack(art.archive)
    assert files.get("br-base.img") == b"IMG" and files.get("br-base-bin") == b"BIN"


def test_compose_reuses_store(tmp_path):
    """A composed workload already in the store is repackaged (no recompose)."""
    d = tmp_path / "wl"
    d.mkdir()
    (d / "wl.img").write_bytes(b"I")
    (d / "wl-bin").write_bytes(b"B")
    (d / "wl.json").write_text('{"benchmark_name": "wl"}')
    art = FireMarshalNode(image_store=str(tmp_path)).compose(
        base_name="ignored", overlay_files={"root/a": b"x"}, name="wl",
        work_dir=str(tmp_path / "w"))
    assert art.success and art.json_name == "wl.json"
    assert set(_unpack(art.archive)) >= {"wl.img", "wl-bin", "wl.json"}


# --- e2e: real build_base + compose, inside the privileged FireMarshal image -

def _run_e2e() -> int:
    node = FireMarshalNode(image_store="/images", timeout_seconds=3600)

    base = node.build_base("br-base")
    assert base.success, base.stderr[-1500:]

    art = node.compose("br-base", {"root/w/data": b"x"}, "wl-cmd",
                       config={"command": "echo hi", "outputs": ["/output"]})
    assert art.success, art.stderr[-1500:]
    d = json.load(open("/images/wl-cmd/wl-cmd.json"))
    assert d["common_rootfs"] == "wl-cmd.img" and d["common_outputs"] == ["/output"]

    jobs = node.compose("br-base", {"root/w/data": b"x"}, "wl-jobs",
        config={"jobs": [{"name": "j1", "command": "echo j1", "outputs": ["/output"]},
                         {"name": "j2", "command": "echo j2"}],
                "post_run_hook": "handle.py"})
    assert jobs.success, jobs.stderr[-1500:]
    dj = json.load(open("/images/wl-jobs/wl-jobs.json"))
    assert [w["name"] for w in dj["workloads"]] == ["wl-jobs-j1", "wl-jobs-j2"]
    assert dj["post_run_hook"] == "handle.py" and jobs.img_name == ""
    assert {"wl-jobs-j1.img", "wl-jobs-j2.img", "wl-jobs.json"} <= set(_unpack(jobs.archive))

    print("E2E PASS")
    return 0


if __name__ == "__main__":
    sys.exit(_run_e2e())
