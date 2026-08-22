"""The golden model travelling with the simulator that was built against it."""
import hashlib
import os

from chia.chipyard.chisel_build_node import ChiselBuildNode
from chia.chipyard.spike_build_node import STATIC_ARCHIVES
from chia.chipyard.state_def import BuildArtifact, BuildTarget, SpikeBuildArtifact
from chia.chipyard.verilator_run_node import VerilatorRunNode

CONTENT = b"golden-model-bytes"
DIGEST = hashlib.sha256(CONTENT).hexdigest()


def _model(lib_name: str = "libriscv.so") -> SpikeBuildArtifact:
    return SpikeBuildArtifact(
        success=True, lib_name=lib_name, lib_content=CONTENT, digest=DIGEST,
        spike_bin="/spike/build/libriscv.so", stdout="", stderr="", returncode=0)


def _artifact(**kwargs) -> BuildArtifact:
    return BuildArtifact(
        name="chipyard", simulator_binary_content=b"\x7fELF",
        simulator_binary_name="simulator-chipyard.harness-C", config="C",
        config_package="chipyard", target=BuildTarget.VERILATOR, success=True,
        stdout="", stderr="", returncode=0, **kwargs)


def test_build_without_a_golden_model_is_unchanged():
    """The historical path: no staging, no bundling, no extra make args - so an
    existing caller's artifact does not silently grow by the size of libriscv."""
    node = ChiselBuildNode("/cy", config="C", extra_make_args={"VERILATOR_THREADS": "8"})
    assert node.bundle_runtime_libs is False
    assert node._effective_make_args() == {"VERILATOR_THREADS": "8"}


def test_dynamic_golden_model_bundles_and_defeats_the_baked_rpath():
    node = ChiselBuildNode("/cy", config="C", golden_model=_model())
    assert node.bundle_runtime_libs is True
    # chipyard bakes -Wl,-rpath,$(RISCV)/lib in, and a DT_RPATH is consulted
    # before LD_LIBRARY_PATH; new-dtags makes it a DT_RUNPATH, which the run
    # node's LD_LIBRARY_PATH can then override.
    assert "--enable-new-dtags" in node._effective_make_args()["EXTRA_SIM_LDFLAGS"]


def test_new_dtags_is_appended_to_a_callers_ldflags():
    node = ChiselBuildNode("/cy", config="C", golden_model=_model(),
                           extra_make_args={"EXTRA_SIM_LDFLAGS": "-lfoo"})
    flags = node._effective_make_args()["EXTRA_SIM_LDFLAGS"]
    assert flags.startswith("-lfoo") and "--enable-new-dtags" in flags


def test_static_golden_model_names_every_archive_and_bundles_nothing():
    """-lriscv resolves to the .so whenever both exist, so the archives have to
    be named outright - and libriscv.a is not one archive's worth of work:
    on its own it leaves ~900 undefined softfloat symbols, because only the
    shared library absorbed them. A binary with spike compiled in then has
    nothing left to stage at run time."""
    node = ChiselBuildNode("/cy", config="C", golden_model=_model(),
                           static_golden_model=True, clean_sim=True)
    assert node.bundle_runtime_libs is False
    lrsicv = node._effective_make_args()["LRISCV"].split()
    assert [os.path.basename(a) for a in lrsicv] == list(STATIC_ARCHIVES)
    assert lrsicv[0].endswith("/lib/libriscv.a")


def test_bundling_can_be_forced_or_suppressed():
    assert ChiselBuildNode("/cy", config="C", bundle_runtime_libs=True).bundle_runtime_libs
    assert not ChiselBuildNode("/cy", config="C", golden_model=_model(),
                               bundle_runtime_libs=False).bundle_runtime_libs


def test_unresolvable_golden_model_is_still_bundled(monkeypatch, tmp_path):
    """`libriscv.so => not found` is the normal ldd reading in a container
    without the rpath target. Dropping it there would ship a simulator with no
    golden model, which is the failure this is meant to make impossible."""
    binary = tmp_path / "sim"
    binary.write_bytes(b"\x7fELF")
    node = ChiselBuildNode("/cy", config="C", golden_model=_model())
    monkeypatch.setattr(
        node, "_parse_ldd",
        lambda _out: [("libriscv.so", None), ("libc.so.6", "/usr/lib/libc.so.6")])
    libs = dict(node._collect_runtime_libs(str(binary)))
    assert libs["libriscv.so"] == CONTENT      # carried despite being unresolved
    assert "libc.so.6" not in libs             # every image has it


def test_parse_ldd_keeps_unresolved_entries():
    parsed = dict(ChiselBuildNode._parse_ldd(
        "\tlibriscv.so => not found\n"
        "\tlibdramsim.so => /opt/lib/libdramsim.so (0x00007f)\n"
        "\t/lib64/ld-linux-x86-64.so.2 (0x00007f)\n"))
    assert parsed == {"libriscv.so": None, "libdramsim.so": "/opt/lib/libdramsim.so"}


def test_run_node_stages_libs_beside_the_binary(tmp_path):
    node = VerilatorRunNode()
    node._setup(_artifact(runtime_libs=[("libriscv.so", CONTENT)],
                          golden_model_digest=DIGEST),
                b"elf", "t.riscv", str(tmp_path), {})
    staged = os.path.join(node._task_dir, "libriscv.so")
    assert open(staged, "rb").read() == CONTENT
    # First on the path, so it wins over anything the image installed.
    assert node._env()["LD_LIBRARY_PATH"].split(":")[0] == node._task_dir


def test_run_node_stages_nothing_for_a_legacy_artifact(tmp_path):
    node = VerilatorRunNode()
    node._setup(_artifact(), b"elf", "t.riscv", str(tmp_path), {})
    assert sorted(os.listdir(node._task_dir)) == [
        "simulator-chipyard.harness-C", "t.riscv"]
