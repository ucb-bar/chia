"""SpikeBuildNode: the golden model is rebuilt, verified, and configurable."""
import hashlib
import os
import subprocess

import pytest

from chia.chipyard import spike_build_node as sbn
from chia.chipyard.spike_build_node import (
    STATIC_ARCHIVES, SpikeBuildNode, stage_golden_model)
from chia.chipyard.state_def import SpikeBuildArtifact

OLD = 1_000_000_000  # fixed past mtime for sources; anything written now is newer


def _tree(tmp_path, build_dir=True, sources=True):
    spike = tmp_path / "toolchains" / "riscv-tools" / "riscv-isa-sim"
    spike.mkdir(parents=True)
    if sources:
        (spike / "processor.cc").write_text("// source")
        os.utime(spike / "processor.cc", (OLD, OLD))
        (spike / "configure").write_text("#!/bin/sh\n")
    if build_dir:
        (spike / "build").mkdir()
    return spike


def _node(tmp_path, **kwargs):
    kwargs.setdefault("install", False)
    kwargs.setdefault("strip", False)   # keep _read_maybe_stripped subprocess-free
    return SpikeBuildNode(chipyard_path=str(tmp_path),
                          riscv_path=str(tmp_path / "riscv"), **kwargs)


class _Runs:
    """Record every command the node launches and fabricate its result.

    A successful ``make`` writes libriscv.so (newer than the OLD-stamped
    sources, so the staleness check passes); everything else is output-free.
    """

    def __init__(self, spike, fail=()):
        self.argvs = []
        self._spike = spike
        self._fail = set(fail)

    def tool(self, index):
        return os.path.basename(str(self.argvs[index][0]))

    def __call__(self, argv, cwd=None, **kwargs):
        self.argvs.append([str(a) for a in argv])
        tool = os.path.basename(str(argv[0]))
        lib = self._spike / "build" / "libriscv.so" if self._spike else None
        if tool == "make" and "make" not in self._fail and not lib.exists():
            lib.write_bytes(b"model")
        return subprocess.CompletedProcess(argv, 1 if tool in self._fail else 0,
                                           "", "")


@pytest.fixture
def runs(tmp_path, monkeypatch):
    spike = _tree(tmp_path)
    recorder = _Runs(spike)
    monkeypatch.setattr(sbn.subprocess, "run", recorder)
    return recorder


def test_existing_configuration_is_left_alone(tmp_path, runs):
    """chipyard's build-setup already ran configure; rerunning it by default
    would discard that configuration and force a full rebuild on every call."""
    artifact = _node(tmp_path).build()
    assert artifact.success
    assert runs.tool(0) == "make"
    assert artifact.digest == hashlib.sha256(b"model").hexdigest()


def test_configure_args_reconfigure_before_make(tmp_path, runs):
    artifact = _node(tmp_path,
                     configure_args=["--with-isa=rv64gcv", "CXXFLAGS=-O0 -g"]).build()
    assert artifact.success
    assert runs.tool(0) == "configure"
    assert f"--prefix={tmp_path / 'riscv'}" in runs.argvs[0]
    assert runs.argvs[0][-2:] == ["--with-isa=rv64gcv", "CXXFLAGS=-O0 -g"]
    assert runs.tool(1) == "make"


def test_missing_build_dir_gets_a_default_configure(tmp_path, monkeypatch):
    """A fresh checkout has sources but no build dir; that is a reason to
    configure with defaults, not to fail."""
    spike = _tree(tmp_path, build_dir=False)
    recorder = _Runs(spike)
    monkeypatch.setattr(sbn.subprocess, "run", recorder)
    assert _node(tmp_path).build().success
    assert recorder.tool(0) == "configure"


def test_missing_sources_are_fatal(tmp_path, monkeypatch):
    """Without sources the alternative is co-simulating against whatever
    library the image happens to carry, silently."""
    _tree(tmp_path, sources=False)
    monkeypatch.setattr(sbn.subprocess, "run", _Runs(None))
    artifact = _node(tmp_path).build()
    assert not artifact.success and "sources" in artifact.stderr


def test_configure_failure_stops_before_make(tmp_path, monkeypatch):
    spike = _tree(tmp_path)
    recorder = _Runs(spike, fail={"configure"})
    monkeypatch.setattr(sbn.subprocess, "run", recorder)
    artifact = _node(tmp_path, configure_args=["--bogus"]).build()
    assert not artifact.success
    assert [recorder.tool(i) for i in range(len(recorder.argvs))] == ["configure"]


def test_make_failure_is_fatal(tmp_path, monkeypatch):
    """And reported as the make failure it is - not as a downstream symptom
    like a missing library, which would send whoever reads it hunting in the
    wrong place."""
    spike = _tree(tmp_path)
    monkeypatch.setattr(sbn.subprocess, "run", _Runs(spike, fail={"make"}))
    artifact = _node(tmp_path).build()
    assert not artifact.success
    assert "rebuilding libriscv failed" in artifact.stderr


def test_stale_library_is_rejected(tmp_path, runs):
    """make being happy is not evidence the library contains the edit: a tree
    patched after the last build otherwise ships the pre-patch model and every
    divergence the patch removes gets re-reported as new."""
    node = _node(tmp_path)
    assert node.build().success
    lib = (tmp_path / "toolchains" / "riscv-tools" / "riscv-isa-sim" /
           "build" / "libriscv.so")
    os.utime(lib, (OLD - 10, OLD - 10))    # library now predates the sources
    artifact = node.build()                # fake make leaves the old library
    assert not artifact.success and "older" in artifact.stderr


def test_static_build_names_every_archive(tmp_path, runs):
    """libriscv.a alone leaves ~900 undefined softfloat symbols; the make
    targets have to name all four archives or the link step cannot resolve."""
    _node(tmp_path, build_static=True).build()
    make = runs.argvs[-1]
    for archive in STATIC_ARCHIVES:
        assert archive in make


def test_install_writes_the_artifacts_own_bytes(tmp_path, runs):
    """The library the simulator links against and the one that travels with it
    must be the same bytes, or the digest describes the wrong file."""
    (tmp_path / "riscv" / "lib").mkdir(parents=True)
    artifact = _node(tmp_path, install=True).build()
    installed = (tmp_path / "riscv" / "lib" / "libriscv.so").read_bytes()
    assert installed == artifact.lib_content == b"model"


def test_stage_golden_model_places_the_lib(tmp_path):
    (tmp_path / "riscv" / "lib").mkdir(parents=True)
    artifact = SpikeBuildArtifact(
        success=True, lib_name="libriscv.so", lib_content=b"travelled",
        digest=hashlib.sha256(b"travelled").hexdigest(),
        spike_bin="", stdout="", stderr="", returncode=0)
    stage_golden_model(artifact, str(tmp_path / "riscv"))
    assert (tmp_path / "riscv" / "lib" / "libriscv.so").read_bytes() == b"travelled"
