"""Shared fixtures and factories for the sram_cacti unit tests."""

import subprocess

import pytest

from chia.chipyard.macrocompiler import SRAMSpec, rename_ports
from chia.vlsi.sram_cacti.cacti_runner import CACTIResult


# A realistic (trimmed) CACTI 7 stdout containing all five parsed lines.
SAMPLE_CACTI_STDOUT = """\
Cache Parameters:
  Total cache size (bytes): 8192
  Number of banks: 1

Access time (ns): 0.453
Cycle time (ns): 0.612
Total dynamic read energy per access (nJ): 0.0123
Total leakage power of a bank (mW): 1.234
Cache height x width (mm): 0.100000 x 0.200000
"""


@pytest.fixture
def sample_cacti_stdout():
    return SAMPLE_CACTI_STDOUT


def _build_spec(name="mem", depth=64, width=32, ports="rw", mask_gran=None):
    """Construct an SRAMSpec with parsed_ports / counts populated like parse_mems_conf."""
    parsed, num_r, num_w, num_rw = rename_ports(ports)
    return SRAMSpec(
        name=name, depth=depth, width=width, ports=ports, mask_gran=mask_gran,
        num_rw_ports=num_rw, num_read_ports=num_r, num_write_ports=num_w,
        parsed_ports=parsed,
    )


@pytest.fixture
def make_spec():
    """Factory fixture: make_spec(name=, depth=, width=, ports=, mask_gran=)."""
    return _build_spec


def _build_result(**overrides):
    defaults = dict(
        access_time_ns=1.0, cycle_time_ns=1.2, read_energy_nj=0.01,
        leakage_power_mw=0.5, height_mm=0.1, width_mm=0.2,
        area_um2=20000.0, full_out="fake CACTI output",
    )
    defaults.update(overrides)
    return CACTIResult(**defaults)


@pytest.fixture
def make_result():
    """Factory fixture: make_result(area_um2=, access_time_ns=, ...)."""
    return _build_result


@pytest.fixture
def fake_run(monkeypatch):
    """Patch subprocess.run (as used by cacti_runner) with a canned result.

    Usage:
        fake_run(stdout=SAMPLE_CACTI_STDOUT)            # rc 0
        fake_run(stdout=..., returncode=1)              # CACTI "fails"
        fake_run(raises=subprocess.TimeoutExpired(...)) # CACTI times out
    """
    import chia.vlsi.sram_cacti.cacti_runner as cr

    def _install(stdout="", returncode=0, raises=None):
        def _fake(*args, **kwargs):
            if raises is not None:
                raise raises
            return subprocess.CompletedProcess(args, returncode, stdout=stdout, stderr="")
        monkeypatch.setattr(cr.subprocess, "run", _fake)

    return _install
