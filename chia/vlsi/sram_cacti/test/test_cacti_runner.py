"""Unit tests for cacti_runner: config generation, CACTI invocation/parsing, fallback."""

import math
import subprocess

import pytest

from chia.vlsi.sram_cacti.cacti_runner import (
    run_cacti,
    analytical_area_estimate,
    _block_size_bytes,
    _generate_cacti_cfg,
)

CACTI_STDOUT_NO_DIMS = "Access time (ns): 0.453\n"
CACTI_STDOUT_MINIMAL = (
    "Access time (ns): 0.453\n"
    "Cache height x width (mm): 0.100000 x 0.200000\n"
)


class TestBlockSizeBytes:
    @pytest.mark.parametrize("width,expected", [
        (1, 1), (4, 1), (8, 1), (32, 4), (45, 6), (64, 8),
        (65, 9), (128, 16), (176, 22), (256, 32),
    ])
    def test_values(self, width, expected):
        # ceil(width/8), no power-of-two rounding, floored at 1 byte
        assert _block_size_bytes(width) == expected


class TestGenerateCactiCfg:
    def test_technology_plumbed(self, make_spec):
        assert "-technology (u) 0.045" in _generate_cacti_cfg(make_spec(), technology_um=0.045)

    def test_default_technology(self, make_spec):
        assert "-technology (u) 0.13" in _generate_cacti_cfg(make_spec())

    def test_bus_width_matches_spec(self, make_spec):
        assert "-output/input bus width 39" in _generate_cacti_cfg(make_spec(width=39))

    @pytest.mark.parametrize("width,exp_block", [(8, 1), (32, 4), (64, 8), (65, 9), (128, 16)])
    def test_block_size_is_ceil_bytes(self, make_spec, width, exp_block):
        # block size = ceil(width/8) bytes — no power-of-two padding
        assert f"-block size (bytes) {exp_block}" in _generate_cacti_cfg(make_spec(depth=100, width=width))

    def test_size_bytes_and_ram_type(self, make_spec):
        cfg = _generate_cacti_cfg(make_spec(depth=256, width=32))  # block 4B -> 1024
        assert "-size (bytes) 1024" in cfg
        assert '-cache type "ram"' in cfg


class TestRunCacti:
    def test_happy_path(self, make_spec, fake_run, sample_cacti_stdout):
        fake_run(stdout=sample_cacti_stdout)
        r = run_cacti(make_spec(width=32), cacti_path="/x/cacti")
        assert r is not None
        assert r.access_time_ns == 0.453
        assert r.cycle_time_ns == 0.612
        assert r.read_energy_nj == 0.0123
        assert r.leakage_power_mw == 1.234
        assert (r.height_mm, r.width_mm) == (0.1, 0.2)
        assert r.full_out == sample_cacti_stdout

    def test_area_is_raw_dimensions(self, make_spec, fake_run, sample_cacti_stdout):
        # area = height*width*1e6 = 0.1*0.2*1e6 = 20000, with no padding/correction
        fake_run(stdout=sample_cacti_stdout)
        r = run_cacti(make_spec(width=32), cacti_path="/x/cacti")
        assert r.area_um2 == pytest.approx(20000.0)

    def test_returncode_nonzero_returns_none(self, make_spec, fake_run, sample_cacti_stdout):
        fake_run(stdout=sample_cacti_stdout, returncode=1)
        assert run_cacti(make_spec(), cacti_path="/x/cacti") is None

    def test_timeout_returns_none(self, make_spec, fake_run):
        fake_run(raises=subprocess.TimeoutExpired(cmd="cacti", timeout=30))
        assert run_cacti(make_spec(), cacti_path="/x/cacti") is None

    def test_missing_dimensions_returns_none(self, make_spec, fake_run):
        fake_run(stdout=CACTI_STDOUT_NO_DIMS)
        assert run_cacti(make_spec(), cacti_path="/x/cacti") is None

    def test_optional_fields_default_to_zero(self, make_spec, fake_run):
        fake_run(stdout=CACTI_STDOUT_MINIMAL)
        r = run_cacti(make_spec(width=32), cacti_path="/x/cacti")
        assert r is not None
        assert r.access_time_ns == 0.453
        assert r.cycle_time_ns == 0.0
        assert r.read_energy_nj == 0.0
        assert r.leakage_power_mw == 0.0

    def test_scientific_notation(self, make_spec, fake_run):
        fake_run(stdout="Access time (ns): 1.2e-03\nCache height x width (mm): 1.0e-01 x 2.0e-01\n")
        r = run_cacti(make_spec(width=32), cacti_path="/x/cacti")
        assert r.access_time_ns == pytest.approx(0.0012)

    @pytest.mark.xfail(reason="run_cacti does not yet catch FileNotFoundError for a missing binary",
                       strict=False)
    def test_missing_binary_returns_none(self, make_spec):
        # No mock: real subprocess against a nonexistent path (and nonexistent cwd).
        assert run_cacti(make_spec(), cacti_path="/nonexistent/dir/cacti") is None


class TestAnalyticalAreaEstimate:
    def test_area_formula(self, make_spec):
        r = analytical_area_estimate(make_spec(depth=64, width=32), cell_area_per_bit_um2=2.0)
        assert r.area_um2 == 64 * 32 * 2.0
        assert r.height_mm == pytest.approx(math.sqrt(64 * 32 * 2.0) / 1000)
        assert r.width_mm == r.height_mm

    def test_access_time(self, make_spec):
        r = analytical_area_estimate(make_spec(depth=1024, width=8))
        assert r.access_time_ns == pytest.approx(0.5 + 0.01 * math.log2(1024))
        assert r.cycle_time_ns == pytest.approx(r.access_time_ns * 1.2)

    def test_depth_one_edge(self, make_spec):
        r = analytical_area_estimate(make_spec(depth=1, width=8))
        assert r.access_time_ns == pytest.approx(0.5 + 0.01 * math.log2(2))

    def test_full_out_populated(self, make_spec):
        r = analytical_area_estimate(make_spec())
        assert isinstance(r.full_out, str) and r.full_out
