"""Unit tests for liberty_gen: corners + Liberty .lib text generation."""

import pytest

from chia.vlsi.sram_cacti.liberty_gen import (
    generate_liberty,
    LibertyCorner,
    SKY130_CORNERS,
)


class TestSky130Corners:
    def test_three_corners_and_suffixes(self):
        assert {c.lib_suffix for c in SKY130_CORNERS} == {
            "ff_n40C_1v95", "ss_100C_1v60", "tt_025C_1v80"
        }

    def test_corner_pvt_values(self):
        by = {c.name: c for c in SKY130_CORNERS}
        assert (by["tt"].temperature, by["tt"].voltage) == (25, 1.80)
        assert (by["ff"].temperature, by["ff"].voltage) == (-40, 1.95)
        assert (by["ss"].temperature, by["ss"].voltage) == (100, 1.60)


class TestGenerateLiberty:
    def test_default_corner_is_tt(self, make_spec, make_result):
        lib = generate_liberty(make_spec(), make_result())
        assert "nom_temperature : 25" in lib
        assert "nom_voltage : 1.8" in lib
        assert "PVT_1P8V_25C" in lib

    def test_explicit_corner(self, make_spec, make_result):
        ff = next(c for c in SKY130_CORNERS if c.name == "ff")
        lib = generate_liberty(make_spec(), make_result(), ff)
        assert "nom_temperature : -40" in lib
        assert "PVT_1P95V_-40C" in lib

    def test_cell_name_and_area(self, make_spec, make_result):
        lib = generate_liberty(make_spec(name="btb"), make_result(area_um2=1234.5), cell_prefix="cacti_")
        assert "library (cacti_btb)" in lib
        assert "cell (cacti_btb)" in lib
        assert "area : 1234.500" in lib

    def test_cell_prefix_empty(self, make_spec, make_result):
        lib = generate_liberty(make_spec(name="btb"), make_result(), cell_prefix="")
        assert "cell (btb)" in lib

    def test_rw_pins(self, make_spec, make_result):
        lib = generate_liberty(make_spec(ports="rw"), make_result())
        for pin in ["RW0_clk", "RW0_en", "RW0_wmode", "RW0_addr", "RW0_wdata", "RW0_rdata"]:
            assert pin in lib

    def test_read_write_pins(self, make_spec, make_result):
        lib = generate_liberty(make_spec(ports="read,write"), make_result())
        assert "R0_data" in lib   # reader output bus
        assert "W0_data" in lib   # writer input bus
        assert "R0_addr" in lib and "W0_addr" in lib

    def test_masked_write_has_wmask(self, make_spec, make_result):
        lib = generate_liberty(make_spec(ports="mrw", width=32, mask_gran=8), make_result())
        assert "RW0_wmask" in lib

    def test_no_mask_when_unmasked(self, make_spec, make_result):
        lib = generate_liberty(make_spec(ports="rw", width=32), make_result())
        assert "wmask" not in lib

    def test_addr_bus_width(self, make_spec, make_result):
        # depth 1024 -> addr_bits 10
        lib = generate_liberty(make_spec(depth=1024, ports="rw"), make_result())
        assert "bit_width : 10" in lib

    def test_timing_derived_from_access(self, make_spec, make_result):
        lib = generate_liberty(make_spec(ports="rw"), make_result(access_time_ns=2.0))
        assert 'values ("2.0000")' in lib   # clk_to_q == access_time_ns

    @pytest.mark.parametrize("ports,prefixes", [
        ("rw", ["RW0"]),
        ("read,write", ["R0", "W0"]),
        ("read,read,mwrite,mwrite", ["R0", "R1", "W0", "W1"]),
        ("rw,read", ["RW0", "R0"]),
    ])
    def test_port_prefixes(self, make_spec, make_result, ports, prefixes):
        lib = generate_liberty(make_spec(ports=ports, width=32, mask_gran=8), make_result())
        for p in prefixes:
            assert f"{p}_clk" in lib
