"""Unit tests for lef_gen: LEF macro text and pin enumeration."""

import pytest

from chia.vlsi.sram_cacti.lef_gen import generate_lef, _enumerate_pins


class TestGenerateLef:
    def test_macro_header(self, make_spec, make_result):
        lef = generate_lef(make_spec(name="btb"), make_result(width_mm=0.2, height_mm=0.1))
        assert "MACRO cacti_btb" in lef
        assert "CLASS BLOCK BLACKBOX ;" in lef
        assert "SIZE 200.000 BY 100.000 ;" in lef   # mm * 1000
        assert "FOREIGN cacti_btb" in lef

    def test_power_pins(self, make_spec, make_result):
        lef = generate_lef(make_spec(), make_result())
        assert "PIN VDD" in lef and "USE POWER ;" in lef
        assert "PIN VSS" in lef and "USE GROUND ;" in lef

    def test_default_layers(self, make_spec, make_result):
        lef = generate_lef(make_spec(), make_result())
        assert "LAYER met4 ;" in lef               # default pin layer
        for obs in ("met1", "met2", "met3"):
            assert f"LAYER {obs} ;" in lef

    def test_pin_layer_configurable(self, make_spec, make_result):
        lef = generate_lef(make_spec(), make_result(), pin_layer="met2")
        assert "LAYER met2 ;" in lef

    def test_cell_prefix_empty(self, make_spec, make_result):
        lef = generate_lef(make_spec(name="x"), make_result(), cell_prefix="")
        assert "MACRO x" in lef

    def test_clock_pin_use_clock(self, make_spec, make_result):
        lef = generate_lef(make_spec(ports="rw"), make_result())
        assert "PIN RW0_clk" in lef
        seg = lef[lef.index("PIN RW0_clk"):]
        assert "USE CLOCK ;" in seg[:200]

    def test_column_wrap_handles_many_pins(self, make_spec, make_result):
        # wide bus + very short macro forces the pin-column wrap branch
        spec = make_spec(depth=1024, width=128, ports="rw")
        lef = generate_lef(spec, make_result(height_mm=0.002, width_mm=0.002))
        assert lef.count("PIN ") > 100   # all pins emitted, no crash


class TestEnumeratePins:
    def test_rw(self, make_spec):
        pins = _enumerate_pins(make_spec(depth=64, width=8, ports="rw"))
        names = [n for n, _, _ in pins]
        assert ("RW0_clk", "INPUT", "CLOCK") in pins
        assert "RW0_addr[5]" in names           # addr_bits=6 -> [5:0]
        assert "RW0_wdata[7]" in names
        assert "RW0_rdata[0]" in names

    @pytest.mark.parametrize("ports,must_have", [
        ("read,write", ["R0_data[0]", "W0_data[0]"]),
        ("mrw", ["RW0_wmask[0]"]),
    ])
    def test_port_variants(self, make_spec, ports, must_have):
        names = [n for n, _, _ in _enumerate_pins(make_spec(width=32, mask_gran=8, ports=ports))]
        for m in must_have:
            assert m in names
