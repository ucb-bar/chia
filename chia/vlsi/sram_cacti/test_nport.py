"""Tests for N-port SRAM support in the CACTI pipeline.

Validates that rename_ports(), generate_macro_stubs(),
generate_cacti_macrocompiler_lib(), generate_liberty(), and generate_lef()
correctly handle arbitrary port combinations including 3+ port SRAMs.
"""

import json
import math
import re
import pytest

from chia.vlsi.sram_cacti.cacti_runner import (
    SRAMSpec,
    CACTIResult,
    rename_ports,
    parse_mems_conf,
)
from chia.vlsi.sram_cacti.lef_gen import generate_lef
from chia.vlsi.sram_cacti.liberty_gen import generate_liberty
from chia.chipyard.macrocompiler import generate_macro_stubs
from chia.vlsi.sram_cacti.sram_characterize import (
    generate_cacti_macrocompiler_lib,
    _compute_family,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_spec(name: str, depth: int, width: int, ports_str: str,
               mask_gran: int | None = None) -> SRAMSpec:
    """Create an SRAMSpec by parsing a ports string, as parse_mems_conf does."""
    parsed_ports, num_r, num_w, num_rw = rename_ports(ports_str)
    return SRAMSpec(
        name=name, depth=depth, width=width, ports=ports_str,
        mask_gran=mask_gran,
        num_rw_ports=num_rw, num_read_ports=num_r, num_write_ports=num_w,
        parsed_ports=parsed_ports,
    )


def _make_cacti_result() -> CACTIResult:
    """Create a dummy CACTIResult for Liberty generation tests."""
    return CACTIResult(
        access_time_ns=1.0, cycle_time_ns=1.2,
        read_energy_nj=0.01, leakage_power_mw=0.001,
        height_mm=0.1, width_mm=0.1, area_um2=10000.0,
        full_out="dummy",
    )


# ---------------------------------------------------------------------------
# rename_ports tests
# ---------------------------------------------------------------------------

class TestRenamePorts:
    def test_single_rw(self):
        parsed, nr, nw, nrw = rename_ports("rw")
        assert parsed == [("RW0", False)]
        assert (nr, nw, nrw) == (0, 0, 1)

    def test_single_mrw(self):
        parsed, nr, nw, nrw = rename_ports("mrw")
        assert parsed == [("RW0", True)]
        assert (nr, nw, nrw) == (0, 0, 1)

    def test_write_read(self):
        parsed, nr, nw, nrw = rename_ports("write,read")
        assert parsed == [("W0", False), ("R0", False)]
        assert (nr, nw, nrw) == (1, 1, 0)

    def test_mwrite_read(self):
        parsed, nr, nw, nrw = rename_ports("mwrite,read")
        assert parsed == [("W0", True), ("R0", False)]
        assert (nr, nw, nrw) == (1, 1, 0)

    def test_write_write_read(self):
        """The motivating 3-port case: ITTAGE table_ext."""
        parsed, nr, nw, nrw = rename_ports("write,write,read")
        assert parsed == [("W0", False), ("W1", False), ("R0", False)]
        assert (nr, nw, nrw) == (1, 2, 0)

    def test_read_read_mwrite_mwrite(self):
        parsed, nr, nw, nrw = rename_ports("read,read,mwrite,mwrite")
        assert parsed == [("R0", False), ("R1", False), ("W0", True), ("W1", True)]
        assert (nr, nw, nrw) == (2, 2, 0)

    def test_rw_read(self):
        parsed, nr, nw, nrw = rename_ports("rw,read")
        assert parsed == [("RW0", False), ("R0", False)]
        assert (nr, nw, nrw) == (1, 0, 1)

    def test_dual_rw(self):
        parsed, nr, nw, nrw = rename_ports("rw,rw")
        assert parsed == [("RW0", False), ("RW1", False)]
        assert (nr, nw, nrw) == (0, 0, 2)


# ---------------------------------------------------------------------------
# parse_mems_conf integration tests
# ---------------------------------------------------------------------------

class TestParseMemsConf:
    def test_standard_conf_lines(self):
        content = (
            "name foo_ext depth 128 width 44 ports mwrite,read mask_gran 11\n"
            "name bar_ext depth 256 width 64 ports rw\n"
        )
        specs = parse_mems_conf(content)
        assert len(specs) == 2

        assert specs[0].name == "foo_ext"
        assert specs[0].parsed_ports == [("W0", True), ("R0", False)]
        assert specs[0].num_write_ports == 1
        assert specs[0].num_read_ports == 1

        assert specs[1].name == "bar_ext"
        assert specs[1].parsed_ports == [("RW0", False)]
        assert specs[1].num_rw_ports == 1

    def test_three_port_conf_line(self):
        content = "name table_ext depth 128 width 56 ports write,write,read\n"
        specs = parse_mems_conf(content)
        assert len(specs) == 1
        spec = specs[0]
        assert spec.parsed_ports == [("W0", False), ("W1", False), ("R0", False)]
        assert spec.num_read_ports == 1
        assert spec.num_write_ports == 2
        assert spec.num_rw_ports == 0


# ---------------------------------------------------------------------------
# generate_macro_stubs tests
# ---------------------------------------------------------------------------

class TestGenerateVerilogStubs:
    def test_rw_stub(self):
        spec = _make_spec("mem_ext", 256, 64, "rw")
        stubs = generate_macro_stubs([spec], "cacti_")
        assert len(stubs) == 1
        fname, content = stubs[0]
        assert fname == "cacti_mem_ext.v"
        assert "module cacti_mem_ext(" in content
        assert "RW0_addr" in content
        assert "RW0_clk" in content
        assert "RW0_en" in content
        assert "RW0_wmode" in content
        assert "RW0_wdata" in content
        assert "RW0_rdata" in content
        assert "output" in content  # has output pin

    def test_mrw_stub_has_wmask(self):
        spec = _make_spec("mem_ext", 256, 64, "mrw", mask_gran=8)
        stubs = generate_macro_stubs([spec], "cacti_")
        _, content = stubs[0]
        assert "RW0_wmask" in content

    def test_mwrite_read_stub(self):
        spec = _make_spec("table_ext", 128, 44, "mwrite,read", mask_gran=11)
        stubs = generate_macro_stubs([spec], "cacti_")
        _, content = stubs[0]
        assert "W0_addr" in content
        assert "W0_data" in content
        assert "W0_mask" in content
        assert "R0_addr" in content
        assert "R0_data" in content
        assert "output" in content

    def test_write_read_stub(self):
        spec = _make_spec("ebtb_ext", 128, 40, "write,read")
        stubs = generate_macro_stubs([spec], "cacti_")
        _, content = stubs[0]
        assert "W0_addr" in content
        assert "R0_data" in content
        assert "W0_mask" not in content  # no mask

    def test_three_port_stub(self):
        """The motivating case: write,write,read produces W0, W1, R0 ports."""
        spec = _make_spec("table_ext", 128, 56, "write,write,read")
        stubs = generate_macro_stubs([spec], "cacti_")
        assert len(stubs) == 1
        fname, content = stubs[0]
        assert fname == "cacti_table_ext.v"
        # All three ports present
        assert "W0_addr" in content
        assert "W0_clk" in content
        assert "W0_data" in content
        assert "W1_addr" in content
        assert "W1_clk" in content
        assert "W1_data" in content
        assert "R0_addr" in content
        assert "R0_clk" in content
        assert "R0_data" in content
        # Has output pin (R0_data)
        assert "output" in content
        # Not empty
        assert content.strip() != "module cacti_table_ext(\n\n);\nendmodule"

    def test_four_port_stub(self):
        spec = _make_spec("big_ext", 64, 32, "read,read,mwrite,mwrite", mask_gran=8)
        stubs = generate_macro_stubs([spec], "cacti_")
        _, content = stubs[0]
        assert "R0_data" in content
        assert "R1_data" in content
        assert "W0_data" in content
        assert "W1_data" in content
        assert "W0_mask" in content
        assert "W1_mask" in content


# ---------------------------------------------------------------------------
# generate_liberty tests
# ---------------------------------------------------------------------------

class TestGenerateLiberty:
    def test_rw_liberty_has_output(self):
        spec = _make_spec("mem_ext", 256, 64, "rw")
        result = _make_cacti_result()
        lib = generate_liberty(spec, result)
        assert "RW0_rdata" in lib
        assert "direction : output" in lib
        assert "area : 10000.000" in lib

    def test_mwrite_read_liberty_has_output(self):
        spec = _make_spec("table_ext", 128, 44, "mwrite,read", mask_gran=11)
        result = _make_cacti_result()
        lib = generate_liberty(spec, result)
        assert "R0_data" in lib
        assert "W0_data" in lib
        assert "W0_mask" in lib
        assert "direction : output" in lib

    def test_three_port_liberty_has_all_pins(self):
        """write,write,read Liberty has output on R0 and inputs on W0, W1."""
        spec = _make_spec("table_ext", 128, 56, "write,write,read")
        result = _make_cacti_result()
        lib = generate_liberty(spec, result)
        # All three ports have clock and enable
        assert "W0_clk" in lib
        assert "W1_clk" in lib
        assert "R0_clk" in lib
        # R0 has output
        assert "R0_data" in lib
        # W0, W1 have input data
        assert "W0_data" in lib
        assert "W1_data" in lib
        # Has at least one output direction (from R0_data)
        assert "direction : output" in lib

    def test_mrw_liberty_has_mask(self):
        spec = _make_spec("mem_ext", 256, 64, "mrw", mask_gran=8)
        result = _make_cacti_result()
        lib = generate_liberty(spec, result)
        assert "RW0_wmask" in lib


# ---------------------------------------------------------------------------
# generate_cacti_macrocompiler_lib tests
# ---------------------------------------------------------------------------

class TestGenerateMacroCompilerLib:
    def test_rw_lib(self):
        spec = _make_spec("mem_ext", 256, 64, "rw")
        lib_json = generate_cacti_macrocompiler_lib([spec])
        entries = json.loads(lib_json)
        assert len(entries) == 1
        entry = entries[0]
        assert entry["name"] == "cacti_mem_ext"
        assert entry["family"] == "1rw"
        assert len(entry["ports"]) == 1
        assert entry["ports"][0]["address port name"] == "RW0_addr"

    def test_mwrite_read_lib(self):
        spec = _make_spec("table_ext", 128, 44, "mwrite,read", mask_gran=11)
        lib_json = generate_cacti_macrocompiler_lib([spec])
        entries = json.loads(lib_json)
        entry = entries[0]
        assert entry["family"] == "1r1w"
        assert len(entry["ports"]) == 2
        port_names = {p["address port name"] for p in entry["ports"]}
        assert port_names == {"W0_addr", "R0_addr"}
        # Masked write port has mask
        w_port = next(p for p in entry["ports"] if p["address port name"] == "W0_addr")
        assert w_port["mask port name"] == "W0_mask"
        assert w_port["mask granularity"] == 11

    def test_three_port_lib(self):
        spec = _make_spec("table_ext", 128, 56, "write,write,read")
        lib_json = generate_cacti_macrocompiler_lib([spec])
        entries = json.loads(lib_json)
        entry = entries[0]
        assert entry["family"] == "1r2w"
        assert len(entry["ports"]) == 3
        port_names = {p["address port name"] for p in entry["ports"]}
        assert port_names == {"W0_addr", "W1_addr", "R0_addr"}

    def test_four_port_lib(self):
        spec = _make_spec("big_ext", 64, 32, "read,read,mwrite,mwrite", mask_gran=8)
        lib_json = generate_cacti_macrocompiler_lib([spec])
        entries = json.loads(lib_json)
        entry = entries[0]
        assert entry["family"] == "2r2w"
        assert len(entry["ports"]) == 4


# ---------------------------------------------------------------------------
# _compute_family tests
# ---------------------------------------------------------------------------

class TestComputeFamily:
    def test_single_rw(self):
        spec = _make_spec("x", 8, 8, "rw")
        assert _compute_family(spec) == "1rw"

    def test_write_read(self):
        spec = _make_spec("x", 8, 8, "write,read")
        assert _compute_family(spec) == "1r1w"

    def test_write_write_read(self):
        spec = _make_spec("x", 8, 8, "write,write,read")
        assert _compute_family(spec) == "1r2w"

    def test_mixed(self):
        spec = _make_spec("x", 8, 8, "rw,read")
        assert _compute_family(spec) == "1r1rw"


# ---------------------------------------------------------------------------
# generate_lef tests
# ---------------------------------------------------------------------------

class TestGenerateLef:
    def test_rw_lef_basic(self):
        spec = _make_spec("mem_ext", 256, 64, "rw")
        result = _make_cacti_result()
        lef = generate_lef(spec, result)
        assert 'MACRO cacti_mem_ext' in lef
        assert 'CLASS BLOCK BLACKBOX' in lef
        assert 'SIZE 100.000 BY 100.000' in lef  # 0.1mm * 1000 = 100 um
        assert 'BUSBITCHARS "[]"' in lef
        assert 'PIN VDD' in lef
        assert 'USE POWER' in lef
        assert 'PIN VSS' in lef
        assert 'USE GROUND' in lef
        assert 'RW0_clk' in lef
        assert 'RW0_en' in lef
        assert 'RW0_wmode' in lef
        assert 'RW0_addr[0]' in lef
        assert 'RW0_wdata[0]' in lef
        assert 'RW0_rdata[0]' in lef
        assert 'DIRECTION OUTPUT' in lef
        assert 'OBS' in lef
        assert 'LAYER met1' in lef
        assert 'END LIBRARY' in lef

    def test_mrw_lef_has_wmask(self):
        spec = _make_spec("mem_ext", 256, 64, "mrw", mask_gran=8)
        result = _make_cacti_result()
        lef = generate_lef(spec, result)
        assert 'RW0_wmask[0]' in lef
        assert 'RW0_wmask[7]' in lef

    def test_mwrite_read_lef(self):
        spec = _make_spec("table_ext", 128, 44, "mwrite,read", mask_gran=11)
        result = _make_cacti_result()
        lef = generate_lef(spec, result)
        assert 'W0_mask[0]' in lef
        assert 'W0_data[0]' in lef
        assert 'R0_data[0]' in lef
        # R0_data should be OUTPUT
        r0_data_match = re.search(
            r'PIN R0_data\[0\]\s+DIRECTION (\w+)', lef)
        assert r0_data_match and r0_data_match.group(1) == "OUTPUT"
        # W0_data should be INPUT
        w0_data_match = re.search(
            r'PIN W0_data\[0\]\s+DIRECTION (\w+)', lef)
        assert w0_data_match and w0_data_match.group(1) == "INPUT"

    def test_three_port_lef(self):
        spec = _make_spec("table_ext", 128, 56, "write,write,read")
        result = _make_cacti_result()
        lef = generate_lef(spec, result)
        assert 'W0_addr[0]' in lef
        assert 'W0_clk' in lef
        assert 'W1_addr[0]' in lef
        assert 'W1_clk' in lef
        assert 'R0_addr[0]' in lef
        assert 'R0_clk' in lef
        assert 'R0_data[0]' in lef
        assert 'DIRECTION OUTPUT' in lef

    def test_lef_dimensions(self):
        spec = _make_spec("mem_ext", 256, 64, "rw")
        result = CACTIResult(
            access_time_ns=1.0, cycle_time_ns=1.2,
            read_energy_nj=0.01, leakage_power_mw=0.001,
            height_mm=0.25, width_mm=0.15, area_um2=37500.0,
            full_out="dummy",
        )
        lef = generate_lef(spec, result)
        assert 'SIZE 150.000 BY 250.000' in lef

    def test_lef_pin_layer_configurable(self):
        spec = _make_spec("mem_ext", 256, 64, "rw")
        result = _make_cacti_result()
        lef = generate_lef(spec, result, pin_layer="M4",
                           obs_layers=["M1", "M2", "M3"])
        assert 'LAYER M4' in lef
        assert 'LAYER M1' in lef
        assert 'LAYER M2' in lef
        assert 'LAYER M3' in lef
        assert 'LAYER met4' not in lef
        assert 'LAYER met1' not in lef

    def test_lef_pin_names_match_liberty(self):
        """Pin names in LEF should match pin names in Liberty."""
        spec = _make_spec("mem_ext", 256, 64, "mrw", mask_gran=8)
        result = _make_cacti_result()
        lef = generate_lef(spec, result)
        lib = generate_liberty(spec, result)

        # Extract pin names from LEF (PIN <name> lines)
        lef_pins = set(re.findall(r'^\s+PIN (\S+)$', lef, re.MULTILINE))
        lef_pins -= {"VDD", "VSS"}  # power pins are pg_pin in Liberty

        # Extract pin names from Liberty: all pin() entries (scalar and
        # per-bit inside bus definitions) give us the full set.
        lib_pins = set(re.findall(r'\bpin \((\S+?)\)', lib))

        assert lef_pins == lib_pins, (
            f"Pin mismatch: LEF-only={lef_pins - lib_pins}, "
            f"Liberty-only={lib_pins - lef_pins}")
