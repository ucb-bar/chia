"""Cross-function consistency and end-to-end tests for the CACTI pipeline.

These guard the invariant that matters most for synthesis: the pin names emitted
by the Liberty cell, the LEF macro, and the Verilog blackbox stub must agree.
"""

import re

import pytest

from chia.vlsi.sram_cacti.liberty_gen import generate_liberty
from chia.vlsi.sram_cacti.lef_gen import generate_lef, _enumerate_pins
from chia.chipyard.macrocompiler import generate_macro_stubs
from chia.vlsi.sram_cacti import sram_characterize as sc
from chia.vlsi.sram_cacti.sram_characterize import characterize_top_mems_conf_with_cacti

PORT_CONFIGS = ["rw", "read,write", "mrw", "read,read,mwrite", "rw,read"]


def _pin_bases(spec):
    """Base signal-pin names (index stripped) from the LEF enumeration."""
    return {re.sub(r"\[\d+\]$", "", n) for n, _, _ in _enumerate_pins(spec)}


class TestPinConsistency:
    @pytest.mark.parametrize("ports", PORT_CONFIGS)
    def test_lef_pins_present_in_stub(self, make_spec, ports):
        spec = make_spec(width=32, mask_gran=8, ports=ports)
        stub = dict(generate_macro_stubs([spec], "cacti_"))[f"cacti_{spec.name}.v"]
        for base in _pin_bases(spec):
            assert base in stub, f"{base} missing from Verilog stub"

    @pytest.mark.parametrize("ports", PORT_CONFIGS)
    def test_lef_pins_present_in_liberty(self, make_spec, make_result, ports):
        spec = make_spec(width=32, mask_gran=8, ports=ports)
        lib = generate_liberty(spec, make_result())
        for base in _pin_bases(spec):
            assert base in lib, f"{base} missing from Liberty cell"


class TestEndToEnd:
    def test_full_flow(self, monkeypatch, make_result):
        monkeypatch.setattr(sc, "run_cacti", lambda spec, **kw: make_result(area_um2=5000.0))
        conf = "\n".join([
            "name dcache depth 2048 width 64 ports rw",
            "name icache depth 1024 width 128 ports read,write",
            "name tiny depth 8 width 8 ports rw",
        ])
        out = characterize_top_mems_conf_with_cacti(
            [("Top.top.mems.conf", conf)], cacti_path="x")

        assert set(out.sram_names) == {"dcache", "icache"}   # tiny stays a synflop
        for lib in out.sram_libs:
            assert set(lib["lib_contents"]) == {"ff_n40C_1v95", "ss_100C_1v60", "tt_025C_1v80"}
            assert lib["lef_content"]
            assert lib["name"] in {"dcache", "icache"}


class TestReExports:
    def test_cacti_runner_reexports(self):
        from chia.vlsi.sram_cacti import cacti_runner as cr
        from chia.chipyard import macrocompiler as mc
        assert cr.parse_mems_conf is mc.parse_mems_conf
        assert cr.rename_ports is mc.rename_ports
        assert cr.SRAMSpec is mc.SRAMSpec

    def test_sram_characterize_reexports(self):
        from chia.chipyard import macrocompiler as mc
        assert sc.assemble_generated_src_with_cacti is mc.assemble_generated_src_with_macros
        assert sc.remap_with_macrocompiler is mc.remap_with_macrocompiler
        assert sc._compute_family is mc._compute_family
