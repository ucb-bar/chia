"""Unit tests for the CACTI MacroCompiler MDF adapter."""

import json

import pytest

from chia.vlsi.sram_cacti.cacti_macrocompiler import generate_cacti_macrocompiler_lib


class TestGenerateCactiMacrocompilerLib:
    def test_valid_json_list(self, make_spec):
        data = json.loads(generate_cacti_macrocompiler_lib([make_spec(name="m")]))
        assert isinstance(data, list) and len(data) == 1

    def test_entry_fields(self, make_spec):
        data = json.loads(generate_cacti_macrocompiler_lib(
            [make_spec(name="m", depth=64, width=32, ports="rw")]))
        e = data[0]
        assert e["name"] == "cacti_m"
        assert e["type"] == "sram"
        assert e["depth"] == "64"     # depth is a string in MDF
        assert e["width"] == 32       # width is an int
        assert e["mux"] == 1
        assert e["vt"] == "svt"
        assert e["mask"] == "false"
        assert e["family"] == "1rw"
        assert len(e["ports"]) == 1

    def test_prefix_override(self, make_spec):
        data = json.loads(generate_cacti_macrocompiler_lib([make_spec(name="m")], cell_prefix="foo_"))
        assert data[0]["name"] == "foo_m"

    def test_mask_true_when_mask_gran(self, make_spec):
        data = json.loads(generate_cacti_macrocompiler_lib(
            [make_spec(name="m", ports="mrw", width=32, mask_gran=8)]))
        assert data[0]["mask"] == "true"

    @pytest.mark.parametrize("ports,family", [
        ("rw", "1rw"),
        ("read,write", "1r1w"),
        ("read,read,write", "2r1w"),
        ("rw,rw", "2rw"),
    ])
    def test_family(self, make_spec, ports, family):
        data = json.loads(generate_cacti_macrocompiler_lib([make_spec(ports=ports)]))
        assert data[0]["family"] == family
