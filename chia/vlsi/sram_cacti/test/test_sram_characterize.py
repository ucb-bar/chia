"""Unit tests for the sram_characterize orchestration layer."""

import pytest

from chia.vlsi.sram_cacti import sram_characterize as sc
from chia.vlsi.sram_cacti.sram_characterize import (
    CharacterizedSRAM,
    CactiCharacterization,
    characterize_srams_with_cacti,
    characterize_top_mems_conf_with_cacti,
)
from chia.vlsi.sram_cacti.liberty_gen import SKY130_CORNERS


class TestDataclasses:
    def test_to_lib_dict(self, make_spec, make_result):
        cs = CharacterizedSRAM(name="m", spec=make_spec(name="m"), result=make_result(),
                               lib_contents={"tt_025C_1v80": "<lib>"}, lef_content="<lef>")
        assert cs.to_lib_dict() == {
            "name": "m", "lib_contents": {"tt_025C_1v80": "<lib>"}, "lef_content": "<lef>",
        }

    def test_views_derive_from_srams(self, make_spec, make_result):
        cs = CharacterizedSRAM(name="m", spec=make_spec(name="m"), result=make_result(),
                               lib_contents={}, lef_content="")
        char = CactiCharacterization(generated_src_files=[("a", "b")], srams=[cs])
        assert char.sram_names == ["m"]
        assert char.sram_libs == [cs.to_lib_dict()]

    def test_empty_default(self):
        char = CactiCharacterization(generated_src_files=[])
        assert char.srams == [] and char.sram_names == [] and char.sram_libs == []


@pytest.fixture
def patch_run_cacti(monkeypatch):
    """Replace run_cacti (as used by sram_characterize) with a canned result."""
    def _install(result):
        monkeypatch.setattr(sc, "run_cacti", lambda spec, **kw: result)
    return _install


class TestCharacterizeSrams:
    def test_tiny_large_split(self, make_spec, patch_run_cacti, make_result):
        patch_run_cacti(make_result())
        tiny = make_spec(name="t", depth=4, width=8)       # 4 bytes
        big = make_spec(name="b", depth=1024, width=64)    # 8192 bytes
        out = characterize_srams_with_cacti([tiny, big], [], cacti_path="x")
        assert out.sram_names == ["b"]

    def test_threshold_boundary_is_inclusive(self, make_spec, patch_run_cacti, make_result):
        patch_run_cacti(make_result())
        spec = make_spec(name="m", depth=256, width=8)     # exactly 256 bytes
        out = characterize_srams_with_cacti([spec], [], cacti_path="x", synflop_threshold_bytes=256)
        assert out.sram_names == ["m"]                     # >= threshold -> characterized

    def test_all_tiny(self, make_spec, patch_run_cacti, make_result):
        patch_run_cacti(make_result())
        out = characterize_srams_with_cacti([make_spec(depth=2, width=8)], [], cacti_path="x")
        assert out.srams == []

    def test_empty_specs(self):
        assert characterize_srams_with_cacti([], [], cacti_path="x").srams == []

    def test_analytical_fallback_when_run_cacti_none(self, make_spec, monkeypatch):
        monkeypatch.setattr(sc, "run_cacti", lambda spec, **kw: None)
        out = characterize_srams_with_cacti([make_spec(depth=1024, width=64)], [], cacti_path="x")
        assert len(out.srams) == 1
        assert out.srams[0].result.area_um2 > 0            # analytical estimate filled in

    def test_corners_default_three(self, make_spec, patch_run_cacti, make_result):
        patch_run_cacti(make_result())
        out = characterize_srams_with_cacti([make_spec(depth=1024, width=64)], [], cacti_path="x")
        assert set(out.srams[0].lib_contents) == {"ff_n40C_1v95", "ss_100C_1v60", "tt_025C_1v80"}

    def test_corners_custom_single(self, make_spec, patch_run_cacti, make_result):
        patch_run_cacti(make_result())
        tt = next(c for c in SKY130_CORNERS if c.name == "tt")
        out = characterize_srams_with_cacti(
            [make_spec(depth=1024, width=64)], [], cacti_path="x", corners=[tt])
        assert list(out.srams[0].lib_contents) == ["tt_025C_1v80"]

    def test_generated_src_passthrough(self, make_spec, patch_run_cacti, make_result):
        patch_run_cacti(make_result())
        gen = [("Top.sv", "module")]
        out = characterize_srams_with_cacti([make_spec(depth=1024, width=64)], gen, cacti_path="x")
        assert out.generated_src_files is gen

    def test_cell_prefix_plumbing(self, make_spec, patch_run_cacti, make_result):
        patch_run_cacti(make_result())
        out = characterize_srams_with_cacti(
            [make_spec(name="m", depth=1024, width=64)], [], cacti_path="x", cell_prefix="cacti_")
        assert "cacti_m" in out.srams[0].lib_contents["tt_025C_1v80"]


class TestCharacterizeTopMemsConf:
    def test_no_conf_returns_empty_passthrough(self):
        gen = [("foo.sv", "x"), ("bar.v", "y")]
        out = characterize_top_mems_conf_with_cacti(gen, cacti_path="x")
        assert out.srams == []
        assert out.generated_src_files is gen

    def test_parses_and_delegates(self, monkeypatch):
        captured = {}

        def fake_char(specs, gen, cacti_path, **kw):
            captured["specs"] = specs
            captured["kw"] = kw
            return CactiCharacterization(generated_src_files=gen)

        monkeypatch.setattr(sc, "characterize_srams_with_cacti", fake_char)
        conf = "name big depth 1024 width 64 ports rw\nname tiny depth 4 width 8 ports rw"
        characterize_top_mems_conf_with_cacti(
            [("Top.top.mems.conf", conf)], cacti_path="cpath",
            synflop_threshold_bytes=128, technology_um=0.045,
            cell_prefix="pre_", corners=["c1"])
        assert [s.name for s in captured["specs"]] == ["big", "tiny"]
        assert captured["kw"]["synflop_threshold_bytes"] == 128
        assert captured["kw"]["technology_um"] == 0.045
        assert captured["kw"]["cell_prefix"] == "pre_"
        assert captured["kw"]["corners"] == ["c1"]

    def test_end_to_end_with_fake_cacti(self, monkeypatch, make_result):
        monkeypatch.setattr(sc, "run_cacti", lambda spec, **kw: make_result())
        conf = "name big depth 1024 width 64 ports rw\nname tiny depth 4 width 8 ports rw"
        out = characterize_top_mems_conf_with_cacti([("Top.top.mems.conf", conf)], cacti_path="x")
        assert out.sram_names == ["big"]
        assert set(out.srams[0].lib_contents) == {"ff_n40C_1v95", "ss_100C_1v60", "tt_025C_1v80"}
        assert out.srams[0].lef_content
