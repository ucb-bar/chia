"""The build configs BitstreamBuildNode hands to `firesim buildbitstream`."""

import yaml

from chia.firesim import ecad_node
from chia.firesim.state_def import BuildRecipe


def test_configs_carry_every_key_buildbitstream_reads(tmp_path, monkeypatch):
    monkeypatch.setattr(ecad_node, "DEPLOY", str(tmp_path))
    (tmp_path / "built-hwdb-entries").mkdir()
    (tmp_path / "built-hwdb-entries" / "r").write_text("stale")

    ecad_node.BitstreamBuildNode._write_configs(BuildRecipe(name="r"))

    build = yaml.safe_load((tmp_path / "config_build.yaml").read_text())
    assert build["builds_to_run"] == ["r"]
    assert build["agfis_to_share"] == [] and build["share_with_accounts"] == {}
    # Vivado runs on the host, which is localhost under --net=host.
    assert build["build_farm"]["recipe_arg_overrides"]["build_farm_hosts"] == [
        "ubuntu@localhost"]
    # BuildConfigFile opens the hwdb too, and fails on an empty file.
    assert yaml.safe_load((tmp_path / "config_hwdb.yaml").read_text()) == {}

    recipe = yaml.safe_load((tmp_path / "config_build_recipes.yaml").read_text())["r"]
    for key in ("DESIGN", "TARGET_CONFIG", "PLATFORM_CONFIG", "post_build_hook",
                "platform_config_args", "bit_builder_recipe"):
        assert key in recipe
    assert recipe["bit_builder_recipe"] == "bit-builder-recipes/f2.yaml"
    # A stale AGFI entry would be read back as this build's result.
    assert not (tmp_path / "built-hwdb-entries" / "r").exists()
