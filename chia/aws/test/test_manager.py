import pytest
from chia.aws.manager import AWSWorkerSpec
from chia.firesim.specs import ECAD, F2_SIM
from chia.firesim.state_def import BuildRecipe


def test_specs_produce_distinct_node_types():
    for spec in (F2_SIM, ECAD):
        nt = spec.node_type()
        assert nt.name == spec.name
        assert nt.resources == spec.resources
        assert nt.docker.image == spec.image
        # DockerManager adds --net=host itself; the spec must not repeat it.
        assert "--net=host" not in nt.docker.run_options
    assert F2_SIM.resources != ECAD.resources


def test_only_the_sim_worker_reaches_its_host():
    # The run manager ssh's to the instance for the FPGA; the build runs
    # entirely in its container, Vivado mounted in.
    assert F2_SIM.host_ssh_key and not ECAD.host_ssh_key


def test_spec_defaults_to_the_fpga_developer_ami():
    assert AWSWorkerSpec("x", "c5.large", {"x": 1}, "img").ami_id is None


def test_build_recipe_quintuplet():
    assert BuildRecipe(name="r").quintuplet() == (
        "f2-firesim-FireSim-FireSimRocketConfig-BaseF2Config")
