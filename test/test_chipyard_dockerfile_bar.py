from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = REPO_ROOT / "dockerfiles" / "ChipyardDockerfileBAR"


def test_bar_dockerfile_patches_chipyard_glibc_rewrite():
    normalized = " ".join(DOCKERFILE.read_text().split())
    assert (
        'SYS_GLIBC=$(ldd --version | awk \'/ldd/{print $NF}\') '
        'if [ "$SYS_GLIBC" = "2.35" ]; then SYS_GLIBC=2.34 fi '
        'DEFAULT_GLIBC=$(grep -i "sysroot_linux-64=" conda-reqs/chipyard-base.yaml '
        '| awk -F= \'{print $2}\' | awk \'{print $1}\')'
    ) in normalized


def test_bar_dockerfile_clones_aws_fpga_firesim_f2():
    """If FireSim lacks the F2 platform submodule, clone it before SDK setup."""
    text = DOCKERFILE.read_text()
    assert "if [ ! -f sims/firesim/platforms/f2/aws-fpga-firesim-f2/sdk_setup.sh ]" in text
    assert "git clone" in text
    assert "aws-fpga-firesim-f2.git" in text
    assert "sims/firesim/platforms/f2/aws-fpga-firesim-f2" in text
    assert "source sdk_setup.sh" in text


def test_bar_dockerfile_does_not_pin_chipyard_ref():
    text = DOCKERFILE.read_text()
    assert "ARG CHIPYARD_REF" not in text
    assert 'if [ -n "${CHIPYARD_REF}" ]' not in text
    assert 'git -C /home/ray/chipyard checkout "${CHIPYARD_REF}"' not in text
