"""EC2 workers for FireSim, ready to hand to ``AWSManager.launch``.

Each is a ``(NodeTypeConfig, AWSNodeConfig)`` pair — a cluster file's node type
and its ``aws_nodes:`` entry. ``KeyName`` and the ssh key are account values
AWSManager fills in; an empty ``ImageId`` means the FPGA Developer AMI.
"""

from __future__ import annotations

from chia.cluster.aws_nodes import AWSNodeConfig
from chia.cluster.config import DockerConfig, NodeTypeConfig

FPGA_RESOURCE = "firesim_fpga"


def _root_volume(gb: int) -> dict:
    return {"BlockDeviceMappings": [{"DeviceName": "/dev/sda1",
                                     "Ebs": {"VolumeSize": gb, "VolumeType": "gp3"}}]}


def _aws_vivado_firesim_docker(xilinx: str) -> DockerConfig:
    """The bitstream build container, with the instance's Vivado 2024.2 from ``xilinx``."""
    return DockerConfig(
        image="ghcr.io/ucb-bar/chia-chisel-build:latest",
        container_name="chia-ecad",
        run_options=[
            # Vivado's multi-process synthesis leaves orphan workers. Without
            # an init as PID 1 to reap them, Vivado waits on them forever.
            "--init",
            "-v", f"{xilinx}:{xilinx}:ro",
            "-e", f"XILINX_VIVADO={xilinx}/Vivado/2024.2",
        ],
        # Puts Vivado on the PATH of the container's login shells, as the
        # AMI's login shell does on the instance.
        run_setup_commands=[
            "echo 'export PATH=$XILINX_VIVADO/bin:$PATH' | sudo tee /etc/profile.d/vivado.sh",
        ],
    )


# Runs a simulation on the FPGA attached to the instance. The manager inside
# the container runs FireSim's commands for its run farm host on the instance
# itself through nsenter (--privileged, --pid=host), and copies its files in the
# instance's home, mounted at the same path; rslave shows the container the disk
# images FireSim mounts there.
F2_SIM = (
    NodeTypeConfig(
        name="firesim",
        resources={FPGA_RESOURCE: 1},
        docker=DockerConfig(
            image="ghcr.io/ucb-bar/chia-firesim:latest",
            container_name="chia-firesim",
            run_options=["--privileged", "--pid=host", "-v", "/home/ubuntu:/home/ubuntu:rslave"],
        ),
    ),
    AWSNodeConfig(KeyName="", InstanceType="f2.6xlarge", count=1, ImageId="",
                  extra_args=_root_volume(300)),
)

# Builds a bitstream with FireSim's own build code, AGFI included, all in the
# container. Vivado is the instance's (the FPGA Developer AMI), mounted in.
F2_VIVADO = (
    NodeTypeConfig(name="ecad", resources={"F2_VIVADO": 1},
                   docker=_aws_vivado_firesim_docker("/opt/Xilinx")),
    AWSNodeConfig(KeyName="", InstanceType="z1d.2xlarge", count=1, ImageId="",
                  extra_args={"IamInstanceProfile": {"Name": "FireSim"},
                              **_root_volume(500)}),   # Vivado intermediates
)

# Builds Corigine MimicTurbo GT (VU19P) bitstreams as F2_VIVADO builds F2 ones, with
# the Vivado of AMD's "Vivado ML 2024.2 Developer AMI". The account must subscribe
# to that AWS Marketplace AMI. The ID is us-east-1's; a cluster file sets another.
AWS_VIVADO = (
    NodeTypeConfig(name="amd_ecad", resources={"AWS_VIVADO": 1, "VIVADO-2024.2": 1},
                   docker=_aws_vivado_firesim_docker("/tools/Xilinx")),
    AWSNodeConfig(KeyName="", InstanceType="z1d.2xlarge", count=1,
                  ImageId="ami-0aca2408692c992fc", extra_args=_root_volume(500)),
)
