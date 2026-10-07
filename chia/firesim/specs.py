"""EC2 workers for FireSim, ready to hand to ``AWSManager.launch``.

Each is a ``(NodeTypeConfig, AWSNodeConfig)`` pair — a cluster file's node type
and its ``aws_nodes:`` entry. ``KeyName`` and the ssh key are account values
AWSManager fills in; an empty ``ImageId`` means the FPGA Developer AMI.
"""

from __future__ import annotations

from chia.cluster.aws_nodes import AWSNodeConfig
from chia.cluster.config import DockerConfig, NodeTypeConfig

FPGA_RESOURCE = "firesim_fpga"
ECAD_RESOURCE = "F2_vivado"


def _root_volume(gb: int) -> dict:
    return {"BlockDeviceMappings": [{"DeviceName": "/dev/sda1",
                                     "Ebs": {"VolumeSize": gb, "VolumeType": "gp3"}}]}


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
F2_ECAD = (
    NodeTypeConfig(
        name="ecad",
        resources={ECAD_RESOURCE: 1},
        docker=DockerConfig(
            image="ghcr.io/ucb-bar/chia-chisel-build:latest",
            container_name="chia-ecad",
            run_options=[
                # Vivado's multi-process synthesis leaves orphan workers. Without
                # an init as PID 1 to reap them, Vivado waits on them forever.
                "--init",
                "-v", "/opt/Xilinx:/opt/Xilinx:ro",
                "-e", "XILINX_VIVADO=/opt/Xilinx/Vivado/2024.2",
            ],
        ),
    ),
    AWSNodeConfig(KeyName="", InstanceType="z1d.2xlarge", count=1, ImageId="",
                  extra_args={"IamInstanceProfile": {"Name": "FireSim"},
                              **_root_volume(500)}),   # Vivado intermediates
)
