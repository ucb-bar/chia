"""EC2 workers for FireSim, ready to hand to ``AWSManager.launch``.

Each is a ``(NodeTypeConfig, AWSNodeConfig)`` pair — a cluster file's node type
and its ``aws_nodes:`` entry. ``KeyName`` and the ssh key are account values
AWSManager fills in; an empty ``ImageId`` means the FPGA Developer AMI.
"""

from __future__ import annotations

from chia.cluster.aws_nodes import AWSNodeConfig
from chia.cluster.config import DockerConfig, NodeTypeConfig

ECAD_RESOURCE = "F2_vivado"

# Vivado's working directory on the ECAD instance, mounted into the container
# at the same path: the container copies the design in, Vivado on the host
# builds it there.
BUILD_DIR = "/home/ubuntu/firesim-build"


def _root_volume(gb: int) -> dict:
    return {"BlockDeviceMappings": [{"DeviceName": "/dev/sda1",
                                     "Ebs": {"VolumeSize": gb, "VolumeType": "gp3"}}]}


# Builds a bitstream with FireSim's own build code, AGFI included: Chisel in
# the container, Vivado on the instance (the FPGA Developer AMI).
F2_ECAD = (
    NodeTypeConfig(
        name="ecad",
        resources={ECAD_RESOURCE: 1},
        docker=DockerConfig(
            image="ghcr.io/ucb-bar/chia-chisel-build:latest",
            container_name="chia-ecad",
            run_options=[
                # aws_create_afi runs from the container; the role supplies
                # credentials, but not a region.
                "-e", "AWS_DEFAULT_REGION=us-east-1",
                # Lets the container run the Vivado step on the host with nsenter.
                "--privileged", "--pid=host",
                "-v", f"{BUILD_DIR}:{BUILD_DIR}",
            ],
        ),
    ),
    AWSNodeConfig(KeyName="", InstanceType="z1d.2xlarge", count=1, ImageId="",
                  extra_args={"IamInstanceProfile": {"Name": "FireSim"},
                              **_root_volume(500)}),   # Vivado intermediates
)
