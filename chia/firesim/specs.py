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
# at the same path so its tens of GB of writes bypass the container's overlay.
BUILD_DIR = "/home/ubuntu/firesim-build"


def _root_volume(gb: int) -> dict:
    return {"BlockDeviceMappings": [{"DeviceName": "/dev/sda1",
                                     "Ebs": {"VolumeSize": gb, "VolumeType": "gp3"}}]}


# Builds a bitstream with FireSim's own build code, AGFI included, entirely
# inside the container.
ECAD = (
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
                # The AMI's Vivado, so the whole build runs in the container.
                # The AMI has used both install roots; a missing one mounts as
                # an empty dir.
                "-v", "/tools/Xilinx:/tools/Xilinx:ro",
                "-v", "/opt/Xilinx:/opt/Xilinx:ro",
                "-v", f"{BUILD_DIR}:{BUILD_DIR}",
            ],
        ),
    ),
    AWSNodeConfig(KeyName="", InstanceType="z1d.2xlarge", count=1, ImageId="",
                  extra_args={"IamInstanceProfile": {"Name": "FireSim"},
                              **_root_volume(500)}),   # Vivado intermediates
)
