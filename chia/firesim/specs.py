"""The two EC2 worker kinds FireSim needs, ready to hand to ``AWSManager``."""

from __future__ import annotations

from chia.aws.manager import AWSWorkerSpec

FPGA_RESOURCE = "firesim_fpga"
ECAD_RESOURCE = "F2_vivado"

# Runs a simulation on the FPGA attached to the instance. The manager inside
# the container reaches that FPGA by ssh'ing to "localhost", which --net=host
# makes the instance itself; --privileged and /dev are what let it through.
F2_SIM = AWSWorkerSpec(
    name="firesim",
    instance_type="f2.6xlarge",
    resources={FPGA_RESOURCE: 1},
    image="ghcr.io/ucb-bar/chia-firesim:latest",
    run_options=["--privileged", "-v", "/dev:/dev"],
    host_ssh_key="/home/ray/firesim.pem",   # the path deploy/firesim hardcodes
)

# Builds a bitstream with FireSim's own `buildbitstream`, AGFI included.
ECAD = AWSWorkerSpec(
    name="ecad",
    instance_type="z1d.2xlarge",
    resources={ECAD_RESOURCE: 1},
    image="ghcr.io/ucb-bar/chia-chisel-build:latest",
    volume_size_gb=500,      # Vivado writes tens of GB of intermediates
    # aws_create_afi runs from the container; the role supplies credentials,
    # but not a region.
    run_options=["-e", "AWS_DEFAULT_REGION=us-east-1"],
    iam_instance_profile="FireSim",
    host_ssh_key="/home/ray/firesim.pem",
)
