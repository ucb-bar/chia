"""The two EC2 worker kinds FireSim needs, ready to hand to ``AWSManager``."""

from __future__ import annotations

from chia.aws.manager import AWSWorkerSpec

FPGA_RESOURCE = "firesim_fpga"
ECAD_RESOURCE = "ecad"

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

# Builds a bitstream. Chipyard is in the container (Chisel elaboration, driver
# build); Vivado is on the FPGA Developer AMI outside it, which the node reaches
# over ssh to localhost. Vivado writes tens of GB of intermediates, hence the
# volume. Bump instance_type for anything BOOM-sized.
ECAD = AWSWorkerSpec(
    name="ecad",
    instance_type="z1d.2xlarge",
    resources={ECAD_RESOURCE: 1},
    image="ghcr.io/ucb-bar/chia-chisel-build:latest",
    volume_size_gb=500,
    run_options=["--privileged", "-v", "/dev:/dev"],
    host_ssh_key="/home/ray/.ssh/id_rsa",
)
