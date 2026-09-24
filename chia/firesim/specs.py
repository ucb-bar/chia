"""The two EC2 worker kinds FireSim needs, ready to hand to ``AWSManager``."""

from __future__ import annotations

from chia.aws.manager import AWSWorkerSpec

FPGA_RESOURCE = "firesim_fpga"
ECAD_RESOURCE = "ecad"

# Where the container reaches the host's sshd once the host has given up
# 127.0.0.1 (see _MOVE_HOST_SSHD).
HOST_LOOPBACK = "127.0.0.2"

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

# `firesim buildbitstream` ssh's to localhost for Chisel, and needs chipyard
# there; Vivado runs on the build-farm host and needs the AMI. Under --net=host
# both share one network namespace, so they split loopback: the host's sshd
# keeps its private IP and 127.0.0.2, freeing 127.0.0.1 for the container's.
# Socket activation (Ubuntu 22.10+) ignores ListenAddress, hence the switch to
# the plain service.
_MOVE_HOST_SSHD = f"""
IP=$(curl -s -H "X-aws-ec2-metadata-token: $(curl -s -X PUT \\
  -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' \\
  http://169.254.169.254/latest/api/token)" \\
  http://169.254.169.254/latest/meta-data/local-ipv4)
printf 'ListenAddress %s\\nListenAddress {HOST_LOOPBACK}\\n' "$IP" \\
  > /etc/ssh/sshd_config.d/00-chia.conf
systemctl disable --now ssh.socket 2>/dev/null || true
systemctl enable ssh.service
systemctl restart ssh.service
"""

# Builds a bitstream with FireSim's own `buildbitstream`, AGFI included.
ECAD = AWSWorkerSpec(
    name="ecad",
    instance_type="z1d.2xlarge",
    resources={ECAD_RESOURCE: 1},
    image="ghcr.io/ucb-bar/chia-firesim-build:latest",
    volume_size_gb=500,      # Vivado writes tens of GB of intermediates
    push_aws_creds=True,     # aws_create_afi runs from the container
    host_ssh_key="/home/ray/firesim.pem",
    user_data=_MOVE_HOST_SSHD,
)
