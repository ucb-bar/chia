"""EphemeralEC2Host — combines an EC2 instance with Chia's SSHClient for
remote execution, with automatic cleanup via context manager.
"""

from __future__ import annotations

from chia.aws.config import AWSConfig
from chia.aws.ec2 import EC2Instance, terminate_ec2_instances
from chia.cluster.log import get_logger
from chia.cluster.ssh import SSHClient

logger = get_logger("aws.host")


class EphemeralEC2Host:
    """An ephemeral EC2 instance paired with an SSH connection.

    Intended for use as a context manager so the instance is always
    terminated, even on exceptions::

        with EphemeralEC2Host(ec2_inst, aws_cfg) as host:
            host.wait_ready()
            host.run("echo hello")
    """

    def __init__(self, ec2_instance: EC2Instance, aws_config: AWSConfig) -> None:
        ip = ec2_instance.public_ip if aws_config.use_public_ip else ec2_instance.private_ip
        if not ip:
            raise ValueError(
                f"Instance {ec2_instance.instance_id} has no "
                f"{'public' if aws_config.use_public_ip else 'private'} IP"
            )
        self.ssh = SSHClient(ip, aws_config.ssh_user, aws_config.ssh_private_key)
        self.instance_id = ec2_instance.instance_id
        self.ip = ip
        self._region = aws_config.region

    def __enter__(self) -> EphemeralEC2Host:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.terminate()

    def wait_ready(self, timeout: float = 300) -> None:
        """Block until SSH is reachable."""
        logger.info(f"[{self.instance_id}] Waiting for SSH on {self.ip}...")
        self.ssh.wait_for_ssh(timeout=timeout)
        logger.info(f"[{self.instance_id}] SSH ready")

    def rsync_up(self, local_path: str, remote_path: str,
                 exclude: list[str] | None = None,
                 filter_rules: list[str] | None = None) -> None:
        """Upload files from the local machine to this host."""
        self.ssh.rsync_up(local_path, remote_path, exclude=exclude,
                          filter_rules=filter_rules)

    def rsync_down(self, remote_path: str, local_path: str,
                   exclude: list[str] | None = None) -> None:
        """Download files from this host to the local machine."""
        self.ssh.rsync_down(remote_path, local_path, exclude=exclude)

    def run(self, cmd: str, timeout: int = 300, check: bool = True,
            retries: int = 0, retry_delay: float = 5.0):
        """Run a command over SSH."""
        return self.ssh.run(cmd, timeout=timeout, check=check,
                            retries=retries, retry_delay=retry_delay)

    def run_script(self, commands: list[str], timeout: int = 600,
                   check: bool = True):
        """Run multiple commands in a single SSH session."""
        return self.ssh.run_script(commands, timeout=timeout, check=check)

    def terminate(self) -> None:
        """Terminate this EC2 instance."""
        logger.info(f"[{self.instance_id}] Terminating instance")
        terminate_ec2_instances([self.instance_id], region=self._region)
