from __future__ import annotations

import shlex
import subprocess
import time

from chia.cluster.log import get_logger

logger = get_logger("ssh")


class SSHError(Exception):
    pass


class SSHClient:
    def __init__(self, ip: str, user: str, private_key: str | None = None,
                 connect_timeout: int = 30, identities_only: bool = False,
                 proxy_command: str | None = None):
        self.ip = ip
        self.user = user
        self.private_key = private_key
        self.connect_timeout = connect_timeout
        # When the node accepts ONLY this dedicated key (AWS instances), offer
        # only it. Otherwise ssh also offers every ssh-agent key first; a full
        # agent (>= sshd MaxAuthTries, default 6) exhausts the limit and the
        # server disconnects with "Too many authentication failures" before the
        # dedicated key is ever tried. On-prem nodes keep identities_only=False
        # so they can authenticate via their forwarded agent keys.
        self.identities_only = identities_only
        # Optional ssh ProxyCommand (e.g. "nc -X 5 -x 127.0.0.1:1055 %h %p" to
        # reach a host through a tailscale userspace-networking SOCKS5 proxy,
        # or a jump-host command). Applied to ssh and rsync alike.
        self.proxy_command = proxy_command

    def _ssh_base_args(self) -> list[str]:
        args = [
            "ssh",
            "-A",
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", f"ConnectTimeout={self.connect_timeout}",
            "-o", "ServerAliveInterval=60",
            "-o", "ServerAliveCountMax=10",
            "-o", "LogLevel=ERROR",
        ]
        if self.proxy_command:
            args += ["-o", f"ProxyCommand={self.proxy_command}"]
        if self.private_key:
            args += ["-i", self.private_key]
            if self.identities_only:
                args += ["-o", "IdentitiesOnly=yes"]
        args.append(f"{self.user}@{self.ip}")
        return args

    def _rsync_base_args(self) -> list[str]:
        ssh_cmd = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
        if self.proxy_command:
            # rsync splits the -e string on whitespace but keeps quoted
            # segments together, so the ProxyCommand value must be quoted.
            ssh_cmd += f' -o "ProxyCommand={self.proxy_command}"'
        if self.private_key:
            ssh_cmd += f" -i {self.private_key}"
            if self.identities_only:
                ssh_cmd += " -o IdentitiesOnly=yes"
        return ["rsync", "-avz", "-e", ssh_cmd]

    def run(self, cmd: str, timeout: int = 300, check: bool = True,
            retries: int = 0, retry_delay: float = 5.0) -> subprocess.CompletedProcess:
        full_cmd = self._ssh_base_args() + [
            "bash", "--login", "-c", shlex.quote(cmd)
        ]
        logger.debug(f"[{self.ip}] Running: {cmd}")

        for attempt in range(retries + 1):
            try:
                result = subprocess.run(
                    full_cmd, capture_output=True, text=True, timeout=timeout
                )
                break
            except subprocess.TimeoutExpired:
                if attempt < retries:
                    logger.warning(
                        f"[{self.ip}] SSH timed out after {timeout}s "
                        f"(attempt {attempt + 1}/{retries + 1}), retrying in {retry_delay}s..."
                    )
                    time.sleep(retry_delay)
                    continue
                logger.error(
                    f"[{self.ip}] Command timed out after {timeout}s: {cmd}")
                raise SSHError(
                    f"Command on {self.ip} timed out after {timeout}s: {cmd}"
                )

        if result.stdout.strip():
            logger.debug(f"[{self.ip}] stdout: {result.stdout.strip()}")
        if result.stderr.strip():
            log_fn = logger.warning if (check and result.returncode != 0) else logger.debug
            log_fn(f"[{self.ip}] stderr: {result.stderr.strip()}")

        if check and result.returncode != 0:
            logger.error(
                f"[{self.ip}] Command failed (exit {result.returncode}). "
                f"Reproduce with:\n{shlex.join(full_cmd)}")
            if result.stderr.strip():
                logger.error(f"[{self.ip}] stderr: {result.stderr.strip()}")
            raise SSHError(
                f"Command failed on {self.ip} (exit {result.returncode}): {cmd}\n"
                f"stderr: {result.stderr.strip()}"
            )
        return result

    def run_commands(self, commands: list[str], timeout: int = 300) -> None:
        for cmd in commands:
            if not cmd.strip():
                continue
            self.run(cmd, timeout=timeout)

    def run_script(self, commands: list[str], timeout: int = 600,
                   check: bool = True) -> subprocess.CompletedProcess:
        """Run multiple commands in a single SSH session so environment
        changes (source, export, conda activate, etc.) persist across
        all commands.  Commands are piped as a bash script via stdin
        with 'set -e' so any failure aborts immediately.
        """
        cmds = [c for c in commands if c.strip()]
        if not cmds:
            return subprocess.CompletedProcess(args=[], returncode=0)

        script_lines = ["set -e"] + cmds
        script = "\n".join(script_lines)

        full_cmd = self._ssh_base_args() + ["bash", "--login"]
        logger.debug(f"[{self.ip}] Running script ({len(cmds)} commands):")
        for c in cmds:
            logger.debug(f"[{self.ip}]   {c}")

        try:
            result = subprocess.run(
                full_cmd, input=script, capture_output=True, text=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            # The script is piped over stdin, so it is absent from the raw
            # TimeoutExpired (its .cmd is only 'ssh ... bash --login'). Surface
            # the commands that were running so the timeout is diagnosable.
            cmd_list = "\n".join(f"  {c}" for c in cmds)
            logger.error(
                f"[{self.ip}] Script timed out after {timeout}s. "
                f"Commands running when it timed out:\n{cmd_list}")
            raise SSHError(
                f"Script on {self.ip} timed out after {timeout}s. "
                f"A command was still running (a slow 'docker run'/image pull "
                f"is the usual cause). Commands:\n{script}"
            )

        if result.stdout.strip():
            logger.debug(f"[{self.ip}] stdout: {result.stdout.strip()}")
        if result.stderr.strip():
            log_fn = logger.warning if (check and result.returncode != 0) else logger.debug
            log_fn(f"[{self.ip}] stderr: {result.stderr.strip()}")

        if check and result.returncode != 0:
            reproduce = (
                f"{shlex.join(full_cmd)} <<'CHIA_EOF'\n"
                f"{script}\n"
                f"CHIA_EOF"
            )
            logger.error(
                f"[{self.ip}] Script failed (exit {result.returncode}). "
                f"Reproduce with:\n{reproduce}")
            if result.stderr.strip():
                logger.error(f"[{self.ip}] stderr: {result.stderr.strip()}")
            raise SSHError(
                f"Script failed on {self.ip} (exit {result.returncode}).\n"
                f"stderr: {result.stderr.strip()}"
            )
        return result

    def rsync_up(self, local_path: str, remote_path: str,
                 exclude: list[str] | None = None,
                 filter_rules: list[str] | None = None) -> None:
        args = self._rsync_base_args()
        for pattern in (exclude or []):
            args += ["--exclude", pattern]
        for rule in (filter_rules or []):
            args += ["--filter", f":- {rule}"]
        args += [local_path, f"{self.user}@{self.ip}:{remote_path}"]

        logger.debug(f"[{self.ip}] rsync {local_path} -> {remote_path}")
        result = subprocess.run(args, capture_output=True, text=True, timeout=300)
        if result.stdout.strip():
            logger.debug(f"[{self.ip}] rsync output: {result.stdout.strip()}")
        if result.returncode != 0:
            raise SSHError(
                f"rsync failed to {self.ip}: {result.stderr.strip()}"
            )

    def rsync_down(self, remote_path: str, local_path: str,
                  exclude: list[str] | None = None) -> None:
        """Download files from a remote host to the local machine.

        Symmetric to rsync_up() — swaps source and destination.
        """
        args = self._rsync_base_args()
        for pattern in (exclude or []):
            args += ["--exclude", pattern]
        args += [f"{self.user}@{self.ip}:{remote_path}", local_path]

        logger.debug(f"[{self.ip}] rsync {remote_path} -> {local_path}")
        result = subprocess.run(args, capture_output=True, text=True, timeout=300)
        if result.stdout.strip():
            logger.debug(f"[{self.ip}] rsync output: {result.stdout.strip()}")
        if result.returncode != 0:
            raise SSHError(
                f"rsync failed from {self.ip}: {result.stderr.strip()}"
            )

    def wait_for_ssh(self, timeout: float = 60.0, interval: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        logger.debug(f"[{self.ip}] Waiting for SSH (timeout={timeout}s)")
        last_error = "connection attempt timed out"
        while True:
            try:
                result = self.run("echo ok", timeout=10, check=False)
                if result.returncode == 0:
                    logger.debug(f"[{self.ip}] SSH ready")
                    return
                if result.stderr.strip():
                    last_error = result.stderr.strip().splitlines()[-1]
            except subprocess.TimeoutExpired:
                last_error = "connection attempt timed out"
            if time.monotonic() >= deadline:
                raise SSHError(
                    f"SSH to {self.ip} not available after {timeout}s "
                    f"(last error: {last_error})"
                )
            logger.debug(f"[{self.ip}] SSH not ready, retrying in {interval}s...")
            time.sleep(interval)
