from __future__ import annotations

import shlex

from chia.cluster.config import DockerConfig
from chia.cluster.log import get_logger
from chia.cluster.ssh import SSHClient

logger = get_logger("docker")


class DockerManager:
    def __init__(self, ssh: SSHClient, docker_config: DockerConfig):
        self.ssh = ssh
        self.config = docker_config
        self.engine = docker_config.engine

    def setup_container(self) -> None:
        ip = self.ssh.ip
        name = self.config.container_name
        engine = self.engine

        # Commands run via 'bash --login -c' (see SSHClient), so the engine
        # must resolve in a login shell — catch a missing binary up front.
        result = self.ssh.run(f"command -v {shlex.quote(engine)}", check=False)
        if result.returncode != 0:
            raise RuntimeError(
                f"[{ip}] '{engine}' not found in login-shell PATH (chia runs "
                f"commands via 'bash --login -c'). If {engine} is installed in "
                f"a conda env, activate it in a login-sourced dotfile (e.g. "
                f"~/.profile) or symlink the binary into a directory on PATH."
            )

        if self.config.pull_before_run:
            logger.info(f"[{ip}] Pulling {engine} image {self.config.image}")
            self.ssh.run(f"{engine} pull {shlex.quote(self.config.image)}",
                         timeout=self.config.pull_timeout)

        # Check if container exists (returncode != 0 means it doesn't exist at all)
        result = self.ssh.run(
            f"{engine} inspect -f '{{{{.State.Running}}}}' {shlex.quote(name)} 2>/dev/null",
            check=False,
        )
        if result.returncode != 0:
            # Container does not exist — nothing to clean up
            logger.debug(f"[{ip}] Container '{name}' does not exist yet")
        elif "true" in result.stdout.lower():
            logger.info(f"[{ip}] Container '{name}' already running")
            return
        else:
            # Container exists but is stopped — remove it before re-creating
            logger.debug(f"[{ip}] Removing stopped container '{name}'")
            self.ssh.run(f"{engine} rm -f {shlex.quote(name)}", check=False)

        run_opts = " ".join(self.config.run_options)

        docker_run = (
            f"{engine} run -d --name {shlex.quote(name)} "
            f"--net=host --shm-size=8g --pull=never {run_opts} "
            f"{shlex.quote(self.config.image)} sleep infinity"
        )

        # Build a single script so everything runs in one SSH session
        # (preserves the forwarded SSH agent socket).
        script = [docker_run]

        # Fix socket permissions so non-root container users can connect
        script.append(
            f"{engine} exec --user root {shlex.quote(name)} "
            f"bash -c 'if [ -e /ssh-agent ]; then chmod 777 /ssh-agent; fi'"
        )

        setup_cmds = [c for c in self.config.run_setup_commands if c.strip()]
        if setup_cmds:
            setup_script = "\n".join(["set -e"] + setup_cmds)
            script.append(
                f"{engine} exec -i {shlex.quote(name)} bash --login "
                f"<<'CHIA_DOCKER_SCRIPT'\n{setup_script}\nCHIA_DOCKER_SCRIPT"
            )

        logger.info(f"[{ip}] Starting container '{name}' from {self.config.image}")
        if setup_cmds:
            logger.info(f"[{ip}] Running {len(setup_cmds)} setup commands in '{name}'")
        result = self.ssh.run_script(script)
        if result and result.stdout.strip():
            logger.info(f"[{ip}] [{name}] stdout: {result.stdout.strip()}")
        if result and result.stderr.strip():
            logger.info(f"[{ip}] [{name}] stderr: {result.stderr.strip()}")

    def exec_command(self, cmd: str, timeout: int | None = None):
        logger.debug(f"[{self.ssh.ip}] {self.engine} exec [{self.config.container_name}]: {cmd}")
        return self.ssh.run(
            f"{self.engine} exec {shlex.quote(self.config.container_name)} "
            f"bash -lc {shlex.quote(cmd)}",
            timeout=timeout,
        )

    def exec_commands(self, commands: list[str], timeout: int | None = None) -> None:
        for cmd in commands:
            if not cmd.strip():
                continue
            self.exec_command(cmd, timeout=timeout)

    def exec_script(self, commands: list[str], timeout: int | None = None):
        """Run multiple commands inside the container in a single shell
        session, so environment changes persist across commands.
        Pipes a script via stdin to '<engine> exec -i ... bash --login'.
        """
        cmds = [c for c in commands if c.strip()]
        if not cmds:
            return
        logger.debug(f"[{self.ssh.ip}] {self.engine} exec script ({len(cmds)} commands) "
                      f"in [{self.config.container_name}]:")
        for c in cmds:
            logger.debug(f"[{self.ssh.ip}]   {c}")

        script_lines = ["set -e"] + cmds
        script = "\n".join(script_lines)

        # Use ssh to run '<engine> exec -i <container> bash --login' and pipe the script
        return self.ssh.run_script(
            [f"{self.engine} exec -i {shlex.quote(self.config.container_name)} bash --login <<'CHIA_DOCKER_SCRIPT'\n{script}\nCHIA_DOCKER_SCRIPT"],
            timeout=timeout,
        )

    def stop_container(self) -> None:
        logger.info(f"[{self.ssh.ip}] Stopping container '{self.config.container_name}'")
        self.ssh.run(
            f"{self.engine} stop {shlex.quote(self.config.container_name)}",
            check=False,
        )
