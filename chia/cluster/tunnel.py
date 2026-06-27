from __future__ import annotations

import os
import signal
import subprocess
import time

from chia.cluster.config import SSHAuthConfig, TunnelConfig
from chia.cluster.log import get_logger

logger = get_logger("tunnel")

_PID_DIR = "/tmp"


def _pid_file(key: str) -> str:
    # Replace dots with dashes for safe filenames (e.g. 127.0.0.2 -> 127-0-0-2)
    safe_key = key.replace(".", "-")
    return os.path.join(_PID_DIR, f"chia_tunnel_{safe_key}.pid")


def _kill_orphaned_tunnel(ip: str, tunnel_ip: str | None = None,
                          gcs_port: int | None = None) -> None:
    """Kill any leftover SSH tunnel process for *ip*.

    Checks the PID file first, then falls back to a process search
    in case the PID file was overwritten or deleted.  When *tunnel_ip*
    is provided, also searches for ``ssh -N`` processes that bind to
    that address — this catches orphans from previous runs where the
    remote IP changed (e.g. dynamic EC2 IPs) but the tunnel bind
    address (e.g. ``127.0.0.2``) stayed the same.
    """
    killed_pids: set[int] = set()

    # Try PID files keyed by both remote IP and tunnel_ip
    for key in [ip, tunnel_ip] if tunnel_ip else [ip]:
        pid_path = _pid_file(key)
        if os.path.exists(pid_path):
            try:
                with open(pid_path) as f:
                    pid = int(f.read().strip())
                if pid not in killed_pids:
                    os.kill(pid, signal.SIGTERM)
                    killed_pids.add(pid)
                    logger.info(f"Killed orphaned tunnel (pid {pid}) for {key} via PID file")
            except (ValueError, ProcessLookupError, PermissionError):
                pass
            try:
                os.remove(pid_path)
            except OSError:
                pass

    # Fallback: find ssh -N processes targeting this IP or binding to tunnel_ip.
    # When *gcs_port* is provided, anchor the pattern on the GCS reverse-forward
    # spec ("<ip>:<gcs_port>:") so the sweep matches ONLY chia worker tunnels —
    # a bare "ssh -N.*<tunnel_ip>" also matches unrelated tunnels whose command
    # lines merely mention the address (e.g. the vext Cadence license loops,
    # which bind 127.0.0.{1,2,3}:5280 on the EC2 side and were being killed at
    # every bring-up by this sweep).
    if gcs_port is not None:
        patterns = [f"ssh -N.*{ip}:{gcs_port}:"]
        if tunnel_ip:
            patterns.append(f"ssh -N.*{tunnel_ip}:{gcs_port}:")
    else:
        patterns = [f"ssh -N.*{ip}"]
        if tunnel_ip:
            patterns.append(f"ssh -N.*{tunnel_ip}")

    for pattern in patterns:
        try:
            result = subprocess.run(
                ["pgrep", "-f", pattern],
                capture_output=True, text=True, timeout=5,
            )
            for line in result.stdout.strip().splitlines():
                pid = int(line.strip())
                if pid in killed_pids:
                    continue
                try:
                    os.kill(pid, signal.SIGTERM)
                    killed_pids.add(pid)
                    logger.info(f"Killed orphaned tunnel (pid {pid}) via pattern '{pattern}'")
                except (ProcessLookupError, PermissionError):
                    pass
        except (subprocess.TimeoutExpired, ValueError):
            pass


class TunnelManager:
    """Manages SSH tunnels for cross-network Ray cluster connectivity.

    Each tunneled worker gets a single ``ssh -N`` process carrying all
    reverse (-R) and forward (-L) port mappings.  Tunnels are keyed by
    ``tunnel_ip`` (the unique loopback address, e.g. ``127.0.0.2``),
    allowing multiple tunnels to the same remote IP.
    """

    def __init__(self) -> None:
        # tunnel_ip -> subprocess.Popen
        self._procs: dict[str, subprocess.Popen] = {}

    def start_tunnel(
        self,
        tunnel_ip: str,
        ip: str,
        ssh_auth: SSHAuthConfig,
        tunnel_config: TunnelConfig,
        head_gcs_port: int = 6379,
        head_ip: str | None = None,
        relay_ip: str | None = None,
        reverse_tool_ports: list[int] | None = None,
        reverse_head_worker_ports: list[int] | None = None,
    ) -> None:
        """Launch an SSH tunnel to *ip* keyed by *tunnel_ip*.

        When *head_ip* is provided, tool HTTP port forwards bind on
        *head_ip* instead of *tunnel_ip* so that non-head local nodes
        can reach tunnelled tools.

        When *reverse_tool_ports* is provided (typically only for the
        first tunnel per physical EC2 IP), ``-R`` reverse tunnels are
        added so that EC2 workers can reach tools on the head or on
        other EC2 nodes via the head-as-hub.  The reverse tunnels bind
        on *relay_ip* (a safe loopback like ``127.200.0.1``) on the
        EC2 side, forwarding to *head_ip* on the head side.

        When *reverse_head_worker_ports* is provided, ``-R`` reverse
        tunnels are added for the head node's pinned Ray worker ports,
        enabling EC2 workers to reach Ray actors on the head (e.g.
        ProfileCollectorActor).  Combined with iptables DNAT on the
        EC2 host, this transparently redirects actor RPCs through the
        tunnel.
        """
        if tunnel_ip in self._procs:
            logger.warning(f"Tunnel {tunnel_ip} already running (pid {self._procs[tunnel_ip].pid})")
            return

        # Kill any orphaned tunnel from a previous run.
        # Only match by tunnel_ip (the unique loopback address), not the
        # remote IP — multiple tunnels may share the same remote IP.
        if tunnel_config.kill_orphaned_tunnels:
            _kill_orphaned_tunnel(tunnel_ip,
                                  gcs_port=tunnel_config.gcs_tunnel_port)

        cmd = [
            "ssh", "-N",
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "LogLevel=ERROR",
            "-o", "ServerAliveInterval=30",
            "-o", "ServerAliveCountMax=3",
            "-o", "ExitOnForwardFailure=yes",
        ]
        if ssh_auth.ssh_private_key:
            # Use only the dedicated key — the ambient ssh-agent's keys would
            # otherwise be offered first and exhaust sshd MaxAuthTries before it.
            cmd += ["-i", ssh_auth.ssh_private_key, "-o", "IdentitiesOnly=yes"]

        tun_ip = tunnel_config.tunnel_ip  # e.g. 127.0.0.2

        # Reverse tunnel: remote worker reaches head GCS via <tunnel_ip>:<gcs_tunnel_port>.
        # We bind on tunnel_ip (e.g. 127.0.0.2) rather than 0.0.0.0 to avoid
        # exposing the GCS port to the remote host's network.
        cmd += ["-R", f"{tun_ip}:{tunnel_config.gcs_tunnel_port}:127.0.0.1:{head_gcs_port}"]

        # Forward tunnels: head reaches remote Ray services via <tunnel_ip>:<port>.
        # The worker registers as tunnel_ip in the Ray cluster, so GCS on the
        # head will contact the worker at tunnel_ip:port.  On the remote
        # side we connect to tunnel_ip as well (loopback alias).
        # Node manager
        cmd += ["-L", f"{tun_ip}:{tunnel_config.ray_node_manager_port}:{tun_ip}:{tunnel_config.ray_node_manager_port}"]
        # Object manager
        cmd += ["-L", f"{tun_ip}:{tunnel_config.ray_object_manager_port}:{tun_ip}:{tunnel_config.ray_object_manager_port}"]
        # Ray worker ports
        for port in range(tunnel_config.ray_worker_port_min, tunnel_config.ray_worker_port_max + 1):
            cmd += ["-L", f"{tun_ip}:{port}:{tun_ip}:{port}"]
        # Tool HTTP ports — bind on head_ip so non-head local nodes can
        # reach tunnelled tools at head_ip:port (not just tun_ip:port).
        tool_bind_ip = head_ip if head_ip else tun_ip
        for port in range(tunnel_config.tool_port_min, tunnel_config.tool_port_max + 1):
            cmd += ["-L", f"{tool_bind_ip}:{port}:{tun_ip}:{port}"]

        # Reverse tunnels for tool ports — allows EC2 workers to reach
        # tools (on head or other EC2 nodes) via the head-as-hub.
        # On the EC2 side we bind on relay_ip (a safe 127.x loopback);
        # on the head side we connect to head_ip (real interface or
        # -L forward listener).
        if reverse_tool_ports and relay_ip and head_ip:
            for port in reverse_tool_ports:
                cmd += ["-R", f"{relay_ip}:{port}:{head_ip}:{port}"]

        # Reverse tunnels for head worker ports — allows EC2 workers to
        # reach Ray actors and worker processes on the head node.  Binds
        # on relay_ip on the EC2 side; iptables DNAT redirects head_ip
        # traffic there.  The head-side destination must be head_ip (not
        # 127.0.0.1) because Ray workers bind on the node's real IP.
        if reverse_head_worker_ports and relay_ip:
            head_dst = head_ip or "127.0.0.1"
            for port in reverse_head_worker_ports:
                cmd += ["-R", f"{relay_ip}:{port}:{head_dst}:{port}"]

        cmd.append(f"{ssh_auth.ssh_user}@{ip}")

        logger.info(f"Starting SSH tunnel {tunnel_ip} -> {ip}")
        logger.debug(f"Tunnel command: {' '.join(cmd)}")

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        self._procs[tunnel_ip] = proc

        # Write PID file for cross-process cleanup
        pid_path = _pid_file(tunnel_ip)
        with open(pid_path, "w") as f:
            f.write(str(proc.pid))
        logger.info(f"Tunnel {tunnel_ip} -> {ip} started (pid {proc.pid}, pidfile {pid_path})")

    def stop_tunnel(self, tunnel_ip: str) -> None:
        """Stop the SSH tunnel keyed by *tunnel_ip*."""
        proc = self._procs.pop(tunnel_ip, None)
        if proc is not None:
            logger.info(f"Stopping tunnel {tunnel_ip} (pid {proc.pid})")
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)

        # Clean up PID file
        pid_path = _pid_file(tunnel_ip)
        if os.path.exists(pid_path):
            os.remove(pid_path)

    def stop_all(self) -> None:
        """Stop all managed tunnels."""
        for tunnel_ip in list(self._procs.keys()):
            self.stop_tunnel(tunnel_ip)

    def wait_for_tunnel(self, tunnel_ip: str, timeout: float = 30.0) -> None:
        """Verify the SSH tunnel keyed by *tunnel_ip* is alive.

        Polls the subprocess; raises if it exits prematurely.
        """
        proc = self._procs.get(tunnel_ip)
        if proc is None:
            raise RuntimeError(f"No tunnel process found for {tunnel_ip}")

        deadline = time.monotonic() + timeout
        # Give SSH a moment to establish the connection
        time.sleep(2)
        while time.monotonic() < deadline:
            ret = proc.poll()
            if ret is None:
                # Still running — tunnel is up
                logger.info(f"Tunnel {tunnel_ip} is alive (pid {proc.pid})")
                return
            else:
                stderr = proc.stderr.read().decode() if proc.stderr else ""
                raise RuntimeError(
                    f"Tunnel {tunnel_ip} exited with code {ret}. stderr: {stderr}"
                )
        raise RuntimeError(f"Tunnel {tunnel_ip} health-check timed out after {timeout}s")

    def get_tunneled_ips(self) -> list[str]:
        """Return list of tunnel_ips with active tunnels."""
        return list(self._procs.keys())


def kill_orphaned_tunnels(ips: list[str], tunnel_ip: str | None = None) -> None:
    """Kill orphaned SSH tunnel processes for the given IPs.

    Used during teardown to clean up tunnels that may have been
    started by a previous ``chia up`` invocation.
    """
    for ip in ips:
        _kill_orphaned_tunnel(ip, tunnel_ip=tunnel_ip)
