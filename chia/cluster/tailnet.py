from __future__ import annotations

import ipaddress
import json
from dataclasses import dataclass

from chia.cluster.config import (
    ClusterConfig, ConfigError, NodeAssignment, TailnetConfig,
)
from chia.cluster.log import get_logger
from chia.cluster.ssh import SSHClient

logger = get_logger("tailnet")

# Worker block sub-layout (offsets within a worker's port block):
#   +0 node manager, +1 object manager,
#   +_TOOL_OFFSET.. tool ports, +_WORKER_PORT_OFFSET.. Ray worker ports.
_TOOL_OFFSET = 16
_WORKER_PORT_OFFSET = 64

# Remote file paths ($USER is expanded by the remote shell).
_REMOTE_BASE = "/tmp/chia_tailnet_relay_$USER"

# The relay that runs on every tailnet node (and the head). Pure-stdlib
# Python 3: listens on peer nodes' advertised loopback addresses and
# forwards each accepted connection through the local tailscaled SOCKS5
# proxy to the peer's tailnet IP (same port). Inbound tailnet traffic
# needs no relay: userspace tailscaled delivers it to 127.0.0.1:<port>,
# where Ray's wildcard-bound services receive it directly.
RELAY_SCRIPT = r'''
import json, selectors, socket, struct, sys, threading


def socks5_connect(proxy, dest_ip, dest_port, timeout=15):
    s = socket.create_connection(proxy, timeout=timeout)
    try:
        s.sendall(b"\x05\x01\x00")
        if s.recv(2) != b"\x05\x00":
            raise OSError("SOCKS5 greeting failed")
        try:  # IPv4 literal, else a hostname (e.g. MagicDNS) the proxy resolves
            addr = b"\x01" + socket.inet_aton(dest_ip)
        except OSError:
            host = dest_ip.encode()
            addr = b"\x03" + bytes([len(host)]) + host
        s.sendall(b"\x05\x01\x00" + addr + struct.pack(">H", dest_port))
        reply = b""
        while len(reply) < 10:
            chunk = s.recv(10 - len(reply))
            if not chunk:
                raise OSError("SOCKS5 connect: short reply")
            reply += chunk
        if reply[1] != 0:
            raise OSError("SOCKS5 connect failed (code %d)" % reply[1])
        s.settimeout(None)
        return s
    except Exception:
        s.close()
        raise


def _pump(src, dst):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    try:
        dst.shutdown(socket.SHUT_WR)  # propagate half-close
    except OSError:
        pass


def _handle(conn, proxy, dest_ip, dest_port):
    try:
        up = socks5_connect(proxy, dest_ip, dest_port)
    except Exception as e:
        sys.stderr.write("relay: dial %s:%d failed: %s\n" % (dest_ip, dest_port, e))
        conn.close()
        return
    for s in (conn, up):
        try:
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        except OSError:
            pass
    t = threading.Thread(target=_pump, args=(conn, up), daemon=True)
    t.start()
    _pump(up, conn)
    t.join()
    for s in (conn, up):
        try:
            s.close()
        except OSError:
            pass


def main():
    with open(sys.argv[1]) as f:
        spec = json.load(f)
    proxy_host, proxy_port = spec["socks_proxy"].rsplit(":", 1)
    proxy = (proxy_host, int(proxy_port))

    threading.stack_size(256 * 1024)
    sel = selectors.DefaultSelector()
    for entry in spec["listeners"]:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            srv.bind((entry["bind_ip"], entry["port"]))
        except OSError as e:
            sys.stderr.write("relay: cannot bind %s:%d: %s\n"
                             % (entry["bind_ip"], entry["port"], e))
            sys.exit(1)
        srv.listen(128)
        srv.setblocking(False)
        sel.register(srv, selectors.EVENT_READ, entry)

    print("CHIA_RELAY_READY %d" % len(spec["listeners"]), flush=True)
    while True:
        for key, _ in sel.select():
            try:
                conn, _addr = key.fileobj.accept()
            except OSError:
                continue
            conn.setblocking(True)
            entry = key.data
            threading.Thread(
                target=_handle,
                args=(conn, proxy, entry["dest_ip"], entry["port"]),
                daemon=True,
            ).start()


if __name__ == "__main__":
    main()
'''


@dataclass
class TailnetWorkerAlloc:
    """Per-worker addressing for a tailnet cluster.

    Each tailnet worker registers in Ray under a unique loopback
    ``advertise_ip`` and owns a globally unique block of pinned ports —
    Ray services bind the wildcard address, so peers' relay listeners
    may only use port numbers no local service binds.
    """
    advertise_ip: str
    tailnet_ip: str  # the host's tailscale 100.x address
    node_manager_port: int
    object_manager_port: int
    tool_port_min: int
    tool_port_max: int
    worker_port_min: int
    worker_port_max: int

    def ports(self) -> list[int]:
        return ([self.node_manager_port, self.object_manager_port]
                + list(range(self.tool_port_min, self.tool_port_max + 1))
                + list(range(self.worker_port_min, self.worker_port_max + 1)))


def head_ports(tn: TailnetConfig) -> list[int]:
    """Every head port a tailnet worker may need to dial."""
    return ([tn.gcs_port, tn.head_node_manager_port, tn.head_object_manager_port]
            + list(range(tn.head_worker_port_min, tn.head_worker_port_max + 1))
            + list(range(tn.head_tool_port_min, tn.head_tool_port_max + 1)))


def allocate_tailnet_workers(
    config: ClusterConfig,
    assignments: list[NodeAssignment],
) -> dict[tuple[str, str, int], TailnetWorkerAlloc]:
    """Compute per-worker advertise IPs and port blocks.

    Returns a dict keyed by ``(ip, node_type_name, worker_index)`` for
    every assignment on a tailnet IP.  Advertise IPs count up from
    127.0.0.2; port blocks are consecutive ``worker_block_size`` slices
    from ``worker_block_base``.
    """
    tn = config.tailnet_config
    assert tn is not None

    needed = _WORKER_PORT_OFFSET + tn.worker_port_count
    if needed > tn.worker_block_size:
        raise ConfigError(
            f"tailnet: worker_block_size ({tn.worker_block_size}) too small for "
            f"{tn.worker_port_count} worker ports (needs >= {needed})")
    if tn.tool_port_count > _WORKER_PORT_OFFSET - _TOOL_OFFSET:
        raise ConfigError(
            f"tailnet: tool_port_count ({tn.tool_port_count}) too large "
            f"(max {_WORKER_PORT_OFFSET - _TOOL_OFFSET})")

    result: dict[tuple[str, str, int], TailnetWorkerAlloc] = {}
    next_addr = ipaddress.IPv4Address("127.0.0.2")
    head_port_set = set(head_ports(tn))

    idx = 0
    for a in assignments:
        if not config.is_tailnet(a.ip):
            continue
        base = tn.worker_block_base + idx * tn.worker_block_size
        alloc = TailnetWorkerAlloc(
            advertise_ip=str(next_addr),
            tailnet_ip=a.ip,
            node_manager_port=base,
            object_manager_port=base + 1,
            tool_port_min=base + _TOOL_OFFSET,
            tool_port_max=base + _TOOL_OFFSET + tn.tool_port_count - 1,
            worker_port_min=base + _WORKER_PORT_OFFSET,
            worker_port_max=base + _WORKER_PORT_OFFSET + tn.worker_port_count - 1,
        )
        if alloc.advertise_ip == tn.head_advertise_ip:
            raise ConfigError(
                f"tailnet: worker advertise IP collides with head_advertise_ip "
                f"({tn.head_advertise_ip})")
        block = set(alloc.ports())
        if block & head_port_set:
            raise ConfigError(
                f"tailnet: worker port block [{base}, {base + tn.worker_block_size}) "
                f"overlaps the head port ranges — adjust worker_block_base/"
                f"head_worker_port_min or reduce worker count")
        result[(a.ip, a.node_type.name, a.worker_index)] = alloc

        idx += 1
        next_addr += 1
        if next_addr == ipaddress.IPv4Address("127.0.0.1"):
            next_addr += 1

    return result


def build_relay_spec(
    config: ClusterConfig,
    allocs: dict[tuple[str, str, int], TailnetWorkerAlloc],
    host_ip: str | None,
) -> dict:
    """Build the relay listener spec for one host.

    *host_ip* is the host's tailnet IP, or ``None`` for the head node.
    The relay listens on the advertised loopback addresses of every
    PEER node (never its own — same-host peers are reached directly via
    the local wildcard binds, and binding them would collide).
    """
    tn = config.tailnet_config
    assert tn is not None

    listeners: list[dict] = []
    if host_ip is not None:
        # Workers dial the head at its advertised loopback IP.
        for port in head_ports(tn):
            listeners.append({"bind_ip": tn.head_advertise_ip, "port": port,
                              "dest_ip": tn.head_tailnet_ip})
    for alloc in allocs.values():
        if alloc.tailnet_ip == host_ip:
            continue  # same host: local wildcard binds serve these directly
        for port in alloc.ports():
            listeners.append({"bind_ip": alloc.advertise_ip, "port": port,
                              "dest_ip": alloc.tailnet_ip})

    return {"socks_proxy": tn.socks_proxy, "listeners": listeners}


def start_relay(ssh: SSHClient, spec: dict) -> None:
    """Deploy and (re)start the tailnet relay on *ssh*'s host."""
    if not spec["listeners"]:
        logger.debug(f"[{ssh.ip}] No relay listeners needed, skipping")
        return
    spec_json = json.dumps(spec, indent=1)
    script = [
        f"cat > {_REMOTE_BASE}.py <<'CHIA_RELAY_SCRIPT_EOF'\n"
        f"{RELAY_SCRIPT}\n"
        f"CHIA_RELAY_SCRIPT_EOF",
        f"cat > {_REMOTE_BASE}.json <<'CHIA_RELAY_SPEC_EOF'\n"
        f"{spec_json}\n"
        f"CHIA_RELAY_SPEC_EOF",
        # [y] avoids the pattern matching any process whose argv quotes it.
        f'pkill -f "chia_tailnet_rela[y]_$USER.py" 2>/dev/null || true',
        "sleep 0.5",
        f"rm -f {_REMOTE_BASE}.log",
        f"nohup python3 {_REMOTE_BASE}.py {_REMOTE_BASE}.json "
        f"> {_REMOTE_BASE}.log 2>&1 &",
        f"echo $! > {_REMOTE_BASE}.pid",
        'ok=""',
        f'for i in $(seq 1 40); do '
        f'if grep -q CHIA_RELAY_READY {_REMOTE_BASE}.log 2>/dev/null; '
        f'then ok=1; break; fi; sleep 0.5; done',
        f'if [ -z "$ok" ]; then echo "chia tailnet relay failed to start:"; '
        f'cat {_REMOTE_BASE}.log; exit 1; fi',
        f"grep CHIA_RELAY_READY {_REMOTE_BASE}.log",
    ]
    ssh.run_script(script, timeout=120)
    logger.info(f"[{ssh.ip}] Tailnet relay up "
                f"({len(spec['listeners'])} listeners)")


def stop_relay(ssh: SSHClient) -> None:
    """Stop the tailnet relay on *ssh*'s host (best effort)."""
    ssh.run_script([
        f'pkill -f "chia_tailnet_rela[y]_$USER.py" 2>/dev/null || true',
        f"rm -f {_REMOTE_BASE}.pid",
    ], check=False)
    logger.info(f"[{ssh.ip}] Tailnet relay stopped")
