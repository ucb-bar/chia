from __future__ import annotations

import ipaddress
import json
import re
import uuid
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

# The relay that runs on every tailnet machine (including the head).
# Pure-stdlib Python 3: listens on peer workers' advertised loopback addresses and
# forwards each accepted connection through the local tailscaled SOCKS5
# proxy to the peer's tailnet IP (same port). Inbound tailnet traffic
# needs no relay: userspace tailscaled delivers it to 127.0.0.1:<port>,
# where Ray's wildcard-bound services receive it directly.
#
# SIGHUP hot-reloads the spec file in place (``chia up --add``): new
# listeners are bound, removed ones closed, and ``routes`` is updated —
# without touching established connections. The READY line's trailing
# "reload" advertises this; relays started before it existed lack it.
RELAY_SCRIPT = r'''
import json, selectors, signal, socket, struct, sys, threading


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


def _splice(conn, up):
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


def _handle_connect(conn, proxy, routes):
    # Single-listener HTTP CONNECT proxy. Ray's gRPC (with grpc_proxy set)
    # sends "CONNECT <advertise_ip>:<port>"; we map the advertise IP to its
    # owning machine's tailnet IP via `routes` and dial through SOCKS — or
    # straight to 127.0.0.1 when the destination is local to this machine
    # (route value null). No per-port listeners: the destination rides in
    # the request, so one socket serves every peer and port.
    try:
        buf = b""
        while b"\r\n\r\n" not in buf:
            d = conn.recv(4096)
            if not d:
                conn.close(); return
            buf += d
            if len(buf) > 65536:
                conn.close(); return
        head, _, rest = buf.partition(b"\r\n\r\n")
        line = head.split(b"\r\n", 1)[0].decode("latin1")
        parts = line.split()
        if len(parts) < 2 or parts[0].upper() != "CONNECT":
            conn.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n"); conn.close(); return
        host, _, port_s = parts[1].rpartition(":")
        port = int(port_s)
        if host not in routes:
            sys.stderr.write("relay: CONNECT no route for %s\n" % host)
            conn.sendall(b"HTTP/1.1 502 No Route\r\n\r\n"); conn.close(); return
        tailnet_ip = routes[host]   # null => local
        try:
            if tailnet_ip is None:
                up = socket.create_connection(("127.0.0.1", port), timeout=15)
                # The 15s deadline is for connect() only — left armed it
                # makes _pump's recv() raise after any 15s idle gap,
                # killing long-lived streams (e.g. in-flight gRPC calls
                # to head-colocated workers). socks5_connect clears its
                # timeout the same way.
                up.settimeout(None)
            else:
                up = socks5_connect(proxy, tailnet_ip, port)
        except Exception as e:
            sys.stderr.write("relay: CONNECT %s:%d dial failed: %s\n"
                             % (host, port, e))
            conn.sendall(b"HTTP/1.1 502 Dial Failed\r\n\r\n"); conn.close(); return
        conn.sendall(b"HTTP/1.1 200 Connection established\r\n\r\n")
        if rest:                       # bytes the client pipelined after CONNECT
            up.sendall(rest)
        _splice(conn, up)
    except Exception:
        try:
            conn.close()
        except OSError:
            pass


def _handle(conn, proxy, dest_ip, dest_port, via="socks"):
    try:
        if via == "direct":
            up = socket.create_connection((dest_ip, dest_port), timeout=15)
            up.settimeout(None)  # connect deadline only — streams may idle
        else:
            up = socks5_connect(proxy, dest_ip, dest_port)
    except Exception as e:
        sys.stderr.write("relay: dial %s:%d (%s) failed: %s\n"
                         % (dest_ip, dest_port, via, e))
        conn.close()
        return
    _splice(conn, up)


def _bind(sel, servers, entry):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        srv.bind((entry["bind_ip"], entry["port"]))
    except OSError:
        srv.close()
        raise
    srv.listen(128)
    srv.setblocking(False)
    sel.register(srv, selectors.EVENT_READ, entry)
    servers[(entry["bind_ip"], entry["port"])] = srv


def _reload(spec_path, sel, servers, routes):
    # Diff the new spec against the live listeners: keep (re-tagging
    # their entry), close, or bind. Established connections are never
    # touched — closing a listener only stops new accepts.
    try:
        with open(spec_path) as f:
            spec = json.load(f)
    except (OSError, ValueError) as e:
        sys.stderr.write("relay: reload failed to read spec: %s\n" % e)
        print("CHIA_RELAY_RELOAD_FAILED", flush=True)
        return
    wanted = {(e["bind_ip"], e["port"]): e for e in spec["listeners"]}
    for addr in [a for a in servers if a not in wanted]:
        srv = servers.pop(addr)
        sel.unregister(srv)
        srv.close()
    errors = 0
    for addr, entry in wanted.items():
        if addr in servers:
            sel.modify(servers[addr], selectors.EVENT_READ, entry)
            continue
        try:
            _bind(sel, servers, entry)
        except OSError as e:
            sys.stderr.write("relay: cannot bind %s:%d: %s\n"
                             % (addr[0], addr[1], e))
            errors += 1
    # Updated in place: CONNECT handler threads share this dict.
    new_routes = spec.get("routes", {})
    routes.update(new_routes)
    for host in [h for h in routes if h not in new_routes]:
        routes.pop(host, None)
    print("CHIA_RELAY_RELOADED %s %d errors=%d"
          % (spec.get("reload_token", "-"), len(servers), errors), flush=True)


def main():
    spec_path = sys.argv[1]
    with open(spec_path) as f:
        spec = json.load(f)
    proxy_host, proxy_port = spec["socks_proxy"].rsplit(":", 1)
    proxy = (proxy_host, int(proxy_port))
    routes = dict(spec.get("routes", {}))

    threading.stack_size(256 * 1024)
    sel = selectors.DefaultSelector()
    servers = {}
    for entry in spec["listeners"]:
        try:
            _bind(sel, servers, entry)
        except OSError as e:
            sys.stderr.write("relay: cannot bind %s:%d: %s\n"
                             % (entry["bind_ip"], entry["port"], e))
            sys.exit(1)

    # SIGHUP -> reload, via a self-pipe so the reload runs in the select
    # loop rather than inside the signal handler.
    wake_r, wake_w = socket.socketpair()
    wake_r.setblocking(False)
    wake_w.setblocking(False)
    sel.register(wake_r, selectors.EVENT_READ, None)

    def _on_hup(_signum, _frame):
        try:
            wake_w.send(b"\0")
        except OSError:
            pass  # a wakeup is already pending
    signal.signal(signal.SIGHUP, _on_hup)

    print("CHIA_RELAY_READY %d reload" % len(spec["listeners"]), flush=True)
    while True:
        for key, _ in sel.select():
            if key.data is None:
                try:
                    wake_r.recv(4096)
                except OSError:
                    pass
                _reload(spec_path, sel, servers, routes)
                continue
            try:
                conn, _addr = key.fileobj.accept()
            except OSError:
                continue  # also: a listener closed by a reload in this batch
            conn.setblocking(True)
            entry = key.data
            if entry.get("via") == "connect":
                t = threading.Thread(target=_handle_connect,
                                     args=(conn, proxy, routes),
                                     daemon=True)
            else:
                t = threading.Thread(
                    target=_handle,
                    args=(conn, proxy, entry["dest_ip"], entry["port"],
                          entry.get("via", "socks")),
                    daemon=True)
            t.start()


if __name__ == "__main__":
    main()
'''


@dataclass
class TailnetWorkerAlloc:
    """Per-worker addressing for a tailnet cluster.

    Each tailnet worker registers in Ray under a globally unique
    loopback ``advertise_ip`` (the routing key) and owns a block of
    pinned ports that need only be unique per machine — Ray's gRPC is
    reached through the per-machine CONNECT proxy, so no per-port
    listeners exist to force cluster-wide uniqueness.
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


def _ts_ports(tn: TailnetConfig) -> tuple[int, int]:
    """SOCKS5 and HTTP proxy ports for a CHIA-managed tailscaled."""
    socks_port = int(tn.socks_proxy.rsplit(":", 1)[1])
    return socks_port, socks_port + 1


def ts_hostname(cluster_name: str, ip: str) -> str:
    """A DNS-safe tailnet machine name: chia-<cluster>-<host>."""
    name = f"chia-{cluster_name}-{ip}".lower()
    name = re.sub(r"[^a-z0-9-]+", "-", name).strip("-")
    return name[:63]


def tailscale_install_command(tn: TailnetConfig) -> str:
    """Idempotent shell command installing userspace tailscale binaries.

    Downloads the static tarball (no root needed) into
    ``tn.tailscale_dir`` unless ``tailscaled`` is already there.
    Suitable for cloud machine setup_commands.
    """
    if not tn.tailscale_dir:
        raise ConfigError(
            "tailnet.tailscale_dir is empty — configs built via "
            "build_config() get a per-cluster default; set it explicitly "
            "when constructing TailnetConfig directly")
    d = tn.tailscale_dir
    v = tn.tailscale_version
    return (
        f'TS_DIR="{d}"; '
        f'if [ ! -x "$TS_DIR/tailscaled" ]; then '
        f'case "$(uname -m)" in aarch64|arm64) TS_ARCH=arm64;; *) TS_ARCH=amd64;; esac; '
        f'mkdir -p "$TS_DIR" && '
        f'curl -fsSL "https://pkgs.tailscale.com/stable/tailscale_{v}_${{TS_ARCH}}.tgz" '
        f'| tar -xz -C "$TS_DIR" --strip-components=1; '
        f'fi; mkdir -p "$TS_DIR/data" "$TS_DIR/run"'
    )


def ensure_tailscale(ssh: SSHClient, tn: TailnetConfig,
                     hostname: str | None = None) -> str:
    """Install/start userspace tailscaled on *ssh*'s host and join the
    tailnet; return the host's tailnet IPv4 address.

    Fully idempotent: skips the install if the binaries exist, the
    daemon start if this install's daemon is already running (matched by
    its absolute statedir, so a user-run tailscaled elsewhere on the
    host is never touched), and the ``tailscale up`` if already joined
    (state persists in the statedir across restarts).
    """
    socks_port, http_port = _ts_ports(tn)
    hostname_flag = f" --hostname={hostname}" if hostname else ""
    if tn.auth_key:
        join_cmd = (
            f'"$TS_DIR/tailscale" --socket="$TS_DIR/run/tailscaled.sock" up '
            f'--auth-key={tn.auth_key} --accept-dns=false{hostname_flag}'
        )
    else:
        join_cmd = (
            'echo "chia: machine is not joined to the tailnet and no '
            'tailnet.auth_key is configured" >&2; exit 1'
        )
    script = [
        tailscale_install_command(tn),
        f'TS_DIR="{tn.tailscale_dir}"',
        # Start this install's daemon if it isn't running (matched by
        # absolute statedir so other tailscaled instances are ignored).
        f'if ! pgrep -f -- "--statedir=$TS_DIR/data" >/dev/null 2>&1; then '
        f'nohup "$TS_DIR/tailscaled" --tun=userspace-networking '
        f'--statedir="$TS_DIR/data" --socket="$TS_DIR/run/tailscaled.sock" '
        f'--socks5-server=localhost:{socks_port} '
        f'--outbound-http-proxy-listen=localhost:{http_port} '
        f'> "$TS_DIR/tailscaled.log" 2>&1 & fi',
        'for i in $(seq 1 40); do [ -S "$TS_DIR/run/tailscaled.sock" ] && break; sleep 0.5; done',
        'if [ ! -S "$TS_DIR/run/tailscaled.sock" ]; then '
        'echo "chia: tailscaled socket never appeared:" >&2; '
        'cat "$TS_DIR/tailscaled.log" >&2; exit 1; fi',
        # Join unless already up (statedir persists the login).
        f'if ! "$TS_DIR/tailscale" --socket="$TS_DIR/run/tailscaled.sock" '
        f'status >/dev/null 2>&1; then {join_cmd}; fi',
        'echo "CHIA_TS_IP=$("$TS_DIR/tailscale" --socket="$TS_DIR/run/tailscaled.sock" ip -4)"',
    ]
    result = ssh.run_script(script, timeout=300)
    for line in result.stdout.splitlines():
        if line.startswith("CHIA_TS_IP="):
            ts_ip = line.split("=", 1)[1].strip()
            if ts_ip:
                logger.info(f"[{ssh.ip}] joined tailnet as {ts_ip}")
                return ts_ip
    raise RuntimeError(
        f"Could not determine tailnet IP on {ssh.ip} — "
        f"'tailscale ip -4' returned nothing.\nstdout: {result.stdout}")


def allocate_tailnet_workers(
    config: ClusterConfig,
    assignments: list[NodeAssignment],
    tailnet_ip_map: dict[str, str] | None = None,
) -> dict[tuple[str, str, int], TailnetWorkerAlloc]:
    """Compute per-worker advertise IPs and port blocks.

    Returns a dict keyed by ``(ip, node_type_name, worker_index)`` for
    every assignment on a tailnet IP — plus every assignment colocated
    on the head machine, which participates exactly like a tailnet
    worker (unique advertise IP, per-machine port block) except that
    peers dial it at ``head_tailnet_ip`` and the head's relay treats it
    as local.  Advertise IPs count up from 127.0.0.2 (globally unique —
    they're the routing key).

    Port blocks are consecutive ``worker_block_size`` slices from
    ``worker_block_base``, indexed PER MACHINE: two workers on different
    machines reuse the same block (no per-port listeners exist to
    collide), only workers sharing a physical machine need distinct
    blocks.

    *tailnet_ip_map* maps a worker's cluster address (how CHIA SSHes to
    it, e.g. an EC2 public IP) to its tailnet address, for machines whose
    tailnet IP is discovered at bring-up. Hosts absent from the map are
    assumed to be addressed by their tailnet IP directly.
    """
    tn = config.tailnet_config
    assert tn is not None
    _check_block_layout(tn)

    result: dict[tuple[str, str, int], TailnetWorkerAlloc] = {}
    next_addr = ipaddress.IPv4Address("127.0.0.2")

    idx_by_machine: dict[str, int] = {}
    for a in assignments:
        if not _is_participant(config, a.ip):
            continue
        block_idx = idx_by_machine.get(a.ip, 0)
        idx_by_machine[a.ip] = block_idx + 1
        result[(a.ip, a.node_type.name, a.worker_index)] = _make_alloc(
            tn, block_idx, str(next_addr),
            _host_tailnet_ip(config, a.ip, tailnet_ip_map))

        next_addr += 1
        if next_addr == ipaddress.IPv4Address("127.0.0.1"):
            next_addr += 1

    return result


def _is_participant(config: ClusterConfig, ip: str) -> bool:
    # Workers colocated on the head machine aren't tailnet-marked (no
    # SSH proxy, no tailscaled of their own) but are full mesh
    # participants: peers reach them at the head's tailnet address.
    return config.is_tailnet(ip) or ip == config.head_ip


def _host_tailnet_ip(config: ClusterConfig, ip: str,
                     tailnet_ip_map: dict[str, str] | None) -> str:
    """The tailnet address peers dial to reach machine *ip*."""
    if ip == config.head_ip:
        return config.tailnet_config.head_tailnet_ip
    return (tailnet_ip_map or {}).get(ip, ip)


def _check_block_layout(tn: TailnetConfig) -> None:
    needed = _WORKER_PORT_OFFSET + tn.worker_port_count
    if needed > tn.worker_block_size:
        raise ConfigError(
            f"tailnet: worker_block_size ({tn.worker_block_size}) too small for "
            f"{tn.worker_port_count} worker ports (needs >= {needed})")
    if tn.tool_port_count > _WORKER_PORT_OFFSET - _TOOL_OFFSET:
        raise ConfigError(
            f"tailnet: tool_port_count ({tn.tool_port_count}) too large "
            f"(max {_WORKER_PORT_OFFSET - _TOOL_OFFSET})")


def _make_alloc(tn: TailnetConfig, block_idx: int, advertise_ip: str,
                tailnet_ip: str) -> TailnetWorkerAlloc:
    """Build (and validate) the alloc for port block *block_idx*."""
    base = tn.worker_block_base + block_idx * tn.worker_block_size
    alloc = TailnetWorkerAlloc(
        advertise_ip=advertise_ip,
        tailnet_ip=tailnet_ip,
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
    if set(alloc.ports()) & set(head_ports(tn)):
        raise ConfigError(
            f"tailnet: worker port block [{base}, {base + tn.worker_block_size}) "
            f"overlaps the head port ranges — adjust worker_block_base/"
            f"head_worker_port_min or reduce worker count")
    if alloc.worker_port_max > 65535:
        raise ConfigError(
            f"tailnet: worker port block [{base}, {base + tn.worker_block_size}) "
            f"exceeds the top of port space (65535) — reduce worker "
            f"count or worker_block_size, or lower worker_block_base")
    return alloc


def live_tailnet_workers(
    config: ClusterConfig,
    ray_nodes: list[dict],
    head_routes: dict[str, str | None],
    tailnet_ip_map: dict[str, str] | None = None,
) -> list[tuple[str, dict, TailnetWorkerAlloc]]:
    """Reconstruct the allocs of a running tailnet cluster's workers.

    Returns ``(cluster_ip, resources, alloc)`` for every alive Ray node
    except the head. A worker's advertise IP is its Ray ``NodeName``;
    its machine comes from the head relay's ``routes`` (the tailnet IP
    peers dial it at, or null for the head machine), mapped back to a
    cluster address; its port block from its ``NodeManagerPort``.
    Reading these from the live cluster rather than re-deriving them
    from the YAML keeps ``chia up --add`` correct after the YAML
    changed (e.g. a higher ``num_workers`` shifts every later worker's
    YAML-derived advertise IP).
    """
    tn = config.tailnet_config
    assert tn is not None
    by_tailnet_ip = {_host_tailnet_ip(config, ip, tailnet_ip_map): ip
                     for ip in config.worker_ips
                     if config.is_tailnet(ip)}

    result: list[tuple[str, dict, TailnetWorkerAlloc]] = []
    for node in ray_nodes:
        adv_ip = node.get("NodeName")
        if not node.get("Alive") or adv_ip == tn.head_advertise_ip:
            continue
        if adv_ip not in head_routes:
            raise RuntimeError(
                f"Ray node {adv_ip} is not in the head relay's routes — "
                f"cannot tell which machine it runs on (was the relay spec "
                f"on the head overwritten?). Run a full 'chia down' and "
                f"'chia up' instead")
        route = head_routes[adv_ip]
        if route is None:
            cluster_ip = config.head_ip
        else:
            # A machine no longer in the YAML keeps its tailnet address
            # as its key: still a peer in every relay spec, never set up.
            cluster_ip = by_tailnet_ip.get(route, route)
        offset = int(node.get("NodeManagerPort", -1)) - tn.worker_block_base
        if offset < 0 or offset % tn.worker_block_size:
            raise RuntimeError(
                f"Ray node {adv_ip} has node manager port "
                f"{node.get('NodeManagerPort')}, outside the tailnet port "
                f"layout (worker_block_base={tn.worker_block_base}, "
                f"worker_block_size={tn.worker_block_size}) — the layout "
                f"changed since 'chia up'. Run a full 'chia down' and "
                f"'chia up' instead")
        alloc = _make_alloc(tn, offset // tn.worker_block_size, adv_ip,
                            route if route is not None else tn.head_tailnet_ip)
        result.append((cluster_ip, node.get("Resources", {}), alloc))
    return result


def allocate_added_tailnet_workers(
    config: ClusterConfig,
    new_assignments: list[NodeAssignment],
    live: list[tuple[str, TailnetWorkerAlloc]],
    tailnet_ip_map: dict[str, str] | None = None,
) -> dict[tuple[str, str, int], TailnetWorkerAlloc]:
    """Allocate workers joining a running cluster around the *live* ones.

    *live* is ``(cluster_ip, alloc)`` for the workers already running
    (see :func:`live_tailnet_workers`). Each new worker takes the lowest
    advertise IP no live worker holds and the lowest port block free on
    its machine, so nothing already running is renumbered. Advertise IPs
    of dead workers are reused: every relay's route is rewritten when
    the cluster's relays are reloaded.
    """
    tn = config.tailnet_config
    assert tn is not None
    _check_block_layout(tn)

    used_ips = {alloc.advertise_ip for _, alloc in live}
    used_ips.add(tn.head_advertise_ip)
    used_blocks: dict[str, set[int]] = {}
    for ip, alloc in live:
        used_blocks.setdefault(ip, set()).add(
            (alloc.node_manager_port - tn.worker_block_base)
            // tn.worker_block_size)

    result: dict[tuple[str, str, int], TailnetWorkerAlloc] = {}
    next_addr = ipaddress.IPv4Address("127.0.0.2")
    for a in new_assignments:
        if not _is_participant(config, a.ip):
            continue
        while str(next_addr) in used_ips or \
                next_addr == ipaddress.IPv4Address("127.0.0.1"):
            next_addr += 1
        blocks = used_blocks.setdefault(a.ip, set())
        block_idx = min(set(range(len(blocks) + 1)) - blocks)
        result[(a.ip, a.node_type.name, a.worker_index)] = _make_alloc(
            tn, block_idx, str(next_addr),
            _host_tailnet_ip(config, a.ip, tailnet_ip_map))
        used_ips.add(str(next_addr))
        blocks.add(block_idx)

    return result


def build_relay_spec(
    config: ClusterConfig,
    allocs: dict[tuple[str, str, int], TailnetWorkerAlloc],
    host_ip: str | None,
) -> dict:
    """Build the relay spec for one host.

    *host_ip* is the machine's cluster address (how CHIA SSHes to it),
    or ``None`` for the head machine.

    The relay carries all of Ray's gRPC through a single HTTP CONNECT
    listener, with a ``routes`` table mapping every advertise IP to its
    owning machine's tailnet IP (null for this machine's own
    participants → dialed locally, skipping a tailscale hairpin).
    Workers colocated on the head machine are the head relay's own
    participants (keyed by ``config.head_ip``, matched here even though
    the head relay is requested with ``host_ip=None``).

    ChiaTool traffic is plain HTTP (httpx) which can't use the CONNECT
    proxy, so peer TOOL ports keep small per-port SOCKS listeners; and
    since tool servers bind the advertise IP (not wildcard) while
    tailscaled delivers inbound to 127.0.0.1, each host also runs a
    local ``direct`` bridge for its OWN tool ports.
    """
    tn = config.tailnet_config
    assert tn is not None

    # Allocs are keyed by cluster address; the head relay's own machine
    # is addressed by config.head_ip.
    local_ip = config.head_ip if host_ip is None else host_ip

    # routes: advertise_ip -> owning machine's tailnet IP, or None when
    # the participant lives on THIS machine (dial 127.0.0.1 directly).
    routes: dict[str, str | None] = {
        tn.head_advertise_ip: None if host_ip is None else tn.head_tailnet_ip
    }
    for (cluster_ip, _, _), alloc in allocs.items():
        routes[alloc.advertise_ip] = None if cluster_ip == local_ip else alloc.tailnet_ip

    listeners: list[dict] = [
        {"bind_ip": "127.0.0.1", "port": tn.connect_proxy_port, "via": "connect"}
    ]

    # PEER tool ports: per-port SOCKS listeners (head is a peer to
    # workers and vice versa).
    if host_ip is not None:
        for port in range(tn.head_tool_port_min, tn.head_tool_port_max + 1):
            listeners.append({"bind_ip": tn.head_advertise_ip, "port": port,
                              "dest_ip": tn.head_tailnet_ip})
    else:
        # OWN (head) tool bridge.
        for port in range(tn.head_tool_port_min, tn.head_tool_port_max + 1):
            listeners.append({"bind_ip": "127.0.0.1", "port": port,
                              "dest_ip": tn.head_advertise_ip, "via": "direct"})
    for (cluster_ip, _, _), alloc in allocs.items():
        if cluster_ip == local_ip:
            # OWN tools: inbound bridge 127.0.0.1:<port> -> advertise IP.
            for port in range(alloc.tool_port_min, alloc.tool_port_max + 1):
                listeners.append({"bind_ip": "127.0.0.1", "port": port,
                                  "dest_ip": alloc.advertise_ip, "via": "direct"})
        else:
            for port in range(alloc.tool_port_min, alloc.tool_port_max + 1):
                listeners.append({"bind_ip": alloc.advertise_ip, "port": port,
                                  "dest_ip": alloc.tailnet_ip})

    return {"socks_proxy": tn.socks_proxy, "listeners": listeners,
            "routes": routes}


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


def update_relay(ssh: SSHClient, spec: dict) -> None:
    """Hot-reload the relay on *ssh*'s host with *spec*, or start it.

    A running relay is sent SIGHUP to diff in the new listeners and
    routes, so the Ray connections it is carrying survive (``chia up
    --add``). A host without a running relay gets a fresh
    :func:`start_relay`. A relay started before hot reload existed is
    restarted too, with a warning: that drops its in-flight connections.
    """
    if not spec["listeners"]:
        logger.debug(f"[{ssh.ip}] No relay listeners needed, skipping")
        return
    token = uuid.uuid4().hex
    spec_json = json.dumps({**spec, "reload_token": token}, indent=1)
    # One if/elif/else rather than early `exit 0`s: in a login shell,
    # `exit` runs ~/.bash_logout, and Ubuntu's stock one (clear_console
    # failing with no console) turns `exit 0` into exit status 1.
    result = ssh.run_script([
        f"PID=$(cat {_REMOTE_BASE}.pid 2>/dev/null || true)",
        f'if [ -z "$PID" ] || ! pgrep -f "chia_tailnet_rela[y]_$USER.py" '
        f'| grep -qx "$PID"; then echo CHIA_RELAY_ABSENT',
        f'elif ! grep -q "^CHIA_RELAY_READY .* reload$" {_REMOTE_BASE}.log '
        f'2>/dev/null; then echo CHIA_RELAY_LEGACY',
        "else",
        f"cat > {_REMOTE_BASE}.json <<'CHIA_RELAY_SPEC_EOF'\n"
        f"{spec_json}\n"
        f"CHIA_RELAY_SPEC_EOF",
        'kill -HUP "$PID"',
        f'for i in $(seq 1 40); do '
        f'grep -q "^CHIA_RELAY_RELOADED {token} " {_REMOTE_BASE}.log && break; '
        f'sleep 0.5; done',
        f'if ! grep "^CHIA_RELAY_RELOADED {token} " {_REMOTE_BASE}.log; then '
        f'echo "chia tailnet relay did not acknowledge the reload:"; '
        f'tail -n 50 {_REMOTE_BASE}.log; exit 1; fi',
        "fi",
    ], timeout=120)
    if "CHIA_RELAY_ABSENT" in result.stdout:
        start_relay(ssh, spec)
        return
    if "CHIA_RELAY_LEGACY" in result.stdout:
        logger.warning(
            f"[{ssh.ip}] Tailnet relay predates hot reload — restarting it, "
            f"which drops the Ray connections it carries")
        start_relay(ssh, spec)
        return
    ack = next(line for line in result.stdout.splitlines()
               if line.startswith(f"CHIA_RELAY_RELOADED {token} "))
    if not ack.endswith(" errors=0"):
        log = ssh.run(f"tail -n 50 {_REMOTE_BASE}.log", check=False)
        raise RuntimeError(
            f"Tailnet relay on {ssh.ip} failed to bind new listener(s) on "
            f"reload ({ack}):\n{log.stdout}")
    logger.info(f"[{ssh.ip}] Tailnet relay reloaded "
                f"({len(spec['listeners'])} listeners)")


def read_relay_spec(ssh: SSHClient) -> dict | None:
    """The spec of the relay deployed on *ssh*'s host, or ``None``."""
    result = ssh.run(f"cat {_REMOTE_BASE}.json", check=False)
    if result.returncode != 0:
        return None
    try:
        return json.loads(result.stdout)
    except ValueError:
        return None


def query_tailnet_ip(ssh: SSHClient, tn: TailnetConfig) -> str | None:
    """The tailnet IPv4 of the CHIA-managed tailscaled on *ssh*'s host,
    or ``None`` when it isn't running — unlike :func:`ensure_tailscale`,
    never installs, starts, or joins anything (safe for ``--dry-run``)."""
    try:
        result = ssh.run_script([
            f'TS_DIR="{tn.tailscale_dir}"',
            'if [ -S "$TS_DIR/run/tailscaled.sock" ]; then '
            'echo "CHIA_TS_IP=$("$TS_DIR/tailscale" '
            '--socket="$TS_DIR/run/tailscaled.sock" ip -4 2>/dev/null)"; fi',
        ], timeout=60, check=False)
    except Exception as e:
        logger.debug(f"[{ssh.ip}] tailnet IP query failed: {e}")
        return None
    for line in result.stdout.splitlines():
        if line.startswith("CHIA_TS_IP="):
            return line.split("=", 1)[1].strip() or None
    return None


def stop_relay(ssh: SSHClient) -> None:
    """Stop the tailnet relay on *ssh*'s host (best effort)."""
    ssh.run_script([
        f'pkill -f "chia_tailnet_rela[y]_$USER.py" 2>/dev/null || true',
        f"rm -f {_REMOTE_BASE}.pid",
    ], check=False)
    logger.info(f"[{ssh.ip}] Tailnet relay stopped")


def stop_tailscaled(ssh: SSHClient, tn: TailnetConfig) -> None:
    """Stop the CHIA-managed tailscaled on *ssh*'s host (best effort).

    Matches only the daemon whose statedir lives under
    ``tn.tailscale_dir`` — a personally-run tailscaled is never touched.
    State persists in the statedir, so a later ``chia up`` rejoins
    without consuming the auth key (unless the dir was cleaned).
    """
    ssh.run_script([
        f'TS_DIR="{tn.tailscale_dir}"',
        'pkill -f -- "--statedir=$TS_DIR/data" 2>/dev/null || true',
    ], check=False)
    logger.info(f"[{ssh.ip}] Managed tailscaled stopped")
