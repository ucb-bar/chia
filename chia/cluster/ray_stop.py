"""Scoped ``ray stop``: stop only the Ray processes that belong to ONE
cluster, identified by the cluster's GCS address embedded in each process's
command line.

Plain ``ray stop`` kills every Ray process on the host (it matches by process
name, with no notion of cluster identity), so on a machine shared by several
Ray clusters it takes them all down. This module is a standalone port of the
*consumer* half of ray-project/ray#60454 — it reads the GCS address out of
each candidate process's argv and stops only the ones matching a target
address. It does NOT require any patch to Ray.

It is intentionally self-contained (only ``psutil`` + the stdlib, plus an
optional import of the installed Ray's process list) so chia can pipe it to a
node over SSH and run it under that node's Ray environment, where ``psutil``
is already present. See ``chia/cluster/node_setup.py`` for the delivery path.

Run standalone:

    python3 ray_stop.py --address <host>:<port> [--force] [--grace-period N]

Exit code is always 0 on a completed sweep (teardown is best-effort); the
number of matched/stopped processes is printed for the caller to log.
"""
from __future__ import annotations

import argparse
import os
import socket
import sys
import time
from typing import List, Optional, Tuple

try:
    import psutil
except ImportError:
    # Ray bundles psutil under ray/thirdparty_files and adds it to sys.path on
    # ``import ray``; a chia env commonly has no standalone psutil, only Ray's
    # bundled copy. Import ray first, then retry.
    try:
        import ray  # noqa: F401  (side effect: puts bundled psutil on sys.path)
        import psutil
    except Exception:  # pragma: no cover - only if Ray itself is absent
        psutil = None


# Kept in sync with ray.autoscaler._private.constants.RAY_PROCESSES. We import
# the installed Ray's copy when available so we track whatever version is on
# the node; the embedded list is a fallback for when that import fails.
def _load_ray_processes() -> List[Tuple[str, bool]]:
    try:
        from ray.autoscaler._private.constants import RAY_PROCESSES

        return [(kw, bool(by_cmd)) for kw, by_cmd in RAY_PROCESSES]
    except Exception:
        darwin = sys.platform == "darwin"
        return [
            ("raylet", True),
            ("plasma_store", True),
            ("monitor.py", False),
            ("ray.util.client.server", False),
            ("default_worker.py", False),
            ("setup_worker.py", False),
            ("ray::", not darwin),
            ("io.ray.runtime.runner.worker.DefaultWorker", False),
            ("log_monitor.py", False),
            ("ray::DashboardAgent", False),
            (os.path.join("dashboard", "dashboard.py"), False),
            ("ray::RuntimeEnvAgent", False),
            ("ray_process_reaper.py", False),
            ("gcs_server", True),
        ]


# ---------------------------------------------------------------------------
# GCS address extraction (mirrors _extract_gcs_address_from_cmdline in the PR)
# ---------------------------------------------------------------------------


def _extract_flag_value(cmdline: List[str], flag: str) -> Optional[str]:
    """Value of ``--flag=value`` or ``--flag value`` (also single-dash)."""
    for prefix in (f"--{flag}", f"-{flag}"):
        eq = prefix + "="
        for i, tok in enumerate(cmdline):
            if tok.startswith(eq):
                return tok[len(eq):]
            if tok == prefix and i + 1 < len(cmdline):
                return cmdline[i + 1]
    return None


def _extract_java_property_value(cmdline: List[str], prop: str) -> Optional[str]:
    """Value of a JVM ``-Dprop=value`` system property."""
    needle = f"-D{prop}="
    for tok in cmdline:
        if tok.startswith(needle):
            return tok[len(needle):]
    return None


def extract_gcs_address_from_cmdline(cmdline: List[str]) -> Optional[str]:
    """Extract the GCS address a process is attached to, or None if unknown.

    Handles every Ray process shape that carries the address in argv:

    1. Most processes:      ``--gcs-address=ip:port``
    2. C++ worker wrappers: ``--ray_address=ip:port``
    3. Java worker wrappers: ``-Dray.address=ip:port``
    4. Ray Client server:   ``--address=ip:port`` (only for ray.util.client.server)
    5. gcs_server itself:   ``--node-ip-address=ip`` + ``--gcs_server_port=port``

    Processes whose identity can't be recovered (e.g. after setproctitle has
    clobbered argv) return None and are left alone — they fate-share off their
    cluster's raylet/gcs and exit on their own.
    """
    if not cmdline:
        return None

    # 1. Most Python processes (raylet, monitor, dashboard, agents, workers).
    addr = _extract_flag_value(cmdline, "gcs-address")
    if addr:
        return addr

    # 2. C++ worker wrapper.
    addr = _extract_flag_value(cmdline, "ray_address")
    if addr:
        return addr

    # 3. Java worker wrapper.
    addr = _extract_java_property_value(cmdline, "ray.address")
    if addr:
        return addr

    # 4. Ray Client server uses a bare --address (avoid matching unrelated
    #    processes that happen to carry --address, e.g. `ray start` itself).
    joined = " ".join(cmdline)
    if "ray.util.client.server" in joined:
        addr = _extract_flag_value(cmdline, "address")
        if addr:
            return addr

    # 5. gcs_server self-identifies by node ip + its own port.
    if any("gcs_server" in tok for tok in cmdline):
        node_ip = _extract_flag_value(cmdline, "node-ip-address")
        port = _extract_flag_value(cmdline, "gcs_server_port")
        if node_ip and port and port != "0":
            return build_address(node_ip, port)

    return None


# ---------------------------------------------------------------------------
# Address normalization (mirrors is_same_gcs_address in the PR)
# ---------------------------------------------------------------------------


def build_address(host: str, port) -> str:
    """Join ``host`` and ``port``, bracketing IPv6 literals."""
    host = str(host)
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"{host}:{port}"


def _split_host_port(address: str) -> Tuple[str, Optional[str]]:
    address = address.strip()
    if address.startswith("["):  # [ipv6]:port
        end = address.find("]")
        if end != -1:
            host = address[1:end]
            rest = address[end + 1:]
            port = rest[1:] if rest.startswith(":") else None
            return host, port
    if address.count(":") == 1:
        host, port = address.split(":", 1)
        return host, port
    return address, None


# Ray's is_localhost set (see ray._private.services.is_localhost). NOTE:
# 127.0.0.N for N>=2 (chia's tunnel IPs) is deliberately NOT localhost — those
# are concrete, distinct addresses that must never collapse together.
_LOCALHOST_NAMES = {"localhost", "127.0.0.1", "::1"}

_NODE_IP: Optional[str] = None


def _is_localhost(host: str) -> bool:
    return host.strip().lower() in _LOCALHOST_NAMES


def get_node_ip_address() -> str:
    """This machine's primary, remotely-reachable IP (cached).

    Mirrors Ray's ``get_node_ip_address``: open a UDP socket toward a public
    address and read back the local endpoint the OS chose. Ray runs this at
    ``ray start`` time to rewrite a ``localhost``/``127.0.0.1`` head into a
    routable IP, so a head process advertises that real IP — we resolve our
    target the same way (on the head, where this runs) so they compare equal.
    """
    global _NODE_IP
    if _NODE_IP is not None:
        return _NODE_IP
    ip = "127.0.0.1"
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 53))
            ip = s.getsockname()[0]
        finally:
            s.close()
    except OSError:
        try:
            ip = socket.gethostbyname(socket.gethostname())
        except (OSError, UnicodeError):
            ip = "127.0.0.1"
    _NODE_IP = ip
    return ip


def _normalize_host(host: str) -> str:
    host = host.strip().lower()
    if _is_localhost(host):
        # Ray rewrote localhost -> a routable node IP at start time, so the
        # process advertises the real IP; resolve our side to match.
        return get_node_ip_address()
    # A concrete 127.x.x.x loopback (chia tunnel IPs are 127.0.0.N, N>=2) is
    # already exact — keep it as-is.
    try:
        socket.inet_aton(host)
        return host
    except OSError:
        pass
    # Hostname: best-effort resolve so ``head`` and its IP compare equal.
    try:
        return socket.gethostbyname(host)
    except (OSError, UnicodeError):
        return host


def normalize_gcs_address(address: str) -> str:
    host, port = _split_host_port(address)
    return build_address(_normalize_host(host), port)


def is_same_gcs_address(a: str, b: str) -> bool:
    if a is None or b is None:
        return False
    return normalize_gcs_address(a) == normalize_gcs_address(b)


def cmdline_matches_gcs_address(cmdline: List[str], targets) -> bool:
    """True if the process's GCS address matches any of ``targets``.

    ``targets`` may be a single address string or an iterable of them.
    """
    addr = extract_gcs_address_from_cmdline(cmdline)
    if addr is None:
        return False
    if isinstance(targets, str):
        targets = (targets,)
    return any(is_same_gcs_address(addr, t) for t in targets)


# ---------------------------------------------------------------------------
# Scoped kill (mirrors the bucketed kill loop in the PR's `stop`)
# ---------------------------------------------------------------------------


def selective_ray_stop(
    targets,
    force: bool = False,
    grace_period: float = 30.0,
) -> Tuple[int, int]:
    """Stop only Ray processes whose GCS address matches one of ``targets``.

    ``targets`` may be a single address string or an iterable of them (a
    process matches if it matches any).

    Returns ``(found, stopped)``. Processes are killed in three ordered
    buckets — raylet first, everything else, gcs_server last — matching Ray's
    own ``ray stop`` so fate-sharing agents don't emit spurious errors on the
    way down. SIGTERM is sent first, escalating to SIGKILL after the grace
    period (or immediately with ``force``).
    """
    if psutil is None:
        raise RuntimeError("psutil is required for scoped ray stop")

    if isinstance(targets, str):
        targets = [targets]
    targets = list(targets)

    processes_to_kill = _load_ray_processes()
    assert processes_to_kill[0][0] == "raylet"
    assert processes_to_kill[-1][0] == "gcs_server"
    buckets = [
        [processes_to_kill[0]],
        processes_to_kill[1:-1],
        [processes_to_kill[-1]],
    ]

    # Snapshot every process once.
    proc_infos = []
    for proc in psutil.process_iter(["name", "cmdline"]):
        try:
            proc_infos.append((proc, proc.info["name"], proc.info["cmdline"] or []))
        except psutil.Error:
            pass

    is_linux = sys.platform.startswith("linux")
    total_found = 0
    total_stopped = 0

    for bucket in buckets:
        matched = []
        seen_pids = set()
        for keyword, filter_by_cmd in bucket:
            # Linux truncates process names to 15 chars; a longer keyword can
            # never match a name, so skip those (same guard as Ray).
            if filter_by_cmd and is_linux and len(keyword) > 15:
                continue
            for proc, name, cmdline in proc_infos:
                if proc.pid in seen_pids:
                    continue
                corpus = name if filter_by_cmd else " ".join(cmdline)
                if keyword not in corpus:
                    continue
                # Scope: only ours. Skip processes we can't attribute.
                if not cmdline_matches_gcs_address(cmdline, targets):
                    continue
                matched.append(proc)
                seen_pids.add(proc.pid)

        if not matched:
            continue
        total_found += len(matched)

        for proc in matched:
            try:
                proc.kill() if force else proc.terminate()
            except psutil.NoSuchProcess:
                pass
            except (psutil.Error, OSError):
                pass

        gone, alive = psutil.wait_procs(
            matched, timeout=0 if force else grace_period / len(buckets)
        )
        total_stopped += len(gone)
        for proc in alive:  # escalate any stragglers
            try:
                proc.kill()
            except psutil.Error:
                pass
        psutil.wait_procs(alive, timeout=2)

    return total_found, total_stopped


def count_foreign_ray_processes(targets) -> int:
    """Number of live Ray processes attributable to a *different* cluster than
    ``targets`` (a GCS address in argv that matches none of the targets).

    Used to decide whether a shared Docker container may be removed. Processes
    whose cluster can't be determined (no GCS address in argv) are treated as
    ours and are NOT counted, so a container that is really single-tenant still
    reports zero.
    """
    if psutil is None:
        raise RuntimeError("psutil is required for scoped ray stop")
    if isinstance(targets, str):
        targets = [targets]
    targets = list(targets)

    keywords = _load_ray_processes()
    is_linux = sys.platform.startswith("linux")
    counted: set = set()
    foreign = 0
    for proc in psutil.process_iter(["name", "cmdline"]):
        try:
            name = proc.info["name"]
            cmdline = proc.info["cmdline"] or []
        except psutil.Error:
            continue
        is_ray = False
        for keyword, filter_by_cmd in keywords:
            if filter_by_cmd and is_linux and len(keyword) > 15:
                continue
            corpus = name if filter_by_cmd else " ".join(cmdline)
            if keyword in corpus:
                is_ray = True
                break
        if not is_ray or proc.pid in counted:
            continue
        counted.add(proc.pid)
        addr = extract_gcs_address_from_cmdline(cmdline)
        if addr is not None and not any(is_same_gcs_address(addr, t) for t in targets):
            foreign += 1
    return foreign


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Scoped ray stop by GCS address")
    parser.add_argument(
        "--address",
        required=True,
        help="GCS address (host:port) of the cluster to stop.",
    )
    parser.add_argument("--force", action="store_true", help="SIGKILL immediately.")
    parser.add_argument(
        "--grace-period",
        type=float,
        default=30.0,
        help="Seconds to wait for graceful exit before SIGKILL.",
    )
    parser.add_argument(
        "--match-node-ip",
        action="store_true",
        help=(
            "Also stop processes advertising THIS machine's own node IP at the "
            "target's port. Intended for a head whose configured IP differs "
            "from the IP Ray actually advertised (e.g. a multi-homed host). "
            "Safe: a GCS port is unique per cluster on a machine, so matching "
            "by port can only ever hit this cluster's own processes."
        ),
    )
    args = parser.parse_args(argv)

    targets = [args.address]
    if args.match_node_ip:
        _, port = _split_host_port(args.address)
        if port:
            targets.append(build_address(get_node_ip_address(), port))

    try:
        found, stopped = selective_ray_stop(
            targets, force=args.force, grace_period=args.grace_period
        )
        foreign = count_foreign_ray_processes(targets)
    except Exception as e:
        # Best-effort: report so the caller can fall back to a blanket stop.
        print(f"CHIA_RAYSTOP_ERROR: {e}", file=sys.stderr)
        return 1

    print(f"CHIA_RAYSTOP: found={found} stopped={stopped} foreign={foreign} "
          f"address={args.address}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
