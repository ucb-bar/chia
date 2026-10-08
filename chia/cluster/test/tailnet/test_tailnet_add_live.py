"""Live end-to-end tests for ``chia up --add`` on tailnet (tailscale) clusters.

Each test extends one of the EC2 example configs in ``examples/tailscale/``
(``cluster_ec2.yaml``, ``cluster_ec2_fullymanaged.yaml``) and:

  1. ``chia up`` the example and runs ``connectivity-matrix.py`` on it;
  2. holds a Ray actor on the EC2 worker, in use across the add;
  3. ``chia up --add`` a grown config — a second EC2 machine with a new
     logical worker (``ec2_added``), and a second logical worker on the
     head machine (``head_added``) listed FIRST, so that YAML-derived
     allocation would renumber every live worker;
  4. checks every pre-existing Ray node is still alive and the held
     actor still answers (the relays were hot-reloaded, not restarted);
  5. checks a repeated ``chia up --add`` has nothing to add;
  6. reruns the connectivity matrix including the added workers;
  7. ``chia down``s the grown config (terminating both EC2 instances).

They launch real EC2 instances, join them to your tailnet, and start
Ray on the head with the examples' ``ray stop`` — which kills any other
Ray on the head machine. Run them ON the head machine, gated behind
``CHIA_RUN_TAILNET_LIVE_TESTS=1``::

    export CHIA_RUN_TAILNET_LIVE_TESTS=1
    export HEAD_IP=$(hostname -I | awk '{print $1}')
    export TS_AUTHKEY=$(cat examples/tailscale/keyfile)  # reusable tskey-auth-...
    export AWS_KEYPAIR=<EC2 key pair; its key in your ssh-agent>
    # optional: AWS_KEYPAIR_PEM   the key pair's private key, if not in the agent
    # optional: WORKER_IP         on-prem worker for cluster_ec2_fullymanaged.yaml
    # optional: WORKER_TAILNET_IP on-prem tailnet worker for cluster_ec2.yaml
    # optional: HEAD_TAILNET_IP   for cluster_ec2.yaml, when you already run a
    #                             userspace tailscaled on the head (SOCKS5 on
    #                             127.0.0.1:1055); otherwise the test starts one
    python -m pytest chia/cluster/test/tailnet/test_tailnet_add_live.py -v -s

AWS API access comes from ``~/.aws`` as for any ``chia up``. Without the
optional on-prem worker the example's ``tailscale_worker`` type is dropped.
"""

from __future__ import annotations

import copy
import dataclasses
import json
import os
import socket
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
import yaml

from chia.cluster.config import TailnetConfig, build_config, load_raw_config

REPO = Path(__file__).resolve().parents[4]
EXAMPLES = REPO / "examples" / "tailscale"
MATRIX = EXAMPLES / "connectivity-matrix.py"
CHIA = Path(sys.executable).parent / "chia"

# Long: EC2 provisioning + the default AWS setup (conda, chia) dominates.
UP_TIMEOUT = 3600
# A userspace tailscaled for the head of cluster_ec2.yaml, when the
# caller doesn't already run one (the example expects SOCKS5 on 1055).
_HEAD_TS = TailnetConfig(socks_proxy="127.0.0.1:1055",
                         tailscale_dir="/tmp/chia-live-test-head/tailscale")

_RUN = os.environ.get("CHIA_RUN_TAILNET_LIVE_TESTS") == "1"
pytestmark = pytest.mark.skipif(
    not _RUN,
    reason="set CHIA_RUN_TAILNET_LIVE_TESTS=1 to run (launches EC2 instances, "
           "joins your tailnet, and runs `ray stop` on the head)")


def _require(*names: str) -> None:
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        pytest.skip(f"set {', '.join(missing)} to run")


def _is_local(host: str) -> bool:
    try:
        addr = socket.gethostbyname(host)
    except OSError:
        return False
    out = subprocess.run(["hostname", "-I"], capture_output=True, text=True)
    return addr.startswith("127.") or addr in out.stdout.split()


def _port_listening(port: int) -> bool:
    with socket.socket() as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def _extend(example: str, worker_env: str) -> tuple[dict, dict]:
    """The example config (base) and its grown ``--add`` counterpart.

    Placeholders like ``${HEAD_IP}`` stay unexpanded: ``chia`` expands
    them from the environment when it loads the written files.
    """
    with open(EXAMPLES / example) as f:
        base = yaml.safe_load(f)
    # Never collide with a real cluster brought up from the example.
    base["cluster_name"] = f"{base['cluster_name']}LiveAdd"
    if not os.environ.get(worker_env):
        del base["available_node_types"]["tailscale_worker"]
    if os.environ.get("AWS_KEYPAIR_PEM"):
        base["aws_nodes"]["ec2_worker"]["ssh_private_key"] = "${AWS_KEYPAIR_PEM}"

    grown = copy.deepcopy(base)
    grown["aws_nodes"]["ec2_worker"]["count"] += 1
    env_cmds = base["available_node_types"]["head_worker"]["worker_env_commands"]
    grown["available_node_types"] = {
        "head_added": {"resources": {"head_added": 2}, "num_workers": 1,
                       "compatible_ips": ["${HEAD_IP}"],
                       "worker_env_commands": env_cmds},
        **base["available_node_types"],
        "ec2_added": {"resources": {"ec2_added": 2}, "num_workers": 1,
                      "compatible_ips": ["@ec2_worker:1"],
                      "worker_env_commands": env_cmds},
    }
    return base, grown


def _write(path: Path, raw: dict) -> str:
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return str(path)


def _chia(*args: str) -> str:
    """Run the chia CLI, streaming its output (visible with ``-s``);
    return the combined output, failing on error."""
    print(f"$ chia {' '.join(args)}", flush=True)
    proc = subprocess.Popen([str(CHIA), *args], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)
    lines = []
    for line in proc.stdout:
        print(line, end="", flush=True)
        lines.append(line)
    returncode = proc.wait(timeout=UP_TIMEOUT)
    out = "".join(lines)
    assert returncode == 0, f"chia {' '.join(args)} failed:\n{out}"
    return out


def _driver_env(config_path: str) -> dict:
    """Env for a driver on the head of a tailnet cluster (README.md)."""
    tn = build_config(load_raw_config(config_path)).tailnet_config
    return {**os.environ,
            "RAY_ADDRESS": f"{tn.head_advertise_ip}:{tn.gcs_port}",
            "RAY_grpc_enable_http_proxy": "1",
            "grpc_proxy": f"http://127.0.0.1:{tn.connect_proxy_port}",
            "no_grpc_proxy": f"{tn.head_advertise_ip},127.0.0.1,localhost"}


def _matrix(env: dict, *extra_tags: str) -> str:
    result = subprocess.run([sys.executable, str(MATRIX), *extra_tags],
                            capture_output=True, text=True, env=env,
                            timeout=1800)
    out = result.stdout + result.stderr
    print(out)
    assert result.returncode == 0 and "CONNECTIVITY MATRIX PASSED" in out, out
    return out


_NODES = textwrap.dedent("""
    import json, os, ray
    ray.init(address=os.environ["RAY_ADDRESS"])
    print("CHIA_NODES:" + json.dumps(
        {n["NodeID"]: n["Alive"] for n in ray.nodes()}))
""")


def _alive_nodes(env: dict) -> set[str]:
    result = subprocess.run([sys.executable, "-c", _NODES], capture_output=True,
                            text=True, env=env, timeout=300)
    line = next(l for l in result.stdout.splitlines()
                if l.startswith("CHIA_NODES:"))
    return {nid for nid, alive in json.loads(line[11:]).items() if alive}


# Holds an actor on the EC2 worker across the add: its gRPC channels ride
# the head relay and the EC2 machine's relay, both reloaded by --add.
_HOLDER = textwrap.dedent("""
    import os, socket, sys, ray
    ray.init(address=os.environ["RAY_ADDRESS"])

    @ray.remote(num_cpus=0, resources={"ec2_worker": 0.05})
    class Pinger:
        def ping(self, i):
            return f"{i}@{socket.gethostname()}"

    pinger = Pinger.remote()
    print("HELD", ray.get(pinger.ping.remote(0), timeout=300), flush=True)
    sys.stdin.readline()
    print("AFTER", ray.get(pinger.ping.remote(1), timeout=120), flush=True)
""")


def _check_add(base_path: str, grown_path: str) -> None:
    env = _driver_env(base_path)
    _matrix(env)

    before = _alive_nodes(env)
    holder = subprocess.Popen([sys.executable, "-c", _HOLDER],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              text=True, env=env)
    try:
        held = holder.stdout.readline()
        assert held.startswith("HELD 0@"), held

        out = _chia("up", grown_path, "--add", "-y")
        assert "Will add 2 new worker(s)" in out, out

        holder.stdin.write("go\n")
        holder.stdin.flush()
        after_line = holder.stdout.readline()
        assert after_line.startswith("AFTER 1@"), after_line
        assert holder.wait(timeout=60) == 0
    finally:
        if holder.poll() is None:
            holder.kill()

    after = _alive_nodes(env)
    assert before <= after, f"--add disturbed live nodes: {before - after}"
    assert len(after) == len(before) + 2

    out = _chia("up", grown_path, "--add", "-y")
    assert "All desired workers already exist" in out, out

    out = _matrix(env, "head_added", "ec2_added")
    assert "'head_added'" in out and "'ec2_added'" in out


@pytest.fixture
def head_tailscaled(live_env, monkeypatch):
    """For cluster_ec2.yaml: a userspace tailscaled on the head, unless
    the caller runs one (and exports HEAD_TAILNET_IP)."""
    if os.environ.get("HEAD_TAILNET_IP"):
        yield
        return
    if _port_listening(1055):
        pytest.skip("something already serves 127.0.0.1:1055 — export "
                    "HEAD_TAILNET_IP for the tailscaled behind it")
    from chia.cluster.ssh import SSHClient
    from chia.cluster.tailnet import ensure_tailscale, stop_tailscaled
    ssh = SSHClient(os.environ["HEAD_IP"], os.environ["USER"])
    tn = dataclasses.replace(_HEAD_TS, auth_key=os.environ["TS_AUTHKEY"])
    monkeypatch.setenv("HEAD_TAILNET_IP",
                       ensure_tailscale(ssh, tn, hostname="chia-live-test-head"))
    try:
        yield
    finally:
        stop_tailscaled(ssh, tn)


@pytest.fixture
def live_env():
    _require("HEAD_IP", "TS_AUTHKEY", "AWS_KEYPAIR")
    if not _is_local(os.environ["HEAD_IP"]):
        pytest.skip("run on the head machine (HEAD_IP is not local): the "
                    "connectivity matrix drives the cluster from the head")


def _run(tmp_path: Path, example: str, worker_env: str) -> None:
    base, grown = _extend(example, worker_env)
    base_path = _write(tmp_path / "base.yaml", base)
    grown_path = _write(tmp_path / "grown.yaml", grown)
    try:
        _chia("up", base_path, "-y")
        _check_add(base_path, grown_path)
    finally:
        # Tear down with the config matching the instances that exist:
        # the grown one references a second machine that only --add
        # provisions (down rejects an @ec2_worker:1 with no instance).
        _chia("down", grown_path if _instances(base) > 1 else base_path, "-y")


def _instances(raw: dict) -> int:
    """How many of the cluster's ``ec2_worker`` instances are running."""
    from chia.cluster.aws_nodes import discover_aws_nodes
    found = discover_aws_nodes(raw["cluster_name"], raw["aws_nodes"]["region"])
    return len(found.get("ec2_worker", []))


def test_add_cluster_ec2(tmp_path, head_tailscaled):
    _run(tmp_path, "cluster_ec2.yaml", "WORKER_TAILNET_IP")


def test_add_cluster_ec2_fullymanaged(tmp_path, live_env):
    _run(tmp_path, "cluster_ec2_fullymanaged.yaml", "WORKER_IP")
