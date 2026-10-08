"""Unit tests for ``chia up --add`` on tailnet (tailscale) clusters.

Covers reconstructing a running cluster's tailnet workers from Ray's
node table, allocating added workers around them, the add orchestration
(tailnet joins, relay hot-reload, worker setup), the ``chia up --add``
CLI flow with cloud provisioning, and the relay's SIGHUP hot reload —
the last against a real relay process, over loopback only.

Run:
  python -m pytest test/tailnet/test_tailnet_add.py -v
"""

import argparse
import copy
import json
import os
import queue
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

from chia.cluster import node_setup, tailnet
from chia.cluster.config import assign_nodes, build_config
from chia.cluster.node_setup import TailnetAddPlan, plan_tailnet_add
from chia.cluster.tailnet import (
    RELAY_SCRIPT, allocate_added_tailnet_workers, allocate_tailnet_workers,
    build_relay_spec, live_tailnet_workers, update_relay,
)

HEAD = "10.0.0.1"
M2, M3 = "100.64.0.2", "100.64.0.3"


def _make_raw():
    return {
        "cluster_name": "tailnet-add-test",
        "tailnet": {"head_tailnet_ip": "100.64.0.1"},
        "provider": {"head_ip": HEAD},
        "auth": {"ssh_user": "u"},
        "available_node_types": {
            "tw": {
                "resources": {"tw": 2},
                "num_workers": 3,
                "compatible_ips": [M2, M3],
            },
        },
        "head_start_ray_commands": ["ray start --head --port=6379"],
        "worker_start_ray_commands": ["ray start --address=$RAY_HEAD_IP:6379"],
    }


def _running(raw, dead=()):
    """Simulate ``chia up`` of *raw*: the Ray node table and head routes.

    *dead* lists ``(ip, type, index)`` keys whose Ray node is not alive.
    """
    config = build_config(raw)
    allocs = allocate_tailnet_workers(config, assign_nodes(config))
    head = config.tailnet_config.head_advertise_ip
    nodes = [{"NodeName": head, "Alive": True, "Resources": {"CPU": 4},
              "NodeManagerPort": config.tailnet_config.head_node_manager_port}]
    for key, alloc in allocs.items():
        nt = config.node_types[key[1]]
        nodes.append({"NodeName": alloc.advertise_ip, "Alive": key not in dead,
                      "Resources": {"CPU": 1, **nt.resources},
                      "NodeManagerPort": alloc.node_manager_port})
    routes = build_relay_spec(config, allocs, None)["routes"]
    return allocs, nodes, routes


def _grown(raw):
    """*raw* with a new type listed BEFORE ``tw`` — so the YAML-derived
    allocation of every ``tw`` worker shifts — plus one more ``tw``."""
    raw = copy.deepcopy(raw)
    types = raw["available_node_types"]
    types["tw"]["num_workers"] = 4
    raw["available_node_types"] = {
        "aa": {"resources": {"aa": 1}, "num_workers": 1, "compatible_ips": [M2]},
        **types,
    }
    return raw


class TestLiveWorkers(unittest.TestCase):

    def test_reconstructs_machine_and_block(self):
        raw = _make_raw()
        allocs, nodes, routes = _running(raw)
        config = build_config(raw)
        live = live_tailnet_workers(config, nodes, routes)
        self.assertEqual(len(live), 3)  # head excluded
        got = {(ip, alloc.advertise_ip, alloc.node_manager_port)
               for ip, _, alloc in live}
        want = {(k[0], a.advertise_ip, a.node_manager_port)
                for k, a in allocs.items()}
        self.assertEqual(got, want)
        for ip, _, alloc in live:
            orig = next(a for a in allocs.values()
                        if a.advertise_ip == alloc.advertise_ip)
            self.assertEqual(alloc, orig)

    def test_dead_nodes_skipped(self):
        raw = _make_raw()
        key = next(iter(allocate_tailnet_workers(
            build_config(raw), assign_nodes(build_config(raw)))))
        _, nodes, routes = _running(raw, dead={key})
        live = live_tailnet_workers(build_config(raw), nodes, routes)
        self.assertEqual(len(live), 2)

    def test_managed_machine_mapped_back_to_cluster_address(self):
        # A managed cloud machine is SSH'd at its public IP but routed at
        # its tailnet IP: the head route must map back to the public IP.
        raw = _make_raw()
        raw["tailnet"]["auth_key"] = "tskey-auth-test"
        raw["available_node_types"]["tw"]["compatible_ips"] = ["203.0.113.1"]
        raw["auth"]["overrides"] = {"203.0.113.1": {
            "tailnet": True, "manage_tailscale": True}}
        config = build_config(raw)
        ts_map = {"203.0.113.1": "100.64.0.11"}
        allocs = allocate_tailnet_workers(config, assign_nodes(config), ts_map)
        routes = build_relay_spec(config, allocs, None)["routes"]
        nodes = [{"NodeName": a.advertise_ip, "Alive": True, "Resources": {},
                  "NodeManagerPort": a.node_manager_port}
                 for a in allocs.values()]
        live = live_tailnet_workers(config, nodes, routes, ts_map)
        self.assertEqual({ip for ip, _, _ in live}, {"203.0.113.1"})
        self.assertEqual({a.tailnet_ip for _, _, a in live}, {"100.64.0.11"})

    def test_head_colocated_worker_maps_to_head(self):
        raw = _make_raw()
        raw["available_node_types"]["tw"]["compatible_ips"] = [HEAD, M2]
        raw["available_node_types"]["tw"]["num_workers"] = 2
        _, nodes, routes = _running(raw)
        config = build_config(raw)
        live = live_tailnet_workers(config, nodes, routes)
        self.assertEqual(sorted(ip for ip, _, _ in live), sorted([HEAD, M2]))
        head_alloc = next(a for ip, _, a in live if ip == HEAD)
        self.assertEqual(head_alloc.tailnet_ip,
                         config.tailnet_config.head_tailnet_ip)

    def test_unrouted_node_fails_loudly(self):
        raw = _make_raw()
        _, nodes, routes = _running(raw)
        del routes[nodes[1]["NodeName"]]
        with self.assertRaises(RuntimeError):
            live_tailnet_workers(build_config(raw), nodes, routes)

    def test_changed_port_layout_fails_loudly(self):
        raw = _make_raw()
        _, nodes, routes = _running(raw)
        raw["tailnet"]["worker_block_base"] = 30000
        with self.assertRaises(RuntimeError):
            live_tailnet_workers(build_config(raw), nodes, routes)


class TestAddedAllocation(unittest.TestCase):

    def test_new_workers_avoid_live_ips_and_blocks(self):
        raw = _make_raw()
        allocs, nodes, routes = _running(raw)
        grown = build_config(_grown(raw))
        reconstructed = live_tailnet_workers(grown, nodes, routes)
        live = [(ip, a) for ip, _, a in reconstructed]
        new_assignments = node_setup.compute_new_assignments(
            assign_nodes(grown),
            [{"NodeName": ip, "Alive": True, "Resources": r}
             for ip, r, _ in reconstructed])
        self.assertEqual(sorted(a.node_type.name for a in new_assignments),
                         ["aa", "tw"])
        new = allocate_added_tailnet_workers(grown, new_assignments, live)
        live_ips = {a.advertise_ip for _, a in live}
        for key, alloc in new.items():
            self.assertNotIn(alloc.advertise_ip, live_ips)
            machine_ports = set()
            for ip, a in live:
                if ip == key[0]:
                    machine_ports |= set(a.ports())
            self.assertFalse(set(alloc.ports()) & machine_ports)
        # The YAML-derived allocation would have renumbered live workers.
        shifted = allocate_tailnet_workers(grown, assign_nodes(grown))
        self.assertNotEqual(
            {a.advertise_ip for k, a in shifted.items() if k[1] == "tw"},
            {a.advertise_ip for a in allocs.values()})

    def test_lowest_free_ip_and_block_reused(self):
        raw = _make_raw()
        allocs, nodes, routes = _running(raw)
        # Kill the worker holding 127.0.0.2; its IP and block free up.
        dead_key = next(k for k, a in allocs.items()
                        if a.advertise_ip == "127.0.0.2")
        _, nodes, routes = _running(raw, dead={dead_key})
        config = build_config(raw)
        live = live_tailnet_workers(config, nodes, routes)
        new_assignments = node_setup.compute_new_assignments(
            assign_nodes(config),
            [{"NodeName": ip, "Alive": True, "Resources": r}
             for ip, r, _ in live])
        self.assertEqual(len(new_assignments), 1)
        new = allocate_added_tailnet_workers(
            config, new_assignments, [(ip, a) for ip, _, a in live])
        (alloc,) = new.values()
        self.assertEqual(alloc.advertise_ip, "127.0.0.2")
        self.assertEqual(alloc.node_manager_port,
                         allocs[dead_key].node_manager_port)

    def test_skips_head_advertise_ip(self):
        raw = _make_raw()
        raw["tailnet"]["head_advertise_ip"] = "127.0.0.2"
        config = build_config(raw)
        a = assign_nodes(config)[0]
        new = allocate_added_tailnet_workers(config, [a], [])
        self.assertEqual(next(iter(new.values())).advertise_ip, "127.0.0.3")


class TestPlanTailnetAdd(unittest.TestCase):

    def _plan(self, raw, nodes, routes, ts_ips=None):
        config = build_config(raw)
        with mock.patch.object(node_setup, "_make_ssh",
                               side_effect=lambda cfg, ip: mock.MagicMock(ip=ip)), \
             mock.patch.object(node_setup, "read_relay_spec",
                               return_value={"routes": routes}), \
             mock.patch.object(node_setup, "query_tailnet_ip",
                               side_effect=lambda ssh, tn:
                                   (ts_ips or {}).get(ssh.ip)):
            new_assignments, plan = plan_tailnet_add(
                config, assign_nodes(config), nodes)
        return config, new_assignments, plan

    def test_nothing_to_add(self):
        raw = _make_raw()
        _, nodes, routes = _running(raw)
        _, new_assignments, plan = self._plan(raw, nodes, routes)
        self.assertEqual(new_assignments, [])
        self.assertEqual(plan.new, {})
        self.assertEqual(len(plan.live), 3)

    def test_grown_yaml_adds_only_missing_workers(self):
        raw = _make_raw()
        _, nodes, routes = _running(raw)
        config, new_assignments, plan = self._plan(_grown(raw), nodes, routes)
        self.assertEqual(sorted(a.node_type.name for a in new_assignments),
                         ["aa", "tw"])
        self.assertEqual(set(plan.new),
                         {(a.ip, a.node_type.name, a.worker_index)
                          for a in new_assignments})

    def test_head_without_relay_spec_is_head_only(self):
        raw = _make_raw()
        config = build_config(raw)
        head = [{"NodeName": config.tailnet_config.head_advertise_ip,
                 "Alive": True, "Resources": {}}]
        _, new_assignments, plan = self._plan(raw, head, {})
        self.assertEqual(len(new_assignments), 3)
        self.assertEqual(plan.live, [])

    def test_manage_all_discovers_head_tailnet_ip(self):
        raw = _make_raw()
        raw["tailnet"] = {"manage_all": True, "auth_key": "tskey-auth-test"}
        raw["available_node_types"]["tw"]["compatible_ips"] = ["10.0.0.2"]
        config, _, plan = self._plan(
            raw, [], {}, ts_ips={HEAD: "100.64.0.1", "10.0.0.2": "100.64.0.2"})
        self.assertEqual(config.tailnet_config.head_tailnet_ip, "100.64.0.1")
        self.assertEqual(plan.tailnet_ip_map, {"10.0.0.2": "100.64.0.2"})
        self.assertEqual({a.tailnet_ip for a in plan.new.values()},
                         {"100.64.0.2"})


class TestAddNodesToCluster(unittest.TestCase):

    def _run(self, raw, plan, new_assignments, joined=None):
        config = build_config(raw)
        relays, workers, joins = {}, [], []

        def _ensure(ssh, tn, hostname=None):
            joins.append(ssh.ip)
            return (joined or {})[ssh.ip]

        with mock.patch.object(node_setup, "_make_ssh",
                               side_effect=lambda cfg, ip: mock.MagicMock(ip=ip)), \
             mock.patch.object(node_setup, "update_relay",
                               side_effect=lambda ssh, spec:
                                   relays.__setitem__(ssh.ip, spec)), \
             mock.patch.object(node_setup, "ensure_tailscale",
                               side_effect=_ensure), \
             mock.patch.object(node_setup, "setup_worker_node",
                               side_effect=lambda cfg, a, **kw:
                                   workers.append((a, kw))):
            node_setup.add_nodes_to_cluster(config, new_assignments,
                                            tailnet_plan=plan)
        return config, relays, workers, joins

    def test_requires_plan(self):
        raw = _make_raw()
        config = build_config(raw)
        with self.assertRaises(ValueError):
            node_setup.add_nodes_to_cluster(config, assign_nodes(config))

    def test_reloads_every_relay_and_sets_up_only_new(self):
        raw = _make_raw()
        _, nodes, routes = _running(raw)
        grown = _grown(raw)
        config = build_config(grown)
        live = [(ip, a) for ip, _, a in
                live_tailnet_workers(config, nodes, routes)]
        new_assignments = [a for a in assign_nodes(config)
                           if a.node_type.name == "aa"
                           or (a.node_type.name == "tw" and a.worker_index == 3)]
        # aa lands on M2 and tw-3 on M2/M3: every relay — the head's and
        # each machine's — must learn both, whichever machine hosts them.
        new_alloc = allocate_added_tailnet_workers(config, new_assignments, live)
        plan = TailnetAddPlan(live=live, new=new_alloc)
        _, relays, workers, joins = self._run(grown, plan, new_assignments)

        self.assertEqual(joins, [])  # unmanaged machines: nothing to join
        self.assertEqual(set(relays), {HEAD, M2, M3})
        every_adv = {a.advertise_ip for _, a in live} | \
                    {a.advertise_ip for a in new_alloc.values()}
        for spec in relays.values():
            self.assertTrue(every_adv <= set(spec["routes"]))
        self.assertEqual(len(workers), len(new_assignments))
        for a, kw in workers:
            key = (a.ip, a.node_type.name, a.worker_index)
            self.assertEqual(kw["tailnet_alloc"], new_alloc[key])
            self.assertTrue(kw["skip_ray_stop"])

    def test_new_managed_machine_joined_and_routed_by_tailnet_ip(self):
        raw = _make_raw()
        raw["tailnet"]["auth_key"] = "tskey-auth-test"
        _, nodes, routes = _running(raw)
        grown = copy.deepcopy(raw)
        grown["available_node_types"]["cw"] = {
            "resources": {"cw": 1}, "num_workers": 1,
            "compatible_ips": ["203.0.113.1"]}
        grown["auth"]["overrides"] = {"203.0.113.1": {
            "tailnet": True, "manage_tailscale": True}}
        config = build_config(grown)
        live = [(ip, a) for ip, _, a in
                live_tailnet_workers(config, nodes, routes)]
        new_assignments = [a for a in assign_nodes(config)
                           if a.node_type.name == "cw"]
        plan = TailnetAddPlan(live=live, new=allocate_added_tailnet_workers(
            config, new_assignments, live))
        _, relays, workers, joins = self._run(
            grown, plan, new_assignments, joined={"203.0.113.1": "100.64.0.11"})

        self.assertEqual(joins, ["203.0.113.1"])
        (a, kw), = workers
        self.assertEqual(kw["tailnet_alloc"].tailnet_ip, "100.64.0.11")
        adv = kw["tailnet_alloc"].advertise_ip
        self.assertEqual(relays[HEAD]["routes"][adv], "100.64.0.11")
        self.assertEqual(relays[M2]["routes"][adv], "100.64.0.11")
        self.assertIsNone(relays["203.0.113.1"]["routes"][adv])


def _cloud_raw():
    return {
        "cluster_name": "cloud-add-test",
        "provider": {"head_ip": HEAD},
        "auth": {"ssh_user": "u"},
        "tailnet": {"head_tailnet_ip": "100.64.0.1",
                    "auth_key": "tskey-auth-test"},
        "aws_nodes": {"region": "us-west-2", "ec2_worker": {
            "KeyName": "k", "InstanceType": "t3.large", "count": 2,
            "ssh_user": "ubuntu", "ssh_private_key": "/keys/k.pem"}},
        "available_node_types": {
            "cw": {"resources": {"cw": 4}, "num_workers": 2,
                   "compatible_ips": ["@ec2_worker:0", "@ec2_worker:1"]},
        },
        "head_start_ray_commands": ["ray start --head --port=6379"],
        "worker_start_ray_commands": ["ray start --address=$RAY_HEAD_IP:6379"],
    }


class TestCmdUpAddCloud(unittest.TestCase):
    """``chia up --add`` provisions a missing AWS machine into a running
    tailnet cluster, the same way it does for a tunneled one."""

    def test_provisions_installs_tailscale_and_adds(self):
        from chia.cli import up
        import chia.cluster.aws_nodes as aws_nodes

        raw = _cloud_raw()
        old_ip, new_ip = "203.0.113.1", "203.0.113.2"
        # The live cluster: one cw worker on the existing machine.
        tn_head = "127.200.0.1"
        nodes = [
            {"NodeName": tn_head, "Alive": True, "Resources": {},
             "NodeManagerPort": 23744},
            {"NodeName": "127.0.0.2", "Alive": True,
             "Resources": {"CPU": 2, "cw": 4}, "NodeManagerPort": 24000},
        ]
        routes = {tn_head: None, "127.0.0.2": "100.64.0.11"}
        setup_cmds = {}
        captured = {}

        def _run_setup(node_configs, ip_map, get_auth):
            for name, cfg in node_configs.items():
                if name in ip_map:
                    setup_cmds[name] = list(cfg.effective_setup_commands)

        def _add(config, new_assignments, tailnet_plan=None):
            captured["new"] = new_assignments
            captured["plan"] = tailnet_plan

        args = argparse.Namespace(config_file=None, dry_run=False, yes=True,
                                  verbose=False, add=True)
        with mock.patch.object(aws_nodes, "discover_aws_nodes",
                               return_value={"ec2_worker": [old_ip]}), \
             mock.patch.object(aws_nodes, "provision_missing_aws_nodes",
                               return_value=({"ec2_worker": [old_ip, new_ip]},
                                             {"ec2_worker": [new_ip]})), \
             mock.patch.object(aws_nodes, "run_aws_setup",
                               side_effect=_run_setup), \
             mock.patch.object(up, "query_ray_cluster_nodes",
                               return_value=nodes), \
             mock.patch.object(node_setup, "_make_ssh",
                               side_effect=lambda cfg, ip: mock.MagicMock(ip=ip)), \
             mock.patch.object(node_setup, "read_relay_spec",
                               return_value={"routes": routes}), \
             mock.patch.object(node_setup, "query_tailnet_ip",
                               side_effect=lambda ssh, tn:
                                   {old_ip: "100.64.0.11"}.get(ssh.ip)), \
             mock.patch.object(up, "add_nodes_to_cluster", side_effect=_add), \
             mock.patch("builtins.print"):
            up._cmd_up_add(args, raw, up.parse_aws_nodes(raw), None,
                           mock.MagicMock())

        # Fresh machine got the tailscale install in its setup commands.
        self.assertTrue(any("pkgs.tailscale.com" in c
                            for c in setup_cmds["ec2_worker"]))
        # Only the worker on the new machine is added, with a fresh alloc.
        (a,) = captured["new"]
        self.assertEqual(a.ip, new_ip)
        plan = captured["plan"]
        self.assertEqual([ip for ip, _ in plan.live], [old_ip])
        (alloc,) = plan.new.values()
        self.assertEqual(alloc.advertise_ip, "127.0.0.3")


class TestUpdateRelay(unittest.TestCase):
    """The controller's branches, against a scripted SSH client."""

    SPEC = {"socks_proxy": "127.0.0.1:1055", "routes": {},
            "listeners": [{"bind_ip": "127.0.0.1", "port": 1, "via": "connect"}]}

    def _ssh(self, stdout_for):
        ssh = mock.MagicMock(ip="h")

        def _run_script(script, **kw):
            heredoc = next(c for c in script if "CHIA_RELAY_SPEC_EOF" in c)
            token = json.loads(heredoc.split("\n", 1)[1].rsplit("\n", 1)[0])[
                "reload_token"]
            return subprocess.CompletedProcess([], 0, stdout_for(token), "")
        ssh.run_script.side_effect = _run_script
        ssh.run.return_value = subprocess.CompletedProcess([], 0, "log", "")
        return ssh

    def test_absent_relay_is_started(self):
        ssh = self._ssh(lambda t: "CHIA_RELAY_ABSENT\n")
        with mock.patch.object(tailnet, "start_relay") as start:
            update_relay(ssh, self.SPEC)
        start.assert_called_once_with(ssh, self.SPEC)

    def test_legacy_relay_is_restarted(self):
        ssh = self._ssh(lambda t: "CHIA_RELAY_LEGACY\n")
        with mock.patch.object(tailnet, "start_relay") as start, \
             self.assertLogs("chia.tailnet", level="WARNING"):
            update_relay(ssh, self.SPEC)
        start.assert_called_once()

    def test_reload_acknowledged(self):
        ssh = self._ssh(lambda t: f"CHIA_RELAY_RELOADED {t} 1 errors=0\n")
        with mock.patch.object(tailnet, "start_relay") as start:
            update_relay(ssh, self.SPEC)
        start.assert_not_called()

    def test_reload_bind_errors_raise(self):
        ssh = self._ssh(lambda t: f"CHIA_RELAY_RELOADED {t} 1 errors=2\n")
        with self.assertRaises(RuntimeError):
            update_relay(ssh, self.SPEC)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _Echo:
    """A loopback echo server."""

    def __init__(self):
        self.srv = socket.socket()
        self.srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srv.bind(("127.0.0.1", 0))
        self.srv.listen(16)
        self.port = self.srv.getsockname()[1]
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self):
        while True:
            try:
                conn, _ = self.srv.accept()
            except OSError:
                return
            threading.Thread(target=self._echo, args=(conn,), daemon=True).start()

    @staticmethod
    def _echo(conn):
        with conn:
            while data := conn.recv(4096):
                conn.sendall(data)


@unittest.skipUnless(sys.platform.startswith("linux"),
                     "binds 127.0.0.x aliases (Linux loopback is a /8)")
class TestRelayHotReload(unittest.TestCase):
    """Runs the real relay script and SIGHUPs it."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.script = os.path.join(self.tmp.name, "relay.py")
        self.spec_path = os.path.join(self.tmp.name, "spec.json")
        with open(self.script, "w") as f:
            f.write(RELAY_SCRIPT)
        self.echo_a, self.echo_b = _Echo(), _Echo()
        self.connect_port = _free_port()
        self.lines: queue.Queue = queue.Queue()

    def tearDown(self):
        if getattr(self, "proc", None):
            self.proc.kill()
            self.proc.wait()
            self.proc.stdout.close()
        self.echo_a.srv.close()
        self.echo_b.srv.close()
        self.tmp.cleanup()

    def _write(self, listeners, routes, token="-"):
        with open(self.spec_path, "w") as f:
            json.dump({"socks_proxy": "127.0.0.1:9", "listeners": listeners,
                       "routes": routes, "reload_token": token}, f)

    def _wait_for(self, prefix):
        while True:
            line = self.lines.get(timeout=10)
            if line.startswith(prefix):
                return line

    def _start(self):
        self.proc = subprocess.Popen(
            [sys.executable, self.script, self.spec_path],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        threading.Thread(target=lambda: [self.lines.put(l.strip())
                                         for l in self.proc.stdout],
                         daemon=True).start()
        return self._wait_for("CHIA_RELAY_READY")

    def _connect(self, host, port):
        s = socket.create_connection(("127.0.0.1", self.connect_port), timeout=5)
        s.sendall(f"CONNECT {host}:{port} HTTP/1.1\r\n\r\n".encode())
        status = b""
        while b"\r\n\r\n" not in status:
            status += s.recv(1)
        return s, status.split(b"\r\n", 1)[0].decode()

    @staticmethod
    def _echoes(s, payload=b"ping"):
        s.sendall(payload)
        return s.recv(len(payload)) == payload

    def test_reload_keeps_connections_and_applies_diff(self):
        connect = {"bind_ip": "127.0.0.1", "port": self.connect_port,
                   "via": "connect"}
        # A direct bridge 127.0.0.5:<a> -> 127.0.0.1:<a>.
        bridge_a = {"bind_ip": "127.0.0.5", "port": self.echo_a.port,
                    "dest_ip": "127.0.0.1", "via": "direct"}
        self._write([connect, bridge_a], {"127.0.0.9": None})
        self.assertTrue(self._start().endswith(" reload"))

        held, status = self._connect("127.0.0.9", self.echo_a.port)
        self.assertIn("200", status)
        self.assertTrue(self._echoes(held))

        # Reload: drop bridge_a and route .9, add bridge_b and route .10.
        bridge_b = {"bind_ip": "127.0.0.6", "port": self.echo_b.port,
                    "dest_ip": "127.0.0.1", "via": "direct"}
        self._write([connect, bridge_b], {"127.0.0.10": None}, token="t1")
        self.proc.send_signal(signal.SIGHUP)
        self.assertEqual(self._wait_for("CHIA_RELAY_RELOADED"),
                         "CHIA_RELAY_RELOADED t1 2 errors=0")

        # The connection established before the reload survives it.
        self.assertTrue(self._echoes(held, b"still here"))
        held.close()
        # New route and new listener are live; removed ones are gone.
        s, status = self._connect("127.0.0.10", self.echo_b.port)
        self.assertIn("200", status)
        self.assertTrue(self._echoes(s))
        s.close()
        s, status = self._connect("127.0.0.9", self.echo_a.port)
        self.assertIn("502", status)
        s.close()
        with socket.create_connection(("127.0.0.6", self.echo_b.port),
                                      timeout=5) as s:
            self.assertTrue(self._echoes(s))
        with self.assertRaises(ConnectionRefusedError):
            socket.create_connection(("127.0.0.5", self.echo_a.port), timeout=5)

    def test_bind_failure_reported_not_fatal(self):
        connect = {"bind_ip": "127.0.0.1", "port": self.connect_port,
                   "via": "connect"}
        self._write([connect], {})
        self._start()
        # The echo server already holds 127.0.0.1:<a>: this bind must fail.
        taken = {"bind_ip": "127.0.0.1", "port": self.echo_a.port,
                 "dest_ip": "127.0.0.2", "via": "direct"}
        self._write([connect, taken], {}, token="t2")
        self.proc.send_signal(signal.SIGHUP)
        self.assertEqual(self._wait_for("CHIA_RELAY_RELOADED"),
                         "CHIA_RELAY_RELOADED t2 1 errors=1")
        self.assertIsNone(self.proc.poll())  # still serving


class _LoginShellSSH:
    """Runs scripts in a local ``bash --login`` shaped like an SSH session
    on stock Ubuntu: SHLVL=1 and a ``~/.bash_logout`` that fails (as
    Ubuntu's does — clear_console has no console), under a throwaway
    HOME and USER so relay paths can't touch a real relay."""

    def __init__(self, home):
        from chia.cluster.ssh import SSHClient
        self.ip = "localhost"
        with open(os.path.join(home, ".bash_logout"), "w") as f:
            f.write("false\n")
        env = ["env", "-i", f"HOME={home}", f"USER=chiatest{os.getpid()}",
               f"PATH={os.path.dirname(sys.executable)}:/usr/bin:/bin",
               "SHLVL=0"]
        self._env = env
        # run_script pipes its script to `<base args> bash --login` — the
        # remote half of an SSH session, here run locally.
        self._client = SSHClient("localhost", "u")
        self._client._ssh_base_args = lambda: list(env)

    def run_script(self, commands, **kw):
        return self._client.run_script(commands, **kw)

    def run(self, cmd, check=True, **kw):
        return subprocess.run(self._env + ["bash", "--login", "-c", cmd],
                              capture_output=True, text=True, check=check)


@unittest.skipUnless(sys.platform.startswith("linux"),
                     "binds 127.0.0.x aliases (Linux loopback is a /8)")
class TestRelayControlInLoginShell(unittest.TestCase):
    """start_relay / update_relay / stop_relay scripts, run for real.

    Regression: an early ``exit 0`` in update_relay's script made it fail
    on every fresh Ubuntu machine (``~/.bash_logout`` turns it into 1).
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ssh = _LoginShellSSH(self.tmp.name)
        port = _free_port()
        self.spec1 = {"socks_proxy": "127.0.0.1:9", "routes": {"127.0.0.9": None},
                      "listeners": [{"bind_ip": "127.0.0.1", "port": port,
                                     "via": "connect"}]}
        self.spec2 = {**self.spec1, "routes": {"127.0.0.10": None}}

    def tearDown(self):
        tailnet.stop_relay(self.ssh)
        self.ssh.run("rm -f /tmp/chia_tailnet_relay_$USER.*", check=False)
        self.tmp.cleanup()

    def _pid(self):
        return self.ssh.run("cat /tmp/chia_tailnet_relay_$USER.pid",
                            check=False).stdout.strip()

    def test_absent_then_reload_in_place(self):
        update_relay(self.ssh, self.spec1)          # fresh machine: starts
        started = self._pid()
        self.assertTrue(started)
        update_relay(self.ssh, self.spec2)          # running: hot reload
        self.assertEqual(self._pid(), started)
        spec = tailnet.read_relay_spec(self.ssh)
        self.assertEqual(spec["routes"], {"127.0.0.10": None})


if __name__ == "__main__":
    unittest.main()
