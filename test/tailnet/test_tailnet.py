"""Unit tests for tailnet (tailscale) cluster config, allocation, and scripts.

These tests require NO network access — they cover config parsing,
validation, port-block allocation, relay spec generation, and the
generated head/worker scripts.

Run:
  python -m pytest test/tailnet/test_tailnet.py -v
"""

import copy
import unittest

from chia.cluster.config import (
    ConfigError, assign_nodes, build_config,
    _inject_cloud_tailnet_overrides, parse_aws_nodes, parse_gcp_nodes,
)
from chia.cluster.node_setup import build_head_script, build_worker_script
from chia.cluster.ssh import SSHClient
from chia.cluster.tailnet import (
    allocate_tailnet_workers, build_relay_spec, head_ports,
    tailscale_install_command, ts_hostname,
)

PROXY_CMD = "nc -X 5 -x 127.0.0.1:1055 %h %p"


def _make_raw(**overrides):
    base = {
        "cluster_name": "tailnet-test",
        "tailnet": {"head_tailnet_ip": "100.64.0.1"},
        "provider": {"head_ip": "10.0.0.1"},
        "auth": {"ssh_user": "u"},
        "available_node_types": {
            "tw": {
                "resources": {"tw": 2},
                "num_workers": 3,
                "compatible_ips": ["100.64.0.2", "100.64.0.3"],
            },
        },
        "head_start_ray_commands": [
            "ray stop",
            "ray start --head --port=6379",
        ],
        "worker_start_ray_commands": [
            "ray stop",
            "ray start --address=$RAY_HEAD_IP:6379",
        ],
    }
    base.update(overrides)
    return base


class TestTailnetConfig(unittest.TestCase):

    def test_workers_auto_marked_with_derived_proxy(self):
        """The tailnet: block alone opts every non-head worker in."""
        config = build_config(_make_raw())
        self.assertIsNotNone(config.tailnet_config)
        self.assertEqual(config.tailnet_config.head_tailnet_ip, "100.64.0.1")
        for ip in ("100.64.0.2", "100.64.0.3"):
            self.assertTrue(config.is_tailnet(ip))
            self.assertFalse(config.is_tunneled(ip))
            auth = config.get_ssh_auth(ip)
            self.assertEqual(auth.ssh_proxy_command, PROXY_CMD)
            self.assertEqual(auth.ssh_user, "u")
        # The head is dialed directly — no proxy, not a tailnet worker.
        self.assertFalse(config.is_tailnet("10.0.0.1"))
        self.assertIsNone(config.get_ssh_auth("10.0.0.1").ssh_proxy_command)

    def test_custom_socks_proxy_changes_derived_command(self):
        raw = _make_raw()
        raw["tailnet"]["socks_proxy"] = "127.0.0.1:2233"
        config = build_config(raw)
        self.assertEqual(config.get_ssh_auth("100.64.0.2").ssh_proxy_command,
                         "nc -X 5 -x 127.0.0.1:2233 %h %p")

    def test_explicit_override_wins(self):
        raw = _make_raw()
        raw["auth"]["overrides"] = {
            "100.64.0.2": {"ssh_proxy_command": "corkscrew p 1056 %h %p",
                           "ssh_user": "other"},
        }
        config = build_config(raw)
        auth = config.get_ssh_auth("100.64.0.2")
        self.assertEqual(auth.ssh_proxy_command, "corkscrew p 1056 %h %p")
        self.assertEqual(auth.ssh_user, "other")
        self.assertTrue(config.is_tailnet("100.64.0.2"))  # still auto-marked
        # The other worker keeps the derived default.
        self.assertEqual(config.get_ssh_auth("100.64.0.3").ssh_proxy_command,
                         PROXY_CMD)

    def test_global_proxy_command_fallback(self):
        raw = _make_raw()
        raw["auth"]["ssh_proxy_command"] = "corkscrew proxy 1056 %h %p"
        config = build_config(raw)
        self.assertEqual(config.get_ssh_auth("100.64.0.2").ssh_proxy_command,
                         "corkscrew proxy 1056 %h %p")
        self.assertEqual(config.get_ssh_auth("10.0.0.1").ssh_proxy_command,
                         "corkscrew proxy 1056 %h %p")

    def test_unknown_tailnet_field_fails(self):
        raw = _make_raw()
        raw["tailnet"]["gcs_prot"] = 1234  # typo
        with self.assertRaises(ConfigError):
            build_config(raw)

    def test_missing_head_tailnet_ip_fails(self):
        raw = _make_raw()
        raw["tailnet"] = {}
        with self.assertRaises(ConfigError):
            build_config(raw)

    def test_tailnet_flag_without_section_fails(self):
        raw = _make_raw()
        del raw["tailnet"]
        raw["auth"]["overrides"] = {"100.64.0.2": {"tailnet": True}}
        with self.assertRaises(ConfigError):
            build_config(raw)

    def test_head_colocated_worker_allowed(self):
        raw = _make_raw()
        raw["available_node_types"]["local"] = {
            "resources": {"local": 1},
            "num_workers": 1,
            "compatible_ips": ["10.0.0.1"],  # the head itself
        }
        config = build_config(raw)  # should not raise
        self.assertFalse(config.is_tailnet("10.0.0.1"))

    def test_tunnel_and_tailnet_flag_mutually_exclusive(self):
        raw = _make_raw()
        raw["auth"]["overrides"] = {"100.64.0.2": {"tunnel": True,
                                                   "tailnet": True}}
        with self.assertRaises(ConfigError):
            build_config(raw)

    def test_tunneled_worker_in_tailnet_cluster_fails(self):
        raw = _make_raw()
        raw["auth"]["overrides"] = {"100.64.0.2": {"tunnel": True}}
        with self.assertRaises(ConfigError):
            build_config(raw)


class TestAllocation(unittest.TestCase):

    def _alloc(self, raw=None):
        config = build_config(raw or _make_raw())
        assignments = assign_nodes(config)
        return config, assignments, allocate_tailnet_workers(config, assignments)

    def test_unique_advertise_ips_and_disjoint_ports(self):
        config, assignments, allocs = self._alloc()
        self.assertEqual(len(allocs), 3)
        adv_ips = [a.advertise_ip for a in allocs.values()]
        self.assertEqual(len(set(adv_ips)), 3)
        self.assertNotIn("127.0.0.1", adv_ips)
        self.assertNotIn(config.tailnet_config.head_advertise_ip, adv_ips)
        # All port blocks pairwise disjoint and disjoint from head ports.
        seen = set(head_ports(config.tailnet_config))
        for a in allocs.values():
            block = set(a.ports())
            self.assertFalse(block & seen)
            seen |= block

    def test_worker_port_count_sizing(self):
        config, _, allocs = self._alloc()
        tn = config.tailnet_config
        for a in allocs.values():
            self.assertEqual(a.worker_port_max - a.worker_port_min + 1,
                             tn.worker_port_count)

    def test_oversized_block_fails(self):
        raw = _make_raw()
        raw["tailnet"]["worker_port_count"] = 1000
        raw["tailnet"]["worker_block_size"] = 256
        config = build_config(raw)
        assignments = assign_nodes(config)
        with self.assertRaises(ConfigError):
            allocate_tailnet_workers(config, assignments)

    def _alloc_n_workers(self, n):
        """Allocate n workers on n distinct tailnet IPs with defaults."""
        ips = [f"100.64.{1 + i // 250}.{2 + i % 250}" for i in range(n)]
        raw = _make_raw()
        raw["available_node_types"] = {
            "tw": {"resources": {"tw": 1}, "num_workers": n,
                   "compatible_ips": ips},
        }
        config = build_config(raw)
        return allocate_tailnet_workers(config, assign_nodes(config))

    def test_default_capacity_is_162_workers(self):
        """The head owns the block immediately below worker_block_base,
        so worker blocks growing upward meet no head port at all — they
        can grow until the top of port space (65535). 162 blocks fit;
        the 163rd would need ports past 65535 and is refused."""
        allocs = self._alloc_n_workers(162)
        self.assertEqual(len(allocs), 162)
        head_ports_set = set(head_ports(build_config(_make_raw()).tailnet_config))
        self.assertTrue(all(p < 24000 for p in head_ports_set))
        top = max(p for a in allocs.values() for p in a.ports())
        self.assertLessEqual(top, 65535)
        with self.assertRaises(ConfigError):
            self._alloc_n_workers(163)


class TestRelaySpec(unittest.TestCase):

    def test_worker_spec_covers_head_and_remote_peers_only(self):
        config = build_config(_make_raw())
        assignments = assign_nodes(config)
        allocs = allocate_tailnet_workers(config, assignments)
        tn = config.tailnet_config

        spec = build_relay_spec(config, allocs, "100.64.0.2")
        self.assertEqual(spec["socks_proxy"], tn.socks_proxy)
        socks = [e for e in spec["listeners"] if e.get("via", "socks") == "socks"]
        direct = [e for e in spec["listeners"] if e.get("via") == "direct"]
        bind_ips = {e["bind_ip"] for e in socks}
        # Head listeners present...
        self.assertIn(tn.head_advertise_ip, bind_ips)
        # ...remote peers present, same-host peers excluded.
        for key, a in allocs.items():
            if a.tailnet_ip == "100.64.0.2":
                self.assertNotIn(a.advertise_ip, bind_ips)
            else:
                self.assertIn(a.advertise_ip, bind_ips)
        # SOCKS destinations are tailnet IPs.
        for e in socks:
            self.assertTrue(e["dest_ip"].startswith("100.64."))
        # Self-inbound tool bridges: 127.0.0.1:<tool port> -> own
        # advertise IP, direct dial (tools bind the worker's advertise IP while
        # tailscaled delivers inbound to 127.0.0.1).
        own = [a for k, a in allocs.items() if k[0] == "100.64.0.2"]
        self.assertTrue(own)
        for a in own:
            for port in range(a.tool_port_min, a.tool_port_max + 1):
                self.assertIn({"bind_ip": "127.0.0.1", "port": port,
                               "dest_ip": a.advertise_ip, "via": "direct"},
                              direct)

    def test_head_spec_has_no_head_listeners(self):
        config = build_config(_make_raw())
        tn = config.tailnet_config
        assignments = assign_nodes(config)
        allocs = allocate_tailnet_workers(config, assignments)
        spec = build_relay_spec(config, allocs, None)
        socks = [e for e in spec["listeners"] if e.get("via", "socks") == "socks"]
        bind_ips = {e["bind_ip"] for e in socks}
        self.assertNotIn(tn.head_advertise_ip, bind_ips)
        for a in allocs.values():
            self.assertIn(a.advertise_ip, bind_ips)
        # Head-local tool bridge covers the head tool range.
        direct = [e for e in spec["listeners"] if e.get("via") == "direct"]
        for port in range(tn.head_tool_port_min, tn.head_tool_port_max + 1):
            self.assertIn({"bind_ip": "127.0.0.1", "port": port,
                           "dest_ip": tn.head_advertise_ip, "via": "direct"},
                          direct)


class TestScripts(unittest.TestCase):

    def test_head_script_injects_advertise_ip_and_pins(self):
        config = build_config(_make_raw())
        tn = config.tailnet_config
        script = build_head_script(config)
        start = [l for l in script if "ray start" in l and "--head" in l][0]
        self.assertIn(f"--node-ip-address={tn.head_advertise_ip}", start)
        self.assertIn(f"--min-worker-port={tn.head_worker_port_min}", start)
        self.assertIn(f"export CHIA_TOOL_ADVERTISE_HOST={tn.head_advertise_ip}",
                      script)

    def test_worker_script_uses_alloc(self):
        config = build_config(_make_raw())
        assignments = assign_nodes(config)
        allocs = allocate_tailnet_workers(config, assignments)
        a = assignments[0]
        alloc = allocs[(a.ip, a.node_type.name, a.worker_index)]
        tn = config.tailnet_config
        script = build_worker_script(config, a, tailnet_alloc=alloc)
        self.assertIn(f"export RAY_HEAD_IP={tn.head_advertise_ip}", script)
        start = [l for l in script if "ray start" in l][0]
        self.assertIn(f"--address={tn.head_advertise_ip}:{tn.gcs_port}", start)
        self.assertIn(f"--node-ip-address={alloc.advertise_ip}", start)
        self.assertIn(f"--min-worker-port={alloc.worker_port_min}", start)
        self.assertNotIn("CHIA_TOOL_RELAY_HOST", "\n".join(script))


class TestManageAll(unittest.TestCase):

    def _reachable_raw(self):
        """manage_all cluster whose workers have ordinary reachable IPs."""
        raw = _make_raw()
        raw["tailnet"] = {"manage_all": True, "auth_key": "tskey-auth-test"}
        raw["available_node_types"]["tw"]["compatible_ips"] = \
            ["10.0.0.5", "10.0.0.6"]
        return raw

    def test_manage_all_with_reachable_workers(self):
        config = build_config(self._reachable_raw())
        tn = config.tailnet_config
        self.assertTrue(tn.manage_all)
        self.assertEqual(tn.head_tailnet_ip, "")  # discovered at bring-up
        for ip in ("10.0.0.5", "10.0.0.6"):
            auth = config.get_ssh_auth(ip)
            self.assertTrue(auth.manage_tailscale)
            self.assertTrue(config.is_tailnet(ip))
            self.assertIsNone(auth.ssh_proxy_command)  # dialed directly

    def test_manage_all_rejects_tailnet_addressed_workers(self):
        raw = _make_raw()  # workers at 100.64.0.2/3
        raw["tailnet"]["manage_all"] = True
        with self.assertRaises(ConfigError):
            build_config(raw)

    def test_optout_under_manage_all(self):
        raw = self._reachable_raw()
        raw["available_node_types"]["tw"]["compatible_ips"] = \
            ["10.0.0.5", "100.64.0.9"]
        raw["auth"]["overrides"] = {"100.64.0.9": {"manage_tailscale": False}}
        config = build_config(raw)  # opted-out machine is fine
        self.assertFalse(config.get_ssh_auth("100.64.0.9").manage_tailscale)
        self.assertIsNotNone(config.get_ssh_auth("100.64.0.9").ssh_proxy_command)
        self.assertTrue(config.get_ssh_auth("10.0.0.5").manage_tailscale)

    def test_explicit_manage_on_tailnet_address_rejected(self):
        raw = _make_raw()  # no manage_all
        raw["auth"]["overrides"] = {"100.64.0.2": {"manage_tailscale": True}}
        with self.assertRaises(ConfigError):
            build_config(raw)

    def test_head_tailnet_ip_still_required_without_manage_all(self):
        raw = _make_raw()
        raw["tailnet"] = {}
        with self.assertRaises(ConfigError):
            build_config(raw)

    def test_default_dir_is_per_cluster_and_sanitized(self):
        raw = self._reachable_raw()
        raw["cluster_name"] = "My Cluster/v2"
        config = build_config(raw)
        self.assertEqual(config.tailnet_config.tailscale_dir,
                         "/tmp/My-Cluster-v2/tailscale")

    def test_explicit_dir_wins(self):
        raw = self._reachable_raw()
        raw["tailnet"]["tailscale_dir"] = "/tmp/custom-ts"
        config = build_config(raw)
        self.assertEqual(config.tailnet_config.tailscale_dir, "/tmp/custom-ts")

    def test_overlong_dir_rejected(self):
        raw = self._reachable_raw()
        raw["tailnet"]["tailscale_dir"] = "/tmp/" + "x" * 120
        with self.assertRaises(ConfigError):
            build_config(raw)


def _make_cloud_raw(join_tailnet=None, with_tailnet_section=True):
    """A raw config with one AWS machine type (post-placeholder-expansion)."""
    node = {"KeyName": "k", "InstanceType": "t3.large", "count": 2,
            "ssh_user": "ubuntu", "ssh_private_key": "/keys/k.pem"}
    if join_tailnet is not None:
        node["join_tailnet"] = join_tailnet
    raw = {
        "cluster_name": "cloud-test",
        "provider": {"head_ip": "10.0.0.1"},
        "auth": {"ssh_user": "u"},
        "aws_nodes": {"region": "us-west-2", "ec2_worker": node},
        "available_node_types": {
            "cw": {"resources": {"cw": 4}, "num_workers": 2,
                   "compatible_ips": ["203.0.113.1", "203.0.113.2"]},
        },
        "head_start_ray_commands": ["ray start --head --port=6379"],
        "worker_start_ray_commands": ["ray start --address=$RAY_HEAD_IP:6379"],
    }
    if with_tailnet_section:
        raw["tailnet"] = {"head_tailnet_ip": "100.64.0.1",
                          "auth_key": "tskey-auth-test"}
    return raw


class TestCloudTailnet(unittest.TestCase):

    IP_MAP = {"ec2_worker": ["203.0.113.1", "203.0.113.2"]}

    def _apply(self, raw):
        from chia.cluster.config import apply_cloud_network_mode
        aws_result = parse_aws_nodes(raw)
        return apply_cloud_network_mode(
            raw, aws_result, self.IP_MAP, None, {}), raw

    def test_aws_defaults_to_tailnet_when_section_present(self):
        joining, raw = self._apply(_make_cloud_raw())
        self.assertIn("ec2_worker", joining)
        config = build_config(raw)
        for ip in self.IP_MAP["ec2_worker"]:
            self.assertTrue(config.is_tailnet(ip))
            self.assertFalse(config.is_tunneled(ip))
            auth = config.get_ssh_auth(ip)
            self.assertTrue(auth.manage_tailscale)
            self.assertEqual(auth.ssh_user, "ubuntu")
            self.assertEqual(auth.ssh_private_key, "/keys/k.pem")
            # Public IP → orchestration SSH is direct, no SOCKS proxy.
            self.assertIsNone(auth.ssh_proxy_command)

    def test_aws_defaults_to_tunnels_without_section(self):
        joining, raw = self._apply(_make_cloud_raw(with_tailnet_section=False))
        self.assertEqual(joining, {})
        config = build_config(raw)
        for ip in self.IP_MAP["ec2_worker"]:
            self.assertTrue(config.is_tunneled(ip))

    def test_explicit_opt_out_keeps_tunnels_but_mixing_fails(self):
        # join_tailnet: false with a tailnet section → tunnels injected,
        # and build_config rejects the tunnel/tailnet mix loudly.
        joining, raw = self._apply(_make_cloud_raw(join_tailnet=False))
        self.assertEqual(joining, {})
        with self.assertRaises(ConfigError):
            build_config(raw)

    def test_explicit_join_without_section_fails(self):
        with self.assertRaises(ConfigError):
            self._apply(_make_cloud_raw(join_tailnet=True,
                                        with_tailnet_section=False))

    def test_missing_auth_key_fails(self):
        raw = _make_cloud_raw()
        del raw["tailnet"]["auth_key"]
        with self.assertRaises(ConfigError):
            self._apply(raw)

    def test_gcp_join_tailnet_parses(self):
        raw = {"gcp_nodes": {"project": "p", "gw": {
            "machine_type": "n1-standard-1", "count": 1,
            "join_tailnet": True}}}
        node_configs, *_ = parse_gcp_nodes(raw)
        self.assertTrue(node_configs["gw"].join_tailnet)

    def test_alloc_uses_tailnet_ip_map(self):
        joining, raw = self._apply(_make_cloud_raw())
        config = build_config(raw)
        assignments = assign_nodes(config)
        ip_map = {"203.0.113.1": "100.64.0.11", "203.0.113.2": "100.64.0.12"}
        allocs = allocate_tailnet_workers(config, assignments, ip_map)
        by_cluster_ip = {k[0]: a for k, a in allocs.items()}
        self.assertEqual(by_cluster_ip["203.0.113.1"].tailnet_ip, "100.64.0.11")
        self.assertEqual(by_cluster_ip["203.0.113.2"].tailnet_ip, "100.64.0.12")

    def test_relay_spec_excludes_own_workers_by_cluster_address(self):
        joining, raw = self._apply(_make_cloud_raw())
        config = build_config(raw)
        assignments = assign_nodes(config)
        ip_map = {"203.0.113.1": "100.64.0.11", "203.0.113.2": "100.64.0.12"}
        allocs = allocate_tailnet_workers(config, assignments, ip_map)
        spec = build_relay_spec(config, allocs, "203.0.113.1")
        own = next(a for k, a in allocs.items() if k[0] == "203.0.113.1")
        other = next(a for k, a in allocs.items() if k[0] == "203.0.113.2")
        bind_ips = {e["bind_ip"] for e in spec["listeners"]}
        self.assertNotIn(own.advertise_ip, bind_ips)
        self.assertIn(other.advertise_ip, bind_ips)
        # Dial destinations use the discovered tailnet IPs.
        dests = {e["dest_ip"] for e in spec["listeners"]
                 if e["bind_ip"] == other.advertise_ip}
        self.assertEqual(dests, {"100.64.0.12"})

    def test_install_command_and_hostname(self):
        config = build_config(_make_cloud_raw())
        # build_config leaves aws_nodes in raw; use the tailnet config
        tn = config.tailnet_config
        cmd = tailscale_install_command(tn)
        self.assertIn(f"tailscale_{tn.tailscale_version}_", cmd)
        self.assertIn("pkgs.tailscale.com", cmd)
        self.assertIn(tn.tailscale_dir, cmd)
        self.assertEqual(ts_hostname("My Cluster", "203.0.113.1"),
                         "chia-my-cluster-203-0-113-1")

    def test_injector_respects_existing_overrides(self):
        raw = _make_cloud_raw()
        raw["auth"]["overrides"] = {"203.0.113.1": {"ssh_user": "custom"}}
        _inject_cloud_tailnet_overrides(
            raw, self.IP_MAP, parse_aws_nodes(raw)[0])
        entry = raw["auth"]["overrides"]["203.0.113.1"]
        self.assertEqual(entry["ssh_user"], "custom")  # explicit wins
        self.assertTrue(entry["tailnet"])
        self.assertTrue(entry["manage_tailscale"])


class TestSSHProxyCommand(unittest.TestCase):

    def test_ssh_args_include_proxy(self):
        c = SSHClient("100.64.0.2", "u", proxy_command=PROXY_CMD)
        args = c._ssh_base_args()
        self.assertIn(f"ProxyCommand={PROXY_CMD}", args)

    def test_rsync_args_quote_proxy(self):
        c = SSHClient("100.64.0.2", "u", proxy_command=PROXY_CMD)
        args = c._rsync_base_args()
        ssh_cmd = args[args.index("-e") + 1]
        self.assertIn(f'"ProxyCommand={PROXY_CMD}"', ssh_cmd)

    def test_no_proxy_no_option(self):
        c = SSHClient("10.0.0.1", "u")
        self.assertFalse(any("ProxyCommand" in a for a in c._ssh_base_args()))


if __name__ == "__main__":
    unittest.main()
