"""Unit tests for tailnet (tailscale) cluster config, allocation, and scripts.

These tests require NO network access — they cover config parsing,
validation, port-block allocation, relay spec generation, and the
generated head/worker scripts.

Run:
  python -m pytest test/tailnet/test_tailnet.py -v
"""

import copy
import unittest

from chia.cluster.config import ConfigError, assign_nodes, build_config
from chia.cluster.node_setup import build_head_script, build_worker_script
from chia.cluster.ssh import SSHClient
from chia.cluster.tailnet import (
    allocate_tailnet_workers, build_relay_spec, head_ports,
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
        # The head is dialed directly — no proxy, not a tailnet node.
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


class TestRelaySpec(unittest.TestCase):

    def test_worker_spec_covers_head_and_remote_peers_only(self):
        config = build_config(_make_raw())
        assignments = assign_nodes(config)
        allocs = allocate_tailnet_workers(config, assignments)
        tn = config.tailnet_config

        spec = build_relay_spec(config, allocs, "100.64.0.2")
        self.assertEqual(spec["socks_proxy"], tn.socks_proxy)
        bind_ips = {e["bind_ip"] for e in spec["listeners"]}
        # Head listeners present...
        self.assertIn(tn.head_advertise_ip, bind_ips)
        # ...remote peers present, same-host peers excluded.
        for key, a in allocs.items():
            if a.tailnet_ip == "100.64.0.2":
                self.assertNotIn(a.advertise_ip, bind_ips)
            else:
                self.assertIn(a.advertise_ip, bind_ips)
        # Destinations: every listener forwards to the same port on a
        # tailnet IP.
        for e in spec["listeners"]:
            self.assertTrue(e["dest_ip"].startswith("100.64."))

    def test_head_spec_has_no_head_listeners(self):
        config = build_config(_make_raw())
        assignments = assign_nodes(config)
        allocs = allocate_tailnet_workers(config, assignments)
        spec = build_relay_spec(config, allocs, None)
        bind_ips = {e["bind_ip"] for e in spec["listeners"]}
        self.assertNotIn(config.tailnet_config.head_advertise_ip, bind_ips)
        for a in allocs.values():
            self.assertIn(a.advertise_ip, bind_ips)


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
