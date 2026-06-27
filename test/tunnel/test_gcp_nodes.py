"""Offline tests for GCP node config parsing, placeholder/tunnel handling, and
the compute_v1 instance/firewall mapping (mocked clients — no network/creds).

The GCP analog of ``test_aws_nodes.py``.  Two layers:

  * Pure-config tests (``parse_gcp_nodes`` + the shared, cloud-agnostic
    ``_expand_node_placeholders`` / ``_inject_cloud_tunnel_overrides``) — these
    need neither google-cloud-compute nor any credentials.

  * Mapping tests (``_build_instance`` / ``provision_gcp_nodes`` /
    ``discover_gcp_nodes`` / ``ensure_ssh_firewall``) — these build/inspect real
    ``compute_v1`` protos but mock the network clients, so they run with the
    library installed and never touch GCP.

Run:
  python -m pytest test/tunnel/test_gcp_nodes.py -v
"""

import types
import unittest
from unittest import mock

import pytest

from chia.cluster.config import (
    ConfigError, parse_gcp_nodes,
    _expand_node_placeholders, _inject_cloud_tunnel_overrides,
)


# ---------------------------------------------------------------------------
# Pure config parsing (no compute_v1, no network)
# ---------------------------------------------------------------------------

class TestParseGCPNodes(unittest.TestCase):

    def _raw(self, **node_over):
        node = {"machine_type": "n1-standard-4", "count": 2}
        node.update(node_over)
        return {"gcp_nodes": {
            "project": "my-project",
            "zone": "us-central1-a",
            "gcp_worker": node,
        }}

    def test_basic_parse_and_pop(self):
        raw = self._raw(ssh_user="ubuntu", spot=True)
        result = parse_gcp_nodes(raw)
        self.assertIsNotNone(result)
        nodes, project, zone, network, subnet = result
        self.assertEqual(project, "my-project")
        self.assertEqual(zone, "us-central1-a")
        self.assertIsNone(network)
        self.assertIsNone(subnet)
        cfg = nodes["gcp_worker"]
        self.assertEqual(cfg.machine_type, "n1-standard-4")
        self.assertEqual(cfg.count, 2)
        self.assertEqual(cfg.ssh_user, "ubuntu")
        self.assertTrue(cfg.spot)
        # Section is popped so build_config never sees it.
        self.assertNotIn("gcp_nodes", raw)

    def test_absent_section_returns_none(self):
        self.assertIsNone(parse_gcp_nodes({"cluster_name": "x"}))

    def test_missing_project_raises(self):
        raw = self._raw()
        raw["gcp_nodes"].pop("project")
        with self.assertRaises(ConfigError):
            parse_gcp_nodes(raw)

    def test_missing_machine_type_raises(self):
        raw = self._raw()
        raw["gcp_nodes"]["gcp_worker"].pop("machine_type")
        with self.assertRaises(ConfigError):
            parse_gcp_nodes(raw)

    def test_missing_count_raises(self):
        raw = self._raw()
        raw["gcp_nodes"]["gcp_worker"].pop("count")
        with self.assertRaises(ConfigError):
            parse_gcp_nodes(raw)

    def test_invalid_node_name_raises(self):
        raw = {"gcp_nodes": {"project": "p",
                             "Bad_Name": {"machine_type": "n1-standard-4", "count": 1}}}
        with self.assertRaises(ConfigError):
            parse_gcp_nodes(raw)

    def test_extra_args_captured(self):
        raw = self._raw(can_ip_forward=True,
                        guest_accelerators=[{"accelerator_count": 1}])
        nodes, *_ = parse_gcp_nodes(raw)
        cfg = nodes["gcp_worker"]
        self.assertEqual(cfg.extra_args["can_ip_forward"], True)
        self.assertIn("guest_accelerators", cfg.extra_args)
        # Known keys must NOT leak into extra_args.
        self.assertNotIn("machine_type", cfg.extra_args)
        self.assertNotIn("count", cfg.extra_args)

    def test_defaults(self):
        nodes, *_ = parse_gcp_nodes(self._raw())
        cfg = nodes["gcp_worker"]
        self.assertFalse(cfg.spot)
        self.assertFalse(cfg.use_os_login)
        self.assertEqual(cfg.setup_timeout, 1800)
        self.assertEqual(cfg.ssh_timeout, 120)
        self.assertTrue(cfg.image)  # DEFAULT_IMAGE

    def test_network_subnetwork_parsed(self):
        raw = self._raw()
        raw["gcp_nodes"]["network"] = "my-net"
        raw["gcp_nodes"]["subnetwork"] = "my-subnet"
        _nodes, _proj, _zone, network, subnet = parse_gcp_nodes(raw)
        self.assertEqual(network, "my-net")
        self.assertEqual(subnet, "my-subnet")


# ---------------------------------------------------------------------------
# Shared placeholder + tunnel injection (cloud-agnostic) on GCP IPs
# ---------------------------------------------------------------------------

class TestPlaceholdersAndTunnels(unittest.TestCase):

    def test_expand_gcp_placeholders(self):
        ip_map = {"gcp_worker": ["1.1.1.1", "2.2.2.2"], "gcp_verilator": ["3.3.3.3"]}
        data = {"worker_ips": ["@gcp_worker:0", "@gcp_worker:1", "@gcp_verilator:0"]}
        out = _expand_node_placeholders(data, ip_map)
        self.assertEqual(out["worker_ips"], ["1.1.1.1", "2.2.2.2", "3.3.3.3"])

    def test_expand_merged_aws_and_gcp(self):
        ip_map = {"ec2_worker": ["10.0.0.1"], "gcp_worker": ["20.0.0.1"]}
        data = ["@ec2_worker:0", "@gcp_worker:0"]
        self.assertEqual(_expand_node_placeholders(data, ip_map),
                         ["10.0.0.1", "20.0.0.1"])

    def test_unknown_reference_raises(self):
        with self.assertRaises(ConfigError):
            _expand_node_placeholders("@nope:0", {"gcp_worker": ["1.1.1.1"]})

    def test_index_out_of_range_raises(self):
        with self.assertRaises(ConfigError):
            _expand_node_placeholders("@gcp_worker:5", {"gcp_worker": ["1.1.1.1"]})

    def test_inject_tunnel_and_ssh_user(self):
        from chia.cluster.gcp_nodes import GCPNodeConfig
        nodes = {"gcp_worker": GCPNodeConfig(
            machine_type="n1-standard-4", count=1, ssh_user="ubuntu")}
        raw = {"auth": {"ssh_user": "jt"}}
        _inject_cloud_tunnel_overrides(raw, {"gcp_worker": ["1.1.1.1"]}, nodes)
        ov = raw["auth"]["overrides"]["1.1.1.1"]
        self.assertEqual(ov["tunnel"], True)        # default TunnelConfig sentinel
        self.assertEqual(ov["ssh_user"], "ubuntu")  # per-node override injected

    def test_inject_respects_tunnel_defaults(self):
        from chia.cluster.gcp_nodes import GCPNodeConfig
        nodes = {"gcp_worker": GCPNodeConfig(machine_type="n1-standard-4", count=1)}
        raw = {"tunnel_defaults": {"ray_worker_port_min": 31000,
                                   "ray_worker_port_max": 31010}}
        _inject_cloud_tunnel_overrides(raw, {"gcp_worker": ["1.1.1.1"]}, nodes)
        tun = raw["auth"]["overrides"]["1.1.1.1"]["tunnel"]
        self.assertEqual(tun["ray_worker_port_min"], 31000)


# ---------------------------------------------------------------------------
# SSH metadata (pure; no compute_v1 needed)
# ---------------------------------------------------------------------------

class TestGCPSSHMetadata(unittest.TestCase):

    def _cfg(self, tmp_path, **kw):
        from chia.cluster.gcp_nodes import GCPNodeConfig
        return GCPNodeConfig(machine_type="n1-standard-4", count=1, **kw)

    def test_metadata_keys_path(self):
        from chia.cluster.gcp_nodes import _gcp_ssh_metadata, GCPNodeConfig
        import tempfile, os
        f = tempfile.NamedTemporaryFile("w", suffix=".pub", delete=False)
        f.write("ssh-ed25519 AAAAKEY user@host\n")
        f.close()
        cfg = GCPNodeConfig(machine_type="n1-standard-4", count=1,
                            ssh_public_key=f.name)
        items = dict(_gcp_ssh_metadata(cfg, "ubuntu", None))
        os.unlink(f.name)
        self.assertEqual(items["ssh-keys"], "ubuntu:ssh-ed25519 AAAAKEY user@host")
        self.assertEqual(items["enable-oslogin"], "FALSE")

    def test_derive_pubkey_from_private_key(self):
        from chia.cluster.gcp_nodes import _gcp_ssh_metadata, GCPNodeConfig
        import tempfile, os
        priv = tempfile.NamedTemporaryFile("w", suffix="", delete=False)
        priv.close()
        with open(priv.name + ".pub", "w") as p:
            p.write("ssh-rsa DERIVED user@host\n")
        cfg = GCPNodeConfig(machine_type="n1-standard-4", count=1,
                            ssh_private_key=priv.name)
        items = dict(_gcp_ssh_metadata(cfg, "ubuntu", None))
        os.unlink(priv.name)
        os.unlink(priv.name + ".pub")
        self.assertIn("ssh-rsa DERIVED", items["ssh-keys"])

    def test_os_login_path(self):
        from chia.cluster.gcp_nodes import _gcp_ssh_metadata, GCPNodeConfig
        cfg = GCPNodeConfig(machine_type="n1-standard-4", count=1, use_os_login=True)
        items = dict(_gcp_ssh_metadata(cfg, None, None))
        self.assertEqual(items["enable-oslogin"], "TRUE")
        self.assertNotIn("ssh-keys", items)

    def test_missing_user_raises(self):
        from chia.cluster.gcp_nodes import _gcp_ssh_metadata, GCPNodeConfig
        cfg = GCPNodeConfig(machine_type="n1-standard-4", count=1,
                            ssh_public_key="/tmp/x.pub")
        with self.assertRaises(RuntimeError):
            _gcp_ssh_metadata(cfg, None, None)

    def test_missing_pubkey_raises(self):
        from chia.cluster.gcp_nodes import _gcp_ssh_metadata, GCPNodeConfig
        cfg = GCPNodeConfig(machine_type="n1-standard-4", count=1)
        with self.assertRaises(RuntimeError):
            _gcp_ssh_metadata(cfg, "ubuntu", None)


# ---------------------------------------------------------------------------
# Sanitizers / URL helpers (pure)
# ---------------------------------------------------------------------------

class TestHelpers(unittest.TestCase):

    def test_sanitize_name_strips_underscores(self):
        from chia.cluster.gcp_nodes import _sanitize_name
        self.assertEqual(_sanitize_name("chia-gcp_test-worker-0"),
                         "chia-gcp-test-worker-0")

    def test_sanitize_name_leading_digit(self):
        from chia.cluster.gcp_nodes import _sanitize_name
        self.assertTrue(_sanitize_name("9abc")[0].isalpha())

    def test_sanitize_label_keeps_underscore(self):
        from chia.cluster.gcp_nodes import _sanitize_label
        self.assertEqual(_sanitize_label("GCP_Test"), "gcp_test")

    def test_network_url(self):
        from chia.cluster.gcp_nodes import _network_url
        self.assertEqual(_network_url("default"), "global/networks/default")
        self.assertEqual(_network_url("global/networks/foo"),
                         "global/networks/foo")

    def test_region_of(self):
        from chia.cluster.gcp_nodes import _region_of
        self.assertEqual(_region_of("us-central1-a"), "us-central1")

    def test_machine_type_url(self):
        from chia.cluster.gcp_nodes import _machine_type_url
        self.assertEqual(_machine_type_url("us-central1-a", "n1-standard-4"),
                         "zones/us-central1-a/machineTypes/n1-standard-4")


# ===========================================================================
# Mapping tests — require google-cloud-compute (a declared dep). Mock clients.
# ===========================================================================

compute_v1 = pytest.importorskip("google.cloud.compute_v1")


def _make_fake_cv(instances_client=None, firewalls_client=None, subnets_client=None):
    """A stand-in for compute_v1: real proto classes, fake network clients."""
    from google.cloud import compute_v1 as real
    fake = types.SimpleNamespace()
    for attr in (
        "Instance", "AttachedDisk", "AttachedDiskInitializeParams",
        "NetworkInterface", "AccessConfig", "Tags", "Metadata", "Items",
        "Scheduling", "Allowed", "Firewall",
        "AggregatedListInstancesRequest", "AggregatedListSubnetworksRequest",
    ):
        setattr(fake, attr, getattr(real, attr))
    fake.InstancesClient = lambda *a, **k: instances_client
    fake.FirewallsClient = lambda *a, **k: firewalls_client
    fake.SubnetworksClient = lambda *a, **k: subnets_client
    return fake


class _Op:
    def result(self, timeout=None):
        return None


class _FakeInstancesClient:
    def __init__(self, ip_by_name=None):
        self.inserted = []   # (project, zone, instance_resource)
        self.deleted = []    # (project, zone, instance)
        self._ip_by_name = ip_by_name or {}
        self._listing = []   # list[(scope, scoped)]

    def insert(self, project, zone, instance_resource):
        self.inserted.append((project, zone, instance_resource))
        return _Op()

    def get(self, project, zone, instance):
        ip = self._ip_by_name.get(instance, "203.0.113.9")
        return compute_v1.Instance(
            name=instance,
            network_interfaces=[compute_v1.NetworkInterface(
                access_configs=[compute_v1.AccessConfig(nat_i_p=ip)])],
        )

    def delete(self, project, zone, instance):
        self.deleted.append((project, zone, instance))
        return _Op()

    def set_listing(self, listing):
        self._listing = listing

    def aggregated_list(self, request=None):
        return iter(self._listing)


@pytest.fixture
def pubkey(tmp_path):
    p = tmp_path / "id.pub"
    p.write_text("ssh-ed25519 AAAAKEY user@host\n")
    return str(p)


class TestBuildInstance:

    def test_full_instance(self, pubkey):
        from chia.cluster.gcp_nodes import _build_instance, GCPNodeConfig
        cfg = GCPNodeConfig(
            machine_type="n1-standard-16", count=1, spot=True,
            disk_size_gb=128, ssh_public_key=pubkey,
            image="projects/p/global/images/img",
        )
        inst = _build_instance("gcp_test", "gcp_verilator", cfg, 0,
                               "us-central1-b", "my-net", "my-subnet",
                               "ubuntu", None)
        assert inst.name == "chia-gcp-test-gcp-verilator-0"
        assert inst.machine_type == "zones/us-central1-b/machineTypes/n1-standard-16"
        assert inst.disks[0].initialize_params.source_image == "projects/p/global/images/img"
        assert inst.disks[0].initialize_params.disk_size_gb == 128
        assert inst.network_interfaces[0].network == "global/networks/my-net"
        assert inst.network_interfaces[0].subnetwork == "my-subnet"
        assert inst.network_interfaces[0].access_configs[0].type_ == "ONE_TO_ONE_NAT"
        assert dict(inst.labels) == {
            "chia-cluster": "gcp_test",
            "chia-node-type": "gcp_verilator",
            "chia-node-index": "0",
        }
        assert list(inst.tags.items) == ["chia-gcp-test"]
        meta = {i.key: i.value for i in inst.metadata.items}
        assert meta["ssh-keys"].startswith("ubuntu:ssh-ed25519")
        assert inst.scheduling.provisioning_model == "SPOT"

    def test_extra_args_deep_merge(self, pubkey):
        from chia.cluster.gcp_nodes import _build_instance, GCPNodeConfig
        cfg = GCPNodeConfig(
            machine_type="n1-standard-4", count=1, spot=True,
            ssh_public_key=pubkey,
            extra_args={"can_ip_forward": True,
                        "scheduling": {"min_node_cpus": 2}},
        )
        inst = _build_instance("c", "w", cfg, 0, "us-central1-a",
                               None, None, "ubuntu", None)
        assert inst.can_ip_forward is True
        # nested merge preserves the spot provisioning model AND adds the field
        assert inst.scheduling.provisioning_model == "SPOT"
        assert inst.scheduling.min_node_cpus == 2


class TestProvisionAndDiscover:

    def test_provision_inserts_and_collects_ips(self, monkeypatch, pubkey):
        from chia.cluster import gcp_nodes as g
        ic = _FakeInstancesClient(ip_by_name={
            "chia-c-gcp-worker-0": "34.0.0.1",
            "chia-c-gcp-worker-1": "34.0.0.2",
        })
        monkeypatch.setattr(g, "_compute", lambda: _make_fake_cv(instances_client=ic))
        monkeypatch.setattr(g, "ensure_ssh_firewall", lambda *a, **k: "tag")

        cfg = g.GCPNodeConfig(machine_type="n1-standard-4", count=2,
                              ssh_public_key=pubkey)
        ip_map = g.provision_gcp_nodes("c", {"gcp_worker": cfg},
                                       "proj", "us-central1-a",
                                       default_ssh_user="ubuntu")
        assert ip_map == {"gcp_worker": ["34.0.0.1", "34.0.0.2"]}
        assert len(ic.inserted) == 2
        # instances carry the cluster label for later discovery
        first = ic.inserted[0][2]
        assert dict(first.labels)["chia-cluster"] == "c"

    def test_discover_parses_labels(self, monkeypatch):
        from chia.cluster import gcp_nodes as g
        ic = _FakeInstancesClient()

        def _inst(name, ntype, idx, ip):
            return compute_v1.Instance(
                name=name, status="RUNNING",
                labels={"chia-cluster": "c", "chia-node-type": ntype,
                        "chia-node-index": str(idx)},
                network_interfaces=[compute_v1.NetworkInterface(
                    access_configs=[compute_v1.AccessConfig(nat_i_p=ip)])],
            )
        scoped = types.SimpleNamespace(instances=[
            _inst("a", "gcp_worker", 1, "5.0.0.2"),
            _inst("b", "gcp_worker", 0, "5.0.0.1"),
        ])
        ic.set_listing([("zones/us-central1-a", scoped)])
        monkeypatch.setattr(g, "_compute", lambda: _make_fake_cv(instances_client=ic))

        ip_map = g.discover_gcp_nodes("c", "proj")
        # sorted by chia-node-index
        assert ip_map == {"gcp_worker": ["5.0.0.1", "5.0.0.2"]}

    def test_teardown_deletes_all(self, monkeypatch):
        from chia.cluster import gcp_nodes as g
        ic = _FakeInstancesClient()
        scoped = types.SimpleNamespace(instances=[
            compute_v1.Instance(name="a"), compute_v1.Instance(name="b")])
        ic.set_listing([("zones/us-central1-a", scoped)])
        monkeypatch.setattr(g, "_compute", lambda: _make_fake_cv(instances_client=ic))

        deleted = g.teardown_gcp_nodes("c", "proj")
        assert sorted(deleted) == ["a", "b"]
        assert {d[2] for d in ic.deleted} == {"a", "b"}
        assert {d[1] for d in ic.deleted} == {"us-central1-a"}


class _FakeFirewallsClient:
    def __init__(self):
        self.inserted = []
        from google.api_core.exceptions import NotFound
        self._NotFound = NotFound

    def get(self, project, firewall):
        raise self._NotFound("nope")

    def insert(self, project, firewall_resource):
        self.inserted.append(firewall_resource)
        return _Op()

    def patch(self, project, firewall, firewall_resource):
        self.inserted.append(firewall_resource)
        return _Op()

    def delete(self, project, firewall):
        return _Op()


class _FakeSubnetsClient:
    def __init__(self, cidrs):
        self._cidrs = cidrs

    def aggregated_list(self, request=None):
        from google.cloud import compute_v1 as real
        subs = [real.Subnetwork(network="x/networks/default", ip_cidr_range=c)
                for c in self._cidrs]
        scoped = types.SimpleNamespace(subnetworks=subs)
        return iter([("regions/us-central1", scoped)])


class TestFirewall:

    def test_ssh_and_intra_vpc_rules(self, monkeypatch):
        from chia.cluster import gcp_nodes as g
        monkeypatch.setenv("CHIA_SSH_ALLOWED_CIDRS", "9.9.9.9/32")
        monkeypatch.setenv("CHIA_ALLOW_INTRA_VPC", "true")
        fc = _FakeFirewallsClient()
        sc = _FakeSubnetsClient(["10.128.0.0/20"])
        monkeypatch.setattr(
            g, "_compute",
            lambda: _make_fake_cv(firewalls_client=fc, subnets_client=sc))
        # avoid touching default-allow-ssh
        monkeypatch.setattr(g, "_maybe_lockdown_default_ssh", lambda *a, **k: None)

        tag = g.ensure_ssh_firewall("gcp_test", "proj", network="default")
        assert tag == "chia-gcp-test"
        names = {fw.name for fw in fc.inserted}
        assert "chia-gcp-test-ssh" in names
        assert "chia-gcp-test-internal" in names

        ssh_rule = next(fw for fw in fc.inserted if fw.name == "chia-gcp-test-ssh")
        assert list(ssh_rule.source_ranges) == ["9.9.9.9/32"]
        assert list(ssh_rule.target_tags) == ["chia-gcp-test"]
        assert ssh_rule.allowed[0].I_p_protocol == "tcp"
        assert list(ssh_rule.allowed[0].ports) == ["22"]

        internal = next(fw for fw in fc.inserted if fw.name == "chia-gcp-test-internal")
        assert list(internal.source_ranges) == ["10.128.0.0/20"]


if __name__ == "__main__":
    unittest.main()
