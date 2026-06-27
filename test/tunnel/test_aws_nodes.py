"""Unit tests for AWS node provisioning config parsing and placeholder expansion.

These tests do NOT require AWS credentials or network access — they only
test the config parsing and placeholder replacement logic.

Run:
  python -m pytest test/tunnel/test_aws_nodes.py -v
"""

import copy
import unittest

from chia.cluster.aws_nodes import AWSNodeConfig, DEFAULT_IMAGE_ID, DEFAULT_REGION
from chia.cluster.config import (
    ConfigError, _expand_aws_placeholders, build_config,
    load_raw_config, parse_aws_nodes,
)


class TestParseAWSNodes(unittest.TestCase):

    def _make_raw(self, **overrides):
        base = {
            "cluster_name": "test",
            "aws_nodes": {
                "ec2_worker": {
                    "KeyName": "mykey",
                    "InstanceType": "t2.micro",
                    "count": 2,
                },
            },
            "provider": {"head_ip": "10.0.0.1"},
            "auth": {"ssh_user": "testuser"},
        }
        base.update(overrides)
        return base

    def test_parse_basic(self):
        raw = self._make_raw()
        result = parse_aws_nodes(raw)
        self.assertIsNotNone(result)
        nodes, region = result
        self.assertEqual(region, DEFAULT_REGION)
        self.assertIn("ec2_worker", nodes)
        cfg = nodes["ec2_worker"]
        self.assertEqual(cfg.KeyName, "mykey")
        self.assertEqual(cfg.InstanceType, "t2.micro")
        self.assertEqual(cfg.count, 2)
        self.assertEqual(cfg.ImageId, DEFAULT_IMAGE_ID)
        self.assertEqual(cfg.extra_args, {})

    def test_parse_pops_from_raw(self):
        raw = self._make_raw()
        parse_aws_nodes(raw)
        self.assertNotIn("aws_nodes", raw)

    def test_parse_missing_returns_none(self):
        raw = self._make_raw()
        del raw["aws_nodes"]
        result = parse_aws_nodes(raw)
        self.assertIsNone(result)

    def test_parse_custom_region(self):
        raw = self._make_raw()
        raw["aws_nodes"]["region"] = "eu-west-1"
        _, region = parse_aws_nodes(raw)
        self.assertEqual(region, "eu-west-1")

    def test_parse_custom_image_id(self):
        raw = self._make_raw()
        raw["aws_nodes"]["ec2_worker"]["ImageId"] = "ami-custom123"
        nodes, _ = parse_aws_nodes(raw)
        self.assertEqual(nodes["ec2_worker"].ImageId, "ami-custom123")

    def test_parse_extra_args(self):
        raw = self._make_raw()
        raw["aws_nodes"]["ec2_worker"]["BlockDeviceMappings"] = [
            {"DeviceName": "/dev/sda1", "Ebs": {"VolumeSize": 200}}
        ]
        nodes, _ = parse_aws_nodes(raw)
        cfg = nodes["ec2_worker"]
        self.assertIn("BlockDeviceMappings", cfg.extra_args)
        self.assertEqual(cfg.extra_args["BlockDeviceMappings"][0]["Ebs"]["VolumeSize"], 200)

    def test_parse_missing_key_name(self):
        raw = self._make_raw()
        del raw["aws_nodes"]["ec2_worker"]["KeyName"]
        with self.assertRaises(ConfigError):
            parse_aws_nodes(raw)

    def test_parse_missing_instance_type(self):
        raw = self._make_raw()
        del raw["aws_nodes"]["ec2_worker"]["InstanceType"]
        with self.assertRaises(ConfigError):
            parse_aws_nodes(raw)

    def test_parse_missing_count(self):
        raw = self._make_raw()
        del raw["aws_nodes"]["ec2_worker"]["count"]
        with self.assertRaises(ConfigError):
            parse_aws_nodes(raw)

    def test_parse_multiple_node_types(self):
        raw = self._make_raw()
        raw["aws_nodes"]["gpu_worker"] = {
            "KeyName": "gpukey",
            "InstanceType": "p3.2xlarge",
            "count": 1,
        }
        nodes, _ = parse_aws_nodes(raw)
        self.assertEqual(len(nodes), 2)
        self.assertIn("ec2_worker", nodes)
        self.assertIn("gpu_worker", nodes)


class TestExpandAWSPlaceholders(unittest.TestCase):

    def setUp(self):
        self.ip_map = {
            "ec2_worker": ["1.2.3.4", "5.6.7.8"],
            "gpu_worker": ["10.0.0.1"],
        }

    def test_string_replacement(self):
        result = _expand_aws_placeholders("@ec2_worker:0", self.ip_map)
        self.assertEqual(result, "1.2.3.4")

    def test_string_replacement_second_index(self):
        result = _expand_aws_placeholders("@ec2_worker:1", self.ip_map)
        self.assertEqual(result, "5.6.7.8")

    def test_string_with_surrounding_text(self):
        result = _expand_aws_placeholders("ssh user@@ec2_worker:0", self.ip_map)
        self.assertEqual(result, "ssh user@1.2.3.4")

    def test_list_replacement(self):
        data = ["@ec2_worker:0", "@gpu_worker:0", "static"]
        result = _expand_aws_placeholders(data, self.ip_map)
        self.assertEqual(result, ["1.2.3.4", "10.0.0.1", "static"])

    def test_dict_value_replacement(self):
        data = {"worker_ips": ["@ec2_worker:0"], "head": "static"}
        result = _expand_aws_placeholders(data, self.ip_map)
        self.assertEqual(result, {"worker_ips": ["1.2.3.4"], "head": "static"})

    def test_dict_key_replacement(self):
        data = {"@ec2_worker:0": {"ssh_user": "ec2-user"}}
        result = _expand_aws_placeholders(data, self.ip_map)
        self.assertIn("1.2.3.4", result)
        self.assertNotIn("@ec2_worker:0", result)

    def test_nested_replacement(self):
        data = {
            "auth": {
                "overrides": {
                    "@ec2_worker:0": {
                        "ssh_user": "ec2-user",
                        "tunnel": {"tunnel_ip": "127.0.0.2"},
                    },
                },
            },
            "provider": {"worker_ips": ["@ec2_worker:0"]},
        }
        result = _expand_aws_placeholders(data, self.ip_map)
        self.assertIn("1.2.3.4", result["auth"]["overrides"])
        self.assertEqual(result["provider"]["worker_ips"], ["1.2.3.4"])

    def test_unknown_node_name_raises(self):
        with self.assertRaises(ConfigError) as ctx:
            _expand_aws_placeholders("@nonexistent:0", self.ip_map)
        self.assertIn("nonexistent", str(ctx.exception))

    def test_index_out_of_bounds_raises(self):
        with self.assertRaises(ConfigError) as ctx:
            _expand_aws_placeholders("@ec2_worker:5", self.ip_map)
        self.assertIn("out of range", str(ctx.exception))

    def test_non_string_passthrough(self):
        self.assertEqual(_expand_aws_placeholders(42, self.ip_map), 42)
        self.assertEqual(_expand_aws_placeholders(True, self.ip_map), True)
        self.assertIsNone(_expand_aws_placeholders(None, self.ip_map))

    def test_no_placeholder_unchanged(self):
        data = {"key": "value", "list": [1, 2, "three"]}
        result = _expand_aws_placeholders(copy.deepcopy(data), self.ip_map)
        self.assertEqual(result, data)


class TestLoadRawAndBuildConfig(unittest.TestCase):
    """Verify the split load/build path produces the same result as the
    combined load_config for a config without aws_nodes."""

    def test_split_matches_combined(self):
        import tempfile, yaml, os
        config_data = {
            "cluster_name": "split_test",
            "provider": {"head_ip": "10.0.0.1"},
            "auth": {"ssh_user": "testuser"},
            "available_node_types": {
                "worker": {"resources": {"cpu": 1}, "num_workers": 1,
                           "compatible_ips": ["10.0.0.2"]},
            },
            "head_start_ray_commands": ["ray start --head"],
            "worker_start_ray_commands": ["ray start --address=$RAY_HEAD_IP:6379"],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config_data, f)
            path = f.name

        try:
            from chia.cluster.config import load_config
            combined = load_config(path)

            raw = load_raw_config(path)
            split = build_config(raw)

            self.assertEqual(combined.cluster_name, split.cluster_name)
            self.assertEqual(combined.head_ip, split.head_ip)
            self.assertEqual(combined.worker_ips, split.worker_ips)
            self.assertEqual(combined.ssh_user, split.ssh_user)
            self.assertEqual(len(combined.node_types), len(split.node_types))
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
