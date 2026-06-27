"""
Smoke test for EC2 cross-network cluster via SSH tunnels.

Verifies that a Ray task can be scheduled on the tunnelled EC2 worker
and return its hostname.

Run:
  chia up test/test_ec2_cluster.yaml
  python -m pytest test/test_ec2_cluster.py -v
"""

import os
import socket
import unittest

import ray


def _has_resource(name: str) -> bool:
    return ray.cluster_resources().get(name, 0) > 0


class TestEC2Cluster(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if not ray.is_initialized():
            ray.init(
                address="auto",
                runtime_env={"working_dir": os.path.dirname(os.path.dirname(os.path.dirname(__file__)))},
            )

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_ec2_hostname(self):
        """Run ``hostname`` on the EC2 worker and verify it looks like an EC2 host."""
        if not _has_resource("ec2"):
            self.skipTest("cluster missing resource: ec2")

        @ray.remote(resources={"ec2": 1})
        def get_hostname():
            return socket.gethostname()

        result = ray.get(get_hostname.remote())
        print(f"EC2 hostname: {result}")
        self.assertTrue(
            result.startswith("ip-"),
            f"Expected EC2 hostname starting with 'ip-', got: {result}",
        )


if __name__ == "__main__":
    unittest.main()
