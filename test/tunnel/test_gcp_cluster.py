"""Smoke test for a GCP cross-network cluster via SSH tunnels.

Verifies that a Ray task can be scheduled on a tunnelled GCP worker and return
its hostname.  The GCP analog of test_ec2_cluster.py.

Run:
  chia up test/tunnel/test_gcp_cluster.yaml
  python -m pytest test/tunnel/test_gcp_cluster.py -v

The existing provider-agnostic tunnel suite also runs against this cluster:
  python -m pytest test/tunnel/test_ec2_concurrency.py::TestTunnelConcurrency::test_verilator_run_worker_port_capacity -v -s
  python -m pytest test/tunnel/test_ec2_tool.py -v
"""

import os
import socket
import unittest

import ray


def _has_resource(name: str) -> bool:
    return ray.cluster_resources().get(name, 0) > 0


class TestGCPCluster(unittest.TestCase):

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

    def test_gcp_hostname(self):
        """Run ``hostname`` on the GCP worker and verify it's a chia-named GCE host."""
        if not _has_resource("gcp"):
            self.skipTest("cluster missing resource: gcp")

        @ray.remote(resources={"gcp": 1})
        def get_hostname():
            return socket.gethostname()

        result = ray.get(get_hostname.remote())
        print(f"GCP hostname: {result}")
        # GCE sets the hostname to the instance name, which chia derives as
        # chia-<cluster>-<type>-<index>.
        self.assertTrue(
            result.startswith("chia-"),
            f"Expected GCE hostname starting with 'chia-', got: {result}",
        )


if __name__ == "__main__":
    unittest.main()
