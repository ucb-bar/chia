"""Offline unit tests for the dispatch-proxy reachability logic.

No cluster needed: ``ray`` is faked (``is_initialized`` / ``nodes`` /
``get_runtime_context``) so we can assert exactly when ``should_proxy`` decides
to relay a dispatch through the head. The rule under test is fail-safe — relay
unless every node the task could land on is provably directly reachable.

Topology used here (mirrors a real tunnelled cluster):
  head  (node:__internal_head__)          — the hub, reaches everyone
  lan   (local resource, LAN node-ip)       — reaches head + lan + self
  rem1  (gcp, 127.0.0.x tunnel node-ip)    — reaches only self
  rem2  (gcp + verilator_run, 127.0.0.x)

Run:
  python -m pytest test/tunnel/test_dispatch_proxy.py -v
"""

import types

import pytest
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

import chia.base.dispatch_proxy as dp

# Ray validates that a NodeAffinity node_id is a real 56-char hex string, so
# the fake node ids must be valid hex (not "head"/REM1).
HEAD = "1" * 56
LAN1 = "2" * 56
REM1 = "3" * 56
REM2 = "4" * 56

NODES = [
    {"NodeID": HEAD, "Alive": True, "NodeName": "10.0.0.1",
     "Resources": {"node:__internal_head__": 0.001, "node:10.0.0.1": 1.0, "CPU": 8}},
    {"NodeID": LAN1, "Alive": True, "NodeName": "10.0.0.2",
     "Resources": {"node:10.0.0.2": 1.0, "local": 1, "CPU": 8}},
    {"NodeID": REM1, "Alive": True, "NodeName": "127.0.0.2",
     "Resources": {"node:127.0.0.2": 1.0, "gcp": 1, "CPU": 8}},
    {"NodeID": REM2, "Alive": True, "NodeName": "127.0.0.3",
     "Resources": {"node:127.0.0.3": 1.0, "gcp": 1, "verilator_run": 8, "CPU": 8}},
]


def _install_fake_ray(monkeypatch, current_id, *, initialized=True,
                      node_id_raises=False, nodes=NODES):
    class _Ctx:
        def get_node_id(self):
            if node_id_raises:
                raise RuntimeError("boom")
            return current_id

    fake = types.SimpleNamespace(
        is_initialized=lambda: initialized,
        nodes=lambda: list(nodes),
        get_runtime_context=lambda: _Ctx(),
    )
    monkeypatch.setattr(dp, "ray", fake)
    dp._nodes_cache = None          # reset the module's TTL cache
    dp._nodes_cache_ts = 0.0


def _affinity(node_id, soft=False):
    return {"scheduling_strategy": NodeAffinitySchedulingStrategy(node_id, soft=soft)}


# ---------------------------------------------------------------------------
# Head never relays (and the proxy actor, which lives on the head, can't recurse)
# ---------------------------------------------------------------------------

def test_head_never_proxies_affinity(monkeypatch):
    _install_fake_ray(monkeypatch, HEAD)
    assert dp.should_proxy(_affinity(REM1)) is False

def test_head_never_proxies_unconstrained(monkeypatch):
    _install_fake_ray(monkeypatch, HEAD)
    assert dp.should_proxy({}) is False

def test_head_never_proxies_resource(monkeypatch):
    _install_fake_ray(monkeypatch, HEAD)
    assert dp.should_proxy({"resources": {"gcp": 1}}) is False


# ---------------------------------------------------------------------------
# LAN worker: the new local -> remote capability
# ---------------------------------------------------------------------------

def test_lan_to_remote_affinity_relays(monkeypatch):
    _install_fake_ray(monkeypatch, LAN1)
    assert dp.should_proxy(_affinity(REM1)) is True

def test_lan_to_head_affinity_direct(monkeypatch):
    _install_fake_ray(monkeypatch, LAN1)
    assert dp.should_proxy(_affinity(HEAD)) is False

def test_lan_to_self_affinity_direct(monkeypatch):
    _install_fake_ray(monkeypatch, LAN1)
    assert dp.should_proxy(_affinity(LAN1)) is False

def test_lan_remote_resource_relays(monkeypatch):
    # {"gcp":1} can only land on rem1/rem2 — both unreachable from lan.
    _install_fake_ray(monkeypatch, LAN1)
    assert dp.should_proxy({"resources": {"gcp": 1}}) is True

def test_lan_local_resource_direct(monkeypatch):
    # {"local":1} only exists on lan1 (self) — reachable.
    _install_fake_ray(monkeypatch, LAN1)
    assert dp.should_proxy({"resources": {"local": 1}}) is False

def test_lan_unconstrained_relays(monkeypatch):
    # No affinity, no resources -> could land anywhere -> can't prove -> relay.
    _install_fake_ray(monkeypatch, LAN1)
    assert dp.should_proxy({}) is True

def test_lan_soft_affinity_relays(monkeypatch):
    # Soft affinity can fall back to any node -> can't prove -> relay.
    _install_fake_ray(monkeypatch, LAN1)
    assert dp.should_proxy(_affinity(LAN1, soft=True)) is True


# ---------------------------------------------------------------------------
# Remote worker: preserves prior behavior (relay cross-spoke / ->LAN / ->head),
# with a self-dispatch fast-path.
# ---------------------------------------------------------------------------

def test_remote_to_local_relays(monkeypatch):
    _install_fake_ray(monkeypatch, REM1)
    assert dp.should_proxy(_affinity(LAN1)) is True

def test_remote_to_other_remote_relays(monkeypatch):
    _install_fake_ray(monkeypatch, REM1)
    assert dp.should_proxy(_affinity(REM2)) is True

def test_remote_to_head_relays(monkeypatch):
    # Conservative: remote reaches only itself directly in the matrix.
    _install_fake_ray(monkeypatch, REM1)
    assert dp.should_proxy(_affinity(HEAD)) is True

def test_remote_to_self_direct(monkeypatch):
    _install_fake_ray(monkeypatch, REM1)
    assert dp.should_proxy(_affinity(REM1)) is False

def test_remote_shared_resource_relays(monkeypatch):
    # {"gcp":1} -> rem1 (self, reachable) OR rem2 (unreachable) -> relay.
    _install_fake_ray(monkeypatch, REM1)
    assert dp.should_proxy({"resources": {"gcp": 1}}) is True


# ---------------------------------------------------------------------------
# Guards & fail-safe fallback
# ---------------------------------------------------------------------------

def test_not_initialized(monkeypatch):
    _install_fake_ray(monkeypatch, LAN1, initialized=False)
    assert dp.should_proxy(_affinity(REM1)) is False

def test_num_returns_gt_1(monkeypatch):
    _install_fake_ray(monkeypatch, REM1)
    assert dp.should_proxy({"num_returns": 2, **_affinity(LAN1)}) is False

def test_fallback_uses_env_rule_on_error(monkeypatch):
    # If the reachability analysis throws, fall back to the original env rule:
    # only reverse-tunnelled workers (relay-host env set) proxy.
    _install_fake_ray(monkeypatch, REM1, node_id_raises=True)
    monkeypatch.setenv("CHIA_TOOL_RELAY_HOST", "10.0.0.1")
    monkeypatch.setenv("CHIA_TOOL_ADVERTISE_HOST", "10.0.0.1")
    assert dp.should_proxy(_affinity(LAN1)) is True

def test_fallback_no_env_no_proxy_on_error(monkeypatch):
    _install_fake_ray(monkeypatch, LAN1, node_id_raises=True)
    monkeypatch.delenv("CHIA_TOOL_RELAY_HOST", raising=False)
    monkeypatch.delenv("CHIA_TOOL_ADVERTISE_HOST", raising=False)
    assert dp.should_proxy(_affinity(REM1)) is False


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
