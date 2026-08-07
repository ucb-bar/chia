"""Live end-to-end test for scoped ``chia down`` on a host running TWO Ray
clusters at once, including a Docker container SHARED between them.

Scenario (the shared-host trap that scoped teardown exists for):

  * ``down_cluster_a`` — head GCS on :6379, a local bare worker (``generic_a``),
    a local docker worker in the SHARED container ``chia-verilator-run-${USER}``
    (``verilator_run``), and a tunneled EC2 docker worker (``nano_a``).
  * ``down_cluster_b`` — head GCS on :6378, a local bare worker (``generic_b``),
    a local docker worker in the SAME shared container (``verilator_run``), and a
    docker worker in its own container ``chia-verilator-run-b-${USER}``.

Both heads, cluster_a's tunnel, and the shared container live on the one head
machine. Scoped ``chia down cluster_a`` must:

  * stop only cluster_a's Ray processes (matched by GCS address),
  * tear down only cluster_a's tunnels and terminate only its EC2 instance,
  * scoped-stop cluster_a's Ray INSIDE the shared container but LEAVE THE
    CONTAINER RUNNING, because cluster_b's Ray is still in it —

leaving cluster_b fully alive. A host-global ``ray stop`` (pre-scoped-teardown)
would kill both clusters, and a blanket ``docker rm -f`` would destroy the
shared container and cluster_b's worker with it.

Liveness is checked with ``chia status --chia-cluster <yaml>``, which pins
``RAY_ADDRESS`` to that cluster's ``head_ip:port`` (so it can't be fooled by
whichever GCS ``ray``'s auto-discovery would otherwise pick). Which cluster is
up is confirmed from the per-cluster worker resources in ``ray status`` output,
and the shared container's survival is checked directly with ``docker inspect``.

This spins up real EC2 instances, so it is gated behind
``CHIA_RUN_DOWN_LIVE_TESTS=1``. It also needs, besides the chia_env and AWS
config, an ``ssh-agent`` holding a GitHub-authorized key: chia bootstraps each
EC2 node by ``git clone``-ing chia over the forwarded agent (``ssh -A``), so an
empty/absent agent makes the cloud workers fail to set up::

    conda activate chia_env
    source /scratch/angelacui/aws/setup.sh
    eval "$(ssh-agent -s)" && ssh-add ~/.ssh/id_rsa   # key with GitHub access
    export THIS_MACHINE=$(hostname -I | awk '{print $1}')
    CHIA_RUN_DOWN_LIVE_TESTS=1 \
      python -m pytest chia/cluster/test/down/test_scoped_down_live.py -v -s
"""
from __future__ import annotations

import os
import socket
import subprocess
from pathlib import Path

import pytest

HERE = Path(__file__).parent
CLUSTER_A = str(HERE / "cluster_a.yaml")
CLUSTER_B = str(HERE / "cluster_b.yaml")

# Per-cluster marker resources, used to tell the clusters apart in `ray status`.
# cluster_a: bare worker (generic_a) + EC2 tunnel worker (nano_a).
# cluster_b: bare worker (generic_b) + its docker workers (verilator_run), one
# of which is in the container shared with cluster_a — so `verilator_run` still
# present on :6378 after downing cluster_a proves the shared-container worker
# survived.
A_MARKERS = ("generic_a", "nano_a")
B_MARKERS = ("generic_b", "verilator_run")

A_GCS_PORT = 6379
B_GCS_PORT = 6378

# Container shared by cluster_a's and cluster_b's docker workers, plus the
# container unique to cluster_b (worker_index 0 in both cases).
_USER = os.environ.get("USER", "user")
SHARED_CONTAINER = f"chia-verilator-run-{_USER}-0"
B_CONTAINER = f"chia-verilator-run-b-{_USER}-0"

_RUN = os.environ.get("CHIA_RUN_DOWN_LIVE_TESTS") == "1"
_live = pytest.mark.skipif(
    not _RUN,
    reason="set CHIA_RUN_DOWN_LIVE_TESTS=1 to run (brings up two real Ray "
           "clusters incl. EC2 instances and tears them down)",
)

# Live bring-up/teardown of an EC2-backed cluster is slow (instance launch,
# docker pull, tunnels): give each chia invocation a generous ceiling.
UP_TIMEOUT = 1800
DOWN_TIMEOUT = 900
STATUS_TIMEOUT = 120


def _this_machine() -> str:
    out = subprocess.run(["hostname", "-I"], capture_output=True, text=True)
    parts = out.stdout.split()
    return parts[0] if parts else socket.gethostbyname(socket.gethostname())


def _env() -> dict:
    """Environment for chia subprocesses: THIS_MACHINE + inherited AWS config."""
    env = os.environ.copy()
    env.setdefault("THIS_MACHINE", _this_machine())
    return env


def _chia(*args: str, timeout: int) -> subprocess.CompletedProcess:
    """Run ``chia <args>`` and return the completed process (never raises on
    non-zero exit; callers assert on returncode/output)."""
    return subprocess.run(
        ["chia", *args], capture_output=True, text=True,
        timeout=timeout, env=_env(),
    )


def _scoped_stop(port: int) -> None:
    """Best-effort scoped stop of any leftover cluster on ``head_ip:port`` —
    safe on a shared host (only that address is touched)."""
    ip = _env()["THIS_MACHINE"]
    subprocess.run(
        ["python", "-m", "chia.cluster.ray_stop",
         "--address", f"{ip}:{port}", "--force"],
        capture_output=True, text=True, timeout=120, env=_env(),
    )


def _status(cluster_yaml: str) -> subprocess.CompletedProcess:
    """``chia status --chia-cluster <yaml>`` — RAY_ADDRESS pinned to that
    cluster's head GCS address."""
    return _chia("status", "--chia-cluster", cluster_yaml, timeout=STATUS_TIMEOUT)


def _is_alive(cluster_yaml: str, markers: tuple[str, ...]) -> bool:
    """A cluster is alive iff ``chia status`` for it succeeds AND reports every
    one of its marker resources (proving the right cluster answered)."""
    res = _status(cluster_yaml)
    if res.returncode != 0:
        return False
    blob = res.stdout + res.stderr
    return all(m in blob for m in markers)


def _container_running(name: str) -> bool:
    r = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", name],
                       capture_output=True, text=True)
    return r.returncode == 0 and r.stdout.strip() == "true"


def _rm_container(name: str) -> None:
    subprocess.run(["docker", "rm", "-f", name], capture_output=True, text=True)


# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def two_clusters_up():
    _scoped_stop(A_GCS_PORT)   # clear leftovers from a prior run (scoped, safe)
    _scoped_stop(B_GCS_PORT)
    _rm_container(SHARED_CONTAINER)
    _rm_container(B_CONTAINER)

    try:
        a = _chia("up", CLUSTER_A, "-y", timeout=UP_TIMEOUT)
        assert a.returncode == 0, f"chia up cluster_a failed:\n{a.stdout}\n{a.stderr}"

        b = _chia("up", CLUSTER_B, "-y", timeout=UP_TIMEOUT)
        assert b.returncode == 0, f"chia up cluster_b failed:\n{b.stdout}\n{b.stderr}"

        yield
    finally:
        # Always down BOTH configs, even after a partial/failed bring-up, so a
        # launched EC2 instance can never leak. `chia down` on a cluster that is
        # already gone is a safe no-op (it just finds nothing to terminate).
        for cfg in (CLUSTER_A, CLUSTER_B):
            _chia("down", cfg, "-y", timeout=DOWN_TIMEOUT)
        _scoped_stop(A_GCS_PORT)
        _scoped_stop(B_GCS_PORT)
        _rm_container(SHARED_CONTAINER)
        _rm_container(B_CONTAINER)


@_live
def test_scoped_down_kills_only_target_cluster(two_clusters_up):
    # Both clusters must be up first, each identifiable by its own resources...
    assert _is_alive(CLUSTER_A, A_MARKERS), "cluster_a should be up before down"
    assert _is_alive(CLUSTER_B, B_MARKERS), "cluster_b should be up before down"
    # ...and both clusters' Ray must be running in the one shared container.
    assert _container_running(SHARED_CONTAINER), (
        f"shared container {SHARED_CONTAINER} should be running before down")

    # Scoped teardown of cluster_a (scoped is the default).
    down = _chia("down", CLUSTER_A, "-y", timeout=DOWN_TIMEOUT)
    assert down.returncode == 0, f"chia down cluster_a failed:\n{down.stdout}\n{down.stderr}"

    # cluster_a is gone: its head GCS (:6379) no longer answers.
    assert not _is_alive(CLUSTER_A, A_MARKERS), (
        "cluster_a should be DOWN after scoped teardown")

    # The shared container is STILL running — cluster_a's Ray was scoped-stopped
    # inside it, but cluster_b's Ray remained, so it must NOT be `docker rm -f`ed.
    assert _container_running(SHARED_CONTAINER), (
        f"shared container {SHARED_CONTAINER} must NOT be removed while "
        "cluster_b's Ray is still in it")

    # cluster_b survived intact — head, bare worker, its shared-container worker
    # (verilator_run still on :6378), and its unique-container worker. A blanket
    # `ray stop` / `docker rm -f` would have taken these down too.
    assert _is_alive(CLUSTER_B, B_MARKERS), (
        "cluster_b should still be ALIVE after scoping down cluster_a — including "
        "its worker in the shared container")
