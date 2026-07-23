"""End-to-end worker-detection tests for ``chia up --add`` on a host that
runs *two* Ray clusters at once.

The scenario is the aliasing trap: ``cluster_a`` (head on port 6379) and
``cluster_b`` (head on port 6378) run on the same machine and deliberately
share one ``--temp-dir`` (``/tmp/$USER/cluster_a``).  Because they share the
temp-dir, ``ray.init(address="auto")`` cannot tell them apart — it connects to
whichever GCS started *last*.  ``chia up`` avoids that by addressing the head
explicitly as ``head_ip:port`` (``ClusterConfig.head_ray_address``), parsed
from the YAML's ``--port`` flag.

Tests: 
  * ``test_add_*`` — bring up both clusters, then:
      - ``chia up --add cluster_b_add.yaml`` must detect the one missing
        worker by querying cluster_b (6378).
      - ``chia up --add cluster_a.yaml`` must report nothing to add, because
        cluster_a (6379) already has its worker.

The integration tests bring clusters up and tear them down with a host-wide
``ray stop``, which would kill any other Ray instance on the machine.  They
are therefore gated behind ``CHIA_RUN_MULTI_CLUSTER_TESTS=1``::

    THIS_MACHINE=$(hostname -I | awk '{print $1}') \
    CHIA_RUN_MULTI_CLUSTER_TESTS=1 \
    python -m pytest test_multi_cluster_add.py -v -s
"""

from __future__ import annotations

import argparse
import os
import re
import socket
import subprocess
from pathlib import Path

import pytest

from chia.cli.up import cmd_up

HERE = Path(__file__).parent
CLUSTER_A = str(HERE / "cluster_a.yaml")
CLUSTER_B = str(HERE / "cluster_b.yaml")
CLUSTER_B_ADD = str(HERE / "cluster_b_add.yaml")

_RUN_INTEGRATION = os.environ.get("CHIA_RUN_MULTI_CLUSTER_TESTS") == "1"
_integration = pytest.mark.skipif(
    not _RUN_INTEGRATION,
    reason="set CHIA_RUN_MULTI_CLUSTER_TESTS=1 to run (brings up/tears down "
           "real local Ray clusters; teardown runs a host-wide `ray stop`)",
)


def _this_machine() -> str:
    """Primary host IP, matching the docs' ``hostname -I | awk '{print $1}'``."""
    out = subprocess.run(["hostname", "-I"], capture_output=True, text=True)
    parts = out.stdout.split()
    return parts[0] if parts else socket.gethostbyname(socket.gethostname())


@pytest.fixture(scope="module", autouse=True)
def _this_machine_env():
    """The YAMLs interpolate ${THIS_MACHINE}; make sure it is set."""
    prev = os.environ.get("THIS_MACHINE")
    if not prev:
        os.environ["THIS_MACHINE"] = _this_machine()
    yield
    if prev is None:
        os.environ.pop("THIS_MACHINE", None)
    else:
        os.environ["THIS_MACHINE"] = prev


def _args(config_file: str, *, add: bool = False, dry_run: bool = False):
    return argparse.Namespace(
        config_file=config_file, yes=True, verbose=False,
        dry_run=dry_run, add=add,
    )


def _run_up(config_file: str, *, add: bool = False, dry_run: bool = False):
    cmd_up(_args(config_file, add=add, dry_run=dry_run))


def _ray_stop():
    subprocess.run(["ray", "stop", "--force"], capture_output=True)


def _added_count(capsys, config_file: str) -> int:
    """Run ``chia up --add <config> --dry-run`` and return the planned add count.

    ``--dry-run`` performs the real cluster query + assignment diff and prints
    the plan, then returns without mutating anything.
    """
    _run_up(config_file, add=True, dry_run=True)
    out = capsys.readouterr().out
    m = re.search(r"Will add (\d+) new worker", out)
    assert m is not None, f"add-plan line not found in output:\n{out}"
    return int(m.group(1))

# ---------------------------------------------------------------------------
# Integration tests — bring up both clusters, then exercise --add detection.
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def clusters_up(_this_machine_env):
    try:
        _run_up(CLUSTER_A)   # head :6379 + worker (worker_a)
        _run_up(CLUSTER_B)   # head :6378, no workers
    except SystemExit as e:
        _ray_stop()
        pytest.skip(
            f"chia up exited with {e.code}; integration test needs "
            "SSH-to-localhost, the chia_env conda env, and ray on PATH")
    except Exception as e:  # pragma: no cover - environment-dependent
        _ray_stop()
        pytest.skip(f"could not bring up local clusters: {e!r}")
    yield
    _ray_stop()


@_integration
def test_add_detects_missing_worker_on_correct_cluster(clusters_up, capsys):
    # cluster_b has no workers; cluster_b_add wants one "generic" worker. The
    # query must hit cluster_b (:6378), not the temp-dir-sharing cluster_a
    # (:6379, which DOES have a "generic" worker), and report one to add.
    assert _added_count(capsys, CLUSTER_B_ADD) == 1


@_integration
def test_add_reports_nothing_when_worker_present(clusters_up, capsys):
    # cluster_a already has its "generic" worker. Querying the right cluster
    # (:6379) reports zero to add. With address="auto" this resolves to the
    # last-started cluster (cluster_b, :6378), which has no worker, and wrongly
    # reports one to add — the aliasing bug this suite guards against.
    assert _added_count(capsys, CLUSTER_A) == 0
