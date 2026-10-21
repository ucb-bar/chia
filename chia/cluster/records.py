"""Records of the clusters that ``chia up`` brought up on this machine: one YAML file per
cluster in $XDG_STATE_HOME/chia/clusters (by default ~/.local/state/chia/clusters), which
``chia down`` deletes. A record outlives a reboot of the head, as the cluster's cloud
machines do."""

from __future__ import annotations

import os
import socket
from pathlib import Path

import yaml


def _dir() -> Path:
    return Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local" / "state") / "chia" / "clusters"


def save(name: str, cluster_file: str, head_address: str) -> None:
    """Record cluster *name*, brought up from *cluster_file*, and its head's GCS address."""
    _dir().mkdir(parents=True, exist_ok=True)
    (_dir() / f"{name}.yaml").write_text(yaml.safe_dump(
        {"name": name, "cluster_file": os.path.abspath(cluster_file), "head_address": head_address}))


def remove(name: str) -> None:
    (_dir() / f"{name}.yaml").unlink(missing_ok=True)


def load(name: str) -> dict | None:
    path = _dir() / f"{name}.yaml"
    return yaml.safe_load(path.read_text()) if path.is_file() else None


def load_all() -> list[dict]:
    return [yaml.safe_load(p.read_text()) for p in sorted(_dir().glob("*.yaml"))]


def is_up(address: str) -> bool:
    """True if the head's GCS accepts a connection at *address* (``host:port``) within 3 s."""
    host, _, port = address.rpartition(":")
    try:
        with socket.create_connection((host.strip("[]"), int(port)), timeout=3):
            return True
    except (OSError, ValueError):
        return False
