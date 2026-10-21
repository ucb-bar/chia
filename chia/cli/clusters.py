"""``chia clusters`` — list the clusters that chia brought up on this machine (see
chia/cluster/records.py), and whether each one is up."""

from __future__ import annotations

from chia.cluster import records


def cmd_clusters(args) -> None:
    rows = [(r["name"], "up" if records.is_up(r["head_address"]) else "not up",
             r["head_address"], r["cluster_file"]) for r in records.load_all()]
    if not rows:
        print("No chia clusters.")
        return
    rows.insert(0, ("NAME", "STATUS", "HEAD ADDRESS", "CLUSTER FILE"))
    widths = [max(len(row[i]) for row in rows) for i in range(3)]
    for row in rows:
        print("  ".join(cell.ljust(w) for cell, w in zip(row, widths)) + "  " + row[3])
