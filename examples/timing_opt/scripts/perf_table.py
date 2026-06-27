"""Generate the performance-results table for the timing-improvement campaign.

Reads DB/timing.db and emits a per-branch row with:
  - achievable period (target + |worst_slack|)  -> frequency
  - geomean IPC across a configurable benchmark set
  - IPC ratio vs the baseline branch (using common tests only)
  - total speedup = freq_ratio * IPC_ratio

Run from 2_timingimprovement/.

Constants are tuned for the current MegaBoom-on-sky130 sweep:
  TARGET_PERIOD_NS = 10.0 ns  (Genus clock period target)
  BASELINE_BRANCH_NAME = ''
"""

import argparse
import json
import math
import sqlite3
from pathlib import Path

TARGET_PERIOD_NS = 10.0
BASELINE_BRANCH_NAME = "baseline_resynth"

EMBENCH = {
    "aha-mont64", "crc32", "depthconv", "edn", "huffbench", "matmult-int",
    "md5sum", "nettle-aes", "nettle-sha256", "nsichneu", "picojpeg", "qrduino",
    "sglib-combined", "slre", "statemate", "tarfind", "ud", "wikisort", "xgboost",
}

SUITES = {
    "all":        EMBENCH,
    "embench":    EMBENCH,

}


def per_test_ipc(conn, branch_id, suite):
    """Return {test_name: IPC} for tests in `suite` that passed for this branch."""
    out = {}
    for tn, cj, passed in conn.execute(
        "SELECT test_name, counters_json, passed FROM perf_results WHERE branch_id=?",
        (branch_id,),
    ):
        if not passed or tn not in suite:
            continue
        d = json.loads(cj)
        cycles = d.get("cycles", 0)
        if cycles > 0:
            out[tn] = d["instret"] / cycles
    return out


def geomean(values):
    if not values:
        return float("nan")
    return math.exp(sum(math.log(v) for v in values) / len(values))


def format_table(rows):
    header = ["Branch", "Slack (ns)", "Period (ns)", "Freq (MHz)",
              "Geomean IPC", "Freq ratio", "IPC ratio", "Speedup"]
    print("| " + " | ".join(header) + " |")
    print("|" + "|".join(["---"] * len(header)) + "|")
    for r in rows:
        print(f"| {r['name']} | {r['slack']:.3f} | {r['period']:.3f} | "
              f"{r['freq']:.1f} | {r['ipc']:.4f} | {r['freq_ratio']:.3f} | "
              f"{r['ipc_ratio']:.4f} | {r['speedup']:.3f}× |")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default="DB/timing.db")
    ap.add_argument("--suite", choices=SUITES.keys(), default="all",
                    help="benchmark set used for geomean IPC")
    ap.add_argument("--target", type=float, default=TARGET_PERIOD_NS,
                    help="synthesis target period in ns")
    ap.add_argument("--baseline", default=BASELINE_BRANCH_NAME,
                    help="branch to use as IPC reference")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    suite = SUITES[args.suite]

    base_row = conn.execute(
        "SELECT id, worst_slack_ns FROM branches WHERE name=? AND status='ok'",
        (args.baseline,),
    ).fetchone()
    if not base_row:
        raise SystemExit(f"baseline branch {args.baseline!r} not found in DB")
    base_id, base_slack = base_row
    base_period = args.target + abs(base_slack)
    base_ipc = per_test_ipc(conn, base_id, suite)

    rows = []
    for bid, name, slack in conn.execute(
        "SELECT id, name, worst_slack_ns FROM branches WHERE status='ok' "
        "AND worst_slack_ns IS NOT NULL ORDER BY id"
    ):
        period = args.target + abs(slack)
        freq = 1000.0 / period
        ipcs = per_test_ipc(conn, bid, suite)
        common = set(ipcs) & set(base_ipc)
        if not common:
            continue
        gm = geomean([ipcs[t] for t in common])
        base_gm = geomean([base_ipc[t] for t in common])
        ipc_ratio = gm / base_gm
        rows.append({
            "name": name,
            "slack": slack,
            "period": period,
            "freq": freq,
            "ipc": gm,
            "ipc_ratio": ipc_ratio,
            "freq_ratio": base_period / period,
            "speedup": (base_period / period) * ipc_ratio,
        })

    print(f"Suite: {args.suite}  (n={len(suite)} candidate tests)")
    print(f"Baseline: {args.baseline}  (period={base_period:.3f} ns, "
          f"freq={1000/base_period:.1f} MHz, IPC={geomean(base_ipc.values()):.4f})")
    print()
    format_table(rows)


if __name__ == "__main__":
    main()
