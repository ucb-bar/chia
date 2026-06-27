#!/usr/bin/env python3
"""Run baremetal microbenchmarks on gem5 and compare against cached Verilator goldens.

Trimmed to exactly what the gem5_align alignment flow uses: gem5 is run on a
caller-supplied benchmark subset and compared against pre-staged Verilator golden
logs (``--gem5-only``). It does **not** build benchmarks or run Verilator itself;
the loop produces the golden cache separately and stages the logs under
``<results-dir>/verilator/`` before invoking this script.

All paths are supplied explicitly by the caller (``--gem5-bin``, ``--gem5-config``,
``--baremetal-dir``, ...);

Outputs:
- JSON (``compare_results.json``) with full per-benchmark data.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Canonical single-workload gem5 run node: builds the command, runs gem5, and
# parses stats.txt into a Gem5RunResult.  Called in-process here (a local
# @ChiaFunction call, the same pattern Gem5ToolServer uses) so the gem5 side of
# this script reuses the canonical runner instead of hand-rolling it.
from chia.simulators.gem5 import Gem5Node


def parse_verilator_metrics(log_text: str) -> Tuple[Optional[int], Optional[int], str]:
    # Preferred ROI print: "ROI cycles=<N> instret=<N> ..."
    pattern = r"\s+cycles=(\d+)\r?\n\s+instret=(\d+)"
    inst: Optional[int] = None
    cyc: Optional[int] = None

    match = re.search(pattern, log_text)
    if match:
        cyc = int(match.group(1))
        inst = int(match.group(2))

    if inst is None and cyc is None:
        return None, None, "failed to parse verilator metrics from log"
    return inst, cyc, ""


@dataclass
class RunResult:
    benchmark: str
    benchmark_notes: str
    error_messages: str
    gem5_status: str
    verilator_status: str
    gem5_simInst: Optional[int]
    gem5_numCycles: Optional[int]
    verilator_simInst: Optional[int]
    verilator_numCycles: Optional[int]
    cycle_pct_diff: Optional[float]
    delta_abs_simInst: Optional[float]
    gem5_over_verilator_cycle_ratio: Optional[float]
    gem5_wall_s: Optional[float] = None
    gem5_host_seconds: Optional[float] = None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "benchmark": self.benchmark,
            "percentage_diff": self.cycle_pct_diff,
            "verilator_numCycles": self.verilator_numCycles,
            "gem5_numCycles": self.gem5_numCycles,
            "gem5/ver_cycle_ratio": self.gem5_over_verilator_cycle_ratio,
            "verilator_simInst": self.verilator_simInst,
            "gem5_insDelta": self.delta_abs_simInst,
            "verilator_status": self.verilator_status,
            "gem5_status": self.gem5_status,
            "benchmark_notes": self.benchmark_notes,
            "error_messages": self.error_messages,
            "gem5_wall_s": self.gem5_wall_s,
            "gem5_host_seconds": self.gem5_host_seconds,
        }


def _compute_delta(gem5_v: Optional[float], ver_v: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    if gem5_v is None or ver_v is None:
        return None, None
    abs_d = gem5_v - ver_v
    rel_d = None if ver_v == 0 else abs_d / ver_v
    return abs_d, rel_d


def _read_benchmark_note(microbench_dir: Path, bench: str) -> str:
    desc = microbench_dir / bench / "desc.txt"
    if not desc.exists():
        return ""
    # Keep desc as a single line in the JSON row.
    text = desc.read_text().strip()
    return " ".join(text.split())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gem5-bin", type=Path, required=True)
    parser.add_argument("--gem5-config", type=Path, required=True)
    parser.add_argument(
        "--baremetal-dir",
        type=Path,
        required=True,
        help="Benchmark workspace root (holds build/ubench, microbench, results).",
    )
    parser.add_argument(
        "--build-ubench-dir",
        type=Path,
        default=None,
        help="Defaults to <baremetal-dir>/build/ubench.",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=None,
        help="Defaults to <baremetal-dir>/results.",
    )
    parser.add_argument(
        "--gem5-debug-flags",
        default="",
        help="Comma-separated gem5 debug flags (e.g. 'O3PipeView'). Inserted before the config.py.",
    )
    parser.add_argument(
        "--gem5-debug-file",
        default="",
        help="gem5 --debug-file value (relative to --outdir). '.gz' suffix enables gzip compression.",
    )
    parser.add_argument(
        "--memory-backend",
        choices=["ideal", "match_dram", "dramsim2"],
        default="ideal",
        help="Memory backend selector forwarded to the gem5 config.py.",
    )
    parser.add_argument(
        "--benchmarks",
        nargs="*",
        default=[],
        help="Benchmark list (required in practice; the caller always supplies it).",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=1,
        help="Number of benchmarks to run in parallel.",
    )
    # Accepted for command-line compatibility with the alignment flow's callers.
    # This trimmed script never builds and always loads Verilator metrics from the
    # pre-staged golden logs, so both flags are effectively always-on no-ops.
    parser.add_argument("--gem5-only", action="store_true")
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()

    baremetal_dir = args.baremetal_dir.resolve()
    build_ubench_dir = (
        args.build_ubench_dir.resolve()
        if args.build_ubench_dir is not None
        else (baremetal_dir / "build/ubench")
    )
    microbench_dir = baremetal_dir / "microbench"
    gem5_config = args.gem5_config.resolve()
    results_dir = (
        args.results_dir.resolve()
        if args.results_dir is not None
        else (baremetal_dir / "results")
    )

    gem5_bin = args.gem5_bin.resolve()
    if not gem5_bin.exists():
        print(f"gem5 binary not found: {gem5_bin}. Use --gem5-bin to set it explicitly.")
        return 2

    results_dir.mkdir(parents=True, exist_ok=True)
    gem5_logs_dir = results_dir / "gem5"
    ver_logs_dir = results_dir / "verilator"
    gem5_logs_dir.mkdir(parents=True, exist_ok=True)
    ver_logs_dir.mkdir(parents=True, exist_ok=True)

    benches = args.benchmarks
    if not benches:
        print("No benchmarks supplied. Pass --benchmarks.")
        return 2

    # Logical stat name -> candidate stats.txt keys (first present wins),
    # forwarded to Gem5Node.run_gem5 which fills num_cycles / sim_insts from them.
    gem5_inst_keys = ["simInsts", "simInst", "system.cpu.committedInsts"]
    gem5_cycle_keys = ["system.cpu.numCycles", "numCycles"]

    states: Dict[str, Dict[str, Any]] = {}
    for b in benches:
        states[b] = {
            "benchmark_notes": _read_benchmark_note(microbench_dir, b),
            "gem5_status": "pending...",
            "verilator_status": "pending...",
            "gem5_simInst": None,
            "gem5_numCycles": None,
            "verilator_simInst": None,
            "verilator_numCycles": None,
            "gem5_wall_s": None,
            "gem5_host_seconds": None,
            "notes": [],
        }

    def run_gem5_job(bench: str) -> Dict[str, Any]:
        gem5_bench = build_ubench_dir / f"{bench}.gem5.elf"
        gem5_outdir = gem5_logs_dir / bench
        gem5_outdir.mkdir(parents=True, exist_ok=True)
        notes: List[str] = []

        # Everything AFTER the config .py is a config-script arg: the kernel ELF
        # and the memory-backend selector.
        config_args = ["--kernel", str(gem5_bench), "--memory-backend", args.memory_backend]

        # Canonical single-workload gem5 run node. In-process local call (no Ray
        # dispatch): builds the gem5 command, runs it, and parses stats.txt.
        res = Gem5Node.run_gem5(
            str(gem5_bin),
            str(gem5_config),
            str(gem5_outdir),
            workload_name=bench,
            config_args=config_args,
            debug_flags=(args.gem5_debug_flags or None),
            debug_file=(args.gem5_debug_file or None),
            stats_keys={"insts": gem5_inst_keys, "cycles": gem5_cycle_keys},
            stats_block="first",
            cwd=str(baremetal_dir.parent.parent),
            timeout_s=None,
        )

        # Mirror gem5's (truncated) stdout to the per-bench log so a failed run
        # still leaves something inspectable alongside stats.txt.
        (gem5_outdir / "stdout.log").write_text(
            f"$ gem5 {bench}  (via chia.simulators.gem5.Gem5Node.run_gem5)\n\n"
            f"{res.stdout_tail}\n"
        )

        if res.error_messages:
            notes.append(f"gem5: {res.error_messages}")
        return {
            "bench": bench,
            "status": res.status,
            "inst": res.sim_insts,
            "cyc": res.num_cycles,
            "wall_s": res.wall_s,
            "host_seconds": res.host_seconds,
            "notes": notes,
        }

    def load_verilator_golden(bench: str) -> Dict[str, Any]:
        ver_log = ver_logs_dir / f"{bench}.log"
        notes: List[str] = []
        if not ver_log.exists():
            return {
                "bench": bench,
                "status": "missing_previous_log",
                "inst": None,
                "cyc": None,
                "notes": [f"missing previous verilator log: {ver_log}"],
            }

        v_inst, v_cyc, v_err = parse_verilator_metrics(ver_log.read_text())
        status = "from_last_run"
        if v_err:
            notes.append(f"verilator parse: {v_err}")
            status = "parse_failed_previous_log"

        return {
            "bench": bench,
            "status": status,
            "inst": v_inst,
            "cyc": v_cyc,
            "notes": notes,
        }

    jobs = max(1, args.jobs)
    futures: List[concurrent.futures.Future] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as pool:
        for b in benches:
            gem5_bench = build_ubench_dir / f"{b}.gem5.elf"
            if gem5_bench.exists():
                futures.append(pool.submit(run_gem5_job, b))
            else:
                states[b]["gem5_status"] = "missing_binary"
                states[b]["notes"].append(f"missing {gem5_bench}")
            # Verilator metrics always come from the pre-staged golden logs.
            res = load_verilator_golden(b)
            states[b]["verilator_status"] = res["status"]
            states[b]["verilator_simInst"] = res["inst"]
            states[b]["verilator_numCycles"] = res["cyc"]
            states[b]["notes"].extend(res["notes"])

        for fut in concurrent.futures.as_completed(futures):
            res = fut.result()
            b = res["bench"]
            s = states[b]
            s["gem5_status"] = res["status"]
            s["gem5_simInst"] = res["inst"]
            s["gem5_numCycles"] = res["cyc"]
            s["gem5_wall_s"] = res.get("wall_s")
            s["gem5_host_seconds"] = res.get("host_seconds")
            s["notes"].extend(res["notes"])
            print(f"[{b}] gem5={s['gem5_status']} verilator={s['verilator_status']}")

    rows: List[RunResult] = []
    for b in benches:
        s = states[b]
        d_inst_abs, _d_inst_rel = _compute_delta(
            s["gem5_simInst"], s["verilator_simInst"]
        )
        cycle_pct_diff = None
        if (
            s["gem5_numCycles"] is not None
            and s["verilator_numCycles"] is not None
        ):
            cyc_avg = (s["gem5_numCycles"] + s["verilator_numCycles"]) / 2
            if cyc_avg != 0:
                cycle_pct_diff = round(
                    (abs(s["gem5_numCycles"] - s["verilator_numCycles"]) / cyc_avg)
                    * 100,
                    4,
                )
        gem5_over_verilator_ratio = None
        if (
            s["gem5_numCycles"] is not None
            and s["verilator_numCycles"] is not None
            and s["verilator_numCycles"] != 0
        ):
            gem5_over_verilator_ratio = round(
                s["gem5_numCycles"] / s["verilator_numCycles"], 4
            )
        rows.append(
            RunResult(
                benchmark=b,
                error_messages="; ".join(s["notes"]),
                cycle_pct_diff=cycle_pct_diff,
                gem5_over_verilator_cycle_ratio=gem5_over_verilator_ratio,
                verilator_numCycles=s["verilator_numCycles"],
                gem5_numCycles=s["gem5_numCycles"],
                verilator_simInst=s["verilator_simInst"],
                gem5_simInst=s["gem5_simInst"],
                delta_abs_simInst=d_inst_abs,
                verilator_status=s["verilator_status"],
                gem5_status=s["gem5_status"],
                benchmark_notes=s["benchmark_notes"],
                gem5_wall_s=s.get("gem5_wall_s"),
                gem5_host_seconds=s.get("gem5_host_seconds"),
            )
        )

    def sort_key(r: RunResult) -> Tuple[int, float]:
        if r.cycle_pct_diff is None:
            return (1, -1.0)
        return (0, r.cycle_pct_diff)

    rows.sort(key=sort_key, reverse=True)

    out_json = results_dir / "compare_results.json"
    out_json.write_text(json.dumps([r.as_dict() for r in rows], indent=2))

    print(f"Wrote: {out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
