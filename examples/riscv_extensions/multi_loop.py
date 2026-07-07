"""VEXT outer driver — fan out one pipeline per extension, archive each to the
durable log DB, and summarize. Owns placement/parallelism/archival; the inner
loop (single_loop.run_vext_loop) owns one extension's work (field guide §2).
"""

from __future__ import annotations

import argparse
import datetime
import io
import os
import sys
import tarfile
import uuid
from concurrent.futures import ThreadPoolExecutor

_VEXT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(_VEXT_DIR, "..", "..")))   # repo root (chia)
sys.path.insert(0, os.path.abspath(os.path.join(_VEXT_DIR, "..")))          # examples/ (riscv_extensions, timing_opt, common)

import ray

from chia.base.ChiaFunction import get
from chia.trace.profiler import start_collector

import riscv_extensions.db_node as db_node
from riscv_extensions.constants import (
    EXTENSIONS,
    LLM_EXTRA_ARGS,
    LLM_MODEL,
    VEXT_LOG_ROOT,
    sky130_vlsi_runtime_env,
)
from riscv_extensions.single_loop import VextResult, run_vext_loop

MAX_PARALLEL_PIPELINES = 4

# Code+prompt+spec snapshot archived per sweep for reproducibility (field §11).
_SNAPSHOT_EXCLUDE = ("__pycache__", ".pyc")


def _tar_dir(path: str) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as t:
        t.add(path, arcname=".",
              filter=lambda ti: None if any(x in ti.name for x in _SNAPSHOT_EXCLUDE) else ti)
    return buf.getvalue()


def _render_summary(result: VextResult, sweep_n: int) -> str:
    verdict = "✓ converged" if result.converged else "✗ did not converge"
    return (
        f"# VEXT sweep_{sweep_n} — {result.extension}\n\n"
        f"- model: `{LLM_MODEL}` {LLM_EXTRA_ARGS}\n"
        f"- result: **{verdict}**\n"
        f"- iterations: {result.iterations}\n"
        f"- tests passing: {result.num_pass}/{result.num_tests}\n"
    )


def _render_ppa(results) -> str:
    """User-facing PPA summary (same shape as the synth-only rerun): baseline Total
    Area + worst slack, then each extension's delta vs baseline. Display only — the
    per-run ppa.md / events are unchanged."""
    rs = [r for r in results if r.baseline_area is not None and r.impl_area is not None]
    if not rs:
        return ""
    def slk(s):
        return f"{s:.3f} ns ({'VIOLATED' if s < 0 else 'MET'})" if s is not None else "N/A"
    out = ["\nPPA (Total Area = Cell + Net, incl. SRAM macros):\n",
           f"\n## baseline\n- total_area: {rs[0].baseline_area:,.2f}\n- slack: {slk(rs[0].baseline_slack)}\n"]
    for r in rs:
        ba, bs, ca, cs = r.baseline_area, r.baseline_slack, r.impl_area, r.impl_slack
        da = f"{ca - ba:+,.2f} ({(ca - ba) / ba * 100:+.1f}%)" if ba else "N/A"
        ds = f"{cs - bs:+.3f} ns" if (cs is not None and bs is not None) else "N/A"
        out.append(f"\n## {r.extension}\n- total_area: {ca:,.2f}  (Δ vs baseline: {da})\n"
                   f"- slack: {slk(cs)}  (Δ: {ds})\n")
    return "".join(out)


def _run_one(ext_name: str, ts: str, seed_diff: str | None = None, synth: bool = True,
             prebuilt: bool = False):
    """One extension: claim a sweep, snapshot src, run the inner loop, then archive
    whatever exists. Best-effort and self-contained: a crash — or nodes that never
    produced anything (missing/empty work_root, DB down) — yields (None, sweep_path)
    with a FAILED summary, never an exception, so it can't abort a sibling pipeline
    or the profiler archival."""
    ext = EXTENSIONS[ext_name]
    run_id = f"{ts}-{ext_name}-{uuid.uuid4().hex[:6]}"
    tag = run_id.rsplit("-", 1)[-1]
    work_root = os.path.join(VEXT_LOG_ROOT, ext_name, run_id)
    sweep_n = sweep_path = result = None
    try:
        sweep_n, sweep_path = get(db_node.claim_sweep.chia_remote(ext_name))
        get(db_node.archive_dir.chia_remote(sweep_path, "src", _tar_dir(_VEXT_DIR), extension=ext_name))
        result = run_vext_loop(ext, run_id, work_root,
                               seed_diff=seed_diff, archive_dir=sweep_path, synth=synth,
                               prebuilt=prebuilt)
    except Exception:
        import traceback
        print(f"[{ext_name}] pipeline FAILED:\n{traceback.format_exc()}", flush=True)
    if sweep_path:                          # archive what exists; skip cleanly if nothing was produced
        try:
            get(db_node.pool_finalize.chia_remote(db_node.pool_path(run_id), extension=ext_name))
            if os.path.isdir(work_root) and os.listdir(work_root):
                get(db_node.archive_dir.chia_remote(sweep_path, f"{ext_name}-{tag}", _tar_dir(work_root), extension=ext_name))
            get(db_node.write_text.chia_remote(sweep_path, "summary.md", _render_summary(result, sweep_n)
                if result else f"# {ext_name} sweep_{sweep_n}: FAILED before completion\n", extension=ext_name))
        except Exception as e:
            print(f"[{ext_name}] archive incomplete: {type(e).__name__}: {e}", flush=True)
    print(f"[{ext_name}] sweep_{sweep_n} -> {sweep_path}: {result}", flush=True)
    return result, sweep_path


def _parse_args():
    p = argparse.ArgumentParser(description="VEXT outer driver — sweep over extensions.")
    p.add_argument("--extensions", nargs="+", default=["bitmanip"],
                   choices=sorted(EXTENSIONS))
    p.add_argument("--seed-diff", default=None,
                   help="probe diff from a prior run to seed BOOM with (resume; "
                        "meant for single-extension runs)")
    p.add_argument("--no-synth", action="store_true",
                   help="skip sky130 PPA synthesis (run on a cluster with no synth node)")
    p.add_argument("--prebuilt-stress", action="store_true",
                   help="seed the stress pool from committed prebuilt binaries instead of "
                        "generating with riscv-dv (run on a cluster with no Xcelium license)")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    os.makedirs(VEXT_LOG_ROOT, exist_ok=True)

    ray.init(address="auto", runtime_env=sky130_vlsi_runtime_env())
    os.chdir(VEXT_LOG_ROOT)                          # one process-wide chdir (§9)
    profiler_dir = os.path.join(VEXT_LOG_ROOT, f"profiler_{ts}")
    start_collector(log_dir=profiler_dir)            # no namespace (§10)

    seed = open(args.seed_diff).read() if args.seed_diff else None
    n = min(MAX_PARALLEL_PIPELINES, len(args.extensions))
    with ThreadPoolExecutor(max_workers=n) as ex:
        futures = [ex.submit(_run_one, e, ts, seed, not args.no_synth, args.prebuilt_stress)
                   for e in args.extensions]
        outcomes = [f.result() for f in futures]   # _run_one never raises; (None, sweep_path) on failure

    # Ship the profiler into every claimed sweep, best-effort — _run_one returns
    # instead of raising, so this runs even when a pipeline failed.
    profiler_tar = _tar_dir(profiler_dir)
    for _result, sweep_path in outcomes:
        if not sweep_path:
            continue
        try:
            get(db_node.archive_dir.chia_remote(sweep_path, "profiler", profiler_tar))
        except Exception as e:
            print(f"profiler archive skipped for {sweep_path}: {e}", flush=True)

    results = [r for r, _ in outcomes if r is not None]
    converged = sum(1 for r in results if r.converged)
    print(f"\nVEXT sweep done: {converged}/{len(args.extensions)} extensions converged")
    for r in results:
        print(f"  {r.extension}: {'OK' if r.converged else 'INCOMPLETE'} "
              f"({r.num_pass}/{r.num_tests} tests, {r.iterations} iters)")
    ppa_summary = _render_ppa(results)
    if ppa_summary:
        print(ppa_summary)

    from chia.trace.profiler import stop_collector
    stop_collector()
    return 0 if converged == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
