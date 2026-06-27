"""Render a ChiaProfileCollector JSONL log as an interactive HTML timeline.

Parses the same profiler events as :mod:`profile_viz` but outputs a
self-contained HTML file with a Canvas 2D timeline that supports zoom,
pan, hover tooltips, click-to-inspect, and per-node filtering.

Usage::

    from chia.trace.profile_viz_html import render_profile_html
    render_profile_html("ChiaProfileCollector.log", "output.html")

Or via CLI::

    chia viz-profile ChiaProfileCollector.log --format html
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from chia.trace.profile_viz import (
    ProfiledCall,
    _assign_rows,
    _assign_rows_grouped,
    _call_display_name,
    _node_id_color_map,
    attach_tool_info,
    infer_data_edges,
    infer_nesting_edges,
    load_events,
    pair_events,
    segment_runs,
)


def _find_unpaired_dispatches(run_events: list[dict]) -> list[dict]:
    """Return start events for tasks that were dispatched but never completed."""
    pending: dict[tuple[str, str], list[dict]] = {}
    for evt in run_events:
        etype = evt.get("type", "")
        key = (evt.get("call_id", ""), evt.get("func", ""))
        if etype in ("dispatch", "local_start"):
            pending.setdefault(key, []).append(evt)
        elif etype in ("complete", "local_end"):
            queue = pending.get(key, [])
            if queue:
                queue.pop(0)
                if not queue:
                    del pending[key]
    return [evt for events in pending.values() for evt in events]


def _serialize_call(
    call: ProfiledCall, min_ts: float, row: int, collapsed_row: int,
) -> dict:
    return {
        "uid": call.uid,
        "call_id": call.call_id,
        "func": call.func,
        "display_name": _call_display_name(call),
        "dispatch_ts": round(call.dispatch_ts - min_ts, 4),
        "complete_ts": round(call.complete_ts - min_ts, 4),
        "exec_time_s": round(call.exec_time_s, 6),
        "worker_ip": call.worker_ip,
        "worker_id": call.worker_id,
        "node_id": call.node_id,
        "is_remote": call.is_remote,
        "resources": call.resources,
        "tools": call.tools,
        "extra": call.extra,
        "row": row,
        "collapsed_row": collapsed_row,
    }


def render_profile_html(
    log_path: str,
    output_path: str,
    run_index: Optional[int] = None,
    gap_threshold: float = 600.0,
) -> None:
    """Parse a profiler JSONL log and render an interactive HTML timeline.

    Args:
        log_path: Path to the ChiaProfileCollector JSONL log.
        output_path: Output file path (e.g. ``output.html``).
        run_index: Which run segment to visualize (0-indexed).
            ``None`` means the last run.
        gap_threshold: Timestamp gap in seconds to segment runs.
    """
    events = load_events(log_path)
    if not events:
        print(f"No events found in {log_path}")
        return

    runs = segment_runs(events, gap_threshold)
    if not runs:
        print(f"No runs found in {log_path}")
        return

    # When no run_index is specified, process all events as one run so that
    # dispatch/complete pairs are matched even when long-running tasks cause
    # their events to span multiple gap-separated segments.
    if run_index is None:
        run_events = events
        run_index = 0
    else:
        if run_index < 0 or run_index >= len(runs):
            print(f"Run index {run_index} out of range (0-{len(runs) - 1})")
            return
        run_events = runs[run_index]

    calls = pair_events(run_events, run_index)

    unpaired = _find_unpaired_dispatches(run_events)
    if unpaired:
        print(
            f"Note: {len(unpaired)} task(s) dispatched but not yet completed "
            f"(excluded from visualization):"
        )
        for evt in unpaired:
            print(f"  - {evt.get('func', '?')}  (call_id: {evt.get('call_id', '?')})")

    if not calls:
        print(f"No completed calls found in run {run_index}")
        return

    attach_tool_info(calls, run_events)
    data_edges = infer_data_edges(calls)
    nesting_edges = infer_nesting_edges(calls)

    # Prefer nesting edges over data edges for the same pair (more informative)
    nesting_set = set(nesting_edges)
    data_edges = [(s, t) for s, t in data_edges if (s, t) not in nesting_set]

    nid_colors = _node_id_color_map(calls)
    row_assignment, groups = _assign_rows_grouped(calls, nesting_edges)

    # Collapsed view: only root-level calls (not a child in any nesting edge).
    child_uids = {c_uid for _, c_uid in nesting_edges}
    root_calls = [c for c in calls if c.uid not in child_uids]
    collapsed_assignment = _assign_rows(root_calls)

    min_ts = min(c.dispatch_ts for c in calls)
    max_ts_rel = max(c.complete_ts - min_ts for c in calls)

    calls_json = [
        _serialize_call(
            c, min_ts, row_assignment[c.uid],
            collapsed_assignment.get(c.uid, -1),
        )
        for c in calls
    ]

    data = {
        "meta": {
            "log_file": log_path,
            "run_index": run_index,
            "num_runs": len(runs),
            "total_calls": len(calls),
            "time_range": [0.0, round(max_ts_rel, 4)],
        },
        "calls": calls_json,
        "data_edges": list(data_edges),
        "nesting_edges": list(nesting_edges),
        "node_colors": nid_colors,
        "groups": groups,
    }

    # Escape </script> to prevent early tag close inside the JSON block
    data_json = json.dumps(data, separators=(",", ":"), default=str)
    data_json = data_json.replace("</script>", "<\\/script>")

    template_path = Path(__file__).parent / "profile_template.html"
    template = template_path.read_text(encoding="utf-8")
    html = template.replace("{{DATA_JSON}}", data_json)

    Path(output_path).write_text(html, encoding="utf-8")
    print(f"Interactive profile written to {output_path}")
