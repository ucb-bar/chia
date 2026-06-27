"""Render a ChiaProfileCollector JSONL log as a dependency graph.

Parses dispatch/complete event pairs, infers data-dependency and
control-flow (nesting) edges, and renders via graphviz with node
width proportional to execution time.

Usage::

    from chia.trace.profile_viz import render_profile
    render_profile("ChiaProfileCollector.log", "output.svg")

Or via CLI::

    chia viz-profile ChiaProfileCollector.log
"""

from __future__ import annotations

import json
import html
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import graphviz


# -- Data structures ----------------------------------------------------------

@dataclass
class ProfiledCall:
    """A paired dispatch+complete representing one function execution."""
    uid: str
    call_id: str
    func: str
    dispatch_ts: float
    complete_ts: float
    exec_time_s: float
    worker_ip: str
    worker_id: str
    node_id: str
    caller_worker_id: str
    is_remote: bool
    resources: dict = field(default_factory=dict)
    obj_ref_deps: list[str] = field(default_factory=list)
    tools: list[dict] = field(default_factory=list)
    extra: dict = field(default_factory=dict)
    display_name: str = ""


# -- Parsing ------------------------------------------------------------------

def load_events(log_path: str) -> list[dict]:
    """Read JSONL log, return list of event dicts sorted by ts."""
    events = []
    with open(log_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    events.sort(key=lambda e: e.get("ts", 0))
    return events


def segment_runs(events: list[dict], gap_threshold: float = 600.0) -> list[list[dict]]:
    """Split sorted events into run segments based on timestamp gaps."""
    if not events:
        return []
    runs: list[list[dict]] = [[events[0]]]
    for evt in events[1:]:
        if evt["ts"] - runs[-1][-1]["ts"] > gap_threshold:
            runs.append([])
        runs[-1].append(evt)
    return runs


def pair_events(run_events: list[dict], run_index: int) -> list[ProfiledCall]:
    """Match dispatch/complete and local_start/local_end pairs.

    Handles call_id reuse by keying on ``(call_id, func)`` with a FIFO
    queue of pending start events.
    """
    # Pending start events: (call_id, func) -> [event, ...]
    pending: dict[tuple[str, str], list[dict]] = {}
    calls: list[ProfiledCall] = []
    seq = 0

    for evt in run_events:
        etype = evt.get("type", "")
        call_id = evt.get("call_id", "")
        func = evt.get("func", "")

        if etype in ("dispatch", "local_start"):
            key = (call_id, func)
            pending.setdefault(key, []).append(evt)

        elif etype in ("complete", "local_end"):
            key = (call_id, func)
            queue = pending.get(key, [])
            if not queue:
                continue
            start_evt = queue.pop(0)
            if not queue:
                del pending[key]

            uid = f"r{run_index}_{seq}"
            seq += 1

            calls.append(ProfiledCall(
                uid=uid,
                call_id=call_id,
                func=func,
                display_name=evt.get("display_name", start_evt.get("display_name", "")),
                dispatch_ts=start_evt["ts"],
                complete_ts=evt["ts"],
                exec_time_s=evt.get("exec_time_s", evt["ts"] - start_evt["ts"]),
                worker_ip=evt.get("worker_ip", start_evt.get("worker_ip", "unknown")),
                worker_id=evt.get("worker_id", start_evt.get("worker_id", "")),
                node_id=evt.get("node_id", start_evt.get("node_id", "")),
                caller_worker_id=start_evt.get("caller_worker_id", ""),
                is_remote=start_evt.get("is_remote", True),
                resources=start_evt.get("resources", {}),
                obj_ref_deps=start_evt.get("obj_ref_deps", []),
                extra=evt.get("extra", {}),
            ))

    return calls


def attach_tool_info(calls: list[ProfiledCall], run_events: list[dict]) -> None:
    """Attach tool locations from ``prompt`` events to enclosing calls.

    A ``prompt`` event is enclosed by call C if C.worker_id matches the
    event's worker_id and the event timestamp falls within C's time window.
    """
    prompt_events = [e for e in run_events if e.get("type") == "prompt"
                     and e.get("tools")]
    if not prompt_events:
        return

    by_worker: dict[str, list[ProfiledCall]] = {}
    for c in calls:
        by_worker.setdefault(c.worker_id, []).append(c)

    for evt in prompt_events:
        wid = evt.get("worker_id", "")
        ts = evt["ts"]
        for c in by_worker.get(wid, []):
            if c.dispatch_ts <= ts <= c.complete_ts:
                # Deduplicate tools by hostname
                existing = {t["hostname"] for t in c.tools}
                for tool in evt["tools"]:
                    if tool.get("hostname") and tool["hostname"] not in existing:
                        c.tools.append(tool)
                        existing.add(tool["hostname"])
                break


# -- Edge inference -----------------------------------------------------------

def infer_data_edges(calls: list[ProfiledCall]) -> list[tuple[str, str]]:
    """Return ``(source_uid, target_uid)`` edges from ``obj_ref_deps``.

    Since ``call_id`` can be reused within a run (profiler counter resets),
    we match each dependency to the call with the same ``call_id`` that
    completed most recently before the dependent call's dispatch.
    """
    # Group calls by call_id, sorted by complete_ts.
    by_cid: dict[str, list[ProfiledCall]] = {}
    for c in calls:
        by_cid.setdefault(c.call_id, []).append(c)

    edges: list[tuple[str, str]] = []
    for c in calls:
        for dep_cid in c.obj_ref_deps:
            candidates = by_cid.get(dep_cid, [])
            # Find the candidate that completed most recently before c's dispatch.
            best = None
            for cand in candidates:
                if cand.uid == c.uid:
                    continue
                if cand.complete_ts <= c.dispatch_ts:
                    if best is None or cand.complete_ts > best.complete_ts:
                        best = cand
            if best is not None:
                edges.append((best.uid, c.uid))
    return edges


def infer_nesting_edges(calls: list[ProfiledCall]) -> list[tuple[str, str]]:
    """Return ``(parent_uid, child_uid)`` edges based on ``caller_worker_id``.

    A call C was spawned by call P if P.worker_id == C.caller_worker_id
    and P's execution time window encloses C's dispatch timestamp.
    """
    # Group calls by worker_id for fast lookup.
    by_worker: dict[str, list[ProfiledCall]] = {}
    for c in calls:
        by_worker.setdefault(c.worker_id, []).append(c)

    edges: list[tuple[str, str]] = []
    for c in calls:
        cwid = c.caller_worker_id
        if not cwid:
            continue
        candidates = by_worker.get(cwid, [])
        for p in candidates:
            if p.uid == c.uid:
                continue
            # P was executing when C was dispatched.
            if p.dispatch_ts <= c.dispatch_ts <= p.complete_ts:
                edges.append((p.uid, c.uid))
                break  # innermost enclosing parent
    return edges


# -- Rendering ----------------------------------------------------------------

_ROW_HEIGHT = 0.8        # inches per lane
_ROW_SPACING = 1.0       # inches between lane centers
_MIN_WIDTH = 0.6         # minimum box width in inches (for very short calls)
_X_INCHES_PER_SEC = 0.5  # horizontal inches per second of wall-clock time
_MIN_GRAPH_WIDTH = 10.0  # minimum graph width in inches

_NODE_PALETTE = [
    "#A8D8EA", "#A8E6CF", "#FFE0AC", "#FFB7B2",
    "#C3B1E1", "#B5EAD7", "#E2F0CB", "#FFDAC1",
]


def _call_display_name(call: ProfiledCall) -> str:
    """Return the label shown in visualizations for a profiled call."""
    return call.display_name or call.func


def _node_id_color_map(calls: list[ProfiledCall]) -> dict[str, str]:
    """Assign a unique fill color to each distinct node_id."""
    node_ids = sorted(set(c.node_id for c in calls if c.node_id))
    return {nid: _NODE_PALETTE[i % len(_NODE_PALETTE)] for i, nid in enumerate(node_ids)}


def _compute_width(exec_time_s: float, x_scale: float) -> float:
    """Map exec_time to box width in inches using the X scale."""
    return max(_MIN_WIDTH, exec_time_s * x_scale)


def _assign_rows(calls: list[ProfiledCall]) -> dict[str, int]:
    """Assign each call to the first available row (greedy, by dispatch_ts)."""
    sorted_calls = sorted(calls, key=lambda c: c.dispatch_ts)
    row_free_at: list[float] = []  # each entry = complete_ts of last box in that row
    assignment: dict[str, int] = {}
    for call in sorted_calls:
        placed = False
        for row_idx, free_ts in enumerate(row_free_at):
            if call.dispatch_ts >= free_ts:
                assignment[call.uid] = row_idx
                row_free_at[row_idx] = call.complete_ts
                placed = True
                break
        if not placed:
            assignment[call.uid] = len(row_free_at)
            row_free_at.append(call.complete_ts)
    return assignment


def _collect_descendants(
    root_uid: str,
    children_of: dict[str, list[str]],
    by_uid: dict[str, ProfiledCall],
) -> list[ProfiledCall]:
    """BFS from *root_uid* to collect the root and all its descendants."""
    from collections import deque
    group: list[ProfiledCall] = []
    queue = deque([root_uid])
    visited: set[str] = set()
    while queue:
        uid = queue.popleft()
        if uid in visited or uid not in by_uid:
            continue
        visited.add(uid)
        group.append(by_uid[uid])
        queue.extend(children_of.get(uid, []))
    return group


def _find_contiguous_free(
    row_free_at: list[float], start_ts: float, n_rows: int,
) -> int:
    """Return the lowest base row where *n_rows* contiguous rows are free."""
    base = 0
    while True:
        all_free = True
        for r in range(base, base + n_rows):
            if r < len(row_free_at) and start_ts < row_free_at[r]:
                all_free = False
                base = r + 1
                break
        if all_free:
            return base


def _assign_rows_grouped(
    calls: list[ProfiledCall],
    nesting_edges: list[tuple[str, str]],
) -> tuple[dict[str, int], list[dict]]:
    """Assign rows treating each group as a multi-row block.

    A group (parent + all descendants) is packed internally via greedy
    first-fit to determine how many rows it needs.  It is then treated
    as a single block with ``start = min(dispatch_ts)`` and
    ``end = max(complete_ts)`` and scheduled into the lowest contiguous
    free rows in a shared global row space.  Orphan calls are 1-row
    blocks in the same space.

    Returns ``(row_assignment, groups)`` where *groups* is a list of
    ``{"label": str, "start_row": int, "end_row": int, "root_uid": str}``.
    """
    if not calls:
        return {}, []

    by_uid: dict[str, ProfiledCall] = {c.uid: c for c in calls}

    # Build adjacency from nesting edges.
    children_of: dict[str, list[str]] = {}
    parent_of: dict[str, str] = {}
    for p_uid, c_uid in nesting_edges:
        children_of.setdefault(p_uid, []).append(c_uid)
        parent_of[c_uid] = p_uid

    # Identify group roots (has children) vs orphans (standalone).
    root_uids = [c.uid for c in calls if c.uid not in parent_of]

    # Build schedulable items: (start_ts, end_ts, n_rows, kind, payload)
    items: list[tuple] = []
    for uid in root_uids:
        if uid in children_of:
            group_calls = _collect_descendants(uid, children_of, by_uid)
            internal = _assign_rows(group_calls)
            n_rows = max(internal.values()) + 1 if internal else 1
            start = min(c.dispatch_ts for c in group_calls)
            end = max(c.complete_ts for c in group_calls)
            items.append((start, end, n_rows, "group",
                          (uid, group_calls, internal)))
        else:
            c = by_uid[uid]
            items.append((c.dispatch_ts, c.complete_ts, 1, "orphan", c))

    items.sort(key=lambda x: x[0])

    # Global greedy first-fit for contiguous row blocks.
    row_free_at: list[float] = []
    assignment: dict[str, int] = {}
    groups_meta: list[dict] = []

    for start, end, n_rows, kind, payload in items:
        base = _find_contiguous_free(row_free_at, start, n_rows)

        # Extend row_free_at if needed.
        while len(row_free_at) < base + n_rows:
            row_free_at.append(0.0)

        if kind == "group":
            root_uid, group_calls, internal = payload
            for c in group_calls:
                assignment[c.uid] = internal[c.uid] + base
            for r in range(base, base + n_rows):
                row_free_at[r] = end
            groups_meta.append({
                "label": _call_display_name(by_uid[root_uid]),
                "start_row": base,
                "end_row": base + n_rows - 1,
                "root_uid": root_uid,
            })
        else:
            assignment[payload.uid] = base
            row_free_at[base] = end

    return assignment, groups_meta


def _node_label(call: ProfiledCall, min_ts: float = 0.0) -> str:
    """Build an HTML-like label for a graphviz node."""
    t_rel = call.dispatch_ts - min_ts
    parts = [f'<<B>{html.escape(_call_display_name(call))}</B>']
    if call.display_name and call.display_name != call.func:
        parts.append(f'<BR/><FONT POINT-SIZE="8">{html.escape(call.func)}</FONT>')
    parts.append(f'<BR/><FONT POINT-SIZE="9">start: {t_rel:.2f}s  dur: {call.exec_time_s:.4f}s</FONT>')
    if call.node_id:
        parts.append(f'<BR/><FONT POINT-SIZE="9">{call.node_id[:12]}</FONT>')
    if call.resources:
        res_str = ", ".join(str(k) for k in call.resources)
        parts.append(f'<BR/><FONT POINT-SIZE="8">[{res_str}]</FONT>')
    if not call.is_remote:
        parts.append('<BR/><FONT POINT-SIZE="8">(local)</FONT>')
    if call.tools:
        for tool in call.tools:
            name = tool.get("name", "")
            tnid = tool.get("node_id", "")
            tnid_short = tnid[:12] if tnid else tool.get("hostname", "")
            parts.append(f'<BR/><FONT POINT-SIZE="8">tool: {name} @ {tnid_short}</FONT>')
    if call.extra.get("test_binary"):
        parts.append(f'<BR/><FONT POINT-SIZE="9">bench: {call.extra["test_binary"]}</FONT>')
    if call.extra.get("cost_usd") is not None or call.extra.get("input_tokens"):
        cost = call.extra.get("cost_usd")
        in_tok = call.extra.get("input_tokens")
        out_tok = call.extra.get("output_tokens")
        meta_parts = []
        if cost is not None:
            meta_parts.append(f"${cost:.2f}")
        if in_tok:
            meta_parts.append(f"{in_tok}in")
        if out_tok:
            meta_parts.append(f"{out_tok}out")
        if meta_parts:
            parts.append(f'<BR/><FONT POINT-SIZE="8">{" | ".join(meta_parts)}</FONT>')
    return "".join(parts) + ">"


def render_profile(
    log_path: str,
    output_path: str,
    fmt: str = "svg",
    run_index: Optional[int] = None,
    gap_threshold: float = 600.0,
    x_inches_per_sec: float = _X_INCHES_PER_SEC,
) -> None:
    """Parse a profiler JSONL log and render a Gantt-style dependency graph.

    The horizontal axis represents wall-clock time (left = earliest).
    Calls are packed into rows using greedy allocation.
    Color indicates the node_id that executed the call.

    Args:
        log_path: Path to the ChiaProfileCollector JSONL log.
        output_path: Output file path (e.g. ``output.svg``).
        fmt: Output format — ``svg``, ``png``, or ``pdf``.
        run_index: Which run segment to visualize (0-indexed).
            ``None`` means the last run.
        gap_threshold: Timestamp gap in seconds to segment runs.
        x_inches_per_sec: Horizontal inches per second of wall-clock time.
    """
    events = load_events(log_path)
    if not events:
        print(f"No events found in {log_path}")
        return

    runs = segment_runs(events, gap_threshold)
    if not runs:
        print(f"No runs found in {log_path}")
        return

    if run_index is None:
        run_index = len(runs) - 1
    if run_index < 0 or run_index >= len(runs):
        print(f"Run index {run_index} out of range (0-{len(runs) - 1})")
        return

    run_events = runs[run_index]
    calls = pair_events(run_events, run_index)
    if not calls:
        print(f"No paired calls found in run {run_index}")
        return

    attach_tool_info(calls, run_events)
    data_edges = infer_data_edges(calls)
    nesting_edges = infer_nesting_edges(calls)

    # Deduplicate: if both a data edge and nesting edge exist between the
    # same pair, keep only the nesting edge (it's more informative).
    nesting_set = set(nesting_edges)
    data_edges = [(s, t) for s, t in data_edges if (s, t) not in nesting_set]

    # -- Layout: X = wall-clock time, Y = greedy-packed rows --------------
    nid_colors = _node_id_color_map(calls)
    row_assignment = _assign_rows(calls)

    # X scaling: map wall-clock seconds to inches (time flows rightward).
    min_ts = min(c.dispatch_ts for c in calls)
    max_ts = max(c.complete_ts for c in calls)
    duration = max_ts - min_ts or 1.0
    x_scale = max(_MIN_GRAPH_WIDTH, duration * x_inches_per_sec) / duration

    num_rows = max(row_assignment.values()) + 1 if row_assignment else 1

    # -- Build graphviz (neato engine with pinned positions) ---------------
    dot = graphviz.Digraph(format=fmt, engine="neato")
    dot.attr(
        fontname="Helvetica",
        label=f"Chia Profile: run {run_index} ({len(calls)} calls)\\n{Path(log_path).name}",
        labelloc="t",
        fontsize="16",
        splines="true",
        overlap="true",
    )
    dot.attr("node", fontname="Helvetica", fontsize="10")
    dot.attr("edge", fontname="Helvetica", fontsize="9")

    # Nodes — pinned at (time_offset, -row)
    # Width is proportional to exec_time using the X scale.
    for call in calls:
        width = _compute_width(call.exec_time_s, x_scale)
        color = nid_colors.get(call.node_id, "#FFFFFF")
        row = row_assignment[call.uid]
        # Left edge of box aligns with dispatch_ts; center is offset by half width.
        x_left = (call.dispatch_ts - min_ts) * x_scale
        x = x_left + width / 2.0
        y = -row * _ROW_SPACING
        dot.node(
            call.uid,
            label=_node_label(call, min_ts),
            shape="box",
            style="filled,rounded",
            fillcolor=color,
            width=str(round(width, 2)),
            height=str(_ROW_HEIGHT),
            fixedsize="true",
            pos=f"{x},{y}!",
        )

    # Data dependency edges
    for src, dst in data_edges:
        dot.edge(src, dst, style="solid", color="black",
                 label="data", fontsize="8")

    # Nesting edges
    for parent, child in nesting_edges:
        dot.edge(parent, child, style="dashed", color="#4A90D9",
                 label="spawns", fontsize="8")

    # Legend — pinned below the main graph
    legend_y = -(num_rows * _ROW_SPACING + 1.0)
    for i, (nid, color) in enumerate(nid_colors.items()):
        leg_id = f"_leg_{i}"
        lx = i * 3.0
        dot.node(leg_id, label=nid[:12], shape="box", style="filled,rounded",
                 fillcolor=color, fontsize="9", width="1.5",
                 height="0.4", fixedsize="true",
                 pos=f"{lx},{legend_y}!")

    # Render
    out = str(output_path)
    if out.endswith(f".{fmt}"):
        out = out[: -(len(fmt) + 1)]
    dot.render(out, cleanup=True)
    print(f"Profile graph written to {out}.{fmt}")
