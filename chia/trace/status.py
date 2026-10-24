"""Live status tables for work spread over the cluster.

Any worker calls :func:`report` to set the fields of one row of a board; the
rows live in one actor on the head, which :func:`watch` creates. :func:`watch`
prints a board as a table every few seconds from the calling process (the
driver), so the table goes to the job output as one block, not through Ray's
per-line worker log forwarding. With no process watching, :func:`report` does
nothing.

    watch("FPGA")                                                               # driver
    report("FPGA", "ip-172-31-5-9", job="spec-403.gcc", state="runworkload")   # any worker

A row whose ``state`` is ``done`` or ``failed`` is finished; a board is printed
while any row is not, and once more when the last one finishes.
"""

from __future__ import annotations

import threading
import time

import ray

_NAME = "ChiaStatusBoard"
_FINAL = ("done", "failed")
_board_handle = None
_watched: set[str] = set()


class StatusBoard:
    """The latest fields of every row, and when each row's state last changed."""

    def __init__(self):
        self._boards: dict[str, dict[str, tuple[dict, float]]] = {}

    def update(self, board: str, key: str, fields: dict) -> None:
        rows = self._boards.setdefault(board, {})
        old, since = rows.get(key, ({}, 0.0))
        if fields.get("state", old.get("state")) != old.get("state"):
            since = time.time()
        rows[key] = ({**old, **fields}, since or time.time())

    def rows(self, board: str) -> dict[str, tuple[dict, float]]:
        return dict(self._boards.get(board, {}))


def report(board: str, key: str, **fields) -> None:
    """Set ``fields`` on row ``key`` of ``board``; does not wait. Only :func:`watch`
    creates the board, so with no process watching this does nothing."""
    try:
        handle = ray.get_actor(_NAME)
    except ValueError:
        return
    handle.update.remote(board, key, fields)


def watch(board: str, interval_s: float = 10) -> None:
    """Print ``board`` every ``interval_s`` seconds from a daemon thread of this process."""
    if board not in _watched:
        _watched.add(board)
        _board()
        threading.Thread(target=_watch, args=(board, interval_s), daemon=True).start()


def unwatch(board: str) -> None:
    """Stop printing ``board`` in this process."""
    _watched.discard(board)


def _board():
    # One board per job: a named actor lives in its job's namespace. The process
    # that creates it owns it, so only watch() calls this.
    global _board_handle
    if _board_handle is None:
        _board_handle = ray.remote(StatusBoard).options(
            name=_NAME, get_if_exists=True, num_cpus=0,
            resources={"node:__internal_head__": 0.001}).remote()
    return _board_handle


def _watch(board: str, interval_s: float) -> None:
    last = None
    while True:
        time.sleep(interval_s)
        if board not in _watched:
            return
        if not ray.is_initialized():
            continue
        rows = ray.get(_board().rows.remote(board))
        active = any(f.get("state") not in _FINAL for f, _ in rows.values())
        if rows and (active or rows != last):
            print(_render(board, rows), flush=True)
        last = rows


def _render(board: str, rows: dict[str, tuple[dict, float]]) -> str:
    now = time.time()
    columns = list(dict.fromkeys(k for fields, _ in rows.values() for k in fields))
    table = [["node", *columns, "for"]] + [
        [key, *(str(fields.get(c, "")) for c in columns), _duration(now - since)]
        for key, (fields, since) in sorted(rows.items())]
    widths = [max(len(r[i]) for r in table) for i in range(len(table[0]))]
    states = [fields.get("state", "") for fields, _ in rows.values()]
    summary = ", ".join(f"{states.count(s)} {s}" for s in dict.fromkeys(states))
    lines = [f"{board} status @ {time.strftime('%H:%M:%S')}: {len(rows)} nodes, {summary}"]
    lines += ["  " + "  ".join(v.ljust(w) for v, w in zip(r, widths)).rstrip() for r in table]
    return "\n".join(lines)


def _duration(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m" if h else f"{m}m{s:02d}s" if m else f"{s}s"
