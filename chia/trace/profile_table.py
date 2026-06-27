"""Aggregate exec_time_s stats per function from ChiaProfileCollector JSONL logs."""

from __future__ import annotations

import csv
import json
import math
import sys
from pathlib import Path
from typing import Iterable


LOG_FILENAME = "ChiaProfileCollector.log"


def find_log_files(paths: Path | str | Iterable[Path | str]) -> list[Path]:
    """Expand ``paths`` (a single file/dir or iterable of them) into log files.

    Results are deduplicated while preserving first-seen order.
    """
    if isinstance(paths, (str, Path)):
        paths = [paths]
    seen: set[Path] = set()
    out: list[Path] = []
    for p in paths:
        p = Path(p)
        if p.is_file():
            candidates = [p]
        elif p.is_dir():
            candidates = sorted(p.rglob(LOG_FILENAME))
        else:
            raise FileNotFoundError(f"Path not found: {p}")
        for c in candidates:
            resolved = c.resolve()
            if resolved not in seen:
                seen.add(resolved)
                out.append(c)
    return out


def collect_exec_times(log_paths: Iterable[Path],
                       func_names: list[str] | None) -> dict[str, list[float]]:
    """Parse JSONL logs and return ``{func_name: [exec_time_s, ...]}``.

    Only ``complete`` and ``local_end`` events carry ``exec_time_s`` and are counted.
    If ``func_names`` is None, every function found is included.
    """
    name_filter = set(func_names) if func_names is not None else None
    by_func: dict[str, list[float]] = {}
    for log_path in log_paths:
        with open(log_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    evt = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if evt.get("type") not in ("complete", "local_end"):
                    continue
                func = evt.get("func")
                if not func:
                    continue
                if name_filter is not None and func not in name_filter:
                    continue
                et = evt.get("exec_time_s")
                if et is None:
                    continue
                by_func.setdefault(func, []).append(float(et))
    return by_func


def _mean_std(values: list[float]) -> tuple[float, float]:
    n = len(values)
    if n == 0:
        return (float("nan"), float("nan"))
    mean = sum(values) / n
    if n < 2:
        return (mean, 0.0)
    var = sum((v - mean) ** 2 for v in values) / (n - 1)
    return (mean, math.sqrt(var))


def load_func_names(spec: str | None) -> list[str] | None:
    """Resolve ``spec`` into an ordered, deduplicated list of function names.

    ``spec`` may be ``None`` (return None → include every function),
    a path to a file (one name per line, ``#`` comments allowed),
    or a comma-separated list. Order is preserved so CSV rows match
    the order the user requested.
    """
    if spec is None:
        return None
    p = Path(spec)
    if p.is_file():
        raw: list[str] = []
        with open(p) as f:
            for line in f:
                name = line.split("#", 1)[0].strip()
                if name:
                    raw.append(name)
    else:
        raw = [part.strip() for part in spec.split(",") if part.strip()]
    return list(dict.fromkeys(raw))


def write_csv(stats: dict[str, list[float]],
              output: str | None,
              requested: list[str] | None) -> None:
    """Write a CSV with one row per function.

    If ``requested`` is provided, emit a row for every requested name
    (even if zero samples were found) in the order it was requested.
    Otherwise emit rows sorted by function name.
    """
    if requested is not None:
        funcs = requested
    else:
        funcs = sorted(stats.keys())

    fh = open(output, "w", newline="") if output else sys.stdout
    try:
        writer = csv.writer(fh)
        writer.writerow(["func", "count", "mean_exec_time_s",
                         "std_exec_time_s", "min_exec_time_s",
                         "max_exec_time_s", "total_exec_time_s"])
        for func in funcs:
            values = stats.get(func, [])
            count = len(values)
            if count == 0:
                writer.writerow([func, 0, "", "", "", "", ""])
                continue
            mean, std = _mean_std(values)
            writer.writerow([
                func,
                count,
                f"{mean:.6f}",
                f"{std:.6f}",
                f"{min(values):.6f}",
                f"{max(values):.6f}",
                f"{sum(values):.6f}",
            ])
    finally:
        if output:
            fh.close()


def render_profile_table(log_paths: str | Iterable[str],
                         output: str | None = None,
                         funcs: str | None = None) -> None:
    """CLI entrypoint: aggregate one or more logs/dirs and write CSV."""
    paths = find_log_files(log_paths)
    if not paths:
        raise FileNotFoundError(
            f"No {LOG_FILENAME} files found under {log_paths}")
    requested = load_func_names(funcs)
    stats = collect_exec_times(paths, requested)
    write_csv(stats, output, requested)
    msg = f"Aggregated {len(paths)} log file(s)"
    if output:
        msg += f" → {output}"
    print(msg, file=sys.stderr)
