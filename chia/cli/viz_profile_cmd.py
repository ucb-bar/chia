"""CLI handler for ``chia viz-profile`` subcommand."""

import os
import sys
from pathlib import Path


def cmd_viz_profile(args):
    """Render a profiler JSONL log as a dependency graph (or CSV table)."""
    log_paths = args.log_file
    fmt = args.format

    if fmt == "table":
        missing = [p for p in log_paths if not os.path.exists(p)]
        if missing:
            print(f"Error: path(s) not found: {', '.join(missing)}", file=sys.stderr)
            sys.exit(1)
        from chia.trace.profile_table import render_profile_table
        render_profile_table(log_paths, output=args.output, funcs=args.funcs)
        return

    if len(log_paths) != 1:
        print(f"Error: --format {fmt} accepts exactly one log file "
              f"(got {len(log_paths)}). Use --format table for aggregation.",
              file=sys.stderr)
        sys.exit(1)
    log_path = log_paths[0]
    if not os.path.isfile(log_path):
        print(f"Error: file not found: {log_path}", file=sys.stderr)
        sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else Path(log_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    stem = Path(log_path).stem
    out_path = str(output_dir / f"{stem}_profile.{fmt}")

    if fmt == "html":
        from chia.trace.profile_viz_html import render_profile_html
        render_profile_html(
            log_path,
            out_path,
            run_index=args.run,
            gap_threshold=args.gap_threshold,
        )
    else:
        from chia.trace.profile_viz import render_profile
        render_profile(
            log_path,
            out_path,
            fmt=fmt,
            run_index=args.run,
            gap_threshold=args.gap_threshold,
            x_inches_per_sec=args.x_scale,
        )
