"""CLI handler for ``chia viz`` subcommand."""

import os
import sys
from pathlib import Path


def cmd_viz(args):
    """Generate a flow graph from a Chia loop Python file."""
    source_path = args.source_file
    if not os.path.isfile(source_path):
        print(f"Error: file not found: {source_path}", file=sys.stderr)
        sys.exit(1)

    from chia.trace.viz import render_flow

    fmt = args.format
    output_dir = Path(args.output_dir) if args.output_dir else Path(source_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    stem = Path(source_path).stem
    out_path = str(output_dir / f"{stem}_flow.{fmt}")

    render_flow(
        source_path,
        out_path,
        fmt=fmt,
        orchestrator_name=args.func,
    )
