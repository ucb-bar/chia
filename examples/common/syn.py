"""Shared BoomTile inner-loop synthesis.

Houses :class:`SynResult` and :func:`run_inner_loop_syn`, the
inner-loop synthesis ChiaFunction that builds CACTI characterization,
optionally remaps SRAMs via MacroCompiler, and runs BoomTile through
Genus synthesis.

"""

import json
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

from chia.base.ChiaFunction import ChiaFunction, get
from chia.trace.profiler import get_profiler
from chia.chipyard.macrocompiler import generate_macro_stubs
from chia.vlsi.sram_cacti.sram_characterize import (
    assemble_generated_src_with_cacti,
    generate_cacti_macrocompiler_lib,
    parse_mems_conf,
)

from common.common_nodes import (
    _parse_verilog_modules,
    _resolve_boom_tile_module,
    parse_area_from_reports,
    run_cacti_characterization,
    run_macrocompiler_remap,
)
from common.boom_tile_syn import (
    _save_variant_results,
    run_boom_tile_synthesis,
)


@dataclass
class SynResult:
    """Outcome of one BoomTile inner-loop synthesis run.

    Fields:
        success: Whether Genus exited cleanly with a complete report set.
        area: Parsed total area from the Genus reports, or ``None`` if
            parsing failed or no area report was produced.
        boom_tile_module: Resolved BoomTile module name (e.g.
            ``BoomTile_1``) — needed because the BoomTile-wrapper module
            is renamed per-config and the area lookup keys off this name.
        elapsed: Wall-time spent inside the synthesis dispatch
            (``run_boom_tile_synthesis``) only, in seconds.
        total_elapsed: Wall-time for the whole inner loop (CACTI + remap
            + synthesis + report parsing), in seconds.
    """
    success: bool
    area: float | None
    boom_tile_module: str
    elapsed: float
    total_elapsed: float


@ChiaFunction()
def run_inner_loop_syn(
    generated_src: list[tuple[str, str]],
    branch_name: str,
    output_dir: str,
    chipyard_task_options: dict,
    boom_tile_syn_timeout: int = 172800,  # 48 hours
    cacti_path: str = "/path/to/cacti/cacti",
    chipyard_path: str = "/home/ray/chipyard/",
    sky130_col_path: str = "/path/to/sky130_col",
) -> SynResult:
    """Synthesize BoomTile from a pre-built ``generated_src``.

    Args:
        generated_src: List of ``(filename, content)`` Verilog/conf tuples
            collected by the build helper. Must include the ``*.top.mems.conf``
            file if SRAM remap is desired; otherwise the pipeline falls back
            to synflops.
        branch_name: FlatDB branch the build came from — used only for log
            prefixes and to scope profiler info, not for filesystem paths.
        output_dir: Absolute path (usually ``{FLATDB_PATH}/{branch_name}/{tag}/``)
            where ``generated_src.json``, ``cacti_libs/``, ``top.mems.conf``,
            ``syn_obj/``, and the per-variant report files are written. The
            directory is created if missing and overwritten if it exists, so
            debug-retry rebuilds can re-dispatch synthesis into the same path.
        chipyard_task_options: Ray scheduling options pinning
            :func:`run_macrocompiler_remap` to the same chipyard node that
            produced the build (``tapeout.jar`` from the build sits there).
            Synthesis itself uses ``SPREAD`` so it can land on any spare
            chipyard worker.
        boom_tile_syn_timeout: Timeout for the Genus synthesis dispatch
            (``run_boom_tile_synthesis``), in seconds.
        cacti_path: Path to the CACTI binary, passed to
            :func:`run_cacti_characterization`.
        chipyard_path: Chipyard checkout on the chipyard worker (holds
            ``tapeout.jar``), passed to :func:`run_macrocompiler_remap`.
        sky130_col_path: Sky130 collateral directory, passed to
            :func:`run_boom_tile_synthesis`.

    Returns:
        A :class:`SynResult` summarising the run. Always returned; the
        caller checks ``.success`` to distinguish OK vs. failed runs.
    """
    profiler = get_profiler()
    profiler.add_info({"branch_name": branch_name, "step": "inner_loop_synthesis",
                       "output_dir": output_dir})

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    t_total = time.time()
    print(f"[{branch_name}/{out.name}] === BoomTile inner-loop synthesis ===")

    # Only synthesis reports are persisted to the variant dir (see
    # _save_variant_results below). The generated Verilog, CACTI libs, and
    # mems.conf are kept in memory for this run but not written to disk.
    modules = _parse_verilog_modules(generated_src)
    boom_tile = _resolve_boom_tile_module(modules, set(modules.keys()))
    if boom_tile is None:
        raise RuntimeError("Could not resolve BoomTile module from generated Verilog")
    print(f"[{branch_name}/{out.name}]   BoomTile module: {boom_tile}")

    print(f"[{branch_name}/{out.name}] Running CACTI characterization...")
    generated_src, cacti_libs, _ = get(
        run_cacti_characterization.options(scheduling_strategy="SPREAD").chia_remote(
            generated_src, cacti_path,
        )
    )
    print(f"[{branch_name}/{out.name}]   {len(cacti_libs) if cacti_libs else 0} CACTI libs")

    if cacti_libs:
        mems_conf = next(
            (c for n, c in generated_src if n.endswith(".top.mems.conf")), None,
        )
        if mems_conf:
            specs = parse_mems_conf(mems_conf)
            mc_lib_json = generate_cacti_macrocompiler_lib(specs)
            remapped_v = get(run_macrocompiler_remap.options(**chipyard_task_options).chia_remote(
                mems_conf, mc_lib_json, chipyard_path,
            ))
            if remapped_v:
                stubs = generate_macro_stubs(specs, "cacti_")
                generated_src = assemble_generated_src_with_cacti(
                    generated_src, remapped_v, stubs,
                )
                print(f"[{branch_name}/{out.name}]   MacroCompiler remap OK — "
                      f"{len(generated_src)} files")
            else:
                print(f"[{branch_name}/{out.name}]   MacroCompiler remap FAILED — "
                      f"using synflops")

    obj_dir = str(out / "syn_obj")
    if os.path.isdir(obj_dir):
        shutil.rmtree(obj_dir)
    print(f"[{branch_name}/{out.name}] Dispatching BoomTile synthesis (obj_dir={obj_dir})")
    t0_syn = time.time()
    result = get(run_boom_tile_synthesis.options(scheduling_strategy="SPREAD").chia_remote(
        generated_src, boom_tile, obj_dir, boom_tile_syn_timeout, cacti_libs,
        sky130_col_path,
    ))
    elapsed = time.time() - t0_syn
    area = parse_area_from_reports(result.reports)
    status = "OK" if result.success else "FAILED"
    area_str = f"{area:.2f}" if area is not None else "N/A"
    print(f"[{branch_name}/{out.name}]   Synthesis {status}: area={area_str}, "
          f"reports={len(result.reports)}, elapsed={elapsed:.1f}s")
    _save_variant_results(out, boom_tile, result, area, elapsed)

    total_elapsed = time.time() - t_total
    print(f"[{branch_name}/{out.name}] Total synthesis wall time: {total_elapsed:.0f}s "
          f"({total_elapsed / 3600:.2f}h)")
    return SynResult(
        success=result.success,
        area=area,
        boom_tile_module=boom_tile,
        elapsed=elapsed,
        total_elapsed=total_elapsed,
    )
