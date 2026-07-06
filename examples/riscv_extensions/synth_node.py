"""Sky130 PPA synthesis node for the RISC-V extension loop.

Own copy of the BoomTile synth dispatch, decoupled from timing_opt: it reads this
example's VEXT_ paths (SKY130_COL_PATH) and wraps the shared
sky130_vlsi.Sky130SynNode. Mirrors timing_opt.boom_tile_syn.run_boom_tile_synthesis.
"""

import io
import os
import shutil
import tarfile
import time

from chia.base.ChiaFunction import ChiaFunction
from sky130_vlsi.hammer_syn_node import Sky130SynNode, _collect_reports
from sky130_vlsi.state_def import SynthesisResult

from riscv_extensions.constants import SKY130_COL_PATH, SYNTH_TIMEOUT_S

# Genus scratch dropped from the returned tarball (useless post-run, ~2 GB raw).
_EXCLUDE_DIRS = {"super_thread_debug", "fv", "input_src"}


@ChiaFunction(resources={"VLSI": 1, "Syn": 0})
def run_boom_tile_synthesis(
    generated_src_files: list[tuple[str, str]],
    vlsi_top: str,
    obj_dir: str,
    timeout_seconds: int = SYNTH_TIMEOUT_S,
    cacti_sram_libs: list[dict[str, str]] | None = None,
) -> tuple[SynthesisResult, bytes]:
    """Synthesize BoomTile in ``obj_dir`` on a VLSI worker; return the result plus
    a gzipped tarball of the (pruned) Hammer/Genus tree for the head to archive.
    The cluster has no shared FS, so obj_dir is tarred back and then deleted."""
    shutil.rmtree(obj_dir, ignore_errors=True)
    os.makedirs(obj_dir, exist_ok=True)
    print(f"  [synthesis] Starting vlsi_top='{vlsi_top}' "
          f"(timeout={timeout_seconds}s, input_files={len(generated_src_files)}, "
          f"cacti_sram_libs={len(cacti_sram_libs) if cacti_sram_libs else 0})")
    t0 = time.time()
    node = Sky130SynNode(
        sky130_col_path=SKY130_COL_PATH,
        input_files=generated_src_files,
        vlsi_top=vlsi_top,
        obj_dir=obj_dir,
        timeout_seconds=timeout_seconds,
        cacti_sram_libs=cacti_sram_libs,
    )
    # On a Genus crash/timeout, recover whatever reports landed and return a
    # failure result so we still tar the (partial) collateral back to the head.
    try:
        result = node.syn()
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"  [synthesis] vlsi_top='{vlsi_top}' RAISED after {time.time() - t0:.1f}s: {e}")
        try:
            partial_reports = _collect_reports(obj_dir)
        except Exception as ce:
            print(f"  [synthesis] could not collect partial reports: {ce}")
            partial_reports = {}
        result = SynthesisResult(success=False, stdout="",
                                 stderr=f"synthesis raised:\n{tb}",
                                 returncode=-1, reports=partial_reports)
    elapsed = time.time() - t0
    print(f"  [synthesis] Finished vlsi_top='{vlsi_top}': "
          f"{'OK' if result.success else 'FAILED'} [{elapsed:.1f}s]")
    if not result.success:
        print(f"  [synthesis] stderr (last 1000): "
              f"{result.stderr[-1000:] if result.stderr else '(empty)'}")

    # Tar obj_dir back to the head, dropping Genus scratch to keep it small:
    # super_thread_debug/, fv/, input_src/, per-stage pre_*/ dirs, and *.scr.
    def _tar_filter(tarinfo):
        parts = tarinfo.name.split("/")
        for p in parts[1:]:
            if p in _EXCLUDE_DIRS or p.startswith("pre_"):
                return None
        if parts[-1].endswith(".scr"):
            return None
        return tarinfo

    archive_bytes = b""
    try:
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tf:
            tf.add(obj_dir, arcname="syn_obj", filter=_tar_filter)
        archive_bytes = buf.getvalue()
        print(f"  [synthesis] obj_dir tarred ({len(archive_bytes) / 1e6:.1f} MB)")
    except Exception as e:
        print(f"  [synthesis] tar failed: {e}")
    shutil.rmtree(obj_dir, ignore_errors=True)
    return result, archive_bytes
