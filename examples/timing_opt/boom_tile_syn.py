"""Synthesize full BoomTile for each Boom optimization variant in opts_to_syn.

Loads pre-built generated Verilog from each variant directory, strips stale
CACTI artifacts, rebuilds a correct per-variant .top.mems.conf, then runs
the full CACTI + MacroCompiler pipeline before dispatching synthesis with
BoomTile as vlsi_top using 4 VLSI resources per run.

Results (area_estimates.json, synthesis_log.md, synthesis_reports/) are
written back into each variant's directory, overwriting prior results.

Usage:
    ray job submit --working-dir . -- python boom_tile_syn.py
"""

import io
import json
import math
import os
import re
import shutil
import sys
import tarfile
import time
from pathlib import Path

import ray

from chia.base.ChiaFunction import ChiaFunction, get
from chia.examples.common.common_nodes import (
    _parse_verilog_modules,
    _resolve_boom_tile_module,
    run_cacti_characterization,
    parse_area_from_reports,
)
from timing_opt.constants import CACTI_PATH, SKY130_COL_PATH
from sky130_vlsi.state_def import SynthesisResult
from sky130_vlsi.hammer_syn_node import Sky130SynNode
from chia.chipyard.macrocompiler import generate_macro_stubs
from chia.vlsi.sram_cacti.cacti_runner import parse_mems_conf

OPTS_DIR = Path("PATH/TO/OPTS")
BOOM_TILE_SYN_TIMEOUT = 172800  # 48 hours
VLSI_PER_RUN = 1  # 1 VLSI resource per synthesis run

# Base mems.conf from the Chipyard MegaBoomBigCacheConfig build
BASE_MEMS_CONF_PATH = (
    # USER MUST FILL IN
)
MEMS_CONF_INJECT_NAME = (
    "chipyard.harness.TestHarness.MegaBoomChiaBigCacheConfig.top.mems.conf"
)


@ChiaFunction(resources={"VLSI": VLSI_PER_RUN, "Syn": 0})
def run_boom_tile_synthesis(
    generated_src_files: list[tuple[str, str]],
    vlsi_top: str,
    obj_dir: str,
    timeout_seconds: int = BOOM_TILE_SYN_TIMEOUT,
    cacti_sram_libs: list[dict[str, str]] | None = None,
) -> tuple[SynthesisResult, bytes]:
    """Run synthesis in ``obj_dir`` and return the result plus a tarball of it.

    ``obj_dir`` is a path on this worker's local /scratch (the cluster has no
    shared filesystem). After synthesis we tar the full Hammer/Genus working
    tree to gzipped bytes so the head node can extract it into the run's DB
    folder, then we delete the worker copy to free local scratch. The starting
    rmtree clears any leftover from a prior run on this same node.
    """
    shutil.rmtree(obj_dir, ignore_errors=True)
    os.makedirs(obj_dir, exist_ok=True)
    print(f"  [synthesis] Starting synthesis for vlsi_top='{vlsi_top}' "
          f"(timeout={timeout_seconds}s, input_files={len(generated_src_files)}, "
          f"obj_dir={obj_dir}, cacti_sram_libs={len(cacti_sram_libs) if cacti_sram_libs else 0})")
    t0 = time.time()
    node = Sky130SynNode(
        sky130_col_path=SKY130_COL_PATH,
        input_files=generated_src_files,
        vlsi_top=vlsi_top,
        obj_dir=obj_dir,
        timeout_seconds=timeout_seconds,
        cacti_sram_libs=cacti_sram_libs,
    )
    # Catch a synthesis raise (e.g. Genus crash / timeout) and recover whatever
    # reports landed on disk, building a failure SynthesisResult so we fall
    # through to the tar below — the head then gets the (partial) syn_obj for
    # debug. Propagating would skip the tar and strand the collateral on the
    # worker.
    try:
        result = node.syn()
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"  [synthesis] vlsi_top='{vlsi_top}' RAISED after "
              f"{time.time() - t0:.1f}s — collecting partial collateral: {e}")
        try:
            from sky130_vlsi.hammer_syn_node import _collect_reports
            partial_reports = _collect_reports(obj_dir)
        except Exception as ce:
            print(f"  [synthesis] could not collect partial reports: {ce}")
            partial_reports = {}
        result = SynthesisResult(
            success=False, stdout="",
            stderr=f"synthesis raised:\n{tb}",
            returncode=-1, reports=partial_reports,
        )
    elapsed = time.time() - t0
    print(f"  [synthesis] Finished vlsi_top='{vlsi_top}': "
          f"{'OK' if result.success else 'FAILED'} [{elapsed:.1f}s]")
    if not result.success:
        print(f"  [synthesis] vlsi_top='{vlsi_top}' stderr (last 1000 chars): "
              f"{result.stderr[-1000:] if result.stderr else '(empty)'}")

    # Pack obj_dir for transfer back to the head node (no shared FS to copy across).
    # The head extracts these bytes into DB/files/<branch>/syn_obj. Best-effort:
    # an empty tarball is fine, we just won't have the working dir archived.
    #
    # Cut the bloat: a full Genus obj_dir for BoomTile is ~2.7 GB raw, dominated
    # by Genus internal scratch that's useless once the run is done. We exclude
    # those upfront so the tarball (and the extracted DB copy) stay under ~500 MB:
    #   - super_thread_debug/ : per-thread parallel-synth debug logs (~1.4 GB)
    #   - fv/                 : formal-verification scratch (~140 MB)
    #   - pre_<stage>/        : 9 per-stage Genus checkpoints (~330 MB total)
    #   - *.scr               : Genus replay scripts (~140 MB for BoomTile.mapped.scr)
    #   - input_src/          : duplicate of DB's generated_src (~48 MB)
    # What survives: reports/, the mapped netlist (.v), .sdf, find_regs_paths.json,
    # tech-sky130-cache/, inputs_override.yml, results/, genus_invs_des/.
    _EXCLUDE_DIRS = {"super_thread_debug", "fv", "input_src"}

    def _tar_filter(tarinfo):
        # tarinfo.name is the in-archive path, prefixed with "syn_obj/" because
        # we set arcname="syn_obj" on the root add().
        parts = tarinfo.name.split("/")
        for p in parts[1:]:
            if p in _EXCLUDE_DIRS:
                return None
            if p.startswith("pre_"):
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
        print(f"  [synthesis] obj_dir tarred ({len(archive_bytes)/1e6:.1f} MB compressed)")
    except Exception as e:
        print(f"  [synthesis] tar of obj_dir failed: {e}")
    # Free worker scratch.
    shutil.rmtree(obj_dir, ignore_errors=True)
    return result, archive_bytes


def _generate_mems_v(sram_specs) -> str:
    """Generate a complete .top.mems.v with pass-through wrappers around cacti_* macros.

    Each _ext module is a simple wrapper that instantiates its cacti_*_ext
    counterpart with direct port connections. This ensures the mems.v matches
    the mems.conf exactly.
    """
    modules = []
    for spec in sram_specs:
        addr_bits = max(1, math.ceil(math.log2(max(spec.depth, 2))))
        mask_bits = math.ceil(spec.width / spec.mask_gran) if spec.mask_gran else 0

        ports = []       # port declarations
        connections = []  # cacti_ instance connections

        for prefix, masked in spec.parsed_ports:
            is_rw = prefix.startswith("RW")
            is_reader = prefix.startswith("R") and not is_rw
            is_writer = prefix.startswith("W")

            # addr
            if addr_bits > 1:
                ports.append(f"  input  [{addr_bits-1}:0] {prefix}_addr")
            else:
                ports.append(f"  input         {prefix}_addr")
            connections.append(f"    .{prefix}_addr({prefix}_addr)")

            # clk
            ports.append(f"  input         {prefix}_clk")
            connections.append(f"    .{prefix}_clk({prefix}_clk)")

            # en
            ports.append(f"  input         {prefix}_en")
            connections.append(f"    .{prefix}_en({prefix}_en)")

            if is_rw:
                ports.append(f"  input         {prefix}_wmode")
                connections.append(f"    .{prefix}_wmode({prefix}_wmode)")
                if spec.width > 1:
                    ports.append(f"  input  [{spec.width-1}:0] {prefix}_wdata")
                    ports.append(f"  output [{spec.width-1}:0] {prefix}_rdata")
                else:
                    ports.append(f"  input  [0:0] {prefix}_wdata")
                    ports.append(f"  output [0:0] {prefix}_rdata")
                connections.append(f"    .{prefix}_wdata({prefix}_wdata)")
                connections.append(f"    .{prefix}_rdata({prefix}_rdata)")
                if masked and mask_bits > 0:
                    if mask_bits > 1:
                        ports.append(f"  input  [{mask_bits-1}:0] {prefix}_wmask")
                    else:
                        ports.append(f"  input  [0:0] {prefix}_wmask")
                    connections.append(f"    .{prefix}_wmask({prefix}_wmask)")
            elif is_reader:
                if spec.width > 1:
                    ports.append(f"  output [{spec.width-1}:0] {prefix}_data")
                else:
                    ports.append(f"  output [0:0] {prefix}_data")
                connections.append(f"    .{prefix}_data({prefix}_data)")
            elif is_writer:
                if spec.width > 1:
                    ports.append(f"  input  [{spec.width-1}:0] {prefix}_data")
                else:
                    ports.append(f"  input  [0:0] {prefix}_data")
                connections.append(f"    .{prefix}_data({prefix}_data)")
                if masked and mask_bits > 0:
                    if mask_bits > 1:
                        ports.append(f"  input  [{mask_bits-1}:0] {prefix}_mask")
                    else:
                        ports.append(f"  input  [0:0] {prefix}_mask")
                    connections.append(f"    .{prefix}_mask({prefix}_mask)")

        lines = [f"module {spec.name}("]
        lines.append(",\n".join(ports))
        lines.append(");")
        lines.append(f"  cacti_{spec.name} mem_0_0 (")
        lines.append(",\n".join(connections))
        lines.append("  );")
        lines.append("endmodule")
        modules.append("\n".join(lines))

    return "\n\n\n".join(modules) + "\n"


def _strip_cacti_artifacts(
    generated_src: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    """Remove stale cacti_*.v stub files from generated_src.

    Keeps .top.mems.v (needed by CACTI characterization to confirm it exists;
    will be replaced by assemble_generated_src_with_cacti after MacroCompiler).
    """
    cleaned = []
    removed = []
    for fname, content in generated_src:
        if fname.startswith("cacti_") and fname.endswith(".v"):
            removed.append(fname)
            continue
        cleaned.append((fname, content))
    if removed:
        print(f"    Stripped {len(removed)} stale CACTI stubs")
    return cleaned


def _build_mems_conf(
    generated_src: list[tuple[str, str]],
    base_mems_conf: str,
) -> str:
    """Build a correct per-variant .top.mems.conf.

    Starts from the base mems.conf but derives specs from the actual Verilog
    for any _ext module that is new or has changed shape. This ensures the
    mems.conf matches the variant's actual SRAM instantiations.
    """
    # Parse base entries into a dict for easy override
    base_entries = {}  # name -> full line
    for line in base_mems_conf.strip().splitlines():
        parts = line.strip().split()
        if len(parts) >= 2 and parts[0] == "name":
            base_entries[parts[1]] = line.strip()

    # Find all _ext module instantiations in the Verilog
    # Pattern: <ext_name> <instance_name> ( ... )
    ext_instantiations = {}  # sram_name -> wrapper module content
    inst_re = re.compile(r'^\s+(\w+_ext)\s+\w+_ext\s*\(', re.MULTILINE)
    for fname, content in generated_src:
        if not fname.endswith((".v", ".sv")):
            continue
        for m in inst_re.finditer(content):
            ext_name = m.group(1)
            if ext_name not in ext_instantiations:
                ext_instantiations[ext_name] = content

    # Derive specs for ALL _ext modules found in the Verilog
    derived = {}  # name -> mems.conf line
    for ext_name, wrapper_content in ext_instantiations.items():
        line = _derive_mems_conf_from_wrapper(ext_name, wrapper_content)
        if line:
            derived[ext_name] = line

    # Build final mems.conf: use base entries but override with derived
    # specs when available (derived specs reflect the actual Verilog)
    final_lines = []
    overridden = []
    for name, base_line in base_entries.items():
        if name in derived:
            final_lines.append(derived[name])
            if derived[name] != base_line:
                overridden.append(name)
        else:
            final_lines.append(base_line)

    # Add any derived entries not in base (new SRAMs)
    for name, line in derived.items():
        if name not in base_entries:
            final_lines.append(line)

    if overridden:
        print(f"    Overrode base specs for: {overridden}")

    return "\n".join(final_lines) + "\n"


def _derive_mems_conf_from_wrapper(ext_name: str, wrapper_content: str) -> str | None:
    """Derive a mems.conf line for an _ext SRAM from its wrapper module's port connections.

    Parses the wrapper module that instantiates the _ext to find port widths
    and determine the SRAM spec.
    """
    # Find the instantiation block for this _ext
    # Pattern: <ext_name> <instance_name> (
    inst_pattern = re.compile(
        rf'^\s+{re.escape(ext_name)}\s+\w+\s*\((.*?)\);',
        re.MULTILINE | re.DOTALL,
    )
    m = inst_pattern.search(wrapper_content)
    if not m:
        return None

    inst_block = m.group(1)

    # Parse port connections to find port names
    port_re = re.compile(r'\.(\w+)\s*\(')
    port_names = port_re.findall(inst_block)

    # Categorize ports
    read_ports = set()
    write_ports = set()
    rw_ports = set()
    has_mask = False

    for pname in port_names:
        prefix_m = re.match(r'(R\d+|W\d+|RW\d+)_', pname)
        if not prefix_m:
            continue
        prefix = prefix_m.group(1)
        suffix = pname[len(prefix) + 1:]

        if prefix.startswith("RW"):
            rw_ports.add(prefix)
            if suffix == "wmask":
                has_mask = True
        elif prefix.startswith("R"):
            read_ports.add(prefix)
        elif prefix.startswith("W"):
            write_ports.add(prefix)
            if suffix == "mask":
                has_mask = True

    # Find addr and data widths from the wrapper module's own port declarations
    # The wrapper module has the same port widths as the _ext it instantiates
    module_re = re.compile(r'^module\s+\w+\s*\((.*?)\);', re.MULTILINE | re.DOTALL)
    mod_m = module_re.search(wrapper_content)
    if not mod_m:
        return None

    addr_width = None
    data_width = None
    mask_width = None
    mod_body = mod_m.group(1)
    # Match bracketed ports: input [7:0] R0_addr
    port_decl_re = re.compile(r'(?:input|output)\s+\[(\d+):0\]\s+(\w+)')
    for pm in port_decl_re.finditer(mod_body):
        msb = int(pm.group(1))
        pname = pm.group(2)
        w = msb + 1
        if pname.endswith("_addr") and addr_width is None:
            addr_width = w
        elif pname.endswith("_data") and data_width is None:
            data_width = w
        elif pname.endswith("_mask"):
            mask_width = w
    # Match single-bit ports: input R0_data (no [N:0] = width 1)
    singlebit_re = re.compile(r'(?:input|output)\s+(\w+_(?:addr|data|mask))\s*[,\)]')
    for pm in singlebit_re.finditer(mod_body):
        pname = pm.group(1)
        if pname.endswith("_data") and data_width is None:
            data_width = 1
        elif pname.endswith("_addr") and addr_width is None:
            addr_width = 1
        elif pname.endswith("_mask") and mask_width is None:
            mask_width = 1

    if addr_width is None or data_width is None:
        return None

    depth = 2 ** addr_width

    # Build port type string
    port_parts = []
    if rw_ports:
        for _ in rw_ports:
            port_parts.append("mrw" if has_mask else "rw")
    else:
        for _ in read_ports:
            port_parts.append("read")
        for _ in write_ports:
            port_parts.append("mwrite" if has_mask else "write")

    ports_str = ",".join(port_parts) if port_parts else "rw"

    line = f"name {ext_name} depth {depth} width {data_width} ports {ports_str}"
    if has_mask and mask_width and data_width:
        mask_gran = data_width // mask_width
        if mask_gran > 0:
            line += f" mask_gran {mask_gran}"

    return line


def _save_variant_results(opt_dir, boom_tile_module, result, area, elapsed):
    """Save synthesis results into the variant's directory."""
    # area_estimates.json
    area_estimates = {boom_tile_module: area} if area is not None else {}
    with open(opt_dir / "area_estimates.json", "w") as f:
        json.dump(area_estimates, f)

    # Raw reports JSON (includes syn-rundir contents)
    report_filename = f"synthesis_reports_{boom_tile_module}_child_reports.json"
    with open(opt_dir / report_filename, "w") as f:
        json.dump(result.reports, f)

    # Individual report files preserving directory structure
    run_dir = opt_dir / "synthesis_reports" / f"{boom_tile_module}_child"
    os.makedirs(run_dir, exist_ok=True)
    for rpt_name, rpt_content in result.reports.items():
        rpt_path = run_dir / rpt_name
        os.makedirs(rpt_path.parent, exist_ok=True)
        with open(rpt_path, "w") as rpt_f:
            rpt_f.write(rpt_content)

    # stdout/stderr/summary
    with open(run_dir / "stdout.txt", "w") as f:
        f.write(result.stdout or "")
    with open(run_dir / "stderr.txt", "w") as f:
        f.write(result.stderr or "")
    with open(run_dir / "summary.txt", "w") as f:
        area_str = f"{area:.2f}" if area is not None else "N/A"
        f.write(
            f"module: {boom_tile_module}\n"
            f"success: {result.success}\n"
            f"returncode: {result.returncode}\n"
            f"area: {area_str}\n"
            f"elapsed: {elapsed:.1f}s\n"
            f"reports: {len(result.reports)}\n"
        )

    # synthesis_log.md
    status = "OK" if result.success else "FAILED"
    area_str = f"{area:.2f}" if area is not None else "N/A"
    log_lines = [
        "# BoomTile Synthesis Log\n",
        f"## {boom_tile_module}: {status}, area={area_str}, elapsed={elapsed:.1f}s\n",
    ]
    if not result.success:
        log_lines.append(
            f"```\nreturncode={result.returncode}\n"
            f"stderr (last 2000):\n{result.stderr[-2000:]}\n```\n"
        )
    log_lines.append(f"\n## Reports collected: {len(result.reports)}\n")
    for rpt_name in sorted(result.reports.keys()):
        log_lines.append(f"- {rpt_name}")
    with open(opt_dir / "synthesis_log.md", "w") as f:
        f.write("\n".join(log_lines))

    print(f"  Saved {len(result.reports)} reports + stdout/stderr to {run_dir}/")


def main():
    t_total = time.time()
    print("=" * 70)
    print("BoomTile Full-Module Synthesis")
    print(f"  Source dir:  {OPTS_DIR}")
    print(f"  Timeout:     {BOOM_TILE_SYN_TIMEOUT}s ({BOOM_TILE_SYN_TIMEOUT / 3600:.1f}h)")
    print(f"  VLSI/run:    {VLSI_PER_RUN}")
    print("=" * 70)

    # ---- 1. Ray init + resource check ----
    ray.init(address="auto", ignore_reinit_error=True)
    resources = ray.cluster_resources()
    print(f"\nCluster resources: VLSI={resources.get('VLSI', 0)}, "
          f"chipyard={resources.get('chipyard', 0)}, "
          f"Syn={resources.get('Syn', 0)}")

    if resources.get("VLSI", 0) < VLSI_PER_RUN:
        print(f"ERROR: Need at least {VLSI_PER_RUN} VLSI resources, "
              f"have {resources.get('VLSI', 0)}")
        sys.exit(1)

    # ---- 2. Discover and load variants ----
    # Optional filter: pass variant substrings as CLI args to run only those
    filters = sys.argv[1:]
    print("\n--- Loading variants from opts_to_syn ---")
    if filters:
        print(f"  Filter: {filters}")
    variant_data = {}
    for entry in sorted(OPTS_DIR.iterdir()):
        if not entry.is_dir():
            continue
        if filters and not any(f in entry.name for f in filters):
            continue
        gen_src_path = entry / "generated_src.json"
        if not gen_src_path.exists():
            print(f"  SKIP {entry.name}: no generated_src.json")
            continue
        print(f"  Loading {entry.name} ...", end="", flush=True)
        t0 = time.time()
        try:
            generated_src = json.loads(gen_src_path.read_text())
        except (json.JSONDecodeError, OSError) as e:
            print(f" ERROR: {e}")
            continue
        print(f" {len(generated_src)} files [{time.time() - t0:.1f}s]")
        variant_data[entry.name] = {"generated_src": generated_src}

    if not variant_data:
        print("ERROR: No valid variants found")
        sys.exit(1)
    print(f"\nLoaded {len(variant_data)} variants")

    # ---- 3. Resolve BoomTile module name ----
    print("\n--- Resolving BoomTile module names ---")
    for name, data in list(variant_data.items()):
        modules = _parse_verilog_modules(data["generated_src"])
        boom_tile = _resolve_boom_tile_module(modules, set(modules.keys()))
        if boom_tile is None:
            print(f"  ERROR: Could not resolve BoomTile for {name} — skipping")
            del variant_data[name]
            continue
        data["boom_tile_module"] = boom_tile
        print(f"  {name}: {boom_tile}")

    if not variant_data:
        print("ERROR: No variants with resolvable BoomTile module")
        sys.exit(1)

    # ---- 4. Fix empty CACTI stubs + build per-variant mems.conf ----
    # The generated_src already has MacroCompiler-remapped Verilog with cacti_*
    # stubs. Most are correct, but some variants have empty stubs for SRAMs
    # whose shape changed. We regenerate those from the actual Verilog.
    # We also build a correct mems.conf per variant for CACTI Liberty generation.
    print(f"\n--- Building per-variant mems.conf ---")
    if not os.path.exists(BASE_MEMS_CONF_PATH):
        print(f"ERROR: Base mems.conf not found at {BASE_MEMS_CONF_PATH}")
        sys.exit(1)
    with open(BASE_MEMS_CONF_PATH) as f:
        base_mems_conf = f.read()
    n_base = len(base_mems_conf.strip().splitlines())
    print(f"  Base mems.conf: {n_base} SRAM entries")

    for name, data in variant_data.items():
        mems_conf = _build_mems_conf(data["generated_src"], base_mems_conf)
        n_total = len(mems_conf.strip().splitlines())
        n_extra = n_total - n_base

        # Regenerate .top.mems.v and cacti_*.v stubs entirely from the
        # derived mems.conf so they always match. This replaces the stale
        # MacroCompiler output which may have missing or empty modules.
        specs = parse_mems_conf(mems_conf)
        fresh_mems_v = _generate_mems_v(specs)
        fresh_stubs = generate_macro_stubs(specs, "cacti_")
        fresh_stub_map = {fname: content for fname, content in fresh_stubs}

        # Replace mems.v and cacti stubs in generated_src
        new_src = []
        for fname, content in data["generated_src"]:
            if fname.endswith(".top.mems.v"):
                new_src.append((fname, fresh_mems_v))
            elif fname in fresh_stub_map:
                new_src.append((fname, fresh_stub_map[fname]))
                del fresh_stub_map[fname]
            else:
                new_src.append((fname, content))
        # Add any new stubs that didn't exist before
        for fname, content in fresh_stub_map.items():
            new_src.append((fname, content))
        data["generated_src"] = new_src

        # Inject mems.conf into a CACTI-only copy (not passed to synthesis)
        cacti_src = list(data["generated_src"])
        cacti_src.append((MEMS_CONF_INJECT_NAME, mems_conf))
        data["cacti_src"] = cacti_src
        data["mems_conf"] = mems_conf

        parts = [f"{n_total} entries"]
        if n_extra > 0:
            parts[0] = f"{n_base} base + {n_extra} extra = {n_total} entries"
        print(f"  {name}: {', '.join(parts)}")

    # ---- 5. CACTI SRAM characterization (parallel) ----
    print(f"\n--- CACTI SRAM Characterization ({len(variant_data)} variants in parallel) ---")
    cacti_refs = {}
    for name, data in variant_data.items():
        cacti_refs[name] = run_cacti_characterization.chia_remote(
            data["cacti_src"], CACTI_PATH,
        )

    for name, ref in cacti_refs.items():
        try:
            _, cacti_libs, sram_names = get(ref)
            variant_data[name]["cacti_libs"] = cacti_libs
            count = len(cacti_libs) if cacti_libs else 0
            print(f"  {name}: {count} CACTI libs")
        except Exception as e:
            print(f"  WARNING: CACTI failed for {name}: {e}")
            variant_data[name]["cacti_libs"] = None
        variant_data[name].pop("cacti_src", None)

    # MacroCompiler remap skipped — the generated_src already has remapped
    # .top.mems.v

    # ---- 6. Dispatch BoomTile synthesis (parallel, 4 VLSI each) ----
    # Uses persistent obj_dir on /scratch so the full syn-rundir is preserved.
    vlsi_needed = len(variant_data) * VLSI_PER_RUN
    vlsi_avail = resources.get("VLSI", 0)
    print(f"\n--- Dispatching BoomTile synthesis ---")
    print(f"  {len(variant_data)} variants x {VLSI_PER_RUN} VLSI = {vlsi_needed} VLSI needed "
          f"({vlsi_avail} available)")
    if vlsi_needed > vlsi_avail:
        print(f"  WARNING: Not enough VLSI for full parallelism — some runs will queue")

    syn_refs = {}
    ref_to_name = {}  # map ObjectRef -> variant name for ray.wait()
    syn_dispatch_times = {}
    for name, data in variant_data.items():
        boom_tile = data["boom_tile_module"]
        obj_dir = str(OPTS_DIR / name / "syn_obj")
        # Clean previous run's obj_dir if it exists
        if os.path.isdir(obj_dir):
            shutil.rmtree(obj_dir)
        print(f"  Dispatching {name} (vlsi_top={boom_tile}, obj_dir={obj_dir}) ...")
        ref = run_boom_tile_synthesis.chia_remote(
            data["generated_src"],
            boom_tile,
            obj_dir,
            BOOM_TILE_SYN_TIMEOUT,
            data.get("cacti_libs"),
        )
        syn_refs[name] = ref
        ref_to_name[ref] = name
        syn_dispatch_times[name] = time.time()

    print(f"\n  All {len(syn_refs)} synthesis runs dispatched. Waiting for completion...")

    # ---- 7. Collect results as they finish (ray.wait) ----
    print(f"\n--- Collecting synthesis results (as they finish) ---")
    results = {}
    pending = list(syn_refs.values())

    while pending:
        done, pending = ray.wait(pending, num_returns=1, timeout=60)
        if not done:
            # Timeout — just print a heartbeat
            elapsed = time.time() - t_total
            print(f"  ... {len(pending)} runs still pending [{elapsed:.0f}s total]")
            continue

        ref = done[0]
        name = ref_to_name[ref]
        boom_tile = variant_data[name]["boom_tile_module"]
        opt_dir = OPTS_DIR / name
        obj_dir = opt_dir / "syn_obj"

        try:
            result = get(ref)
            elapsed = time.time() - syn_dispatch_times[name]
            area = parse_area_from_reports(result.reports)
            results[name] = {
                "success": result.success,
                "area": area,
                "boom_tile_module": boom_tile,
                "elapsed": elapsed,
            }
            status = "OK" if result.success else "FAILED"
            area_str = f"{area:.2f}" if area is not None else "N/A"
            print(f"  {name}: {status}, area={area_str}, "
                  f"reports={len(result.reports)}, elapsed={elapsed:.1f}s")

            _save_variant_results(opt_dir, boom_tile, result, area, elapsed)

        except ray.exceptions.RayTaskError as e:
            elapsed = time.time() - syn_dispatch_times[name]
            print(f"  {name}: EXCEPTION [{elapsed:.1f}s]: {str(e)[:500]}")
            results[name] = {
                "success": False, "area": None,
                "boom_tile_module": boom_tile,
                "elapsed": elapsed, "error": str(e)[:2000],
            }
            with open(opt_dir / "synthesis_log.md", "w") as f:
                f.write(f"# BoomTile Synthesis Log\n\n"
                        f"## {boom_tile}: EXCEPTION\n```\n{str(e)[:2000]}\n```\n")

        except ray.exceptions.TaskCancelledError:
            elapsed = time.time() - syn_dispatch_times[name]
            print(f"  {name}: CANCELLED [{elapsed:.1f}s]")
            results[name] = {
                "success": False, "area": None,
                "boom_tile_module": boom_tile,
                "elapsed": elapsed, "error": "CANCELLED",
            }
            with open(opt_dir / "synthesis_log.md", "w") as f:
                f.write(f"# BoomTile Synthesis Log\n\n## {boom_tile}: CANCELLED\n")

        # syn-rundir is already on /scratch at obj_dir
        print(f"  Full syn-rundir at: {obj_dir}/syn-rundir/")

    # ---- 8. Print summary ----
    total_elapsed = time.time() - t_total
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"{'Variant':<55} {'Module':<15} {'Area':>12} {'Status':>8} {'Time':>10}")
    print("-" * 100)
    for name, r in results.items():
        area_str = f"{r['area']:.2f}" if r.get("area") is not None else "N/A"
        status = "OK" if r["success"] else "FAILED"
        elapsed_str = f"{r['elapsed']:.0f}s"
        print(f"{name:<55} {r['boom_tile_module']:<15} {area_str:>12} {status:>8} {elapsed_str:>10}")
    print("-" * 100)
    print(f"Total wall time: {total_elapsed:.0f}s ({total_elapsed / 3600:.1f}h)")

    succeeded = sum(1 for r in results.values() if r["success"])
    failed = len(results) - succeeded
    print(f"Succeeded: {succeeded}, Failed: {failed}")

    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
