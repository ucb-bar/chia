"""End-to-end test for chia.chipyard.macrocompiler on a local chipyard cluster.

Exercises the MacroCompiler glue against the REAL Chipyard tools inside a
``ghcr.io/ucb-bar/chia-chisel-build`` worker container: the prebuilt
``tapeout.jar`` MacroCompiler (``remap_with_macrocompiler``) plus Verilator
linting of the remapped collateral. No synthesis / P&R.

Two stages, mirroring the cacti smoke test:

  STAGE 1 — one task submitted to the ``chipyard`` resource (so it lands in the
  container) that drives the whole module as plain in-process calls and
  validates every artifact with the real tools:

    1. ``parse_mems_conf`` — parse a representative ``.top.mems.conf``.
    2. ``generate_macrocompiler_lib`` — build the MDF JSON library (one
       exact-match entry per SRAM so MacroCompiler maps 1:1).
    3. ``remap_with_macrocompiler`` — run the real ``tapeout.jar`` MacroCompiler
       to remap the synflop SRAMs onto the library macros; checks the remapped
       Verilog defines a wrapper module per SRAM and instantiates each
       ``mapped_<name>`` macro.
    4. ``generate_macro_stubs`` — blackbox Verilog stubs for the mapped macros.
    5. Verilator ``--lint-only`` over the remapped ``.top.mems.v`` + the stubs
       together (a self-contained module set), proving the generated collateral
       actually parses/elaborates in a real Verilog frontend.
    6. ``assemble_generated_src_with_macros`` against the real remap output +
       stubs (swap the synflop ``.top.mems.v``, drop a conflicting ``_ext.sv``,
       keep unrelated files, append the stubs), plus direct checks of
       ``rename_ports`` and ``SRAMSpec.size_bytes`` — closing out the rest of
       the module's surface.

  STAGE 2 — dispatches ``remap_with_macrocompiler`` itself via ``.chia_remote``
  so the Ray path is exercised directly: the ``chipyard`` resource requirement,
  the trampoline, and string round-tripping across the worker boundary — not
  just the plain in-process call stage 1 makes.

Usage (host, chia env active, cluster up via chipyard_local.yaml):
    cd <repo root>
    python chia/chipyard/test/cluster/macrocompiler_e2e_test.py
"""

import os
import re
import sys

# Allow `python .../macrocompiler_e2e_test.py` from any cwd: put the repo root
# on sys.path so the `chia` namespace package (resolved via path, not installed)
# imports. cluster -> test -> chipyard -> chia -> <repo root> == 4 levels up.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), *([os.pardir] * 4)))
sys.path.insert(0, _REPO_ROOT)

import ray

from chia.base.ChiaFunction import ChiaFunction, get

# Path to the chipyard install inside the chia-chisel-build worker container.
CHIPYARD_PATH = os.environ.get("CHIPYARD_PATH", "/home/ray/chipyard")

# A few representative SRAMs (mixed sizes/ports, one masked). All are big enough
# that a 1:1 macro mapping is meaningful; the names become wrapper modules.
SAMPLE_MEMS_CONF = "\n".join([
    "name dcache_data depth 2048 width 64 ports rw",
    "name icache_tag  depth 512  width 88 ports read,write",
    "name regfile     depth 64   width 64 ports mrw mask_gran 8",
])

CELL_PREFIX = "mapped_"


@ChiaFunction(resources={"chipyard": 1})
def macrocompiler_validation_e2e(mems_conf: str, chipyard_path: str) -> dict:
    """Runs on a chipyard worker: real tapeout.jar remap + Verilator lint.

    Drives parse -> generate_macrocompiler_lib -> remap_with_macrocompiler ->
    generate_macro_stubs, then lints the remapped Verilog + stubs with the real
    Verilator. Also exercises the remaining module surface in-process:
    assemble_generated_src_with_macros (against the real remap output + stubs),
    rename_ports, and SRAMSpec.size_bytes. Returns a JSON-able report (no
    non-serializable objects).
    """
    import os
    import shutil
    import subprocess
    import tempfile

    from chia.chipyard.macrocompiler import (
        parse_mems_conf,
        generate_macrocompiler_lib,
        remap_with_macrocompiler,
        generate_macro_stubs,
        assemble_generated_src_with_macros,
        rename_ports,
    )

    # The Ray worker runs under anaconda's python without sourcing chipyard's
    # env.sh, so verilator isn't on PATH — it lives in chipyard's conda env.
    # Resolve it there, falling back to PATH for non-chipyard hosts.
    verilator = os.path.join(chipyard_path, ".conda-env", "bin", "verilator")
    if not os.path.isfile(verilator):
        verilator = shutil.which("verilator")

    report: dict = {
        "chipyard_path": chipyard_path,
        "tapeout_jar": os.path.join(chipyard_path, ".classpath_cache", "tapeout.jar"),
        "tapeout_jar_present": os.path.isfile(
            os.path.join(chipyard_path, ".classpath_cache", "tapeout.jar")),
        "verilator": verilator,
        "num_specs": 0,
        "spec_names": [],
        "lib_entries": 0,
        "lib_names": [],
        "remap_ok": False,
        "remapped_modules": [],
        "mapped_refs": [],
        "missing_wrappers": [],
        "missing_macro_insts": [],
        "stubs": [],
        "lint_ok": None,
        "lint_err": None,
        "assemble_ok": None,
        "assemble_err": None,
        "rename_ports_ok": None,
        "rename_ports_err": None,
        "size_bytes_ok": None,
        "size_bytes_err": None,
        "error": None,
    }

    try:
        specs = parse_mems_conf(mems_conf)
        report["num_specs"] = len(specs)
        report["spec_names"] = [s.name for s in specs]

        lib_json = generate_macrocompiler_lib(specs, CELL_PREFIX)
        import json as _json
        lib_entries = _json.loads(lib_json)
        report["lib_entries"] = len(lib_entries)
        report["lib_names"] = sorted(e.get("name") for e in lib_entries)

        remapped = remap_with_macrocompiler(mems_conf, lib_json, chipyard_path)
        if not remapped:
            report["error"] = "remap_with_macrocompiler returned None/empty"
            return report
        report["remap_ok"] = True

        report["remapped_modules"] = sorted(
            set(re.findall(r"^\s*module\s+(\w+)", remapped, re.MULTILINE)))
        report["mapped_refs"] = sorted(
            set(re.findall(rf"\b{re.escape(CELL_PREFIX)}\w+\b", remapped)))

        # Every SRAM should yield a same-named wrapper module that instantiates
        # its mapped_<name> macro.
        report["missing_wrappers"] = [
            s.name for s in specs if s.name not in report["remapped_modules"]]
        report["missing_macro_insts"] = [
            s.name for s in specs
            if f"{CELL_PREFIX}{s.name}" not in remapped]

        stubs = generate_macro_stubs(specs, CELL_PREFIX)
        report["stubs"] = [n for n, _ in stubs]

        # Verilator lint: remapped wrappers + macro stubs are a closed module set.
        if report["verilator"]:
            with tempfile.TemporaryDirectory() as d:
                files = []
                mv = os.path.join(d, "top.mems.v")
                with open(mv, "w") as f:
                    f.write(remapped)
                files.append(mv)
                for name, content in stubs:
                    p = os.path.join(d, name)
                    with open(p, "w") as f:
                        f.write(content)
                    files.append(p)
                r = subprocess.run(
                    [report["verilator"], "--lint-only", "-Wno-fatal", "-Wno-WIDTH"] + files,
                    capture_output=True, text=True, timeout=120)
                report["lint_ok"] = r.returncode == 0
                if r.returncode != 0:
                    report["lint_err"] = (r.stdout + r.stderr)[-800:]

        # assemble_generated_src_with_macros: feed a synthetic generated_src
        # (a synflop .top.mems.v to be swapped, a conflicting _ext.sv that
        # redefines a remapped module and must be dropped, an unrelated .sv to
        # keep, and the .top.mems.conf to keep) and verify the swap/drop/append
        # behavior against the REAL remap output + stubs.
        conflict_mod = report["remapped_modules"][0]
        gen_src = [
            ("Top.top.mems.v", "module SYNFLOP_PLACEHOLDER(); endmodule\n"),
            ("Top.top.mems.conf", mems_conf),
            (f"{conflict_mod}_ext.sv", f"module {conflict_mod}();\nendmodule\n"),
            ("Unrelated.sv", "module Unrelated();\nendmodule\n"),
        ]
        assembled = assemble_generated_src_with_macros(gen_src, remapped, stubs)
        names = [n for n, _ in assembled]
        contents = dict(assembled)
        stub_names = {n for n, _ in stubs}
        problems = []
        if contents.get("Top.top.mems.v") != remapped:
            problems.append("synflop .top.mems.v was not replaced with remapped version")
        if f"{conflict_mod}_ext.sv" in names:
            problems.append(f"conflicting {conflict_mod}_ext.sv was not dropped")
        if "Unrelated.sv" not in names:
            problems.append("unrelated .sv was wrongly dropped")
        if "Top.top.mems.conf" not in names:
            problems.append(".top.mems.conf was wrongly dropped")
        if not stub_names.issubset(set(names)):
            problems.append("macro stubs were not appended")
        report["assemble_ok"] = not problems
        if problems:
            report["assemble_err"] = "; ".join(problems)

        # rename_ports: direct assertion of the per-type counters + (prefix,
        # masked) tuples (only otherwise reached indirectly via parse_mems_conf).
        rp_result, n_r, n_w, n_rw = rename_ports("write,write,read")
        rp_expected = ([("W0", False), ("W1", False), ("R0", False)], 1, 2, 0)
        report["rename_ports_ok"] = (rp_result, n_r, n_w, n_rw) == rp_expected
        if not report["rename_ports_ok"]:
            report["rename_ports_err"] = f"got {(rp_result, n_r, n_w, n_rw)!r}"
        # masked rw token -> RW0 masked True
        mrw_result, _, _, mrw_count = rename_ports("mrw")
        if mrw_result != [("RW0", True)] or mrw_count != 1:
            report["rename_ports_ok"] = False
            report["rename_ports_err"] = (
                (report["rename_ports_err"] or "") + f"; mrw -> {mrw_result!r}")

        # SRAMSpec.size_bytes: depth * width // 8 for each parsed spec.
        sb_problems = [
            f"{s.name}: size_bytes={s.size_bytes} != {s.depth * s.width // 8}"
            for s in specs if s.size_bytes != s.depth * s.width // 8
        ]
        report["size_bytes_ok"] = not sb_problems
        if sb_problems:
            report["size_bytes_err"] = "; ".join(sb_problems)
    except Exception as e:  # noqa: BLE001 - report any failure back to the host
        report["error"] = repr(e)

    return report


def _check_stage1(report: dict) -> bool:
    print(f"chipyard path : {report['chipyard_path']}")
    print(f"tapeout.jar   : {report['tapeout_jar']}  present={report['tapeout_jar_present']}")
    print(f"verilator     : {report['verilator']}")

    if report["error"]:
        print(f"  ! stage-1 task error: {report['error']}")
        if not report["tapeout_jar_present"]:
            print("    (tapeout.jar missing — rebuild the chia-chisel-build image, or "
                  "build it once: cd /home/ray/chipyard && make .classpath_cache/tapeout.jar)")
        return False

    ok = True
    print(f"\n=== parse + MDF library ===")
    print(f"specs parsed  : {report['num_specs']} {report['spec_names']}")
    print(f"MDF entries   : {report['lib_entries']} {report['lib_names']}")
    if report["lib_entries"] != report["num_specs"] or report["num_specs"] == 0:
        ok = False
        print("  ! expected one MDF entry per SRAM spec")

    print(f"\n=== MacroCompiler remap (real tapeout.jar) ===")
    print(f"remap_ok      : {report['remap_ok']}")
    print(f"modules       : {report['remapped_modules']}")
    print(f"mapped_ refs  : {report['mapped_refs']}")
    if not report["remap_ok"]:
        ok = False
    if report["missing_wrappers"]:
        ok = False
        print(f"  ! no wrapper module for: {report['missing_wrappers']}")
    if report["missing_macro_insts"]:
        ok = False
        print(f"  ! mapped_<name> macro not instantiated for: {report['missing_macro_insts']}")

    print(f"\n=== Verilator lint (remapped + stubs) ===")
    print(f"stubs         : {report['stubs']}")
    print(f"lint_ok       : {report['lint_ok']}")
    if report["lint_ok"] is False:
        ok = False
        print(f"  ! verilator lint error:\n{report['lint_err']}")
    elif report["lint_ok"] is None:
        print("  (verilator not found on the worker — lint skipped)")

    print(f"\n=== module-surface checks (assemble / rename_ports / size_bytes) ===")
    print(f"assemble_ok    : {report['assemble_ok']}")
    if report["assemble_ok"] is False:
        ok = False
        print(f"  ! assemble_generated_src_with_macros: {report['assemble_err']}")
    print(f"rename_ports_ok: {report['rename_ports_ok']}")
    if report["rename_ports_ok"] is False:
        ok = False
        print(f"  ! rename_ports: {report['rename_ports_err']}")
    print(f"size_bytes_ok  : {report['size_bytes_ok']}")
    if report["size_bytes_ok"] is False:
        ok = False
        print(f"  ! size_bytes: {report['size_bytes_err']}")
    return ok


def _check_stage2(chipyard_path: str) -> bool:
    """Dispatch remap_with_macrocompiler via .chia_remote and validate output."""
    from chia.chipyard.macrocompiler import (
        parse_mems_conf, generate_macrocompiler_lib, remap_with_macrocompiler,
    )

    print(f"\n=== remote dispatch (.chia_remote on the 'chipyard' resource) ===")
    specs = parse_mems_conf(SAMPLE_MEMS_CONF)
    lib_json = generate_macrocompiler_lib(specs, CELL_PREFIX)

    remapped = get(remap_with_macrocompiler.chia_remote(
        SAMPLE_MEMS_CONF, lib_json, chipyard_path))
    if not remapped:
        print("remap_with_macrocompiler.chia_remote -> None  FAIL")
        return False

    modules = set(re.findall(r"^\s*module\s+(\w+)", remapped, re.MULTILINE))
    wrappers_ok = all(s.name in modules for s in specs)
    insts_ok = all(f"{CELL_PREFIX}{s.name}" in remapped for s in specs)
    ok = wrappers_ok and insts_ok
    print(f"remap_with_macrocompiler.chia_remote: modules={sorted(modules)} "
          f"wrappers_ok={wrappers_ok} insts_ok={insts_ok} -> {'OK' if ok else 'FAIL'}")
    return ok


def main() -> int:
    print(f"[driver] connecting to ray cluster (working_dir={_REPO_ROOT})")
    # Ship the live repo as the worker working_dir: the chia-chisel-build image's
    # installed chia may predate chia.chipyard.macrocompiler, so workers must
    # import the source from here (same approach as torture_e2e_driver).
    ray.init(
        address="auto",
        runtime_env={
            "working_dir": _REPO_ROOT,
            "excludes": [".venv/**", ".git/**", "**/__pycache__/**",
                         "**/*.pyc", "**/.pytest_cache/**",
                         "**/HELLOLOG/**", "**/tags"],
        },
    )

    print("Submitting macrocompiler_validation_e2e to the 'chipyard' resource...\n")
    report = get(macrocompiler_validation_e2e.chia_remote(SAMPLE_MEMS_CONF, CHIPYARD_PATH))

    ok = _check_stage1(report)
    ok = _check_stage2(CHIPYARD_PATH) and ok

    print("\n" + ("PASS: MacroCompiler remapped every SRAM and the remapped "
                  "Verilog + stubs lint cleanly."
                  if ok else "FAIL: see the markers above."))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
