"""End-to-end smoke test for the chia-cacti worker.

Submits a single task to the "cacti" resource (so it lands in the
ghcr.io/ucb-bar/chia-cacti container) that exercises the SRAM/CACTI pipeline
against the REAL tools (cacti + OpenROAD/OpenSTA) across several code paths and
validates every piece of generated collateral, headlessly — no synthesis/P&R.

Scenarios exercised on the worker (all validated with real read_liberty /
read_lef / read_verilog):

  1. Default ``cacti_`` prefix — real CACTI characterize; validates EVERY
     Liberty corner (ff/ss/tt) and the LEF for each large SRAM.
  2. Extmodule mode (``cell_prefix=""``, the firtool ``--repl-seq-mem`` naming
     path) — real CACTI; validates the bare-named Liberty/LEF read.
  3. Analytical fallback — CACTI is forced to fail (``/bin/false``) so the
     analytical_area_estimate path runs; validates the fallback-derived
     Liberty/LEF still load in the real tools.
  4. MacroCompiler MDF library (generate_cacti_macrocompiler_lib) — well-formed
     JSON with one entry per characterized macro.
  5. MacroCompiler blackbox Verilog stubs (generate_macro_stubs) — parsed by
     OpenROAD read_verilog.

The whole thing runs on the worker (where cacti + openroad live); the host
driver just submits it and prints the report.

The driver then runs a second stage that dispatches the core ChiaFunctions
(run_cacti, characterize_top_mems_conf_with_cacti, characterize_srams_with_cacti)
via ``.chia_remote`` so the Ray scheduling path (the ``cacti`` resource
requirement, the trampoline, and serialization of the SRAMSpec/CACTIResult/
CactiCharacterization dataclasses across the worker boundary) is exercised too —
not just the plain in-process calls the first task makes.

Usage (host, chia env active, cluster up via cacti_local.yaml):
    cd <repo root>
    python chia/vlsi/sram_cacti/cluster/cacti_smoke_test.py
"""

import os
import sys

# Allow `python .../cacti_smoke_test.py` from any cwd: put the repo root on
# sys.path so the `chia` namespace package (resolved via path, not installed)
# imports. cluster -> sram_cacti -> vlsi -> chia -> <repo root> == 4 levels up.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), *([os.pardir] * 4))))

import ray

from chia.base.ChiaFunction import ChiaFunction, get

# A few representative SRAMs (mixed sizes/ports, one masked, one tiny synflop).
SAMPLE_MEMS_CONF = "\n".join([
    "name dcache_data depth 2048 width 64 ports rw",
    "name icache_tag  depth 512  width 88 ports read,write",
    "name regfile     depth 64   width 64 ports mrw mask_gran 8",
    "name tiny_q      depth 8    width 8  ports rw",   # < threshold -> stays synflop
])

# Minimal tech LEF so the macro-only .lef can be read in isolation.
_TECH_LEF = """\
VERSION 5.7 ;
BUSBITCHARS "[]" ;
DIVIDERCHAR "/" ;
UNITS
  DATABASE MICRONS 1000 ;
END UNITS
LAYER met1
  TYPE ROUTING ;
END met1
LAYER met2
  TYPE ROUTING ;
END met2
LAYER met3
  TYPE ROUTING ;
END met3
LAYER met4
  TYPE ROUTING ;
END met4
END LIBRARY
"""


@ChiaFunction(resources={"cacti": 1})
def cacti_validation_smoke(mems_conf: str) -> dict:
    """Runs on a cacti worker: real CACTI characterize + OpenROAD validation.

    Drives several characterization scenarios (default prefix, extmodule mode,
    analytical fallback) plus the MacroCompiler MDF/stub generators, validating
    every Liberty corner, LEF, and Verilog stub with the real tools.
    """
    import json
    import os
    import shutil
    import subprocess
    import tempfile

    # Resolve tools from the container's PATH. cacti lives at /scratch/cacti.
    # read_liberty works in OpenSTA (`sta`) or OpenROAD; read_lef/read_verilog
    # are OpenROAD-only.
    cacti_bin = shutil.which("cacti") or "/scratch/cacti/cacti"
    liberty_tool = shutil.which("sta") or shutil.which("openroad")
    lef_tool = shutil.which("openroad")
    verilog_tool = shutil.which("openroad")
    # A binary that always exits non-zero, to force CACTI's failure path so the
    # analytical fallback runs deterministically.
    failing_cacti = shutil.which("false") or "/bin/false"

    from chia.vlsi.sram_cacti.sram_characterize import characterize_top_mems_conf_with_cacti
    from chia.vlsi.sram_cacti.cacti_macrocompiler import generate_cacti_macrocompiler_lib
    from chia.chipyard.macrocompiler import generate_macro_stubs

    def _run_tcl(tool, script_text):
        with tempfile.NamedTemporaryFile("w", suffix=".tcl", delete=False) as f:
            f.write(script_text)
            path = f.name
        argv = ([tool, "-no_splash", "-exit", path] if os.path.basename(tool) == "sta"
                else [tool, "-no_init", "-exit", path])
        r = subprocess.run(argv, capture_output=True, text=True, timeout=120)
        os.unlink(path)
        return r.returncode == 0, (r.stdout + r.stderr)

    def _validate_sram(s):
        """Validate every Liberty corner and the LEF for one characterized SRAM."""
        used_fallback = s.result.full_out.startswith("This is a fake")
        entry = {
            "name": s.name,
            "area_um2": round(s.result.area_um2, 1),
            "access_ns": round(s.result.access_time_ns, 4),
            "real_cacti": not used_fallback,
            "corners": {},        # corner_suffix -> {"valid": bool, "err": str|None}
            "lef_valid": None,
            "lef_err": None,
        }
        with tempfile.TemporaryDirectory() as d:
            if liberty_tool:
                # Validate EVERY corner, not just tt — ff/ss differ only in the
                # temperature/voltage header fields a malformed corner would trip on.
                for suffix, content in sorted(s.lib_contents.items()):
                    libp = os.path.join(d, f"{s.name}_{suffix}.lib")
                    with open(libp, "w") as f:
                        f.write(content)
                    ok_lib, lib_out = _run_tcl(liberty_tool, f"read_liberty {libp}\nexit 0\n")
                    entry["corners"][suffix] = {
                        "valid": ok_lib,
                        "err": None if ok_lib else lib_out[-400:],
                    }

            if lef_tool:
                techp = os.path.join(d, "tech.lef")
                macrop = os.path.join(d, f"{s.name}.lef")
                with open(techp, "w") as f:
                    f.write(_TECH_LEF)
                with open(macrop, "w") as f:
                    f.write(s.lef_content)
                ok_lef, lef_out = _run_tcl(lef_tool, f"read_lef {techp}\nread_lef {macrop}\nexit 0\n")
                entry["lef_valid"] = ok_lef
                if not ok_lef:
                    entry["lef_err"] = lef_out[-400:]
        return entry

    def _run_scenario(label, cacti_path, cell_prefix, expect_real_cacti):
        # Plain call — we're already on the cacti worker.
        char = characterize_top_mems_conf_with_cacti(
            [("Top.top.mems.conf", mems_conf)], cacti_path, cell_prefix=cell_prefix)
        return {
            "label": label,
            "cell_prefix": cell_prefix,
            "expect_real_cacti": expect_real_cacti,
            "num_srams": len(char.srams),
            "srams": [_validate_sram(s) for s in char.srams],
            "_char": char,   # kept for the macrocompiler stage; stripped before return
        }

    report = {
        "cacti_bin": cacti_bin,
        "liberty_tool": liberty_tool,
        "lef_tool": lef_tool,
        "verilog_tool": verilog_tool,
        "scenarios": [],
        "macrocompiler": {},
    }

    scenarios = [
        _run_scenario("default (cacti_ prefix)", cacti_bin, "cacti_", expect_real_cacti=True),
        _run_scenario("extmodule mode (no prefix)", cacti_bin, "", expect_real_cacti=True),
        _run_scenario("analytical fallback", failing_cacti, "cacti_", expect_real_cacti=False),
    ]

    # MacroCompiler MDF library + blackbox Verilog stubs for the default-scenario
    # macros (exercises the chipyard.macrocompiler generators, untouched above).
    specs = [s.spec for s in scenarios[0]["_char"].srams]
    mc = {"mdf_ok": None, "mdf_err": None, "macros": [], "stubs": []}
    try:
        mdf_json = generate_cacti_macrocompiler_lib(specs)
        entries = json.loads(mdf_json)
        names = {e.get("name") for e in entries}
        expected = {f"cacti_{spec.name}" for spec in specs}
        mc["mdf_ok"] = expected.issubset(names) and len(entries) == len(specs)
        mc["macros"] = sorted(names)
        if not mc["mdf_ok"]:
            mc["mdf_err"] = f"expected macros {sorted(expected)}, got {sorted(names)}"
    except Exception as e:  # noqa: BLE001 - report any malformed MDF
        mc["mdf_ok"] = False
        mc["mdf_err"] = repr(e)

    if verilog_tool:
        for stub_name, stub_content in generate_macro_stubs(specs, "cacti_"):
            with tempfile.TemporaryDirectory() as d:
                vp = os.path.join(d, stub_name)
                with open(vp, "w") as f:
                    f.write(stub_content)
                ok_v, v_out = _run_tcl(verilog_tool, f"read_verilog {vp}\nexit 0\n")
            mc["stubs"].append({
                "name": stub_name,
                "valid": ok_v,
                "err": None if ok_v else v_out[-400:],
            })
    report["macrocompiler"] = mc

    # Strip the non-serializable char objects before returning to the host.
    for sc in scenarios:
        sc.pop("_char", None)
    report["scenarios"] = scenarios
    return report


def _print_scenario(scenario) -> bool:
    """Print one scenario's per-SRAM table; return True if it fully validated."""
    print(f"\n=== scenario: {scenario['label']} "
          f"(prefix={scenario['cell_prefix']!r}, expect_real_cacti={scenario['expect_real_cacti']}) ===")

    if scenario["num_srams"] == 0:
        print("  ! no SRAMs characterized — check the mems.conf / threshold.")
        return False

    hdr = f"{'sram':<14}{'area_um2':>12}{'access_ns':>11}{'real_cacti':>12}{'corners_ok':>12}{'lef_ok':>8}"
    print(hdr)
    print("-" * len(hdr))

    ok = True
    for s in scenario["srams"]:
        corners = s["corners"]
        corners_ok = bool(corners) and all(c["valid"] for c in corners.values())
        n_ok = sum(1 for c in corners.values() if c["valid"])
        corners_label = f"{n_ok}/{len(corners)}"
        print(f"{s['name']:<14}{s['area_um2']:>12}{s['access_ns']:>11}"
              f"{str(s['real_cacti']):>12}{corners_label:>12}{str(s['lef_valid']):>8}")

        if s["real_cacti"] != scenario["expect_real_cacti"]:
            ok = False
            print(f"   ! {s['name']}: real_cacti={s['real_cacti']} but expected "
                  f"{scenario['expect_real_cacti']}")
        if not corners_ok:
            ok = False
            for suffix, c in sorted(corners.items()):
                if not c["valid"]:
                    print(f"   ! {s['name']} corner {suffix} liberty error:\n{c['err']}")
        if s["lef_valid"] is False:
            ok = False
            print(f"   ! {s['name']} lef error:\n{s['lef_err']}")
    return ok


def _remote_dispatch_checks(cacti_bin: str) -> bool:
    """Dispatch the core ChiaFunctions via ``.chia_remote`` and check results.

    Unlike the first task (which calls everything as plain in-process
    functions), this submits run_cacti / characterize_* as their own Ray tasks
    on the ``cacti`` resource — exercising the trampoline, cluster scheduling,
    and dataclass serialization across the worker boundary. ``cacti_bin`` is the
    worker-resolved path from the first task, so it is valid in-container.
    """
    from chia.vlsi.sram_cacti.cacti_runner import parse_mems_conf, run_cacti
    from chia.vlsi.sram_cacti.sram_characterize import (
        characterize_top_mems_conf_with_cacti,
        characterize_srams_with_cacti,
        DEFAULT_SYNFLOP_THRESHOLD_BYTES as _THRESH,
    )

    _CORNERS = {"ff_n40C_1v95", "ss_100C_1v60", "tt_025C_1v80"}
    src = [("Top.top.mems.conf", SAMPLE_MEMS_CONF)]
    specs = parse_mems_conf(SAMPLE_MEMS_CONF)
    large = [s for s in specs if s.size_bytes >= _THRESH]
    expected = {s.name for s in large}

    print(f"\n=== remote dispatch (.chia_remote on the 'cacti' resource) ===")
    ok = True

    # 1) characterize_top_mems_conf_with_cacti — parses the conf, then characterizes.
    char = get(characterize_top_mems_conf_with_cacti.chia_remote(src, cacti_bin))
    names = set(char.sram_names)
    real = bool(char.srams) and all(not s.result.full_out.startswith("This is a fake")
                                    for s in char.srams)
    corners_ok = all(set(s.lib_contents) == _CORNERS for s in char.srams)
    lef_ok = all(s.lef_content for s in char.srams)
    top_ok = names == expected and real and corners_ok and lef_ok
    ok = ok and top_ok
    print(f"characterize_top_mems_conf_with_cacti.chia_remote: srams={sorted(names)} "
          f"real_cacti={real} corners_ok={corners_ok} -> {'OK' if top_ok else 'FAIL'}")
    if not top_ok:
        print(f"   ! expected srams={sorted(expected)}, all real cacti, 3 corners + LEF each")

    # 2) characterize_srams_with_cacti — the sibling entry point (pre-parsed specs).
    char2 = get(characterize_srams_with_cacti.chia_remote(specs, src, cacti_bin))
    srams_ok = set(char2.sram_names) == expected
    ok = ok and srams_ok
    print(f"characterize_srams_with_cacti.chia_remote:         srams={sorted(char2.sram_names)} "
          f"-> {'OK' if srams_ok else 'FAIL'}")

    # 3) run_cacti — a single large spec; returns a CACTIResult (None on failure).
    spec = max(large, key=lambda s: s.size_bytes)
    res = get(run_cacti.chia_remote(spec, cacti_path=cacti_bin))
    run_ok = res is not None and res.area_um2 > 0 and not res.full_out.startswith("This is a fake")
    ok = ok and run_ok
    area = round(res.area_um2, 1) if res is not None else None
    print(f"run_cacti.chia_remote({spec.name}): area_um2={area} -> {'OK' if run_ok else 'FAIL'}")
    if res is None:
        print("   ! run_cacti returned None (real CACTI failed on the worker)")
    return ok


def main() -> int:
    ray.init(address="auto")
    print("Submitting cacti_validation_smoke to the 'cacti' resource...\n")
    report = get(cacti_validation_smoke.chia_remote(SAMPLE_MEMS_CONF))

    print(f"cacti binary : {report['cacti_bin']}")
    print(f"liberty tool : {report['liberty_tool']}  (read_liberty)")
    print(f"lef tool     : {report['lef_tool']}  (read_lef)")
    print(f"verilog tool : {report['verilog_tool']}  (read_verilog)")

    ok = True
    for scenario in report["scenarios"]:
        ok = _print_scenario(scenario) and ok

    # MacroCompiler MDF + stub validation
    mc = report["macrocompiler"]
    print(f"\n=== macrocompiler collateral ===")
    print(f"MDF library  : ok={mc['mdf_ok']}  macros={mc['macros']}")
    if not mc["mdf_ok"]:
        ok = False
        print(f"   ! MDF error: {mc['mdf_err']}")
    if mc["stubs"]:
        n_ok = sum(1 for st in mc["stubs"] if st["valid"])
        print(f"Verilog stubs: {n_ok}/{len(mc['stubs'])} parsed by read_verilog")
        for st in mc["stubs"]:
            if not st["valid"]:
                ok = False
                print(f"   ! stub {st['name']} read_verilog error:\n{st['err']}")

    # Second stage: exercise the .chia_remote dispatch path for the core
    # ChiaFunctions (reusing the worker-resolved cacti path from stage one).
    ok = _remote_dispatch_checks(report["cacti_bin"]) and ok

    print("\n" + ("PASS: all scenarios characterized and every Liberty/LEF/stub validated."
                  if ok else "FAIL: see the markers above."))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
