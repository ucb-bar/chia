"""Tier-2 validation: load the generated Liberty/LEF into the actual tool
readers, headlessly, without running synthesis or P&R.

  - Liberty -> OpenSTA `read_liberty`
  - LEF     -> OpenROAD `read_lef` (with a minimal tech LEF for context)

Both are skipped unless the tool binary is on PATH, so the default unit run
stays hermetic. The chia-cacti worker image ships OpenSTA, so the Liberty
check runs there; the LEF check runs wherever OpenROAD is available.
"""

import os
import re
import shutil
import subprocess

import pytest

from chia.vlsi.sram_cacti.liberty_gen import generate_liberty
from chia.vlsi.sram_cacti.lef_gen import generate_lef

PORTS = ["rw", "read,write", "mrw"]

# A macro-only (cell) LEF can't be read in isolation — readers need the layers
# and units a tech LEF provides. This minimal tech LEF defines just enough for
# OpenROAD to resolve the met1-met4 layers our macro LEF references.
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


def _no_error_lines(output: str) -> bool:
    return not any(line.lstrip().startswith("Error") for line in output.splitlines())


def lint_lef(text: str) -> list[str]:
    """Lightweight structural check for the LEF subset generate_lef emits.

    Dependency-free (no OpenROAD/KLayout): validates general LEF well-formedness
    — VERSION/END LIBRARY framing, balanced MACRO/PIN/PORT/OBS...END nesting,
    a numeric SIZE per macro, and 4-coordinate RECT statements. Returns a list
    of problem strings (empty == structurally OK).
    """
    problems: list[str] = []
    lines = [ln.strip() for ln in text.splitlines()]

    if not any(ln.startswith("VERSION") for ln in lines):
        problems.append("missing VERSION")
    if "END LIBRARY" not in text:
        problems.append("missing END LIBRARY")

    stack: list[tuple[str, str | None]] = []   # (kind, name) for MACRO/PIN/PORT/OBS
    current_macro: str | None = None
    macro_has_size: dict[str, bool] = {}

    for n, line in enumerate(lines, 1):
        if not line:
            continue
        parts = line.split()
        kw = parts[0]

        if kw == "MACRO":
            name = parts[1] if len(parts) > 1 else None
            stack.append(("MACRO", name))
            current_macro = name
            macro_has_size[name] = False
        elif kw == "PIN":
            stack.append(("PIN", parts[1] if len(parts) > 1 else None))
        elif kw in ("PORT", "OBS"):
            stack.append((kw, None))
        elif kw == "SIZE":
            if not re.match(r"SIZE\s+[\d.]+\s+BY\s+[\d.]+\s*;", line):
                problems.append(f"line {n}: malformed SIZE: {line!r}")
            elif current_macro is not None:
                macro_has_size[current_macro] = True
        elif kw == "RECT":
            if not re.match(r"RECT(\s+-?[\d.]+){4}\s*;", line):
                problems.append(f"line {n}: malformed RECT: {line!r}")
        elif kw == "END":
            rest = parts[1:]
            if rest == ["LIBRARY"]:
                continue
            if not rest:                                    # bare END closes PORT/OBS
                if stack and stack[-1][0] in ("PORT", "OBS"):
                    stack.pop()
                else:
                    problems.append(f"line {n}: unexpected bare END")
            else:                                           # END <name> closes MACRO/PIN
                name = rest[0]
                if stack and stack[-1][1] == name:
                    kind, _ = stack.pop()
                    if kind == "MACRO":
                        current_macro = None
                else:
                    top = stack[-1] if stack else "nothing open"
                    problems.append(f"line {n}: 'END {name}' does not close {top}")

    if stack:
        problems.append(f"unclosed blocks: {stack}")
    for name, has_size in macro_has_size.items():
        if not has_size:
            problems.append(f"MACRO {name} missing SIZE")
    return problems


@pytest.mark.parametrize("ports", ["rw", "read,write", "mrw", "read,read,mwrite", "rw,read"])
def test_lef_structurally_valid(make_spec, make_result, ports):
    """Ungated: every generated LEF passes the structural lint (runs everywhere)."""
    lef = generate_lef(make_spec(width=32, mask_gran=8, ports=ports),
                       make_result(width_mm=0.2, height_mm=0.1))
    assert lint_lef(lef) == [], lint_lef(lef)


def test_lint_catches_missing_end_library(make_spec, make_result):
    lef = generate_lef(make_spec(), make_result()).replace("END LIBRARY", "")
    assert any("END LIBRARY" in p for p in lint_lef(lef))


def test_lint_catches_unbalanced_block(make_spec, make_result):
    # drop the macro's closing "END <name>" -> should report an unclosed block
    lef = generate_lef(make_spec(name="m"), make_result())
    broken = lef.replace("END cacti_m", "")
    assert any("unclosed" in p or "does not close" in p for p in lint_lef(broken))


def test_lint_catches_malformed_rect():
    bad = (
        "VERSION 5.7 ;\n"
        "MACRO m\n"
        "  SIZE 1.000 BY 1.000 ;\n"
        "  OBS\n"
        "    LAYER met1 ;\n"
        "      RECT 0 0 1 ;\n"   # only 3 coordinates
        "  END\n"
        "END m\n"
        "END LIBRARY\n"
    )
    assert any("RECT" in p for p in lint_lef(bad))


def test_lint_catches_missing_size():
    bad = "VERSION 5.7 ;\nMACRO m\n  CLASS BLOCK ;\nEND m\nEND LIBRARY\n"
    assert any("missing SIZE" in p for p in lint_lef(bad))


_STA = shutil.which("sta")
_OPENROAD = shutil.which("openroad")
# Both OpenSTA and OpenROAD implement `read_liberty`; only OpenROAD reads LEF.
# The chia-cacti image ships OpenROAD (which embeds the OpenSTA engine).
_LIBERTY_TOOL = _STA or _OPENROAD


def _run_tcl(tool: str, script_path: str) -> subprocess.CompletedProcess:
    """Run a Tcl script headlessly under OpenSTA (`sta`) or OpenROAD."""
    if os.path.basename(tool) == "sta":
        cmd = [tool, "-no_splash", "-exit", script_path]
    else:  # openroad
        cmd = [tool, "-no_init", "-exit", script_path]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=120)


@pytest.mark.skipif(_LIBERTY_TOOL is None, reason="neither sta nor openroad on PATH")
@pytest.mark.parametrize("ports", PORTS)
def test_liberty_loads_in_timing_tool(tmp_path, make_spec, make_result, ports):
    lib = tmp_path / "cell.lib"
    lib.write_text(generate_liberty(make_spec(width=32, mask_gran=8, ports=ports), make_result()))
    script = tmp_path / "check.tcl"
    script.write_text(f"read_liberty {lib}\nexit 0\n")

    r = _run_tcl(_LIBERTY_TOOL, str(script))
    out = r.stdout + r.stderr
    assert r.returncode == 0, out          # a read_liberty parse error aborts the tool (nonzero)
    assert _no_error_lines(out), out


@pytest.mark.skipif(_OPENROAD is None, reason="OpenROAD (read_lef) not installed")
@pytest.mark.parametrize("ports", PORTS)
def test_lef_loads_in_openroad(tmp_path, make_spec, make_result, ports):
    tech = tmp_path / "tech.lef"
    tech.write_text(_TECH_LEF)
    macro = tmp_path / "macro.lef"
    macro.write_text(generate_lef(make_spec(width=32, mask_gran=8, ports=ports), make_result()))
    script = tmp_path / "check.tcl"
    script.write_text(f"read_lef {tech}\nread_lef {macro}\nexit 0\n")

    r = _run_tcl(_OPENROAD, str(script))
    out = r.stdout + r.stderr
    assert r.returncode == 0, out
