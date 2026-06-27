"""Top-level orchestration: characterize SRAMs with CACTI and generate Liberty files.

Parses .top.mems.conf from generated_src, runs CACTI for large SRAMs,
generates Liberty files, and optionally runs MacroCompiler to remap synflop
_ext modules to CACTI-characterized library macros.
"""

import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass, field

from chia.vlsi.sram_cacti.cacti_runner import (
    SRAMSpec,
    CACTIResult,
    parse_mems_conf,
    run_cacti,
    analytical_area_estimate,
)
from chia.vlsi.sram_cacti.lef_gen import generate_lef
from chia.vlsi.sram_cacti.liberty_gen import generate_liberty, LibertyCorner, SKY130_CORNERS

from chia.vlsi.sram_cacti.cacti_macrocompiler import (  # noqa: F401
    generate_cacti_macrocompiler_lib,
)
from chia.chipyard.macrocompiler import (  # noqa: F401
    remap_with_macrocompiler,
    assemble_generated_src_with_macros,
    _compute_family,
)
from chia.base.ChiaFunction import ChiaFunction

# Back-compat alias: this assembly step moved to chia.chipyard.macrocompiler and
# was renamed (it's MacroCompiler-specific, not CACTI-specific). Existing call
# sites still import the old name from here.
assemble_generated_src_with_cacti = assemble_generated_src_with_macros

logger = logging.getLogger(__name__)

# SRAMs smaller than this (in bytes) are kept as synflops
DEFAULT_SYNFLOP_THRESHOLD_BYTES = 256


@dataclass
class CharacterizedSRAM:
    """A single CACTI-characterized SRAM and its generated collateral."""
    name: str
    spec: SRAMSpec
    result: CACTIResult            # CACTI (or analytical) area/timing result
    lib_contents: dict[str, str]   # corner lib_suffix -> Liberty content
    lef_content: str

    def to_lib_dict(self) -> dict:
        """Legacy dict shape."""
        return {
            "name": self.name,
            "lib_contents": self.lib_contents,
            "lef_content": self.lef_content,
        }


@dataclass
class CactiCharacterization:
    """Result of :func:`characterize_top_mems_conf_with_cacti`.

    The ``sram_libs`` / ``sram_names`` views are derived from ``srams`` so the
    existing dict-based consumers keep working, while ``srams`` exposes the
    full per-SRAM CACTI result.
    """
    generated_src_files: list[tuple[str, str]]
    srams: list[CharacterizedSRAM] = field(default_factory=list)

    @property
    def sram_names(self) -> list[str]:
        return [s.name for s in self.srams]

    @property
    def sram_libs(self) -> list[dict]:
        return [s.to_lib_dict() for s in self.srams]


@ChiaFunction(resources={"cacti": 1})
def characterize_top_mems_conf_with_cacti(
    generated_src_files: list[tuple[str, str]],
    cacti_path: str,
    synflop_threshold_bytes: int = DEFAULT_SYNFLOP_THRESHOLD_BYTES,
    technology_um: float = 0.130,
    cell_prefix: str = "cacti_",
    corners: Sequence[LibertyCorner] = SKY130_CORNERS,
) -> "CactiCharacterization":
    """Firtool specific characterize SRAMs with CACTI and generate Liberty files.

    Finds .top.mems.conf in generated_src_files, runs CACTI for each large
    SRAM, generates .lib files, and replaces .top.mems.v to remove synflop
    definitions for SRAMs that become library cells.

    Small SRAMs (< synflop_threshold_bytes) keep their synflop implementations
    and are synthesized as flip-flop arrays by Genus.

    Args:
        generated_src_files: List of (filename, contents) from the build.
        cacti_path: Path to the CACTI binary.
        synflop_threshold_bytes: SRAMs smaller than this stay as synflops.
        technology_um: Technology node in microns (default 0.130 for Sky130).
        cell_prefix: Prefix on the Liberty/LEF cell name. Default ``cacti_``
            disambiguates from chipyard's .top.mems.v synflop wrapper for the
            MacroCompiler remap step. Pass "" when the caller's Verilog refers
            to the bare _ext module directly (e.g. firtool --repl-seq-mem),
            so Genus can link the extmodule reference to the Liberty cell
            without an intervening rewrite.
        corners: Liberty corners to characterize each SRAM at. Defaults to
            SKY130_CORNERS (ff/ss/tt). Each corner produces one entry in the
            per-SRAM lib_contents dict, keyed by its lib_suffix.

    Returns:
        A CactiCharacterization with:

        - generated_src_files: the (unmodified) input files
        - srams: a CharacterizedSRAM per large SRAM (name, spec, CACTI result,
          per-corner Liberty contents, and LEF content)

        It also exposes .sram_libs (legacy list-of-dicts) and .sram_names views.
    """
    # Find .top.mems.conf
    mems_conf_content = None
    mems_conf_name = None
    for fname, content in generated_src_files:
        if fname.endswith(".top.mems.conf"):
            mems_conf_content = content
            mems_conf_name = fname
            break

    if mems_conf_content is None:
        print("  [cacti] No .top.mems.conf found — skipping SRAM characterization")
        return CactiCharacterization(generated_src_files=generated_src_files)

    # Parse SRAM specs
    all_specs = parse_mems_conf(mems_conf_content)
    print(f"  [cacti] Parsed {len(all_specs)} SRAMs from {mems_conf_name}")

    return characterize_srams_with_cacti(
        all_specs,
        generated_src_files,
        cacti_path,
        synflop_threshold_bytes=synflop_threshold_bytes,
        technology_um=technology_um,
        cell_prefix=cell_prefix,
        corners=corners,
    )


@ChiaFunction(resources={"cacti": 1})
def characterize_srams_with_cacti(
    sram_specs: list[SRAMSpec],
    generated_src_files: list[tuple[str, str]],
    cacti_path: str,
    synflop_threshold_bytes: int = DEFAULT_SYNFLOP_THRESHOLD_BYTES,
    technology_um: float = 0.130,
    cell_prefix: str = "cacti_",
    corners: Sequence[LibertyCorner] = SKY130_CORNERS,
) -> "CactiCharacterization":
    """Characterize SRAM specs with CACTI and generate Liberty/LEF.

    Splits sram_specs into tiny SRAMs (kept as synflops, below
    synflop_threshold_bytes) and large SRAMs (CACTI-characterized), runs CACTI
    for the large ones, and returns a CactiCharacterization. generated_src_files
    is passed through into the result unchanged.

    See characterize_top_mems_conf_with_cacti for the variant that first parses
    the specs out of a .top.mems.conf in generated_src_files.
    """
    # Split into tiny (synflop) and large (CACTI)
    tiny_specs = [s for s in sram_specs if s.size_bytes < synflop_threshold_bytes]
    large_specs = [s for s in sram_specs if s.size_bytes >= synflop_threshold_bytes]

    print(f"  [cacti] Tiny SRAMs (synflop, < {synflop_threshold_bytes}B): {len(tiny_specs)}")
    for s in tiny_specs:
        print(f"    - {s.name}: {s.depth}x{s.width} {s.ports} ({s.size_bytes}B)")
    print(f"  [cacti] Large SRAMs (CACTI characterization): {len(large_specs)}")
    for s in large_specs:
        print(f"    - {s.name}: {s.depth}x{s.width} {s.ports} ({s.size_bytes}B)")

    # Run CACTI and generate Liberty for large SRAMs
    srams: list[CharacterizedSRAM] = []

    for spec in large_specs:
        print(f"  [cacti] Characterizing {spec.name} ({spec.depth}x{spec.width}, {spec.ports})...")
        result = run_cacti(spec, technology_um=technology_um, cacti_path=cacti_path)

        if result is None:
            print(f"  [cacti]   CACTI failed — using analytical fallback")
            result = analytical_area_estimate(spec)

        print(f"  [cacti]   Area: {result.area_um2:.0f} um2, "
              f"Access: {result.access_time_ns:.3f} ns, "
              f"Leakage: {result.leakage_power_mw:.4f} mW")

        # Generate Liberty content for each requested corner, keyed by suffix
        corner_libs = {
            corner.lib_suffix: generate_liberty(spec, result, corner, cell_prefix=cell_prefix)
            for corner in corners
        }
        # Generate LEF (one per SRAM — geometry is not corner-dependent)
        lef_content = generate_lef(spec, result, cell_prefix=cell_prefix)
        srams.append(CharacterizedSRAM(
            name=spec.name,
            spec=spec,
            result=result,
            lib_contents=corner_libs,  # dict: corner_suffix -> lib content
            lef_content=lef_content,
        ))

    print(f"  [cacti] Generated {len(srams)} Liberty definitions ({len(corners)} corners each)")

    # Don't modify generated_src here — the MacroCompiler remap step will
    # replace .top.mems.v with CACTI-mapped versions.
    return CactiCharacterization(generated_src_files=generated_src_files, srams=srams)