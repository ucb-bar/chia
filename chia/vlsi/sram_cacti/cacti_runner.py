"""Methods to run CACTI 7, and return characterization results."""

import logging
import math
import os
import re
import subprocess
import tempfile
from dataclasses import dataclass, field

# parse_mems_conf / rename_ports moved to chia.chipyard.macrocompiler (they parse
# the Chipyard .top.mems.conf, not CACTI); re-exported here for existing callers.
from chia.chipyard.macrocompiler import (  # noqa: F401
    SRAMSpec,
    parse_mems_conf,
    rename_ports,
)
from chia.base.ChiaFunction import ChiaFunction

logger = logging.getLogger(__name__)


@dataclass
class CACTIResult:
    access_time_ns: float
    cycle_time_ns: float
    read_energy_nj: float
    leakage_power_mw: float
    height_mm: float
    width_mm: float
    area_um2: float
    full_out: str


def _block_size_bytes(width_bits: int) -> int:
    """CACTI line size for a width-bit word: the word rounded up to whole bytes.

    CACTI's only block-size constraint is ``block >= ceil(width/8)`` — it does
    NOT require a power of two (verified across every BOOM SRAM geometry). We
    deliberately do NOT pad up to a power-of-two byte count. That padding
    inflated narrow SRAMs (e.g. an 8-bit array characterized 8x too wide), and
    the area-only "correction" that tried to undo it only touched ``area_um2``
    (the Liberty ``area``) — never ``height_mm``/``width_mm``, which build the
    LEF ``SIZE`` that physical-mode Genus actually reports. So the correction
    was dead for the reported area and left Liberty and LEF inconsistent.
    """
    return max(1, math.ceil(width_bits / 8))


def _generate_cacti_cfg(spec: SRAMSpec, technology_um: float = 0.130) -> str:
    """Generate a CACTI 7 config file for the given SRAM spec."""
    block_size_bytes = _block_size_bytes(spec.width)
    size_bytes = spec.depth * block_size_bytes

    return f"""\
-size (bytes) {size_bytes}
-block size (bytes) {block_size_bytes}
-associativity 1
-read-write port {spec.num_rw_ports}
-exclusive read port {spec.num_read_ports}
-exclusive write port {spec.num_write_ports}
-single ended read ports 0
-UCA bank count 1
-technology (u) {technology_um}
-page size (bits) 8192
-burst length 8
-internal prefetch width 8
-Data array cell type - "itrs-hp"
-Data array peripheral type - "itrs-hp"
-Tag array cell type - "itrs-hp"
-Tag array peripheral type - "itrs-hp"
-output/input bus width {spec.width}
-operating temperature (K) 360
-cache type "ram"
-tag size (b) "default"
-access mode (normal, sequential, fast) - "normal"
-design objective (weight delay, dynamic power, leakage power, cycle time, area) 0:0:0:0:100
-deviate (delay, dynamic power, leakage power, cycle time, area) 100000:100000:100000:100000:100000
-Optimize ED or ED^2 (ED, ED^2, NONE): "NONE"
-Cache model (NUCA, UCA)  - "UCA"
-NUCA bank count 0
-Wire signaling (fullswing, lowswing, default) - "Global_30"
-Wire inside mat - "semi-global"
-Wire outside mat - "semi-global"
-Interconnect projection - "conservative"
-Core count 8
-Cache level (L2/L3) - "L3"
-Add ECC - "false"
-Print level (DETAILED, CONCISE) - "CONCISE"
-Print input parameters - "false"
-Force cache config - "false"
-Ndwl 1
-Ndbl 1
-Nspd 0
-Ndcm 1
-Ndsam1 0
-Ndsam2 0
-dram_type "DDR3"
-io state "WRITE"
-addr_timing 1.0
-mem_density 4 Gb
-bus_freq 800 MHz
-duty_cycle 1.0
-activity_dq 1.0
-activity_ca 0.5
-num_dq 72
-num_dqs 18
-num_ca 25
-num_clk 2
-num_mem_dq 2
-mem_data_width 8
-rtt_value 10000
-ron_value 34
-tflight_value
-num_bobs 1
-capacity 80
-num_channels_per_bob 1
-first metric "Cost"
-second metric "Bandwidth"
-third metric "Energy"
-DIMM model "ALL"
-mirror_in_bob "F"
"""


_ACCESS_TIME_RE = re.compile(r"Access time \(ns\):\s*([\d.eE+\-]+)")
_CYCLE_TIME_RE = re.compile(r"Cycle time \(ns\):\s*([\d.eE+\-]+)")
_READ_ENERGY_RE = re.compile(r"Total dynamic read energy per access \(nJ\):\s*([\d.eE+\-]+)")
_LEAKAGE_RE = re.compile(r"Total leakage power of a bank\s*\(mW\):\s*([\d.eE+\-]+)")
_DIMENSIONS_RE = re.compile(r"Cache height x width \(mm\):\s*([\d.eE+\-]+)\s*x\s*([\d.eE+\-]+)")


@ChiaFunction(resources={"cacti": 1})
def run_cacti(
    spec: SRAMSpec,
    technology_um: float = 0.130,
    cacti_path: str = "cacti",
) -> CACTIResult | None:
    """Run CACTI for a single SRAM spec and return parsed results.

    Returns None if CACTI fails.
    """
    cfg_content = _generate_cacti_cfg(spec, technology_um)

    work_dir = tempfile.mkdtemp(prefix=f"cacti_{spec.name}_")
    cfg_path = os.path.join(work_dir, "sram.cfg")
    with open(cfg_path, "w") as f:
        f.write(cfg_content)

    try:
        result = subprocess.run(
            [cacti_path, "-infile", cfg_path],
            capture_output=True, text=True, timeout=30,
            # dirname is "" for a bare "cacti" on PATH; pass None (cwd unchanged)
            # rather than "" (which subprocess rejects).
            cwd=os.path.dirname(cacti_path) or None,
        )
    except subprocess.TimeoutExpired:
        logger.warning(f"CACTI timed out for {spec.name}")
        return None

    if result.returncode != 0:
        logger.warning(f"CACTI failed for {spec.name} (rc={result.returncode}): "
                       f"{result.stderr[:200]}")
        return None

    stdout = result.stdout

    access_m = _ACCESS_TIME_RE.search(stdout)
    cycle_m = _CYCLE_TIME_RE.search(stdout)
    energy_m = _READ_ENERGY_RE.search(stdout)
    leakage_m = _LEAKAGE_RE.search(stdout)
    dims_m = _DIMENSIONS_RE.search(stdout)

    if not access_m or not dims_m:
        logger.warning(f"Could not parse CACTI output for {spec.name}")
        return None

    height_mm = float(dims_m.group(1))
    width_mm = float(dims_m.group(2))
    # Area comes straight from CACTI's characterized dimensions: block size is
    # now ceil(width/8) (see _block_size_bytes), so there is no power-of-two pad
    # to undo. area_um2, height_mm, and width_mm are mutually consistent, so the
    # Liberty `area` and the LEF `SIZE` agree.
    area_um2 = height_mm * width_mm * 1e6

    return CACTIResult(
        access_time_ns=float(access_m.group(1)),
        cycle_time_ns=float(cycle_m.group(1)) if cycle_m else 0.0,
        read_energy_nj=float(energy_m.group(1)) if energy_m else 0.0,
        leakage_power_mw=float(leakage_m.group(1)) if leakage_m else 0.0,
        height_mm=height_mm,
        width_mm=width_mm,
        area_um2=area_um2,
        full_out=stdout
    )


def analytical_area_estimate(spec: SRAMSpec, cell_area_per_bit_um2: float = 1.0) -> CACTIResult:
    """Fallback area estimate when CACTI fails.

    Uses a simple model: area = depth * width * cell_area_per_bit.
    Timing is estimated from simple RC scaling.
    """
    area = spec.depth * spec.width * cell_area_per_bit_um2
    side = math.sqrt(area)
    # Rough timing: 0.5ns base + 0.01ns per row of depth
    access_time = 0.5 + 0.01 * math.log2(max(spec.depth, 2))
    return CACTIResult(
        access_time_ns=access_time,
        cycle_time_ns=access_time * 1.2,
        read_energy_nj=0.001 * spec.depth * spec.width / 8192,
        leakage_power_mw=0.01 * spec.depth * spec.width / 8192,
        height_mm=side / 1000,
        width_mm=side / 1000,
        area_um2=area,
        full_out="This is a fake analytical estimate not actually produced by CACTI"
    )
