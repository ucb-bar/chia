"""Generate minimal Liberty (.lib) files for SRAM macros from CACTI results.

Produces Liberty files with area, timing arcs, and pin definitions that match
the Chisel-generated _ext module port names. Uses scalar delay model (no NLDM
tables) — sufficient for synthesis area estimation and basic timing.

"""

import math
from dataclasses import dataclass
from chia.vlsi.sram_cacti.cacti_runner import SRAMSpec, CACTIResult


@dataclass
class LibertyCorner:
    name: str           # "ff", "ss", "tt"
    temperature: int    # -40, 100, 25
    voltage: float      # 1.95, 1.60, 1.80
    pvt_name: str       # "PVT_1P95V_-40C", etc.
    lib_suffix: str     # output key / filename suffix, e.g. "ff_n40C_1v95"


# Typical Sky130 corner; also the fallback when generate_liberty gets no corner.
_SKY130_TT = LibertyCorner("tt", 25, 1.80, "PVT_1P8V_25C", "tt_025C_1v80")

# Default corner set, matching the SRAM22 Sky130 characterization (ff/ss/tt).
SKY130_CORNERS: tuple[LibertyCorner, ...] = (
    LibertyCorner("ff", -40, 1.95, "PVT_1P95V_-40C", "ff_n40C_1v95"),
    LibertyCorner("ss", 100, 1.60, "PVT_1P6V_100C", "ss_100C_1v60"),
    _SKY130_TT,
)


def generate_liberty(
    spec: SRAMSpec,
    result: CACTIResult,
    corner: LibertyCorner | None = None,
    cell_prefix: str = "cacti_",
) -> str:
    """Generate a Liberty .lib file for an SRAM macro at a specific corner.

    The cell name and pin names match the Chisel-generated _ext module so
    Genus can use this as a drop-in library cell.

    If corner is None, defaults to typical (tt_025C_1v80).

    cell_prefix defaults to ``cacti_`` so the Liberty cell name doesn't collide
    with the synflop wrapper named ``<spec.name>`` in chipyard's .top.mems.v
    (MacroCompiler then rewrites synflop refs to point at ``cacti_<name>``).
    Pass "" when the upstream Verilog references <spec.name> directly as an
    extmodule (e.g. firtool --repl-seq-mem) so the Liberty cell name lines up.
    """
    if corner is None:
        corner = _SKY130_TT

    addr_bits = max(1, math.ceil(math.log2(max(spec.depth, 2))))
    mask_bits = math.ceil(spec.width / spec.mask_gran) if spec.mask_gran else 0

    setup_time = result.access_time_ns * 0.2
    hold_time = result.access_time_ns * 0.05
    clk_to_q = result.access_time_ns
    leakage_nw = result.leakage_power_mw * 1e6

    cell_name = f"{cell_prefix}{spec.name}"

    lines = []
    _a = lines.append

    # Library header
    _a(f'library ({cell_name}) {{')
    _a(f'  delay_model : table_lookup;')
    _a(f'  capacitive_load_unit (1,pf);')
    _a(f'  current_unit : "1mA";')
    _a(f'  leakage_power_unit : "1nW";')
    _a(f'  time_unit : "1ns";')
    _a(f'  voltage_unit : "1V";')
    # Delay/slew measurement thresholds. Required by STA readers (OpenSTA errors
    # "missing one or more thresholds" without them); standard NLDM defaults.
    _a(f'  input_threshold_pct_rise : 50;')
    _a(f'  input_threshold_pct_fall : 50;')
    _a(f'  output_threshold_pct_rise : 50;')
    _a(f'  output_threshold_pct_fall : 50;')
    _a(f'  slew_lower_threshold_pct_rise : 20;')
    _a(f'  slew_lower_threshold_pct_fall : 20;')
    _a(f'  slew_upper_threshold_pct_rise : 80;')
    _a(f'  slew_upper_threshold_pct_fall : 80;')
    _a(f'  slew_derate_from_library : 1.0;')
    _a(f'  nom_process : 1;')
    _a(f'  nom_temperature : {corner.temperature};')
    _a(f'  nom_voltage : {corner.voltage};')
    _a(f'  voltage_map (VDD, {corner.voltage});')
    _a(f'  voltage_map (VSS, 0);')
    _a(f'  operating_conditions ({corner.pvt_name}) {{')
    _a(f'    process : 1;')
    _a(f'    temperature : {corner.temperature};')
    _a(f'    voltage : {corner.voltage};')
    _a(f'  }}')
    _a(f'  default_operating_conditions : {corner.pvt_name};')
    _a(f'  bus_naming_style : "%s[%d]";')
    _a(f'')

    # Bus type definitions
    bus_types = _get_bus_types(spec, addr_bits, mask_bits, cell_name)
    for bt in bus_types:
        _a(f'  type ({bt["name"]}) {{')
        _a(f'    base_type : array;')
        _a(f'    data_type : bit;')
        _a(f'    bit_width : {bt["width"]};')
        _a(f'    bit_from  : {bt["width"] - 1};')
        _a(f'    bit_to    : 0;')
        _a(f'    downto    : true;')
        _a(f'  }}')
        _a(f'')

    # Cell definition
    _a(f'  cell ({cell_name}) {{')
    _a(f'    area : {result.area_um2:.3f};')
    _a(f'    cell_leakage_power : {leakage_nw:.3f};')
    _a(f'    dont_touch : true;')
    _a(f'    interface_timing : true;')
    _a(f'    is_macro_cell : true;')
    _a(f'')

    # Power pins
    _a(f'    pg_pin (VDD) {{')
    _a(f'      direction : inout;')
    _a(f'      pg_type : primary_power;')
    _a(f'      voltage_name : "VDD";')
    _a(f'    }}')
    _a(f'    pg_pin (VSS) {{')
    _a(f'      direction : inout;')
    _a(f'      pg_type : primary_ground;')
    _a(f'      voltage_name : "VSS";')
    _a(f'    }}')
    _a(f'')

    # Memory declaration
    _a(f'    memory () {{')
    _a(f'      address_width : {addr_bits};')
    _a(f'      type : ram;')
    _a(f'      word_width : {spec.width};')
    _a(f'    }}')
    _a(f'')

    # Generate pins for each parsed port
    for prefix, masked in spec.parsed_ports:
        _gen_port_pins(lines, spec, prefix, masked, addr_bits, mask_bits,
                       setup_time, hold_time, clk_to_q, cell_name)

    _a(f'  }}')  # end cell
    _a(f'}}')  # end library
    _a(f'')

    return '\n'.join(lines)


def _get_bus_types(spec: SRAMSpec, addr_bits: int, mask_bits: int, cell_name: str = "") -> list[dict]:
    """Generate bus type definitions needed for this SRAM."""
    types = []
    for prefix, masked in spec.parsed_ports:
        is_rw = prefix.startswith("RW")
        is_reader = prefix.startswith("R") and not is_rw
        is_writer = prefix.startswith("W")

        types.append({"name": f"bus_{cell_name}_{prefix}_addr", "width": addr_bits})

        if is_rw:
            types.append({"name": f"bus_{cell_name}_{prefix}_wdata", "width": spec.width})
            types.append({"name": f"bus_{cell_name}_{prefix}_rdata", "width": spec.width})
            if masked and mask_bits > 0:
                types.append({"name": f"bus_{cell_name}_{prefix}_wmask", "width": mask_bits})
        elif is_reader:
            types.append({"name": f"bus_{cell_name}_{prefix}_data", "width": spec.width})
        elif is_writer:
            types.append({"name": f"bus_{cell_name}_{prefix}_data", "width": spec.width})
            if masked and mask_bits > 0:
                types.append({"name": f"bus_{cell_name}_{prefix}_mask", "width": mask_bits})
    return types


def _gen_clock_pin(lines, pin_name, setup_time, cap=0.005):
    """Generate a clock pin definition."""
    lines.append(f'    pin ({pin_name}) {{')
    lines.append(f'      clock : true;')
    lines.append(f'      direction : input;')
    lines.append(f'      related_ground_pin : VSS;')
    lines.append(f'      related_power_pin : VDD;')
    lines.append(f'      capacitance : {cap};')
    lines.append(f'      timing () {{')
    lines.append(f'        timing_type : min_pulse_width;')
    lines.append(f'        related_pin : "{pin_name}";')
    lines.append(f'        rise_constraint (scalar) {{ values ("{setup_time:.4f}"); }}')
    lines.append(f'        fall_constraint (scalar) {{ values ("{setup_time:.4f}"); }}')
    lines.append(f'      }}')
    lines.append(f'    }}')
    lines.append(f'')


def _gen_input_pin(lines, pin_name, clk_name, setup_time, hold_time, cap=0.003):
    """Generate an input pin with setup/hold timing."""
    lines.append(f'    pin ({pin_name}) {{')
    lines.append(f'      direction : input;')
    lines.append(f'      related_ground_pin : VSS;')
    lines.append(f'      related_power_pin : VDD;')
    lines.append(f'      capacitance : {cap};')
    lines.append(f'      timing () {{')
    lines.append(f'        related_pin : "{clk_name}";')
    lines.append(f'        timing_type : setup_rising;')
    lines.append(f'        rise_constraint (scalar) {{ values ("{setup_time:.4f}"); }}')
    lines.append(f'        fall_constraint (scalar) {{ values ("{setup_time:.4f}"); }}')
    lines.append(f'      }}')
    lines.append(f'      timing () {{')
    lines.append(f'        related_pin : "{clk_name}";')
    lines.append(f'        timing_type : hold_rising;')
    lines.append(f'        rise_constraint (scalar) {{ values ("{hold_time:.4f}"); }}')
    lines.append(f'        fall_constraint (scalar) {{ values ("{hold_time:.4f}"); }}')
    lines.append(f'      }}')
    lines.append(f'    }}')
    lines.append(f'')


def _gen_input_bus(lines, bus_name, bus_type, clk_name, width, setup_time, hold_time):
    """Generate an input bus with per-pin setup/hold timing."""
    lines.append(f'    bus ({bus_name}) {{')
    lines.append(f'      bus_type : {bus_type};')
    lines.append(f'      direction : input;')
    for i in range(width - 1, -1, -1):
        _gen_input_pin(lines, f'{bus_name}[{i}]', clk_name, setup_time, hold_time)
    lines.append(f'    }}')
    lines.append(f'')


def _gen_output_bus(lines, bus_name, bus_type, clk_name, width, clk_to_q):
    """Generate an output bus with clock-to-Q timing."""
    lines.append(f'    bus ({bus_name}) {{')
    lines.append(f'      bus_type : {bus_type};')
    lines.append(f'      direction : output;')
    for i in range(width - 1, -1, -1):
        lines.append(f'      pin ({bus_name}[{i}]) {{')
        lines.append(f'        related_ground_pin : VSS;')
        lines.append(f'        related_power_pin : VDD;')
        lines.append(f'        max_capacitance : 0.5;')
        lines.append(f'        timing () {{')
        lines.append(f'          related_pin : "{clk_name}";')
        lines.append(f'          timing_sense : non_unate;')
        lines.append(f'          timing_type : rising_edge;')
        lines.append(f'          cell_rise (scalar) {{ values ("{clk_to_q:.4f}"); }}')
        lines.append(f'          cell_fall (scalar) {{ values ("{clk_to_q:.4f}"); }}')
        lines.append(f'          rise_transition (scalar) {{ values ("0.05"); }}')
        lines.append(f'          fall_transition (scalar) {{ values ("0.05"); }}')
        lines.append(f'        }}')
        lines.append(f'      }}')
    lines.append(f'    }}')
    lines.append(f'')


def _gen_port_pins(lines, spec, prefix, masked, addr_bits, mask_bits, setup_time, hold_time, clk_to_q, cell_name=""):
    """Generate Liberty pins for a single port (R, W, or RW) identified by prefix."""
    is_rw = prefix.startswith("RW")
    is_reader = prefix.startswith("R") and not is_rw
    is_writer = prefix.startswith("W")

    clk = f"{prefix}_clk"
    _gen_clock_pin(lines, clk, setup_time)
    _gen_input_pin(lines, f"{prefix}_en", clk, setup_time, hold_time)

    if is_rw:
        _gen_input_pin(lines, f"{prefix}_wmode", clk, setup_time, hold_time)
        _gen_input_bus(lines, f"{prefix}_addr", f"bus_{cell_name}_{prefix}_addr",
                       clk, addr_bits, setup_time, hold_time)
        _gen_input_bus(lines, f"{prefix}_wdata", f"bus_{cell_name}_{prefix}_wdata",
                       clk, spec.width, setup_time, hold_time)
        _gen_output_bus(lines, f"{prefix}_rdata", f"bus_{cell_name}_{prefix}_rdata",
                        clk, spec.width, clk_to_q)
        if masked and mask_bits > 0:
            _gen_input_bus(lines, f"{prefix}_wmask", f"bus_{cell_name}_{prefix}_wmask",
                           clk, mask_bits, setup_time, hold_time)
    elif is_reader:
        _gen_input_bus(lines, f"{prefix}_addr", f"bus_{cell_name}_{prefix}_addr",
                       clk, addr_bits, setup_time, hold_time)
        _gen_output_bus(lines, f"{prefix}_data", f"bus_{cell_name}_{prefix}_data",
                        clk, spec.width, clk_to_q)
    elif is_writer:
        _gen_input_bus(lines, f"{prefix}_addr", f"bus_{cell_name}_{prefix}_addr",
                       clk, addr_bits, setup_time, hold_time)
        _gen_input_bus(lines, f"{prefix}_data", f"bus_{cell_name}_{prefix}_data",
                       clk, spec.width, setup_time, hold_time)
        if masked and mask_bits > 0:
            _gen_input_bus(lines, f"{prefix}_mask", f"bus_{cell_name}_{prefix}_mask",
                           clk, mask_bits, setup_time, hold_time)
