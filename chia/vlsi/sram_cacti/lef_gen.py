"""Generate minimal LEF (.lef) files for SRAM macros from CACTI results.

Produces LEF files with macro dimensions, pin shapes, and obstruction layers
matching the CACTI-characterized SRAM area.  Pin names match the Liberty cell
and Verilog blackbox stubs (cacti_<name>).

LEF is geometry-only (no timing) so ONE file is generated per SRAM,
not per corner like Liberty.

Uses CLASS BLOCK BLACKBOX per LEF 5.7 spec -- designed for blocks that
contain a SIZE statement estimating total area with approximate pin locations.
"""

import math
from chia.vlsi.sram_cacti.cacti_runner import SRAMSpec, CACTIResult

# Pin geometry constants (microns)
_PIN_WIDTH = 0.2    # horizontal extent of each signal pin rectangle
_PIN_HEIGHT = 0.2   # vertical extent of each signal pin rectangle
_PIN_SPACING = 0.2  # gap between consecutive pin rectangles
_POWER_STRIPE_H = 0.5  # height of VDD/VSS power stripes


def generate_lef(
    spec: SRAMSpec,
    result: CACTIResult,
    pin_layer: str = "met4",
    obs_layers: list[str] | None = None,
    cell_prefix: str = "cacti_",
) -> str:
    """Generate a minimal LEF file for an SRAM macro.

    Args:
        spec: SRAM specification (pin names, depth, width, ports).
        result: CACTI characterization result (physical dimensions).
        pin_layer: Metal layer name for pin geometry (default "met4" for Sky130).
        obs_layers: Metal layers for obstruction block.
            Defaults to ["met1", "met2", "met3"] for Sky130.
        cell_prefix: Prefix on the LEF MACRO name (must match Liberty cell
            name). See generate_liberty for the disambiguation rationale.

    Returns:
        Complete LEF file content as a string.
    """
    if obs_layers is None:
        obs_layers = ["met1", "met2", "met3"]

    cell_name = f"{cell_prefix}{spec.name}"
    width_um = result.width_mm * 1000.0
    height_um = result.height_mm * 1000.0

    pins = _enumerate_pins(spec)

    lines: list[str] = []
    _a = lines.append

    # Header
    _a('VERSION 5.7 ;')
    _a('BUSBITCHARS "[]" ;')
    _a('DIVIDERCHAR "/" ;')
    _a('')

    # Macro definition
    _a(f'MACRO {cell_name}')
    _a(f'  CLASS BLOCK BLACKBOX ;')
    _a(f'  ORIGIN 0 0 ;')
    _a(f'  FOREIGN {cell_name} 0 0 ;')
    _a(f'  SIZE {width_um:.3f} BY {height_um:.3f} ;')
    _a(f'  SYMMETRY X Y ;')
    _a(f'')

    # Power pins (full-width stripes)
    _a(f'  PIN VDD')
    _a(f'    DIRECTION INOUT ;')
    _a(f'    USE POWER ;')
    _a(f'    PORT')
    _a(f'      LAYER {pin_layer} ;')
    _a(f'        RECT 0.000 {height_um - _POWER_STRIPE_H:.3f} {width_um:.3f} {height_um:.3f} ;')
    _a(f'    END')
    _a(f'  END VDD')

    _a(f'  PIN VSS')
    _a(f'    DIRECTION INOUT ;')
    _a(f'    USE GROUND ;')
    _a(f'    PORT')
    _a(f'      LAYER {pin_layer} ;')
    _a(f'        RECT 0.000 0.000 {width_um:.3f} {_POWER_STRIPE_H:.3f} ;')
    _a(f'    END')
    _a(f'  END VSS')

    # Signal and clock pins
    y_offset = _POWER_STRIPE_H + _PIN_SPACING  # start above VSS stripe
    x_offset = 0.0
    available_height = height_um - 2 * _POWER_STRIPE_H - 2 * _PIN_SPACING

    for pin_name, direction, use in pins:
        # Wrap to next column if we've exceeded available height
        if y_offset + _PIN_HEIGHT > height_um - _POWER_STRIPE_H - _PIN_SPACING:
            x_offset += _PIN_WIDTH + _PIN_SPACING
            y_offset = _POWER_STRIPE_H + _PIN_SPACING

        x0 = x_offset
        y0 = y_offset
        x1 = x_offset + _PIN_WIDTH
        y1 = y_offset + _PIN_HEIGHT

        _a(f'  PIN {pin_name}')
        _a(f'    DIRECTION {direction} ;')
        _a(f'    USE {use} ;')
        _a(f'    PORT')
        _a(f'      LAYER {pin_layer} ;')
        _a(f'        RECT {x0:.3f} {y0:.3f} {x1:.3f} {y1:.3f} ;')
        _a(f'    END')
        _a(f'  END {pin_name}')

        y_offset += _PIN_HEIGHT + _PIN_SPACING

    # Obstructions
    if obs_layers:
        _a(f'  OBS')
        for layer in obs_layers:
            _a(f'    LAYER {layer} ;')
            _a(f'      RECT 0.000 0.000 {width_um:.3f} {height_um:.3f} ;')
        _a(f'  END')

    _a(f'END {cell_name}')
    _a(f'')
    _a(f'END LIBRARY')
    _a(f'')

    return '\n'.join(lines)


def _enumerate_pins(spec: SRAMSpec) -> list[tuple[str, str, str]]:
    """Enumerate all signal pins for the SRAM macro.

    Returns a list of (pin_name, direction, use) tuples where:
    - direction is "INPUT" or "OUTPUT"
    - use is "CLOCK" or "SIGNAL"

    Pin names and directions match liberty_gen._gen_port_pins and
    chia.chipyard.macrocompiler.generate_macro_stubs exactly.
    """
    addr_bits = max(1, math.ceil(math.log2(max(spec.depth, 2))))
    mask_bits = math.ceil(spec.width / spec.mask_gran) if spec.mask_gran else 0

    pins: list[tuple[str, str, str]] = []

    for prefix, masked in spec.parsed_ports:
        is_rw = prefix.startswith("RW")
        is_reader = prefix.startswith("R") and not is_rw
        is_writer = prefix.startswith("W")

        # Clock and enable (every port has these)
        pins.append((f"{prefix}_clk", "INPUT", "CLOCK"))
        pins.append((f"{prefix}_en", "INPUT", "SIGNAL"))

        if is_rw:
            pins.append((f"{prefix}_wmode", "INPUT", "SIGNAL"))
            # Address bus
            for i in range(addr_bits - 1, -1, -1):
                pins.append((f"{prefix}_addr[{i}]", "INPUT", "SIGNAL"))
            # Write data bus
            for i in range(spec.width - 1, -1, -1):
                pins.append((f"{prefix}_wdata[{i}]", "INPUT", "SIGNAL"))
            # Read data bus
            for i in range(spec.width - 1, -1, -1):
                pins.append((f"{prefix}_rdata[{i}]", "OUTPUT", "SIGNAL"))
            # Write mask bus
            if masked and mask_bits > 0:
                for i in range(mask_bits - 1, -1, -1):
                    pins.append((f"{prefix}_wmask[{i}]", "INPUT", "SIGNAL"))

        elif is_reader:
            for i in range(addr_bits - 1, -1, -1):
                pins.append((f"{prefix}_addr[{i}]", "INPUT", "SIGNAL"))
            for i in range(spec.width - 1, -1, -1):
                pins.append((f"{prefix}_data[{i}]", "OUTPUT", "SIGNAL"))

        elif is_writer:
            for i in range(addr_bits - 1, -1, -1):
                pins.append((f"{prefix}_addr[{i}]", "INPUT", "SIGNAL"))
            for i in range(spec.width - 1, -1, -1):
                pins.append((f"{prefix}_data[{i}]", "INPUT", "SIGNAL"))
            if masked and mask_bits > 0:
                for i in range(mask_bits - 1, -1, -1):
                    pins.append((f"{prefix}_mask[{i}]", "INPUT", "SIGNAL"))

    return pins
