"""CACTI-specific adapter over the generic Chipyard MacroCompiler glue.

The generic MacroCompiler support — MDF library generation and the
``tapeout.jar`` remap step — lives in ``chia.chipyard.macrocompiler``. This
module just wraps it with the ``cacti_`` cell-name prefix used by the CACTI
characterization flow.
"""

from chia.chipyard.macrocompiler import SRAMSpec, generate_macrocompiler_lib


def generate_cacti_macrocompiler_lib(sram_specs: list[SRAMSpec], cell_prefix: str = "cacti_") -> str:
    """Generate the MacroCompiler MDF library for CACTI-characterized SRAMs.

    Thin wrapper over :func:`generate_macrocompiler_lib` that defaults the
    cell-name prefix to ``cacti_``.
    """
    return generate_macrocompiler_lib(sram_specs, cell_prefix)
