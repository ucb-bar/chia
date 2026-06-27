"""Custom Hammer CLI driver used by Chia's synthesis node.

Subclasses ``hammer.vlsi.CLIDriver`` to replace the Genus ``generate_reports``
synthesis step with one that additionally emits a constrained timing report
with 500 critical paths (``final_constrained.rpt``), and bumps the existing
unconstrained timing report from Hammer's default 50 to 500 paths.

Invoked as a script with the same CLI as ``hammer-vlsi``.
"""
from hammer.vlsi import CLIDriver, HammerTool
from hammer.vlsi.hooks import HammerToolHookAction

MAX_PATHS = 500


def _generate_reports_max_paths(ht: HammerTool) -> bool:
    ht.verbose_append("write_reports -directory reports -tag final")
    try:
        _phys = ht.get_setting("synthesis.genus.phys_flow_effort")
    except KeyError:
        _phys = "none"   # key absent in this hammer/config; synflop runs have no phys flow
    if _phys.lower() != "none":
        ht.verbose_append("report_ple > reports/final_ple.rpt")
    ht.verbose_append(
        f"report_timing -max_paths {MAX_PATHS} > reports/final_constrained.rpt"
    )
    ht.verbose_append(
        f"report_timing -unconstrained -max_paths {MAX_PATHS} "
        "> reports/final_unconstrained.rpt"
    )
    return True


class ChiaCLIDriver(CLIDriver):
    def get_extra_synthesis_hooks(self) -> list[HammerToolHookAction]:
        return [
            HammerTool.make_replacement_hook(
                "generate_reports", _generate_reports_max_paths
            ),
        ]


def main() -> None:
    ChiaCLIDriver().main()


if __name__ == "__main__":
    main()
