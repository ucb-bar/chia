from dataclasses import dataclass, field


@dataclass
class TestBinary:
    """A precompiled test binary for verilator simulation."""
    name: str
    content: bytes
    log_to_db: bool = True
    timeout_seconds: int | None = None
    timeout_cycles: int | None = None
    verilator_threads: int = 1


@dataclass
class TMACounters:
    """Parsed Top-down Microarchitecture Analysis counters from a single verilator run."""
    test_name: str
    counters: dict[str, float] = field(default_factory=dict)
    passed: bool = False
    log_to_db: bool = True


@dataclass
class OptContext:
    """Context about the optimization being debugged, fed to the debugger LLM.

    Built by build_opt_context() from the implement-node summary. Passed to
    format_build_error / format_test_error so the debugger sees what the
    optimization was trying to do — preventing cheap "revert and declare
    victory" fixes. The design_goal slice carries the per-file summary table
    the implementer writes, so the debugger still learns which files to focus
    on without needing a literal diff.
    """
    recommendation_title: str     # e.g. "Decoded Instruction Cache Cross-Branch Trace Fill"
    design_goal: str              # slice of implement_*.md starting at "### Files Changed"
    implement_summary: str        # text result of the llm implement_single_optimization node
