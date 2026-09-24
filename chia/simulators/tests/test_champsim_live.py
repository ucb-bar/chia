"""Live tests for chia.simulators.champsim — ChampSimNode + stats parsing.

Tiers:

  Tier 0 — Pure functions, no cluster required.  Tests stats parsing,
  derived metric computation, custom stats extraction, and build diagnostic
  filtering.  Runs with ``pytest -v -k "parse or derived or extract or filter"``.

  Tier 1 — Synthetic environment, real Ray.  Exercises build/run/capture/restore
  through ``chia_remote`` dispatch with a synthetic ChampSim environment on a
  resource-tagged worker.  Added in Plan 02.

Configuration (env vars, Tier 1+):
  CHAMPSIM_TEST_RAY_ADDRESS   Ray head address           (default "a9:6379")
  CHAMPSIM_TEST_RESOURCE      bundle/run resource label  (default "champsim")
  CHAMPSIM_TEST_PG_TIMEOUT    seconds to wait for a bundle (default 60)

Run Tier 0:
  pytest chia/simulators/tests/test_champsim_live.py -v -x -k "parse or derived or extract or filter"
"""

from __future__ import annotations

import json
import os

import pytest

# Import pure functions and dataclasses from champsim — no Ray dependency.
from chia.simulators.champsim import (
    CachePrefetchStats,
    CacheStats,
    ChampSimBuildResult,
    ChampSimRunResult,
    ChampSimSourceState,
    _compute_derived_metrics,
    _extract_custom_prefetch_stats,
    _filter_build_diagnostics,
    _parse_champsim_json,
)


# ===========================================================================
# Tier 1 configuration (env vars)
# ===========================================================================

# Resource label for the champsim worker bundle.  Set to "CPU" to run on any
# free node without requiring a dedicated champsim resource.
RES = os.environ.get("CHAMPSIM_TEST_RESOURCE", "champsim")

# Ray head address for Tier 1 tests.
RAY_ADDR = os.environ.get("CHAMPSIM_TEST_RAY_ADDRESS", "a9:6379")

# Timeout (seconds) waiting for a placement-group bundle.
PG_TIMEOUT = float(os.environ.get("CHAMPSIM_TEST_PG_TIMEOUT", "60"))

# Ship the local chia repo to workers so chia.simulators is importable even
# when the worker image's baked-in chia predates this module (chia is a
# namespace package, so the uploaded copy's `simulators` merges onto the
# installed one).  Set CHAMPSIM_TEST_WORKING_DIR="" to disable (cluster
# already has this module).
try:
    import chia as _chia
    _DEFAULT_WORKING_DIR = os.path.dirname(list(_chia.__path__)[0])
except Exception:
    _DEFAULT_WORKING_DIR = ""
WORKING_DIR = os.environ.get("CHAMPSIM_TEST_WORKING_DIR", _DEFAULT_WORKING_DIR)


# ===========================================================================
# Tier 1 test data — next_line prefetcher source for synthetic builds
# ===========================================================================

# Header-only next_line prefetcher matching build_champsim's convention:
# the entire module lives in the .h file (struct + inline method bodies).
_NEXT_LINE_SRC = """\
#include <cstdint>
#include "address.h"
#include "modules.h"

struct next_line : public champsim::modules::prefetcher {
  using prefetcher::prefetcher;

  uint32_t prefetcher_cache_operate(champsim::address addr, champsim::address ip,
                                     uint8_t cache_hit, bool useful_prefetch,
                                     access_type type, uint32_t metadata_in) {
    champsim::block_number pf_addr{addr};
    prefetch_line(champsim::address{pf_addr + 1}, true, metadata_in);
    return metadata_in;
  }

  uint32_t prefetcher_cache_fill(champsim::address addr, long set, long way,
                                  uint8_t prefetch, champsim::address evicted_addr,
                                  uint32_t metadata_in) {
    return metadata_in;
  }
};
"""


# ===========================================================================
# Tier 0 — pure functions (no cluster)
# ===========================================================================

# Canned ChampSim --json output matching the RESEARCH.md "ChampSim JSON Output
# Structure" code example.  Two phases: Warmup and Simulation.  The Simulation
# phase has: roi.cores[0] with instructions=1000, cycles=500.  Three caches:
# cpu0_L1D, cpu0_L2C (with prefetch counters), and LLC.
_CANNED_JSON = [
    {
        "name": "Warmup",
        "traces": ["trace.champsimtrace.gz"],
        "roi": {
            "cores": [
                {
                    "instructions": 500,
                    "cycles": 300,
                    "Avg ROB occupancy at mispredict": 20.0,
                    "mispredict": {
                        "BRANCH_DIRECT_JUMP": 0,
                        "BRANCH_INDIRECT": 0,
                        "BRANCH_CONDITIONAL": 2,
                        "BRANCH_DIRECT_CALL": 0,
                        "BRANCH_INDIRECT_CALL": 0,
                        "BRANCH_RETURN": 0,
                    },
                }
            ],
            "DRAM": [
                {
                    "RQ ROW_BUFFER_HIT": 0,
                    "RQ ROW_BUFFER_MISS": 5,
                    "WQ ROW_BUFFER_HIT": 0,
                    "WQ ROW_BUFFER_MISS": 0,
                    "AVG DBUS CONGESTED CYCLE": 0.0,
                    "REFRESHES ISSUED": 0,
                }
            ],
            "cpu0_L1D": {
                "prefetch requested": 0,
                "prefetch issued": 0,
                "useful prefetch": 0,
                "useless prefetch": 0,
                "miss latency": 100.0,
                "LOAD": {"hit": [25], "miss": [10], "mshr_merge": [0]},
                "RFO": {"hit": [0], "miss": [0], "mshr_merge": [0]},
                "PREFETCH": {"hit": [0], "miss": [0], "mshr_merge": [0]},
                "WRITE": {"hit": [0], "miss": [0], "mshr_merge": [0]},
                "TRANSLATION": {"hit": [0], "miss": [0], "mshr_merge": [0]},
            },
            "cpu0_L2C": {
                "prefetch requested": 50,
                "prefetch issued": 40,
                "useful prefetch": 15,
                "useless prefetch": 25,
                "miss latency": 120.0,
                "LOAD": {"hit": [20], "miss": [5], "mshr_merge": [0]},
                "RFO": {"hit": [0], "miss": [0], "mshr_merge": [0]},
                "PREFETCH": {"hit": [5], "miss": [35], "mshr_merge": [0]},
                "WRITE": {"hit": [0], "miss": [0], "mshr_merge": [0]},
                "TRANSLATION": {"hit": [0], "miss": [0], "mshr_merge": [0]},
            },
            "LLC": {
                "prefetch requested": 0,
                "prefetch issued": 0,
                "useful prefetch": 0,
                "useless prefetch": 0,
                "miss latency": 250.0,
                "LOAD": {"hit": [3], "miss": [2], "mshr_merge": [0]},
                "RFO": {"hit": [0], "miss": [0], "mshr_merge": [0]},
                "PREFETCH": {"hit": [0], "miss": [0], "mshr_merge": [0]},
                "WRITE": {"hit": [0], "miss": [0], "mshr_merge": [0]},
                "TRANSLATION": {"hit": [0], "miss": [0], "mshr_merge": [0]},
            },
        },
        "sim": {},
    },
    {
        "name": "Simulation",
        "traces": ["trace.champsimtrace.gz"],
        "roi": {
            "cores": [
                {
                    "instructions": 1000,
                    "cycles": 500,
                    "Avg ROB occupancy at mispredict": 42.0,
                    "mispredict": {
                        "BRANCH_DIRECT_JUMP": 0,
                        "BRANCH_INDIRECT": 0,
                        "BRANCH_CONDITIONAL": 5,
                        "BRANCH_DIRECT_CALL": 0,
                        "BRANCH_INDIRECT_CALL": 0,
                        "BRANCH_RETURN": 0,
                    },
                }
            ],
            "DRAM": [
                {
                    "RQ ROW_BUFFER_HIT": 0,
                    "RQ ROW_BUFFER_MISS": 10,
                    "WQ ROW_BUFFER_HIT": 0,
                    "WQ ROW_BUFFER_MISS": 0,
                    "AVG DBUS CONGESTED CYCLE": 0.0,
                    "REFRESHES ISSUED": 0,
                }
            ],
            "cpu0_L1D": {
                "prefetch requested": 0,
                "prefetch issued": 0,
                "useful prefetch": 0,
                "useless prefetch": 0,
                "miss latency": 200.5,
                "LOAD": {"hit": [50], "miss": [20], "mshr_merge": [0]},
                "RFO": {"hit": [0], "miss": [0], "mshr_merge": [0]},
                "PREFETCH": {"hit": [0], "miss": [0], "mshr_merge": [0]},
                "WRITE": {"hit": [0], "miss": [0], "mshr_merge": [0]},
                "TRANSLATION": {"hit": [0], "miss": [0], "mshr_merge": [0]},
            },
            "cpu0_L2C": {
                "prefetch requested": 100,
                "prefetch issued": 80,
                "useful prefetch": 30,
                "useless prefetch": 50,
                "miss latency": 150.0,
                "LOAD": {"hit": [40], "miss": [10], "mshr_merge": [0]},
                "RFO": {"hit": [0], "miss": [0], "mshr_merge": [0]},
                "PREFETCH": {"hit": [10], "miss": [70], "mshr_merge": [0]},
                "WRITE": {"hit": [0], "miss": [0], "mshr_merge": [0]},
                "TRANSLATION": {"hit": [0], "miss": [0], "mshr_merge": [0]},
            },
            "LLC": {
                "prefetch requested": 0,
                "prefetch issued": 0,
                "useful prefetch": 0,
                "useless prefetch": 0,
                "miss latency": 300.0,
                "LOAD": {"hit": [5], "miss": [5], "mshr_merge": [0]},
                "RFO": {"hit": [0], "miss": [0], "mshr_merge": [0]},
                "PREFETCH": {"hit": [0], "miss": [0], "mshr_merge": [0]},
                "WRITE": {"hit": [0], "miss": [0], "mshr_merge": [0]},
                "TRANSLATION": {"hit": [0], "miss": [0], "mshr_merge": [0]},
            },
        },
        "sim": {},
    },
]


# ---------------------------------------------------------------------------
# Tier 0: _parse_champsim_json tests
# ---------------------------------------------------------------------------


def test_parse_champsim_json_ipc():
    """IPC should be instructions / cycles from the Simulation phase."""
    cache_stats, core_info = _parse_champsim_json(_CANNED_JSON)
    expected_ipc = 1000 / 500  # 2.0
    assert core_info["instructions"] == 1000, "Instructions mismatch"
    assert core_info["cycles"] == 500, "Cycles mismatch"
    assert abs(core_info["ipc"] - expected_ipc) < 1e-9, (
        f"IPC should be {expected_ipc}, got {core_info['ipc']}"
    )


def test_parse_champsim_json_cache_stats():
    """Parsed cache_stats should contain L1D, L2C, LLC (normalized from cpu0_L1D etc)."""
    cache_stats, core_info = _parse_champsim_json(_CANNED_JSON)
    assert "L1D" in cache_stats, "L1D not found in cache_stats"
    assert "L2C" in cache_stats, "L2C not found in cache_stats"
    assert "LLC" in cache_stats, "LLC not found in cache_stats"
    # Verify there are no cpu0_ prefixed keys.
    for key in cache_stats:
        assert not key.startswith("cpu0_"), f"Cache name should be normalized: {key}"


def test_parse_cache_prefetch_counters():
    """L2C prefetch counters should match canned data."""
    cache_stats, _ = _parse_champsim_json(_CANNED_JSON)
    l2c = cache_stats["L2C"]
    assert l2c.prefetch.requested == 100, "prefetch requested mismatch"
    assert l2c.prefetch.issued == 80, "prefetch issued mismatch"
    assert l2c.prefetch.useful == 30, "useful prefetch mismatch"
    assert l2c.prefetch.useless == 50, "useless prefetch mismatch"


def test_run_champsim_returns_the_raw_record(monkeypatch, tmp_path):
    """The parsed --json record travels beside the typed stats."""
    pytest.importorskip("ray")
    from chia.simulators import champsim as champsim_module
    from chia.simulators.champsim import ChampSimNode

    def fake_run(cmd, cwd, timeout_s, env=None):
        with open(cmd[cmd.index("--json") + 1], "w") as stats_file:
            json.dump(_CANNED_JSON, stats_file)
        return 0, "", "", False, 0.01

    monkeypatch.setattr(champsim_module, "_run_logged", fake_run)
    trace = tmp_path / "t.champsimtrace.xz"
    trace.write_bytes(b"")
    result = ChampSimNode.run_champsim(b"not a binary", str(trace))
    assert result.success is True
    assert result.raw_stats == _CANNED_JSON
    assert result.ipc == 2.0


# ---------------------------------------------------------------------------
# Tier 0: _compute_derived_metrics tests
# ---------------------------------------------------------------------------


def test_derived_metrics_accuracy_coverage_mpki():
    """Verify accuracy, coverage, and MPKI computation with known values.

    Given L2C with useful=30, issued=80, LOAD miss=[10], RFO miss=[0],
    instructions=1000:
      accuracy  = 30/80 = 0.375
      coverage  = 30/(30+10) = 0.75
      mpki      = 10*1000/1000 = 10.0
    """
    pf = CachePrefetchStats(requested=100, issued=80, useful=30, useless=50)
    cs = CacheStats(
        name="L2C",
        prefetch=pf,
        load_miss=[10],
        rfo_miss=[0],
    )
    _compute_derived_metrics(cs, total_instructions=1000)

    assert abs(cs.prefetch.accuracy - 0.375) < 1e-9, (
        f"accuracy should be 0.375, got {cs.prefetch.accuracy}"
    )
    assert abs(cs.prefetch.coverage - 0.75) < 1e-9, (
        f"coverage should be 0.75, got {cs.prefetch.coverage}"
    )
    assert abs(cs.prefetch.mpki - 10.0) < 1e-9, (
        f"mpki should be 10.0, got {cs.prefetch.mpki}"
    )


def test_derived_metrics_zero_division():
    """When denominators are zero, derived metrics should be None."""
    # issued=0 -> accuracy is None
    pf = CachePrefetchStats(requested=0, issued=0, useful=0, useless=0)
    cs = CacheStats(
        name="LLC",
        prefetch=pf,
        load_miss=[0],
        rfo_miss=[0],
    )
    _compute_derived_metrics(cs, total_instructions=0)

    assert cs.prefetch.accuracy is None, (
        "accuracy should be None when issued=0"
    )
    assert cs.prefetch.coverage is None, (
        "coverage should be None when useful=0 and demand_misses=0"
    )
    assert cs.prefetch.mpki is None, (
        "mpki should be None when total_instructions=0"
    )


# ---------------------------------------------------------------------------
# Tier 0: _extract_custom_prefetch_stats tests
# ---------------------------------------------------------------------------


def test_extract_custom_prefetch_stats():
    """Custom stats should be extracted from stdout lines after the marker."""
    stdout = (
        "Some warmup output\n"
        "ChampSim completed all CPUs\n"
        "\n"
        "Region of Interest Statistics\n"
        "my_counter : 42\n"
        "another_stat = 3.14\n"
        "  spaced_key   :   99  \n"
        "[{\"name\": \"Simulation\"}]\n"
    )
    stats = _extract_custom_prefetch_stats(stdout)
    assert stats["my_counter"] == "42", f"Expected '42', got {stats.get('my_counter')}"
    assert stats["another_stat"] == "3.14", (
        f"Expected '3.14', got {stats.get('another_stat')}"
    )
    assert stats["spaced_key"] == "99", f"Expected '99', got {stats.get('spaced_key')}"
    # Should stop before JSON output.
    assert len(stats) == 3, f"Expected 3 stats, got {len(stats)}"


def test_extract_custom_prefetch_stats_empty():
    """Empty dict when no custom stats marker is present."""
    stdout = "Normal build output\nNo stats here\n"
    stats = _extract_custom_prefetch_stats(stdout)
    assert stats == {}, f"Expected empty dict, got {stats}"


# ---------------------------------------------------------------------------
# Tier 0: _filter_build_diagnostics tests
# ---------------------------------------------------------------------------


def test_filter_build_diagnostics():
    """Error lines and their context should be preserved."""
    stdout = (
        "Building objects...\n"
        "g++ -c -o foo.o foo.cc\n"
        "In file included from bar.h:10:\n"
        "foo.cc:42:5: error: undefined reference to 'missing_symbol'\n"
        "make: *** [Makefile:123] Error 1\n"
    )
    stderr = ""
    result = _filter_build_diagnostics(stdout, stderr)
    # Should contain the error line.
    assert "undefined reference" in result, "Error line missing from diagnostics"
    # Should contain the context line above (In file included...).
    assert "In file included from" in result, "Context line missing from diagnostics"


def test_filter_build_diagnostics_no_errors():
    """When no error lines are found, fall back to last 30 lines."""
    lines = [f"build step {i}" for i in range(50)]
    stdout = "\n".join(lines)
    stderr = ""
    result = _filter_build_diagnostics(stdout, stderr)
    # Should contain the last 30 lines (steps 20-49).
    assert "build step 49" in result, "Last line missing from fallback diagnostics"
    assert "build step 20" in result, "Line 20 missing from fallback diagnostics"
    # Should NOT contain early lines.
    assert "build step 5" not in result, "Early line should not be in fallback diagnostics"


# ---------------------------------------------------------------------------
# Tier 0: ChampSimNode import validation
# ---------------------------------------------------------------------------


def test_champsim_node_import():
    """Validate that ChampSimNode is importable and has the expected structure.

    This test exercises the export chain:
      chia.simulators.champsim.ChampSimNode -> _MEMBER_FNS tuple with 4 names.

    When ray is not installed, ChampSimNode is not defined in the module, so
    this test uses the _HAS_RAY flag to conditionally check.
    """
    from chia.simulators.champsim import _HAS_RAY

    if _HAS_RAY:
        from chia.simulators.champsim import ChampSimNode
        assert hasattr(ChampSimNode, "_MEMBER_FNS"), (
            "ChampSimNode missing _MEMBER_FNS"
        )
        assert len(ChampSimNode._MEMBER_FNS) == 4, (
            f"Expected 4 member functions, got {len(ChampSimNode._MEMBER_FNS)}"
        )
        expected = {
            "build_champsim",
            "run_champsim",
            "capture_champsim_source_state",
            "restore_champsim_source_state",
        }
        assert set(ChampSimNode._MEMBER_FNS) == expected, (
            f"_MEMBER_FNS mismatch: {ChampSimNode._MEMBER_FNS}"
        )
        assert hasattr(ChampSimNode, "_DEFAULT_BUNDLE"), (
            "ChampSimNode missing _DEFAULT_BUNDLE"
        )
        assert ChampSimNode._DEFAULT_BUNDLE.get("champsim") == 1.0, (
            "Default bundle should have champsim=1.0"
        )
    else:
        # When ray is not installed, verify the pure dataclasses and helpers
        # are still importable (the imports at the top of this file would have
        # failed otherwise, so this is mostly a clarity assertion).
        assert CachePrefetchStats is not None
        assert ChampSimBuildResult is not None
        pytest.skip("ChampSimNode requires ray (not installed)")


# ===========================================================================
# Tier 1 — Synthetic environment, real Ray
# ===========================================================================
#
# Tier 1 tests validate ChampSimNode's build/run/capture/restore through real
# chia_remote dispatch to a resource-tagged worker.  They create a synthetic
# ChampSim environment on the worker (fake config.sh, fake make, fake binary)
# rather than running the real ChampSim build, so they exercise *our* wrapper
# logic end-to-end without a real simulator.
#
# To run Tier 1 tests:
#   CHAMPSIM_TEST_RESOURCE=CPU pytest chia/simulators/tests/test_champsim_live.py -v -k "tier1"
#
# Tier 1 tests are gated on a connected Ray cluster and will be skipped if
# ray.init() fails.
