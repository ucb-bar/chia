"""Timing-improvement example: multi-branch BoomTile critical-path.

A Claude-in-the-loop flow that reads a Genus timing report, edits Chisel to
shorten critical paths, rebuilds + re-synthesizes, and records each child
branch in a SQLite-backed tree (see ``db.py``). Self-contained: depends only
on the installed ``chia`` package and the sibling ``common`` helpers 

  * ``constants``        — constants.py, trimmed to the timing flow's knobs
                           (paths, build config, workload thread/timeout maps,
                           timing-report relpaths, DB + scratch dirs).
  * ``common_constants`` — Ray runtime-env for standalone-script ``ray.init``.
  * ``improve_timing``   — improve_timing.py: the per-branch loop
                           (load → diff → LLM edit → build → CACTI/MC prep →
                           parallel synth + verilator → debug retry → persist)
                           plus ``seed_flow`` and the ``ExperimentLogger``
                           actor. Build/verilator/synth-prep primitives now
                           come from ``common`` (the old ``timing_backend`` is
                           gone); ``load_and_configure_test_binaries`` is kept
                           local glue.
  * ``timing_experiment_tool`` — timing_experiment_tool.py: the MCP tool that
                           lets the LLM start/poll fast sub-block Genus runs to
                           A/B test timing edits.
  * ``boom_tile_syn``    — boom_tile_syn.py: BoomTile synthesis with worker-
                           local obj_dir tarball transfer (kept here rather
                           than ``common.boom_tile_syn``, which uses a
                           shared-filesystem obj_dir model).
  * ``db``               — db.py: SQLite ``TimingDB`` (branch tree, files,
                           perf_results, llm_experiments) + ``parse_worst_slack``.

The shared infrastructure this package reuses from ``common``:
  * ``common.common_nodes``   — collect_diff, reset_and_apply_diff, debug_failure,
                             VerilatorTestOutcome, the generated-Verilog
                             parsing / hierarchy helpers, CACTI / MacroCompiler
                             task nodes + ``run_cacti_macrocompiler_prep``,
                             Genus area parsing, timing-log helpers.
  * ``common.build``       — non-debug build_megaboom / build_with_debug_retry /
                             build_all_thread_variants.
  * ``common.verilator``   — non-debug run_verilator_test / dispatch_verilator_tests.
  * ``common.common_helpers`` — format_test_error, parse_tma_counters, load_test_binaries.
"""
