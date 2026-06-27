"""Shared helpers for the BOOM RTL optimization case study.

Self-contained: depends only on the installed ``chia`` package (Ray
framework, build/run nodes)

  * ``state_def``        — state_def.py (TestBinary, TMACounters, OptContext)
  * ``common_helpers``      — common_helpers.py (TMA counter parsing, test-binary
                           loading, build/test error formatting, the
                           claude_creds prompt task)
  * ``common_nodes``        — helpers (collect_diff, reset_and_apply_diff,
                           debug_failure, VerilatorTestOutcome,
                           synthesis/area helpers, timing log).
                           debug_failure adds session_transcript /
                           return_transcript params (vs the original) so
                           pooled debug calls can actually resume a prior
                           session across LLM-pool workers.
  * ``build``            — common_build.py (build + LLM build-debug retry)
  * ``verilator``        — common_verilator.py (test dispatch, waves, S3;
                           expects the adjacent ``dramsim_ini/`` dir)
"""
