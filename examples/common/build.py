"""Shared chipyard-build helpers

Companion to ``common/verilator.py``. ``build_megaboom_debug`` is the
debug-build counterpart of ``build_megaboom``
and exposes the selective-waveform compile-time knobs:

* ``wf_scopes`` — list of hierarchy paths the gen-collateral pragma
  injection script should keep traced (max 8). Sibling scopes get
  ``/* verilator tracing_off */`` so the compiled simulator only emits
  signals from the listed subtrees. Empty list means "trace everything",
  same as a stock debug build.

* ``clean_wf_stamp`` — mirrors ``rm -f .wf_scopes.stamp`` from
  ``sims/verilator/run_wave.sh:13``. Forces the WF_SCOPES-aware Make
  recipe to re-run even when Make would otherwise believe nothing
  changed. Defaults to True since most callers iterate on scopes.

The runtime side (PC-triggered windows + S3 upload of the VCD) lives
in ``run_verilator_test_debug`` over in ``common/verilator.py``.

The plain-build ``build_megaboom`` / ``build_with_debug_retry`` /
``build_all_thread_variants``
live at the bottom of this module — used by pipelines like the
timing-improvement example that never need a -debug binary.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime
from uuid import uuid4

from chia.base.ChiaFunction import get
from chia.base.tools.BashTool import BashTool
from chia.chipyard.chisel_build_node import ChiselBuildNode
from chia.chipyard.state_def import BuildArtifact, BuildTarget

from chia.examples.common.common_nodes import (
    _elapsed,
    _log_timing,
    collect_diff,
    debug_failure,
)
from chia.examples.common.common_helpers import format_build_error
from common.state_def import OptContext


def build_with_debug_retry(
    chipyard_bash: BashTool,
    iteration: int,
    log_dir: str,
    timing_path: str,
    max_retries: int = 3,
    label: str = "",
    chipyard_task_options: dict = {},
    verilator_threads: int = 1,
    collect_generated_src: bool = False,
    debug_log_base_dir: str = "",
    branch_name: str = "chipyard",
    opt_context: OptContext | None = None,
    *,
    config: str = "MegaBoomChiaBigCacheConfig",
    config_package: str = "chipyard",
    chipyard_path: str = "/home/ray/chipyard/",
    boom_repo_path: str = "/home/ray/chipyard/generators/boom/src/main/scala/v3",
    llm_env: str = "/home/ray/llm_env",
    submodules: list[str] = ["generators/boom", "generators/rocket-chip", "generators/rocket-chip-inclusive-cache", "generators/rocket-chip-blocks", "generators/bar-fetchers"],
    # Inline debugger prompt + aux reference files (-> debug_failure); when
    # debug_prompt_text is "", debug_failure falls back to the /debugging
    # slash command installed in llm_env.
    debug_prompt_text: str = "",
    debug_aux_files: dict[str, str] | None = None,
) -> BuildArtifact | None:
    """Build MegaBoom (non-debug). On failure, run the debugger LLM and retry.

    Returns the successful BuildArtifact, or None if all retries exhausted.
    ``chipyard_task_options`` pins the build to the correct placement-group
    node. A shared session_id persists the debug session across retries.
    """
    prefix = f"[{label}] " if label else ""
    debug_session_id = str(uuid4())

    for attempt in range(max_retries + 1):
        print(f"{prefix}{'Building' if attempt == 0 else f'Rebuilding (retry {attempt})'} "
              f"MegaBoom (threads={verilator_threads})...")
        t0 = time.time()
        # Build the non-debug verilator simulator directly via ChiselBuildNode
        build_node = ChiselBuildNode(
            chipyard_path=chipyard_path,
            config=config,
            config_package=config_package,
            target=BuildTarget.VERILATOR,
            make_jobs=16,
            extra_make_args={"VERILATOR_THREADS": str(verilator_threads)},
            timeout_seconds=60000,
            collect_generated_src=collect_generated_src,
            clean_sim=True,
            name=branch_name,
        )
        # Instance-method @ChiaFunction: pass the node as the first (self) arg.
        artifact = get(
            build_node.build.options(**chipyard_task_options).chia_remote(build_node)
        )
        _log_timing(timing_path, iteration, f"{label}_build{attempt}", time.time() - t0)
        print(f"{prefix}  Build {'OK' if artifact.returncode == 0 else 'FAILED'} [{_elapsed(t0)}]")

        if artifact.returncode == 0:
            return artifact

        error_ctx = format_build_error(artifact, iteration, attempt + 1, boom_repo_path,
                                       opt_context=opt_context)
        print(error_ctx)
        if debug_log_base_dir:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            err_ctx_path = os.path.join(debug_log_base_dir, f"{timestamp}_error_context.txt")
            with open(err_ctx_path, "w") as f:
                f.write(error_ctx)
            _err, _diff = get(collect_diff.options(**chipyard_task_options).chia_remote(
                chipyard_path, submodules))
            diff_path = os.path.join(debug_log_base_dir, f"{timestamp}_diff.json")
            with open(diff_path, "w") as f:
                json.dump(_diff if not _err else {}, f)

        if attempt < max_retries:
            t0 = time.time()
            dbg_cli = get(debug_failure.chia_remote(
                error_ctx, chipyard_bash, attempt + 1,
                session_id=debug_session_id,
                llm_env=llm_env,
                prompt_text=debug_prompt_text,
                aux_files=debug_aux_files,
            ))
            _log_timing(timing_path, iteration, f"{label}_debug{attempt+1}", time.time() - t0)

            if debug_log_base_dir:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                debug_log_path = os.path.join(debug_log_base_dir, f"{timestamp}_debug_failure_build.md")
                with open(debug_log_path, "w") as f:
                    f.write(f"# debug_failure (attempt={attempt + 1}, type=build)\n\n")
                    f.write(f"Success: {dbg_cli.success}\n\n")
                    f.write(dbg_cli.result or "")
                    if dbg_cli.stream_result:
                        f.write("\n\n## Stream log\n\n")
                        f.write(dbg_cli.stream_result)
                print(f"{prefix}  Debug response saved to {debug_log_path}")

            if not dbg_cli.success:
                print(f"{prefix}  Debugger failed — giving up [{_elapsed(t0)}]")
                return None
            print(f"{prefix}  Debugger done — retrying [{_elapsed(t0)}]")
        else:
            print(f"{prefix}  Max retries exhausted")

    return None


def build_all_thread_variants(
    test_binaries: list,
    chipyard_bash: BashTool,
    iteration: int,
    log_dir: str,
    timing_path: str,
    max_debug_retries: int,
    label: str,
    chipyard_task_options: dict,
    collect_generated_src_for_first: bool = False,
    debug_log_base_dir: str = "",
    branch_name: str = "chipyard",
    opt_context: OptContext | None = None,
    *,
    config: str = "MegaBoomChiaBigCacheConfig",
    config_package: str = "chipyard",
    chipyard_path: str = "/home/ray/chipyard/",
    boom_repo_path: str = "/home/ray/chipyard/generators/boom/src/main/scala/v3",
    llm_env: str = "/home/ray/llm_env",
    submodules: list[str] = ["generators/boom", "generators/rocket-chip", "generators/rocket-chip-inclusive-cache", "generators/rocket-chip-blocks", "generators/bar-fetchers"],
    debug_prompt_text: str = "",
    debug_aux_files: dict[str, str] | None = None,
) -> dict[int, BuildArtifact] | None:
    """Build one (non-debug) MegaBoom per unique verilator_threads value.

    Builds are sequential (each blocks the same chipyard node). Returns a dict
    mapping thread_count → BuildArtifact, or None if any variant fails. The
    waveform-capable counterpart is :func:`build_all_thread_variants_debug`.
    """
    unique_threads = sorted({tb.verilator_threads for tb in test_binaries})
    artifacts: dict[int, BuildArtifact] = {}
    for idx, n_threads in enumerate(unique_threads):
        collect_src = collect_generated_src_for_first and idx == 0
        artifact = build_with_debug_retry(
            chipyard_bash, iteration=iteration, log_dir=log_dir,
            timing_path=timing_path, max_retries=max_debug_retries,
            label=f"{label}_t{n_threads}",
            chipyard_task_options=chipyard_task_options,
            verilator_threads=n_threads,
            collect_generated_src=collect_src,
            debug_log_base_dir=debug_log_base_dir,
            branch_name=branch_name,
            opt_context=opt_context,
            config=config, config_package=config_package,
            chipyard_path=chipyard_path, boom_repo_path=boom_repo_path,
            llm_env=llm_env, submodules=submodules,
            debug_prompt_text=debug_prompt_text,
            debug_aux_files=debug_aux_files,
        )
        if artifact is None:
            return None
        artifacts[n_threads] = artifact
    return artifacts
