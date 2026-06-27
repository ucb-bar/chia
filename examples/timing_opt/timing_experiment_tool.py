"""LLM-invokable synthesis-experiment tool for the BoomTile timing flow.

Exposes six MCP methods Claude can call during the ``/improve_timing`` step:

  - ``rebuild_verilog()``                      — re-elaborate Chisel into Verilog
    via Chipyard's ``make verilog`` (no Verilator C++ compile).
  - ``list_modules()``                         — list the *child* Verilog module
    names so the LLM knows what ``vlsi_top`` values are available.
  - ``list_modules_parent()``                  — list the *parent* branch's
    module names, available without ``rebuild_verilog()``. Useful in Phase 1
    to ground the mental model and to detect structural drift between parent
    and child (renames, splits, new modules).
  - ``start_synth_child(vlsi_top, ...)``       — dispatch a sub-block synth on
    the LLM-edited (child) RTL. **Async**: returns a short handle string
    immediately; the heavy Genus run happens in a background Ray task.
  - ``start_synth_parent(vlsi_top, ...)``      — same, but on the parent
    branch's *unmodified* RTL. Issue together with ``start_synth_child`` as
    parallel MCP tool calls for the A/B comparison; both Genus runs proceed
    concurrently on the VLSI worker (capacity permitting).
  - ``synth_status(handle, max_wait_seconds)`` — check on an in-flight synth.
    Returns ``status: running`` (with elapsed_seconds) or the full summary
    on completion. ``max_wait_seconds`` lets Claude block opportunistically;
    keep it under ~200 so the MCP HTTP round-trip stays inside Claude's
    session timeout.

This start / poll split is deliberate: a sub-block Genus run on MegaBoom
typically takes 5-30 min, which exceeds Claude's MCP HTTP timeout. By
returning a handle in sub-seconds and letting Claude poll, we get arbitrary
synth durations without disconnects.

The tool actor lives on the **head node** (pinned via
``NodeAffinitySchedulingStrategy``). Work that has to run on the chipyard
worker — ``make verilog`` (rebuild_verilog) and staging of per-experiment
Genus reports to ``/tmp/improve_timing/experiments/<exp_id>/`` so
``chipyard_bash`` can grep them — is dispatched as **chipyard-PG-pinned
Ray remotes** (``_rebuild_verilog_remote`` and
``_stage_experiment_reports_remote`` below). The ``_experiment_orchestrator``
Ray task that runs the actual synth pipeline is dispatched with
``NodeAffinitySchedulingStrategy(head_node_id)`` so its inner cacti / Genus
child tasks are submitted from a head-on-head context and route through the
cluster normally — this avoids the AWS-to-AWS cross-worker lease storm we
hit when the orchestrator was dispatched without affinity (see
``chia/issuesexperienced/cross-worker-lease-storm.md``).

``/scratch`` is per-machine, so the synth worker tars its ``syn_obj`` and
returns the bytes, exactly like the rest of the flow — see
``boom_tile_syn.run_boom_tile_synthesis``. Results are recorded in the
SQLite DB via the head-node ``ExperimentLogger`` actor (passed in by name).
"""

from __future__ import annotations

import os
import subprocess
import time
import uuid
from pathlib import Path
from typing import Optional

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from chia.base.ChiaFunction import ChiaFunction, get
from chia.base.tools.ChiaTool import ChiaTool

from chia.examples.common.common_nodes import (
    _build_children_map,
    _get_all_descendants,
    _parse_verilog_modules,
    parse_area_from_reports,
    run_cacti_macrocompiler_prep,
)
from timing_opt.boom_tile_syn import run_boom_tile_synthesis
from timing_opt.constants import (
    EXPERIMENT_REPORTS_LLM_DIR,
    SYN_OBJ_SCRATCH_DIR,
    TIMING_REPORT_RELPATHS,
)


@ChiaFunction(resources={"chipyard": 1.0})
def _rebuild_verilog_remote(
    chipyard_path: str,
    build_config: str,
    make_timeout: int,
) -> tuple[str, list[tuple[str, str]] | None]:
    """Run Chipyard's ``make verilog`` on the chipyard worker.

    Pinned to the chipyard PG via the ``chipyard`` resource so it lands
    on the worker that has the chipyard tree (and shares /tmp with
    ``chipyard_bash``). On success, walks the gen-collateral/ directory
    + ``.top.mems.conf`` to produce a list of ``(filename, content)``
    tuples and returns ``(summary_string, files)``. On failure returns
    ``(error_string, None)`` so the actor's ``rebuild_verilog`` method
    can update its cached state only when the result is good.
    """
    t0 = time.time()
    verilator_dir = os.path.join(chipyard_path, "sims/verilator")
    cmd = ["make", "verilog", f"CONFIG={build_config}"]
    try:
        result = subprocess.run(
            cmd, cwd=verilator_dir, capture_output=True, text=True,
            timeout=make_timeout,
        )
    except subprocess.TimeoutExpired:
        return (
            f"make verilog TIMED OUT after {make_timeout}s. "
            f"Inspect with chipyard_bash in {verilator_dir}.",
            None,
        )
    elapsed = time.time() - t0
    if result.returncode != 0:
        return (
            f"make verilog FAILED (rc={result.returncode}, {elapsed:.1f}s)\n"
            f"command: {' '.join(cmd)}\n"
            f"cwd: {verilator_dir}\n"
            f"stderr (last 4000 chars):\n{result.stderr[-4000:]}",
            None,
        )
    # Collect generated_src files
    gen_src_root = Path(chipyard_path) / "sims/verilator/generated-src"
    candidates = [
        p for p in gen_src_root.glob(f"*{build_config}*")
        if (p / "gen-collateral").is_dir()
    ]
    if not candidates:
        return (
            f"make verilog OK ({elapsed:.1f}s) but no gen-collateral/ dir "
            f"under {gen_src_root} matched '*{build_config}*'",
            None,
        )
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    cfg_dir = candidates[0]
    gc_dir = cfg_dir / "gen-collateral"
    files: list[tuple[str, str]] = []
    for p in sorted(gc_dir.rglob("*")):
        if p.is_file():
            rel = p.relative_to(gc_dir).as_posix()
            try:
                files.append((rel, p.read_text(errors="replace")))
            except Exception:
                continue
    for mc in cfg_dir.glob("*.top.mems.conf"):
        files.append((mc.name, mc.read_text(errors="replace")))
    summary = (
        f"make verilog OK in {elapsed:.1f}s: collected {len(files)} files. "
        f"Call list_modules() for available vlsi_top names, then "
        f"start_synth_child(vlsi_top=..., timeout_seconds=...) to test a sub-block."
    )
    return summary, files


@ChiaFunction(resources={"chipyard": 1.0})
def _stage_experiment_reports_remote(
    exp_id: Optional[int],
    reports: dict[str, str],
) -> str:
    """Pinned to the chipyard PG so the files land where ``chipyard_bash``
    will read them. The actor (head-pinned) calls this each time a synth
    completes, via ``_stage_experiment_reports_remote.options(**pg_opts).chia_remote(...)``.
    """
    exp_tag = str(exp_id) if exp_id is not None else f"unknown_{uuid.uuid4().hex[:8]}"
    local_dir = os.path.join(EXPERIMENT_REPORTS_LLM_DIR, exp_tag)
    try:
        os.makedirs(local_dir, exist_ok=True)
        for relpath, text in (reports or {}).items():
            if not text:
                continue
            basename = relpath.rsplit("/", 1)[-1]
            with open(os.path.join(local_dir, basename), "w") as f:
                f.write(text)
    except Exception as e:
        print(f"  [experiment] stage to {local_dir} failed: {e}")
    return local_dir


@ChiaFunction(num_cpus=0)
def _experiment_orchestrator(
    generated_src: list[tuple[str, str]],
    vlsi_top: str,
    timeout_seconds: int,
    side: str,
    pg_opts: dict,
    logger_actor_name: str,
) -> dict:
    """Background Ray task that runs a single sub-block synth experiment end-to-end.

    Does the full pipeline:
    1. CACTI characterization + MacroCompiler remap (dispatches its own
       VLSI and chipyard child tasks).
    2. Validates ``vlsi_top`` against the post-prep module set.
    3. Filters generated_src to the transitive closure of ``vlsi_top``.
    4. Dispatches Genus via ``run_boom_tile_synthesis.chia_remote``.
    5. Records to the head-side ``ExperimentLogger`` actor.
    6. Returns a dict with every field ``TimingExperimentTool.synth_status``
       needs to format the final user-facing response (including the raw
       ``reports`` dict so the actor can stage them to /tmp on the chipyard
       worker for ``chipyard_bash`` to grep).

    Runs as a regular Ray task (not from inside an actor), which sidesteps a
    placement-group / runtime-env interaction we hit when CACTI was
    dispatched directly from the experiment-tool actor — those dispatches
    stayed PENDING_NODE_ASSIGNMENT forever. Submitting CACTI from inside
    this task (a free-scheduled Ray task) places normally.

    ``num_cpus=0`` because this is a thin orchestrator that mostly waits on
    other Ray tasks. It can run on any node.
    """
    print(f"  [experiment:{side}] CACTI + MacroCompiler prep")
    cacti_prepped_src, cacti_libs, _ = run_cacti_macrocompiler_prep(
        generated_src, pg_opts,
    )

    modules = _parse_verilog_modules(cacti_prepped_src)
    all_names = set(modules.keys())
    if vlsi_top not in all_names:
        close = sorted(n for n in all_names if vlsi_top.lower() in n.lower())[:8]
        return {
            "status": "vlsi_top_not_found",
            "side": side,
            "vlsi_top": vlsi_top,
            "close": close,
            "elapsed_seconds": 0.0,
        }

    children_map = _build_children_map(modules, all_names)
    keep_modules = {vlsi_top} | _get_all_descendants(vlsi_top, children_map)
    filtered_src = [
        (fname, content) for fname, content in cacti_prepped_src
        if any(m in content for m in keep_modules)
        or fname.endswith(".top.mems.conf")
    ]
    print(
        f"  [experiment:{side}] vlsi_top={vlsi_top}: {len(keep_modules)} "
        f"modules in closure, {len(filtered_src)}/{len(cacti_prepped_src)} "
        f"files to synth"
    )

    obj_dir = os.path.join(
        SYN_OBJ_SCRATCH_DIR, f"experiment_{uuid.uuid4().hex[:8]}", "syn_obj",
    )
    t_syn = time.time()
    try:
        syn_result, archive_bytes = get(
            run_boom_tile_synthesis.chia_remote(
                filtered_src, vlsi_top, obj_dir, timeout_seconds, cacti_libs,
            )
        )
    except Exception as e:
        elapsed = time.time() - t_syn
        logger = ray.get_actor(logger_actor_name)
        ray.get(logger.record.remote(
            vlsi_top=vlsi_top, status="synthesis_failed",
            syn_result=None, archive_bytes=b"", elapsed_seconds=elapsed,
        ))
        return {
            "status": "synthesis_failed",
            "side": side,
            "vlsi_top": vlsi_top,
            "elapsed_seconds": elapsed,
            "error": str(e),
        }
    elapsed = time.time() - t_syn

    area = parse_area_from_reports(syn_result.reports)
    status_str = "ok" if syn_result.success else "synthesis_failed"
    logger = ray.get_actor(logger_actor_name)
    record = ray.get(logger.record.remote(
        vlsi_top=vlsi_top, status=status_str,
        syn_result=syn_result, archive_bytes=archive_bytes,
        elapsed_seconds=elapsed,
    ))

    return {
        "status": status_str,
        "side": side,
        "vlsi_top": vlsi_top,
        "modules_in_closure": len(keep_modules),
        "area": area,
        "elapsed_seconds": elapsed,
        "worst_slack_line": (record or {}).get("worst_slack_line"),
        "files_dir": (record or {}).get("files_dir"),
        "exp_id": (record or {}).get("exp_id"),
        # Raw reports — passed through Ray object store to the actor, which
        # writes them to /tmp on the chipyard worker for chipyard_bash to grep.
        "reports": syn_result.reports,
    }


class TimingExperimentTool(ChiaTool):
    """LLM-callable rebuild + sub-block-synth tool.

    Stateful across method calls within an LLM session:

    - ``rebuild_verilog()`` caches the child generated source on the actor.
    - The first ``start_synth_child(...)`` / ``start_synth_parent(...)`` call
      runs CACTI + MacroCompiler prep for that side (~2 min) and caches the
      result; subsequent calls reuse it. SRAM macros don't vary per
      ``vlsi_top``, so this is safe.
    - In-flight synths are tracked in a per-handle dict; ``synth_status`` polls
      the corresponding Ray task and returns either "running" or the full
      structured summary.
    """

    def __init__(
        self,
        name: str,
        chipyard_path: str,
        build_config: str,
        parent_branch: str,
        logger_actor_name: str,
        pg_opts: dict,
        head_node_id: str,
        default_timeout: int = 5400,  # 1.5 hours
        make_timeout: int = 7200,
        task_options: Optional[dict] = None,
        parent_generated_src: Optional[list[tuple[str, str]]] = None,
    ):
        super().__init__(name, task_options=task_options)
        # All instance attributes must be set BEFORE super().__post_init__()
        # because __post_init__ materializes the Ray actor — anything assigned
        # after that point only lives on the client-side proxy, not the remote
        # actor process that handles MCP method calls. (BashTool does the same
        # thing with self.work_dir / self.timeout_seconds.)
        self._chipyard_path = chipyard_path
        self._build_config = build_config
        self._parent_branch = parent_branch
        self._logger_actor_name = logger_actor_name
        # pg_opts: scheduling options for the chipyard placement group bundle.
        # Used to dispatch chipyard-bound work (make verilog, report staging)
        # while the actor itself lives on the head node.
        self._pg_opts = pg_opts
        # head_node_id: where to pin _experiment_orchestrator. The orchestrator
        # must run on the head so its inner cacti/Genus child tasks are
        # head-submitted (avoids the cross-AWS-worker lease storm).
        self._head_node_id = head_node_id
        self._default_timeout = default_timeout
        self._make_timeout = make_timeout
        # Cached generated source. Child fills via rebuild_verilog(); parent
        # is fixed for the session (loaded from the DB at startup).
        # CACTI/MacroCompiler prep is NOT cached on the actor — it runs
        # inside the orchestrator Ray task each call. Earlier we cached it
        # per side, but the actor-side CACTI dispatch deadlocked in
        # PENDING_NODE_ASSIGNMENT (see _start_synth docstring), so we moved
        # the whole pipeline into the background orchestrator.
        self._generated_src: list[tuple[str, str]] | None = None
        self._parent_generated_src: list[tuple[str, str]] | None = parent_generated_src
        # In-flight synth tasks. Keyed by short handle string returned to the
        # LLM; value is {ref, side, vlsi_top, started}. Drained when
        # synth_status fetches the result successfully (or errors).
        self._inflight: dict[str, dict] = {}
        self.mcp.add_tool(self.rebuild_verilog,      name=f"{name}_rebuild_verilog")
        self.mcp.add_tool(self.list_modules,         name=f"{name}_list_modules")
        self.mcp.add_tool(self.list_modules_parent,  name=f"{name}_list_modules_parent")
        self.mcp.add_tool(self.start_synth_child,    name=f"{name}_start_synth_child")
        self.mcp.add_tool(self.start_synth_parent,   name=f"{name}_start_synth_parent")
        self.mcp.add_tool(self.synth_status,         name=f"{name}_synth_status")
        super().__post_init__()

    # ------------------------------------------------------------------ #
    # rebuild_verilog: dispatch `make verilog` to the chipyard worker
    # ------------------------------------------------------------------ #
    def rebuild_verilog(self) -> str:
        """Re-elaborate Chisel into Verilog using Chipyard's ``make verilog`` target.

        Dispatches ``make verilog CONFIG=<BUILD_CONFIG>`` to a chipyard-PG-pinned
        Ray task (``_rebuild_verilog_remote``) that runs on the chipyard worker
        where the chipyard tree lives. This skips the Verilator C++ simulator
        build, so it's much faster than the full build the surrounding pipeline
        does. After it succeeds, the returned generated Verilog files (and the
        ``.top.mems.conf``) are cached on this actor and available to
        ``list_modules()`` and ``start_synth_child()``.

        Returns a one-line summary on success, or a stderr tail on failure.
        """
        summary, files = get(
            _rebuild_verilog_remote.options(**self._pg_opts).chia_remote(
                self._chipyard_path, self._build_config, self._make_timeout,
            )
        )
        if files is not None:
            self._generated_src = files
        return summary

    # ------------------------------------------------------------------ #
    # list_modules / list_modules_parent
    # ------------------------------------------------------------------ #
    def list_modules(self) -> str:
        """List the Verilog module names from the cached *child* generated source.

        Call ``rebuild_verilog()`` first. One name per line; you can pass any
        of these as ``vlsi_top`` to ``synth_module``. Output is truncated past
        500 modules (use chipyard_bash to grep gen-collateral directly).
        """
        if not self._generated_src:
            return "ERROR: no Verilog cached. Call rebuild_verilog() first."
        return self._format_module_list(self._generated_src, side="child")

    def list_modules_parent(self) -> str:
        """List the Verilog module names from the *parent* branch's RTL.

        Available immediately — no ``rebuild_verilog()`` required. Use this in
        Phase 1 to ground your mental model of what modules exist, and to
        diff against ``list_modules()`` after editing to spot structural
        drift (renames, splits, new modules). Pass any returned name as
        ``vlsi_top`` to ``synth_module_parent``.
        """
        if not self._parent_generated_src:
            return (
                "ERROR: parent generated_src was not provided when the tool "
                "was constructed. Re-launch the flow with a populated DB."
            )
        return self._format_module_list(self._parent_generated_src, side="parent")

    def _format_module_list(
        self, generated_src: list[tuple[str, str]], side: str,
    ) -> str:
        """Parse Verilog modules from ``generated_src`` and return one per line."""
        modules = _parse_verilog_modules(generated_src)
        names = sorted(modules.keys())
        if len(names) > 500:
            shown = "\n".join(names[:500])
            return (
                shown
                + f"\n... ({len(names) - 500} more {side} modules truncated; "
                f"grep gen-collateral/ via chipyard_bash to see all)\n"
            )
        return "\n".join(names) + "\n"

    # ------------------------------------------------------------------ #
    # start_synth_child / start_synth_parent — dispatch sub-block synths
    # ------------------------------------------------------------------ #
    def start_synth_child(self, vlsi_top: str, timeout_seconds: int = 0) -> str:
        """Start a sub-block synth on the LLM-edited (child) RTL.

        Async: returns a short handle string in sub-seconds; CACTI prep,
        MacroCompiler remap, and the actual Genus run all happen in the
        background as a single Ray task. Call ``synth_status(handle=...)``
        to check progress and retrieve results when done.

        Total background runtime: ~2 min CACTI/MC prep + 5-30 min Genus,
        per call. (Prep isn't cached across calls — keeps the actor stateless
        and avoids the placement-group hang we hit with actor-cached prep.)

        Issue this in parallel with ``start_synth_parent(vlsi_top=...)`` for
        an A/B comparison: both Genus runs proceed concurrently on the VLSI
        worker (capacity permitting). Then poll the two handles via
        ``synth_status``.

        Args:
            vlsi_top: A Verilog module name from the child RTL. Use
                ``list_modules()`` to see valid options. Pick the *smallest*
                module that contains the bottleneck cluster you targeted in
                Phase 1 — smaller = faster Genus runtime per experiment.
            timeout_seconds: Per-synthesis cap. 0 → use the tool's default
                (5400s = 1.5 hours). Tighten with ``timeout_seconds`` for
                small blocks to fail-fast.

        Returns:
            A multi-line string of the form::

                started: handle=abc12345, side=child, vlsi_top=IssueUnitCollapsing, timeout_seconds=5400
                poll with synth_status(handle="abc12345", max_wait_seconds=180)

            Keep the handle and poll via ``synth_status``. The actor will not
            print further progress; ``synth_status`` is your visibility.
        """
        if not self._generated_src:
            return "ERROR: no Verilog cached. Call rebuild_verilog() first."
        return self._start_synth(
            generated_src=self._generated_src,
            vlsi_top=vlsi_top,
            timeout_seconds=timeout_seconds or self._default_timeout,
            side="child",
        )

    def start_synth_parent(self, vlsi_top: str, timeout_seconds: int = 0) -> str:
        """Start a sub-block synth on the parent branch's *unmodified* RTL.

        Analog of ``start_synth_child`` for the pre-edit baseline. The parent
        RTL is fixed for the session (loaded from the DB at tool startup), so
        no ``rebuild_verilog`` precondition.

        Same async semantics as ``start_synth_child``: returns a handle in
        sub-seconds, the full CACTI prep + Genus pipeline runs in the
        background.

        Issue in parallel with ``start_synth_child(vlsi_top=...)`` for the
        A/B comparison; both Genus runs proceed concurrently on the VLSI
        worker.

        Args / returns: same shape as ``start_synth_child``.
        """
        if not self._parent_generated_src:
            return (
                "ERROR: parent generated_src was not provided when the tool "
                "was constructed. Re-launch the flow with a populated DB."
            )
        return self._start_synth(
            generated_src=self._parent_generated_src,
            vlsi_top=vlsi_top,
            timeout_seconds=timeout_seconds or self._default_timeout,
            side="parent",
        )

    def _start_synth(
        self,
        generated_src: list[tuple[str, str]],
        vlsi_top: str,
        timeout_seconds: int,
        side: str,
    ) -> str:
        """Shared start path: submit the orchestrator Ray task and stash its handle.

        The orchestrator runs the full pipeline (CACTI prep → MacroCompiler
        remap → Genus dispatch → log → return) as a background task. We
        explicitly use scheduling_strategy="DEFAULT" so the orchestrator
        runs free of this actor's chipyard placement group — earlier we
        observed actor-dispatched CACTI tasks sitting in
        PENDING_NODE_ASSIGNMENT forever, presumably due to a PG-inheritance
        / runtime-env interaction. Running the whole pipeline as a Ray task
        avoids that path.

        Sub-second return; the actual work happens in the background and is
        polled via ``synth_status(handle, max_wait_seconds=...)``.
        """
        if side not in ("child", "parent"):
            return f"ERROR: invalid side {side!r}; expected 'child' or 'parent'."

        # NodeAffinity to head_node_id (soft=False) — the orchestrator MUST
        # run on the head so its inner cacti / Genus dispatches are
        # head-submitted. We hit a cross-AWS-worker lease storm when the
        # orchestrator landed on a non-head node and tried to dispatch a
        # VLSI=1 child task (see chia/issuesexperienced/cross-worker-lease-storm.md).
        ref = _experiment_orchestrator.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=self._head_node_id, soft=False,
            )
        ).chia_remote(
            generated_src, vlsi_top, timeout_seconds, side,
            self._pg_opts, self._logger_actor_name,
        )
        handle = uuid.uuid4().hex[:8]
        self._inflight[handle] = {
            "ref": ref,
            "side": side,
            "vlsi_top": vlsi_top,
            "started": time.time(),
        }
        print(
            f"  [experiment:{side}] start_synth dispatched: handle={handle}, "
            f"vlsi_top={vlsi_top}, timeout_seconds={timeout_seconds}"
        )
        return (
            f"started: handle={handle}, side={side}, vlsi_top={vlsi_top}, "
            f"timeout_seconds={timeout_seconds}\n"
            f"CACTI/MacroCompiler prep (~2 min) + Genus synth (~5-30 min) "
            f"run in the background.\n"
            f"poll with synth_status(handle=\"{handle}\", max_wait_seconds=180)"
        )

    # ------------------------------------------------------------------ #
    # synth_status — poll an in-flight synth
    # ------------------------------------------------------------------ #
    def synth_status(self, handle: str, max_wait_seconds: int = 0) -> str:
        """Check on an in-flight synth dispatched by ``start_synth_child`` /
        ``start_synth_parent``.

        Args:
            handle: The handle returned by a previous ``start_synth_*`` call.
            max_wait_seconds: 0 → return current state immediately. >0 →
                block up to that many seconds for the synth to finish before
                responding. Cap is 240; keep it under ~200 so the MCP HTTP
                round-trip stays inside Claude's session timeout. Recommended
                value: 180 (3 minutes).

        Returns:
            While running, a multi-line string of the form::

                status: running
                handle: abc12345
                side: child
                vlsi_top: IssueUnitCollapsing
                elapsed_seconds: 423.1
                retry: synth_status(handle="abc12345", max_wait_seconds=180)

            On completion (ok or synthesis_failed)::

                status: ok
                handle: abc12345
                side: child
                exp_id: 7
                vlsi_top: IssueUnitCollapsing
                modules_in_closure: 23
                area: 412855.4
                worst_slack: Path 1: VIOLATED (-8421 ps) Setup Check with Pin core/...
                elapsed_seconds: 1247.3
                timing_report: /tmp/improve_timing/experiments/7/final_constrained.rpt
                reports_dir: /tmp/improve_timing/experiments/7
                db_files_dir: /scratch/.../DB/files/<branch>/experiments/7
                hint: grep the timing_report via chipyard_bash on this node...

            Once a result has been fetched successfully, the handle is dropped
            from the actor's in-flight set — calling ``synth_status`` again
            with the same handle will error.
        """
        inflight = self._inflight.get(handle)
        if not inflight:
            return (
                f"ERROR: no such handle '{handle}'. Either the handle was "
                f"already polled to completion (results are one-shot), it "
                f"was never returned by start_synth_*, or the actor "
                f"restarted."
            )
        ref = inflight["ref"]
        timeout = max(0, min(int(max_wait_seconds), 240))
        ready, _ = ray.wait([ref], timeout=timeout)
        if not ready:
            elapsed = time.time() - inflight["started"]
            return (
                f"status: running\n"
                f"handle: {handle}\n"
                f"side: {inflight['side']}\n"
                f"vlsi_top: {inflight['vlsi_top']}\n"
                f"elapsed_seconds: {elapsed:.1f}\n"
                f"retry: synth_status(handle=\"{handle}\", max_wait_seconds=180)"
            )
        try:
            result = ray.get(ref)
        except Exception as e:
            del self._inflight[handle]
            return (
                f"synth FAILED (handle={handle}, side={inflight['side']}, "
                f"vlsi_top={inflight['vlsi_top']}): {e}"
            )
        del self._inflight[handle]
        return self._format_result(handle, result)

    def _format_result(self, handle: str, result: dict) -> str:
        """Stage reports to /tmp on the chipyard worker and produce the
        user-facing summary string for a completed (or failed) experiment.

        Stage runs as a chipyard-pinned Ray remote
        (``_stage_experiment_reports_remote``) because the actor lives on
        the head — the reports need to land on the chipyard worker's /tmp
        for ``chipyard_bash`` to grep them. The head-side ``files_dir``
        from the DB lives on the head's /scratch and is invisible from
        the chipyard container, so /tmp is the LLM's actual reading path.
        """
        status = result.get("status", "?")
        side = result.get("side", "?")
        vlsi_top = result.get("vlsi_top", "?")
        elapsed = result.get("elapsed_seconds", 0.0)

        if status == "vlsi_top_not_found":
            close = result.get("close") or []
            list_call = "list_modules" if side == "child" else "list_modules_parent"
            hint = (
                f"\nSimilarly-named modules in {side} RTL: {close}" if close
                else f"\n(no similar names in {side} RTL; call {list_call}() to see valid names)"
            )
            return f"ERROR: '{vlsi_top}' is not a Verilog module name.{hint}"

        if status == "synthesis_failed" and "error" in result:
            return (
                f"synth FAILED (handle={handle}, side={side}, vlsi_top={vlsi_top}, "
                f"{elapsed:.1f}s): {result['error']}"
            )

        exp_id = result.get("exp_id")
        reports = result.get("reports") or {}
        local_dir = get(
            _stage_experiment_reports_remote.options(**self._pg_opts).chia_remote(
                exp_id, reports,
            )
        )
        timing_local = next(
            (os.path.join(local_dir, p.split("/")[-1])
             for p in TIMING_REPORT_RELPATHS
             if reports.get(p)),
            None,
        )
        timing_hint = (
            f"timing_report: {timing_local}"
            if timing_local else
            "timing_report: (none — no constrained/setup_view report emitted)"
        )
        return (
            f"status: {status}\n"
            f"handle: {handle}\n"
            f"side: {side}\n"
            f"exp_id: {exp_id}\n"
            f"vlsi_top: {vlsi_top}\n"
            f"modules_in_closure: {result.get('modules_in_closure', '?')}\n"
            f"area: {result.get('area', '?')}\n"
            f"worst_slack: {result.get('worst_slack_line') or '(no slack line)'}\n"
            f"elapsed_seconds: {elapsed:.1f}\n"
            f"{timing_hint}\n"
            f"reports_dir: {local_dir}\n"
            f"db_files_dir: {result.get('files_dir', '(?)')}\n"
            f"hint: grep the timing_report or any file under reports_dir via "
            f"chipyard_bash on this node (same machine as bash). E.g. "
            f"`grep -E '^Path [0-9]+:' {timing_local or local_dir + '/<file>.rpt'} | head -30`."
        )

