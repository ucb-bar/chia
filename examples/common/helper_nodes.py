"""
  * ``collect_diff``                 (:336)  — git diffs from chipyard root +
    submodules; returns ``(error, diffs)``.
  * ``reset_and_apply_diff``         (:401)  — reset chipyard + submodules
    to the commits recorded in HEAD, then git-apply a stored diff dict;
    returns ``(error, message)``.
  * ``debug_failure``                (:1401) — LLM debugger ChiaFunction
    (Claude Code ``/debugging`` command via chipyard_bash).
  * ``VerilatorTestOutcome``         (:1219) — result dataclass used by
    ``common.verilator.dispatch_verilator_tests_debug``.
  * ``_parse_verilog_modules``       (:466), ``_resolve_boom_tile_module``
    (:562) — generated-Verilog parsing for synthesis targeting.
  * ``_build_children_map`` / ``_get_all_descendants`` — module-instantiation
    hierarchy helpers (used by the timing example's experiment tool to filter
    generated_src to a vlsi_top's transitive closure).
  * ``run_cacti_characterization``   (:700), ``run_macrocompiler_remap``
    (:722) — SRAM characterization / remap task nodes.
  * ``run_cacti_macrocompiler_prep`` — CACTI characterize + MacroCompiler
    remap + BoomTile resolve, redone per synthesis attempt.
  * ``parse_area_from_reports``      (:822) + numeric helpers (:784, :789)
    — Genus area report parsing.
"""

from __future__ import annotations

import csv
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from uuid import uuid4

from chia.base.ChiaFunction import ChiaFunction, get
from chia.base.llm_call import QueryResult
from chia.base.tools.BashTool import BashTool
from chia.chipyard.state_def import RunResult


@ChiaFunction(resources={"chipyard": 1.0})
def collect_diff(
    chipyard_path: str = "/home/ray/chipyard/",
    submodules: Optional[list[str]] = ["generators/boom", "generators/rocket-chip", "generators/rocket-chip-inclusive-cache", "generators/rocket-chip-blocks", "generators/bar-fetchers"],
    from_commit: Optional[str] = None,
    to_commit: Optional[str] = None
    ) -> tuple[int, dict[str, str]]:
    """Collect git diffs from the root chipyard repo and specified submodules.

    Returns (error, diffs) where diffs is a dict mapping repo path to diff text.
    Key "" = root chipyard diff; "generators/boom" etc = per-submodule diffs.
    Submodule diffs are collected from within each submodule so they are directly
    git-apply-able in that submodule's directory.

    Returns error=1 if any specified submodules do not exist or if to_commit is
    set without from_commit.
    """
    if to_commit and not from_commit:
        return (1, {})

    if submodules:
        invalid = []
        for sm in submodules:
            sm_path = os.path.join(chipyard_path, sm)
            git_dir = os.path.join(sm_path, ".git")
            if not os.path.exists(sm_path) or not os.path.exists(git_dir):
                invalid.append(sm)
        if invalid:
            return (1, {})

    if from_commit and to_commit:
        diff_range = [from_commit, to_commit]
    elif from_commit:
        diff_range = [from_commit]
    else:
        diff_range = []

    diffs: dict[str, str] = {}

    root_diff = subprocess.run(
        ["git", "diff", "--ignore-submodules=all"] + diff_range,
        cwd=chipyard_path,
        capture_output=True,
        text=True,
    )
    diffs[""] = root_diff.stdout

    if submodules:
        for sm in submodules:
            sm_path = os.path.join(chipyard_path, sm)
            # Stage untracked files as intent-to-add so they appear in the diff
            subprocess.run(["git", "add", "-N", "."], cwd=sm_path)
            sm_diff = subprocess.run(
                ["git", "diff"] + diff_range,
                cwd=sm_path,
                capture_output=True,
                text=True,
            )
            diffs[sm] = sm_diff.stdout
            # Reset the index to undo the intent-to-add
            subprocess.run(["git", "reset"], cwd=sm_path)

    return (0, diffs)


@ChiaFunction(resources={"chipyard": 1.0})
def reset_and_apply_diff(
    diff_dict: dict[str, str],
    chipyard_path: str = "/home/ray/chipyard/",
    submodules: Optional[list[str]] = ["generators/boom", "generators/rocket-chip", "generators/rocket-chip-inclusive-cache", "generators/rocket-chip-blocks", "generators/bar-fetchers"],
) -> tuple[int, str]:
    """Reset chipyard + submodules to their recorded commits, then apply diff_dict.

    diff_dict keys: "" = root chipyard, "generators/boom" etc = submodule paths.
    Returns (error, message).
    """
    # Reset root repo
    subprocess.run(["git", "reset", "--hard", "HEAD"], cwd=chipyard_path)
    subprocess.run(["git", "clean", "-fd"], cwd=chipyard_path)
    # Reset each submodule to the commit recorded in the parent repo
    # (not HEAD, which may have been polluted by previous debug sessions)
    if submodules:
        for sm in submodules:
            sm_path = os.path.join(chipyard_path, sm)
            if os.path.isdir(sm_path):
                # Get the commit the parent repo expects for this submodule
                r = subprocess.run(
                    ["git", "ls-tree", "HEAD", sm],
                    cwd=chipyard_path, capture_output=True, text=True,
                )
                if r.returncode == 0 and r.stdout.strip():
                    expected_commit = r.stdout.split()[2]
                    subprocess.run(["git", "reset", "--hard", expected_commit], cwd=sm_path)
                else:
                    subprocess.run(["git", "reset", "--hard", "HEAD"], cwd=sm_path)
                subprocess.run(["git", "clean", "-fd"], cwd=sm_path)

    if not diff_dict:
        return (0, "Clean state — no diff to apply")

    # Apply root diff
    if diff_dict.get(""):
        r = subprocess.run(
            ["git", "apply", "--ignore-whitespace"],
            input=diff_dict[""], text=True,
            cwd=chipyard_path, capture_output=True,
        )
        if r.returncode != 0:
            return (1, f"root git apply failed: {r.stderr}")

    # Apply per-submodule diffs
    if submodules:
        for sm in submodules:
            if diff_dict.get(sm):
                sm_path = os.path.join(chipyard_path, sm)
                r = subprocess.run(
                    ["git", "apply", "--ignore-whitespace"],
                    input=diff_dict[sm], text=True,
                    cwd=sm_path, capture_output=True,
                )
                if r.returncode != 0:
                    return (1, f"{sm} git apply failed: {r.stderr}")

    return (0, "Applied diff successfully")


def _parse_verilog_modules(generated_src: list[tuple[str, str]]) -> dict[str, str]:
    """Parse generated Verilog files into a map of module_name → module_body.

    Splits each .v/.sv file on ``module <name>`` ... ``endmodule`` boundaries
    and returns a dict mapping each module name to its full declaration text
    (including the ``module`` and ``endmodule`` lines).
    """
    module_re = re.compile(r'^module\s+(\w+)', re.MULTILINE)
    endmodule_re = re.compile(r'^endmodule', re.MULTILINE)
    modules: dict[str, str] = {}

    for filename, contents in generated_src:
        if not filename.endswith((".v", ".sv")):
            continue
        # Find all module declarations in this file
        for m_match in module_re.finditer(contents):
            mod_name = m_match.group(1)
            start = m_match.start()
            # Find the corresponding endmodule
            end_match = endmodule_re.search(contents, m_match.end())
            if end_match:
                body = contents[start:end_match.end()]
            else:
                # No endmodule found — take rest of file
                body = contents[start:]
            modules[mod_name] = body
    return modules


# Regex to detect module instantiations in a Verilog body.
# Matches lines like:  ModuleName instance_name (   or   ModuleName #(
_INST_RE = re.compile(r'^\s+(\w+)\s+(?:\w+\s*\(|#\()', re.MULTILINE)


def _build_children_map(
    modules: dict[str, str],
    all_module_names: set[str],
) -> dict[str, list[str]]:
    """Build a mapping from each module to the module types it instantiates."""
    children_map: dict[str, list[str]] = {}
    for mod_name, body in modules.items():
        children: list[str] = []
        for m in _INST_RE.finditer(body):
            candidate = m.group(1)
            if candidate in all_module_names:
                children.append(candidate)
        children_map[mod_name] = children
    return children_map


def _get_all_descendants(
    module_name: str,
    children_map: dict[str, list[str]],
) -> set[str]:
    """Return every descendant of *module_name* at all depths (excluding itself)."""
    visited: set[str] = set()
    queue = list(children_map.get(module_name, []))
    while queue:
        mod = queue.pop()
        if mod in visited:
            continue
        visited.add(mod)
        queue.extend(children_map.get(mod, []))
    return visited


def _resolve_boom_tile_module(
    modules: dict[str, str],
    all_module_names: set[str],
) -> str | None:
    """Return the module type name for the ``element_reset_domain_boom_tile`` instance.

    Checks whether ``element_reset_domain_boom_tile`` is itself a module name;
    if not, scans module bodies for an instantiation with that instance name
    (e.g. ``BoomTile element_reset_domain_boom_tile (``).
    """
    target = "element_reset_domain_boom_tile"
    if target in modules:
        return target
    inst_name_re = re.compile(
        r'^\s+(\w+)\s+element_reset_domain_boom_tile\s*\(', re.MULTILINE,
    )
    for body in modules.values():
        m = inst_name_re.search(body)
        if m and m.group(1) in all_module_names:
            return m.group(1)
    return None


@ChiaFunction(resources={"VLSI": 1, "Syn": 0})
def run_cacti_characterization(
    generated_src_files: list[tuple[str, str]],
    cacti_path: str = "/path/to/cacti/cacti",
) -> tuple[list[tuple[str, str]], list[dict[str, str]], list[str]]:
    """Run CACTI SRAM characterization on a VLSI node.

    Parses .top.mems.conf, runs CACTI for each large SRAM, generates
    Liberty file contents. Does NOT modify generated_src — that happens
    after MacroCompiler remap.
    """
    # characterize_top_mems_conf_with_cacti is a ChiaFunction requiring the
    # "cacti" resource. From outside chia/vlsi/sram_cacti we invoke it remotely
    # so it lands on a cacti worker, then flatten its CactiCharacterization back
    # to the (generated_src, sram_libs, sram_names) tuple this ChiaFunction has
    # always returned so downstream consumers stay unchanged.
    from chia.vlsi.sram_cacti.sram_characterize import characterize_top_mems_conf_with_cacti
    from chia.base.ChiaFunction import get
    char = get(characterize_top_mems_conf_with_cacti.chia_remote(generated_src_files, cacti_path))
    return char.generated_src_files, char.sram_libs, char.sram_names


@ChiaFunction(resources={"chipyard": 1.0})
def run_macrocompiler_remap(
    mems_conf_content: str,
    macrocompiler_lib_json: str,
    chipyard_path: str = "/home/ray/chipyard/",
) -> str | None:
    """Run MacroCompiler on a chipyard node to remap synflop SRAMs to cacti_* macros.

    Returns the remapped .top.mems.v content, or None on failure.
    """
    from chia.vlsi.sram_cacti.sram_characterize import remap_with_macrocompiler
    return remap_with_macrocompiler(mems_conf_content, macrocompiler_lib_json, chipyard_path)


def run_cacti_macrocompiler_prep(
    gen_src,
    pg_opts,
    cacti_path: str = "/path/to/cacti/cacti",
):
    """CACTI characterize + MacroCompiler remap, then resolve the BoomTile module.

    Returns ``(gen_src, cacti_libs, new_boom_tile)``; ``new_boom_tile`` is None if
    the BoomTile module could not be resolved. Redone per synthesis attempt
    because debugger edits can change the generated Verilog. ``pg_opts`` are the
    placement-group scheduling options forwarded to the MacroCompiler remap task.
    """
    from chia.chipyard.macrocompiler import generate_macro_stubs
    from chia.vlsi.sram_cacti.cacti_runner import parse_mems_conf
    from chia.vlsi.sram_cacti.sram_characterize import (
        assemble_generated_src_with_cacti,
        generate_cacti_macrocompiler_lib,
    )
    # scheduling_strategy="DEFAULT" lets this dispatch escape any enclosing
    # placement group. Critical when called from the experiment-tool actor
    # (which lives inside the chipyard PG bundle, which has no VLSI
    # capacity); without the override Ray implicitly places the cacti task
    # inside the actor's PG and it sits in PENDING_NODE_ASSIGNMENT forever.
    # No-op from the main-flow driver path (the driver isn't in a PG).
    gen_src, cacti_libs, _ = get(
        run_cacti_characterization.options(
            scheduling_strategy="DEFAULT"
        ).chia_remote(gen_src, cacti_path)
    )
    print(f"  CACTI libs: {len(cacti_libs) if cacti_libs else 0}")

    mems_conf = next(
        (c for n, c in gen_src if n.endswith(".top.mems.conf")), None,
    )
    if mems_conf and cacti_libs:
        specs = parse_mems_conf(mems_conf)
        mc_lib_json = generate_cacti_macrocompiler_lib(specs)
        remapped_v = get(run_macrocompiler_remap.options(**pg_opts).chia_remote(
            mems_conf, mc_lib_json,
        ))
        if remapped_v:
            gen_src = assemble_generated_src_with_cacti(
                gen_src, remapped_v, generate_macro_stubs(specs, "cacti_"),
            )
            print(f"  MacroCompiler remap OK — {len(gen_src)} files")
        else:
            print(f"  MacroCompiler remap returned None — proceeding with synflops")
    else:
        print(f"  Skipping MacroCompiler: mems_conf={bool(mems_conf)}, cacti_libs={bool(cacti_libs)}")

    new_modules = _parse_verilog_modules(gen_src)
    new_boom_tile = _resolve_boom_tile_module(new_modules, set(new_modules.keys()))
    return gen_src, cacti_libs, new_boom_tile


def _parse_number_with_commas(s: str) -> float:
    """Parse a number string that may contain commas as thousands separators."""
    return float(s.replace(",", ""))


def _parse_area_from_final_area_rpt(content: str) -> float | None:
    """Parse Total Area from Genus final_area.rpt table format.

    Returns the Total Area value (last numeric column on the data line).
    Total Area = Cell Area + Net Area, which includes the estimated routing
    overhead from wire-load models.
    """
    # Match the data line after the dashes separator: instance name followed by numbers
    data_re = re.compile(
        r'^(\S+)\s+'              # Instance name
        r'(?:\S+\s+)?'           # Optional Module name
        r'(\d[\d,]*)\s+'         # Cell Count
        r'([\d,]+\.?\d*)\s+'     # Cell Area
        r'([\d,]+\.?\d*)\s+'     # Net Area
        r'([\d,]+\.?\d*)',        # Total Area
        re.MULTILINE,
    )
    match = data_re.search(content)
    if match:
        try:
            return _parse_number_with_commas(match.group(5))  # Total Area column
        except ValueError:
            pass
    return None


def parse_area_from_reports(reports: dict[str, str]) -> float | None:
    """Search synthesis report files for total cell area.
    """
    # Primary: parse final_area.rpt directly
    for report_name, report_contents in reports.items():
        if "final_area" in report_name:
            area = _parse_area_from_final_area_rpt(report_contents)
            if area is not None and area > 0:
                print(f"  [synthesis] Parsed area={area:.2f} from report '{report_name}' "
                      f"(final_area.rpt table)")
                return area

    print("  [synthesis] WARNING: Could not parse area from any synthesis report")
    return None


@dataclass
class VerilatorTestOutcome:
    """Result of dispatching verilator tests, with early-exit support."""
    results: list[RunResult] = field(default_factory=list)   # all completed (pass + fail)
    failed: list[RunResult] = field(default_factory=list)    # just failures
    cancelled: bool = False                                   # True if early-exit triggered


def load_prompt(path: str | Path, *args: str) -> str:
    """Read a prompt ``.md`` file and substitute slash-command arguments.

    Mirrors Claude Code's command-argument expansion so the same ``.md``
    files work sent inline instead of as ``/command`` invocations:
    ``$1``..``$9`` get the positional args and ``$ARGUMENTS`` gets the full
    space-joined argument string. Lets pipelines ship their own prompts
    (e.g. ``core_ipc_opt/prompts/``) rather than relying on the commands
    being installed in the LLM environment's ``.claude/commands/``.
    """
    text = Path(path).read_text()
    for i, arg in enumerate(args, start=1):
        text = text.replace(f"${i}", arg)
    if args:
        text = text.replace("$ARGUMENTS", " ".join(args))
    return text


@ChiaFunction()
def debug_failure(
    error_context: str,
    chipyard_bash: BashTool,
    attempt: int = 1,
    session_id: str | None = None,
    session_transcript: bytes | None = None,
    return_transcript: bool = False,
    llm_env: str = "/home/ray/llm_env",
    prompt_text: str = "",
    aux_files: dict[str, str] | None = None,
    claude_binary: str = "claude",
) -> CLIResult | tuple[CLIResult, bytes]:
    """LLM debugger — diagnoses and fixes build/test failures.

    When *session_id* is provided, the session persists across retry
    attempts so the LLM remembers prior fixes it already tried.

    Prompt delivery: by default the debugger relies on the ``/debugging``
    Claude Code slash command being installed in ``llm_env``'s
    ``.claude/commands/``. Callers whose LLM environment does NOT have the
    commands installed pass ``prompt_text`` — the full prompt markdown read
    from their own prompts dir — and it is sent inline instead. Any
    ``{AUX_DIR}`` placeholder in ``prompt_text`` is replaced with a per-call
    temp dir on the LLM machine into which ``aux_files``
    (``{filename: content}`` — e.g. reference docs the prompt tells the LLM
    to read) are written by ``setup()`` and deleted again after the call.

    Session threading: pass ``session_id`` to keep a real Claude Code
    session across retry attempts, and ``session_transcript`` (e.g. the
    bytes returned by a prior call on the same session) to make this call
    ``--resume`` that conversation; set ``return_transcript=True`` to also
    get the updated transcript bytes back for the next call in the chain.
    """
    # Per-call aux dir on the LLM machine (uuid computed here; setup() —
    # which runs ON THE LLM MACHINE — actually creates it).
    aux_dir = f"/tmp/llm_aux/{uuid4().hex[:8]}"

    def setup():
        os.chdir(llm_env)
        os.makedirs("/tmp/ray/llm_logs", exist_ok=True)
        if aux_files:
            os.makedirs(aux_dir, exist_ok=True)
            for name, content in aux_files.items():
                with open(os.path.join(aux_dir, name), "w") as f:
                    f.write(content)

    def cleanup(cli, out_transcript, _unused):
        shutil.rmtree(aux_dir, ignore_errors=True)
        # Return the (cli, transcript) 2-tuple shape — cleanup's return
        # REPLACES the task's default ``(cli, transcript)``.
        return cli, out_transcript

    if prompt_text:
        prompt = (f"{prompt_text.replace('{AUX_DIR}', aux_dir)}\n\n"
                  f"{error_context}")
    else:
        prompt = f"/debugging {error_context}"

    # Route through chia.models.claude: runs setup + prompt on an 'llm' worker,
    # preserving the (cli, transcript) contract incl. cleanup so session
    # threading is identical. _creds_prompt_task defaults to the 'claude_creds'
    # resource; override it to 'llm' (1.0/call) so it schedules on the cluster's
    # existing LLM nodes.
    from chia.examples.common.common_helpers import _creds_prompt_task
    models_config = dict(
        model="claude-opus-4-6",
        timeout_seconds=1800,
        log_dir="/tmp/ray/llm_logs",
        logging_name=f"debugger_attempt{attempt}",
        resume_session=session_id is not None,
        projects_cwd=None,  # derive from the worker CWD that setup() chdirs to
        extra_cli_args=["--effort", "max"],
    )
    result = get(
        _creds_prompt_task.options(resources={"llm": 1.0}).chia_remote(
            models_config, prompt, [chipyard_bash],
            setup, (), cleanup if aux_files else None, (None,),
            session_id, session_transcript,
        ))
    # With cleanup the task returns cleanup's (cli, transcript); without it
    # the default (cli, transcript). Either way unpack the pair.
    cli, out_transcript = result
    if return_transcript:
        return cli, out_transcript
    return cli


def _log_timing(timing_path: str, iteration: int, step: str, duration_s: float):
    with open(timing_path, "a", newline="") as f:
        csv.writer(f).writerow([iteration, step, f"{duration_s:.1f}"])


def _elapsed(t0):
    s = time.time() - t0
    return f"{s:.1f}s" if s < 60 else f"{s/60:.1f}m"
