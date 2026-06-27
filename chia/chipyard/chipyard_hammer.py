"""chia.chipyard.chipyard_hammer — Chipyard VLSI (hammer) makefile nodes.

Chipyard wraps hammer-vlsi behind ``make`` in ``<chipyard>/vlsi``: the
``buildfile`` target elaborates the design and generates ``$(OBJ_DIR)/
hammer.d``, a make fragment that is ``-include``d to provide the flow targets
(``syn``, ``par``, ``drc``, ``lvs``, ``power``, ``redo-<action>``, ...).
Everything is parameterized by make variables — ``CONFIG``, ``tech_name``,
``TOOLS_CONF`` / ``TECH_CONF`` / ``INPUT_CONFS``, ``VLSI_TOP``, ``OBJ_DIR``,
``HAMMER_EXTRA_ARGS``, ... — so one generic target runner covers the whole
flow.  :meth:`ChipyardHammerNode.make` is that runner.

The chipyard checkout, its generated RTL, and the OBJ_DIR are PATH-BASED on
the worker, so chained targets (buildfile -> syn -> par) and report fetches
(:meth:`ChipyardHammerNode.collect`) must land on the SAME worker.
:class:`ChipyardHammerNode` enforces that via a placement group (see
:class:`chia.base.colocated.ColocatedNode` for the given / reserved / no-PG
construction modes).  ``ChipyardHammerNode.<fn>.chia_remote(...)`` (the class
attribute) is the raw, unpinned form for callers that handle placement
themselves.

Like ``chia.chipyard.chisel_build_node``, this module assumes the worker's
environment is already chipyard-ready (the chipyard docker images source the
env in their setup commands); use the ``env`` argument for per-call overrides.

This module is deliberately independent of ``chia.vlsi.hammer``, which wraps
bare ``hammer-vlsi`` CLI calls: here hammer is an implementation detail behind
chipyard's Makefile.
"""

import glob as _glob
import logging
import os
import signal
import subprocess
from dataclasses import dataclass, field

from chia.base.ChiaFunction import ChiaFunction
from chia.base.colocated import ColocatedNode

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ChipyardHammerResult:
    success: bool
    returncode: int
    target: str
    vlsi_dir: str          # <chipyard>/vlsi on the worker that ran make
    obj_dir: str | None    # the OBJ_DIR passed in (None if chipyard's default)
    stdout: str
    stderr: str
    # Manifest of every file under obj_dir after the run: relative path ->
    # size in bytes.  Empty when obj_dir was not given (chipyard's default
    # OBJ_DIR embeds generated names this node does not compute).  Contents
    # stay on the worker; fetch them with ChipyardHammerNode.collect pinned
    # to the same bundle.
    listing: dict[str, int] = field(default_factory=dict)


@dataclass
class ChipyardHammerCollectResult:
    base_dir: str
    files: dict[str, str]      # relpath -> text contents (errors="replace")
    skipped: dict[str, int]    # matched but over max_bytes_per_file; size shown
    listing: dict[str, int]    # fresh manifest of base_dir at collect time


# ---------------------------------------------------------------------------
# Worker-side helpers (module-level so they resolve by import on the worker)
# ---------------------------------------------------------------------------

def _list_files(base_dir: str) -> dict[str, int]:
    """Manifest of every file under base_dir: relative path -> size in bytes."""
    listing: dict[str, int] = {}
    for root, _dirs, names in os.walk(base_dir):
        for name in names:
            path = os.path.join(root, name)
            try:
                listing[os.path.relpath(path, base_dir)] = os.path.getsize(path)
            except OSError:
                pass  # dangling symlink etc.
    return listing


# ---------------------------------------------------------------------------
# ChipyardHammerNode
# ---------------------------------------------------------------------------

class ChipyardHammerNode(ColocatedNode):
    """Chipyard VLSI make / collect primitives sharing one placement.

    The members are ``@staticmethod @ChiaFunction(resources={"chipyard": 1})``;
    ``__init__`` re-binds each into a per-instance pinned form so
    ``node.<fn>.chia_remote(...)`` lands on this node's bundle::

        with ChipyardHammerNode() as node:   # reserves {"CPU": 1, "chipyard": 1}
            obj_dir = "/scratch/vlsi-build/run1"
            bf = get(node.make.chia_remote(
                "/home/ray/chipyard", "buildfile", config="RocketConfig",
                obj_dir=obj_dir, make_vars={"tech_name": "sky130"}))
            syn = get(node.make.chia_remote(
                "/home/ray/chipyard", "syn", config="RocketConfig",
                obj_dir=obj_dir, make_vars={"tech_name": "sky130"}))
            rpts = get(node.collect.chia_remote(
                obj_dir, ["syn-rundir/reports/**"]))   # same worker, guaranteed
    """

    _MEMBER_FNS = ("make", "collect")
    _DEFAULT_BUNDLE = {"CPU": 1, "chipyard": 1}

    @staticmethod
    @ChiaFunction(resources={"chipyard": 1})
    def make(
        chipyard_path: str,
        target: str,
        config: str | None = None,
        obj_dir: str | None = None,
        make_vars: dict[str, str] | None = None,
        jobs: int = 1,
        env: dict[str, str] | None = None,
        timeout_seconds: int = 86400,
    ) -> ChipyardHammerResult:
        """Run one ``make -C <chipyard>/vlsi <target>`` on a worker.

        Args:
            chipyard_path: Chipyard checkout root on the worker.
            target: Any vlsi Makefile target: "buildfile", "syn", "par",
                "drc", "lvs", "power", "redo-syn", "clean", ...  (the flow
                targets exist after "buildfile" has generated hammer.d).
            config: Chipyard CONFIG (e.g. "RocketConfig"). Convenience for
                ``make_vars={"CONFIG": ...}``.
            obj_dir: Hammer build directory, passed as ``OBJ_DIR=`` and used
                for ``listing``.  When None, chipyard derives its default
                (``vlsi/build/<long_name>-<TOP>``) and ``listing`` is empty.
                Pass the same value across buildfile/syn/par calls.
            make_vars: Any other vlsi Makefile variables, e.g.
                ``{"tech_name": "sky130", "VLSI_TOP": "ChipTop",
                "INPUT_CONFS": "a.yml b.yml", "HAMMER_EXTRA_ARGS": "-p x.yml"}``.
                Appended last, so they win over ``config``/``obj_dir``.
            jobs: make -j level (elaboration in "buildfile" benefits; the
                hammer flow targets manage their own parallelism).
            env: Extra environment variables layered over the worker's
                (assumed chipyard-ready) environment.
            timeout_seconds: Wall-clock limit for the subprocess.
        """
        vlsi_dir = os.path.join(os.path.abspath(chipyard_path), "vlsi")
        if obj_dir is not None:
            obj_dir = os.path.abspath(obj_dir)

        cmd = ["make", "-C", vlsi_dir, f"-j{jobs}"]
        if config is not None:
            cmd.append(f"CONFIG={config}")
        if obj_dir is not None:
            cmd.append(f"OBJ_DIR={obj_dir}")
        for key, value in (make_vars or {}).items():
            cmd.append(f"{key}={value}")
        cmd.append(target)
        logger.info(f"Running: {' '.join(cmd)}")

        # start_new_session puts the whole make/sbt/tool tree in one process
        # group; chia's pid_registry tracks the pgid so chia_cancel() can
        # kill it.
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
            env={**os.environ, **env} if env else None,
        )
        try:
            stdout, stderr = proc.communicate(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            stdout, stderr = proc.communicate()
            stderr += f"\nmake {target} timed out after {timeout_seconds}s"
            logger.error(f"make {target} timed out after {timeout_seconds}s")

        if proc.returncode != 0:
            logger.error(f"make {target} failed (rc={proc.returncode}); "
                         f"stderr tail: {stderr[-500:] if stderr else '(empty)'}")

        return ChipyardHammerResult(
            success=proc.returncode == 0,
            returncode=proc.returncode,
            target=target,
            vlsi_dir=vlsi_dir,
            obj_dir=obj_dir,
            stdout=stdout,
            stderr=stderr,
            listing=_list_files(obj_dir) if obj_dir is not None else {},
        )

    @staticmethod
    @ChiaFunction(resources={"chipyard": 1})
    def collect(
        base_dir: str,
        patterns: list[str],
        max_bytes_per_file: int | None = None,
    ) -> ChipyardHammerCollectResult:
        """Fetch text files from a previous make's OBJ_DIR (or any directory)
        on this worker.

        Dispatch via the pinned instance member (``node.collect.chia_remote``)
        so it lands on the worker that owns the files — an unpinned call may
        not.

        Args:
            base_dir: Directory a previous target ran in (typically the
                ``obj_dir`` passed to :meth:`make`).
            patterns: Globs relative to base_dir (``**`` is recursive), e.g.
                ``["syn-rundir/reports/**", "*.log"]``.  Files matched by
                multiple patterns appear once.
            max_bytes_per_file: When set, files over this size are recorded in
                ``skipped`` instead of shipped through the object store —
                protects against a glob accidentally matching a netlist.
                ``None`` (and 0, the falsy edge) means no cap: everything
                matched is shipped.
        """
        base_dir = os.path.abspath(base_dir)
        files: dict[str, str] = {}
        skipped: dict[str, int] = {}
        for pattern in patterns:
            for path in _glob.glob(os.path.join(base_dir, pattern), recursive=True):
                if not os.path.isfile(path):
                    continue
                rel = os.path.relpath(path, base_dir)
                if rel in files or rel in skipped:
                    continue
                try:
                    size = os.path.getsize(path)
                except OSError:
                    continue
                if max_bytes_per_file and (size > max_bytes_per_file):
                    skipped[rel] = size
                    continue
                with open(path, errors="replace") as f:
                    files[rel] = f.read()
        if skipped:
            logger.warning(
                f"chipyard hammer collect skipped {len(skipped)} file(s) over "
                f"{max_bytes_per_file} bytes: {sorted(skipped)[:5]}"
            )
        return ChipyardHammerCollectResult(
            base_dir=base_dir,
            files=files,
            skipped=skipped,
            listing=_list_files(base_dir),
        )
