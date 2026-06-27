"""chia.vlsi.hammer — Hammer-vlsi nodes.

:meth:`HammerNode.run` wraps one ``hammer-vlsi`` CLI call. Hammer's CLI is
uniform across actions (syn, par, drc, lvs, sim, power, and the *-to-* bridge
actions): configs in via repeated ``-p``, an ``--obj_dir`` for build output,
an ``-o`` output config that feeds the next action, and the action name. One
node therefore covers the whole flow, with the action as a parameter.

obj_dir is PATH-BASED: it lives on the worker that ran the action, so chained
actions (syn -> syn-to-par -> par) and report fetches
(:meth:`HammerNode.collect`) must land on the SAME worker. :class:`HammerNode`
enforces that via a placement group (see :class:`chia.base.colocated
.ColocatedNode` for the given / reserved / no-PG construction modes).
``HammerNode.<fn>.chia_remote(...)`` (the class attribute) is the raw,
unpinned form for callers that handle placement themselves.

This module knows nothing about technologies, tools, or sites — all of that
arrives via the project's configs.
"""

import glob as _glob
import json
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
class HammerResult:
    success: bool
    returncode: int
    action: str
    obj_dir: str          # on the worker that ran the action
    stdout: str
    stderr: str
    # Parsed contents of the action's ``-o`` output config. Feed it to the
    # next action (syn -> syn-to-par -> par) via ``config_contents``.
    output: dict = field(default_factory=dict)
    # Manifest of every file under obj_dir after the run: relative path ->
    # size in bytes. Contents stay on the worker; fetch them selectively with
    # HammerNode.collect pinned to the same bundle.
    listing: dict[str, int] = field(default_factory=dict)


@dataclass
class HammerCollectResult:
    obj_dir: str
    files: dict[str, str]      # relpath -> text contents (errors="replace")
    skipped: dict[str, int]    # matched but over max_bytes_per_file; size shown
    listing: dict[str, int]    # fresh manifest of obj_dir at collect time


@dataclass
class HammerMatchResult:
    obj_dir: str
    matches: list[tuple[str, int]]   # (relpath, size) within the cap, first-seen order
    skipped: dict[str, int]          # matched but over max_bytes_per_file; size shown


@dataclass
class HammerCollectFsResult:
    obj_dir: str               # source dir, on the worker
    dest_dir: str              # where files were written, on the CALLER's disk
    copied: dict[str, int]     # relpath -> bytes written; file is at dest_dir/<relpath>
    skipped: dict[str, int]    # matched but over max_bytes_per_file; size shown


# ---------------------------------------------------------------------------
# Worker-side helpers (module-level so they resolve by import on the worker)
# ---------------------------------------------------------------------------

def _list_files(obj_dir: str) -> dict[str, int]:
    """Manifest of every file under obj_dir: relative path -> size in bytes."""
    listing: dict[str, int] = {}
    for root, _dirs, names in os.walk(obj_dir):
        for name in names:
            path = os.path.join(root, name)
            try:
                listing[os.path.relpath(path, obj_dir)] = os.path.getsize(path)
            except OSError:
                pass  # dangling symlink etc.
    return listing


def _match_files(
    base_dir: str,
    patterns: list[str],
    max_bytes_per_file: int | None,
) -> tuple[list[tuple[str, str, int]], dict[str, int]]:
    """Resolve *patterns* (globs relative to *base_dir*, ``**`` recursive) to
    matching files.

    Returns ``(matches, skipped)`` where ``matches`` is a list of
    ``(relpath, abspath, size)`` for files within the size cap — deduped
    across overlapping patterns, in first-seen order — and ``skipped`` maps
    relpath -> size for files matched but over ``max_bytes_per_file`` (``None``
    or 0 disables the cap).  Shared by :meth:`HammerNode.collect` and
    :meth:`HammerNode.collect_fs` so their glob/dedup/cap semantics are
    identical.
    """
    matches: list[tuple[str, str, int]] = []
    skipped: dict[str, int] = {}
    seen: set[str] = set()
    for pattern in patterns:
        for path in _glob.glob(os.path.join(base_dir, pattern), recursive=True):
            if not os.path.isfile(path):
                continue
            rel = os.path.relpath(path, base_dir)
            if rel in seen:
                continue
            try:
                size = os.path.getsize(path)
            except OSError:
                continue
            seen.add(rel)
            if max_bytes_per_file and (size > max_bytes_per_file):
                skipped[rel] = size
                continue
            matches.append((rel, path, size))
    return matches, skipped


# ---------------------------------------------------------------------------
# HammerNode
# ---------------------------------------------------------------------------

class HammerNode(ColocatedNode):
    """Hammer run / collect primitives sharing one placement.

    The members are ``@staticmethod @ChiaFunction(resources={"hammer": 1})``;
    ``__init__`` re-binds each into a per-instance pinned form so
    ``node.<fn>.chia_remote(...)`` lands on this node's bundle::

        with HammerNode() as node:    # reserves a {"CPU": 1, "hammer": 1} PG
            syn = get(node.run.chia_remote(
                "syn", configs=[...], obj_dir="/scratch/build/run1"))
            rpts = get(node.collect.chia_remote(
                syn.obj_dir, ["syn-rundir/reports/**"]))
            par = get(node.run.chia_remote("syn-to-par", ...))  # same worker

    ``collect`` requires the default bundle's hammer slot, so a collect dispatched
    while a run executes on this bundle waits for it in the default case — it cannot read
    half-written reports.
    """

    # Pinned @ChiaFunction members. collect_fs is intentionally absent: it is a
    # caller-side orchestrator (a plain method) that drives list_matches /
    # read_chunk, not a task that runs on the worker.
    _MEMBER_FNS = ("run", "collect", "list_matches", "read_chunk")
    _DEFAULT_BUNDLE = {"CPU": 1, "hammer": 1}

    @staticmethod
    @ChiaFunction(resources={"hammer": 1})
    def run(
        action: str,
        configs: list[str] | None = None,
        config_contents: dict[str, str] | None = None,
        obj_dir: str = "build",
        extra_args: list[str] | None = None,
        hammer_bin: str = "hammer-vlsi",
        timeout_seconds: int = 86400,
    ) -> HammerResult:
        """Run one ``hammer-vlsi`` action on a worker.

        Args:
            action: Any action hammer-vlsi accepts: "syn", "par", "syn-to-par", ...
            configs: Paths to config YAML/JSON files that exist *on the worker*
                (baked into the image or mounted), passed as ``-p`` in order
                (later files override earlier ones).
            config_contents: filename -> YAML/JSON text, written into
                ``obj_dir/configs/`` on the worker and appended as ``-p`` after
                ``configs``. This is how a flow ships configs (or a previous
                action's ``output``) by value to a remote worker.
            obj_dir: Hammer build directory on the worker.
            extra_args: Extra CLI args, inserted before the action.
            hammer_bin: The hammer executable, or a custom CLIDriver script.
            timeout_seconds: Wall-clock limit for the subprocess.
        """
        obj_dir = os.path.abspath(obj_dir)
        os.makedirs(obj_dir, exist_ok=True)

        config_args = []
        for path in configs or []:
            config_args += ["-p", path]
        if config_contents:
            staged = os.path.join(obj_dir, "configs")
            os.makedirs(staged, exist_ok=True)
            for filename, text in config_contents.items():
                path = os.path.join(staged, filename)
                with open(path, "w") as f:
                    f.write(text)
                config_args += ["-p", path]

        output_json = os.path.join(obj_dir, f"{action}-output.json")
        cmd = [hammer_bin, *config_args, "--obj_dir", obj_dir,
               "-o", output_json, *(extra_args or []), action]
        logger.info(f"Running: {' '.join(cmd)}")

        # start_new_session puts the whole tool tree in one process group;
        # chia's pid_registry tracks the pgid so chia_cancel() can kill it.
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                text=True, start_new_session=True)
        try:
            stdout, stderr = proc.communicate(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            stdout, stderr = proc.communicate()
            stderr += f"\nhammer {action} timed out after {timeout_seconds}s"
            logger.error(f"hammer {action} timed out after {timeout_seconds}s")

        output = {}
        if proc.returncode == 0 and os.path.isfile(output_json):
            with open(output_json) as f:
                output = json.load(f)

        if proc.returncode != 0:
            logger.error(f"hammer {action} failed (rc={proc.returncode}); "
                         f"stderr tail: {stderr[-500:] if stderr else '(empty)'}")

        return HammerResult(
            success=proc.returncode == 0,
            returncode=proc.returncode,
            action=action,
            obj_dir=obj_dir,
            stdout=stdout,
            stderr=stderr,
            output=output,
            listing=_list_files(obj_dir),
        )

    @staticmethod
    @ChiaFunction(resources={"hammer": 1})
    def collect(
        obj_dir: str,
        patterns: list[str],
        max_bytes_per_file: int | None = None,
    ) -> HammerCollectResult:
        """Fetch text files from a previous action's obj_dir on this worker.

        Dispatch via the pinned instance member (``node.collect.chia_remote``)
        so it lands on the worker that owns obj_dir — an unpinned call may not.

        Args:
            obj_dir: The build directory a previous action ran in.
            patterns: Globs relative to obj_dir (``**`` is recursive), e.g.
                ``["syn-rundir/reports/**", "syn-rundir/*.log"]``. Files matched
                by multiple patterns appear once.
            max_bytes_per_file: When set, files over this size are recorded in
                ``skipped`` instead of shipped through the object store —
                protects against a glob accidentally matching a netlist.
                ``None`` (and 0, the falsy edge) means no cap: everything
                matched is shipped.
        """
        obj_dir = os.path.abspath(obj_dir)
        matches, skipped = _match_files(obj_dir, patterns, max_bytes_per_file)
        files: dict[str, str] = {}
        for rel, path, _size in matches:
            with open(path, errors="replace") as f:
                files[rel] = f.read()
        if skipped:
            logger.warning(
                f"hammer collect skipped {len(skipped)} file(s) over "
                f"{max_bytes_per_file} bytes: {sorted(skipped)[:5]}"
            )
        return HammerCollectResult(
            obj_dir=obj_dir,
            files=files,
            skipped=skipped,
            listing=_list_files(obj_dir),
        )

    @staticmethod
    @ChiaFunction(resources={"hammer": 1})
    def list_matches(
        obj_dir: str,
        patterns: list[str],
        max_bytes_per_file: int | None = None,
    ) -> HammerMatchResult:
        """Resolve *patterns* against obj_dir on the worker and return the
        manifest of matching files (relpath + size) without reading contents.

        The planning half of :meth:`collect_fs`: it tells the caller what to
        stream and how big each file is.  Same glob/dedup/cap rules as
        :meth:`collect`.
        """
        obj_dir = os.path.abspath(obj_dir)
        matches, skipped = _match_files(obj_dir, patterns, max_bytes_per_file)
        return HammerMatchResult(
            obj_dir=obj_dir,
            matches=[(rel, size) for rel, _path, size in matches],
            skipped=skipped,
        )

    @staticmethod
    @ChiaFunction(resources={"hammer": 1})
    def read_chunk(obj_dir: str, rel: str, offset: int, length: int) -> bytes:
        """Read ``length`` bytes at ``offset`` from ``obj_dir/rel`` on the
        worker.  The transfer primitive behind :meth:`collect_fs`; returns
        ``b""`` at or past EOF.  ``rel`` is confined to obj_dir."""
        obj_dir = os.path.abspath(obj_dir)
        path = os.path.abspath(os.path.join(obj_dir, rel))
        if path != obj_dir and not path.startswith(obj_dir + os.sep):
            raise ValueError(f"rel {rel!r} escapes obj_dir")
        with open(path, "rb") as f:
            f.seek(offset)
            return f.read(length)

    def collect_fs(
        self,
        obj_dir: str,
        patterns: list[str],
        dest_dir: str,
        max_bytes_per_file: int | None = None,
        chunk_bytes: int = 16 * 1024 * 1024,
    ) -> HammerCollectFsResult:
        """Stream matching files from a previous action's obj_dir onto the
        filesystem of THIS (the calling) process, a chunk at a time.

        Unlike :meth:`collect`, which returns every file's contents in one
        object-store payload, this writes each file to ``dest_dir`` on the
        caller's local disk incrementally — peak memory is ~``chunk_bytes``,
        not the size of the whole collection.  Use it to pull large report
        trees / gate-level netlists back from the worker to a machine that
        does NOT share a filesystem with it.

        This is a caller-side orchestrator, not a ``@ChiaFunction``: it runs
        wherever you call it and writes to that machine's disk, pulling bytes
        from the obj_dir worker via the node's pinned :meth:`list_matches` /
        :meth:`read_chunk` members.  It therefore needs a placement group so
        both members hit the one worker that owns obj_dir — construct the node
        with ``require_colocated=True`` or pass ``placement_group=...``.

        Files keep their path relative to obj_dir: a match at
        ``obj_dir/syn-rundir/reports/x.rpt`` lands at
        ``dest_dir/syn-rundir/reports/x.rpt``.

        Args:
            obj_dir: The build directory a previous action ran in (on the worker).
            patterns: Globs relative to obj_dir (``**`` recursive); same
                matching/dedup/cap rules as :meth:`collect`.
            dest_dir: Destination directory on the calling machine; created as
                needed.
            max_bytes_per_file: When set, files over this size are recorded in
                ``skipped`` and not streamed. ``None`` (and 0) means no cap.
            chunk_bytes: Bytes per worker read — the memory bound per file.
        """
        from chia.base.ChiaFunction import get

        if not self._sched_opts:
            raise RuntimeError(
                "collect_fs needs a placement group so list_matches and "
                "read_chunk hit the same worker; construct HammerNode with "
                "require_colocated=True or pass placement_group=..."
            )
        dest_dir = os.path.abspath(dest_dir)
        manifest = get(self.list_matches.chia_remote(
            obj_dir, patterns, max_bytes_per_file))
        copied: dict[str, int] = {}
        for rel, _size in manifest.matches:
            dst = os.path.join(dest_dir, rel)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            written = 0
            with open(dst, "wb") as out:
                while True:
                    data = get(self.read_chunk.chia_remote(
                        manifest.obj_dir, rel, written, chunk_bytes))
                    if not data:
                        break
                    out.write(data)
                    written += len(data)
                    del data  # free the chunk before fetching the next
            copied[rel] = written
        if manifest.skipped:
            logger.warning(
                f"hammer collect_fs skipped {len(manifest.skipped)} file(s) over "
                f"{max_bytes_per_file} bytes: {sorted(manifest.skipped)[:5]}"
            )
        logger.info(f"hammer collect_fs streamed {len(copied)} file(s) to {dest_dir}")
        return HammerCollectFsResult(
            obj_dir=manifest.obj_dir,
            dest_dir=dest_dir,
            copied=copied,
            skipped=manifest.skipped,
        )
