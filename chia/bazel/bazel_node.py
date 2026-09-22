"""Drive a Bazel workspace from a CHIA workflow.

Wraps the ``bazel`` CLI as ChiaFunctions, so a build, a test run, or a
``bazel run`` becomes a node scheduled onto a ``bazel`` worker. Every method
returns a :class:`~chia.bazel.state_def.BazelResult` and never raises on a
failed build — callers branch on ``success``, so a failing target is data an
agent can be handed rather than an exception that tears the loop down.
Declared outputs are collected with ``cquery --output=files``.

The node is a Bazel *client*, so an external build cluster is just flags:
point ``common_flags`` at ``--remote_executor=``/``--remote_cache=`` and the
actions run on the farm while CHIA orchestrates the graph.

:meth:`BazelNode.source_digest` hashes a target's transitive sources into a
``_chia_tag`` for CHIA's cache; prefer a git tree sha where one exists (see
``examples/git_bazel_loop``), which is exact rather than approximate.
"""

import hashlib
import logging
import os
import subprocess
import time
from pathlib import Path

from chia.base.ChiaFunction import ChiaFunction
from chia.bazel.state_def import BazelCommand, BazelResult


class BazelNode:
    """Runs ``bazel`` over a workspace on a ``bazel`` worker.

    One instance describes *how* to invoke Bazel; the per-call arguments say
    *what* to build. The instance is pickled to the worker, so every path it
    holds must exist there.
    """

    logging_name = "BazelNode"

    def __init__(
        self,
        workspace_dir: str,
        bazel_bin: str = "bazel",
        startup_options: list[str] | None = None,
        output_base: str | None = None,
        common_flags: list[str] | None = None,
        config: str | None = None,
        env: dict[str, str] | None = None,
        timeout_seconds: int = 1800,
        logging_level: int = logging.DEBUG,
    ):
        """
        Args:
            workspace_dir: Bazel workspace root on the worker (the directory
                holding ``MODULE.bazel``/``WORKSPACE``). Per-call overrides are
                accepted by every method.
            bazel_bin: Binary to invoke — ``"bazel"``, ``"bazelisk"``, or an
                absolute path.
            startup_options: Bazel *startup* options, placed before the
                subcommand (e.g. ``["--host_jvm_args=-Xmx4g"]``).
            output_base: ``--output_base`` for the Bazel server. Bazel takes an
                exclusive lock per output base, so concurrent CHIA tasks
                sharing one workspace **serialize** unless each gets its own
                base. Give co-scheduled tasks distinct bases (at the cost of a
                cold analysis cache and a JVM apiece).
            common_flags: Flags appended to every subcommand, e.g.
                ``["--keep_going", "--verbose_failures"]``.
            config: ``--config=<name>`` applied to every subcommand.
            env: Extra environment variables, merged over ``os.environ``.
            timeout_seconds: Wall-clock limit per invocation; on expiry the
                result carries ``returncode=-1`` (never raises).
            logging_level: Logging level for this node's logger.
        """
        self.workspace_dir = workspace_dir
        self.bazel_bin = bazel_bin
        self.startup_options = list(startup_options or [])
        if output_base:
            self.startup_options.append(f"--output_base={output_base}")
        self.common_flags = list(common_flags or [])
        self.config = config
        self.env = dict(env or {})
        self.timeout_seconds = timeout_seconds
        self.logger = logging.getLogger(self.logging_name)
        self.logger.setLevel(logging_level)


    @ChiaFunction(resources={"bazel": 1})
    def build(
        self,
        targets: list[str],
        flags: list[str] | None = None,
        collect_outputs: bool = False,
        workspace_dir: str | None = None,
        with_source_digest: bool = False,
    ) -> BazelResult:
        """Run ``bazel build`` over *targets*.

        Args:
            targets: Target patterns, e.g. ``["//src:app", "//lib/..."]``.
            flags: Extra flags for this call, after ``common_flags``.
            collect_outputs: Read the targets' declared output files back as
                bytes. ``output_paths`` is filled either way. Against a remote
                cache or executor this implies
                ``--remote_download_outputs=toplevel``, since Build without
                the Bytes would otherwise leave nothing local to read; pass
                your own ``--remote_download_*`` flag to override.
            workspace_dir: Override the instance's workspace for this call.
            with_source_digest: Also compute :meth:`source_digest` and record
                it on the result (one extra ``bazel query``).

        Returns:
            A :class:`BazelResult` with ``command=BUILD``.
        """
        cwd = workspace_dir or self.workspace_dir
        flags = self._with_download_flag(flags) if collect_outputs else flags
        result = self._invoke(BazelCommand.BUILD, targets, flags, cwd)
        if result.success:
            self._attach_outputs(result, targets, flags, cwd, collect_outputs)
        if with_source_digest:
            result.source_digest = self.source_digest(targets, workspace_dir=cwd)
        return result

    @ChiaFunction(resources={"bazel": 1})
    def test(
        self,
        targets: list[str],
        flags: list[str] | None = None,
        test_output: str = "errors",
        collect_test_logs: bool = True,
        workspace_dir: str | None = None,
    ) -> BazelResult:
        """Run ``bazel test`` over *targets*.

        A test failure is a normal result (Bazel exits 3), not an exception.

        Args:
            targets: Test target patterns.
            flags: Extra flags for this call.
            test_output: ``--test_output`` mode — ``"errors"`` (default),
                ``"summary"``, ``"all"``, or ``"streamed"``.
            collect_test_logs: Read ``test.log`` / ``test.xml`` out of
                ``bazel-testlogs`` into ``test_logs``. Bazel fetches test logs
                even under ``--remote_download_minimal``, so this needs no
                download flag of its own (verified against Buildbarn).
            workspace_dir: Override the instance's workspace for this call.

        Returns:
            A :class:`BazelResult` with ``command=TEST``.
        """
        cwd = workspace_dir or self.workspace_dir
        call_flags = [f"--test_output={test_output}", *(flags or [])]
        result = self._invoke(BazelCommand.TEST, targets, call_flags, cwd)
        if collect_test_logs:
            result.test_logs = self._collect_test_logs(targets, cwd)
        return result

    @ChiaFunction(resources={"bazel": 1})
    def run(
        self,
        target: str,
        args: list[str] | None = None,
        flags: list[str] | None = None,
        workspace_dir: str | None = None,
    ) -> BazelResult:
        """Build *target* and execute it via ``bazel run``.

        Args:
            target: A single runnable target, e.g. ``"//tools:codegen"``.
            args: Arguments for the binary, passed after ``--``.
            flags: Extra flags for the ``run`` command itself.
            workspace_dir: Override the instance's workspace for this call.

        Returns:
            A :class:`BazelResult` with ``command=RUN``; the binary's output is
            in ``stdout``/``stderr`` alongside Bazel's own.
        """
        cwd = workspace_dir or self.workspace_dir
        tail = ["--", *args] if args else []
        return self._invoke(BazelCommand.RUN, [target, *tail], flags, cwd)

    @ChiaFunction(resources={"bazel": 1})
    def query(
        self,
        expression: str,
        configured: bool = False,
        flags: list[str] | None = None,
        workspace_dir: str | None = None,
    ) -> list[str]:
        """Return the labels matching a Bazel query *expression*.

        Use it to fan a CHIA graph out over a target set discovered at runtime,
        e.g. ``query("tests(//...)")`` then one :meth:`test` node per label.

        Args:
            expression: A query expression, e.g. ``"deps(//src:app)"``.
            configured: Use ``cquery`` (post-analysis, config-aware) instead of
                ``query``.
            flags: Extra flags; pass the same ``--config``/flags as the build
                when ``configured`` is set, or you will query a different
                configuration than you built.
            workspace_dir: Override the instance's workspace for this call.

        Returns:
            Labels, one per line of Bazel's output; empty if the query failed.
        """
        cwd = workspace_dir or self.workspace_dir
        sub = "cquery" if configured else "query"
        result = self._invoke(sub, [expression], [*(flags or []), "--output=label"], cwd,
                              with_common=configured)
        if not result.success:
            self.logger.warning(
                f"{sub} {expression!r} failed (rc={result.returncode}); "
                f"stderr tail: {result.stderr[-500:]}"
            )
            return []
        return [line.split(" ")[0] for line in result.stdout.splitlines() if line.strip()]

    @ChiaFunction(resources={"bazel": 1})
    def shutdown(self, workspace_dir: str | None = None) -> BazelResult:
        """Stop the Bazel server for this output base.

        Worth doing at the end of a long loop: each server holds a JVM and the
        analysis cache resident, which competes with co-scheduled work on the
        same node.
        """
        cwd = workspace_dir or self.workspace_dir
        return self._invoke("shutdown", [], [], cwd, with_common=False)


    def source_digest(
        self,
        targets: list[str],
        extra: list[str] | None = None,
        workspace_dir: str | None = None,
    ) -> str:
        """Hash the transitive sources of *targets* into a cache key.

        Hashes each source and BUILD/``.bzl`` label in the dependency closure
        together with the bytes of the file it names, so editing an input
        changes the digest and editing an unrelated file does not.

        This is *not* Bazel's action key. External-repository files are folded
        in by label only (their pinned versions live in ``MODULE.bazel``, which
        is hashed by content), and the toolchain and flags are covered only to
        the extent you pass them in ``extra``.

        Args:
            targets: Target patterns to take the dependency closure of.
            extra: Additional strings to fold in — the compiler version, the
                ``--config`` name, an agent's iteration number.
            workspace_dir: Override the instance's workspace for this call.

        Returns:
            A 32-hex-character digest, or ``""`` if the query failed.
        """
        cwd = workspace_dir or self.workspace_dir
        union = " union ".join(targets)
        expression = (
            f'let t = deps({union}) in '
            f'kind("source file", $t) union buildfiles($t)'
        )
        labels = self.query(expression, workspace_dir=cwd)
        if not labels:
            return ""

        h = hashlib.sha256()
        for label in sorted(labels):
            h.update(label.encode("utf-8"))
            path = self._label_to_path(label, cwd)
            if path is not None and path.is_file():
                h.update(hashlib.sha256(path.read_bytes()).digest())
        for part in (*targets, self.config or "", *self.common_flags, *(extra or [])):
            h.update(str(part).encode("utf-8"))
        return h.hexdigest()[:32]

    def _with_download_flag(self, flags: list[str] | None) -> list[str]:
        """Ensure requested outputs are materialized locally.

        Under Build without the Bytes, Bazel leaves outputs in the CAS and
        there is nothing for ``_attach_outputs`` to read. A
        ``--remote_download`` flag the caller set themselves wins.
        """
        call_flags = list(flags or [])
        if any(f.startswith("--remote_download")
               for f in (*self.common_flags, *call_flags)):
            return call_flags
        return [*call_flags, "--remote_download_outputs=toplevel"]

    @staticmethod
    def _label_to_path(label: str, workspace_dir: str) -> Path | None:
        """Map a workspace-local label to a file path; None for external ones."""
        if not label.startswith("//"):
            return None
        package, _, name = label[2:].partition(":")
        return Path(workspace_dir) / package / name if package else Path(workspace_dir) / name


    def _invoke(
        self,
        command: "BazelCommand | str",
        arguments: list[str],
        flags: list[str] | None,
        cwd: str,
        with_common: bool = True,
    ) -> BazelResult:
        """Run one Bazel subcommand and wrap it in a BazelResult.

        ``with_common`` is False for the subcommands that take no build options
        — ``query``, ``info``, ``shutdown`` — which would otherwise reject an
        ordinary ``common_flags`` entry like ``--verbose_failures``.
        """
        sub = command.value if isinstance(command, BazelCommand) else command
        command = BazelCommand(sub)
        build_flags: list[str] = []
        if with_common:
            if self.config:
                build_flags.append(f"--config={self.config}")
            build_flags.extend(self.common_flags)
        argv = [
            self.bazel_bin, *self.startup_options, sub,
            *build_flags, *(flags or []), *arguments,
        ]
        self.logger.info(f"Running: {' '.join(argv)} (cwd={cwd})")

        start = time.time()
        stdout, stderr, returncode = self._exec(argv, cwd)
        duration = time.time() - start

        if returncode != 0:
            self.logger.warning(
                f"bazel {sub} failed (rc={returncode}); stderr tail: {stderr[-500:]}"
            )
        return BazelResult(
            command=command,
            targets=list(arguments),
            success=returncode == 0,
            returncode=returncode,
            stdout=stdout,
            stderr=stderr,
            duration_seconds=duration,
        )

    def _exec(self, argv: list[str], cwd: str) -> tuple[str, str, int]:
        """Run ``argv`` with the node's timeout and environment; rc=-1 on timeout."""
        try:
            proc = subprocess.run(
                argv, cwd=cwd, capture_output=True, text=True,
                timeout=self.timeout_seconds,
                env={**os.environ, **self.env} if self.env else None,
            )
            return proc.stdout, proc.stderr, proc.returncode
        except subprocess.TimeoutExpired as e:
            stdout = self._to_text(e.stdout)
            stderr = self._to_text(e.stderr) + \
                f"\n[BazelNode] timeout after {self.timeout_seconds}s"
            return stdout, stderr, -1
        except (FileNotFoundError, PermissionError) as e:
            return "", f"[BazelNode] cannot execute {argv[0]!r}: {e}", -1

    def _attach_outputs(
        self,
        result: BazelResult,
        targets: list[str],
        flags: list[str] | None,
        cwd: str,
        collect: bool,
    ) -> None:
        """Fill ``output_paths`` (always) and ``outputs`` (when *collect*)."""
        union = " union ".join(targets)
        listing = self._invoke("cquery", [union], [*(flags or []), "--output=files"], cwd)
        if not listing.success:
            self.logger.warning(
                f"cquery --output=files failed for {targets}; outputs not collected"
            )
            return
        result.output_paths = [p for p in listing.stdout.splitlines() if p.strip()]
        if not collect:
            return
        for rel in result.output_paths:
            path = Path(cwd) / rel
            if path.is_file():
                result.outputs[rel] = path.read_bytes()
            else:
                self.logger.debug(f"declared output missing on disk: {rel}")

    def _collect_test_logs(self, targets: list[str], cwd: str) -> dict[str, str]:
        """Read ``test.log``/``test.xml`` under ``bazel-testlogs`` for *targets*."""
        info = self._invoke("info", ["bazel-testlogs"], [], cwd, with_common=False)
        if not info.success:
            return {}
        testlogs = Path(info.stdout.strip())
        logs: dict[str, str] = {}
        for label in targets:
            if not label.startswith("//"):
                continue
            package, _, name = label[2:].partition(":")
            target_dir = testlogs / package / (name or Path(package).name)
            for log in sorted(target_dir.rglob("test.log")) + sorted(target_dir.rglob("test.xml")):
                logs[str(log.relative_to(testlogs))] = log.read_text(errors="replace")
        return logs

    @staticmethod
    def _to_text(value: "str | bytes | None") -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return value or ""
