from dataclasses import dataclass, field
from enum import Enum


class BazelCommand(str, Enum):
    """Which Bazel subcommand a :class:`BazelResult` came from.

    Attributes:
        BUILD: ``bazel build`` — compiles the targets, optionally collecting
            their declared output files.
        TEST: ``bazel test`` — builds and runs test targets; ``test.log`` /
            ``test.xml`` are collected from ``bazel-testlogs``.
        RUN: ``bazel run`` — builds one target and executes it; the binary's
            own stdout/stderr are folded into the result's streams.
        QUERY, CQUERY, INFO, SHUTDOWN: The auxiliary subcommands the node runs
            on the caller's behalf (target enumeration, output-file listing,
            ``bazel-testlogs`` lookup, server teardown).
    """
    BUILD = "build"
    TEST = "test"
    RUN = "run"
    QUERY = "query"
    CQUERY = "cquery"
    INFO = "info"
    SHUTDOWN = "shutdown"


@dataclass
class BazelResult:
    """Result of one :class:`BazelNode` invocation.

    Outputs travel by value (bytes inside this object) so a downstream
    ChiaFunction on another node can consume them without a shared
    filesystem — the same convention as the Chipyard build artifacts.

    Attributes:
        command: The :class:`BazelCommand` that produced this result.
        targets: Target patterns passed to Bazel, e.g. ``["//src:app"]``.
        success: True iff Bazel exited 0. A failed build/test never raises;
            callers branch on this.
        returncode: Bazel's exit code; ``-1`` on timeout. Bazel's own codes are
            meaningful — 1 build failure, 3 tests failed, 4 no tests found,
            37 the server was killed (usually OOM).
        stdout: Captured stdout of the Bazel command.
        stderr: Captured stderr — where Bazel writes progress and errors.
        duration_seconds: Wall-clock time of the invocation.
        output_paths: Declared output files of the targets, as workspace-
            relative paths (from ``cquery --output=files``). Populated
            whenever outputs were requested, even if the bytes were not read.
        outputs: ``path -> bytes`` for the collected output files. Empty
            unless ``collect_outputs=True``.
        test_logs: ``path -> text`` of ``test.log`` / ``test.xml`` files under
            ``bazel-testlogs``. Only populated by :meth:`BazelNode.test`.
        source_digest: The transitive source digest of the targets when the
            caller asked for one (see :meth:`BazelNode.source_digest`);
            ``""`` otherwise. Useful as a ``_chia_tag`` cache key.
    """
    command: BazelCommand
    targets: list[str]
    success: bool
    returncode: int
    stdout: str
    stderr: str
    duration_seconds: float = 0.0
    output_paths: list[str] = field(default_factory=list)
    outputs: dict[str, bytes] = field(default_factory=dict)
    test_logs: dict[str, str] = field(default_factory=dict)
    source_digest: str = ""
