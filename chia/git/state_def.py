from dataclasses import dataclass, field


@dataclass
class GitResult:
    """Result of one raw ``git`` invocation.

    Attributes:
        args: The git subcommand and arguments, without the ``git`` itself.
        success: True iff git exited 0. A failed command never raises;
            callers branch on this.
        returncode: git's exit code; ``-1`` on timeout or a missing binary.
        stdout: Captured stdout, already stripped of the trailing newline.
        stderr: Captured stderr.
    """
    args: list[str]
    success: bool
    returncode: int
    stdout: str
    stderr: str


@dataclass
class Worktree:
    """A checkout of one revision, isolated from every other worker.

    A worktree shares the repository's object database but has its own index
    and working directory, which is what lets N agents edit N variants of the
    same repo concurrently without an index lock between them.

    Attributes:
        path: Absolute path of the working directory.
        base: The revision it was created from, as given.
        base_sha: The resolved commit sha of ``base``.
        created: True if this call created the worktree, False if an existing
            one at ``path`` was reused (and reset to ``base``). Either way the
            worktree is clean and at ``base_sha`` — a failure to get there
            raises rather than reporting it here.
    """
    path: str
    base: str
    base_sha: str
    created: bool = True


@dataclass
class Commit:
    """One variant in the population — a commit and its content identity.

    ``tree`` is the interesting field: it is the hash of the *content*, so two
    variants that differ only in commit metadata (author, date, message, which
    parent they were derived from) share a tree. That makes it the correct
    ``_chia_tag`` for anything whose result depends only on the source, and it
    is what lets the loop skip re-evaluating a variant an agent rediscovered.

    Attributes:
        sha: The commit object's sha.
        tree: The tree object's sha — the content identity.
        parents: Parent commit shas, in order.
        message: The commit message.
        ref: The ref this commit was published under, or ``""``.
    """
    sha: str
    tree: str
    parents: list[str] = field(default_factory=list)
    message: str = ""
    ref: str = ""
