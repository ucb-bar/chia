"""Use a git repository as the population store of a CHIA loop.

Cut a worktree at a parent, let an agent or an algorithm edit it, capture the
result as a commit, attach a score as a note, walk the lineage.

Two properties earn git the job. ``Commit.tree`` is the hash of the source
state, so the same edit from two agents yields the same tree — an exact
``_chia_tag`` and free deduplication. And commits are built with
``commit-tree`` under a fixed identity, so a re-queued node reproduces the same
sha instead of forking the population, which is why these nodes keep Ray's
retries where the database nodes must set ``max_retries=0``.

Worktrees share the repository's object database, so the repo and its
worktrees live on one filesystem. Across several machines that means one
repository per node: :meth:`GitNode.ensure_mirror` clones a bare mirror once
per worker and every worktree is cut from it, which costs a hardlinked
checkout rather than a clone per task. See ``examples/git_bazel_loop``.
"""

import logging
import os
import re
import shutil
import subprocess

from chia.base.ChiaFunction import ChiaFunction
from chia.git.state_def import Commit, GitResult, Worktree


class GitError(RuntimeError):
    """A git plumbing command on the write path failed.

    Writes raise because an invented empty sha is indistinguishable from a real
    one, and as a ``_chia_tag`` it collapses distinct variants onto one cache
    entry. Reads return empty instead, where "absent" is a real answer.
    """

_FIXED_IDENT = {
    "GIT_AUTHOR_NAME": "chia",
    "GIT_AUTHOR_EMAIL": "chia@localhost",
    "GIT_COMMITTER_NAME": "chia",
    "GIT_COMMITTER_EMAIL": "chia@localhost",
    "GIT_AUTHOR_DATE": "1970-01-01T00:00:00+0000",
    "GIT_COMMITTER_DATE": "1970-01-01T00:00:00+0000",
}

_SEP = "\x1f"
_LOG_FORMAT = _SEP.join(["%H", "%T", "%P", "%s"])


class GitNode:
    """Runs ``git`` against one repository, in-process or dispatched.

    Called directly it is a driver-side store, the arrangement
    ``examples/git_bazel_loop`` and ``examples/timing_opt/db.py`` use: git
    plumbing is milliseconds, so a dispatch per call would cost more than the
    call. Called as ``node.commit.chia_remote(node, ...)`` it reaches a
    checkout held on a worker, which is what ``resources={"git": 1}`` is for --
    advertise that resource only if you dispatch.

    ``repo_dir`` may be an ordinary checkout or a bare mirror; :meth:`snapshot`
    always uses the worktree's own index, the rest work against either.

    Writes raise :class:`GitError`; reads return empty.
    """

    logging_name = "GitNode"

    def __init__(
        self,
        repo_dir: str,
        timeout_seconds: int = 300,
        env: dict[str, str] | None = None,
        logging_level: int = logging.DEBUG,
    ):
        """
        Args:
            repo_dir: The repository — an ordinary checkout or a bare mirror.
                Worktrees are cut from it and share its object database.
            timeout_seconds: Wall-clock limit per invocation.
            env: Extra environment variables, merged over ``os.environ``.
            logging_level: Logging level for this node's logger.
        """
        self.repo_dir = repo_dir
        self.timeout_seconds = timeout_seconds
        self.env = dict(env or {})
        self.logger = logging.getLogger(self.logging_name)
        self.logger.setLevel(logging_level)


    @ChiaFunction(resources={"git": 1})
    def init(self, branch: str = "main", bare: bool = False) -> bool:
        """Create an empty repository at ``repo_dir`` if there is not one there.

        Idempotent; returns True if this call created it. A non-bare repository
        is its own worktree, so ``commit(repo_dir, ..., parents=[])`` lands a
        first commit with no worktree to cut.

        Raises:
            GitError: If ``git init`` fails.
        """
        if os.path.exists(os.path.join(self.repo_dir, ".git")) or \
                os.path.isdir(os.path.join(self.repo_dir, "objects")):
            return False
        os.makedirs(self.repo_dir, exist_ok=True)
        args = ["init", "-q", "-b", branch]
        if bare:
            args.append("--bare")
        self._checked(args, f"init {self.repo_dir}")
        return True

    @ChiaFunction(resources={"git": 1})
    def ensure_mirror(self, repo_url: str) -> bool:
        """Clone *repo_url* as a bare mirror at ``repo_dir`` if it is not there.

        Idempotent, so dispatch it once per worker: each node then has its own
        object database and a worktree costs a hardlinked checkout rather than
        a clone. Returns True if this call created the mirror.

        Raises:
            GitError: If the clone fails.
        """
        if os.path.isdir(os.path.join(self.repo_dir, "objects")):
            return False
        os.makedirs(os.path.dirname(self.repo_dir) or ".", exist_ok=True)
        self._checked(["clone", "--mirror", repo_url, self.repo_dir],
                      f"clone mirror {self.repo_dir}", cwd=os.path.dirname(self.repo_dir) or ".")
        return True

    @ChiaFunction(resources={"git": 1})
    def fetch(self, remote: str = "origin", prune: bool = True) -> GitResult:
        """Update the mirror from *remote*.

        On a schedule or when upstream is known to have moved, not per task:
        every agent on a node contends on one ref lock, and a loop breeding
        from its own commits has nothing upstream to fetch.

        Raises:
            GitError: If the fetch fails.
        """
        args = ["fetch", remote]
        if prune:
            args.append("--prune")
        return self._checked(args, f"fetch {remote}")

    @ChiaFunction(resources={"git": 1})
    def worktree(self, path: str, base: str = "HEAD") -> Worktree:
        """Create a detached worktree at *path*, checked out at *base*.

        The unit of isolation: one worktree per concurrent agent lets them edit
        the same repository without contending on an index lock.

        Args:
            path: Where to create the working directory. An existing worktree
                here is reused (and reset to *base*) rather than re-created.
            base: Revision to check out — a sha, ref, or anything
                ``rev-parse`` accepts.

        Returns:
            A :class:`Worktree` whose ``path`` is ready to be edited.

        Raises:
            GitError: If *base* does not resolve, or the worktree cannot be
                created or reset. A half-reset worktree would be snapshotted as
                a real variant carrying the previous candidate's edits, so the
                checkout is ``--force`` and neither step is best-effort.
        """
        base_sha = self.resolve(base)
        if not base_sha:
            raise GitError(f"worktree: {base!r} does not resolve to a commit")

        dot_git = os.path.join(path, ".git")
        if os.path.exists(dot_git):
            self._checked(["-C", path, "checkout", "--detach", "--force", base_sha],
                          f"reset worktree {path}")
            self._checked(["-C", path, "clean", "-fdx"], f"clean worktree {path}")
            return Worktree(path=path, base=base, base_sha=base_sha, created=False)

        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._checked(["worktree", "add", "--detach", "--force", path, base_sha],
                      f"create worktree {path}")
        return Worktree(path=path, base=base, base_sha=base_sha, created=True)

    @ChiaFunction(resources={"git": 1})
    def remove_worktree(self, path: str) -> GitResult:
        """Remove a worktree and prune its administrative entry."""
        result = self._git(["worktree", "remove", "--force", path])
        if not result.success:
            shutil.rmtree(path, ignore_errors=True)
            self._git(["worktree", "prune"])
        return result


    @ChiaFunction(resources={"git": 1})
    def snapshot(self, worktree_path: str) -> str:
        """Stage everything in *worktree_path* and return the resulting tree sha.

        The cheap half of :meth:`commit`: the content identity of the working
        directory without writing a commit, so a loop can ask whether it has
        already evaluated this exact source state.

        Returns:
            The tree sha.

        Raises:
            GitError: If staging or ``write-tree`` fails. This value is a cache
                key, and an empty tag would match every other empty tag.
        """
        self._checked(["-C", worktree_path, "add", "-A"], f"stage {worktree_path}")
        return self._checked(["-C", worktree_path, "write-tree"],
                             f"write-tree {worktree_path}").stdout

    @ChiaFunction(resources={"git": 1})
    def commit(
        self,
        worktree_path: str,
        message: str,
        parents: list[str] | None = None,
        ref: str | None = None,
        tree: str | None = None,
    ) -> Commit:
        """Capture *worktree_path* as a commit, deterministically.

        ``write-tree`` + ``commit-tree`` under a fixed identity, so the sha is
        a pure function of (tree, parents, message): running it twice, or
        having CHIA re-queue it, yields the same sha rather than a duplicate.

        Args:
            worktree_path: The worktree to capture.
            message: Commit message.
            parents: Parent commit shas. An empty list makes a root commit.
            ref: Optional ref to point at the new commit, e.g.
                ``"refs/evo/gen3/a1"``. Refs are how the population is
                enumerated; a commit with no ref is unreachable.
            tree: Commit this tree instead of re-reading the worktree. Pass
                the sha an earlier :meth:`snapshot` returned, so the commit
                records the state that was evaluated; without it, anything the
                evaluation left behind is committed too and the variant's
                identity stops matching the key it was scored under.

        Returns:
            A :class:`Commit`.

        Raises:
            GitError: If the tree, the commit object, or the ref update fails.
                A commit whose ref did not land is unreachable.
        """
        tree = tree or self.snapshot(worktree_path)

        args = ["commit-tree", tree]
        for parent in parents or []:
            args += ["-p", parent]
        args += ["-m", message]
        sha = self._checked(args, f"commit-tree {tree[:10]}", env=_FIXED_IDENT).stdout

        if ref:
            self._checked(["update-ref", ref, sha], f"update-ref {ref}")
        return Commit(sha=sha, tree=tree, parents=parents or [],
                      message=message, ref=ref or "")


    @ChiaFunction(resources={"git": 1})
    def write_note(self, sha: str, content: str, notes_ref: str = "refs/notes/chia") -> GitResult:
        """Attach *content* to commit *sha* as a git note, replacing any prior one.

        Small, durable and versioned; record a CAS digest or object-store key
        here rather than a large artifact. A note can annotate *any* object, so
        keying evidence by tree sha covers every commit with that content, and
        a candidate that was never committed.

        Two hazards. Notes do not travel with a plain clone, fetch or push,
        since ``refs/notes/*`` is in none of the default refspecs; across
        machines either mirror the repository (:meth:`ensure_mirror`) or
        configure ``+refs/notes/*:refs/notes/*``. And ``git notes`` is a
        read-modify-write of one ref's tree, so concurrent writers clobber each
        other while all of them exit 0: 24 concurrent writes to one ref stored
        12-14. Worktrees share the ref namespace, so workers contend like
        threads. Write from one place, or one ``notes_ref`` per writer.

        Raises:
            GitError: If the note cannot be written. Note that a *lost* write
                is not a failed one; see above.
        """
        return self._checked(
            ["notes", f"--ref={notes_ref}", "add", "-f", "-m", content, sha],
            f"write note on {sha[:10]}", env=_FIXED_IDENT,
        )

    @ChiaFunction(resources={"git": 1})
    def read_note(self, sha: str, notes_ref: str = "refs/notes/chia") -> str:
        """Return the note attached to *sha*, or ``""`` if there is none."""
        result = self._git(["notes", f"--ref={notes_ref}", "show", sha])
        return result.stdout if result.success else ""


    @ChiaFunction(resources={"git": 1})
    def resolve(self, rev: str) -> str:
        """Return the sha *rev* names, or ``""`` if it does not resolve."""
        result = self._git(["rev-parse", "--verify", f"{rev}^{{commit}}"])
        return result.stdout if result.success else ""

    @ChiaFunction(resources={"git": 1})
    def tree_of(self, rev: str) -> str:
        """Return the tree sha of *rev* — its content identity."""
        result = self._git(["rev-parse", "--verify", f"{rev}^{{tree}}"])
        return result.stdout if result.success else ""

    @ChiaFunction(resources={"git": 1})
    def list_refs(self, pattern: str = "refs/evo/**") -> list[str]:
        """Return the refs matching *pattern*, e.g. ``refs/evo/gen3/*``."""
        result = self._git(["for-each-ref", "--format=%(refname)", pattern])
        return [line for line in result.stdout.splitlines() if line] if result.success else []

    @ChiaFunction(resources={"git": 1})
    def lineage(self, rev: str, limit: int = 100) -> list[Commit]:
        """Return *rev* and its ancestors, newest first — the variant's
        phylogeny, straight out of the commit DAG."""
        result = self._git(
            ["log", f"--max-count={limit}", f"--format={_LOG_FORMAT}", rev]
        )
        if not result.success:
            return []
        commits = []
        for line in result.stdout.splitlines():
            sha, tree, parents, message = line.split(_SEP)
            commits.append(Commit(sha=sha, tree=tree,
                                  parents=parents.split() if parents else [],
                                  message=message))
        return commits


    def _checked(self, args: list[str], what: str,
                 env: dict[str, str] | None = None,
                 cwd: str | None = None) -> GitResult:
        """Run a git command on the write path; raise :class:`GitError` on failure."""
        result = self._git(args, env=env, cwd=cwd)
        if not result.success:
            raise GitError(f"{what}: git {' '.join(args)} exited "
                           f"{result.returncode}: {result.stderr.strip()[-400:]}")
        return result

    def _git(self, args: list[str], env: dict[str, str] | None = None,
             cwd: str | None = None) -> GitResult:
        """Run one git command in the repo; never raises."""
        argv = ["git", *args]
        where = cwd or self.repo_dir
        self.logger.debug(f"Running: {' '.join(argv)} (cwd={where})")
        merged = {**os.environ, **self.env, **(env or {})}
        try:
            proc = subprocess.run(
                argv, cwd=where, capture_output=True, text=True,
                timeout=self.timeout_seconds, env=merged,
            )
            return GitResult(args=args, success=proc.returncode == 0,
                             returncode=proc.returncode,
                             stdout=proc.stdout.strip(), stderr=proc.stderr)
        except subprocess.TimeoutExpired:
            return GitResult(args=args, success=False, returncode=-1, stdout="",
                             stderr=f"[GitNode] timeout after {self.timeout_seconds}s")
        except (FileNotFoundError, PermissionError) as e:
            return GitResult(args=args, success=False, returncode=-1, stdout="",
                             stderr=f"[GitNode] cannot execute git: {e}")


    @staticmethod
    def safe_ref_component(text: str) -> str:
        """Sanitize *text* into something usable inside a ref name."""
        return re.sub(r"[^A-Za-z0-9._-]", "_", text).strip("._-") or "x"
