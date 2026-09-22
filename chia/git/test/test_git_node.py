"""Unit tests for GitNode, against real temporary repositories.

git is a hard dependency of the node and cheap to run, so these use the real
thing rather than a stub — the behavior worth testing (content identity,
deterministic shas, worktree isolation) is precisely what a stub would fake.

Run: pytest chia/git/test/test_git_node.py
"""

import json
import subprocess

import pytest

from chia.git.git_node import GitError, GitNode


def git(repo, *args):
    return subprocess.run(["git", "-C", str(repo), *args],
                          capture_output=True, text=True, check=True).stdout.strip()


@pytest.fixture
def repo(tmp_path):
    """A repository with one commit, and a GitNode pointed at it."""
    path = tmp_path / "repo"
    path.mkdir()
    git(path, "init", "-q", "-b", "main")
    git(path, "config", "user.email", "t@example.com")
    git(path, "config", "user.name", "t")
    (path / "src").mkdir()
    (path / "src/app.txt").write_text("v1\n")
    git(path, "add", "-A")
    git(path, "commit", "-qm", "seed")
    return path, GitNode(str(path))


def test_worktree_checks_out_the_base_revision(tmp_path, repo):
    path, node = repo
    wt = node.worktree(str(tmp_path / "wt1"), base="HEAD")

    assert wt.base_sha == git(path, "rev-parse", "HEAD")
    assert (tmp_path / "wt1/src/app.txt").read_text() == "v1\n"


def test_worktrees_are_isolated_from_each_other(tmp_path, repo):
    _path, node = repo
    a = node.worktree(str(tmp_path / "a"))
    b = node.worktree(str(tmp_path / "b"))

    (tmp_path / "a/src/app.txt").write_text("edit-a\n")

    assert (tmp_path / "b/src/app.txt").read_text() == "v1\n"
    assert node.snapshot(a.path) != node.snapshot(b.path)


def test_reusing_a_worktree_path_resets_it(tmp_path, repo):
    _path, node = repo
    first = node.worktree(str(tmp_path / "wt"))
    (tmp_path / "wt/src/app.txt").write_text("dirty\n")
    (tmp_path / "wt/untracked.txt").write_text("junk\n")

    again = node.worktree(str(tmp_path / "wt"))

    assert again.base_sha == first.base_sha
    assert first.created and not again.created
    assert (tmp_path / "wt/src/app.txt").read_text() == "v1\n"
    assert not (tmp_path / "wt/untracked.txt").exists()


def test_reuse_switches_commits_over_local_modifications(tmp_path, repo):
    _path, node = repo
    base = node.resolve("HEAD")
    wt = node.worktree(str(tmp_path / "wt"), base=base)
    (tmp_path / "wt/src/app.txt").write_text("candidate-1\n")
    other = node.commit(wt.path, "another parent", parents=[base])

    (tmp_path / "wt/src/app.txt").write_text("uncommitted edit\n")
    reused = node.worktree(str(tmp_path / "wt"), base=other.sha)

    assert reused.base_sha == other.sha and not reused.created
    assert (tmp_path / "wt/src/app.txt").read_text() == "candidate-1\n"


def test_worktree_on_a_bad_revision_raises(tmp_path, repo):
    _path, node = repo

    with pytest.raises(GitError, match="does not resolve"):
        node.worktree(str(tmp_path / "bad"), base="nope")


def test_snapshot_is_content_identity_not_history(tmp_path, repo):
    _path, node = repo
    a = node.worktree(str(tmp_path / "a"))
    b = node.worktree(str(tmp_path / "b"))

    (tmp_path / "a/src/app.txt").write_text("same edit\n")
    (tmp_path / "b/src/app.txt").write_text("same edit\n")

    assert node.snapshot(a.path) == node.snapshot(b.path)


def test_commit_is_deterministic_across_repeats(tmp_path, repo):
    _path, node = repo
    base = node.resolve("HEAD")
    wt = node.worktree(str(tmp_path / "wt"))
    (tmp_path / "wt/src/app.txt").write_text("v2\n")

    first = node.commit(wt.path, "mutate", parents=[base])
    second = node.commit(wt.path, "mutate", parents=[base])

    assert first.sha == second.sha != ""
    assert first.tree == second.tree


def test_commit_sha_tracks_content_and_parents(tmp_path, repo):
    _path, node = repo
    base = node.resolve("HEAD")
    wt = node.worktree(str(tmp_path / "wt"))
    (tmp_path / "wt/src/app.txt").write_text("v2\n")
    one = node.commit(wt.path, "m", parents=[base])

    (tmp_path / "wt/src/app.txt").write_text("v3\n")
    other_content = node.commit(wt.path, "m", parents=[base])
    orphan = node.commit(wt.path, "m", parents=[])

    assert one.sha != other_content.sha
    assert other_content.sha != orphan.sha
    assert other_content.tree == orphan.tree


def test_commit_publishes_a_ref_and_lists_it(tmp_path, repo):
    _path, node = repo
    base = node.resolve("HEAD")
    wt = node.worktree(str(tmp_path / "wt"))
    (tmp_path / "wt/src/app.txt").write_text("v2\n")

    c = node.commit(wt.path, "gen1", parents=[base], ref="refs/evo/gen1/a")

    assert c.ref == "refs/evo/gen1/a"
    assert node.list_refs("refs/evo/**") == ["refs/evo/gen1/a"]
    assert node.resolve("refs/evo/gen1/a") == c.sha


def test_notes_round_trip_and_replace(tmp_path, repo):
    _path, node = repo
    sha = node.resolve("HEAD")

    node.write_note(sha, json.dumps({"score": 1.0}))
    assert json.loads(node.read_note(sha))["score"] == 1.0

    node.write_note(sha, json.dumps({"score": 2.0}))
    assert json.loads(node.read_note(sha))["score"] == 2.0


def test_missing_note_is_empty_not_an_error(repo):
    _path, node = repo

    assert node.read_note(node.resolve("HEAD")) == ""


def test_lineage_walks_the_commit_dag(tmp_path, repo):
    _path, node = repo
    sha = node.resolve("HEAD")
    wt = node.worktree(str(tmp_path / "wt"))

    for i in range(3):
        (tmp_path / "wt/src/app.txt").write_text(f"gen{i}\n")
        sha = node.commit(wt.path, f"gen{i}", parents=[sha]).sha

    line = node.lineage(sha)

    assert [c.message for c in line] == ["gen2", "gen1", "gen0", "seed"]
    assert line[0].parents == [line[1].sha]
    assert all(c.tree for c in line)


def test_remove_worktree_cleans_up(tmp_path, repo):
    _path, node = repo
    wt = node.worktree(str(tmp_path / "wt"))

    node.remove_worktree(wt.path)

    assert not (tmp_path / "wt").exists()
    assert node.worktree(str(tmp_path / "wt")).base_sha != ""


def test_reads_answer_empty_and_writes_raise(tmp_path):
    node = GitNode(str(tmp_path / "not-a-repo"))

    assert node.resolve("HEAD") == ""
    assert node.tree_of("HEAD") == ""
    assert node.lineage("HEAD") == []
    assert node.list_refs() == []

    with pytest.raises(GitError):
        node.snapshot(str(tmp_path))


def test_snapshot_never_returns_a_degenerate_cache_key(tmp_path, repo):
    _path, node = repo

    with pytest.raises(GitError):
        node.snapshot(str(tmp_path / "does-not-exist"))


def test_commit_propagates_a_failed_ref_update(tmp_path, repo):
    _path, node = repo
    base = node.resolve("HEAD")
    wt = node.worktree(str(tmp_path / "wt"))
    (tmp_path / "wt/src/app.txt").write_text("v2\n")

    with pytest.raises(GitError, match="update-ref"):
        node.commit(wt.path, "m", parents=[base], ref="refs/evo/bad ref")


def test_safe_ref_component_sanitizes():
    assert GitNode.safe_ref_component("//fp:model") == "fp_model"
    assert GitNode.safe_ref_component("!!!") == "x"


def test_commit_records_the_evaluated_tree_not_later_droppings(tmp_path, repo):
    _path, node = repo
    base = node.resolve("HEAD")
    wt = node.worktree(str(tmp_path / "wt"))
    (tmp_path / "wt/src/app.txt").write_text("v2\n")
    evaluated = node.snapshot(wt.path)

    (tmp_path / "wt/bazel-out").write_text("build dropping\n")

    pinned = node.commit(wt.path, "m", parents=[base], tree=evaluated)
    loose = node.commit(wt.path, "m", parents=[base])

    assert pinned.tree == evaluated
    assert loose.tree != evaluated


@pytest.fixture
def origin(tmp_path):
    """An upstream repository with two commits, to mirror from."""
    path = tmp_path / "origin"
    path.mkdir()
    git(path, "init", "-q", "-b", "main")
    git(path, "config", "user.email", "t@example.com")
    git(path, "config", "user.name", "t")
    (path / "src").mkdir()
    (path / "src/app.txt").write_text("upstream v1\n")
    git(path, "add", "-A")
    git(path, "commit", "-qm", "first")
    (path / "src/app.txt").write_text("upstream v2\n")
    git(path, "add", "-A")
    git(path, "commit", "-qm", "second")
    return path


def test_ensure_mirror_clones_once_and_is_idempotent(tmp_path, origin):
    node = GitNode(str(tmp_path / "cache/repo.git"))

    assert node.ensure_mirror(str(origin)) is True
    assert node.ensure_mirror(str(origin)) is False
    assert (tmp_path / "cache/repo.git/objects").is_dir()
    assert not (tmp_path / "cache/repo.git/.git").exists()


def test_the_whole_cycle_works_against_a_bare_mirror(tmp_path, origin):
    node = GitNode(str(tmp_path / "cache/repo.git"))
    node.ensure_mirror(str(origin))

    base = node.resolve("HEAD")
    assert base

    a = node.worktree(str(tmp_path / "wt/a"), base=base)
    b = node.worktree(str(tmp_path / "wt/b"), base=base)
    assert (tmp_path / "wt/a/src/app.txt").read_text() == "upstream v2\n"

    (tmp_path / "wt/a/src/app.txt").write_text("edit-a\n")
    (tmp_path / "wt/b/src/app.txt").write_text("edit-a\n")
    assert node.snapshot(a.path) == node.snapshot(b.path)

    commit = node.commit(a.path, "variant", parents=[base], ref="refs/evo/gen0/a")
    node.write_note(commit.sha, '{"score": 3}')

    assert node.resolve("refs/evo/gen0/a") == commit.sha
    assert node.read_note(commit.sha) == '{"score": 3}'
    assert [c.message for c in node.lineage(commit.sha)] == ["variant", "second", "first"]


def test_fetch_brings_upstream_commits_into_the_mirror(tmp_path, origin):
    node = GitNode(str(tmp_path / "cache/repo.git"))
    node.ensure_mirror(str(origin))
    before = node.resolve("HEAD")

    (origin / "src/app.txt").write_text("upstream v3\n")
    git(origin, "add", "-A")
    git(origin, "commit", "-qm", "third")
    node.fetch()

    after = node.resolve("HEAD")
    assert after != before
    assert node.lineage(after)[0].message == "third"


def test_ensure_mirror_raises_on_a_bad_url(tmp_path):
    node = GitNode(str(tmp_path / "cache/repo.git"))

    with pytest.raises(GitError, match="clone mirror"):
        node.ensure_mirror(str(tmp_path / "no-such-repo"))


def test_init_creates_a_repo_and_is_idempotent(tmp_path):
    node = GitNode(str(tmp_path / "fresh"))

    assert node.init() is True
    assert node.init() is False
    assert (tmp_path / "fresh/.git").is_dir()
    assert node.resolve("HEAD") == ""


def test_init_then_commit_lands_a_seed_without_a_worktree(tmp_path):
    node = GitNode(str(tmp_path / "fresh"))
    node.init(branch="main")
    (tmp_path / "fresh/app.txt").write_text("v1\n")

    seed = node.commit(str(tmp_path / "fresh"), "seed", parents=[],
                       ref="refs/heads/main")

    assert seed.sha and not seed.parents
    assert node.resolve("HEAD") == seed.sha
    assert [c.message for c in node.lineage("HEAD")] == ["seed"]


def test_init_bare(tmp_path):
    node = GitNode(str(tmp_path / "bare.git"))

    assert node.init(bare=True) is True
    assert (tmp_path / "bare.git/objects").is_dir()
    assert not (tmp_path / "bare.git/.git").exists()
