# git_bazel_loop

An evolutionary loop over a git repository, evaluated with Bazel.

```
pick a parent -> worktree -> mutate -> snapshot -> build + test
              -> accept or reject -> commit + note -> select
```

Variants are commits under `refs/evo/gen<N>/<tree8>`. Lineage is the commit
DAG. Each verdict is a git note on the *tree* that produced it, which is the
part worth knowing: notes annotate any object, not just commits, so one lookup
answers "have I already scored this source state". Two agents that make the
same edit produce the same tree and the second costs nothing, no worker and no
Ray task. A rejected candidate gets a note too, on a tree that was never
committed, so a known-bad edit is never re-run.

That is also why there is no CHIA cache here. A content-keyed store of verdicts
read back later is what the notes ref already is, and it doesn't evict.
`examples/bazel_build` does use the CHIA cache, because it caches a
`BazelResult` holding output bytes, which has no business being in git.

## Run it

```bash
python examples/git_bazel_loop/evolve.py --local 3           # 3 generations
python examples/git_bazel_loop/evolve.py --local 3           # resumes
python examples/git_bazel_loop/evolve.py --local 3 --gens 5
python examples/git_bazel_loop/evolve.py --evaluator v2      # re-score everything
python examples/git_bazel_loop/evolve.py --clean
```

`--local N` starts a local Ray with N `bazel` workers, so this runs on one
machine with no CHIA cluster. On a cluster, drop the flag and use
`cluster.yaml`. Needs `bazel` (or `bazelisk`) and `git` on PATH.

`--evaluator` names the evidence store (`refs/notes/chia/evidence/<name>`). A
tree sha says what the source was, not what measured it, so bump this when the
evaluator changes or an unchanged tree replays a stale verdict. The old store
stays readable.

Only `bazel` is a worker resource. `GitNode`'s members are called in-process
here, the same arrangement as `examples/timing_opt/db.py`: the repo sits next to
the driver and git plumbing is milliseconds, so a dispatch per call would cost
more than the call. The `resources={"git": 1}` on those members is for the other
case, a checkout held on a worker and reached with `chia_remote`.

## Output

```
=== generation 1  parent c1bc688499 score=-15  (population 12) ===
  tuning 57 -> 56        tree=5d1e38a570 ON RECORD
  tuning 57 -> 53        tree=3c7d7401f1 ON RECORD
  tuning 57 -> 58        tree=837bd8ab8c evaluates
  evaluated 1 candidates in 7.7s
```

`5d1e38a570` was scored in a previous run on a different lineage,
`3c7d7401f1` a generation earlier in this one. Neither is dispatched; a
generation with nothing new reports `evaluated 0 candidates in 0.0s`.

Afterwards the population is an ordinary git DAG:

```bash
git -C out/repo log --graph --format='%h %s' \
    $(git -C out/repo for-each-ref --format='%(refname)' 'refs/evo/**')
```

## The pack

Everything problem-specific is a `Pack`; `run()` is generic over it.

```python
@dataclass
class Pack:
    dispatch: Callable[[str, str], object]   # (worktree, tree) -> ObjectRef
    accept:   Callable[[dict], bool]
    score:    Callable[[dict], float]
    mutate:   Callable[[str, Random], str]
    ref_for:  Callable[[int, str], str]
    notes_ref: str
```

The factoring is from HORIZON's project pack
([arXiv:2606.28279](https://arxiv.org/abs/2606.28279)), which separates the
evaluator, the acceptance predicate and the version-control policy from the loop
driving them. `run()` never mentions widgets, Bazel or `refs/evo`, and returns a
`Campaign` rather than printing, so it can be driven from a test.

Only candidates passing `accept` are committed; the rest go into
`Campaign.rejects`. The population holds versions that passed, not versions that
were tried, so `Variant.score` is a float rather than an optional. The seed is
evaluated and admitted the same way, as generation zero.

`mutate` is a stub that edits one constant, so the example runs with no LLM. The
agentic version is a drop-in:

```python
tool = BashTool(name=f"edit{i}", work_dir=worktree.path,
                task_options={"resources": {"agent": 1}})
get(llm.prompt.chia_remote(llm, "Reduce the critical path in ...", tools=[tool]))
```

## Files

| File | |
|------|---|
| `evolve.py` | The pack for this problem, the loop, a printer. |
| `workspace/` | The seed repository, as real files. |
| `cluster.yaml` | 1 head + 3 `bazel` workers. No `git` worker. |
| `out/repo/` | Generated repository: population and evidence. |
| `out/bazel_disk_cache/` | Bazel action cache shared across the worktrees. |

## Across several machines

Worktrees share their repository's object database, so each node needs its own
copy. `ensure_mirror` clones a bare mirror once per worker:

```python
git_node = GitNode("/cache/repo.git")
get(git_node.ensure_mirror.chia_remote(git_node, "git@github.com:org/repo.git"))
wt = get(git_node.worktree.chia_remote(git_node, f"/work/w{i}", base=parent_sha))
```

After that a worktree is a hardlinked checkout rather than a clone. `fetch()` is
separate and not per task: every agent on a node contends on one ref lock, and
in a loop whose parents are its own commits there is nothing upstream to fetch.

Reuse a fixed set of worktree paths (`w0`, `w1`, ...) rather than a fresh
directory per run. Bazel derives its output base from an md5 of the workspace
path, so a new path each time means a new server, a cold analysis cache and no
action reuse between siblings. Since a Bazel server outlives its client,
churning paths also leaks a JVM per run.

`refs/notes/*` is in none of git's default refspecs, so the evidence store does
not travel with a plain clone, fetch or push. Mirror the repository, or
configure `+refs/notes/*:refs/notes/*`.

## Limits

- Notes are written from the driver, serially, and have to be. `git notes` is a
  read-modify-write of one ref's tree: 24 concurrent writes to one ref stored
  12-14 of them and reported no error. Worktrees share the ref namespace, so
  workers contend exactly like threads. A decentralized version needs one notes
  ref per writer.
- Generations have a barrier, so a generation waits on its slowest candidate.
- `resources={"git": 1}` does not express "the host holding the checkout".
- The fitness is a toy: distance from a constant the mutator cannot see. A real
  loop reads a timing report or a benchmark result.

## Two details that are load-bearing

`GitNode.commit(..., tree=tree)` pins the commit to the snapshot that was
scored. Without it `git add -A` also picks up whatever the build left in the
worktree, Bazel's convenience symlinks and a lockfile, so the commit's tree
stops matching the key the variant was scored under and cross-run dedup quietly
stops working. `workspace/.gitignore` covers the same hazard for the next
snapshot.

Commits use `write-tree` + `commit-tree` under a fixed identity, so the sha is a
pure function of (tree, parents, message). A node CHIA re-queues after a worker
failure produces the same sha instead of forking the population, which is why
these nodes keep Ray's retries where the database nodes set `max_retries=0`.
