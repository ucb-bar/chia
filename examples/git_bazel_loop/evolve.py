"""Edit code in a git worktree, build it with Bazel, score it, repeat.

    pick a parent -> worktree -> mutate -> snapshot -> build + test
                  -> accept or reject -> commit + note -> select

Variants are commits under refs/evo/, lineage is the commit DAG, and each
verdict is a git note on the *tree* that produced it. Notes annotate any
object, so one lookup answers "have I scored this source state before" -- which
is why there is no CHIA cache here.

Everything problem-specific lives in a Pack; run() is generic over it.

    python examples/git_bazel_loop/evolve.py --local 3
    python examples/git_bazel_loop/evolve.py --local 3 --gens 5
    python examples/git_bazel_loop/evolve.py --evaluator v2
    python examples/git_bazel_loop/evolve.py --clean
"""

import argparse
import json
import os
import random
import shutil
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import ray

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from chia.base.ChiaFunction import ChiaFunction, get
from chia.bazel.bazel_node import BazelNode
from chia.git.git_node import GitNode

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "out")
REPO = os.path.join(OUT, "repo")
WORKTREES = os.path.join(OUT, "worktrees")
DISK_CACHE = os.path.join(OUT, "bazel_disk_cache")


TARGET = "//widget:widget"
TEST = "//widget:widget_test"
IDEAL = 71

TEMPLATE = os.path.join(HERE, "workspace")


def seed_repo(git_node: GitNode) -> None:
    """Create the repository from workspace/ and commit it. Idempotent."""
    if not git_node.init(branch="main"):
        return
    shutil.copytree(TEMPLATE, REPO, dirs_exist_ok=True)
    git_node.commit(REPO, "seed", parents=[], ref="refs/heads/main")
    print(f"seeded repo at {REPO}")


@dataclass
class Pack:
    """One problem, as the loop sees it.

    ``dispatch`` is a closure, not a bare node, so the pack can bind a tool
    node without the loop knowing one exists. Only candidates ``accept``
    passes are committed. Version ``notes_ref`` when the evaluator changes: a
    tree sha says what the source was, not what measured it.
    """
    dispatch: Callable[[str, str], object]
    accept: Callable[[dict], bool]
    score: Callable[[dict], float]
    mutate: Callable[[str, random.Random], str]
    ref_for: Callable[[int, str], str]
    notes_ref: str = "refs/notes/chia/evidence/v1"


@ChiaFunction(resources={"bazel": 1})
def evaluate(bazel: BazelNode, worktree_path: str) -> dict:
    """Build and test the variant in *worktree_path*; return the evidence."""
    built = bazel.build([TARGET], workspace_dir=worktree_path, collect_outputs=True)
    if not built.success:
        return {"ok": False, "stage": "build", "stderr": built.stderr[-800:]}

    tested = bazel.test([TEST], workspace_dir=worktree_path, collect_test_logs=False)
    if not tested.success:
        return {"ok": False, "stage": "test", "stderr": tested.stderr[-800:]}

    artifact = next(iter(built.outputs.values()), b"").decode()
    tuned = int(artifact.strip().rsplit("=", 1)[1])
    return {"ok": True, "stage": "scored", "tuned": tuned,
            "score": -abs(tuned - IDEAL),
            "outputs": built.output_paths,
            "build_s": round(built.duration_seconds, 2)}


def mutate_tuning(worktree_path: str, rng: random.Random) -> str:
    """Edit the worktree in place. An agent goes here; see the README."""
    path = os.path.join(worktree_path, "widget/tuning.txt")
    current = int(open(path).read().strip())
    proposal = max(1, current + rng.choice([-16, -8, -4, -1, 1, 4, 8, 16]))
    open(path, "w").write(f"{proposal}\n")
    return f"tuning {current} -> {proposal}"


def widget_pack(bazel: BazelNode, evaluator: str = "v1") -> Pack:
    return Pack(
        dispatch=lambda path, tree: evaluate.chia_remote(bazel, path),
        accept=lambda evidence: evidence["ok"],
        score=lambda evidence: evidence["score"],
        mutate=mutate_tuning,
        ref_for=lambda gen, tree: f"refs/evo/gen{gen}/{tree[:8]}",
        notes_ref=f"refs/notes/chia/evidence/{evaluator}",
    )


@dataclass
class Variant:
    """An accepted member of the population."""
    sha: str
    tree: str
    score: float
    evidence: dict


@dataclass
class Reject:
    """A candidate ``accept`` turned away. Never committed."""
    tree: str
    what: str
    evidence: dict


@dataclass
class Campaign:
    """The record of a run."""
    variants: list[Variant] = field(default_factory=list)
    rejects: list[Reject] = field(default_factory=list)
    replayed: int = 0
    dispatched: int = 0
    resumed: int = 0

    @property
    def best(self) -> Variant:
        return max(self.variants, key=lambda v: v.score)


def evidence_for(git_node: GitNode, pack: Pack, tree: str) -> dict | None:
    """The stored verdict for *tree*, or None if it was never evaluated."""
    note = git_node.read_note(tree, notes_ref=pack.notes_ref)
    return json.loads(note) if note else None


def load_population(git_node: GitNode, pack: Pack) -> list[Variant]:
    """Every variant already in the repository."""
    variants = []
    for ref in git_node.list_refs("refs/evo/**"):
        sha = git_node.resolve(ref)
        tree = git_node.tree_of(sha) if sha else ""
        evidence = evidence_for(git_node, pack, tree) if tree else None
        if evidence is None:
            continue
        variants.append(Variant(sha=sha, tree=tree,
                                score=pack.score(evidence), evidence=evidence))
    return variants


def admit_seed(git_node: GitNode, pack: Pack, worktrees_dir: str) -> Variant:
    """Evaluate the unmodified HEAD and admit it as generation zero.

    Scoring the seed keeps ``Variant.score`` a float everywhere, so selection
    and reporting need no filtering.
    """
    root = git_node.resolve("HEAD")
    worktree = git_node.worktree(os.path.join(worktrees_dir, "w0"), base=root)
    tree = git_node.snapshot(worktree.path)
    evidence = evidence_for(git_node, pack, tree)
    if evidence is None:
        evidence = get(pack.dispatch(worktree.path, tree))
        git_node.write_note(tree, json.dumps(evidence), notes_ref=pack.notes_ref)
    if not pack.accept(evidence):
        raise RuntimeError(
            f"the seed itself was rejected at stage {evidence.get('stage')!r}; "
            f"there is nothing to evolve from:\n{evidence.get('stderr', '')}")
    return Variant(sha=root, tree=tree, score=pack.score(evidence), evidence=evidence)


def select(population: list[Variant], rng: random.Random) -> Variant:
    """Tournament of 3 on score. Higher is better, and 0 can be the optimum,
    so the key is ``v.score`` -- ``v.score or <floor>`` would bury it."""
    return max(rng.sample(population, min(3, len(population))), key=lambda v: v.score)


def propose(git_node: GitNode, pack: Pack, parent: Variant, width: int,
            rng: random.Random, worktrees_dir: str) -> list[tuple[str, str, str]]:
    """Cut a worktree per candidate, mutate it, return ``(path, tree, what)``.

    Two candidates that converge on one tree in the same generation would both
    dispatch, since neither has a note yet, so the duplicate is dropped here.
    """
    candidates, seen = [], set()
    for i in range(width):
        worktree = git_node.worktree(os.path.join(worktrees_dir, f"w{i}"),
                                     base=parent.sha)
        what = pack.mutate(worktree.path, rng)
        tree = git_node.snapshot(worktree.path)
        if tree in seen:
            continue
        seen.add(tree)
        candidates.append((worktree.path, tree, what))
    return candidates


def run(git_node: GitNode, pack: Pack, gens: int, width: int, seed: int,
        worktrees_dir: str, on_event: Callable[[str, dict], None] = None) -> Campaign:
    """Run *gens* generations and return the Campaign.

    Progress goes through *on_event*, so this is drivable from a test.
    """
    emit = on_event or (lambda kind, info: None)
    rng = random.Random(seed)
    campaign = Campaign()

    campaign.variants = load_population(git_node, pack)
    campaign.resumed = len(campaign.variants)
    if not campaign.variants:
        campaign.variants = [admit_seed(git_node, pack, worktrees_dir)]
    emit("start", {"resumed": campaign.resumed, "population": len(campaign.variants)})

    for gen in range(gens):
        parent = select(campaign.variants, rng)
        emit("generation", {"gen": gen, "parent": parent,
                            "population": len(campaign.variants)})

        candidates = propose(git_node, pack, parent, width, rng, worktrees_dir)

        known, pending = {}, []
        for path, tree, what in candidates:
            prior = evidence_for(git_node, pack, tree)
            if prior is None:
                pending.append((path, tree, what))
                campaign.dispatched += 1
            else:
                known[tree] = prior
                campaign.replayed += 1
            emit("candidate", {"tree": tree, "what": what, "known": prior is not None})

        started = time.time()
        refs = [pack.dispatch(path, tree) for path, tree, _ in pending]
        for (_, tree, _), evidence in zip(pending, [get(ref) for ref in refs]):
            git_node.write_note(tree, json.dumps(evidence), notes_ref=pack.notes_ref)
            known[tree] = evidence
        emit("evaluated", {"count": len(pending),
                           "seconds": time.time() - started})

        for path, tree, what in candidates:
            evidence = known[tree]
            if not pack.accept(evidence):
                campaign.rejects.append(Reject(tree=tree, what=what, evidence=evidence))
                emit("reject", {"tree": tree, "what": what, "evidence": evidence})
                continue
            commit = git_node.commit(path, f"gen{gen}: {what}", parents=[parent.sha],
                                     ref=pack.ref_for(gen, tree), tree=tree)
            variant = Variant(sha=commit.sha, tree=tree,
                              score=pack.score(evidence), evidence=evidence)
            campaign.variants.append(variant)
            emit("accept", {"variant": variant, "what": what})

    return campaign


def printer():
    """An *on_event* callback that prints a run."""
    def emit(kind, info):
        if kind == "start" and info["resumed"]:
            print(f"resuming from {info['resumed']} stored variants")
        elif kind == "generation":
            print(f"\n=== generation {info['gen']}  "
                  f"parent {info['parent'].sha[:10]} score={info['parent'].score}  "
                  f"(population {info['population']}) ===")
        elif kind == "candidate":
            print(f"  {info['what']:<22} tree={info['tree'][:10]} "
                  f"{'ON RECORD' if info['known'] else 'evaluates'}")
        elif kind == "evaluated":
            print(f"  evaluated {info['count']} candidates in {info['seconds']:.1f}s")
        elif kind == "accept":
            v = info["variant"]
            print(f"    {v.sha[:10]} score={v.score} accepted")
        elif kind == "reject":
            print(f"    rejected at {info['evidence'].get('stage')}: {info['what']}")
    return emit


def print_summary(git_node: GitNode, pack: Pack, campaign: Campaign) -> None:
    best = campaign.best
    print(f"\nbest: {best.sha[:10]} score={best.score} (0 is ideal)")
    print(f"dispatched {campaign.dispatched}, already on record "
          f"{campaign.replayed}, rejected {len(campaign.rejects)}")
    print("lineage:")
    for commit in git_node.lineage(best.sha, limit=8):
        evidence = evidence_for(git_node, pack, commit.tree)
        score = evidence["score"] if evidence else "-"
        print(f"  {commit.sha[:10]} tree={commit.tree[:10]} "
              f"score={str(score):>5}  {commit.message}")
    print(f"\n  git -C {REPO} log --graph --format='%h %s' $(git -C {REPO} "
          f"for-each-ref --format='%(refname)' 'refs/evo/**')")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gens", type=int, default=3)
    parser.add_argument("--width", type=int, default=3, help="candidates per generation")
    parser.add_argument("--seed", type=int, default=0, help="RNG seed for the mutator")
    parser.add_argument("--local", type=int, metavar="N",
                        help="start a local Ray advertising N 'bazel' workers")
    parser.add_argument("--clean", action="store_true")
    parser.add_argument("--evaluator", default="v1",
                        help="evidence-store version; bump it when the "
                             "evaluator changes so nothing replays a verdict "
                             "produced by a different measurement")
    args = parser.parse_args()

    if args.clean:
        shutil.rmtree(OUT, ignore_errors=True)
        print(f"removed {OUT}")
        return

    if args.local:
        ray.init(ignore_reinit_error=True, resources={"bazel": args.local})
    else:
        ray.init(ignore_reinit_error=True)

    git_node = GitNode(REPO)
    seed_repo(git_node)

    bazel = BazelNode(REPO, common_flags=["--keep_going", f"--disk_cache={DISK_CACHE}"],
                      timeout_seconds=600)
    pack = widget_pack(bazel, evaluator=args.evaluator)

    try:
        campaign = run(git_node, pack, args.gens, args.width, args.seed,
                       WORKTREES, on_event=printer())
        print_summary(git_node, pack, campaign)
    finally:
        for i in range(args.width):
            worktree = os.path.join(WORKTREES, f"w{i}")
            if os.path.isdir(worktree):
                bazel.shutdown(workspace_dir=worktree)
        bazel.shutdown(workspace_dir=REPO)
        ray.shutdown()


if __name__ == "__main__":
    main()
