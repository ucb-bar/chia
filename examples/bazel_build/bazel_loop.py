"""Drive a Bazel workspace as a CHIA graph, skipping targets whose inputs didn't change.

Discover the targets, build them in parallel on ``bazel`` workers, then test.
Each call is tagged with ``BazelNode.source_digest`` -- a hash of the target's
transitive sources and BUILD files -- so an untouched target is replayed from
the CHIA cache. Bazel's own action cache already makes a warm rerun fast; the
CHIA cache skips the dispatch as well, and survives the work landing on a
different machine.

Needs ``bazel`` or ``bazelisk`` on PATH; the workspace under workspace/ is
copied into out/ on first run.

    python examples/bazel_build/bazel_loop.py                  # cold
    python examples/bazel_build/bazel_loop.py                  # warm
    python examples/bazel_build/bazel_loop.py --edit lib       # invalidate a subtree
    python examples/bazel_build/bazel_loop.py --flush-cache
"""

import argparse
import os
import shutil
import sys
import time
from pathlib import Path

import ray

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from chia.base.ChiaFunction import get
from chia.base.bypass import get_active_bypass
from chia.base.cache import stop_cache
from chia.bazel.bazel_node import BazelNode
from common.result_cache import enable_result_cache

HERE = os.path.dirname(os.path.abspath(__file__))
WORKSPACE = os.path.join(HERE, "out", "workspace")
CACHE_DIR = os.path.join(HERE, "out", "cache")
CONFIG = os.path.join(HERE, "bazel_cache.yaml")

TARGETS = ["//lib:core", "//lib:util", "//app:bundle"]


TEMPLATE = os.path.join(HERE, "workspace")


def materialize_workspace() -> None:
    """Copy workspace/ into out/workspace. Never clobbers an --edit."""
    if os.path.isdir(WORKSPACE):
        return
    shutil.copytree(TEMPLATE, WORKSPACE)
    print(f"materialized workspace at {WORKSPACE}")


def edit_source(name: str) -> None:
    """Touch one source file's *contents* so its digest — and only its — changes."""
    path = os.path.join(WORKSPACE, name, f"{'core' if name == 'lib' else name}.txt")
    with open(path, "a") as f:
        f.write(f"edit {time.time()}\n")
    print(f"edited {os.path.relpath(path, WORKSPACE)}")


def run_loop(node: BazelNode) -> None:
    """One pass: digest each target, dispatch the builds in parallel, then test."""
    bypass = get_active_bypass()

    print("\n-- digests (the cache key for each target) --")
    digests = {t: node.source_digest([t]) for t in TARGETS}
    for target, digest in digests.items():
        cached = bypass is not None and bypass.is_bypassed("build", digest)
        print(f"  {target:<16} {digest[:16]}...  {'CACHED (replay)' if cached else 'builds for real'}")

    print("\n-- build (parallel across bazel workers) --")
    start = time.time()
    refs = [
        node.build.chia_remote(node, [target], collect_outputs=True, _chia_tag=digests[target])
        for target in TARGETS
    ]
    results = [get(r) for r in refs]
    print(f"  build wall-clock: {time.time() - start:.1f}s")
    for target, result in zip(TARGETS, results):
        outs = ", ".join(result.output_paths) or "-"
        print(f"  {target:<16} rc={result.returncode} in {result.duration_seconds:5.1f}s  outs: {outs}")

    print("\n-- test --")
    test_result = get(node.test.chia_remote(node, ["//app:bundle_test"]))
    print(f"  //app:bundle_test rc={test_result.returncode} success={test_result.success}")
    for name, log in test_result.test_logs.items():
        print(f"    {name}: {log.strip().splitlines()[-1] if log.strip() else '(empty)'}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bazel-bin", default="bazel", help="bazel or bazelisk binary")
    parser.add_argument("--edit", choices=["lib", "app"], help="edit one subtree's source first")
    parser.add_argument("--flush-cache", action="store_true", help="reset to a cold cache")
    parser.add_argument("--clean", action="store_true", help="delete the generated workspace")
    parser.add_argument("--local", type=int, metavar="N",
                        help="start a local Ray advertising N 'bazel' workers instead of "
                             "connecting to a CHIA cluster")
    args = parser.parse_args()

    if args.clean:
        shutil.rmtree(os.path.join(HERE, "out"), ignore_errors=True)
        print("removed examples/bazel_build/out")
        return

    materialize_workspace()
    if args.edit:
        edit_source(args.edit)

    if args.local:
        ray.init(ignore_reinit_error=True, resources={"bazel": args.local})
    else:
        ray.init(ignore_reinit_error=True)
    enable_result_cache(["build"], yaml_path=CONFIG, cache_dir=CACHE_DIR,
                        size=256, flush=args.flush_cache)
    if args.flush_cache:
        print("flushed cache (cold start)")

    node = BazelNode(
        WORKSPACE,
        bazel_bin=args.bazel_bin,
        common_flags=["--keep_going", "--verbose_failures"],
        timeout_seconds=600,
    )
    run_loop(node)
    get(node.shutdown.chia_remote(node))

    stop_cache()
    ray.shutdown()
    print("\nRun again to see unchanged targets replayed from the cache; "
          "`--edit lib` invalidates //lib:core and, through it, //app:bundle.")


if __name__ == "__main__":
    main()
