"""Minimal example of CHIA caching + bypass on a 4x4 loop.

The loop runs ``N_ITERS`` iterations, each dispatching ``N_PARALLEL`` calls to a
(deliberately slow) ``expensive_step`` ChiaFunction. Every call carries a
``_chia_tag`` of the form ``iter<N>_call<M>`` — the key that both the cache and
bypass use.

There is one loop. **Run the script twice** to see the populate-then-replay
workflow; the difference comes entirely from the cache being cold vs warm:

  * Run 1 — cold cache. The bypass regex ``iter[13]_.*`` selects iterations 1
    and 3, but a ``bypass_cond`` (``cache_hit_cond``) gates the bypass on the
    value actually being in the cache. On a cold cache it isn't, so the cond
    returns False and those calls run for real (then auto-write to the cache,
    ``cache: true`` regex ``iter.*``). Iterations 0 and 2 don't match the bypass
    regex, so they also run for real. Net: all 16 run for real and populate the
    cache.

  * Run 2 — warm cache. Now iterations 1 and 3 hit the cache, the cond returns
    True, and they are served from the cache instantly (no real work) while
    still dispatching through Ray. Iterations 0 and 2 still run for real.

The ``bypass_cond`` is what makes the *same* loop body safe both cold and warm:
without it, Run 1 would dispatch the cache-reading provider on a cold cache and
raise ``KeyError`` (cache miss). The cond falls through to a real run instead.

Run it (a local Ray is started if you're not on a CHIA cluster)::

    python examples/bypass_cache/bypass_cache_loop.py              # run 1: cold
    python examples/bypass_cache/bypass_cache_loop.py              # run 2: warm
    python examples/bypass_cache/bypass_cache_loop.py --flush-cache  # reset to cold

See the "Caching and Bypass" user guide for the full reference.
"""

import argparse
import os
import time

import ray

from chia.base.ChiaFunction import ChiaFunction, get
from chia.base.bypass import Bypass, get_active_bypass
from chia.base.cache import start_cache, stop_cache, get_active_cache

N_ITERS = 4
N_PARALLEL = 4

# Fixed Ray namespace for the cache actor. The cache is a *named* actor; named
# actors are scoped by namespace, and a worker running the bypass provider
# resolves names in whatever namespace it executes in (which is NOT the driver's
# anonymous one once CHIA's dispatch proxy is involved). Pinning everything —
# ray.init, start_cache, and the provider's lookup — to one explicit namespace
# makes the actor reachable from the driver (for the auto-write) and from any
# worker (for the replay read).
CACHE_NAMESPACE = None


@ChiaFunction(resources={"demo": 1})
def expensive_step(iteration: int, call: int, x: int) -> int:
    """Stand-in for a slow build / simulation / LLM call."""
    time.sleep(1.0)  # pretend this is expensive
    result = x * x
    # Printed only when the function actually runs — bypassed calls skip it.
    print(f"      [REAL RUN] iter{iteration} call{call}: {x}^2 = {result}", flush=True)
    return result


def cache_provider(tag, data_path, *args, **kwargs):
    """Bypass provider that serves a previously cached result for *tag*.

    This is the manual read path: the cache never auto-serves, so a bypass
    provider is what turns a cached value into a replayed result.
    """
    hit, value = get(get_active_cache().read.chia_remote(tag))
    if not hit:
        raise KeyError(f"cache miss for tag {tag!r}")
    return value


def cache_hit_cond(tag, data_path, *args, **kwargs):
    """Bypass condition: only bypass when *tag* is actually in the cache.

    Registered via ``set_cond``, this runs on the caller as the last gate after
    the bypass regex matches. On a cold cache it returns False, so the call
    falls through to a real run (which then auto-writes the value) instead of
    dispatching the cache-reading provider against a missing key.
    """
    return get(get_active_cache().has.chia_remote(tag))


def run_loop(label: str) -> None:
    """One 4x4 loop: N_ITERS iterations, N_PARALLEL parallel calls each."""
    print(f"\n=== {label} ===")
    bypass = get_active_bypass()
    for it in range(N_ITERS):
        start_time = time.time()

        # Dispatch all parallel calls first (non-blocking) so they overlap.
        refs = []
        for c in range(N_PARALLEL):
            tag = f"iter{it}_call{c}"
            bypassed = bypass is not None and bypass.is_bypassed("expensive_step", tag)
            print(f"  dispatch {tag:<14} bypass={'YES (served from cache)' if bypassed else 'no  (runs for real)'}")
            refs.append(expensive_step.chia_remote(it, c, x=it * 10 + c, _chia_tag=tag))

        # Resolve each ref. (Cached calls return an ObjectRefCallback that fires
        # the write on get(), so resolve them one at a time, not as a list.)
        results = [get(r) for r in refs]
        end_time = time.time()
        print(f"  -> iter{it} results={results}  (start_time: {start_time:.2f}s, duration: {end_time - start_time:.2f}s)")



def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--flush-cache", action="store_true",
                        help="Clear the cache before running (reset to a cold "
                             "cache so Run 1's all-real behavior is observed).")
    args = parser.parse_args()

    ray.init(ignore_reinit_error=True)

    cache_dir = os.path.join(os.path.dirname(__file__), "out", "cache")
    config = os.path.join(os.path.dirname(__file__), "bypass_cache.yaml")
    # The cache: section of the YAML opts expensive_step into automatic caching.
    # The cache actor is detached (so a bypass provider on a worker in a
    # different Ray job can reach it); pass an explicit namespace so that
    # cross-job lookup resolves.
    cache = start_cache(size=64,
                        units="MB",
                        cache_dir_path=cache_dir,
                        yaml_path=config)
    print(f"cache dir: {cache_dir}")

    if args.flush_cache:
        get(cache.flush.chia_remote())
        print("flushed cache (cold start)")

    warm = get(cache.size_bytes.chia_remote()) > 0
    print(f"cache is {'WARM (expect iters 1 & 3 served instantly)' if warm else 'COLD (expect all 16 to run for real)'}")

    Bypass(yaml_path=config)
    get_active_bypass().set_provider("expensive_step", cache_provider)
    # Gate the bypass on an actual cache hit: on a cold cache the cond returns
    # False and iters 1 & 3 run for real (and populate the cache) instead of the
    # provider raising KeyError on a miss.
    get_active_bypass().set_cond("expensive_step", cache_hit_cond)
    run_loop("replay (regex iter[13]_.* served from cache when present; misses & 0 & 2 run for real)")

    stop_cache()
    print("\nDone. Run again (no --flush-cache) to see iters 1 & 3 served from "
          "the cache instantly; only iters 0 & 2 (and any cold-cache misses) "
          "print [REAL RUN] and take ~0.5s/iter.")
    
    ray.shutdown()


if __name__ == "__main__":
    main()
