"""Test the head-node output cache on a live Ray cluster.

Covers the storage actor and both integration halves:

  Storage (actor called directly):
    1. write / read round-trip; has present/missing
    2. evict (file gone, size drops); flush (all gone, size 0)
    3. units + LRU: exceed the byte budget -> oldest-touched victim evicted,
       most-recently-used survives, size stays <= budget
    4. oversized item (> budget): not cached, write() returns False
    5. warm start: a fresh actor over the same dir rebuilds the index from disk
    11. overwrite: re-writing a tag updates the value and size (no double-count)
    13. config parsing: shorthand `fn: true`, dict form, tags list, cache:false

  Integration (via @ChiaFunction dispatch):
    6. auto-write: a cache:true function's real output is written on get()
       (6b repeats under profiling, to prove the callback sees the unwrapped
       value, not a _ProfiledResult)
    7. provider read: a bypass provider reads the value back from the cache
       actor (hit returns the cached object; miss raises, surfaced to caller)
    8. bypass result is cached (orthogonality): a both-cache+bypass function's
       provider-produced value is itself written to the cache
    9. no-tag / not-configured: ref is a plain ObjectRef, nothing written
    10. auto-write eviction: a small cache overflowed via the auto-write path
        evicts the LRU entry; the touched entry and the newest survive
    14. tags filter: cache `tags:` patterns gate the auto-write (match vs not)

Storage tests create anonymous Cache actors directly so they don't touch the
named singleton. Integration tests call start_cache() once (the production
path) and share that one head-pinned actor.

Run:
  ray job submit --address IP:6379 --working-dir . -- \
      python -m chia.base.test.test_cache_tags
"""

import os
import sys
import tempfile
from pathlib import Path

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from chia.base.ChiaFunction import ChiaFunction, ObjectRefCallback, get
from chia.base.bypass import Bypass
from chia.base.cache import Cache, start_cache, stop_cache, get_active_cache


# Cache dirs for the tests live under this test directory's out/ folder (not
# /tmp). out/ is in the RUNTIME_ENV excludes, so it is never uploaded to
# workers; the cache actor is head-pinned, so it reads/writes here on the head.
_OUT_DIR = Path(__file__).resolve().parent / "out"
_OUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def write_yaml(content: str) -> str:
    fd, path = tempfile.mkstemp(suffix=".yaml")
    with os.fdopen(fd, "w") as f:
        f.write(content)
    return path


def assert_eq(name, actual, expected):
    if actual == expected:
        print(f"  PASS: {name} = {actual!r}")
    else:
        print(f"  FAIL: {name} = {actual!r}, expected {expected!r}")
        sys.exit(1)


def assert_true(name, cond):
    assert_eq(name, bool(cond), True)


# ---------------------------------------------------------------------------
# Test ChiaFunctions and providers (module-level so they serialize to workers)
# ---------------------------------------------------------------------------

@ChiaFunction()
def slow_fn(x: int) -> int:
    return x * x


@ChiaFunction()
def mock_fn(x: int) -> int:
    return x + 1000  # never runs in test 8 (bypassed) — provider returns a sentinel


@ChiaFunction()
def plain_fn(x: int) -> int:
    return x + 1


@ChiaFunction()
def blob_fn(n: int) -> str:
    """Returns an n-byte payload; used to overflow a small cache budget."""
    return "x" * n


def cache_provider(tag, data_path, *args, **kwargs):
    """Bypass provider that serves from the cache (the manual read path)."""
    hit, value = ray.get(get_active_cache().read.remote(tag))
    if not hit:
        raise KeyError(f"cache miss for tag {tag!r}")
    return value


def sentinel_provider(tag, data_path, *args, **kwargs):
    """Bypass provider that returns a constant NOT read from the cache."""
    return "SENTINEL"


def cache_has_cond(tag, data_path, *args, **kwargs):
    """Bypass condition: only bypass when *tag* is present in the cache.

    Mirrors the bypass_cache example. On a miss it returns False so the call
    falls through to a real run (which auto-writes) instead of the provider
    raising on a cold cache.
    """
    return ray.get(get_active_cache().has.remote(tag))


# ---------------------------------------------------------------------------
# Storage tests (direct actor calls)
# ---------------------------------------------------------------------------

def _new_store(budget_bytes, config=None):
    """Anonymous Cache actor in a fresh temp dir. Returns (handle, dir).

    Pinned to the driver (head) node, where _OUT_DIR lives — /scratch is not
    mounted on worker nodes, and in production the cache is always head-pinned.
    """
    d = tempfile.mkdtemp(prefix="chia_cache_test_", dir=str(_OUT_DIR))
    pin = NodeAffinitySchedulingStrategy(
        node_id=ray.get_runtime_context().get_node_id(), soft=False)
    return Cache.options(scheduling_strategy=pin).remote(d, budget_bytes, config or {}), d


def test_1_write_read_has():
    print("\n=== Test 1: write / read / has ===")
    c, _ = _new_store(1024 * 1024)
    assert_eq("write returns True", ray.get(c.write.remote("k", {"v": 42})), True)
    assert_eq("read hit", ray.get(c.read.remote("k")), (True, {"v": 42}))
    assert_eq("read miss", ray.get(c.read.remote("nope")), (False, None))
    assert_eq("has present", ray.get(c.has.remote("k")), True)
    assert_eq("has missing", ray.get(c.has.remote("nope")), False)
    ray.kill(c)


def test_2_evict_flush():
    print("\n=== Test 2: evict / flush ===")
    c, _ = _new_store(1024 * 1024)
    ray.get(c.write.remote("a", "aaa"))
    ray.get(c.write.remote("b", "bbb"))
    assert_eq("evict returns True", ray.get(c.evict.remote("a")), True)
    assert_eq("evicted gone", ray.get(c.has.remote("a")), False)
    assert_eq("other remains", ray.get(c.has.remote("b")), True)
    assert_eq("evict missing False", ray.get(c.evict.remote("a")), False)
    ray.get(c.flush.remote())
    assert_eq("flush clears all", ray.get(c.keys.remote()), [])
    assert_eq("flush resets size", ray.get(c.size_bytes.remote()), 0)
    ray.kill(c)


def test_3_units_and_lru():
    print("\n=== Test 3: units (1 KB) + LRU eviction with recency ===")
    # 1 KB budget; ~400-byte payloads => two fit, a third forces an eviction.
    c, _ = _new_store(1 * 1024)
    payload = "x" * 400
    assert_eq("write k0", ray.get(c.write.remote("k0", payload)), True)
    assert_eq("write k1", ray.get(c.write.remote("k1", payload)), True)
    # Touch k0 so k1 becomes the least-recently-used entry.
    ray.get(c.read.remote("k0"))
    assert_eq("write k2 (forces eviction)", ray.get(c.write.remote("k2", payload)), True)
    assert_eq("k1 evicted (was LRU)", ray.get(c.has.remote("k1")), False)
    assert_eq("k0 survives (was touched)", ray.get(c.has.remote("k0")), True)
    assert_eq("k2 present (newest)", ray.get(c.has.remote("k2")), True)
    assert_true("size within budget", ray.get(c.size_bytes.remote()) <= 1024)
    ray.kill(c)


def test_4_oversized_skipped():
    print("\n=== Test 4: oversized item skipped ===")
    c, _ = _new_store(100)  # 100-byte budget
    assert_eq("oversized write returns False",
              ray.get(c.write.remote("big", "x" * 500)), False)
    assert_eq("oversized not stored", ray.get(c.has.remote("big")), False)
    ray.kill(c)


def test_5_warm_start():
    print("\n=== Test 5: warm start from disk ===")
    # Pin both actors to the driver node so they share the same local dir
    # (mirrors production, where the cache is always head-pinned). Without this
    # the two anonymous actors could land on different nodes with no shared FS.
    node_id = ray.get_runtime_context().get_node_id()
    pin = NodeAffinitySchedulingStrategy(node_id=node_id, soft=False)
    d = tempfile.mkdtemp(prefix="chia_cache_warm_", dir=str(_OUT_DIR))
    c1 = Cache.options(scheduling_strategy=pin).remote(d, 1024 * 1024, {})
    ray.get(c1.write.remote("persisted", {"deep": [1, 2, 3]}))
    ray.kill(c1)  # drop the live actor; data stays on disk
    c2 = Cache.options(scheduling_strategy=pin).remote(d, 1024 * 1024, {})  # fresh actor scans the same dir
    assert_eq("warm-started has", ray.get(c2.has.remote("persisted")), True)
    assert_eq("warm-started read", ray.get(c2.read.remote("persisted")),
              (True, {"deep": [1, 2, 3]}))
    assert_true("warm-started size > 0", ray.get(c2.size_bytes.remote()) > 0)
    ray.kill(c2)


def test_11_overwrite():
    print("\n=== Test 11: overwrite same tag updates value and size ===")
    c, _ = _new_store(1024 * 1024)
    ray.get(c.write.remote("k", "small"))
    size1 = ray.get(c.size_bytes.remote())
    big = "a much longer value " * 5
    ray.get(c.write.remote("k", big))
    size2 = ray.get(c.size_bytes.remote())
    assert_eq("read returns new value", ray.get(c.read.remote("k")), (True, big))
    assert_eq("still a single entry", ray.get(c.keys.remote()), ["k"])
    assert_true("size grew to new value (not double-counted)", size1 < size2)
    # Re-writing the identical value must not change the accounted size.
    ray.get(c.write.remote("k", big))
    assert_eq("re-write same value keeps size stable",
              ray.get(c.size_bytes.remote()), size2)
    ray.kill(c)


def test_13_config_parsing():
    print("\n=== Test 13: cache YAML parsing (shorthand / dict / tags / disabled) ===")
    from chia.base.cache import _load_cache_config
    yaml_text = """
cache:
  shorthand_fn: true
  dict_fn:
    cache: true
  tagged_fn:
    cache: true
    tags: ["iter.*", "warmup"]
  disabled_fn:
    cache: false
"""
    cfg = _load_cache_config(write_yaml(yaml_text))
    assert_eq("shorthand `fn: true` -> True", cfg.get("shorthand_fn"), True)
    assert_eq("dict form -> True", cfg.get("dict_fn"), True)
    assert_eq("tags -> pattern list", cfg.get("tagged_fn"), ["iter.*", "warmup"])
    assert_true("cache:false omitted from config", "disabled_fn" not in cfg)


def test_16_chia_actor_handle():
    """The generic chia_actor() wrapper gives an actor the chia_remote()/get()
    surface, keeps .remote() working, exposes the raw handle via .actor, and is
    idempotent + serializable (so it can be passed into a ChiaFunction)."""
    print("\n=== Test 16: chia_actor handle (.chia_remote / get / .actor) ===")
    from chia.base.ChiaFunction import chia_actor, ChiaActorHandle
    raw, _ = _new_store(1024 * 1024)
    h = chia_actor(raw)
    assert_true("wraps to ChiaActorHandle", isinstance(h, ChiaActorHandle))
    assert_true("idempotent", chia_actor(h) is h)
    # .chia_remote dispatches; get() resolves — same surface as a ChiaFunction.
    assert_eq("write via chia_remote", get(h.write.chia_remote("k", {"v": 1})), True)
    assert_eq("read via chia_remote", get(h.read.chia_remote("k")), (True, {"v": 1}))
    # .remote() still works on the wrapper (existing call sites unaffected).
    assert_eq("remote() still works", ray.get(h.has.remote("k")), True)
    # The wrapper survives a round-trip to a worker (it is passed as a task arg).
    assert_eq("survives pickling to a worker",
              ray.get(_read_via_handle.remote(h, "k")), (True, {"v": 1}))
    # .actor recovers the raw handle (needed for ray.kill / identity).
    ray.kill(h.actor)


@ray.remote
def _read_via_handle(handle, tag):
    """Runs on a worker: proves a ChiaActorHandle deserializes and still works."""
    return get(handle.read.chia_remote(tag))


# ---------------------------------------------------------------------------
# Integration tests (one shared head-pinned cache via start_cache)
# ---------------------------------------------------------------------------

CACHE_YAML = """
cache:
  slow_fn:
    cache: true
  mock_fn:
    cache: true
  blob_fn:
    cache: true
"""

BYPASS_SLOW = """
bypass:
  slow_fn:
    bypass: true
"""

BYPASS_MOCK = """
bypass:
  mock_fn:
    bypass: true
"""

TAGS_CACHE_YAML = """
cache:
  slow_fn:
    cache: true
    tags: ["iter.*"]
"""

@ChiaFunction()
def test_6_auto_write(cache):
    print("\n=== Test 6: auto-write on get() (profiler off) ===")
    Bypass()  # active bypass with nothing bypassed -> slow_fn runs for real
    result = get(slow_fn.chia_remote(7, _chia_tag="t1"))
    assert_eq("real result", result, 49)
    assert_eq("auto-written to cache", ray.get(cache.has.remote("t1")), True)
    assert_eq("cached value", ray.get(cache.read.remote("t1")), (True, 49))

@ChiaFunction()
def test_6b_auto_write_profiled(cache):
    print("\n=== Test 6b: auto-write on get() (profiler ON) ===")
    from chia.trace.profiler import start_collector, stop_collector, get_profiler
    log_dir = tempfile.mkdtemp(prefix="chia_prof_")
    start_collector(log_dir=log_dir)
    get_profiler.reset()  # rebuild the singleton now that the collector exists
    try:
        Bypass()
        # Callback must receive the unwrapped value (49), not a _ProfiledResult.
        result = get(slow_fn.chia_remote(8, _chia_tag="t1p"))
        assert_eq("real result (profiled)", result, 64)
        assert_eq("auto-written (profiled)", ray.get(cache.read.remote("t1p")),
                  (True, 64))
    finally:
        stop_collector()
        get_profiler.reset()

@ChiaFunction()
def test_7_provider_read(cache):
    print("\n=== Test 7: bypass provider reads from cache ===")
    bypass = Bypass(yaml_path=write_yaml(BYPASS_SLOW))
    bypass.set_provider("slow_fn", cache_provider)
    # Hit: "t1" was written in test 6; provider serves it (no real run).
    served = get(slow_fn.chia_remote(999, _chia_tag="t1"))
    assert_eq("provider served cached value", served, 49)
    # Miss: provider raises KeyError, surfaced through ray.get.
    try:
        get(slow_fn.chia_remote(999, _chia_tag="t_missing"))
        print("  FAIL: expected a cache-miss error")
        sys.exit(1)
    except Exception as e:  # noqa: BLE001 — RayTaskError wrapping KeyError
        assert_true("cache miss raised", "cache miss" in str(e))

@ChiaFunction()
def test_8_bypass_result_cached(cache):
    print("\n=== Test 8: bypassed result is itself cached (orthogonality) ===")
    bypass = Bypass(yaml_path=write_yaml(BYPASS_MOCK))
    bypass.set_provider("mock_fn", sentinel_provider)
    assert_eq("b1 absent before", ray.get(cache.has.remote("b1")), False)
    result = get(mock_fn.chia_remote(5, _chia_tag="b1"))
    assert_eq("provider sentinel returned", result, "SENTINEL")
    assert_eq("bypassed result was cached", ray.get(cache.read.remote("b1")),
              (True, "SENTINEL"))

@ChiaFunction()
def test_15_bypass_cond_cache_hit(cache):
    """End-to-end cache-hit condition (the bypass_cache example pattern).

    slow_fn is both cache:true and bypass:true with a cache-hit cond. The first
    call to a fresh tag misses -> cond False -> real run (auto-written). The
    second call to the same tag hits -> cond True -> served from the cache by
    the provider (not recomputed)."""
    print("\n=== Test 15: bypass cond gates on cache hit (miss->real, hit->served) ===")
    bypass = Bypass(yaml_path=write_yaml(BYPASS_SLOW))
    bypass.set_provider("slow_fn", cache_provider)
    bypass.set_cond("slow_fn", cache_has_cond)

    assert_eq("cond1 absent before", ray.get(cache.has.remote("cond1")), False)
    # Cold: cond False -> real run (5*5) -> auto-written under "cond1".
    first = get(slow_fn.chia_remote(5, _chia_tag="cond1"))
    assert_eq("cold miss runs real", first, 25)
    assert_eq("real result auto-cached", ray.get(cache.has.remote("cond1")), True)
    # Warm: cond True -> provider serves the cached 25, ignoring the new arg.
    served = get(slow_fn.chia_remote(999, _chia_tag="cond1"))
    assert_eq("warm hit served from cache (not 999^2)", served, 25)

@ChiaFunction()
def test_9_no_tag_or_unconfigured(cache):
    print("\n=== Test 9: no tag / not configured -> not wrapped, nothing written ===")
    Bypass()
    # No _chia_tag -> plain ObjectRef, no wrapping.
    ref = slow_fn.chia_remote(3)
    assert_true("no-tag ref is plain ObjectRef", not isinstance(ref, ObjectRefCallback))
    assert_eq("no-tag result", get(ref), 9)
    # plain_fn not in the cache config -> not wrapped even with a tag.
    ref2 = plain_fn.chia_remote(3, _chia_tag="u1")
    assert_true("unconfigured ref is plain ObjectRef", not isinstance(ref2, ObjectRefCallback))
    assert_eq("unconfigured result", get(ref2), 4)
    assert_eq("unconfigured not cached", ray.get(cache.has.remote("u1")), False)

@ChiaFunction()
def test_10_auto_write_eviction(cache):
    """Capacity reached through the auto-write path -> LRU eviction.

    Runs driver-side with its own small cache: get() fires the auto-write
    callback on the driver, so it writes to this freshly-started cache rather
    than a worker's memoized handle from the earlier (now-stopped) shared cache.
    """
    print("\n=== Test 10: capacity reached -> LRU eviction via auto-write ===")
    Bypass()  # nothing bypassed -> blob_fn runs for real and auto-caches on get()
    n = 400   # ~415 bytes pickled: two fit in 1 KB, a third forces an eviction
    assert_eq("e0 auto-cached", get(blob_fn.chia_remote(n, _chia_tag="e0")), "x" * n)
    assert_eq("e1 auto-cached", get(blob_fn.chia_remote(n, _chia_tag="e1")), "x" * n)
    # Touch e0 so e1 becomes the least-recently-used entry.
    ray.get(cache.read.remote("e0"))
    assert_eq("e2 auto-cached (overflows budget)",
              get(blob_fn.chia_remote(n, _chia_tag="e2")), "x" * n)
    assert_eq("e1 evicted (was LRU)", ray.get(cache.has.remote("e1")), False)
    assert_eq("e0 survives (touched)", ray.get(cache.has.remote("e0")), True)
    assert_eq("e2 present (newest)", ray.get(cache.has.remote("e2")), True)
    assert_true("size within budget", ray.get(cache.size_bytes.remote()) <= 1024)

@ChiaFunction()
def test_14_tags_filter(cache):
    """Cache `tags:` patterns gate the auto-write, just like bypass tag filters.

    Runs driver-side so the wrap/no-wrap decision (is_cached) and the auto-write
    callback both use this cache's config (slow_fn: tags ["iter.*"]).
    """
    print("\n=== Test 14: cache tags pattern filtering (auto-write) ===")
    Bypass()  # nothing bypassed -> slow_fn runs for real
    # Matching tag -> wrapped and auto-cached.
    matched = slow_fn.chia_remote(6, _chia_tag="iter0")
    assert_true("matching tag wrapped", isinstance(matched, ObjectRefCallback))
    assert_eq("matching tag result", get(matched), 36)
    assert_eq("matching tag cached", ray.get(cache.has.remote("iter0")), True)
    # Non-matching tag -> not wrapped, not cached.
    other = slow_fn.chia_remote(7, _chia_tag="other")
    assert_true("non-matching tag not wrapped", not isinstance(other, ObjectRefCallback))
    assert_eq("non-matching result", get(other), 49)
    assert_eq("non-matching not cached", ray.get(cache.has.remote("other")), False)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    _CHIA_ROOT = Path(__file__).resolve().parents[2]
    RUNTIME_ENV = {
        # Ship the checkout as the job's working_dir, and the packages the
        # pipeline's ChiaFunctions are pickled against (chia.*, common.*,
        # core_ipc_opt.*) as py_modules so they are importable top-level on
        # every worker — the repo-root working_dir alone does not put
        # examples/ on the workers' sys.path.
        "working_dir": str(_CHIA_ROOT),
        "py_modules": [
            str(_CHIA_ROOT),
        ],
        # gitignore-style patterns, applied to the working_dir and py_modules
        # uploads alike. benchmarks/ (~110M of test binaries — the driver reads
        # them from UBENCH_DIR, workers receive contents by value), out/ (per-run
        # outputs, LOG_DIR default) and caches dominate the upload size.
        "excludes": [
            "/.claude/",
            "__pycache__",
            ".mypy_cache",
            "out/"
        ],
    }
    ray.init(address="auto", runtime_env=RUNTIME_ENV)
    print("Connected to Ray cluster")

    # Storage / unit tests (anonymous actors)
    test_1_write_read_has()
    test_2_evict_flush()
    test_3_units_and_lru()
    test_4_oversized_skipped()
    test_5_warm_start()
    test_11_overwrite()
    test_13_config_parsing()
    test_16_chia_actor_handle()

    # Integration tests (one shared, head-pinned, named cache actor)
    cache_dir = tempfile.mkdtemp(prefix="chia_cache_integ_", dir=str(_OUT_DIR))
    cache = start_cache(size=4, units="MB", cache_dir_path=cache_dir,
                        yaml_path=write_yaml(CACHE_YAML))
    try:
        get(test_6_auto_write.chia_remote(cache)) # run as ChiaFunction to test the callback wrapping
        get(test_6b_auto_write_profiled.chia_remote(cache))
        get(test_7_provider_read.chia_remote(cache))
        get(test_8_bypass_result_cached.chia_remote(cache))
        get(test_9_no_tag_or_unconfigured.chia_remote(cache))
        get(test_15_bypass_cond_cache_hit.chia_remote(cache))
    finally:
        stop_cache()

    # Eviction through the auto-write path: a small (1 KB) cache that overflows.
    evict_dir = tempfile.mkdtemp(prefix="chia_cache_evict_", dir=str(_OUT_DIR))
    evict_cache = start_cache(size=1, units="KB", cache_dir_path=evict_dir,
                              yaml_path=write_yaml(CACHE_YAML))
    try:
        get(test_10_auto_write_eviction.chia_remote(evict_cache))
    finally:
        stop_cache()

    # Tag-pattern filtering of the auto-write: its own cache (slow_fn tags).
    tags_dir = tempfile.mkdtemp(prefix="chia_cache_tags_", dir=str(_OUT_DIR))
    tags_cache = start_cache(size=4, units="MB", cache_dir_path=tags_dir,
                             yaml_path=write_yaml(TAGS_CACHE_YAML))
    try:
        get(test_14_tags_filter.chia_remote(tags_cache))
    finally:
        stop_cache()

    print("\n=== ALL TESTS PASSED ===")
