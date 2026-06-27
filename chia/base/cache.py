"""Head-node output cache for ChiaFunction tasks.

A small key/value store, pinned to the head node as a Ray actor, that pickles
arbitrary Python objects keyed by a string tag, with an LRU byte budget and a
warm-start scan from disk. It has two halves that work together with the
:mod:`chia.base.bypass` mechanism:

* **Write is automatic.** A function marked ``cache: true`` in the YAML has its
  real-run output written to the cache automatically by ``ChiaFunction`` (via an
  ``ObjectRefCallback`` that fires when ``get()`` resolves the result), keyed by
  the call's ``_chia_tag``. No user code is needed.

* **Read is manual, via bypass.** The cache does *not* auto-serve. To replay a
  cached value, register a bypass provider that reads it back::

      def cache_provider(tag, data_path, *args, **kwargs):
          hit, value = ray.get(get_active_cache().read.remote(tag))
          if not hit:
              raise KeyError(f"cache miss for tag {tag!r}")
          return value

      bypass.set_provider("run_verilator_test", cache_provider)

  Cache = write-through populate; bypass = read path. They share the tag.

Usage
-----
::

    from chia.base.cache import start_cache

    # Call once on the driver after ray.init(). Idempotent.
    start_cache(size=4, units="GB", cache_dir_path="/data/chia_cache",
                yaml_path=args.bypass_config)

YAML format (same file as bypass, parallel ``cache:`` section)
--------------------------------------------------------------
::

    cache:
      run_verilator_test:
        cache: true
        tags: ["iter.*"]   # optional; mirror bypass tag patterns

    # shorthand (cache, no tags):
    build_megaboom: true
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
from collections import OrderedDict
from pathlib import Path
from typing import Any, Optional

import ray
import yaml
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from chia.base.ChiaFunction import chia_actor, get

logger = logging.getLogger("chia.cache")

# Multipliers from a units string to bytes. KiB/MiB/... are aliases for the
# binary KB/MB/... used here (everything is powers of 1024).
UNITS = {
    "B": 1,
    "KB": 1024,
    "MB": 1024 ** 2,
    "GB": 1024 ** 3,
    "TB": 1024 ** 4,
    "KIB": 1024,
    "MIB": 1024 ** 2,
    "GIB": 1024 ** 3,
    "TIB": 1024 ** 4,
}

# Name used to register / look up the cache actor in Ray. The actor is the
# single source of truth: every process resolves it fresh via ray.get_actor
# (see get_active_cache), so there is no process-local handle or config to go
# stale when the cache is restarted or replaced.
_CACHE_ACTOR_NAME = "ChiaCacheStore"


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------

def _dumps(obj: Any) -> bytes:
    """Serialize *obj* with Ray's vendored ``ray.cloudpickle``.

    cloudpickle is a strict superset of stdlib pickle — it also handles
    lambdas, closures, and locally-defined classes — so we use it for
    everything. ``ray.cloudpickle`` is always importable (Ray is a hard
    dependency); the standalone ``cloudpickle`` package may not be installed.
    """
    from ray import cloudpickle
    return cloudpickle.dumps(obj)


def _loads(blob: bytes) -> Any:
    from ray import cloudpickle
    return cloudpickle.loads(blob)


def _tag_filename(tag: str) -> str:
    """Deterministic, filesystem-safe filename for *tag*.

    A readable slug plus a hash suffix so distinct tags that slug to the same
    string don't collide. Deterministic across runs so the warm-start scan and
    later writes address the same file.
    """
    slug = re.sub(r"[^A-Za-z0-9._-]", "_", tag)[:80]
    digest = hashlib.sha256(tag.encode("utf-8")).hexdigest()[:16]
    return f"{slug}_{digest}.pkl"


# ---------------------------------------------------------------------------
# The cache actor
# ---------------------------------------------------------------------------

@ray.remote(num_cpus=0)
class Cache:
    """Head-pinned LRU object cache backed by pickle files on disk.

    Single-threaded by design (default Ray actor concurrency): every method is
    atomic, so the LRU index, eviction, and flush never race even when many
    workers write concurrently. Do NOT make methods async or raise
    ``max_concurrency``.

    Each pickle on disk stores ``(tag, data)`` so a cold actor can recover the
    raw tag (the index key) during the warm-start scan.
    """

    def __init__(self, cache_dir_path: str, budget_bytes: int, config: dict):
        self._dir = Path(cache_dir_path)
        self._budget = int(budget_bytes)
        self._config = dict(config or {})
        # raw_tag -> (filename, nbytes); insertion/access order == LRU order
        # (front = least recently used, back = most recently used).
        self._index: "OrderedDict[str, tuple[str, int]]" = OrderedDict()
        self._size_bytes = 0

        self._dir.mkdir(parents=True, exist_ok=True)
        self._warm_start()
        logger.info(
            "Cache started: dir=%s budget=%d bytes, warm-started %d entries (%d bytes)",
            self._dir, self._budget, len(self._index), self._size_bytes,
        )

    # ------------------------------------------------------------------
    # Warm start
    # ------------------------------------------------------------------

    def _warm_start(self) -> None:
        """Rebuild the index from ``*.pkl`` files already on disk.

        Seeds LRU order by file mtime (oldest first) so the existing on-disk
        recency is approximately preserved across driver restarts.
        """
        files = sorted(self._dir.glob("*.pkl"), key=lambda p: p.stat().st_mtime)
        for path in files:
            try:
                tag, _data = _loads(path.read_bytes())
            except Exception:  # noqa: BLE001 — skip corrupt/foreign files
                logger.warning("Cache: skipping unreadable file %s", path)
                continue
            nbytes = path.stat().st_size
            # Drop a stale entry for the same tag if the deterministic filename
            # differs (shouldn't happen, but keep the index consistent).
            if tag in self._index:
                old_name, old_nbytes = self._index.pop(tag)
                self._size_bytes -= old_nbytes
            self._index[tag] = (path.name, nbytes)
            self._size_bytes += nbytes

    # ------------------------------------------------------------------
    # Storage API
    # ------------------------------------------------------------------

    def write(self, tag: str, data: Any) -> bool:
        """Pickle ``(tag, data)`` to disk under *tag*, evicting LRU as needed.

        Returns True if stored, False if skipped (a single item larger than the
        whole budget is never cached).
        """
        blob = _dumps((tag, data))
        nbytes = len(blob)

        if nbytes > self._budget:
            logger.warning(
                "Cache: item %r is %d bytes > budget %d; not caching",
                tag, nbytes, self._budget,
            )
            return False

        # Overwrite: remove the old accounting first so eviction math is correct.
        if tag in self._index:
            old_name, old_nbytes = self._index.pop(tag)
            self._size_bytes -= old_nbytes

        # Evict least-recently-used entries until the new item fits.
        while self._size_bytes + nbytes > self._budget and self._index:
            victim_tag, (victim_name, victim_nbytes) = self._index.popitem(last=False)
            self._size_bytes -= victim_nbytes
            self._unlink(victim_name)

        filename = _tag_filename(tag)
        (self._dir / filename).write_bytes(blob)
        self._index[tag] = (filename, nbytes)
        self._index.move_to_end(tag)  # mark MRU
        self._size_bytes += nbytes
        return True

    def read(self, tag: str) -> tuple[bool, Any]:
        """Return ``(True, value)`` on hit (and mark MRU), else ``(False, None)``.

        A single round-trip so callers never race a has()/read() pair against a
        concurrent eviction or flush.
        """
        entry = self._index.get(tag)
        if entry is None:
            return (False, None)
        filename, _nbytes = entry
        path = self._dir / filename
        try:
            _tag, data = _loads(path.read_bytes())
        except Exception:  # noqa: BLE001 — file vanished or corrupt → treat as miss
            self._size_bytes -= self._index.pop(tag, (None, 0))[1]
            return (False, None)
        self._index.move_to_end(tag)
        return (True, data)

    def has(self, tag: str) -> bool:
        return tag in self._index

    def evict(self, tag: str) -> bool:
        """Delete the entry for *tag*. Returns True if something was removed."""
        entry = self._index.pop(tag, None)
        if entry is None:
            return False
        filename, nbytes = entry
        self._size_bytes -= nbytes
        self._unlink(filename)
        return True

    def flush(self) -> None:
        """Clear ALL entries: delete every pickle and reset the index."""
        for filename, _nbytes in self._index.values():
            self._unlink(filename)
        self._index.clear()
        self._size_bytes = 0

    def keys(self) -> list[str]:
        return list(self._index.keys())

    def size_bytes(self) -> int:
        return self._size_bytes

    def get_config(self) -> dict:
        """Return the static ``cache:`` config (for the local is_cached() check)."""
        return dict(self._config)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _unlink(self, filename: str) -> None:
        try:
            (self._dir / filename).unlink()
        except FileNotFoundError:
            pass


# ---------------------------------------------------------------------------
# Module functions
# ---------------------------------------------------------------------------

def _load_cache_config(yaml_path: str) -> dict:
    """Parse the ``cache:`` section of *yaml_path* into ``{func: True | [tags]}``.

    Mirrors :meth:`chia.base.bypass.Bypass._load_yaml` but for the ``cache:``
    key. Functions with ``cache: false`` (or absent) are omitted.
    """
    with open(yaml_path) as f:
        cfg = yaml.safe_load(f) or {}

    config: dict = {}
    for func_name, spec in cfg.get("cache", {}).items():
        if isinstance(spec, bool):
            cache_on, tags = spec, None
        elif isinstance(spec, dict):
            cache_on = spec.get("cache", False)
            tags = spec.get("tags")
        else:
            continue

        if not cache_on:
            continue
        if tags is not None:
            if isinstance(tags, str):
                tags = [tags]
            config[func_name] = tags
        else:
            config[func_name] = True
        logger.info("Cache: %s tags=%s", func_name, tags)
    return config


def start_cache(
    size: float,
    cache_dir_path: str,
    units: str = "B",
    yaml_path: Optional[str] = None,
    namespace: Optional[str] = None,
):
    """Create (or find) the head-pinned cache actor. Idempotent.

    Call from the driver after ``ray.init()``. Mirrors
    :func:`chia.trace.profiler.start_collector`.

    The actor is created ``detached`` so it is reachable from workers in a
    *different* Ray job (e.g. a bypass provider running on a remote worker) — a
    plain named actor is only visible within its creating job. Pair it with an
    explicit ``namespace`` so cross-job lookups resolve it. ``stop_cache`` still
    tears it down; cross-run reuse remains the on-disk pickles + warm start, not
    a lingering live actor.

    Args:
        size: Numeric budget; multiplied by ``UNITS[units]`` to get bytes.
        cache_dir_path: Directory for the pickle files (on the head node).
        units: One of :data:`UNITS` (default ``"B"``).
        yaml_path: Optional path to the bypass/cache YAML; the ``cache:`` section
            opts functions into automatic caching.
        namespace: Optional Ray namespace for the named actor.

    Returns:
        The ``Cache`` actor handle.
    """
    # Idempotent: if the named actor already exists, return it as-is.
    existing = get_active_cache(namespace)
    if existing is not None:
        return existing

    units_key = units.upper()
    if units_key not in UNITS:
        raise ValueError(f"start_cache: unknown units {units!r}; choose from {sorted(UNITS)}")

    config = _load_cache_config(yaml_path) if yaml_path else {}
    budget = int(size * UNITS[units_key])

    # Pin the actor to the node running the driver (where the files live).
    node_id = ray.get_runtime_context().get_node_id()
    scheduling = NodeAffinitySchedulingStrategy(node_id=node_id, soft=False)

    opts = dict(
        name=_CACHE_ACTOR_NAME,
        get_if_exists=True,
        num_cpus=0,
        scheduling_strategy=scheduling,
        # Detached so a bypass provider on a worker in a different Ray job can
        # still reach the cache by name (a plain named actor is job-scoped).
        lifetime="detached",
    )
    if namespace:
        opts["namespace"] = namespace

    handle = chia_actor(Cache.options(**opts).remote(cache_dir_path, budget, config))
    # Block until the actor is live and responding.
    get(handle.size_bytes.remote())
    return handle


def stop_cache(namespace: Optional[str] = None) -> None:
    """Kill the cache actor. Idempotent.

    Cross-run reuse comes from the on-disk pickles + warm-start scan, not from a
    persisted live actor, so killing the actor loses nothing on disk.
    """
    handle = get_active_cache(namespace)
    if handle is not None:
        try:
            # get_active_cache returns a ChiaActorHandle; ray.kill needs the raw
            # Ray handle. getattr(..., "actor", handle) also tolerates a raw one.
            ray.kill(getattr(handle, "actor", handle))
        except Exception:  # noqa: BLE001 — best-effort teardown
            pass


def get_active_cache(namespace: Optional[str] = None):
    """Return the cache actor handle, or None if no cache was started.

    Always resolves the current named actor via ``ray.get_actor`` — no
    process-local handle is cached. A worker (e.g. inside a bypass provider or a
    nested dispatch) reaches the cache with no state threading, and a cache that
    was restarted/replaced is picked up automatically rather than serving a
    stale, dead handle.
    """
    lookup_kwargs = {"namespace": namespace} if namespace else {}
    try:
        return chia_actor(ray.get_actor(_CACHE_ACTOR_NAME, **lookup_kwargs))
    except ValueError:
        return None


def is_cached(func_name: str, tag: Optional[str] = None) -> bool:
    """Should *func_name*'s output be cached on this call?

    True only when the function is configured ``cache: true`` and, if ``tags:``
    patterns are present, *tag* ``re.fullmatch``es one. Mirrors
    :meth:`chia.base.bypass.Bypass.is_bypassed`. The config is read from the
    actor (the source of truth) so it never goes stale across a cache restart;
    only tagged dispatches reach this path (see ``_maybe_wrap_cache``).
    """
    cache = get_active_cache()
    if cache is None:
        return False
    config = get(cache.get_config.remote())

    spec = config.get(func_name)
    if not spec:
        return False
    if isinstance(spec, list):
        if tag is None:
            return False
        return any(re.fullmatch(p, tag) for p in spec)
    return True
