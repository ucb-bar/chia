"""CHIA's cache read path, wired once.

The cache writes; bypass reads, and the read is manual, so every loop that
wants to replay a cached node registers a provider and a condition. Both key
off the call's ``_chia_tag``.
"""

from chia.base.ChiaFunction import get
from chia.base.bypass import Bypass, get_active_bypass
from chia.base.cache import get_active_cache, start_cache


def enable_result_cache(func_names, *, yaml_path, cache_dir, size=512,
                        units="MB", flush=False):
    """Start the cache and register the read path for each of *func_names*.

    Each name must also appear under ``cache:`` and ``bypass:`` in *yaml_path*:
    the YAML decides whether a function is cached, this decides how it is read
    back. Returns the cache handle; call ``stop_cache()`` when done.
    """
    def cache_provider(tag, data_path, *args, **kwargs):
        """Serve the cached result for *tag*; raise on a miss."""
        hit, value = get(get_active_cache().read.chia_remote(tag))
        if not hit:
            raise KeyError(f"cache miss for tag {tag!r}")
        return value

    def cache_hit_cond(tag, data_path, *args, **kwargs):
        """Only replay when *tag* is cached, so a cold run falls through to a
        real call instead of dispatching the provider against a missing key."""
        return get(get_active_cache().has.chia_remote(tag))

    cache = start_cache(size=size, units=units, cache_dir_path=cache_dir,
                        yaml_path=yaml_path)
    if flush:
        get(cache.flush.chia_remote())
    Bypass(yaml_path=yaml_path)
    bypass = get_active_bypass()
    for name in func_names:
        bypass.set_provider(name, cache_provider)
        bypass.set_cond(name, cache_hit_cond)
    return cache
