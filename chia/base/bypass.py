"""General-purpose bypass for ChiaFunction tasks.

When bypass is active, a function is still dispatched through Ray (testing
cluster scheduling, placement groups, resource allocation) but the real
computation is replaced with pre-recorded data.

How it works
------------
The bypass check happens on the caller side (in ``chia_remote()``).
When a function is bypassed:

  1. The caller computes the mock data using the registered provider.
  2. Instead of dispatching the real function, it dispatches a trivial
     ``_bypass_return(data)`` function with the **same scheduling options**
     (placement group, resources, etc.).
  3. The worker receives the pre-computed data and just returns it.

Usage
-----
::

    from chia.base.bypass import Bypass

    # Create a bypass instance — always, as part of loop setup.
    # If yaml_path is None, bypass does nothing (all functions run normally).
    bypass = Bypass(yaml_path=args.bypass_config)

    # Register project-specific providers that build the correct dataclasses.
    bypass.set_provider("my_function", my_provider_fn)

    # Optionally gate the bypass on a runtime condition (default: always on).
    bypass.set_cond("my_function", my_cond_fn)

    # Done — the ChiaFunction trampoline checks the active bypass automatically.

YAML format
-----------
::

    bypass:
      func_name:
        bypass: true
        data: /path/to/file.json  # optional

      # shorthand (bypass, no data):
      simple_function: true

      # not listed = not bypassed (default, no error)

Provider signature
------------------
The provider runs on the worker (same scheduling as the real function)::

    def my_provider(tag, data_path, *args, **kwargs) -> ReturnType

- ``tag``: the ``_chia_tag`` from the caller (str or None)
- ``data_path``: the YAML ``data`` field (str or None)
- ``*args, **kwargs``: the original function arguments

Resolution when a function is bypassed::

    1. Provider registered -> provider(tag, data_path, *args, **kwargs)
    2. Only data_path      -> returns file contents (default file provider)
    3. Neither             -> error

Condition gate
--------------
A function may also register a *condition* via ``set_cond``. It runs on the
caller as the last gate in ``is_bypassed`` — after the bypass flag, the
provider/data check, and the tag patterns — and decides whether the bypass
actually happens::

    def my_cond(tag, data_path, *args, **kwargs) -> bool

A falsy return means the call runs for real instead of being bypassed. With no
condition registered the default is to bypass (``True``). Useful to gate on
runtime state, e.g. only replay from the cache when the value is present.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable, Optional

import ray
import yaml
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger("chia.bypass")

# The active Bypass instance. Set by the Bypass constructor.
# The ChiaFunction trampoline reads this via get_active_bypass().
# If None, no bypass is active and the trampoline skips the check.
_active_bypass = None  # Set by Bypass.__init__, read by get_active_bypass()


def get_active_bypass():
    """Return the active Bypass instance, or None if bypass is not in use."""
    return _active_bypass


# ---------------------------------------------------------------------------
# File server actor
# ---------------------------------------------------------------------------
#
# Bypass data files (perf_results.md, diff.json, LLM markdown) usually live
# on the node that started the loop. Workers are typically on different
# nodes and do not see those files. Routing the reads through a Ray actor
# pinned to the originating node solves this without requiring NFS.
#
# The actor is created on demand by Bypass when YAML contains any ``data:``
# fields, and pinned (soft=False) to the node that called the Bypass
# constructor. The handle is serialized into bypass_state so workers can
# reach it after _restore_bypass.
# ---------------------------------------------------------------------------


@ray.remote(num_cpus=0)
class BypassFileServer:
    """Serves file contents from the node that owns the Bypass to workers.

    Pinned via NodeAffinitySchedulingStrategy so reads happen on the node
    that has the files. Workers receive contents through Ray's object
    store. No shared filesystem required.
    """

    def get_text(self, path: str) -> str:
        return Path(path).read_text()

    def get_bytes(self, path: str) -> bytes:
        return Path(path).read_bytes()

    def exists(self, path: str) -> bool:
        return Path(path).is_file()


def _default_file_provider(tag, data_path, *args, **kwargs):
    """Default provider used when YAML has ``data`` but no provider is registered.

    Reads the file at *data_path* on the head node (via the bypass file
    server actor) and returns its contents as a string. Runs on the worker.

    The file server actor is accessible from any worker — no shared
    filesystem required. Custom providers can similarly access the actor
    via ``get_active_bypass().file_server()`` to read arbitrary files
    from the head node.

    # NOTE: this could be generalized so that any Ray actor (not just the
    # built-in BypassFileServer) can serve as a bypass data source. For
    # example, a node-local cache actor that stores intermediate results
    # keyed by tag, or a database actor. The provider signature already
    # receives tag + data_path, so a future extension could let the YAML
    # specify an actor name instead of a file path, and route through that
    # actor transparently.
    """
    if data_path is None:
        raise ValueError("Bypass: data_path is None; cannot serve default file")
    b = get_active_bypass()
    if b is None:
        raise RuntimeError("Bypass: no active Bypass instance on this worker")
    server = b.file_server()
    return ray.get(server.get_text.remote(data_path))


class Bypass:
    """Controls which ChiaFunction tasks return pre-recorded data.

    Create one instance per loop run, always, as part of setup::

        bypass = Bypass(yaml_path=args.bypass_config)

    If ``yaml_path`` is None, bypass does nothing — ``is_bypassed()``
    returns False for all functions.  No errors, no special handling.

    If ``yaml_path`` is a path, the YAML is parsed and functions listed
    with ``bypass: true`` will be bypassed (if they also have a provider
    or data path).

    The constructor registers itself as the active bypass instance.
    The ChiaFunction trampoline checks this automatically.
    """

    def __init__(self, yaml_path: str | None = None):
        self._bypass: dict[str, bool] = {}
        self._data_paths: dict[str, str] = {}
        self._providers: dict[str, Callable] = {}
        # Optional per-function condition callables. Evaluated on the caller as
        # the LAST gate in is_bypassed() — after the bypass flag, provider/data
        # check, and tag patterns. A registered cond returning falsy means the
        # call runs for real. Not set for a function -> default True (no gate).
        self._conds: dict[str, Callable] = {}
        # Optional per-function tag patterns (list of regex strings).
        # If set, only calls whose tag matches a pattern are bypassed.
        # If not set for a function, all calls are bypassed regardless of tag.
        self._tag_patterns: dict[str, list[str]] = {}
        # Ray actor that serves file contents from this node to workers.
        # Created lazily — eagerly when YAML has ``data:`` fields, or on
        # first call to ``file_server()`` from a custom provider.
        self._file_server = None

        if yaml_path is not None:
            self._load_yaml(yaml_path)
            # Eagerly start the file server if any data: paths were loaded.
            # Workers will receive the actor handle via bypass_state.
            if self._data_paths:
                self._ensure_file_server()

        # Register as the active instance
        global _active_bypass
        _active_bypass = self

    # ------------------------------------------------------------------
    # File server
    # ------------------------------------------------------------------

    def _ensure_file_server(self) -> None:
        """Create the file server actor if it doesn't exist yet.

        The actor is pinned to the node that calls this method (typically
        the node that constructed the Bypass) so it reads from that node's
        local filesystem.
        """
        if self._file_server is not None:
            return
        node_id = ray.get_runtime_context().get_node_id()
        self._file_server = BypassFileServer.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=node_id, soft=False),
        ).remote()
        logger.info("Bypass: started file server pinned to node %s", node_id)

    def file_server(self):
        """Return the bypass file server actor handle, creating it if needed.

        Custom providers can use this to read arbitrary files from the
        node that owns this Bypass::

            def my_provider(tag, data_path, *args, **kwargs):
                server = get_active_bypass().file_server()
                text = ray.get(server.get_text.remote(data_path))
                ...
        """
        self._ensure_file_server()
        return self._file_server

    # ------------------------------------------------------------------
    # Provider registration
    # ------------------------------------------------------------------

    def set_provider(self, func_name: str, provider: Callable) -> None:
        """Register a provider for *func_name*.

        The provider runs on the worker (not the head node) and replaces the
        real function when it is bypassed. It is called as::

            provider(tag, data_path, *args, **kwargs) -> return_value

        - *tag*: the ``_chia_tag`` from the caller (str or None)
        - *data_path*: the YAML ``data`` field for this function (str or None)
        - *args, **kwargs*: the original function arguments, passed through
        """
        self._providers[func_name] = provider

    def set_cond(self, func_name: str, cond: Callable) -> None:
        """Register a bypass condition for *func_name*.

        The condition is an additional gate, evaluated **on the caller** as the
        last step of :meth:`is_bypassed` — after the bypass flag, the
        provider/data check, and the tag patterns. It is called as::

            cond(tag, data_path, *args, **kwargs) -> bool

        with the same arguments as the provider (:meth:`set_provider`). If it
        returns a falsy value the call is **not** bypassed and runs for real;
        if it returns truthy the bypass proceeds. When no condition is
        registered for a function the default is ``True`` (no extra gate).

        Use it to make the bypass decision depend on runtime state, e.g. only
        replay from the cache when the value is actually present::

            def cache_hit(tag, data_path, *args, **kwargs):
                return ray.get(get_active_cache().has.remote(tag))

            bypass.set_cond("run_verilator_test", cache_hit)
        """
        self._conds[func_name] = cond

    # ------------------------------------------------------------------
    # Query and mock data retrieval
    # ------------------------------------------------------------------

    @property
    def active(self) -> bool:
        """True if any functions are configured for bypass."""
        return bool(self._bypass)

    def is_bypassed(self, func_name: str, tag: str | None = None,
                    *args, **kwargs) -> bool:
        """Should *func_name* be bypassed on this call?

        Returns True only when:
          1. bypass[name] is True (from YAML)
          2. There is a provider or a data_path
          3. (if a ``tags`` list is configured) *tag* matches a pattern
          4. (if a condition is registered) the condition returns truthy

        The *tag* parameter identifies a specific call (e.g. "iter0_opt2").
        If the YAML has a ``tags`` list for this function, only calls whose
        tag matches a pattern are bypassed.  If no ``tags`` list, all calls
        are bypassed regardless of tag.

        The optional ``*args, **kwargs`` are the original call arguments; they
        are forwarded to a registered condition (see :meth:`set_cond`). The
        condition is the **last** gate: it runs only after tag matching has
        already passed, and a falsy return means the call runs for real. With
        no condition registered the default is to bypass (``True``). A
        condition may invoke remote calls (e.g. a cache lookup), so callers
        that use :meth:`is_bypassed` as a cheap predicate should be aware it
        can do work when a condition is registered.

        Functions not listed in the YAML return False (not bypassed).
        If no YAML was loaded, always returns False.
        """
        if not self._bypass.get(func_name, False):
            return False
        if func_name not in self._providers and func_name not in self._data_paths:
            return False

        # Tag filtering: if patterns are configured, the call must have a
        # tag that matches at least one pattern.
        patterns = self._tag_patterns.get(func_name)
        if patterns is not None:
            if tag is None:
                return False
            import re
            if not any(re.fullmatch(p, tag) for p in patterns):
                return False

        # Condition gate (last): runtime check after tag matching. Default
        # (no cond registered) is to bypass.
        cond = self._conds.get(func_name)
        if cond is not None:
            data_path = self._data_paths.get(func_name)
            if not cond(tag, data_path, *args, **kwargs):
                return False

        return True

    def get_provider_info(self, func_name: str) -> tuple[Callable, str | None]:
        """Return ``(provider, data_path)`` for dispatching on the worker.

        Resolution:
          1. Provider registered -> ``(provider, data_path_or_None)``
          2. Only data_path (no provider) -> ``(_default_file_provider, data_path)``
          3. Neither -> KeyError

        The provider is executed on the worker, not here. This method only
        looks up what to dispatch.
        """
        data_path = self._data_paths.get(func_name)
        provider = self._providers.get(func_name)

        logger.info("BYPASS %s (data=%s, provider=%s)",
                     func_name, data_path, provider is not None)

        if provider is not None:
            return (provider, data_path)
        if data_path is not None:
            return (_default_file_provider, data_path)

        raise KeyError(
            f"No provider or data source for '{func_name}'. "
            f"Add a 'data' path in the YAML or call "
            f"bypass.set_provider('{func_name}', ...)."
        )

    # ------------------------------------------------------------------
    # Serialization — for propagating bypass state to Ray workers.
    # Workers are separate processes with an empty Bypass singleton.
    # The head node serializes its state, passes it as a parameter,
    # and the worker restores it so nested chia_remote() calls work.
    # ------------------------------------------------------------------

    def get_state(self) -> dict:
        """Return a serializable dict of the bypass state, including providers.

        Providers and conditions are serialized via cloudpickle (Ray's
        default). This works for module-level functions and most common cases.
        Lambdas/closures capturing unpicklable objects will fail with a clear
        error.

        The file server actor handle is included so workers can read files
        from the node that owns this Bypass.
        """
        return {
            "bypass": dict(self._bypass),
            "data_paths": dict(self._data_paths),
            "tag_patterns": dict(self._tag_patterns),
            "providers": dict(self._providers),
            "conds": dict(self._conds),
            "file_server": self._file_server,
        }

    def load_state(self, state: dict) -> None:
        """Restore bypass state from a dict (from :meth:`get_state`)."""
        self._bypass.update(state.get("bypass", {}))
        self._data_paths.update(state.get("data_paths", {}))
        self._tag_patterns.update(state.get("tag_patterns", {}))
        self._providers.update(state.get("providers", {}))
        self._conds.update(state.get("conds", {}))
        if "file_server" in state and state["file_server"] is not None:
            self._file_server = state["file_server"]

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _load_yaml(self, yaml_path: str) -> None:
        """Parse a bypass YAML and populate bypass flags and data paths."""
        with open(yaml_path) as f:
            cfg = yaml.safe_load(f) or {}

        for func_name, spec in cfg.get("bypass", {}).items():
            if isinstance(spec, bool):
                bypass, data, tags = spec, None, None
            elif isinstance(spec, dict):
                bypass = spec.get("bypass", False)
                data = spec.get("data")
                tags = spec.get("tags")
            else:
                continue

            self._bypass[func_name] = bypass
            if bypass and data is not None:
                self._data_paths[func_name] = str(data)
            if bypass and tags is not None:
                if isinstance(tags, str):
                    tags = [tags]
                self._tag_patterns[func_name] = tags
            if bypass:
                logger.info("Bypass: %s data=%s tags=%s", func_name, data, tags)
