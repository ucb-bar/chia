"""Detached, head-node-pinned Ray actor that stores arbitrary key/value
pairs for the cluster to read and write live.

Use this whenever a pipeline needs a small, operator-adjustable piece of
shared state — e.g. concurrency knobs, feature flags, or rate limits —
without rebuilding the detached-actor scaffolding each time.

Because the actor is ``lifetime="detached"``, its values persist across
pipeline restarts. Any Ray client on the cluster can look it up by name
and namespace::

    from chia.base.chia_kv_store import get_or_create_kv_store, query_kv

    handle = get_or_create_kv_store(
        name="my_pipeline_knobs", namespace="my_pipeline",
        num_parallel=2, stage="warmup",
    )
    n = query_kv(handle, "num_parallel", default=2)

An operator can adjust values live from any machine with Ray access::

    python -m chia.base.chia_kv_store \\
        --name my_pipeline_knobs --namespace my_pipeline --set num_parallel 4
    python -m chia.base.chia_kv_store \\
        --name my_pipeline_knobs --namespace my_pipeline --get num_parallel
"""

from __future__ import annotations

import argparse
import ast
from typing import Any, Dict

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


# ---------------------------------------------------------------------------
# Actor
# ---------------------------------------------------------------------------

@ray.remote(num_cpus=0)
class ChiaKVStore:
    """Holds arbitrary key/value pairs in memory.

    Initial values are passed as kwargs to ``__init__`` and stored as a
    dict. Created with ``lifetime="detached"`` by
    :func:`get_or_create_kv_store`, so values survive pipeline restarts.
    """

    def __init__(self, **kwargs: Any) -> None:
        self._data: Dict[str, Any] = dict(kwargs)

    def get(self, key: str, default: Any = None) -> Any:
        """Return ``self._data[key]`` if present, else ``default``."""
        return self._data.get(key, default)

    def set(self, key: str, value: Any) -> Any:
        """Store ``value`` under ``key`` and return it."""
        self._data[key] = value
        return value

    def items(self) -> Dict[str, Any]:
        """Return a shallow copy of the full store — useful for debugging."""
        return dict(self._data)


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def get_or_create_kv_store(
    name: str,
    namespace: str,
    **defaults: Any,
) -> "ray.actor.ActorHandle":
    """Attach to the detached actor if one already exists, otherwise
    create it pinned to the current Ray node.

    The actor is scheduled with ``NodeAffinitySchedulingStrategy`` bound
    to whatever node id ``ray.get_runtime_context().get_node_id()``
    returns at call time. When invoked from a pipeline driver on the
    head node, that pins the actor to the head.

    Args:
        name: Ray actor name.
        namespace: Ray actor namespace.
        **defaults: Initial values used only when creating a fresh
            actor. Ignored when attaching to an existing actor.

    Returns:
        A Ray ActorHandle to the ChiaKVStore.
    """
    try:
        return ray.get_actor(name, namespace=namespace)
    except ValueError:
        head_node_id = ray.get_runtime_context().get_node_id()
        return ChiaKVStore.options(
            name=name,
            namespace=namespace,
            lifetime="detached",
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=head_node_id, soft=False),
        ).remote(**defaults)


def query_kv(
    handle,
    key: str,
    default: Any = None,
    timeout: float = 60.0,
) -> Any:
    """Read ``key`` from the actor with a hard timeout.

    Intended for hot loops that should never wedge on a broken actor:
    any exception (actor dead, timeout, serialization error) or a
    ``None`` handle falls back to ``default``.

    Args:
        handle: ActorHandle returned by
            :func:`get_or_create_kv_store`. May be ``None`` — treated
            the same as an unreachable actor.
        key: Key to read.
        default: Value returned on any error, timeout, or missing key.
        timeout: Hard timeout in seconds for the underlying ``ray.get``.

    Returns:
        The stored value, or ``default`` on any failure.
    """
    if handle is None:
        return default
    try:
        return ray.get(handle.get.remote(key, default), timeout=timeout)
    except Exception:
        # Intentionally broad: any timeout/actor-dead/serialization/bug-in-
        # actor issue should behave identically — fall back to the default.
        return default


# ---------------------------------------------------------------------------
# CLI — operator knob to inspect or adjust values live
# ---------------------------------------------------------------------------

def _parse_value(raw: str) -> Any:
    """Best-effort parse of a CLI-supplied value.

    Tries ``ast.literal_eval`` first so ``"4"`` becomes ``int(4)`` and
    ``"[1,2]"`` becomes ``list``, then falls back to the raw string
    (e.g. ``"hello"`` stays ``"hello"``).
    """
    try:
        return ast.literal_eval(raw)
    except (ValueError, SyntaxError):
        return raw


def _cli() -> None:
    """Small CLI to read or set values on a detached ChiaKVStore.

    Usage::

        python -m chia.base.chia_kv_store \\
            --name NAME --namespace NS --get KEY
        python -m chia.base.chia_kv_store \\
            --name NAME --namespace NS --set KEY VALUE
    """
    parser = argparse.ArgumentParser(
        description="Inspect or update a detached ChiaKVStore actor."
    )
    parser.add_argument("--name", required=True,
                        help="Ray actor name.")
    parser.add_argument("--namespace", required=True,
                        help="Ray actor namespace.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--get", dest="get_key", metavar="KEY",
                       help="Print the value stored under KEY.")
    group.add_argument("--set", dest="set_pair", nargs=2,
                       metavar=("KEY", "VALUE"),
                       help="Store VALUE under KEY. VALUE is parsed via "
                            "ast.literal_eval with a string fallback.")
    args = parser.parse_args()

    if not ray.is_initialized():
        ray.init(address="auto")

    handle = get_or_create_kv_store(name=args.name, namespace=args.namespace)

    if args.get_key is not None:
        print(ray.get(handle.get.remote(args.get_key)))
        return

    key, raw_value = args.set_pair
    value = _parse_value(raw_value)
    stored = ray.get(handle.set.remote(key, value))
    print(f"{key} = {stored!r}")


if __name__ == "__main__":
    _cli()
