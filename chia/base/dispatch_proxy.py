"""Head-relayed task dispatch for workers on reverse-tunneled nodes.

Ray's ownership model makes the submitting worker the *owner* of a task: the
owner negotiates worker leases directly with the target raylet and serves the
result object's data. On a reverse-tunneled worker that breaks for any
task whose demands place it on a LAN node — chia's tunnels are hub-and-spoke
(tunneled worker <-> head only; see cluster/tunnel.py), LAN raylet/worker ports are random
and may be firewalled from tunneled machine, so the owner's lease RPC gets "no route to host" and
the task sits in PENDING_NODE_ASSIGNMENT forever even when capacity is free.

The fix: when ``chia_remote`` is called on a worker that cannot directly reach
the task's target node — a tunneled worker dispatching to a LAN node, OR a LAN
worker dispatching to a tunneled node (``local -> remote``) — the dispatch is
relayed through a ``DispatchProxy`` actor pinned to the head node. The proxy —
not the originating worker — owns the inner task, so every RPC leg rides a
link that already exists. The head is the only node with a bidirectional path
to every spoke, so it is the universal relay. ``should_proxy`` decides this
per-dispatch and fail-safe: it relays unless it can *prove* every node the task
could land on is directly reachable (see the reachability analysis below)::

    tunneled worker -> proxy actor call ..... pinned head worker ports (reverse tunnel + DNAT,
                                  the same path ProfileCollectorActor uses)
    proxy <- args from tunneled worker ...... head-side -L object-manager forward
    proxy -> LAN raylet lease ... plain LAN traffic
    result -> tunneled worker ............... the proxy awaits the value and returns it, so
                                  the result object lives on the head; tunneled worker
                                  fetches it via the DNAT'd head object manager

Costs and limits:

  * The head relays the data — args and results each cross the network twice.
  * ``num_returns > 1`` is not supported (falls back to direct dispatch, with
    a warning, which may hang if the task lands on a LAN node).
  * ObjectRefs nested inside proxied args are re-splatted as top-level args of
    the inner task; if such a ref is *owned by a tunneled worker*, the LAN
    executor cannot reach that owner and the inner task will hang. Pass values,
    not refs, into nested dispatches from tunneled worker-resident tasks.
  * The proxy inherits the creating job's runtime_env, so it is per-job
    (``chia_dispatch_proxy_<job_id>``) and detached; it idles at num_cpus=0
    until the cluster is torn down.
"""
from __future__ import annotations

import logging
import os
import time

import ray

logger = logging.getLogger(__name__)

# Exported by node_setup.build_worker_script ONLY on reverse-tunneled workers,
# which makes them the discriminator for "am I on a reverse tunneled node". The advertise
# host is the head's real IP (the head is never tunneled).
_TUNNEL_ENV = "CHIA_TOOL_RELAY_HOST"
_HEAD_IP_ENV = "CHIA_TOOL_ADVERTISE_HOST"

_PROXY_NAMESPACE = "chia_dispatch"
_proxy_handle = None


# ---------------------------------------------------------------------------
# Reachability analysis
#
# Relay through the head only when the task could land on a node this worker
# cannot reach directly. The rule is FAIL-SAFE: proxy unless we can *prove*
# every possible landing node is directly reachable — a wrong "don't proxy"
# hangs the task forever (PENDING_NODE_ASSIGNMENT), while a wrong "proxy" only
# costs one head relay hop.
#
# Roles (the head-as-hub tunnel topology, cluster/tunnel.py):
#   head   -> reaches every node (the hub); never relays, and the proxy actor
#             itself lives here so it must never recurse.
#   lan    -> reaches the head + other on-prem LAN nodes + itself.
#   remote -> reaches only itself directly; cross-spoke and ->LAN (and, kept
#             conservative, ->head) go through the relay.
# ---------------------------------------------------------------------------

_ROLES_TTL = 5.0
_nodes_cache: list | None = None
_nodes_cache_ts: float = 0.0


def _alive_nodes() -> list:
    """Short-TTL cached snapshot of alive ``ray.nodes()`` entries.

    Conservative by construction: a node absent from a slightly-stale snapshot
    is treated as *unreachable* below (→ relay), never as reachable, so caching
    can never turn a "must proxy" into an incorrect "direct".
    """
    global _nodes_cache, _nodes_cache_ts
    now = time.monotonic()
    if _nodes_cache is None or now - _nodes_cache_ts >= _ROLES_TTL:
        _nodes_cache = [n for n in ray.nodes() if n.get("Alive")]
        _nodes_cache_ts = now
    return _nodes_cache


def _classify(node: dict) -> str:
    """Role of *node*: ``head`` / ``remote`` / ``lan``."""
    res = node.get("Resources", {})
    if "node:__internal_head__" in res:        # Ray's reserved head marker
        return "head"
    if str(node.get("NodeName", "")).startswith("127."):  # tunnel loopback ip
        return "remote"
    return "lan"


def _role_from_env() -> str:
    """Fallback role when the current node isn't in the snapshot."""
    if _TUNNEL_ENV in os.environ and _HEAD_IP_ENV in os.environ:
        return "remote"
    return "lan"


def _target_reachable(my_role: str, my_id: str, target_id: str, roles: dict) -> bool:
    """Can a worker of *my_role* at *my_id* directly own a task on *target_id*?

    An unknown target (not in the current snapshot) is treated as unreachable.
    """
    if my_role == "head":
        return True
    if target_id == my_id:
        return True                     # self is always reachable
    tr = roles.get(target_id)
    if tr is None:
        return False
    if my_role == "lan":
        return tr in ("head", "lan")    # all on the on-prem LAN
    return False                        # remote: only itself (handled above)


def _resolve_targets(opts: dict, nodes: list) -> set | None:
    """Node ids the task could land on, or ``None`` if undeterminable.

    Only two high-confidence cases are resolved; everything else (soft
    affinity, SPREAD, placement groups, unconstrained) returns ``None`` so the
    caller relays. ``None`` means "can't prove → relay".
    """
    ss = opts.get("scheduling_strategy")
    try:
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
    except Exception:
        NodeAffinitySchedulingStrategy = ()  # type: ignore[assignment]

    if NodeAffinitySchedulingStrategy and isinstance(ss, NodeAffinitySchedulingStrategy):
        # Hard pin → exactly one target; a soft pin can fall back anywhere.
        if getattr(ss, "soft", True):
            return None
        return {str(ss.node_id)}
    if ss is not None:
        return None  # a scheduling strategy we don't model

    # Custom-resource constraint (chia's node-type markers: ec2 / gcp /
    # verilator_run / ...). ``resources=`` holds only custom resources; num_cpus
    # etc. are separate opts and don't confine to a node *type*.
    custom = {k: v for k, v in (opts.get("resources") or {}).items()
              if not (k.startswith("node:") or k.startswith("accelerator_type:"))}
    if not custom:
        return None  # unconstrained → could be anywhere
    cands = {
        str(n["NodeID"]) for n in nodes
        if all(n.get("Resources", {}).get(k, 0) >= v for k, v in custom.items())
    }
    return cands or None


def _directly_reachable(opts: dict) -> bool:
    """True only if every node this dispatch could land on is directly
    reachable from the current worker (so no head relay is needed)."""
    my_id = str(ray.get_runtime_context().get_node_id())
    nodes = _alive_nodes()
    roles = {str(n["NodeID"]): _classify(n) for n in nodes}
    my_role = roles.get(my_id) or _role_from_env()
    if my_role == "head":
        return True
    targets = _resolve_targets(opts, nodes)
    if targets is None:
        return False
    return all(_target_reachable(my_role, my_id, t, roles) for t in targets)


def should_proxy(opts: dict | None = None) -> bool:
    """True when this dispatch must be relayed through the head DispatchProxy.

    Relays iff the task could land on a node the current worker cannot reach
    directly (see :func:`_directly_reachable`). This both enables LAN-origin
    dispatches to tunneled nodes (``local -> remote``) and lets provably-direct
    dispatches skip the single-actor relay. The head never relays.
    """
    if not ray.is_initialized():
        return False
    if opts and opts.get("num_returns") not in (None, 1):
        logger.warning(
            "chia dispatch proxy does not support num_returns > 1; dispatching "
            "directly (this may hang if the task schedules on an unreachable node)")
        return False
    try:
        return not _directly_reachable(opts or {})
    except Exception:
        # On any analysis failure, preserve the original behavior: only
        # reverse-tunneled workers relay (never hangs a remote worker).
        logger.debug("dispatch-proxy reachability check failed; "
                     "falling back to env rule", exc_info=True)
        return _TUNNEL_ENV in os.environ and _HEAD_IP_ENV in os.environ


@ray.remote(num_cpus=0, max_restarts=-1)
class DispatchProxy:
    """Head-pinned relay: re-dispatches chia trampolines so the head owns them."""

    async def submit(self, func, opts, bypass_state, hooks, profile, args, kwargs):
        from chia.base.ChiaFunction import _chia_trampoline, _chia_trampoline_profiled
        if profile is not None:
            call_id, dispatch_meta = profile
            remote = ray.remote(_chia_trampoline_profiled).options(**opts)
            # The _ProfiledResult passes through unchanged; the caller's get()
            # unwraps it exactly as it would for a direct dispatch.
            return await remote.remote(func, call_id, dispatch_meta,
                                       bypass_state, hooks, *args, **kwargs)
        remote = ray.remote(_chia_trampoline).options(**opts)
        return await remote.remote(func, bypass_state, hooks, *args, **kwargs)


def get_dispatch_proxy():
    """Get-or-create this job's head-pinned DispatchProxy actor."""
    global _proxy_handle
    if _proxy_handle is None:
        job_id = ray.get_runtime_context().get_job_id()
        _proxy_handle = DispatchProxy.options(
            name=f"chia_dispatch_proxy_{job_id}",
            namespace=_PROXY_NAMESPACE,
            get_if_exists=True,
            lifetime="detached",
            # Pin via Ray's reserved head-node resource. NOT node:<head_ip>:
            # co-located --net=host container raylets share the head's IP and
            # advertise the same node:<ip> resource, so an IP pin can land the
            # proxy in a container raylet — whose random-range worker ports
            # both collide on the shared host and are unreachable from tunneled worker
            # (only the head raylet's pinned 40000-40099 are tunneled+DNAT'd).
            resources={"node:__internal_head__": 0.001},
            max_concurrency=64,
            # Parity with chia's normal tasks (ray default max_retries=3):
            # ride out a proxy restart instead of surfacing ACTOR_UNAVAILABLE.
            max_task_retries=3,
        ).remote()
    return _proxy_handle
