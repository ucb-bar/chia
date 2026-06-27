"""``ray.wait`` wrapper with stuck-PENDING_NODE_ASSIGNMENT detection + retry.

Ray's owner-side ``NormalTaskSubmitter`` sends
``RequestWorkerLease`` RPCs without a client-side timeout. If the chosen
raylet is wedged but TCP-reachable (silent), the lease slot stays full
forever and downstream tasks of the same scheduling key get throttled at
the owner. ``chia_wait`` detects
the pattern (PENDING_NODE_ASSIGNMENT older than ``pending_timeout`` while
the cluster has free resources) and optionally cancels + resubmits.

Use a :class:`TrackedRef` to pair an ObjectRef with the closure that can
re-dispatch it. ``chia_wait`` mirrors ``ray.wait``'s ``(ready, pending)``
return shape but operates on TrackedRefs.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

import ray

logger = logging.getLogger(__name__)

_PENDING_NODE_ASSIGNMENT = "PENDING_NODE_ASSIGNMENT"


@dataclass
class TrackedRef:
    """ObjectRef + resubmit closure + submission timestamp.

    ``submit_fn`` must accept no arguments and produce a fresh ObjectRef
    equivalent to the original submission (same function, args, options).
    Capture call-site state via ``functools.partial`` or a closure.
    """
    ref: "ray.ObjectRef"
    submit_fn: Optional[Callable[[], "ray.ObjectRef"]] = None
    label: str = ""
    submitted_at: float = field(default=0.0)
    retries: int = 0

    def __post_init__(self) -> None:
        if self.submitted_at == 0.0:
            self.submitted_at = time.monotonic()


def chia_wait(
    tracked: list[TrackedRef],
    *,
    num_returns: int = 1,
    timeout: Optional[float] = None,
    pending_timeout: Optional[float] = None,
    retry: bool = False,
    max_retries: int = 1,
    require_demand_absent: bool = False,
    min_free_fraction: float = 0.5,
    cancel_on_stuck: bool = False,
    print_logs: bool = False
) -> tuple[list[TrackedRef], list[TrackedRef]]:
    """Drop-in replacement for :func:`ray.wait` that handles stuck-PENDING tasks.

    Returns ``(ready, pending)`` lists of :class:`TrackedRef` after at most
    ``num_returns`` complete or ``timeout`` seconds elapse — same semantics
    as ``ray.wait``. Additionally, when ``pending_timeout`` is set, any
    pending TrackedRef whose submission age exceeds ``pending_timeout`` is
    classified as "stuck" iff:

      (a) Ray state API reports ``state == PENDING_NODE_ASSIGNMENT``, AND
      (b) ``ray.available_resources()`` covers the task's required
          resources (i.e. it could schedule somewhere if Ray were healthy),
          AND
      (c) for every required resource ``r``,
          ``available[r] / cluster_total[r] >= min_free_fraction``
          (cluster-side starvation check — distinguishes a wedged raylet,
          where the resource is sitting unused, from a busy cluster where
          tasks are merely waiting on the owner-side per-class budget),
          AND
      (d) (when ``require_demand_absent=True``) the cluster has no pending
          demand reported for this task's resource fingerprint.

    For each stuck TrackedRef:
      - If ``retry=True``, ``tr.submit_fn`` is set, and ``tr.retries <
        max_retries``: ``ray.cancel(tr.ref, force=True)`` (best effort),
        then call ``tr.submit_fn()`` for a fresh ref. Mutate ``tr`` in
        place (ref/submitted_at/retries++). The old ref is orphaned — its
        rate-limit slot may stay burnt until the wedged raylet's TCP
        eventually resets.
      - Else: log a warning and leave ``tr`` in pending unchanged.

    The returned ``pending`` list contains the same TrackedRef objects
    passed in (possibly with mutated ``ref``/``submitted_at`` after
    retry); callers can keep parallel bookkeeping keyed on identity.
    """
    if not tracked:
        return [], []

    ref_to_tr = {tr.ref: tr for tr in tracked}
    ready_refs, pending_refs = ray.wait(
        [tr.ref for tr in tracked],
        num_returns=num_returns,
        timeout=timeout,
    )
    ready = [ref_to_tr[r] for r in ready_refs]
    pending = [ref_to_tr[r] for r in pending_refs]

    if pending_timeout is None or not pending:
        return ready, pending

    stuck = _classify_stuck(
        pending, pending_timeout, require_demand_absent, min_free_fraction,
    )
    if not stuck:
        return ready, pending

    for tr in stuck:
        age = time.monotonic() - tr.submitted_at
        wedged_nodes = _wedged_node_ids_for(tr)
        nodes_str = (",".join(n[:12] for n in wedged_nodes)
                     if wedged_nodes else "<unknown>")
        if (retry
                and tr.submit_fn is not None
                and tr.retries < max_retries):
            logger.warning(
                "chia_wait: %s stuck in PENDING_NODE_ASSIGNMENT for %.0fs "
                "(retry %d/%d, prior_nodes=[%s]) — cancelling + resubmitting",
                tr.label or tr.ref.task_id().hex(), age,
                tr.retries + 1, max_retries, nodes_str,
            )
            if print_logs:
                print(
                    f"chia_wait: {tr.label or tr.ref.task_id().hex()} "
                    f"stuck in PENDING_NODE_ASSIGNMENT for {age:.0f}s "
                    f"(retry {tr.retries + 1}/{max_retries}, "
                    f"prior_nodes=[{nodes_str}]) — cancelling + resubmitting"
                )
            _retry_stuck(tr)
        else:
            logger.warning(
                "chia_wait: %s stuck in PENDING_NODE_ASSIGNMENT for %.0fs "
                "(retry disabled or max_retries=%d reached, "
                "prior_nodes=[%s]) - cancelling {%s}",
                tr.label or tr.ref.task_id().hex(), age, max_retries, nodes_str, cancel_on_stuck
            )
            if print_logs:
                print(
                    f"chia_wait: {tr.label or tr.ref.task_id().hex()} "
                    f"stuck in PENDING_NODE_ASSIGNMENT for {age:.0f}s "
                    f"(retry disabled or max_retries={max_retries} reached, "
                    f"prior_nodes=[{nodes_str}]) - cancelling {cancel_on_stuck}"
                )
            if cancel_on_stuck:
                try:
                    ray.cancel(tr.ref, force=True, recursive=True)
                except Exception as e:
                    logger.debug("chia_wait: ray.cancel raised during retry: %s", e)
    return ready, pending


def _wedged_node_ids_for(tr: TrackedRef, print_logs: bool = False) -> list[str]:
    """Best-effort: return distinct node_ids that earlier attempts of
    ``tr.ref`` ran on.

    For a stuck attempt-N ref, prior attempts (0..N-1) appear in the state
    API as separate rows under the same ``task_id`` with non-null
    ``node_id``. For a true attempt-0 stuck task, no prior attempts exist
    — we can't observe the leasing raylet (it's only in the C++
    NormalTaskSubmitter's pending_lease_requests map) — so we return [].

    Returned list is in the order encountered (no de-dup ordering guarantee
    beyond first-seen) and excludes Nones / empty strings.
    """
    try:
        from ray.util.state import list_tasks
        states = list_tasks(
            filters=[("task_id", "=", tr.ref.task_id().hex())],
            limit=10,
            detail=True,
            raise_on_missing_output=False,
        )
    except Exception as e:
        logger.debug("chia_wait: list_tasks(task_id=...) failed: %s", e)
        if (print_logs):
            print(f"chia_wait: list_tasks(task_id=...) failed: {e}")
        return []

    seen: list[str] = []
    for ts in states:
        nid = getattr(ts, "node_id", None)
        if nid and nid not in seen:
            seen.append(nid)
    return seen


def _classify_stuck(
    pending: list[TrackedRef],
    pending_timeout: float,
    require_demand_absent: bool,
    min_free_fraction: float = 0.5,
) -> list[TrackedRef]:
    """Return the subset of *pending* that are wedged-stuck (not just contended)."""
    now = time.monotonic()
    aged = [tr for tr in pending if now - tr.submitted_at > pending_timeout]
    if not aged:
        return []

    aged_by_task_id = {tr.ref.task_id().hex(): tr for tr in aged}

    try:
        from ray.util.state import list_tasks
        task_states = list_tasks(
            filters=[("state", "=", _PENDING_NODE_ASSIGNMENT)],
            limit=max(len(aged_by_task_id) * 4, 100),
            detail=True,
            raise_on_missing_output=False,
        )
    except Exception as e:
        logger.debug("chia_wait: list_tasks query failed: %s", e)
        return []

    confirmed: list[tuple[TrackedRef, dict]] = []
    for ts in task_states:
        tid = getattr(ts, "task_id", None)
        if tid in aged_by_task_id:
            tr = aged_by_task_id[tid]
            req = getattr(ts, "required_resources", None) or {}
            confirmed.append((tr, dict(req)))
    if not confirmed:
        return []

    try:
        avail = ray.available_resources()
        total = ray.cluster_resources()
    except Exception as e:
        logger.debug("chia_wait: resource query failed: %s", e)
        return []

    stuck = [
        tr for tr, req in confirmed
        if _resources_cover(avail, req)
        and _resources_underused(avail, total, req, min_free_fraction)
    ]

    if not stuck or not require_demand_absent:
        return stuck

    demand_text = _safe_debug_status_string()
    if demand_text is None:
        return stuck
    return [
        tr for tr in stuck
        if not _demand_mentions(demand_text,
                                next(req for t, req in confirmed if t is tr))
    ]


def _resources_cover(available: dict, required: dict) -> bool:
    """True iff *available* >= *required* for every resource key in *required*."""
    if not required:
        return True
    for key, need in required.items():
        if available.get(key, 0.0) + 1e-9 < float(need):
            return False
    return True


def _resources_underused(
    available: dict, total: dict, required: dict, min_free_fraction: float,
) -> bool:
    """True iff every required resource is at least *min_free_fraction* free.

    This distinguishes a wedged raylet (the resource is sitting unused, so
    we should retry) from a busy cluster where tasks are merely waiting on
    the owner-side per-class lease budget (in which case retry only burns
    cycles). We compare against the cluster total reported by
    ``ray.cluster_resources()`` rather than required, so a 1-unit task in a
    saturated 300-unit pool is not treated as wedged just because there
    happens to be 1 free slot at the moment of the check.

    Returns False if the cluster reports zero total of any required key —
    that's an infeasible request, not a wedge.
    """
    if not required or min_free_fraction <= 0.0:
        return True
    for key in required.keys():
        tot = float(total.get(key, 0.0))
        if tot <= 0.0:
            return False
        if available.get(key, 0.0) / tot < min_free_fraction:
            return False
    return True


def _safe_debug_status_string() -> Optional[str]:
    """Return autoscaler debug status, or None if unavailable."""
    try:
        from ray.autoscaler._private.commands import debug_status_string
        return debug_status_string()
    except Exception as e:
        logger.debug("chia_wait: debug_status_string failed: %s", e)
        return None


def _demand_mentions(demand_text: str, required: dict) -> bool:
    """Heuristic: does *demand_text* mention all keys/values of *required*?

    Pending Demands strings look like:
        {CPU: 1.0, verilator_run: 1.0}: 14+ pending tasks/actors
    We require the substring "Pending Demands" to be present and every
    required-resource key to appear after it.
    """
    idx = demand_text.find("Pending Demands")
    if idx < 0:
        return False
    section = demand_text[idx:]
    for key in required.keys():
        if key not in section:
            return False
    return True


def _retry_stuck(tr: TrackedRef) -> None:
    """Cancel ``tr.ref`` (best effort) and resubmit via ``tr.submit_fn``."""
    assert tr.submit_fn is not None
    try:
        ray.cancel(tr.ref, force=True, recursive=True)
    except Exception as e:
        logger.debug("chia_wait: ray.cancel raised during retry: %s", e)
    tr.ref = tr.submit_fn()
    tr.submitted_at = time.monotonic()
    tr.retries += 1
