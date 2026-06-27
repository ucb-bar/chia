"""chia.base.colocated — placement-group plumbing for path-based node classes.

Any subsystem whose artifacts live on a worker's filesystem (a gem5 checkout, a
hammer obj_dir, etc) can pin a family of ``@ChiaFunction`` members to one
placement-group bundle and guarantee they land on the same worker.
"""

from __future__ import annotations

import logging

import ray
from ray.util.placement_group import (
    placement_group as _placement_group,
    remove_placement_group as _remove_placement_group,
)
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

logger = logging.getLogger(__name__)


class PinnedChiaFn:
    """Exposes a ``@ChiaFunction`` with ``.chia_remote`` pre-pinned to a
    placement-group bundle.  Resource requirements are carried over unchanged by
    ``.options()``; with no scheduling opts it delegates to the raw function so
    the caller's own placement applies.

    (Lifted from ``chia.simulators.gem5._PinnedChiaFn``; gem5 can migrate here.)
    """

    def __init__(self, fn, scheduling_opts: dict):
        self._fn = fn
        self._opts = dict(scheduling_opts) if scheduling_opts else {}
        self.chia_remote = (
            fn.options(**self._opts).chia_remote if self._opts else fn.chia_remote
        )

    def options(self, **overrides):
        """Layer extra Ray options on top of the node's pinning."""
        merged = {**self._opts, **overrides}
        return self._fn.options(**merged) if merged else self._fn

    def __call__(self, *args, **kwargs):
        """Local (non-Ray) invocation of the underlying function."""
        return self._fn(*args, **kwargs)


class ColocatedNode:
    """Base for node classes whose member ChiaFunctions must share one worker.

    Subclasses declare:

      * ``_MEMBER_FNS``: names of class-level ``@ChiaFunction`` members that
        ``__init__`` re-binds into per-instance :class:`PinnedChiaFn` wrappers,
        so ``node.<fn>.chia_remote(...)`` lands on this node's bundle while
        ``Subclass.<fn>.chia_remote(...)`` (the class attribute) stays unpinned.
      * ``_DEFAULT_BUNDLE``: resource shape of a self-reserved bundle.

    Pinning never alters a member's resource demands — the placement group only
    changes where they are satisfied from (the bundle's reservation instead of
    the node's free pool).  A bundle that cannot satisfy every member is
    therefore allowed: construction logs a warning naming the members that
    don't fit (derived from each member's own ``@ChiaFunction`` options), and
    dispatching one of them later raises ``ValueError`` at submission.

    Placement is decided once at construction:

      * ``placement_group`` given        -> pin members to ``bundle_index`` of it
        (the node will NOT release a PG it did not create).
      * none + ``require_colocated=True`` -> reserve a 1-bundle
        ``_DEFAULT_BUNDLE`` PG (owned + released by :meth:`close`).
      * none + ``require_colocated=False`` -> no pinning; the caller schedules
        each ``.chia_remote`` / ``.options(...)`` call itself.

    Usable as a context manager so a self-reserved PG is released on exit.
    """

    _MEMBER_FNS: tuple[str, ...] = ()
    _DEFAULT_BUNDLE: dict = {"CPU": 1}

    def __init__(
        self,
        placement_group=None,
        require_colocated: bool = True,
        *,
        bundle_index: int = 0,
        reserve_bundle: dict | None = None,
        pg_strategy: str = "STRICT_PACK",
        wait_for_pg: bool = True,
        pg_ready_timeout_s: float | None = None,
    ):
        """Set up placement and bind the member functions.

        Args:
            placement_group: an existing Ray ``PlacementGroup`` to schedule onto.
                If given, ``require_colocated`` is moot (placement is already
                fixed) and this node will not remove the PG on close.
            require_colocated: when no PG is given, reserve one so all members
                co-locate. When False, leave placement to the caller.
            bundle_index: which bundle of the (given or reserved) PG to pin to.
            reserve_bundle: resource shape of a self-reserved bundle
                (default ``_DEFAULT_BUNDLE``). A bundle too small for some
                members is allowed — those members just can't dispatch through
                this node (a construction-time warning lists them).
            pg_strategy: placement strategy for a self-reserved PG.
            wait_for_pg: block on ``pg.ready()`` for a self-reserved PG so the
                node is usable immediately.
            pg_ready_timeout_s: optional timeout for that wait.
        """
        self._owns_pg = False
        self._bundle_index = bundle_index

        if placement_group is not None:
            self._pg = placement_group
        elif require_colocated:
            bundle = reserve_bundle or dict(self._DEFAULT_BUNDLE)
            self._pg = _placement_group([bundle], strategy=pg_strategy)
            self._owns_pg = True
            self._bundle_index = 0
            if wait_for_pg:
                ray.get(self._pg.ready(), timeout=pg_ready_timeout_s)
        else:
            self._pg = None

        if self._pg is not None:
            self._sched_opts = {
                "scheduling_strategy": PlacementGroupSchedulingStrategy(
                    placement_group=self._pg,
                    placement_group_bundle_index=self._bundle_index,
                )
            }
        else:
            self._sched_opts = {}

        # Re-bind each class-level @ChiaFunction into a pinned instance member:
        #   node.<fn>.chia_remote == <fn>.options(<sched>).chia_remote
        for name in self._MEMBER_FNS:
            setattr(self, name, PinnedChiaFn(getattr(type(self), name), self._sched_opts))

        self._warn_unsatisfiable_members()

    def _member_demands(self) -> dict[str, dict]:
        """Per-member resource demands, derived from each ``@ChiaFunction``'s
        decorator options: custom ``resources`` plus ``num_cpus`` (which Ray
        defaults to 1 for tasks)."""
        demands = {}
        for name in self._MEMBER_FNS:
            opts = getattr(getattr(type(self), name), "_chia_options", {})
            demands[name] = {"CPU": opts.get("num_cpus", 1), **opts.get("resources", {})}
        return demands

    def _warn_unsatisfiable_members(self) -> None:
        """Warn (never raise) for members whose demands cannot fit the pinned
        bundle(s) — dispatching such a member through this node raises
        ``ValueError`` at submission.  Advisory only: constructing a node
        against a PG that serves a subset of its members is legitimate."""
        if self._pg is None:
            return
        try:
            specs = self._pg.bundle_specs
        except Exception:
            return  # PG metadata unavailable; skip the advisory check
        bundles = specs if self._bundle_index == -1 else [specs[self._bundle_index]]
        for name, demand in self._member_demands().items():
            if not any(all(b.get(res, 0) >= v for res, v in demand.items())
                       for b in bundles):
                logger.warning(
                    f"{type(self).__name__}.{name} demands {demand}, which does "
                    f"not fit the pinned bundle(s) {bundles}; dispatching it via "
                    f"this node will raise ValueError at submission"
                )

    # -- placement-group lifecycle --------------------------------------------

    @property
    def placement_group(self):
        """The PG members are pinned to (None when the caller handles placement)."""
        return self._pg

    @property
    def owns_placement_group(self) -> bool:
        """True iff this node reserved its PG and will release it on close()."""
        return self._owns_pg

    @property
    def task_options(self) -> dict:
        """Scheduling opts to co-locate an actor (e.g. a ``ChiaTool``) with this
        node's bundle.  Empty when the node has no placement group
        (``require_colocated=False``) — there is no shared placement to inherit,
        so an actor given these opts would not be pinned."""
        return dict(self._sched_opts)

    def close(self) -> None:
        """Release the PG iff this node reserved it. Idempotent."""
        if self._owns_pg and self._pg is not None:
            _remove_placement_group(self._pg)
            self._pg = None
            self._owns_pg = False

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        self.close()
