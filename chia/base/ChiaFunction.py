from __future__ import annotations

import ray
import functools
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    List,
    Optional,
    Protocol,
    Sequence,
    TypeVar,
    Union,
    cast,
    overload,
    ParamSpec
)

from ray import ObjectRef
from ray.experimental.compiled_dag_ref import CompiledDAGRef

P = ParamSpec('P')
R = TypeVar('R')


class _ChiaRemoteHandle(Protocol[P, R]):
    """Typed handle returned by :meth:`ChiaWrapped.options`.

    Parameterized on both ``P`` (call args) and ``R`` (wrapped function's
    return type) so ``chia_remote(...)`` yields ``ObjectRef[R]`` and
    ``get(ref)`` can infer ``R``.
    """
    def chia_remote(self, *args: P.args, **kwargs: P.kwargs) -> ObjectRef[R]: ...
    def chia_remote_blocking(self, *args: P.args, **kwargs: P.kwargs) -> R: ...
    def remote(self, *args: P.args, **kwargs: P.kwargs) -> ObjectRef[R]: ...


class ChiaWrapped(Protocol[P, R]):
    """Protocol describing the object returned by ``@ChiaFunction()``.

    Includes ``__get__`` overloads so that mypy treats instances of this
    protocol as descriptors.  When accessed on a class (``obj is None``),
    the full ``ParamSpec`` is preserved.  When accessed on an *instance*
    (bound-method position), the return type falls back to
    ``ChiaWrapped[..., R]`` because Python's type system cannot express
    "P minus the first argument".  This avoids false positives on method
    calls while keeping full typing for standalone decorated functions.
    """
    _chia_original: Callable[P, R]
    _chia_options: dict[str, Any]
    _chia_remote_func: Any
    _chia_remote_profiled: bool
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R: ...
    def chia_remote(self, *args: P.args, **kwargs: P.kwargs) -> ObjectRef[R]: ...
    def chia_remote_blocking(self, *args: P.args, **kwargs: P.kwargs) -> R: ...
    def options(self, **kwargs: Any) -> _ChiaRemoteHandle[P, R]: ...
    @overload
    def __get__(self, obj: None, objtype: type) -> ChiaWrapped[P, R]: ...
    @overload
    def __get__(self, obj: Any, objtype: type) -> ChiaWrapped[..., R]: ...
    def __get__(self, obj: Any, objtype: type) -> ChiaWrapped[P, R] | ChiaWrapped[..., R]: ...


class ChiaFunction:
    """Decorator that creates both a local and ray.remote version of a function.

    Usage::

        @ChiaFunction(resources={"db1": 1.0})
        def my_function(x, y):
            return x + y

        # Local call:
        my_function(1, 2)

        # Remote call (returns ObjectRef):
        ref = ChiaCallRemote(my_function, 1, 2)
        result = get(ref)

        # Or attribute-based:
        ref = my_function.chia_remote(1, 2)

    For bound methods on a class, the decorator uses a trampoline to
    serialize ``self`` and dispatch via ``ray.remote``::

        class MyTool:
            @ChiaFunction()
            def do_work(self, x):
                return x * 2

        tool = MyTool()
        ref = tool.do_work.chia_remote(tool, x=5)
    """

    def __init__(self, **kwargs):
        """Accept any keyword arguments supported by ``ray.remote().options()``.

        Common options include ``resources``, ``num_cpus``, ``num_gpus``,
        ``num_returns``, ``max_retries``, ``retry_exceptions``,
        ``memory``, ``scheduling_strategy``, ``max_calls``,
        ``runtime_env``, ``name``, ``namespace``, ``lifetime``, etc.
        See Ray documentation for the full list.
        """
        self.options = {k: v for k, v in kwargs.items() if v is not None}

    def __call__(self, func: Callable[P, R]) -> ChiaWrapped[P, R]:
        options_dict = self.options

        @functools.wraps(func)
        def _wrapper(*args, **kwargs):
            from chia.trace.profiler import get_profiler
            profiler = get_profiler()
            if profiler.enabled:
                info = profiler.on_local_start(func, args, kwargs)
                result = func(*args, **kwargs)
                profiler.on_local_end(info, result)
                return result
            return func(*args, **kwargs)

        # Treat the wrapped closure as a ChiaWrapped instance so attribute
        # assignments below typecheck against the Protocol.
        wrapper = cast(ChiaWrapped[P, R], _wrapper)

        wrapper._chia_original = func
        wrapper._chia_options = options_dict
        wrapper._chia_remote_func = None  # lazy init
        wrapper._chia_remote_profiled = False  # tracks trampoline variant

        def _try_bypass(args, kwargs, opts):
            """If func is bypassed, dispatch the provider to the worker.

            Pops _chia_tag from kwargs. Returns an ObjectRef if bypassed,
            None otherwise. The provider runs on the worker with the same
            scheduling options (placement groups, resources) as the real
            function, so cluster scheduling is still exercised.
            """
            tag = kwargs.pop("_chia_tag", None)
            try:
                from chia.base.bypass import get_active_bypass
            except ImportError:
                return None
            b = get_active_bypass()
            # Forward the call args so a registered condition (set_cond) can
            # inspect them. The reserved kwargs (_chia_tag, hooks, display_name)
            # are already popped, so args/kwargs are the clean user arguments.
            if b is not None and b.is_bypassed(func.__name__, tag, *args, **kwargs):
                provider, data_path = b.get_provider_info(func.__name__)
                bypass_state = _get_bypass_state()
                remote = ray.remote(_chia_bypass_trampoline).options(
                    name=f"BYPASS_{func.__qualname__}", **opts)
                return remote.remote(provider, tag, data_path, bypass_state, *args, **kwargs)
            return None

        def _get_bypass_state():
            """Return serialized bypass state if bypass is active, else None.

            Attached to trampoline calls so workers can restore bypass for
            nested chia_remote() dispatches. Returns None when bypass is
            inactive to avoid any overhead.
            """
            try:
                from chia.base.bypass import get_active_bypass
            except ImportError:
                return None
            b = get_active_bypass()
            if b is not None and b.active:
                return b.get_state()
            return None

        def chia_remote(*args, **kwargs):
            """Dispatch func via ray.remote. Lazily initializes the remote wrapper."""
            display_name = kwargs.pop("_chia_display_name", None)
            # Read the cache key before _try_bypass pops _chia_tag from kwargs.
            cache_tag = kwargs.get("_chia_tag")
            hooks = _pop_chia_hooks(kwargs)
            ref = _try_bypass(args, kwargs, options_dict)
            if ref is not None:
                return _maybe_wrap_cache(ref, func.__name__, cache_tag)

            ref = _try_proxy(func, options_dict, _get_bypass_state(), hooks,
                             args, kwargs, display_name)
            if ref is not None:
                return _maybe_wrap_cache(ref, func.__name__, cache_tag)

            from chia.trace.profiler import get_profiler
            profiler = get_profiler()
            use_profiled = profiler.enabled

            # Invalidate cache if profiling state changed.
            if (wrapper._chia_remote_func is not None
                    and wrapper._chia_remote_profiled != use_profiled):
                wrapper._chia_remote_func = None

            if wrapper._chia_remote_func is None:
                trampoline = _chia_trampoline_profiled if use_profiled else _chia_trampoline
                remote = ray.remote(trampoline)
                opts = {"name": func.__qualname__, **options_dict}
                remote = remote.options(**opts)
                wrapper._chia_remote_func = remote
                wrapper._chia_remote_profiled = use_profiled

            bypass_state = _get_bypass_state()
            if use_profiled:
                call_id = profiler.next_call_id()
                dispatch_meta = profiler.prepare_dispatch(
                    options_dict, args, kwargs, display_name=display_name)
                ref = wrapper._chia_remote_func.remote(func, call_id, dispatch_meta, bypass_state, hooks, *args, **kwargs)
            else:
                ref = wrapper._chia_remote_func.remote(func, bypass_state, hooks, *args, **kwargs)
            return _maybe_wrap_cache(ref, func.__name__, cache_tag)

        @functools.wraps(func)
        def chia_remote_blocking(*args, **kwargs):
            """Dispatch func remotely (honoring its resource options), block on
            the result, and return the unwrapped *value* instead of an ObjectRef.

            Convenience for callers that need a synchronous value rather than a
            ref — e.g. an MCP ``ChiaTool`` method, which must return func's
            actual result. ``functools.wraps(func)`` is applied so that
            ``inspect.signature`` (used by FastMCP to build the tool schema)
            reports func's real parameters rather than this wrapper's
            ``(*args, **kwargs)``.
            """
            return get(chia_remote(*args, **kwargs))

        def options(**override_opts):
            """Return a handle whose ``.remote()`` merges *override_opts* on
            top of the decorator-level options — mirroring Ray's
            ``func.options(...).remote(...)`` pattern."""
            merged = {**options_dict, **{k: v for k, v in override_opts.items() if v is not None}}

            class _RemoteHandle:
                def chia_remote(self, *args, **kwargs):
                    display_name = kwargs.pop("_chia_display_name", None)
                    # Read the cache key before _try_bypass pops _chia_tag.
                    cache_tag = kwargs.get("_chia_tag")
                    hooks = _pop_chia_hooks(kwargs)
                    ref = _try_bypass(args, kwargs, merged)
                    if ref is not None:
                        return _maybe_wrap_cache(ref, func.__name__, cache_tag)

                    ref = _try_proxy(func, merged, _get_bypass_state(), hooks,
                                     args, kwargs, display_name)
                    if ref is not None:
                        return _maybe_wrap_cache(ref, func.__name__, cache_tag)

                    from chia.trace.profiler import get_profiler
                    profiler = get_profiler()
                    use_profiled = profiler.enabled
                    trampoline = _chia_trampoline_profiled if use_profiled else _chia_trampoline
                    remote = ray.remote(trampoline)
                    opts = {"name": func.__qualname__, **merged}
                    remote = remote.options(**opts)
                    bypass_state = _get_bypass_state()
                    if use_profiled:
                        call_id = profiler.next_call_id()
                        dispatch_meta = profiler.prepare_dispatch(
                            merged, args, kwargs, display_name=display_name)
                        ref = remote.remote(func, call_id, dispatch_meta, bypass_state, hooks, *args, **kwargs)
                    else:
                        ref = remote.remote(func, bypass_state, hooks, *args, **kwargs)
                    return _maybe_wrap_cache(ref, func.__name__, cache_tag)

                def remote(self, *args, **kwargs):
                    return self.chia_remote(*args, **kwargs)

                def chia_remote_blocking(self, *args, **kwargs):
                    """Like :meth:`chia_remote` but blocks and returns the value."""
                    return get(self.chia_remote(*args, **kwargs))

            return _RemoteHandle()

        # setattr (not direct assignment) because ChiaWrapped declares these
        # as methods; mypy forbids overwriting method slots.
        setattr(wrapper, "chia_remote", chia_remote)
        setattr(wrapper, "chia_remote_blocking", chia_remote_blocking)
        setattr(wrapper, "options", options)
        return wrapper


# ---------------------------------------------------------------------------
# Bypass return function — dispatched to workers when a function is bypassed.
# The worker receives pre-computed mock data and just returns it.
# ---------------------------------------------------------------------------

def _chia_bypass_trampoline(provider, tag, data_path, _chia_bypass_state_, *args, **kwargs):
    """Bypass trampoline: runs the provider on the worker.

    Dispatched with the same scheduling options (placement group, resources)
    as the real function. Restores bypass state first so any nested
    chia_remote() calls from within the provider see the bypass config.
    """
    _restore_bypass(_chia_bypass_state_)
    print(f"[BYPASS] {provider.__name__} (tag={tag})")
    return provider(tag, data_path, *args, **kwargs)


# ---------------------------------------------------------------------------
# Trampolines — execute the real function on the worker
# ---------------------------------------------------------------------------

def _restore_bypass(bypass_state):
    """Restore bypass on this worker if state was provided by the caller."""
    if bypass_state is not None:
        from chia.base.bypass import Bypass, get_active_bypass
        if get_active_bypass() is None or not get_active_bypass().active:
            b = Bypass()
            b.load_state(bypass_state)


# ---------------------------------------------------------------------------
# Optional worker-side setup/cleanup hooks
#
# A chia_remote() call may pass reserved kwargs ``_chia_setup`` /
# ``_chia_setup_args`` / ``_chia_cleanup`` / ``_chia_cleanup_args``. The
# (cloudpicklable) setup callable runs on the worker BEFORE func, the cleanup
# callable runs AFTER in a finally (so it runs even when func raises — but not
# if setup itself raised). Both are side-effect only: neither receives nor can
# replace func's return value. Hooks are skipped on bypassed calls (those go
# through _chia_bypass_trampoline, which never runs func).
#
# Exception semantics:
#   * setup raising  -> propagates; func and cleanup are skipped (nothing was
#                       set up to tear down). The task fails with that error.
#   * cleanup raising -> caught and logged, never re-raised. A best-effort
#                       teardown must not mask func's return value or func's own
#                       exception (which a bare ``finally`` would do).
# ---------------------------------------------------------------------------

_logger = logging.getLogger(__name__)


def _pop_chia_hooks(kwargs):
    """Pop the reserved hook kwargs from *kwargs*; return a hooks dict or None."""
    setup = kwargs.pop("_chia_setup", None)
    cleanup = kwargs.pop("_chia_cleanup", None)
    setup_args = kwargs.pop("_chia_setup_args", ())
    cleanup_args = kwargs.pop("_chia_cleanup_args", ())
    if setup is None and cleanup is None:
        return None
    return {
        "setup": setup, "setup_args": setup_args,
        "cleanup": cleanup, "cleanup_args": cleanup_args,
    }


def _run_chia_setup(hooks):
    """Run the setup hook. Lets exceptions propagate (fails the task)."""
    if hooks and hooks["setup"] is not None:
        hooks["setup"](*(hooks["setup_args"] or ()))


def _run_chia_cleanup(hooks):
    """Run the cleanup hook best-effort: never raises, so a failing teardown
    cannot mask func's result or func's own exception. Logs on failure."""
    if hooks and hooks["cleanup"] is not None:
        try:
            hooks["cleanup"](*(hooks["cleanup_args"] or ()))
        except Exception:  # noqa: BLE001 — best-effort teardown
            _logger.exception(
                "chia cleanup hook raised; ignoring (func result/error preserved)"
            )


def _try_proxy(func, opts, bypass_state, hooks, args, kwargs, display_name):
    """Relay this dispatch through the head's DispatchProxy when running on a
    reverse-tunneled worker, which cannot lease workers on LAN raylets (see
    chia/base/dispatch_proxy.py for the full story). Returns an ObjectRef to
    the relayed call, or None when dispatching directly is fine."""
    from chia.base.dispatch_proxy import get_dispatch_proxy, should_proxy
    if not should_proxy(opts):
        return None
    from chia.trace.profiler import get_profiler
    profiler = get_profiler()
    profile = None
    if profiler.enabled:
        profile = (profiler.next_call_id(),
                   profiler.prepare_dispatch(opts, args, kwargs,
                                             display_name=display_name))
    full_opts = {"name": func.__qualname__, **opts}
    return get_dispatch_proxy().submit.remote(
        func, full_opts, bypass_state, hooks, profile, args, kwargs)


def _chia_trampoline(func, _chia_bypass_state_, _chia_hooks_, *args, **kwargs):
    """Trampoline function used by ChiaFunction to dispatch remote calls.

    _chia_bypass_state_ is automatically attached by chia_remote() when
    bypass is active, so any nested chia_remote() calls from within func
    can check bypass. _chia_hooks_ carries optional worker-side setup/cleanup
    callables. Both are attached by chia_remote() and named with underscores to
    avoid collisions with user function parameters.
    """
    _restore_bypass(_chia_bypass_state_)
    from chia.base.pid_registry import _pid_tracking_scope
    _run_chia_setup(_chia_hooks_)
    try:
        with _pid_tracking_scope():
            return func(*args, **kwargs)
    finally:
        _run_chia_cleanup(_chia_hooks_)


def _chia_trampoline_profiled(func, call_id, dispatch_meta, _chia_bypass_state_, _chia_hooks_, *args, **kwargs):
    """Profiled trampoline — wraps the result with worker metadata.

    Emits the ``dispatch`` event at function start and the ``complete``
    event at function end, both from the worker.  Returns a
    ``_ProfiledResult`` that ``get()`` unwraps transparently.
    """
    _restore_bypass(_chia_bypass_state_)
    import time as _time
    from chia.trace.profiler import _ProfiledResult, get_profiler

    # Ensure the profiler singleton on this worker is enabled.
    # The worker may have created a disabled singleton before the
    # collector actor was started.
    profiler = get_profiler()
    if not profiler.enabled:
        get_profiler.reset()

    # Gather worker metadata once.
    try:
        worker_ip = ray.util.get_node_ip_address()
    except Exception:
        worker_ip = "unknown"
    try:
        ctx = ray.get_runtime_context()
        worker_id = ctx.get_worker_id()
        node_id = ctx.get_node_id()
    except Exception:
        worker_id = "unknown"
        node_id = "unknown"

    # Emit the dispatch event from the worker at actual task start.
    profiler = get_profiler()
    display_name = dispatch_meta.get("display_name", "") if dispatch_meta else ""

    if profiler.enabled and dispatch_meta:
        profiler.on_worker_dispatch(
            call_id, func.__name__, worker_ip, worker_id, node_id,
            dispatch_meta.get("resources", {}),
            dispatch_meta.get("obj_ref_deps", []),
            dispatch_meta.get("caller_worker_id", ""),
            display_name=display_name,
        )

    # perf_counter for high-res duration, time() for wall-clock timestamp
    from chia.base.pid_registry import _pid_tracking_scope
    _run_chia_setup(_chia_hooks_)
    try:
        with _pid_tracking_scope():
            t0 = _time.perf_counter()
            result = func(*args, **kwargs)
            extra = profiler._pop_extra()
            elapsed = _time.perf_counter() - t0
        end_ts = _time.time()  # wall-clock end timestamp

        # Emit the complete event from the worker at the actual end time.
        if profiler.enabled:
            profiler.on_worker_complete(call_id, func.__name__, end_ts, elapsed,
                                        worker_ip, worker_id, node_id, extra=extra,
                                        display_name=display_name)

        return _ProfiledResult(
            value=result,
            worker_ip=worker_ip,
            worker_id=worker_id,
            node_id=node_id,
            exec_time_s=elapsed,
            call_id=call_id,
            func_name=func.__name__,
            display_name=display_name,
            extra=extra,
        )
    finally:
        _run_chia_cleanup(_chia_hooks_)


def ChiaCallRemote(
    func: ChiaWrapped[P, R], *args: P.args, **kwargs: P.kwargs
) -> ObjectRef[R]:
    """Call the ray.remote version of a @ChiaFunction-decorated function.

    Returns a ``ray.ObjectRef[R]`` where ``R`` is the wrapped function's
    return type, so ``get(...)`` can infer the final value type.
    """
    if not hasattr(func, 'chia_remote'):
        name = getattr(func, '__name__', repr(func))
        raise TypeError(f"{name} is not decorated with @ChiaFunction")
    return func.chia_remote(*args, **kwargs)


class ObjectRefCallback:
    """An ObjectRef paired with a callback that :func:`get` runs on resolution.

    Lets a producer attach a post-resolution hook to the ref it hands back, so
    callers can stay async and simply ``get()`` it — the callback runs at fetch
    time and its return becomes ``get``'s result — without passing ``callback=``
    themselves. ``ClaudeCodeLLM.prompt`` returns one of these (carrying
    ``_sync_transcript``) when ``resume_session=True``.

    ``.ref`` exposes the underlying ObjectRef for ``ray.wait`` / :func:`chia_wait`.
    """
    __slots__ = ("ref", "callback")

    def __init__(self, ref, callback: Callable[[Any], Any]):
        self.ref = ref
        self.callback = callback


# ---------------------------------------------------------------------------
# Actor handle wrapper — give a plain Ray actor the chia_remote()/get() surface
# ---------------------------------------------------------------------------

class _ChiaActorMethod:
    """One bound method of a :class:`ChiaActorHandle`.

    Exposes ``chia_remote`` as an alias of Ray's ``remote`` (and keeps
    ``remote`` working), so actor calls read the same as ``@ChiaFunction``
    dispatch: ``get(handle.method.chia_remote(...))``. ``options`` is forwarded
    so ``handle.method.options(...).chia_remote(...)`` works too.
    """
    __slots__ = ("_method",)

    def __init__(self, method):
        self._method = method

    def chia_remote(self, *args, **kwargs):
        return self._method.remote(*args, **kwargs)

    def remote(self, *args, **kwargs):
        return self._method.remote(*args, **kwargs)

    def options(self, **kwargs):
        return _ChiaActorMethod(self._method.options(**kwargs))


class ChiaActorHandle:
    """Thin proxy over a Ray actor handle that adds ``chia_remote`` to each method.

    Built by :func:`chia_actor`. Attribute access returns a
    :class:`_ChiaActorMethod`, so ``handle.method.chia_remote(...)`` dispatches
    the call and :func:`get` resolves it — matching the ``@ChiaFunction``
    surface. ``remote`` still works, so existing ``handle.method.remote(...)``
    call sites are unaffected.

    Note: unlike a real ``@ChiaFunction`` dispatch, ``chia_remote`` here is a
    plain alias for Ray's ``remote`` — no profiling, bypass, or cache wrapping is
    applied; the call goes straight to the actor. Use :attr:`actor` to recover
    the raw Ray handle (e.g. for ``ray.kill`` or identity checks).
    """
    __slots__ = ("_actor",)

    def __init__(self, actor):
        object.__setattr__(self, "_actor", actor)

    @property
    def actor(self):
        """The underlying raw Ray actor handle."""
        return self._actor

    def __getattr__(self, name):
        # Proxy only public method access. Never intercept dunders: cloudpickle
        # and copy probe __reduce_ex__/__getstate__/etc., and returning a
        # _ChiaActorMethod for those would corrupt serialization (the handle is
        # shipped to workers and passed as a task arg).
        if name.startswith("__"):
            raise AttributeError(name)
        return _ChiaActorMethod(getattr(self._actor, name))

    def __reduce__(self):
        # Serialize as the wrapper around the (serializable) Ray handle so the
        # proxy survives being sent to a worker or passed into a ChiaFunction.
        return (chia_actor, (self._actor,))


def chia_actor(handle):
    """Wrap a Ray actor *handle* so its methods accept ``chia_remote`` and
    resolve via :func:`get`.

    Lets actor calls share the dispatch surface used by ``@ChiaFunction``::

        store = chia_actor(some_actor)
        n = get(store.size_bytes.chia_remote())
        get(store.write.chia_remote(key, value))

    ``chia_remote`` here is a plain alias for Ray's ``remote`` — it does NOT run
    the profiling / bypass / cache machinery that ``ChiaWrapped.chia_remote``
    does; the call goes straight to the actor. ``remote`` still works, so
    pre-existing call sites are unchanged. Recover the raw handle via
    ``handle.actor`` (e.g. for ``ray.kill``). Idempotent: wrapping an already
    wrapped handle returns it unchanged.
    """
    if isinstance(handle, ChiaActorHandle):
        return handle
    return ChiaActorHandle(handle)


def _maybe_wrap_cache(ref, func_name, tag):
    """Wrap *ref* so the cache write fires when its result is fetched.

    When *func_name* is configured ``cache: true`` and the call carries a
    ``_chia_tag`` (*tag*), return an :class:`ObjectRefCallback` whose callback
    writes the resolved value to the head-pinned cache actor. ``get()`` resolves
    the inner ref through the profiler unwrap *first*, so the callback receives
    the final user value (not a ``_ProfiledResult``).

    Returns *ref* unchanged when there is no tag, the function isn't cached, no
    cache was started, or *ref* is already an ``ObjectRefCallback`` (``get()``
    supports only one level of wrapping).
    """
    if tag is None or isinstance(ref, ObjectRefCallback):
        return ref
    try:
        from chia.base.cache import get_active_cache, is_cached
    except ImportError:
        return ref
    if not is_cached(func_name, tag):
        return ref
    cache = get_active_cache()
    if cache is None:
        return ref

    def _write(value):
        ray.get(cache.write.remote(tag, value))
        return value

    return ObjectRefCallback(ref, _write)


@overload
def get(ref: ObjectRefCallback, *, timeout: Optional[float] = None, callback: Optional[Callable[[Any], Any]] = None, _use_object_store: bool = False) -> Any: ...
@overload
def get(ref: Sequence[ObjectRef[R]], *, timeout: Optional[float] = None, callback: Optional[Callable[[Any], Any]] = None, _use_object_store: bool = False) -> List[R]: ...
@overload
def get(ref: Sequence[ObjectRef[Any]], *, timeout: Optional[float] = None, callback: Optional[Callable[[Any], Any]] = None, _use_object_store: bool = False) -> List[Any]: ...
@overload
def get(ref: ObjectRef[R], *, timeout: Optional[float] = None, callback: Optional[Callable[[Any], Any]] = None, _use_object_store: bool = False) -> R: ...
@overload
def get(ref: Sequence[CompiledDAGRef], *, timeout: Optional[float] = None, callback: Optional[Callable[[Any], Any]] = None, _use_object_store: bool = False) -> List[Any]: ...
@overload
def get(ref: CompiledDAGRef, *, timeout: Optional[float] = None, callback: Optional[Callable[[Any], Any]] = None, _use_object_store: bool = False) -> Any: ...
def get(
    ref: Union[
        "ObjectRef[Any]",
        Sequence["ObjectRef[Any]"],
        CompiledDAGRef,
        Sequence[CompiledDAGRef],
    ],
    *,
    timeout: Optional[float] = None,
    callback: Optional[Callable[[Any], Any]] = None,
    _use_object_store: bool = False,
) -> Union[Any, List[Any]]:
    """Wrapper around ``ray.get()`` for use in Chia flows.

    Delegates to :func:`ray.get` and post-processes the result through
    the Chia profiler. Overloads mirror :func:`ray.get` so a single
    ``ObjectRef[R]`` returns ``R`` and a sequence of them returns
    ``List[R]``.

    ``callback``: when given, it is invoked with the resolved value (after the
    profiler unwrap) and its return value is what ``get`` returns. This keeps
    dispatch async — the ref is produced non-blocking by ``chia_remote`` — while
    running a post-resolution side effect/transform only when the value is
    fetched (e.g. ``get(ref, callback=fn)``).

    A :class:`ObjectRefCallback` carries its own callback, so ``get(orc)`` runs
    it without an explicit ``callback=``. If both are present they compose: the
    ref's own callback first, then the explicit ``callback``.
    """
    from chia.trace.profiler import get_profiler

    # An ObjectRefCallback carries a callback alongside its ref: resolve the
    # underlying ref, then apply its callback, then any explicit one. Only a
    # single ObjectRefCallback is supported (not lists containing them) — the
    # chia_remote callers that produce one always get() it on its own.
    if isinstance(ref, ObjectRefCallback):
        value = get(ref.ref, timeout=timeout, _use_object_store=_use_object_store)
        if ref.callback is not None:
            value = ref.callback(value)
        if callback is not None:
            value = callback(value)
        return value

    # ray.get's public overloads omit the _use_object_store kwarg, so mypy
    # can't match any overload when we forward it — suppress here.
    raw = ray.get(ref, timeout=timeout, _use_object_store=_use_object_store)  # type: ignore[call-overload]
    profiler = get_profiler()
    value = profiler.on_remote_complete(raw)
    if callback is not None:
        return callback(value)
    return value


def chia_cancel(ref, force=False):
    """Cancel a running ChiaFunction task, killing its subprocesses first.

    Looks up any subprocess PIDs spawned by the task, kills them on the
    correct remote nodes (using process group kill for ``start_new_session``
    subprocesses), then calls ``ray.cancel()``.
    """
    from chia.base.pid_registry import chia_cancel as _cancel
    return _cancel(ref, force=force)


# Re-export chia_wait + TrackedRef so callers have a single import surface.
from chia.base.chia_wait import TrackedRef, chia_wait  # noqa: E402,F401
