"""ChiaFunction profiler — execution timing, worker info, and dependency tracking.

Enabled when a ``ChiaProfileCollector`` Ray actor is running on the head
node.  Call :func:`start_collector` from the driver after ``ray.init()``
to enable profiling.  The actor is automatically cleaned up on
``ray.shutdown()`` or process exit.

When the collector is not running, all profiler methods are fast no-ops.

Event types emitted:
  - dispatch     : remote call dispatched via chia_remote()
  - complete     : remote call result retrieved via get()
  - local_start  : local ChiaFunction call started
  - local_end    : local ChiaFunction call finished
  - (custom)     : any event logged via log_event()
"""

from __future__ import annotations

import json
import os
import socket
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, List, Optional, Protocol
from pathlib import Path

# Types whose id() should NOT be tracked for dependency edges.
# Python interns/reuses small ints, short strings, booleans, and None,
# so id() matching would produce false positives.
_SKIP_DEPENDENCY_TYPES = (int, float, str, bytes, bool, type(None))

# Name used to register / look up the collector actor in Ray.
_COLLECTOR_ACTOR_NAME = "ChiaProfileCollector"


@dataclass
class _ProfiledResult:
    """Wrapper returned by the profiled trampoline on Ray workers.

    Carries the real return value plus metadata collected on the worker.
    ``get()`` unwraps this transparently.
    """
    value: Any
    worker_ip: str
    worker_id: str
    node_id: str
    exec_time_s: float
    call_id: str = ""
    func_name: str = ""
    display_name: str = ""
    extra: dict = field(default_factory=dict)


@dataclass
class _CallInfo:
    """Bookkeeping for an in-flight call."""
    call_id: str
    func_name: str
    options: dict
    dispatch_perf_ts: float  # perf_counter() at dispatch, for exec_time_s calculation
    is_remote: bool


# ------------------------------------------------------------------
# Ray actor collector
# ------------------------------------------------------------------

class ProfileCollectorActor:
    """Ray actor that collects profiler events in memory on the head node.

    Created once via :func:`start_collector`, looked up by workers via
    :func:`get_collector`.

    Events are kept in memory *and* appended as JSONL to a log file at
    ``/tmp/{ray_job_id}/{actor_name}.log``.

    Automatically cleaned up upon ray.shutdown() or process exit.
    """

    def __init__(self, log_dir: Optional[str] = None):
        import ray as _ray

        self._events: List[dict] = []

        # Build default log path: /tmp/ray/{job_id}/{actor_name}.log
        try:
            job_id = _ray.get_runtime_context().get_job_id()
        except Exception:
            job_id = "unknown_job"
        log_dir = log_dir or f"/tmp/ray/{job_id}"
        os.makedirs(log_dir, exist_ok=True)
        self._log_path = Path(os.path.join(log_dir, f"{_COLLECTOR_ACTOR_NAME}.log"))
        self._check_log_path()

    def _check_log_path(self):
        # Create parent directories if they don't exist
        self._log_path.parent.mkdir(parents=True, exist_ok=True)

        # Check if the actual file exists
        if self._log_path.exists():
            return
        else:
            # Create the file if needed
            self._log_path.touch()
            print("Directory and file created.")

    def record(self, event: dict) -> None:
        self._events.append(event)
        with open(self._log_path, "a") as f:
            f.write(json.dumps(event, default=str) + "\n")
            f.flush()

    def record_batch(self, events: List[dict]) -> None:
        self._events.extend(events)
    
        with open(self._log_path, "a") as f:
            for event in events:
                f.write(json.dumps(event, default=str) + "\n")
            f.flush()

    def get_events(self) -> List[dict]:
        return list(self._events)

    def get_log_path(self) -> str:
        """Return the path to the JSONL log file."""
        return str(self._log_path)

    def clear(self) -> None:
        self._events.clear()


def start_collector(log_dir: Optional[str] = None, namespace: Optional[str] = None) -> None:
    """Create the profile collector actor on the head node.

    Call this from the driver after ``ray.init()`` and before any profiled
    ``@ChiaFunction`` calls.  Idempotent — does nothing if the actor already
    exists.  Blocks until the actor is ready.

    Args:
        log_dir: Optional directory to store the JSONL log file.  Defaults to ``/tmp/ray/{job_id}``.
        namespace: Ray namespace for the named actor.  Defaults to
            ``None`` (uses the caller's current namespace).
    """
    global _collector_override
    import ray as _ray

    # Already running?
    if _collector_override is not None:
        return
    lookup_kwargs = {"namespace": namespace} if namespace else {}
    try:
        existing = _ray.get_actor(_COLLECTOR_ACTOR_NAME, **lookup_kwargs)
        _collector_override = existing
        return
    except ValueError:
        pass

    # Inherit the driver's runtime_env so the actor worker can import chia.
    ctx = _ray.get_runtime_context()
    try:
        runtime_env = dict(ctx.runtime_env or {})
    except Exception:
        runtime_env = {}
    # Pin the actor to the head node (where the driver runs).
    head_node_id = ctx.get_node_id()
    scheduling = _ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
        node_id=head_node_id, soft=False,
    )
    actor = _ray.remote(ProfileCollectorActor).options(
        name=_COLLECTOR_ACTOR_NAME,
        num_cpus=0,
        runtime_env=runtime_env,
        scheduling_strategy=scheduling,
        **({"namespace": namespace} if namespace else {}),
    ).remote(log_dir)
    # Block until the actor is live and responding.
    _ray.get(actor.get_events.remote())
    _collector_override = actor


def stop_collector() -> None:
    """Kill the profile collector actor started by ``start_collector()``.
    Call from the driver once all profiled work is done. Idempotent."""
    global _collector_override
    import ray as _ray

    if _collector_override is not None:
        _ray.kill(_collector_override)
        _collector_override = None


# Cached actor handle set by start_collector().  get_collector() checks
# this first before falling back to a named-actor lookup.
_collector_override = None


def get_collector(namespace: Optional[str] = None):
    """Return a handle to the profile collector actor, or ``None``.

    Args:
        namespace: Ray namespace to look up.  Defaults to ``None``
            (uses the caller's current namespace).
    """
    if _collector_override is not None:
        return _collector_override
    import ray as _ray
    lookup_kwargs = {"namespace": namespace} if namespace else {}
    try:
        return _ray.get_actor(_COLLECTOR_ACTOR_NAME, **lookup_kwargs)
    except ValueError:
        return None



# ------------------------------------------------------------------
# ChiaProfiler
# ------------------------------------------------------------------

class ChiaProfiler:
    """Singleton profiler for ChiaFunction calls.

    Enabled when a ``ChiaProfileCollector`` Ray actor is running.
    All events are sent to the actor via fire-and-forget remote calls.
    When the actor is not available, every method is a fast no-op.
    """

    def __init__(self, namespace: Optional[str] = None):
        # Check if the collector actor is available.
        self._collector = get_collector(namespace)
        self._enabled = self._collector is not None
        self._extra = threading.local()  # per-task extra metadata
        if not self._enabled:
            return

        # Globally unique prefix for call IDs — uses Ray worker_id if
        # available, otherwise falls back to a driver-specific ID.
        self._worker_id = self._resolve_worker_id()
        self._prefix = self._worker_id[:12]
        self._counter = 0

        # Dependency tracking: id(resolved_value) -> call_id
        self._value_to_call: dict[int, str] = {}

    def __reduce__(self):
        # On unpickle, call get_profiler() to resolve to the worker's singleton.
        return (get_profiler, ())

    def __getstate__(self):
        state = self.__dict__.copy()
        # threading.local is not picklable; recreate on deserialization
        state.pop('_extra', None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._extra = threading.local()

    @property
    def enabled(self) -> bool:
        return self._enabled

    def add_info(self, info: dict):
        """Augment the current call's profiler event with extra metadata.

        Call from within any @ChiaFunction body. The info dict is merged
        into the complete event. Thread-local, so safe for concurrent tasks.
        """
        if not self._enabled:
            return
        if not hasattr(self._extra, 'data'):
            self._extra.data = {}
        self._extra.data.update(info)

    def _pop_extra(self) -> dict:
        """Retrieve and clear extra info for the current call."""
        data = getattr(self._extra, 'data', {})
        self._extra.data = {}
        return data

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_worker_id() -> str:
        """Return the Ray worker_id, or a driver-unique fallback."""
        try:
            import ray
            return ray.get_runtime_context().get_worker_id()
        except Exception:
            return f"driver_{uuid.uuid4().hex[:12]}"

    def next_call_id(self) -> str:
        """Generate a globally unique call ID."""
        self._counter += 1
        return f"{self._prefix}_{self._counter:06d}"

    def _write(self, event: dict):
        """Send an event to the collector actor to be recorded."""
        try:
            self._collector.record.remote(event)
        except Exception:
            pass  # actor unavailable

    def _register_result(self, value: Any, call_id: str):
        """Store ``id(value) -> call_id`` for later dependency detection."""
        if not isinstance(value, _SKIP_DEPENDENCY_TYPES):
            self._value_to_call[id(value)] = call_id

    def _scan_args_for_edges(self, args: tuple, kwargs: dict) -> list[str]:
        """Return dependent ``call_id``s for args that match a previous result."""
        edges: list[str] = []
        seen: set[str] = set()
        for arg in args:
            if not isinstance(arg, _SKIP_DEPENDENCY_TYPES):
                cid = self._value_to_call.get(id(arg))
                if cid and cid not in seen:
                    edges.append(cid)
                    seen.add(cid)
        for arg in kwargs.values():
            if not isinstance(arg, _SKIP_DEPENDENCY_TYPES):
                cid = self._value_to_call.get(id(arg))
                if cid and cid not in seen:
                    edges.append(cid)
                    seen.add(cid)
        return edges

    # ------------------------------------------------------------------
    # Remote call hooks
    # ------------------------------------------------------------------

    def prepare_dispatch(
        self,
        options: dict,
        args: tuple,
        kwargs: dict,
        display_name: Optional[str] = None,
    ) -> dict:
        """Compute dispatch metadata on the driver before ``.remote()``.

        Returns a dict to be forwarded to the trampoline as ``dispatch_meta``.
        """
        if not self._enabled:
            return {}
        meta = {
            "obj_ref_deps": self._scan_args_for_edges(args, kwargs),
            "caller_worker_id": self._worker_id,
        }
        if display_name:
            meta["display_name"] = str(display_name)
        return meta

    def on_worker_dispatch(self, call_id: str, func_name: str,
                           worker_ip: str, worker_id: str, node_id: str,
                           resources: dict, obj_ref_deps: list,
                           caller_worker_id: str,
                           display_name: str = ""):
        """Emit a dispatch event from the worker at actual task start time.

        Called by ``_chia_trampoline_profiled`` on the Ray worker.
        """
        if not self._enabled:
            return
        event = {
            "type": "dispatch",
            "call_id": call_id,
            "func": func_name,
            "ts": time.time(),
            "is_remote": True,
            "caller_worker_id": caller_worker_id,
            "worker_ip": worker_ip,
            "worker_id": worker_id,
            "node_id": node_id,
            "resources": resources,
            "obj_ref_deps": obj_ref_deps,
        }
        if display_name:
            event["display_name"] = display_name
        self._write(event)

    def on_remote_complete(self, raw_result):
        """Called in ``get()`` after ``ray.get()`` returns.

        Unwraps ``_ProfiledResult`` if present, registers the value for
        dependency tracking, and returns the plain value.
        """
        if isinstance(raw_result, _ProfiledResult):
            value = raw_result.value
        else:
            value = raw_result

        if self._enabled and isinstance(raw_result, _ProfiledResult):
            if raw_result.call_id:
                self._register_result(value, raw_result.call_id)

        return value

    def on_worker_complete(self, call_id: str, func_name: str,
                           end_ts: float, exec_time: float,
                           worker_ip: str, worker_id: str, node_id: str,
                           extra: dict | None = None,
                           display_name: str = ""):
        """Emit a complete event from the worker at the actual function end time.

        Called by ``_chia_trampoline_profiled`` on the Ray worker.
        """
        if not self._enabled:
            return
        event = {
            "type": "complete",
            "call_id": call_id,
            "func": func_name,
            "ts": end_ts,
            "exec_time_s": round(exec_time, 4),
            "worker_ip": worker_ip,
            "worker_id": worker_id,
            "node_id": node_id,
            "is_remote": True,
        }
        if display_name:
            event["display_name"] = display_name
        if extra:
            event["extra"] = extra
        self._write(event)

    # ------------------------------------------------------------------
    # Local call hooks
    # ------------------------------------------------------------------

    def on_local_start(self, func, args: tuple, kwargs: dict):
        """Called at the start of a local ``ChiaFunction`` ``__call__``."""
        if not self._enabled:
            return None
        call_id = self.next_call_id()
        obj_ref_dep_edges = self._scan_args_for_edges(args, kwargs)

        info = _CallInfo(
            call_id=call_id,
            func_name=func.__name__,
            options={},
            dispatch_perf_ts=time.perf_counter(),  # for exec_time_s
            is_remote=False,
        )

        try:
            local_ip = socket.gethostbyname(socket.gethostname())
        except Exception:
            local_ip = "unknown"

        self._write({
            "type": "local_start",
            "call_id": call_id,
            "func": func.__name__,
            "ts": time.time(),  # wall-clock timestamp
            "is_remote": False,
            "worker_id": self._worker_id,
            "worker_ip": local_ip,
            "obj_ref_deps": obj_ref_dep_edges,
        })
        return info

    def on_local_end(self, info: _CallInfo | None, result):
        """Called after a local ``ChiaFunction`` ``__call__`` returns."""
        if not self._enabled or info is None:
            return
        # exec_time_s from perf_counter (high-res monotonic duration)
        exec_time = time.perf_counter() - info.dispatch_perf_ts

        try:
            local_ip = socket.gethostbyname(socket.gethostname())
        except Exception:
            local_ip = "unknown"

        # add_info() metadata accumulated during the local call body. The
        # remote path pops this in the worker trampoline; locally-run
        # ChiaFunctions pop it here so add_info() surfaces identically.
        # Thread-local, so concurrent local calls don't cross metadata.
        extra = self._pop_extra()

        event = {
            "type": "local_end",
            "call_id": info.call_id,
            "func": info.func_name,
            "ts": time.time(),  # wall-clock timestamp
            "exec_time_s": round(exec_time, 4),
            "worker_id": self._worker_id,
            "worker_ip": local_ip,
            "is_remote": False,
        }
        if extra:
            event["extra"] = extra
        self._write(event)
        self._register_result(result, info.call_id)

    # ------------------------------------------------------------------
    # General-purpose custom events
    # ------------------------------------------------------------------

    def log_event(self, event_type: str, **kwargs):
        """Log a custom event to the profiler trace.

        Any code can call this to insert arbitrary metadata into the
        profile.  The event is timestamped and tagged with the current
        ``worker_id`` automatically.

        Args:
            event_type: A short label for the event (e.g., ``"prompt"``,
                ``"checkpoint"``, ``"tool_attach"``).
            **kwargs: Arbitrary key-value pairs included in the event.
        """
        if not self._enabled:
            return
        event = {
            "type": event_type,
            "ts": time.time(),  # wall-clock timestamp
            "worker_id": self._worker_id,
            **kwargs,
        }
        self._write(event)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    
    def close(self):
        pass  # No local resources to clean up.


# ------------------------------------------------------------------
# Singleton
# ------------------------------------------------------------------

_profiler: Optional[ChiaProfiler] = None
_profiler_lock = threading.Lock()


def get_profiler() -> ChiaProfiler:
    """Return the singleton ChiaProfiler, creating it on first call.

    This is a plain function so that pickle stores it by qualified name
    (``chia.trace.profiler.get_profiler``). Ray can serialize references
    to it without touching the lock or instance.
    """
    global _profiler
    if _profiler is None:
        with _profiler_lock:
            if _profiler is None:
                _profiler = ChiaProfiler()
    return _profiler


def reset_profiler() -> None:
    """Discard the current singleton so the next call recreates it."""
    global _profiler
    with _profiler_lock:
        _profiler = None


# Backward compatibility: allow get_profiler.reset()
get_profiler.reset = reset_profiler  # type: ignore[attr-defined]
