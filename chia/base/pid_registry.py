"""PID tracking and cancellation for ChiaFunction remote tasks.

Tracks subprocess PIDs spawned during ``@ChiaFunction`` remote execution
and provides :func:`chia_cancel` to kill those process trees before
cancelling the Ray task.

The Popen hook is scoped per-task via ``threading.local`` so concurrent
Ray tasks on the same worker don't interfere with each other.
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import threading
import time
from contextlib import contextmanager
from typing import Any

import ray

logger = logging.getLogger(__name__)

_REGISTRY_ACTOR_NAME = "ChiaPidRegistry"
_REGISTRY_NAMESPACE = "chia"

# Max time to wait for a SIGTERM'd process to exit before escalating to
# SIGKILL.  The kill helpers poll for actual death and return as soon as
# the target is gone, so this is a ceiling, not a fixed delay.
_KILL_GRACE_SECONDS = 25.0
_KILL_POLL_INTERVAL = 0.1

# ---------------------------------------------------------------------------
# Ray actor — centralized PID registry on the head node
# ---------------------------------------------------------------------------


class PidRegistryActor:
    """Ray actor that maps task IDs to subprocess PIDs.

    Created lazily on first registration, looked up by workers and the
    driver via :func:`_get_registry`.
    """

    def __init__(self):
        # task_id_hex -> [(node_id, pid, is_pgid)]
        self._tasks: dict[str, list[tuple[str, int, bool]]] = {}

    def register(self, task_id: str, node_id: str, pid: int, is_pgid: bool) -> None:
        self._tasks.setdefault(task_id, []).append((node_id, pid, is_pgid))

    def unregister(self, task_id: str) -> None:
        self._tasks.pop(task_id, None)

    def get_and_remove(self, task_id: str) -> list[tuple[str, int, bool]]:
        return self._tasks.pop(task_id, [])

    def kill_all(self, grace: float = _KILL_GRACE_SECONDS) -> int:
        """Kill all tracked subprocess PIDs across all nodes.

        Dispatches remote kill tasks and waits for completion.  Each kill
        sends SIGTERM, polls for the process to exit, and only escalates to
        SIGKILL if it is still alive after ``grace`` seconds.  Returns the
        number of PIDs targeted.
        """
        all_pids = []
        for pid_list in self._tasks.values():
            all_pids.extend(pid_list)
        self._tasks.clear()
        if not all_pids:
            return 0

        kill_refs = []
        for node_id, pid, is_pgid in all_pids:
            try:
                scheduling = ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=node_id, soft=False,
                )
                kill_ref = ray.remote(_kill_pid).options(
                    num_cpus=0,
                    scheduling_strategy=scheduling,
                ).remote(pid, is_pgid, grace)
                kill_refs.append(kill_ref)
            except Exception:
                pass
        if kill_refs:
            try:
                # Each _kill_pid blocks up to `grace`; allow a margin for
                # dispatch/scheduling overhead before giving up the wait.
                ray.get(kill_refs, timeout=grace + 10)
            except Exception:
                pass
        return len(all_pids)


# ---------------------------------------------------------------------------
# Cached actor handle (same pattern as profiler.py get_profiler / get_collector)
# ---------------------------------------------------------------------------

_registry_handle = None
_registry_lock = threading.Lock()
_cleanup_installed = False


def _install_driver_cleanup():
    """Install a SIGTERM handler on the driver so ``ray job stop`` kills tracked PIDs.

    Only effective on the main thread (signal handlers can't be set elsewhere).
    Chains to any previously-installed SIGTERM handler after cleanup.
    """
    global _cleanup_installed
    if _cleanup_installed:
        return
    _cleanup_installed = True

    prev_handler = signal.getsignal(signal.SIGTERM)

    def _sigterm_cleanup(signum, frame):
        # Read the cached handle directly — avoid _get_registry() which
        # takes a lock that the main thread may already hold.
        handle = _registry_handle
        if handle is not None:
            try:
                n = ray.get(handle.kill_all.remote(), timeout=35)
                if n:
                    logger.info(f"SIGTERM cleanup: killed {n} tracked subprocess(es)")
            except Exception:
                logger.debug("SIGTERM cleanup: kill_all failed", exc_info=True)
        if callable(prev_handler) and prev_handler not in (signal.SIG_DFL, signal.SIG_IGN):
            prev_handler(signum, frame)
        else:
            raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, _sigterm_cleanup)


def _reset_registry():
    """Discard the cached registry handle so the next call recreates it."""
    global _registry_handle
    with _registry_lock:
        _registry_handle = None


def _get_registry():
    """Return a handle to the PID registry actor, creating it if needed.

    Returns ``None`` if Ray is not initialized.  Inherits the driver's
    ``runtime_env`` and pins the actor to the current node (same pattern
    as :func:`chia.trace.profiler.start_collector`).
    """
    global _registry_handle
    if _registry_handle is not None:
        return _registry_handle
    with _registry_lock:
        if _registry_handle is not None:
            return _registry_handle
        if not ray.is_initialized():
            return None
        try:
            _registry_handle = ray.get_actor(
                _REGISTRY_ACTOR_NAME, namespace=_REGISTRY_NAMESPACE,
            )
        except ValueError:
            # Actor doesn't exist yet — create it.
            try:
                ctx = ray.get_runtime_context()
                local_node_id = ctx.get_node_id()
                scheduling = ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=local_node_id, soft=False,
                )
            except Exception:
                scheduling = None
            opts = {
                "name": _REGISTRY_ACTOR_NAME,
                "namespace": _REGISTRY_NAMESPACE,
                "num_cpus": 0,
            }
            if scheduling is not None:
                opts["scheduling_strategy"] = scheduling
            try:
                _registry_handle = ray.remote(PidRegistryActor).options(**opts).remote()
            except Exception:
                # Another process created the actor first — just look it up.
                try:
                    _registry_handle = ray.get_actor(
                        _REGISTRY_ACTOR_NAME, namespace=_REGISTRY_NAMESPACE,
                    )
                except Exception:
                    logger.debug("Failed to get/create PID registry actor",
                                 exc_info=True)
                    return None
        except Exception:
            logger.debug("Failed to get/create PID registry actor", exc_info=True)
            return None

        # On the driver (no task context), install a SIGTERM handler so
        # ``ray job stop`` kills all tracked subprocesses before exiting.
        try:
            ray.get_runtime_context().get_task_id()
        except RuntimeError:
            # No task ID → we're on the driver.
            if threading.current_thread() is threading.main_thread():
                try:
                    _install_driver_cleanup()
                except Exception:
                    pass

        return _registry_handle


# ---------------------------------------------------------------------------
# Thread-local Popen hook
# ---------------------------------------------------------------------------

_tls = threading.local()
_hook_installed = False
_hook_lock = threading.Lock()
_original_popen_init = None


def _install_popen_hook():
    """Wrap ``subprocess.Popen.__init__`` to track PIDs. Idempotent."""
    global _hook_installed, _original_popen_init
    if _hook_installed:
        return
    with _hook_lock:
        if _hook_installed:
            return
        _original_popen_init = subprocess.Popen.__init__

        def _tracked_popen_init(self, *args, **kwargs):
            _original_popen_init(self, *args, **kwargs)
            task_id = getattr(_tls, "pid_task_id", None)
            if task_id is not None:
                is_pgid = kwargs.get("start_new_session", False)
                # Store locally so the trampoline can kill on BaseException
                # (e.g. ray job stop) without needing the registry.
                local_list = getattr(_tls, "tracked_pids", None)
                if local_list is not None:
                    local_list.append((self.pid, is_pgid))
                # Also register with the central actor for chia_cancel().
                node_id = getattr(_tls, "pid_node_id", "")
                registry = _get_registry()
                if registry is not None:
                    try:
                        registry.register.remote(task_id, node_id, self.pid, is_pgid)
                    except Exception:
                        pass

        subprocess.Popen.__init__ = _tracked_popen_init
        _hook_installed = True


# ---------------------------------------------------------------------------
# Context manager for trampolines
# ---------------------------------------------------------------------------


def _proc_state_and_pgrp(pid: int) -> tuple[str, int] | None:
    """Return ``(state, pgrp)`` from ``/proc/<pid>/stat``, or ``None`` if gone.

    The ``comm`` field is wrapped in parentheses and may itself contain
    spaces or parentheses, so we split on everything after the final
    ``)`` — state is then the first field, pgrp the third.
    """
    try:
        with open(f"/proc/{pid}/stat", "rb") as f:
            data = f.read()
    except OSError:
        return None
    rparen = data.rfind(b")")
    if rparen == -1:
        return None
    fields = data[rparen + 2:].split()
    try:
        return fields[0].decode(), int(fields[2])
    except (IndexError, ValueError, UnicodeDecodeError):
        return None


def _is_running(pid: int, is_pgid: bool) -> bool:
    """True if a *live* (non-zombie) process still exists for ``pid``.

    A terminated-but-unreaped process (zombie, state ``Z``) counts as
    dead: it is no longer executing, which is what callers care about.
    ``os.kill(pid, 0)`` cannot make this distinction — it reports zombies
    as alive — so we read ``/proc`` directly.  For a process group
    (``is_pgid``), returns True if any group member is still live.
    """
    if is_pgid:
        try:
            entries = os.listdir("/proc")
        except OSError:
            return False
        for entry in entries:
            if not entry.isdigit():
                continue
            info = _proc_state_and_pgrp(int(entry))
            if info is None:
                continue
            state, pgrp = info
            if pgrp == pid and state != "Z":
                return True
        return False
    info = _proc_state_and_pgrp(pid)
    return info is not None and info[0] != "Z"


def _wait_for_death(
    targets: list[tuple[int, bool]],
    grace: float = _KILL_GRACE_SECONDS,
    poll_interval: float = _KILL_POLL_INTERVAL,
) -> list[tuple[int, bool]]:
    """Poll until every target is dead or ``grace`` seconds elapse.

    ``targets`` is a list of ``(pid, is_pgid)``.  Returns the targets
    still alive when the grace period expired (empty if all died first),
    so callers know which ones to SIGKILL.  Returns as soon as the last
    target dies — it does not wait out the full grace period.
    """
    deadline = time.monotonic() + grace
    alive = list(targets)
    while alive:
        alive = [t for t in alive if _is_running(*t)]
        if not alive or time.monotonic() >= deadline:
            break
        time.sleep(poll_interval)
    return alive


def _kill_local_pids(pids: list[tuple[int, bool]]) -> None:
    """Kill all tracked PIDs locally (SIGTERM, then SIGKILL survivors).

    Called from the trampoline when a ``BaseException`` is caught (e.g.
    ``ray job stop`` delivering ``TaskCancelledError``).  This is the
    local-cleanup counterpart to :func:`chia_cancel`'s remote-kill path.

    Returns as soon as every target has exited; only escalates to
    SIGKILL for processes still alive after ``_KILL_GRACE_SECONDS``.
    """
    sent = []
    for pid, is_pgid in pids:
        kill_fn = os.killpg if is_pgid else os.kill
        try:
            kill_fn(pid, signal.SIGTERM)
            sent.append((pid, is_pgid))
        except OSError:
            continue  # already dead
    for pid, is_pgid in _wait_for_death(sent):
        kill_fn = os.killpg if is_pgid else os.kill
        try:
            kill_fn(pid, signal.SIGKILL)
        except OSError:
            pass


@contextmanager
def _pid_tracking_scope():
    """Track subprocess PIDs for the current Ray task.

    Sets thread-local state so the Popen hook registers PIDs under
    this task's ID.  On normal exit, cleans up the registry entry.
    On ``BaseException`` (e.g. ``ray job stop``), kills all tracked
    subprocesses locally before re-raising.
    """
    _install_popen_hook()
    registry = _get_registry()
    if registry is None:
        yield
        return

    try:
        task_id = ray.get_runtime_context().get_task_id()
        node_id = ray.get_runtime_context().get_node_id()
    except Exception:
        yield
        return

    _tls.pid_task_id = task_id
    _tls.pid_node_id = node_id
    _tls.tracked_pids = []
    try:
        yield
    except BaseException:
        # ray job stop / ray.cancel delivers TaskCancelledError here.
        # Kill subprocesses locally — we're on the same node.
        tracked = list(_tls.tracked_pids)
        if tracked:
            logger.info(f"Task interrupted — killing {len(tracked)} tracked subprocess(es)")
            _kill_local_pids(tracked)
        raise
    finally:
        _tls.pid_task_id = None
        _tls.pid_node_id = None
        _tls.tracked_pids = []
        try:
            registry.unregister.remote(task_id)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Remote kill function — dispatched to the target node
# ---------------------------------------------------------------------------


def _kill_pid(pid: int, is_pgid: bool, grace: float = _KILL_GRACE_SECONDS) -> None:
    """Kill a process (or process group): SIGTERM, then SIGKILL if it lingers.

    Returns as soon as the target exits.  Only if it is still alive after
    ``grace`` seconds do we escalate to SIGKILL.
    """
    kill_fn = os.killpg if is_pgid else os.kill
    try:
        kill_fn(pid, signal.SIGTERM)
    except OSError:
        return  # already dead
    if _wait_for_death([(pid, is_pgid)], grace):
        try:
            kill_fn(pid, signal.SIGKILL)
        except OSError:
            pass



# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def chia_cancel(ref: ray.ObjectRef, force: bool = False) -> None:
    """Cancel a running ChiaFunction task, killing its subprocesses first.

    Looks up any subprocess PIDs spawned by the task, kills them on the
    correct remote nodes (using process group kill for ``start_new_session``
    subprocesses), then calls ``ray.cancel()``.

    Args:
        ref: The ``ObjectRef`` returned by ``chia_remote()``.
        force: Passed through to ``ray.cancel()``. If ``True``, the Ray
            worker is killed; if ``False`` (default), a
            ``TaskCancelledError`` is raised cooperatively.
    """
    registry = _get_registry()
    if registry is not None:
        try:
            task_id = ref.task_id().hex()
            pids = ray.get(registry.get_and_remove.remote(task_id))
            if pids:
                kill_refs = []
                for node_id, pid, is_pgid in pids:
                    scheduling = ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                        node_id=node_id, soft=False,
                    )
                    kill_ref = ray.remote(_kill_pid).options(
                        num_cpus=0,
                        scheduling_strategy=scheduling,
                    ).remote(pid, is_pgid)
                    kill_refs.append(kill_ref)
                # Wait for kills to complete before cancelling the task.
                try:
                    ray.get(kill_refs, timeout=35)
                except Exception:
                    pass
        except Exception:
            logger.debug("PID cleanup failed, falling back to ray.cancel only",
                         exc_info=True)

    ray.cancel(ref, force=force)
