from __future__ import annotations

import threading
from typing import Callable, Dict, Optional

from chia.base.tools.ChiaTool import ChiaTool


class AsyncJobTool(ChiaTool):
    """Base for MCP tools that run a long job in the background, then poll for it.

    Why this exists: a single MCP tool call that blocks for minutes holds the
    streamable-HTTP connection open with no traffic for the whole job. That
    transport can lose the response for such a long synchronous call, stranding
    the result and hanging the agent forever (observed on real runs). The fix is
    to never block the transport for long: a ``start`` call kicks the work off in
    a daemon thread and returns immediately, and a ``status`` call long-polls in
    short, bounded chunks (<=120s) until ``done=true``.

    Subclasses register their own start/status tool methods that delegate to
    :meth:`_job_start` and :meth:`_job_status`::

        class BuildTool(AsyncJobTool):
            def __init__(self, name, task_options=None):
                super().__init__(name, task_options=task_options)
                self.mcp.add_tool(self.build, name=f"{name}_build")
                self.mcp.add_tool(self.build_status, name=f"{name}_build_status")
                super().__post_init__()

            def build(self, targets=None) -> dict:
                return self._job_start(lambda: do_build(targets))

            def build_status(self, wait_seconds: int = 60) -> dict:
                return self._job_status(wait_seconds)

    One job runs at a time per tool instance; calling ``_job_start`` while a job
    is in flight returns ``{"started": False, "running": True}`` rather than
    starting a second. The ``work`` callable must return a ``dict``; on
    ``done=true`` its keys are spliced into the status result.

    The job's threading primitives are not picklable, but ChiaTool serializes the
    tool object to the server actor (and to the LLM worker), so they are dropped
    in ``__getstate__`` and recreated in ``__setstate__``.
    """

    #: Hard cap on how long a single status poll may block, so the MCP call
    #: always returns promptly even if the caller passes a large wait.
    _MAX_POLL_SECONDS = 120

    def __init__(self, name: str, task_options: Optional[Dict] = None):
        super().__init__(name, task_options=task_options)
        self._init_job_state()

    def _init_job_state(self) -> None:
        self._job_lock = threading.Lock()
        self._job_done = threading.Event()
        self._job_result: Optional[dict] = None
        self._job_thread: Optional[threading.Thread] = None

    def _ensure_job_state(self) -> None:
        """Create the job-state primitives if they don't exist yet.

        ``__init__`` sets these up for the explicit-init idiom, but a subclass
        built with the ``setup()`` idiom never runs ``AsyncJobTool.__init__``
        (the auto-init brackets only ``ChiaTool.__init__``, skipping intermediate
        bases), so initialize them lazily on first use too. This keeps the job
        machinery working under both idioms with no per-subclass boilerplate.
        """
        if not hasattr(self, "_job_lock"):
            self._init_job_state()

    def _job_start(self, work: Callable[[], dict]) -> dict:
        """Run callable *work* (-> result dict) in a daemon thread; return now.

        Returns ``{"started": True, "running": True}`` on launch, or
        ``{"started": False, "running": True, ...}`` if a job is already running.
        """
        self._ensure_job_state()
        with self._job_lock:
            if self._job_thread is not None and self._job_thread.is_alive():
                return {"started": False, "running": True,
                        "note": "a job is already running here; poll status"}
            self._job_done.clear()
            self._job_result = None

            def _run() -> None:
                r = work()
                with self._job_lock:
                    self._job_result = r
                self._job_done.set()

            self._job_thread = threading.Thread(target=_run, daemon=True)
            self._job_thread.start()
        return {"started": True, "running": True}

    def _job_status(self, wait_seconds: int) -> dict:
        """Block up to ``wait_seconds`` (capped at ``_MAX_POLL_SECONDS`` so the
        MCP call stays short), then report. ``done=true`` splices in the job's
        result dict; otherwise returns ``{"done": False, "running": ...}``."""
        self._ensure_job_state()
        finished = self._job_done.wait(
            timeout=max(0, min(int(wait_seconds), self._MAX_POLL_SECONDS)))
        with self._job_lock:
            if finished and self._job_result is not None:
                return {"done": True, "running": False, **self._job_result}
            if self._job_thread is None:
                return {"done": False, "running": False, "note": "nothing started yet"}
            return {"done": False, "running": True, "note": "still running; poll again"}

    # threading primitives can't be pickled; ChiaTool pickles `self`.
    def __getstate__(self):
        state = super().__getstate__()
        for k in ("_job_lock", "_job_done", "_job_result", "_job_thread"):
            state.pop(k, None)
        return state

    def __setstate__(self, state):
        super().__setstate__(state)
        self._init_job_state()
