from __future__ import annotations

import os
import signal
import subprocess
from typing import Dict, Optional

from chia.base.tools.AsyncJobTool import AsyncJobTool

#: Keep returned output bounded — a long build/test run can emit megabytes.
_MAX_OUTPUT_LINES = 600


def _tail(text: str, n: int = _MAX_OUTPUT_LINES) -> str:
    return "\n".join(text.splitlines()[-n:])


class AsyncBashTool(AsyncJobTool):
    """MCP bash tool for commands that may run for minutes, built on AsyncJobTool.

    Like :class:`chia.base.tools.BashTool.BashTool` it runs shell commands in a
    working directory, but a single command here may run long enough (a full
    build, a test suite) that a *synchronous* MCP call loses its response on the
    streamable-HTTP transport and hangs the agent. So this tool never blocks the
    transport for long: ``<name>_run`` starts the command in the background and
    waits only a short, bounded slice for it to finish; if it is still running it
    returns ``done=false`` and the agent polls ``<name>_run_status`` until
    ``done=true``.

    Fast commands therefore complete inside the single ``run`` call (they finish
    within the initial wait); only genuinely long commands require a poll. One
    command runs at a time per tool instance — start a command, then poll it to
    completion before starting the next (a second ``run`` while one is in flight
    returns ``started=false``).

    Result dict on completion: ``{exit_code: int, output: str}`` (combined
    stdout+stderr, tail-truncated). Mirrors :class:`BashTool`'s combined-stream
    behaviour. ``work_dir`` and ``env`` are plain attributes and pickle normally;
    the job/threading primitives are dropped + recreated by ``AsyncJobTool``.
    """

    def __init__(
        self,
        name: str,
        work_dir: str = "/",
        env: Optional[Dict[str, str]] = None,
        task_options: Optional[Dict] = None,
    ):
        super().__init__(name, task_options=task_options)
        self.work_dir = work_dir
        self.env = dict(env) if env else None
        self.mcp.add_tool(self.run, name=f"{name}_run")
        self.mcp.add_tool(self.run_status, name=f"{name}_run_status")
        super().__post_init__()

    def _exec(self, command: str) -> dict:
        """Run *command* to completion in its own process group; return result.

        ``start_new_session=True`` + ``killpg`` on failure so a child that keeps
        the stdout pipe open can't wedge ``communicate()`` forever (same hazard,
        and fix, as :class:`BashTool` / the build helpers)."""
        run_env = {**os.environ, **self.env} if self.env else None
        try:
            proc = subprocess.Popen(
                command, shell=True, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, text=True, cwd=self.work_dir,
                start_new_session=True, env=run_env)
        except Exception as e:  # noqa: BLE001
            return {"exit_code": -1, "output": f"failed to spawn: {e}"}
        try:
            out, _ = proc.communicate()
            return {"exit_code": proc.returncode, "output": _tail(out or "") or "(no output)"}
        except Exception as e:  # noqa: BLE001
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            return {"exit_code": -1, "output": f"error: {e}"}

    def run(self, command: str, wait_seconds: int = 60) -> dict:
        """Run a bash command in the BACKGROUND, waiting up to *wait_seconds*
        (capped at 120) for it to finish.

        If it finishes in time, returns ``done=true`` with ``{exit_code,
        output}``. If it is still running, returns ``done=false`` — then poll
        ``<name>_run_status`` until ``done=true``. One command at a time: if a
        command is already running, returns ``started=false`` and you should poll
        status instead of starting another.
        """
        start = self._job_start(lambda: self._exec(command))
        if not start.get("started"):
            return {**start, "done": False}
        return self._job_status(wait_seconds)

    def run_status(self, wait_seconds: int = 60) -> dict:
        """Wait up to *wait_seconds* (capped at 120) for the in-flight command,
        then report. ``done=true`` splices in ``{exit_code, output}``."""
        return self._job_status(wait_seconds)
