"""Google Antigravity CLI LLM backend.

``AntigravityLLM`` wraps Google's Antigravity CLI (the ``agy`` binary, installed
via ``curl -fsSL https://antigravity.google/cli/install.sh | bash``) behind the
same synchronous ``prompt`` shape as the other Chia LLM backends. It runs
``agy --print --output-format stream-json`` (non-interactive "print mode"): agy
emits one NDJSON event per line — ``init`` (model, cwd, tools), ``step_update``
(user input, agent text, tool calls with their arguments and output, per-step
token usage) and a final ``result`` (status, response text, conversation id,
total usage). We parse those into a readable turn-by-turn transcript on
``stream_result`` and the final answer on ``result``.

**Auth is OAuth-only.** ``agy`` signs in with a Google account ("Antigravity"
  / Gemini Code Assist) and stores a refresh token on disk; in a container it
  uses file-based token storage under the Gemini config dir. There is no
  API-key path.

**Sessions** the first call starts a conversation and every later
call can pass ``--conversation <id>`` so the model keeps its memory. agy persists
each conversation as a SQLite file ``<gemini_dir>/antigravity-cli/conversations/
<conversation_id>.db``; because ``prompt`` may land on a different
``antigravity_creds`` worker each call, we checkpoint and carry those bytes on
:class:`AntigravityQueryResult` (``session_transcript``) and re-paste them before
the next run, exactly like Claude's ``.jsonl`` transcript. The
``_session_tracked`` wrapper (shared with the Claude backend) harvests them on
``get()`` so callers need no bookkeeping::

    llm = AntigravityLLM(resume_session=True)
    a = get(llm.prompt.chia_remote(llm, "remember X", tools))
    b = get(llm.prompt.chia_remote(llm, "what was X?", tools))   # same conversation

**Tools** are exposed to agy as MCP servers. agy only reads them from the fixed
path ``~/.gemini/config/mcp_config.json`` and waits for every listed server
before answering, so each run gets a private ``HOME`` holding exactly its own
tool list (with ``~/.gemini/antigravity-cli`` symlinked to the real one); the
user's own ``~/.gemini/config`` is never modified and concurrent prompts with
different tool sets don't interfere.

The system prompt is folded into the user message (print mode has no
``--system-prompt`` flag), mirroring :class:`~chia.models.codex.CodexLLM`.
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING
from uuid import uuid4

import ray

from chia.base.ChiaFunction import ChiaFunction
from chia.base.chia_log import chia_log
from chia.base.llm_call import QueryResult, LLMCallBase, UNSET
from chia.models.claude import _session_tracked

if TYPE_CHECKING:
    from chia.base.tools.ChiaTool import ChiaTool


class AntigravityError(Exception):
    """Base for Antigravity CLI errors. Subclasses are Ray-serializable."""

    error_type = "unknown"

    def __init__(self, node_id: str, exit_code: int = -1, raw_message: str = ""):
        self.node_id = node_id
        self.exit_code = exit_code
        self.raw_message = raw_message
        super().__init__(f"{self.error_type} on {node_id}: {raw_message[:200]}")

    def __reduce__(self):
        return (self.__class__, (self.node_id, self.exit_code, self.raw_message))


class RateLimitError(AntigravityError):
    error_type = "rate_limit"

    def __init__(
        self,
        node_id: str,
        reset_time: datetime | None = None,
        raw_message: str = "",
        exit_code: int = -1,
    ):
        self.reset_time = reset_time or datetime.now(timezone.utc) + timedelta(minutes=1)
        super().__init__(node_id=node_id, exit_code=exit_code, raw_message=raw_message)

    def __reduce__(self):
        return (
            self.__class__,
            (self.node_id, self.reset_time, self.raw_message, self.exit_code),
        )


class AuthenticationError(AntigravityError):
    error_type = "authentication_failed"


class BillingError(AntigravityError):
    error_type = "billing_error"


class InvalidRequestError(AntigravityError):
    error_type = "invalid_request"


class ServerError(AntigravityError):
    error_type = "server_error"


class MaxOutputTokensError(AntigravityError):
    error_type = "max_output_tokens"


class UnknownAntigravityError(AntigravityError):
    error_type = "unknown"


_RESET_RE = re.compile(
    r"(?:reset|resets|retry(?:\s|-)?after)\D+(\d{1,2})\s*(am|pm)?(?:\s*\(([^)]+)\))?",
    re.IGNORECASE,
)

# High-precision CLI-failure signatures. agy's print mode exits 0 *even when it
# fails* (e.g. it prints an OAuth URL to stdout and returns 0 when unauthenticated
# — verified against agy 1.0.x), so the return code can't be trusted to flag
# failure. These phrases are specific enough to the CLI's own error output that
# they're safe to match regardless of return code, without misreading a normal
# model answer as an error. Checked before the broad, returncode-gated patterns.
_HARD_FAILURE_SIGNATURES: tuple[tuple[type[AntigravityError], tuple[str, ...]], ...] = (
    (
        AuthenticationError,
        ("authentication required", "not logged into antigravity",
         "please sign in", "please visit the url to log in",
         "authentication timed out", "authentication cancelled", "auth cancelled"),
    ),
)

# Text-pattern classifier for print mode (no structured errors to key off of).
# Order matters: the first matching family wins. Applied only when agy returns a
# nonzero exit code, since on a clean (returncode-0) answer these generic words
# could legitimately appear in the model's prose. RateLimit and the hard
# signatures above are checked unconditionally instead.
_ERROR_PATTERNS: tuple[tuple[type[AntigravityError], tuple[str, ...]], ...] = (
    (
        AuthenticationError,
        ("authentication required", "not logged into antigravity", "please sign in",
         "not logged in", "login", "unauthorized", "401", "permission denied", "403",
         "auth token", "token source"),
    ),
    (BillingError, ("billing", "payment", "402", "out of credit", "quota exceeded",
                    "insufficient", "upgrade your plan")),
    (
        InvalidRequestError,
        ("invalid request", "malformed", "bad request", "invalid model", "unknown model",
         "model not found", "unrecognized", "invalid argument", "400", "404",
         "not supported in the selected location"),
    ),
    (
        ServerError,
        ("500", "502", "503", "server error", "overloaded", "internal error",
         "service unavailable", "unavailable", "connection", "timeout", "timed out",
         "deadline exceeded"),
    ),
    (
        MaxOutputTokensError,
        ("max output", "maximum output", "output token limit", "context length",
         "context window", "too long", "truncated"),
    ),
)


def parse_rate_limit_reset(text: str) -> datetime | None:
    """Parse a human reset time such as ``resets 4pm (America/Los_Angeles)``."""
    match = _RESET_RE.search(text)
    if match is None:
        return None

    hour = int(match.group(1))
    ampm = (match.group(2) or "").lower()
    if ampm == "pm" and hour != 12:
        hour += 12
    elif ampm == "am" and hour == 12:
        hour = 0

    try:
        import zoneinfo

        tz = zoneinfo.ZoneInfo((match.group(3) or "UTC").strip())
    except Exception:
        tz = timezone.utc

    now = datetime.now(tz)
    reset = now.replace(hour=hour, minute=0, second=0, microsecond=0)
    if reset <= now:
        reset += timedelta(days=1)
    return reset.astimezone(timezone.utc)


def _truncate(text: str, limit: int = 2000) -> str:
    return text if len(text) <= limit else text[:limit] + "\n... [truncated]"


@dataclass
class AntigravityQueryResult(QueryResult):
    """:class:`QueryResult` specialised for the Antigravity CLI.
    """

    session_transcript: bytes | None = None
    session_transcript_path: str | None = None
    conversation_id: str | None = None
    usage: dict | None = None
    events: list = field(default_factory=list)
    # Text that may legitimately describe a CLI/provider failure: stderr, agy's
    # ``error`` field, and any non-JSON stdout. Deliberately EXCLUDES the model's
    # answer and the tool outputs in ``stream_result`` — a simulator log or an
    # explanation that mentions "429" or "rate limit" is not a rate limit.
    diagnostics: str = ""


class AntigravityLLM(LLMCallBase):
    """Wrap the Google Antigravity CLI (``agy --print``) as a Chia LLM backend."""

    # Honors --dangerously-skip-permissions; has no opencode-style permission block.
    supports_dangerously_skip_permissions = True

    def __init__(
        self,
        model: str | None = None,
        system_message: str = "",
        timeout_seconds: int = 600,
        retries: int = 3,
        logging_name: str = "antigravity",
        logging_level: int = logging.DEBUG,
        log_dir: str | None = None,
        logging_resource: str | None = None,
        agy_bin: str = "agy",
        work_dir: str | None = None,
        add_dirs: list[str] | None = None,
        gemini_dir: str | None = None,
        dangerously_skip_permissions: bool = True,
        sandbox: bool = False,
        extra_cli_args: list[str] | None = None,
        resume_session: bool = False,
        mcp_timeout_seconds: int | None = None,
        config=UNSET,
    ):
        super().__init__(system_message=system_message,
                         dangerously_skip_permissions=dangerously_skip_permissions,
                         config=config)
        self.logging_level = logging_level
        self.logging_name = logging_name
        self.retries = retries
        self.timeout_seconds = timeout_seconds
        self.model = model
        self.agy_bin = agy_bin
        self.work_dir = work_dir
        self.add_dirs = add_dirs or []
        # Where agy reads its config; MCP servers live in
        self._gemini_dir = gemini_dir
        # self.dangerously_skip_permissions is set by LLMCallBase.__init__.
        self.sandbox = sandbox
        self.extra_cli_args = extra_cli_args or []
        self.mcp_timeout_seconds = mcp_timeout_seconds
        self.logger = logging.getLogger(logging_name)
        self._call_counter = 0
        self._last_metadata: dict = {}
        self._log_prefix = None
        self._session_id: str | None = str(uuid4()) if resume_session else None
        self._conversation_id: str | None = None
        self._session_transcript: bytes | None = None
        self._session_transcript_path: str | None = None

        if self.model is None:
            self.logger.info(
                "AntigravityLLM model is unset; agy will use its configured default model."
            )
        self._logging_resource = logging_resource
        if log_dir is not None:
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            session_tag = f"_{self._session_id[:8]}" if self._session_id else ""
            self._log_prefix = os.path.join(log_dir, f"{logging_name}_{stamp}{session_tag}")

    @_session_tracked
    @ChiaFunction(resources={"antigravity_creds": 0.01})
    def prompt(
        self,
        user_message: str,
        tools: list[ChiaTool] | None = None,
    ) -> AntigravityQueryResult:
        """Send *user_message* to ``agy --print`` and return the response.

        Returns an :class:`AntigravityQueryResult` (``success=True`` on a clean
        run; ``success=False`` with ``returncode=-1`` when every retry failed).
        With ``resume_session=True`` the conversation transcript is restored
        onto this worker before the run and captured off it afterwards, and
        ``prompt.chia_remote`` returns an ``ObjectRefCallback`` that syncs both
        onto this instance on ``get()`` (see ``_sync_transcript``).
        """
        import time as _time

        from chia.trace.profiler import get_profiler

        profiler = get_profiler()
        for attempt in range(self.retries):
            try:
                tool_list = tools or []
                self._last_metadata = {}
                # Paste any carried conversation onto this machine so
                # --conversation finds it whichever worker ran the last call.
                self._restore_transcript()
                cli = self._run_antigravity(user_message, tool_list)
                self._call_counter += 1
                self._last_metadata.update({
                    "model": self.model or "antigravity-default",
                    "conversation_id": cli.conversation_id,
                    "usage": cli.usage,
                    "tools": [
                        {"name": t.name, "hostname": getattr(t, "hostname", None),
                         "port": getattr(t, "port", None), "node_id": getattr(t, "node_id", None)}
                        for t in tool_list
                    ],
                })
                if profiler.enabled:
                    profiler.add_info(self._last_metadata)
                self._classify_error(cli)
                self._capture_transcript(cli)
                cli.success = True
                return cli
            except (RateLimitError, AuthenticationError, BillingError, InvalidRequestError):
                raise
            except MaxOutputTokensError:
                if attempt == 0:
                    self.logger.warning("Max output tokens on attempt %d/%d, retrying once",
                                        attempt + 1, self.retries)
                    continue
                raise
            except ServerError:
                backoff = min(5 * 2 ** attempt, 60)
                self.logger.warning("Server error on attempt %d/%d, backing off %ds",
                                    attempt + 1, self.retries, backoff)
                _time.sleep(backoff)
            except (UnknownAntigravityError, subprocess.TimeoutExpired) as exc:
                self.logger.warning("Antigravity attempt %d/%d failed: %s",
                                    attempt + 1, self.retries, exc)
            except Exception as exc:
                self.logger.warning("Unexpected Antigravity error on attempt %d/%d: %s",
                                    attempt + 1, self.retries, exc)
        return AntigravityQueryResult(result="", returncode=-1, stderr="", stream_result="",
                                      success=False)

    def _sync_transcript(self, cli: AntigravityQueryResult) -> AntigravityQueryResult:
        """Copy a worker-captured conversation off *cli* onto this instance.

        Installed by ``_session_tracked`` as the ``get()`` callback when
        ``resume_session=True`` (the worker's mutations of its pickled ``self``
        are discarded, so the harvest must happen in the calling process).
        Advances the call counter and adopts the conversation id so the NEXT
        dispatch passes ``--conversation``; guarded so a transcript-less result
        (error path) doesn't clobber a prior capture. Pass-through: returns *cli*.
        """
        if self._session_id is not None:
            self._call_counter += 1
            if cli.conversation_id:
                self._conversation_id = cli.conversation_id
            if cli.session_transcript is not None:
                self._session_transcript = cli.session_transcript
                self._session_transcript_path = cli.session_transcript_path
        return cli

    # ------------------------------------------------------------------
    # Conversation persistence (resume_session=True)
    #
    # agy stores each conversation as SQLite (WAL mode) at
    # <gemini_dir>/antigravity-cli/conversations/<conversation_id>.db (+ -wal/-shm
    # side files while open). `--conversation <id>` resumes it from that file
    # alone — verified by copying a checkpointed .db into an empty conversations
    # dir. If the file is missing agy only WARNS and silently starts a NEW
    # conversation, so _run_antigravity checks the returned id against the one
    # requested.
    # ------------------------------------------------------------------

    @property
    def _conversations_dir(self) -> str:
        return os.path.join(self.gemini_dir, "antigravity-cli", "conversations")

    def _transcript_path(self) -> str | None:
        """Path of this conversation's .db on the current machine, or None."""
        if self._session_id is None or not self._conversation_id:
            return None
        return os.path.join(self._conversations_dir, f"{self._conversation_id}.db")

    def _restore_transcript(self) -> None:
        """Paste a carried conversation db onto this machine before a resume run.

        Overwrites any existing copy (and drops stale WAL/SHM side files) so the
        resumed conversation is exactly the one we hold. No-op unless
        ``resume_session=True`` and a transcript has been captured.
        """
        path = self._transcript_path()
        if path is None or self._session_transcript is None:
            return
        os.makedirs(os.path.dirname(path), exist_ok=True)
        for suffix in ("-wal", "-shm"):
            try:
                os.remove(path + suffix)
            except FileNotFoundError:
                pass
        with open(path, "wb") as fh:
            fh.write(self._session_transcript)
        self._session_transcript_path = path

    def _capture_transcript(self, cli: AntigravityQueryResult) -> None:
        """Checkpoint + read the conversation db after a run onto *cli* and self.

        agy leaves recent turns in the ``-wal`` side file; ``PRAGMA
        wal_checkpoint(TRUNCATE)`` folds them into the main ``.db`` so a single
        byte string carries the whole conversation. No-op unless
        ``resume_session=True`` and agy reported a conversation id whose db exists.
        """
        if self._session_id is None or not cli.conversation_id:
            return
        self._conversation_id = cli.conversation_id
        path = self._transcript_path()
        if not path or not os.path.exists(path):
            return
        try:
            con = sqlite3.connect(path)
            try:
                con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            finally:
                con.close()
        except sqlite3.Error as exc:  # carry whatever is in the main file
            self.logger.warning("conversation db checkpoint failed (%s); carrying as-is", exc)
        with open(path, "rb") as fh:
            data = fh.read()
        self._session_transcript = data
        self._session_transcript_path = path
        cli.session_transcript = data
        cli.session_transcript_path = path

    def _get_node_id(self) -> str:
        try:
            return ray.get_runtime_context().get_node_id()
        except Exception:
            return "unknown"

    def _format_prompt(self, user_message: str) -> str:
        if not self.system_message:
            return user_message
        return f"[System Instructions]\n{self.system_message}\n\n[User Request]\n{user_message}"

    @property
    def gemini_dir(self) -> str:
        return self._gemini_dir or os.path.join(os.path.expanduser("~"), ".gemini")

    @gemini_dir.setter
    def gemini_dir(self, value: str | None) -> None:
        self._gemini_dir = value

    # ------------------------------------------------------------------
    # Per-call HOME: agy reads its MCP servers only from the fixed path
    # ~/.gemini/config/mcp_config.json (no per-run override), and it blocks the
    # whole turn until every listed server connects — so a stale or foreign
    # entry from another run can hang a prompt (an unreachable address from a
    # different machine never fails fast). Sharing one file between concurrent
    # prompts with different tool sets on the same machine is equally unsafe.
    # We therefore run each agy with HOME pointed at a private temp dir whose
    # ~/.gemini/config holds exactly this call's tools, while
    # ~/.gemini/antigravity-cli is a symlink to the real one so the OAuth token,
    # settings (project/location) and the conversation store stay shared.
    # ------------------------------------------------------------------

    @property
    def _mcp_config_path(self) -> str:
        """Where the user's own agy MCP config lives. Never written by Chia."""
        return os.path.join(self.gemini_dir, "config", "mcp_config.json")

    def _mcp_servers(self, tools: list[ChiaTool]) -> dict:
        servers = {}
        for tool in tools:
            port = getattr(tool, "port", 8000)
            servers[tool.name] = {
                "serverUrl": f"http://{tool.hostname}:{port}/{tool.name}/mcp",
                "disabled": False,
            }
            if self.mcp_timeout_seconds is not None:
                servers[tool.name]["timeoutSeconds"] = self.mcp_timeout_seconds
        return {"mcpServers": servers}

    def _prepare_run_home(self, tools: list[ChiaTool]) -> str:
        """Build the private HOME for one agy run; returns its path (caller removes it).

        Layout::

            <tmp>/.gemini/antigravity-cli -> <gemini_dir>/antigravity-cli   (shared: token, settings, conversations)
            <tmp>/.gemini/config/config.json                                (copied from the user's, if any)
            <tmp>/.gemini/config/mcp_config.json                            (ONLY this call's tools)
            <tmp>/.cache -> <real home>/.cache                              (agy's playwright driver cache)
        """
        run_home = tempfile.mkdtemp(prefix="agy_home_")
        gem = os.path.join(run_home, ".gemini")
        os.makedirs(os.path.join(gem, "config"))

        real_cli = os.path.join(self.gemini_dir, "antigravity-cli")
        os.makedirs(real_cli, exist_ok=True)
        os.symlink(real_cli, os.path.join(gem, "antigravity-cli"))

        user_cfg = os.path.join(self.gemini_dir, "config", "config.json")
        if os.path.isfile(user_cfg):
            shutil.copy(user_cfg, os.path.join(gem, "config", "config.json"))
        migrated = os.path.join(self.gemini_dir, "config", ".migrated")
        if os.path.isfile(migrated):
            shutil.copy(migrated, os.path.join(gem, "config", ".migrated"))

        with open(os.path.join(gem, "config", "mcp_config.json"), "w") as f:
            json.dump(self._mcp_servers(tools), f, indent=2)

        real_cache = os.path.join(os.path.expanduser("~"), ".cache")
        try:
            os.makedirs(real_cache, exist_ok=True)
            os.symlink(real_cache, os.path.join(run_home, ".cache"))
        except OSError:
            pass  # agy will just use a per-run cache
        return run_home

    def _build_cmd(self, user_message: str) -> list[str]:
        cmd = [self.agy_bin]
        if self.dangerously_skip_permissions:
            cmd.append("--dangerously-skip-permissions")
        if self.sandbox:
            cmd.append("--sandbox")
        if self.model:
            cmd += ["--model", self.model]
        for directory in self.add_dirs:
            cmd += ["--add-dir", directory]
        # Bound agy's own print-mode wait just under our subprocess timeout so it
        # exits with a clean message rather than being killed (Go duration string).
        cmd += ["--print-timeout", f"{self.timeout_seconds}s"]
        # NDJSON events: full tool/usage trace + the conversation id we need to
        # resume (text mode gives only the final answer).
        cmd += ["--output-format", "stream-json"]
        if self._session_id is not None and self._conversation_id:
            cmd += ["--conversation", self._conversation_id]
        cmd += self.extra_cli_args
        # The prompt is the value of --print; pass it last.
        cmd += ["--print", self._format_prompt(user_message)]
        return cmd

    def _run_antigravity(
        self, user_message: str, tools: list[ChiaTool] | None = None
    ) -> AntigravityQueryResult:
        tools = tools or []
        requested = self._conversation_id if self._session_id is not None else None
        run_home = self._prepare_run_home(tools)
        try:
            env = os.environ.copy()
            env["HOME"] = run_home
            result = subprocess.run(
                self._build_cmd(user_message),
                capture_output=True,
                text=True,
                # Give agy's own --print-timeout a chance to fire first.
                timeout=self.timeout_seconds + 30,
                cwd=self.work_dir or None,
                env=env,
            )
        finally:
            shutil.rmtree(run_home, ignore_errors=True)
        events, unparsed = self._parse_events(result.stdout)
        final = next((e["result"] for e in reversed(events)
                      if e.get("event") == "result" and isinstance(e.get("result"), dict)), None)
        stderr = result.stderr
        returncode = result.returncode
        if final is not None:
            final_text = (final.get("response") or "").strip()
            conversation_id = final.get("conversation_id") or None
            usage = final.get("usage")
            if final.get("status", "SUCCESS") != "SUCCESS":
                # agy exits 0 even on failure; surface the error where
                # _classify_error looks and make the exit code say "failed".
                err = final.get("error") or f"agy result status {final.get('status')}"
                stderr = (stderr + "\n" if stderr.strip() else "") + err
                if returncode == 0:
                    returncode = 1
        else:
            # No result event (older agy / text output / crash): treat stdout as
            # the plain-text answer so nothing is lost.
            final_text = "\n".join(unparsed).strip() if unparsed else result.stdout.strip()
            conversation_id, usage = None, None
        # Only genuine diagnostics feed the error classifier (see the field doc).
        diagnostics = "\n".join(p for p in (stderr, "\n".join(unparsed)) if p and p.strip())
        if requested and conversation_id and conversation_id != requested:
            # The carried conversation wasn't found on this machine (agy warns
            # and starts afresh). Keep going on the new id rather than fail the
            # turn, but say so loudly: the model has lost its memory.
            self.logger.warning(
                "agy did not resume conversation %s (got %s); continuing on the new one",
                requested, conversation_id,
            )
        stream = self._build_stream(user_message, final_text, stderr, tools, events, unparsed)
        if self._log_prefix is not None:
            self._write_log(user_message, stream)
        if returncode != 0:
            self.logger.warning("agy exited %d: %s", returncode, stderr[:500])
        return AntigravityQueryResult(
            result=final_text, returncode=returncode, stderr=stderr, stream_result=stream,
            conversation_id=conversation_id, usage=usage, events=events,
            diagnostics=diagnostics,
        )

    @staticmethod
    def _parse_events(stdout: str) -> tuple[list[dict], list[str]]:
        """Split agy's stream-json stdout into (parsed events, non-JSON lines)."""
        events: list[dict] = []
        unparsed: list[str] = []
        for line in stdout.splitlines():
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                unparsed.append(line)
                continue
            if isinstance(obj, dict):
                events.append(obj)
            else:
                unparsed.append(line)
        return events, unparsed

    def _build_stream(
        self,
        user_message: str,
        final_text: str,
        stderr: str,
        tools: list[ChiaTool],
        events: list[dict] | None = None,
        unparsed: list[str] | None = None,
    ) -> str:
        """Render the run as a readable transcript (Claude-style ``[Section]`` blocks).

        With stream-json events: one block per step — ``[Tool Call: name]`` with
        its arguments when a tool step opens, ``[Tool Result]`` when it closes,
        ``[Response]`` for each agent text step (deltas joined) with its token
        usage, plus ``[Init]`` / ``[Result]`` bookends. Without events (plain
        text stdout) it falls back to the final text only.
        """
        prompt = user_message[:500] + ("..." if len(user_message) > 500 else "")
        parts = [f"[User Message]\n{prompt}\n\n"]
        if tools:
            names = ", ".join(t.name for t in tools)
            parts.append(f"[Tools Offered]\n{names}\n\n")
        events = events or []
        text_buf: dict[int, list[str]] = {}
        for ev in events:
            kind = ev.get("event")
            if kind == "init":
                init = ev.get("init") or {}
                parts.append(f"[Init]\nmodel={init.get('model')} conversation={ev.get('conversation_id')} "
                             f"cwd={init.get('cwd')}\n\n")
            elif kind == "step_update":
                st = ev.get("step_update") or {}
                idx, stype, state = st.get("step_index"), st.get("step_type"), st.get("state")
                if stype == "agent_response":
                    if st.get("text_delta"):
                        text_buf.setdefault(idx, []).append(st["text_delta"])
                    if state == "DONE":
                        text = "".join(text_buf.pop(idx, [])).strip()
                        if text:
                            parts.append(f"[Response]\n{_truncate(text)}\n\n")
                        if st.get("usage"):
                            parts.append(f"[Usage]\n{json.dumps(st['usage'])}\n\n")
                elif stype == "tool":
                    info = st.get("tool_info") or {}
                    name = st.get("tool_name") or info.get("name")
                    if state == "ACTIVE" or (state == "DONE" and "output" not in info):
                        parts.append(f"[Tool Call: {name}]\nArgs: "
                                     f"{json.dumps(info.get('parameters', {}))}\n\n")
                    if state == "DONE" and "output" in info:
                        parts.append(f"[Tool Result]\n{_truncate(str(info.get('output')))}\n\n")
                elif stype not in ("user_input", None):
                    parts.append(f"[Step: {stype}] state={state}\n\n")
            elif kind == "result":
                res = ev.get("result") or {}
                parts.append(f"[Result]\nstatus={res.get('status')} conversation={res.get('conversation_id')} "
                             f"turns={res.get('num_turns')} duration={res.get('duration_seconds')}s "
                             f"usage={json.dumps(res.get('usage'))}\n")
                if res.get("error"):
                    parts.append(f"error: {_truncate(str(res['error']))}\n")
                parts.append("\n")
        if not events and final_text:
            parts.append(f"[Response]\n{_truncate(final_text)}\n\n")
        if unparsed:
            parts.append(f"[Unparsed stdout]\n{_truncate(chr(10).join(unparsed))}\n\n")
        if stderr.strip():
            parts.append(f"[stderr]\n{_truncate(stderr.strip())}\n\n")
        return "".join(parts)

    def _write_log(self, user_message: str, stream: str) -> None:
        with io.StringIO() as f:
            f.write("=" * 80 + "\n")
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                    f"Prompt #{self._call_counter} (antigravity)\n")
            f.write("=" * 80 + "\n\n")
            f.write(stream)
            if stream and not stream.endswith("\n"):
                f.write("\n")
            f.write("-" * 80 + "\n\n")
            chia_log(f.getvalue(), f"{self._log_prefix}.log", self._logging_resource)

    def _classify_error(self, cli: QueryResult) -> None:
        """Raise a typed error if *cli* describes a failed run.

        Classifies ONLY diagnostic text: for an :class:`AntigravityQueryResult`
        that is ``diagnostics`` (stderr, agy's ``error``, non-JSON stdout). The
        model's answer and the tool outputs rendered into ``stream_result`` are
        never scanned — with stream-json they routinely contain words like
        "429", "timeout" or "rate limit" from the task itself. A plain
        :class:`QueryResult` (text-mode / legacy callers) falls back to scanning
        everything, since there the final text IS the only place a printed
        OAuth prompt or error could appear.
        """
        diagnostics = getattr(cli, "diagnostics", None)
        if diagnostics is None:
            combined = "\n".join(part for part in (cli.stderr, cli.result, cli.stream_result) if part)
        else:
            combined = diagnostics
        lower = combined.lower()
        node_id = self._get_node_id()

        # Rate limits and the high-precision CLI-failure signatures are checked
        # unconditionally: agy can report both with a returncode of 0.
        if any(k in lower for k in ("rate limit", "usage limit", "too many requests", "429",
                                    "resource_exhausted")):
            raise RateLimitError(
                node_id=node_id,
                reset_time=parse_rate_limit_reset(combined),
                raw_message=combined[:300],
                exit_code=cli.returncode,
            )
        for error_cls, signatures in _HARD_FAILURE_SIGNATURES:
            if any(sig in lower for sig in signatures):
                raise error_cls(node_id=node_id, exit_code=cli.returncode, raw_message=combined[:300])

        # A clean exit with no failure signature is a success. The broad keyword
        # patterns only run on a nonzero exit, where misclassifying prose is moot.
        if cli.returncode == 0:
            return
        for error_cls, patterns in _ERROR_PATTERNS:
            if any(pattern in lower for pattern in patterns):
                raise error_cls(node_id=node_id, exit_code=cli.returncode, raw_message=combined[:300])
        raise UnknownAntigravityError(node_id=node_id, exit_code=cli.returncode, raw_message=combined[:300])
