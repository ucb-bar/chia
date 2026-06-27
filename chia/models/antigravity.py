"""Google Antigravity CLI LLM backend.

``AntigravityLLM`` wraps Google's Antigravity CLI (the ``agy`` binary, installed
via ``curl -fsSL https://antigravity.google/cli/install.sh | bash``) behind the
same synchronous ``prompt`` shape as the other Chia LLM backends. It runs
``agy --print`` (non-interactive "print mode"), which emits the model's final
answer as **plain text** on stdout.

**Auth is OAuth-only.** ``agy`` signs in with a Google account ("Antigravity"
  / Gemini Code Assist) and stores a refresh token on disk; in a container it
  uses file-based token storage under the Gemini config dir. There is no
  API-key path.

* **Output is plain text, not JSON.** Print mode has no structured/streaming
  output format, so there is no per-turn tool/usage trace to parse

The system prompt is folded into the user message (print mode has no
``--system-prompt`` flag), mirroring :class:`~chia.models.codex.CodexLLM`.
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import ray

from chia.base.ChiaFunction import ChiaFunction
from chia.base.llm_call import QueryResult, LLMCallBase

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
         "model not found", "unrecognized", "invalid argument", "400", "404"),
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


class AntigravityLLM(LLMCallBase):
    """Wrap the Google Antigravity CLI (``agy --print``) as a Chia LLM backend."""

    def __init__(
        self,
        model: str | None = None,
        system_message: str = "",
        timeout_seconds: int = 600,
        retries: int = 3,
        logging_name: str = "antigravity",
        logging_level: int = logging.DEBUG,
        log_dir: str | None = None,
        agy_bin: str = "agy",
        work_dir: str | None = None,
        add_dirs: list[str] | None = None,
        gemini_dir: str | None = None,
        dangerously_skip_permissions: bool = True,
        sandbox: bool = False,
        extra_cli_args: list[str] | None = None,
    ):
        super().__init__(system_message=system_message)
        self.logging_level = logging_level
        self.logging_name = logging_name
        self.retries = retries
        self.timeout_seconds = timeout_seconds
        self.model = model
        self.agy_bin = agy_bin
        self.work_dir = work_dir
        self.add_dirs = add_dirs or []
        # Where agy reads its config; MCP servers live in
        # <gemini_dir>/config/mcp_config.json. Defaults to the standard location.
        self.gemini_dir = gemini_dir or os.path.join(os.path.expanduser("~"), ".gemini")
        self.dangerously_skip_permissions = dangerously_skip_permissions
        self.sandbox = sandbox
        self.extra_cli_args = extra_cli_args or []
        self.logger = logging.getLogger(logging_name)
        self._call_counter = 0
        self._last_metadata: dict = {}
        self._log_prefix = None

        self.logger.warning(
            "AntigravityLLM is experimental and has not been production-validated."
        )
        if self.model is None:
            self.logger.info(
                "AntigravityLLM model is unset; agy will use its configured default model."
            )
        if log_dir is not None:
            os.makedirs(log_dir, exist_ok=True)
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self._log_prefix = os.path.join(log_dir, f"{logging_name}_{stamp}")

    @ChiaFunction(resources={"antigravity_creds": 0.01})
    def prompt(
        self,
        user_message: str,
        tools: list[ChiaTool] | None = None,
    ) -> QueryResult:
        """Send *user_message* to ``agy --print`` and return the response."""
        import time as _time

        from chia.trace.profiler import get_profiler

        profiler = get_profiler()
        for attempt in range(self.retries):
            try:
                tool_list = tools or []
                self._last_metadata = {}
                cli = self._run_antigravity(user_message, tool_list)
                self._call_counter += 1
                self._last_metadata.update({
                    "model": self.model or "antigravity-default",
                    "tools": [
                        {"name": t.name, "hostname": getattr(t, "hostname", None),
                         "port": getattr(t, "port", None), "node_id": getattr(t, "node_id", None)}
                        for t in tool_list
                    ],
                })
                if profiler.enabled:
                    profiler.add_info(self._last_metadata)
                self._classify_error(cli)
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
        return QueryResult(result="", returncode=-1, stderr="", stream_result="", success=False)

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
    def _mcp_config_path(self) -> str:
        return os.path.join(self.gemini_dir, "config", "mcp_config.json")

    def _write_mcp_config(self, tools: list[ChiaTool]) -> None:
        """Write the Chia tools into ``<gemini_dir>/config/mcp_config.json``.

        agy reads MCP servers only from this fixed path (no per-run override), so
        we merge into whatever is there: every Chia tool is registered under its
        own name with a streamable-HTTP ``httpUrl``, while server keys we don't
        manage are preserved. With no tools, leave the file untouched.
        """
        if not tools:
            return
        path = self._mcp_config_path
        os.makedirs(os.path.dirname(path), exist_ok=True)

        config: dict = {}
        try:
            with open(path) as f:
                text = f.read().strip()
            if text:
                loaded = json.loads(text)
                if isinstance(loaded, dict):
                    config = loaded
        except (FileNotFoundError, json.JSONDecodeError):
            config = {}

        servers = config.get("mcpServers")
        if not isinstance(servers, dict):
            servers = {}
        for tool in tools:
            port = getattr(tool, "port", 8000)
            servers[tool.name] = {
                "httpUrl": f"http://{tool.hostname}:{port}/{tool.name}/mcp",
            }
        config["mcpServers"] = servers

        with open(path, "w") as f:
            json.dump(config, f, indent=2)

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
        cmd += self.extra_cli_args
        # The prompt is the value of --print; pass it last.
        cmd += ["--print", self._format_prompt(user_message)]
        return cmd

    def _run_antigravity(
        self, user_message: str, tools: list[ChiaTool] | None = None
    ) -> QueryResult:
        tools = tools or []
        self._write_mcp_config(tools)
        result = subprocess.run(
            self._build_cmd(user_message),
            capture_output=True,
            text=True,
            # Give agy's own --print-timeout a chance to fire first.
            timeout=self.timeout_seconds + 30,
            cwd=self.work_dir or None,
            env=os.environ.copy(),
        )
        final_text = result.stdout.strip()
        stream = self._build_stream(user_message, final_text, result.stderr, tools)
        if self._log_prefix is not None:
            self._write_log(user_message, stream)
        if result.returncode != 0:
            self.logger.warning("agy exited %d: %s", result.returncode, result.stderr[:500])
        return QueryResult(final_text, result.returncode, result.stderr, stream)

    def _build_stream(
        self, user_message: str, final_text: str, stderr: str, tools: list[ChiaTool]
    ) -> str:
        prompt = user_message[:500] + ("..." if len(user_message) > 500 else "")
        parts = [f"[User Message]\n{prompt}\n\n"]
        if tools:
            names = ", ".join(t.name for t in tools)
            parts.append(f"[Tools Offered]\n{names}\n\n")
        if final_text:
            parts.append(f"[Response]\n{_truncate(final_text)}\n\n")
        if stderr.strip():
            parts.append(f"[stderr]\n{_truncate(stderr.strip())}\n\n")
        return "".join(parts)

    def _write_log(self, user_message: str, stream: str) -> None:
        with open(f"{self._log_prefix}.log", "a") as f:
            f.write("=" * 80 + "\n")
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                    f"Prompt #{self._call_counter} (antigravity)\n")
            f.write("=" * 80 + "\n\n")
            f.write(stream)
            if stream and not stream.endswith("\n"):
                f.write("\n")
            f.write("-" * 80 + "\n\n")

    def _classify_error(self, cli: QueryResult) -> None:
        combined = "\n".join(part for part in (cli.stderr, cli.result, cli.stream_result) if part)
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
