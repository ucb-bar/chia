"""End-to-end test for chia.models.ollama.OllamaLLM on a local Ollama cluster.

Exercises the Ollama **OpenAI-compatible** Chat Completions interface against a
REAL Ollama server inside a ``chia-ollama`` worker container, dispatched through
a full Ray cluster. The worker advertises the ``ollama_creds`` resource that
``OllamaLLM.prompt`` is gated on, so the prompt task lands in the container,
where the OllamaLLM default ``base_url`` (``http://localhost:11434/v1``) reaches
the local server (chia runs containers ``--net=host``).

Three stages, mirroring the chipyard macrocompiler e2e:

  STAGE 1 — one task submitted to the ``ollama_creds`` resource (so it lands in
  the container). It ensures the model is present (idempotent ``ollama pull``),
  builds an ``OllamaLLM`` and runs a simple prompt against the OpenAI-compat
  endpoint *in-process on the worker*, and returns a JSON-able report (worker
  node id, the answer, token usage). Proves the endpoint is reachable and a real
  completion came back, from inside the cluster.

  STAGE 2 — dispatches ``OllamaLLM.prompt`` itself via ``.chia_remote`` (passing
  the instance as ``self``), so the real production path is exercised directly:
  the ``ollama_creds`` resource requirement, the trampoline, instance + QueryResult
  serialization across the worker boundary — not just the in-process call stage 1
  makes.

  STAGE 3 — tool calling. Stands up a real ``BashTool`` MCP server, then
  dispatches ``OllamaLLM.prompt`` via ``.chia_remote`` with that tool so the
  worker's OpenAI-compat tool loop must (a) connect to the MCP server, (b) get
  the model to emit a structured ``tool_call``, (c) execute it over MCP, and
  (d) feed the result back. We pass a picklable ``_ToolRef`` stand-in (not the
  live ``BashTool`` — its FastMCP instance doesn't cloudpickle across the worker
  boundary) carrying just the name/host/port the loop reads. The check asserts a
  sentinel (``CHIA_TOOL_OK``) round-tripped through the tool, and the full
  turn-by-turn conversation (``cli.stream_result``) is printed so a flaky run is
  debuggable. NOTE: a *malformed* tool call here — the model emitting the call as
  raw text instead of a structured ``tool_call`` — is a known Ollama serving
  flake that shows up after sustained use and clears on a server restart, not a
  bug in the loop; the printed conversation makes that case obvious.

Usage (host, chia env active, cluster up via ollama_local.yaml):
    cd <repo root>
    python chia/models/tests/cluster/ollama_e2e_test.py
"""

import os
import sys

# Allow `python .../ollama_e2e_test.py` from any cwd: put the repo root on
# sys.path so the `chia` namespace package (resolved via path, not installed)
# imports. cluster -> tests -> models -> chia -> <repo root> == 4 levels up.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), *([os.pardir] * 4)))
sys.path.insert(0, _REPO_ROOT)

import ray

from chia.base.ChiaFunction import ChiaFunction, get

# Model to serve/query. Must match what the cluster pulls (OLLAMA_PULL in
# ollama_local.yaml). qwen2.5:3b is small + a reliable tool-caller -> fast on CPU.
E2E_MODEL = os.environ.get("OLLAMA_E2E_MODEL", "qwen2.5:3b")


class _ToolRef:
    """Picklable stand-in for a ChiaTool when dispatched as a chia_remote arg.

    A live ``BashTool`` carries a ``FastMCP`` instance (``self.mcp``) that does
    not cloudpickle cleanly across the worker boundary, but the OpenAICompatLLM
    tool loop only reads ``.name`` / ``.hostname`` / ``.port`` to reach the
    already-running MCP server (``http://{hostname}:{port}/{name}/mcp``). So we
    ship just those four scalars; the server itself keeps running on whichever
    node ``BashTool.__post_init__`` started it on.
    """

    def __init__(self, name: str, hostname: str, port: int, node_id=None):
        self.name = name
        self.hostname = hostname
        self.port = port
        self.node_id = node_id


@ChiaFunction(resources={"ollama_creds": 0.01})
def ollama_query_e2e(model: str, user_message: str, system_message: str) -> dict:
    """Runs on an ollama worker: ensure the model, then query via OllamaLLM.

    Waits for the local Ollama server, ensures *model* is pulled (idempotent),
    builds an OllamaLLM and runs a single prompt against the OpenAI-compatible
    endpoint. Returns a JSON-able report (no non-serializable objects).
    """
    import subprocess
    import time
    import urllib.request

    report: dict = {
        "model": model,
        "node_id": None,
        "server_up": False,
        "model_ready": False,
        "success": None,
        "result": None,
        "output_tokens": 0,
        "num_turns": 0,
        "base_url": None,
        "error": None,
    }

    try:
        report["node_id"] = ray.get_runtime_context().get_node_id()

        # 1) Wait (up to ~60s) for the local Ollama server to answer.
        for _ in range(60):
            try:
                with urllib.request.urlopen("http://127.0.0.1:11434/api/version", timeout=2) as r:
                    if r.status == 200:
                        report["server_up"] = True
                        break
            except Exception:
                time.sleep(1)
        if not report["server_up"]:
            report["error"] = "Ollama server never came up at 127.0.0.1:11434"
            return report

        # 2) Ensure the model is present. `ollama pull` is idempotent and dedups
        # with any pull the container entrypoint started, so this just blocks
        # until the model is available (a no-op if already cached).
        pull = subprocess.run(["ollama", "pull", model], capture_output=True, text=True, timeout=1200)
        report["model_ready"] = pull.returncode == 0
        if pull.returncode != 0:
            report["error"] = f"ollama pull {model} failed: {(pull.stdout + pull.stderr)[-500:]}"
            return report

        # 3) Query the OpenAI-compatible endpoint via OllamaLLM (in-process here).
        from chia.models.ollama import OllamaLLM

        llm = OllamaLLM(model=model, system_message=system_message, max_tokens=512)
        report["base_url"] = llm.base_url
        cli = llm.prompt(user_message, tools=[])
        report["success"] = cli.success
        report["result"] = cli.result
        report["output_tokens"] = llm._last_metadata.get("output_tokens", 0)
        report["num_turns"] = llm._last_metadata.get("num_turns", 0)
    except Exception as e:  # noqa: BLE001 - report any failure back to the host
        report["error"] = repr(e)

    return report


def _check_stage1(report: dict) -> bool:
    print(f"worker node id : {report['node_id']}")
    print(f"server_up      : {report['server_up']}")
    print(f"model_ready    : {report['model_ready']} ({report['model']})")
    print(f"base_url       : {report['base_url']}")
    print(f"success        : {report['success']}")
    print(f"result         : {report['result']!r}")
    print(f"output_tokens  : {report['output_tokens']}   num_turns: {report['num_turns']}")

    if report["error"]:
        print(f"  ! stage-1 task error: {report['error']}")
        return False

    ok = True
    if report["base_url"] != "http://localhost:11434/v1":
        ok = False
        print("  ! unexpected base_url (OllamaLLM default should be localhost:11434/v1)")
    if report["success"] is not True:
        ok = False
        print("  ! prompt did not succeed")
    if "PONG" not in (report["result"] or "").upper():
        ok = False
        print("  ! model did not return the expected token (PONG)")
    if report["output_tokens"] <= 0:
        ok = False
        print("  ! no output tokens reported — was a real completion returned?")
    return ok


def _check_stage2(model: str) -> bool:
    """Dispatch OllamaLLM.prompt via .chia_remote and validate the QueryResult."""
    from chia.models.ollama import OllamaLLM

    print(f"\n=== remote dispatch (OllamaLLM.prompt.chia_remote on 'ollama_creds') ===")
    llm = OllamaLLM(
        model=model,
        system_message="You answer with a single word and nothing else.",
        max_tokens=512,
    )
    # Pass the instance explicitly as `self` (ChiaFunction method dispatch). The
    # task is gated on ollama_creds, so it runs in the container against the
    # local server; the QueryResult round-trips back across the worker boundary.
    cli = get(OllamaLLM.prompt.chia_remote(llm, "Reply with exactly the word: PONG", []))
    ok = cli.success is True and "PONG" in (cli.result or "").upper()
    print(f"success={cli.success} result={cli.result!r} -> {'OK' if ok else 'FAIL'}")
    if "Response" not in (cli.stream_result or ""):
        print("  ! stream_result missing the [Response] section")
        ok = False
    return ok


def _check_stage3_tools(model: str) -> bool:
    """Tool calling: stand up a BashTool MCP server, then dispatch
    OllamaLLM.prompt with that tool via .chia_remote and verify the model
    actually invoked it (a sentinel echoed through bash round-trips back).
    """
    from chia.base.tools.BashTool import BashTool
    from chia.models.ollama import OllamaLLM

    print("\n=== tool calling (OllamaLLM.prompt.chia_remote with a BashTool) ===")

    # __post_init__ starts the MCP server on a cluster node and sets
    # hostname/port/node_id. Single-machine + --net=host means the worker can
    # reach it. Always stop() it (finally) so the uvicorn actor is cleaned up.
    tool = BashTool("bash")
    try:
        print(f"BashTool MCP server: {tool.hostname}:{tool.port} (node {tool.node_id})")
        ref = _ToolRef(tool.name, tool.hostname, tool.port, tool.node_id)

        llm = OllamaLLM(
            model=model,
            system_message=(
                "You are a helpful assistant with access to a bash tool. When "
                "asked to run a command you MUST call the bash tool to run it, "
                "then report its output."
            ),
            # Generous headroom for the tool-call JSON + the follow-up answer so
            # a `length` truncation can never be mistaken for a tool-call flake
            # (stages 1/2 use 512 for a one-word reply; tool calling gets more).
            max_tokens=2048,
        )
        prompt = (
            "Use the bash tool to run exactly this command: echo CHIA_TOOL_OK\n"
            "Then tell me what it printed."
        )
        # Gated on ollama_creds -> runs in the container; the tool loop connects
        # back to the BashTool MCP server over the host network.
        cli = get(OllamaLLM.prompt.chia_remote(llm, prompt, [ref]))

        # The tool loop records the whole exchange in stream_result regardless of
        # any logging flag, so print it — a flaky/malformed run is then visible.
        print("\n--- full conversation (cli.stream_result) ---")
        print(cli.stream_result or "(empty)")
        print("--- end conversation ---\n")

        stream = cli.stream_result or ""
        tool_called = "[Tool Call:" in stream
        tool_result = "[Tool Result" in stream
        sentinel = "CHIA_TOOL_OK" in (cli.result or "") or "CHIA_TOOL_OK" in stream

        ok = bool(cli.success) and tool_called and tool_result and sentinel
        print(f"success={cli.success} tool_called={tool_called} "
              f"tool_result={tool_result} sentinel={sentinel} -> {'OK' if ok else 'FAIL'}")
        if not ok and cli.success:
            print("  ! NOTE: if the model's text in [Response] above looks like a "
                  "raw/garbled tool call (e.g. stray tokens, a bare JSON blob or a "
                  "</tool_call> tag) with no [Tool Call:] line, this is the known "
                  "Ollama serving flake (it stops emitting structured tool_calls "
                  "after sustained use). A server restart restores it; the loop "
                  "itself is correct.")
        return ok
    finally:
        tool.stop()


def main() -> int:
    print(f"[driver] connecting to ray cluster (working_dir={_REPO_ROOT})")
    # Ship the live repo as the worker working_dir (like the macrocompiler e2e),
    # so workers import chia.models.ollama from here even if the image's installed
    # chia predates it.
    ray.init(
        address="auto",
        runtime_env={
            "working_dir": _REPO_ROOT,
            "excludes": [".venv/**", ".git/**", "**/__pycache__/**",
                         "**/*.pyc", "**/.pytest_cache/**",
                         "**/HELLOLOG/**", "**/tags"],
        },
    )

    print(f"Submitting ollama_query_e2e to the 'ollama_creds' resource (model={E2E_MODEL})...\n")
    report = get(ollama_query_e2e.chia_remote(
        E2E_MODEL,
        "Reply with exactly the word: PONG",
        "You answer with a single word and nothing else.",
    ))

    ok = _check_stage1(report)
    ok = _check_stage2(E2E_MODEL) and ok
    ok = _check_stage3_tools(E2E_MODEL) and ok

    print("\n" + ("PASS: queried the Ollama OpenAI-compatible endpoint through the "
                  "Ray cluster, got a real completion, and round-tripped a tool call."
                  if ok else "FAIL: see the markers above."))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
