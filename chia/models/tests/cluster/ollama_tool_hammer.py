"""Hammer the OllamaLLM tool-calling path against a SINGLE running server.

Diagnostic companion to ollama_e2e_test.py stage 3. The e2e proves tool calling
works *once*; this loops it N times against the same already-running Ollama
server (no restart between iterations) to answer: does tool calling stay clean
under sustained use, or does it degrade (the suspected Ollama serving flake where
the model stops emitting structured tool_calls and instead returns the call as
raw text)?

Run against an already-up cluster (ollama_cluster.sh up), chia env active:
    python chia/models/tests/cluster/ollama_tool_hammer.py
Env knobs:
    OLLAMA_E2E_MODEL   model to use (default qwen2.5:3b; match OLLAMA_PULL)
    HAMMER_ITERS       number of iterations (default 25)
    HAMMER_MAX_TOKENS  max_tokens per prompt (default 2048)
"""

import os
import sys
import time

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), *([os.pardir] * 4)))
sys.path.insert(0, _REPO_ROOT)

import ray

from chia.base.ChiaFunction import get

MODEL = os.environ.get("OLLAMA_E2E_MODEL", "qwen2.5:3b")
ITERS = int(os.environ.get("HAMMER_ITERS", "25"))
MAX_TOKENS = int(os.environ.get("HAMMER_MAX_TOKENS", "2048"))


class _ToolRef:
    """Picklable stand-in for a ChiaTool (see ollama_e2e_test.py _ToolRef)."""

    def __init__(self, name, hostname, port, node_id=None):
        self.name = name
        self.hostname = hostname
        self.port = port
        self.node_id = node_id


def main() -> int:
    print(f"[hammer] model={MODEL} iters={ITERS} max_tokens={MAX_TOKENS}")
    ray.init(
        address="auto",
        runtime_env={
            "working_dir": _REPO_ROOT,
            "excludes": [".venv/**", ".git/**", "**/__pycache__/**",
                         "**/*.pyc", "**/.pytest_cache/**",
                         "**/HELLOLOG/**", "**/tags"],
        },
    )

    from chia.base.tools.BashTool import BashTool
    from chia.models.ollama import OllamaLLM

    tool = BashTool("bash")
    try:
        print(f"[hammer] BashTool MCP server: {tool.hostname}:{tool.port} "
              f"(node {tool.node_id})")
        ref = _ToolRef(tool.name, tool.hostname, tool.port, tool.node_id)

        llm = OllamaLLM(
            model=MODEL,
            system_message=(
                "You are a helpful assistant with access to a bash tool. When "
                "asked to run a command you MUST call the bash tool to run it, "
                "then report its output."
            ),
            max_tokens=MAX_TOKENS,
        )

        passes = 0
        first_fail = None
        first_fail_stream = None
        results = []  # (i, ok, dt, tool_called, tool_result, sentinel_ok)

        for i in range(1, ITERS + 1):
            sentinel = f"CHIA_TOOL_OK_{i}"
            prompt = (
                f"Use the bash tool to run exactly this command: echo {sentinel}\n"
                "Then tell me what it printed."
            )
            t0 = time.time()
            try:
                cli = get(OllamaLLM.prompt.chia_remote(llm, prompt, [ref]))
                dt = time.time() - t0
                stream = cli.stream_result or ""
                tool_called = "[Tool Call:" in stream
                tool_result = "[Tool Result" in stream
                sentinel_ok = sentinel in (cli.result or "") or sentinel in stream
                ok = bool(cli.success) and tool_called and tool_result and sentinel_ok
            except Exception as e:  # noqa: BLE001
                dt = time.time() - t0
                stream = f"(exception) {e!r}"
                tool_called = tool_result = sentinel_ok = False
                ok = False

            results.append((i, ok, dt, tool_called, tool_result, sentinel_ok))
            passes += int(ok)
            flag = "OK " if ok else "FAIL"
            print(f"  [{i:>3}/{ITERS}] {flag}  {dt:5.1f}s  "
                  f"tool_called={tool_called} tool_result={tool_result} "
                  f"sentinel={sentinel_ok}")
            if not ok and first_fail is None:
                first_fail = i
                first_fail_stream = stream

        print("\n" + "=" * 70)
        print(f"[hammer] {passes}/{ITERS} clean tool calls against ONE server")
        if first_fail is None:
            print("[hammer] NO degradation observed -> the context bump appears to "
                  "have fixed it (or this server simply held up).")
        else:
            print(f"[hammer] FIRST failure at iteration {first_fail} -> degradation "
                  "still happens under sustained use (the Ollama serving flake; the "
                  "context bump only removed a confound).")
            print("\n--- conversation at first failure (iteration "
                  f"{first_fail}) ---")
            print(first_fail_stream or "(empty)")
            print("--- end ---")
        print("=" * 70)
        return 0 if first_fail is None else 1
    finally:
        tool.stop()
        ray.shutdown()


if __name__ == "__main__":
    sys.exit(main())
