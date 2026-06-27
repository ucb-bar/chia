"""End-to-end test for chia.models.vllm.VLLMLLM on a local vLLM cluster.

Exercises the vLLM **OpenAI-compatible** Chat Completions interface against a
REAL vLLM server inside a ``chia-vllm`` worker container, dispatched through a
full Ray cluster. The worker advertises the ``vllm_creds`` resource that
``VLLMLLM.prompt`` is gated on, so the prompt task lands in the container, where
the VLLMLLM default ``base_url`` (``http://localhost:8000/v1``) reaches the local
server (chia runs containers ``--net=host``).

Two stages, mirroring the Ollama e2e:

  STAGE 1 — one task submitted to the ``vllm_creds`` resource (so it lands in the
  container). It waits for the vLLM server's /health (weight download + load can
  take a while on first run), builds a ``VLLMLLM`` and runs a simple prompt
  against the OpenAI-compat endpoint *in-process on the worker*, returning a
  JSON-able report (worker node id, the answer, token usage).

  STAGE 2 — dispatches ``VLLMLLM.prompt`` itself via ``.chia_remote`` (passing the
  instance as ``self``), exercising the real resource-gated production path and
  the instance/``QueryResult`` round-trip across the worker boundary.

The model queried must match the one the server serves (VLLM_MODEL in
vllm_local.yaml). Override both via VLLM_E2E_MODEL.

Usage (host, chia env active, GPU cluster up via vllm_local.yaml):
    cd <repo root>
    python chia/models/tests/cluster/vllm_e2e_test.py
"""

import os
import sys

# Allow `python .../vllm_e2e_test.py` from any cwd: put the repo root on
# sys.path so the `chia` namespace package (resolved via path, not installed)
# imports. cluster -> tests -> models -> chia -> <repo root> == 4 levels up.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), *([os.pardir] * 4)))
sys.path.insert(0, _REPO_ROOT)

import ray

from chia.base.ChiaFunction import ChiaFunction, get

# Model to query. MUST match what the server serves (VLLM_MODEL in
# vllm_local.yaml). Qwen2.5-3B is ungated + a reliable tool-caller.
E2E_MODEL = os.environ.get("VLLM_E2E_MODEL", "Qwen/Qwen2.5-3B-Instruct")

# vLLM first-run startup (download + load weights into VRAM) can be slow.
HEALTH_TIMEOUT_S = int(os.environ.get("VLLM_E2E_HEALTH_TIMEOUT_S", "900"))


@ChiaFunction(resources={"vllm_creds": 0.01})
def vllm_query_e2e(model: str, user_message: str, system_message: str,
                   health_timeout_s: int) -> dict:
    """Runs on a vLLM worker: wait for /health, then query via VLLMLLM.

    Waits for the local vLLM server to report healthy (it serves exactly the
    model it was launched with), builds a VLLMLLM and runs a single prompt
    against the OpenAI-compatible endpoint. Returns a JSON-able report.
    """
    import time
    import urllib.request

    report: dict = {
        "model": model,
        "node_id": None,
        "server_healthy": False,
        "success": None,
        "result": None,
        "output_tokens": 0,
        "num_turns": 0,
        "base_url": None,
        "error": None,
    }

    try:
        report["node_id"] = ray.get_runtime_context().get_node_id()

        # Wait for the vLLM server's /health (200 once the model is loaded).
        # Port 8200 (not 8000): chia reserves 8000-8010 on tunneled workers.
        deadline = time.time() + health_timeout_s
        while time.time() < deadline:
            try:
                with urllib.request.urlopen("http://127.0.0.1:8200/health", timeout=5) as r:
                    if r.status == 200:
                        report["server_healthy"] = True
                        break
            except Exception:
                pass
            time.sleep(3)
        if not report["server_healthy"]:
            report["error"] = (
                f"vLLM /health not ready within {health_timeout_s}s at "
                "127.0.0.1:8000 (still downloading/loading weights, or it crashed)"
            )
            return report

        # Query the OpenAI-compatible endpoint via VLLMLLM (in-process here).
        from chia.models.vllm import VLLMLLM

        llm = VLLMLLM(model=model, system_message=system_message, max_tokens=512)
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
    print(f"server_healthy : {report['server_healthy']}")
    print(f"model          : {report['model']}")
    print(f"base_url       : {report['base_url']}")
    print(f"success        : {report['success']}")
    print(f"result         : {report['result']!r}")
    print(f"output_tokens  : {report['output_tokens']}   num_turns: {report['num_turns']}")

    if report["error"]:
        print(f"  ! stage-1 task error: {report['error']}")
        return False

    ok = True
    if report["base_url"] != "http://localhost:8200/v1":
        ok = False
        print("  ! unexpected base_url (VLLMLLM default should be localhost:8200/v1)")
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
    """Dispatch VLLMLLM.prompt via .chia_remote and validate the QueryResult."""
    from chia.models.vllm import VLLMLLM

    print(f"\n=== remote dispatch (VLLMLLM.prompt.chia_remote on 'vllm_creds') ===")
    llm = VLLMLLM(
        model=model,
        system_message="You answer with a single word and nothing else.",
        max_tokens=512,
    )
    # Pass the instance explicitly as `self` (ChiaFunction method dispatch). The
    # task is gated on vllm_creds, so it runs in the container against the local
    # server; the QueryResult round-trips back across the worker boundary.
    cli = get(VLLMLLM.prompt.chia_remote(llm, "Reply with exactly the word: PONG", []))
    ok = cli.success is True and "PONG" in (cli.result or "").upper()
    print(f"success={cli.success} result={cli.result!r} -> {'OK' if ok else 'FAIL'}")
    if "Response" not in (cli.stream_result or ""):
        print("  ! stream_result missing the [Response] section")
        ok = False
    return ok


def main() -> int:
    print(f"[driver] connecting to ray cluster (working_dir={_REPO_ROOT})")
    # Ship the live repo as the worker working_dir, so workers import
    # chia.models.vllm from here even if the image's installed chia predates it.
    ray.init(
        address="auto",
        runtime_env={
            "working_dir": _REPO_ROOT,
            "excludes": [".venv/**", ".git/**", "**/__pycache__/**",
                         "**/*.pyc", "**/.pytest_cache/**",
                         "**/HELLOLOG/**", "**/tags"],
        },
    )

    print(f"Submitting vllm_query_e2e to the 'vllm_creds' resource (model={E2E_MODEL})...\n")
    report = get(vllm_query_e2e.chia_remote(
        E2E_MODEL,
        "Reply with exactly the word: PONG",
        "You answer with a single word and nothing else.",
        HEALTH_TIMEOUT_S,
    ))

    ok = _check_stage1(report)
    ok = _check_stage2(E2E_MODEL) and ok

    print("\n" + ("PASS: queried the vLLM OpenAI-compatible endpoint through the "
                  "Ray cluster and got a real completion."
                  if ok else "FAIL: see the markers above."))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
