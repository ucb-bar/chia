"""opencode + on-prem vLLM — end-to-end smoke test (CHIA loop).

Verifies that :class:`chia.models.opencode.OpenCodeLLM` can drive a model served
by a **self-hosted vLLM** instance. It does so by declaring the vLLM endpoint as
a custom opencode provider (:class:`~chia.models.opencode.AdditionalModelProvider`),
constructing an ``OpenCodeLLM`` pointed at ``<provider>/<model>``, sending one
prompt, and asserting a clean round-trip.

Topology (see ``cluster.yaml``)
-------------------------------
Both workers land on the SAME GPU instance (``@vllm_aws:0``) and CHIA runs every
container with ``--net=host`` (chia/cluster/worker_provisioner.py), so they share
the host network namespace::

    ┌──────────────── AWS g5.xlarge  (@vllm_aws:0) ─────────────────┐
    │   vllm worker                    opencode worker              │
    │   `vllm serve` on :8200   <────  `opencode run`               │
    │   (Qwen2.5-3B-Instruct)          http://localhost:8200/v1     │
    └───────────────────────────────────────────────────────────────┘

This loop runs on the head. ``OpenCodeLLM.prompt`` is a ChiaFunction gated on the
``opencode_creds`` resource, so it is dispatched onto the opencode worker, which
then reaches the co-located vLLM server over ``localhost``.

Run
---
    chia up examples/vllm-opencode/cluster.yaml
    cd examples/vllm-opencode
    chia job submit --working-dir . -- python vllm_opencode_loop.py
    chia down examples/vllm-opencode/cluster.yaml

The job's ``--working-dir .`` uploads this example dir and runs the driver from
its snapshot (same as the other single-file example loops). The loop itself only
ships the head's current ``chia`` package to the workers via ``py_modules`` —
that matters here because the pre-built ``chia-opencode`` worker image predates
``AdditionalModelProvider``, so the worker must use the head's code to build the
config and unpickle the provider.

Environment knobs
-----------------
    VLLM_E2E_MODEL          model vLLM serves / opencode requests
                            (default: Qwen/Qwen2.5-3B-Instruct — must match the
                            VLLM_MODEL in cluster.yaml).
    VLLM_OPENCODE_BASE_URL  OpenAI-compatible endpoint the opencode worker calls
                            (default: http://localhost:8200/v1).
    VLLM_OPENCODE_API_KEY   optional key if vLLM was started with --api-key.
    VLLM_OPENCODE_TIMEOUT   per-prompt timeout in seconds (default: 600).
    VLLM_OPENCODE_CONTEXT   model context window opencode assumes (default: 32768
                            — must match --max-model-len in cluster.yaml).
    VLLM_OPENCODE_OUTPUT    max output tokens opencode requests (default: 2048;
                            opencode's ~6k-token baseline input + this must fit
                            inside the context window).
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import ray

from chia.base.ChiaFunction import get
from chia.base.llm_call import QueryResult
from chia.models.opencode import AdditionalModelProvider, OpenCodeLLM

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("vllm_opencode_loop")

# ---------------------------------------------------------------------------
# Config (env-overridable)
# ---------------------------------------------------------------------------

# Ship the head's chia package to the workers via py_modules so the opencode
# worker imports THIS chia (incl. AdditionalModelProvider) regardless of what
# its image baked in.  Resolve it from the importable chia package — NOT
# __file__ — so this still works when the loop runs from a `chia job submit`
# working_dir snapshot (where __file__ lives under runtime_resources/, not the
# repo).  Same pattern as examples/gem5_align.
import chia
# chia is a PEP 660 editable / namespace package (chia.__file__ is None), so take
# the package directory from __path__.  Depending on the setuptools version,
# __path__ can list BOTH the repo root and the package dir (and a finder-hook
# entry); pick the one that actually IS the chia package — the dir containing the
# `base` subpackage — never the repo root or the finder hook.
_CHIA_PKG = next(
    Path(p).resolve() for p in chia.__path__
    if Path(p).is_dir() and (Path(p) / "base").is_dir()
)
RUNTIME_ENV = {
    "py_modules": [str(_CHIA_PKG)],
    # excludes applies to runtime-env uploads (py_modules).
    "excludes": ["**/__pycache__", "**/*.pyc"],
}

# Write collateral into the REAL example dir (resolved via the chia package, so
# it persists on the head even when the driver runs from an ephemeral
# working_dir snapshot).
OUT_DIR = _CHIA_PKG.parent / "examples" / "vllm-opencode" / "out"

# The model vLLM serves — keep in sync with VLLM_MODEL in cluster.yaml. vLLM
# exposes it under its HF id (no --served-model-name was set), so that id is also
# the opencode model id.
MODEL_ID = os.environ.get("VLLM_E2E_MODEL", "Qwen/Qwen2.5-3B-Instruct")

# opencode provider id — the "provider" half of provider/model. Arbitrary but
# must be unique in the config; we pick "vllm".
PROVIDER_ID = "vllm"

# Endpoint the opencode worker calls. localhost works because the opencode and
# vLLM containers are --net=host on the same instance (see module docstring).
VLLM_BASE_URL = os.environ.get("VLLM_OPENCODE_BASE_URL", "http://localhost:8200/v1")

# vLLM here is launched without --api-key (see VLLM_SERVE_ARGS in cluster.yaml),
# so the value is ignored — but the @ai-sdk/openai-compatible client still needs
# *a* key, so we send a dummy. If your server was started with --api-key, set a
# real one; prefer the {env:NAME} template so the secret stays out of the
# temp config opencode writes to disk.
_api_key_env = os.environ.get("VLLM_OPENCODE_API_KEY")
VLLM_API_KEY = _api_key_env if _api_key_env else "dummy"

TIMEOUT_SECONDS = int(os.environ.get("VLLM_OPENCODE_TIMEOUT", "600"))

# Model limits opencode assumes. Two constraints, both learned the hard way:
#   * Without an explicit limit, opencode falls back to a 32000-token output cap
#     for unknown custom models, and vLLM rejects the request outright
#     (max_tokens=32000 cannot be greater than max_model_len).
#   * opencode's agentic baseline request (system prompt + built-in tool schemas
#     + environment context) is ~6k INPUT tokens before the user message, so
#     input + limit.output must fit in --max-model-len: 8192 was too small
#     (6145 input + 2048 output = 8193 > 8192); cluster.yaml serves 32768
#     (Qwen2.5's full native context).
# Keep CONTEXT in sync with --max-model-len in VLLM_SERVE_ARGS.
CONTEXT_LIMIT = int(os.environ.get("VLLM_OPENCODE_CONTEXT", "32768"))
OUTPUT_LIMIT = int(os.environ.get("VLLM_OPENCODE_OUTPUT", "2048"))

# A sentinel round-trip prompt: proves the opencode -> vLLM -> generation path,
# and that opencode routed to our custom provider (not a built-in one).
SENTINEL = "VLLM_OPENCODE_OK"
PROMPT = (
    "You are being health-checked. Reply with EXACTLY this text and nothing "
    f"else: {SENTINEL}"
)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run_loop() -> dict:
    ray.init(address="auto", runtime_env=RUNTIME_ENV)
    OUT_DIR.mkdir(exist_ok=True)
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 1) Declare the on-prem vLLM endpoint as a custom opencode provider.
    provider = AdditionalModelProvider(
        id=PROVIDER_ID,
        name="On-prem vLLM",
        base_url=VLLM_BASE_URL,
        api_key=VLLM_API_KEY,
        # Full-dict form: declare the model's real limits so opencode requests a
        # max_tokens that fits vLLM's --max-model-len (see CONTEXT_LIMIT above).
        models={MODEL_ID: {"limit": {"context": CONTEXT_LIMIT,
                                     "output": OUTPUT_LIMIT}}},
    )

    # 2) Build an OpenCodeLLM that targets <provider>/<model>. opencode splits on
    #    the first "/", so "vllm/Qwen/Qwen2.5-3B-Instruct" -> provider "vllm",
    #    model "Qwen/Qwen2.5-3B-Instruct".
    model = f"{PROVIDER_ID}/{MODEL_ID}"
    # NB: no log_dir — prompt() runs on the remote opencode worker, whose
    # container filesystem doesn't have this head-side path (a log_dir there
    # can't produce logs we could read back). The full transcript comes back in
    # resp.stream_result and is persisted to OUT_DIR on the head below.
    llm = OpenCodeLLM(
        model=model,
        system_message="You are a terse health-check assistant.",
        additional_providers=[provider],
        timeout_seconds=TIMEOUT_SECONDS,
    )

    logger.info("Prompting %s via opencode (endpoint %s) ...", model, VLLM_BASE_URL)
    logger.info("Provider config entry: %s", json.dumps(provider.to_config_entry()))

    # 3) Dispatch onto the opencode worker (gated on opencode_creds) and wait.
    resp: QueryResult = get(llm.prompt.chia_remote(llm, PROMPT))

    # 4) Judge the round-trip. Hard requirement: a successful, non-empty
    #    response. The sentinel is a soft check — small models sometimes chatter
    #    around the requested text — so we report it but don't fail on it alone.
    result_text = (resp.result or "").strip()
    round_trip_ok = bool(resp.success and result_text)
    sentinel_ok = SENTINEL in (resp.result or "")
    status = "passed" if round_trip_ok else "failed"

    summary = {
        "timestamp": run_ts,
        "model": model,
        "endpoint": VLLM_BASE_URL,
        "status": status,
        "round_trip_ok": round_trip_ok,
        "sentinel_ok": sentinel_ok,
        "returncode": resp.returncode,
        "result": resp.result,
        "stderr": resp.stderr,
    }

    # 5) Persist collateral for debugging a broken setup.
    (OUT_DIR / f"{run_ts}_summary.json").write_text(json.dumps(summary, indent=2))
    if resp.stream_result:
        (OUT_DIR / f"{run_ts}_transcript.txt").write_text(resp.stream_result)

    # 6) Report.
    print("=" * 72)
    print(f"vLLM + opencode smoke test: {status.upper()}")
    print(f"  model     : {model}")
    print(f"  endpoint  : {VLLM_BASE_URL}")
    print(f"  returncode: {resp.returncode}")
    print(f"  sentinel  : {'found' if sentinel_ok else 'NOT found'} ({SENTINEL!r})")
    print("-" * 72)
    print("Response:")
    print(resp.result or "<empty>")
    if not round_trip_ok and resp.stderr:
        print("-" * 72)
        print("stderr:")
        print(resp.stderr[:2000])
    print("=" * 72)
    logger.info("Loop finished: status=%s (collateral in %s)", status, OUT_DIR)
    return summary


if __name__ == "__main__":
    result = run_loop()
    # Non-zero exit on failure so `chia job submit` surfaces it as a failed job.
    sys.exit(0 if result["status"] == "passed" else 1)
