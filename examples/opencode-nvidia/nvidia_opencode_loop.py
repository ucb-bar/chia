"""opencode + NVIDIA-hosted Nemotron — end-to-end smoke test (CHIA loop).

Verifies that :class:`chia.models.opencode.OpenCodeLLM` can drive a model served
by **NVIDIA's hosted API** (build.nvidia.com). It does so by declaring the
NVIDIA OpenAI-compatible endpoint as a custom opencode provider
(:class:`~chia.models.opencode.AdditionalModelProvider`), constructing an
``OpenCodeLLM`` pointed at ``<provider>/<model>``, sending one prompt, and
asserting a clean round-trip.

Topology (see ``cluster.yaml``)
-------------------------------
The model is hosted by NVIDIA, so no GPU node is needed — the only worker is the
opencode container on the local machine (``${THIS_MACHINE}``), which calls out
over HTTPS::

    ┌───────── ${THIS_MACHINE} ─────────┐
    │   opencode worker                 │   HTTPS   ┌─────────────────────────┐
    │   `opencode run`  ────────────────┼──────────▶│ integrate.api.nvidia.com │
    │                                   │           │ (Nemotron)              │
    └───────────────────────────────────┘           └─────────────────────────┘

This loop runs on the head. ``OpenCodeLLM.prompt`` is a ChiaFunction gated on
the ``opencode_creds`` resource, so it is dispatched onto the opencode worker.

Auth
----
Requires a real NVIDIA API key (``nvapi-...``, from https://build.nvidia.com).
The driver reads ``NVIDIA_API_KEY`` from its own environment and fails fast if
unset. Because ``chia job submit`` runs the driver via the Ray job agent (which
does NOT inherit your submitting shell's env), pass the key through the job's
runtime env — see the Run section. The driver then forwards it to the opencode
worker via ``runtime_env.env_vars``, and the opencode config references it as an
``{env:NVIDIA_API_KEY}`` template so the literal secret never lands in the temp
config file opencode writes to disk.

Run
---
    export NVIDIA_API_KEY=nvapi-...
    chia up examples/opencode-nvidia/cluster.yaml
    cd examples/opencode-nvidia
    chia job submit --working-dir . \
        --runtime-env-json "{\"env_vars\":{\"NVIDIA_API_KEY\":\"$NVIDIA_API_KEY\"}}" \
        -- python nvidia_opencode_loop.py
    chia down examples/opencode-nvidia/cluster.yaml

The job's ``--working-dir .`` uploads this example dir and runs the driver from
its snapshot (same as the other single-file example loops). The loop itself only
ships the head's current ``chia`` package to the workers via ``py_modules`` —
that matters here because the pre-built ``chia-opencode`` worker image predates
``AdditionalModelProvider``, so the worker must use the head's code to build the
config and unpickle the provider.

Environment knobs
-----------------
    NVIDIA_API_KEY             REQUIRED — your build.nvidia.com key (nvapi-...).
    NVIDIA_OPENCODE_MODEL      model id on the NVIDIA API (default:
                               nvidia/nemotron-3-super-120b-a12b). Any model
                               listed on build.nvidia.com works; the id is
                               the full "<vendor>/<name>" string.
    NVIDIA_OPENCODE_BASE_URL   OpenAI-compatible endpoint
                               (default: https://integrate.api.nvidia.com/v1).
    NVIDIA_OPENCODE_TIMEOUT    per-prompt timeout in seconds (default: 600).
    NVIDIA_OPENCODE_CONTEXT    model context window opencode assumes
                               (default: 262144 — Nemotron 3 Super's 256K).
    NVIDIA_OPENCODE_OUTPUT     max output tokens opencode requests (default:
                               4096; without an explicit limit opencode asks for
                               32000, which the API rejects for most models).
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
logger = logging.getLogger("nvidia_opencode_loop")

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
OUT_DIR = _CHIA_PKG.parent / "examples" / "opencode-nvidia" / "out"

# The model to request from NVIDIA's API. Ids are the full "<vendor>/<name>"
# strings from build.nvidia.com (nvidia/*, meta/*, qwen/*, ...). Default is the
# Nemotron 3 flagship reasoning MoE (120B total / 12B active); siblings on the
# same API: nvidia/nemotron-3-nano-30b-a3b, nvidia/nemotron-3-ultra-550b-a55b.
MODEL_ID = os.environ.get(
    "NVIDIA_OPENCODE_MODEL", "nvidia/nemotron-3-super-120b-a12b"
)

# opencode provider id — the "provider" half of provider/model. Arbitrary but
# must be unique in the config; "nim" avoids colliding with any built-in
# provider opencode may know as "nvidia".
PROVIDER_ID = "nim"

# NVIDIA's OpenAI-compatible endpoint.
NVIDIA_BASE_URL = os.environ.get(
    "NVIDIA_OPENCODE_BASE_URL", "https://integrate.api.nvidia.com/v1"
)

# Real key required (unlike the vllm-opencode example's dummy). Checked in
# run_loop so the error message beats an opaque 401 from the worker.
NVIDIA_API_KEY = os.environ.get("NVIDIA_API_KEY", "")

TIMEOUT_SECONDS = int(os.environ.get("NVIDIA_OPENCODE_TIMEOUT", "600"))

# Model limits opencode assumes. Without an explicit limit, opencode falls back
# to a 32000-token output cap for unknown custom models, which NVIDIA's API
# rejects for most models (max_tokens > allowed). 262144 is Nemotron 3 Super's
# native 256K context (max_position_embeddings in the HF config); 4096 output
# is plenty for a health check and safely under every hosted model's cap.
# Adjust CONTEXT if you point MODEL_ID at a smaller-context model.
CONTEXT_LIMIT = int(os.environ.get("NVIDIA_OPENCODE_CONTEXT", "262144"))
OUTPUT_LIMIT = int(os.environ.get("NVIDIA_OPENCODE_OUTPUT", "4096"))

# A sentinel round-trip prompt: proves the opencode -> NVIDIA -> generation
# path, and that opencode routed to our custom provider (not a built-in one).
SENTINEL = "NVIDIA_OPENCODE_OK"
PROMPT = (
    "You are being health-checked. Reply with EXACTLY this text and nothing "
    f"else: {SENTINEL}"
)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run_loop() -> dict:
    if not NVIDIA_API_KEY:
        logger.error(
            "NVIDIA_API_KEY is not set in the driver's environment. When using "
            "`chia job submit`, pass it through the job runtime env:\n"
            "  chia job submit --working-dir . --runtime-env-json "
            "'{\"env_vars\":{\"NVIDIA_API_KEY\":\"nvapi-...\"}}' "
            "-- python nvidia_opencode_loop.py"
        )
        sys.exit(2)

    # Forward the key to the opencode worker's process env. The provider config
    # below references it as {env:NVIDIA_API_KEY}, which opencode expands at
    # runtime — so the literal key never lands in the on-disk temp config.
    runtime_env = dict(RUNTIME_ENV)
    runtime_env["env_vars"] = {"NVIDIA_API_KEY": NVIDIA_API_KEY}

    # When the key arrived via `chia job submit --runtime-env-json`, the job's
    # runtime env already carries NVIDIA_API_KEY and ray.init refuses to merge
    # the duplicate key (even with an identical value). This flag tells Ray to
    # merge with the driver's env winning per-key; the job's working_dir is
    # preserved either way.
    os.environ.setdefault("RAY_OVERRIDE_JOB_RUNTIME_ENV", "1")

    ray.init(address="auto", runtime_env=runtime_env)
    OUT_DIR.mkdir(exist_ok=True)
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 1) Declare NVIDIA's hosted endpoint as a custom opencode provider.
    provider = AdditionalModelProvider(
        id=PROVIDER_ID,
        name="NVIDIA (hosted)",
        base_url=NVIDIA_BASE_URL,
        api_key="{env:NVIDIA_API_KEY}",
        # Full-dict form: declare the model's real limits so opencode requests a
        # max_tokens the API accepts (see CONTEXT_LIMIT/OUTPUT_LIMIT above).
        models={MODEL_ID: {"limit": {"context": CONTEXT_LIMIT,
                                     "output": OUTPUT_LIMIT}}},
    )

    # 2) Build an OpenCodeLLM that targets <provider>/<model>. opencode splits on
    #    the first "/", so "nim/nvidia/nemotron-3-super-120b-a12b" ->
    #    provider "nim", model "nvidia/nemotron-3-super-120b-a12b".
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

    logger.info("Prompting %s via opencode (endpoint %s) ...", model, NVIDIA_BASE_URL)
    logger.info("Provider config entry: %s", json.dumps(provider.to_config_entry()))

    # 3) Dispatch onto the opencode worker (gated on opencode_creds) and wait.
    resp: QueryResult = get(llm.prompt.chia_remote(llm, PROMPT))

    # 4) Judge the round-trip. Hard requirement: a successful, non-empty
    #    response. The sentinel is a soft check — reasoning models sometimes
    #    chatter around the requested text — so we report it but don't fail on
    #    it alone.
    result_text = (resp.result or "").strip()
    round_trip_ok = bool(resp.success and result_text)
    sentinel_ok = SENTINEL in (resp.result or "")
    status = "passed" if round_trip_ok else "failed"

    summary = {
        "timestamp": run_ts,
        "model": model,
        "endpoint": NVIDIA_BASE_URL,
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
    print(f"NVIDIA + opencode smoke test: {status.upper()}")
    print(f"  model     : {model}")
    print(f"  endpoint  : {NVIDIA_BASE_URL}")
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
