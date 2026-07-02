# vllm-opencode — opencode driving an on-prem vLLM model

A minimal CHIA loop that runs [opencode](https://opencode.ai)
with a **self-hosted vLLM** model. It exercises the
`AdditionalModelProvider` on `chia.models.opencode.OpenCodeLLM`: the on-prem vLLM
endpoint is declared as a *custom opencode provider*, and the loop sends one
prompt through opencode to that model and verifies the round-trip.

## Topology

Both workers are scheduled onto the **same** GPU instance (`@vllm_aws:0`), and
CHIA runs every container with `--net=host`, so they share the host network:

```
┌──────────────── AWS g5.xlarge  (@vllm_aws:0) ─────────────────┐
│   vllm worker                    opencode worker              │
│   `vllm serve` on :8200   <────  `opencode run`               │
│   (Qwen2.5-3B-Instruct)          http://localhost:8200/v1     │
└───────────────────────────────────────────────────────────────┘
```

The driver runs on the head. `OpenCodeLLM.prompt` is a ChiaFunction gated on the
`opencode_creds` resource, so it dispatches onto the opencode worker, which then
reaches the co-located vLLM server over `localhost:8200`.

## Components

| File | Role |
|------|------|
| `vllm_opencode_loop.py` | The loop: declares the vLLM endpoint as an `AdditionalModelProvider`, builds an `OpenCodeLLM(model="vllm/<model>", additional_providers=[...])`, prompts it, and reports PASS/FAIL. Writes collateral to `out/`. |
| `cluster.yaml` | Two node types on one AWS GPU instance: `vllm` (serves the model) and `opencode` (runs the CLI). |
| `env.yml` | Conda env (`vllm_opencode`) for the head. |
| `out/` | Per-run `<ts>_summary.json` + `<ts>_transcript.txt` (git-ignored). |

## How the provider is wired

```python
provider = AdditionalModelProvider(
    id="vllm",
    name="On-prem vLLM",
    base_url="http://localhost:8200/v1",   # reachable via --net=host
    api_key="dummy",                        # vLLM ignores it (no --api-key)
    models={"Qwen/Qwen2.5-3B-Instruct":     # declare the model's REAL limits:
            {"limit": {"context": 32768,    # = --max-model-len in cluster.yaml
                       "output": 2048}}},
)
llm = OpenCodeLLM(model="vllm/Qwen/Qwen2.5-3B-Instruct",
                  additional_providers=[provider])
```

`OpenCodeLLM._build_config` emits this under the config's `provider` block, so
opencode loads it via `@ai-sdk/openai-compatible` and routes
`vllm/Qwen/Qwen2.5-3B-Instruct` to your server (opencode splits `provider/model`
on the first `/`, so the HF-style model id keeps its own slash).

The explicit `limit` matters: without it opencode assumes a 32000-token output
cap for unknown custom models, and vLLM rejects the request
(`max_tokens=32000 cannot be greater than max_model_len`). `limit.context`
must match the server's `--max-model-len`; `limit.output` is the `max_tokens`
opencode requests.

Size the context generously: opencode is an agentic CLI whose baseline request
(system prompt + built-in tool schemas + environment context) is already large in
**input** tokens, and input + `limit.output` must fit in `--max-model-len`.

## Prerequisites

- `chia` installed on the head (`pip install -e .` from the repo root, or use
  `env.yml`).
- `cluster.yaml` filled in: `head_ip`, `${EC2_KEYNAME}`, and a GPU AMI id for
  your region (see the TODOs in the file). For **gated** models add an
  `HF_TOKEN` to the `vllm` `run_options` (don't commit a real token).

## Run

First, setup AWS creds and address the AMI todo in the cluster.yaml

```bash
chia up examples/vllm-opencode/cluster.yaml
cd examples/vllm-opencode
chia job submit --working-dir . -- python vllm_opencode_loop.py
chia down examples/vllm-opencode/cluster.yaml
```

The job's `--working-dir .` uploads this example dir and runs the driver from
its snapshot.
The loop's own `RUNTIME_ENV` only ships the head's current `chia` package to the
workers via `py_modules`
The chia package is resolved from the
importable package (not `__file__`), so it stays correct under the working-dir
snapshot; `out/` likewise resolves to the real example dir on the head.

The loop exits non-zero if the round-trip fails, so a failed `chia job submit`
job flags a broken setup.

## Knobs (environment variables/to be changed in cluster)

| Var | Default | Meaning |
|-----|---------|---------|
| `VLLM_E2E_MODEL` | `Qwen/Qwen2.5-3B-Instruct` | Model vLLM serves / opencode requests. Keep in sync with `VLLM_MODEL` in `cluster.yaml`. |
| `VLLM_OPENCODE_BASE_URL` | `http://localhost:8200/v1` | OpenAI-compatible endpoint the opencode worker calls. |
| `VLLM_OPENCODE_API_KEY` | `dummy` | Set only if vLLM was launched with `--api-key`. |
| `VLLM_OPENCODE_TIMEOUT` | `600` | Per-prompt timeout (seconds). |
| `VLLM_OPENCODE_CONTEXT` | `32768` | Context window opencode assumes. Keep in sync with `--max-model-len` in `cluster.yaml`. |
| `VLLM_OPENCODE_OUTPUT` | `2048` | Max output tokens opencode requests (opencode's ~6k-token baseline input + this must fit inside the context window). |

## Expected output

```
========================================================================
vLLM + opencode smoke test: PASSED
  model     : vllm/Qwen/Qwen2.5-3B-Instruct
  endpoint  : http://localhost:8200/v1
  returncode: 0
  sentinel  : found ('VLLM_OPENCODE_OK')
------------------------------------------------------------------------
Response:
VLLM_OPENCODE_OK
========================================================================
```

The **hard** pass condition is a successful, non-empty response (proves the
opencode → vLLM → generation path). The sentinel token is a soft check — a small
model may chatter around the requested text — so it's reported but not required
for PASS.
