# opencode-nvidia — opencode driving NVIDIA-hosted Nemotron

A minimal CHIA loop that runs [opencode](https://opencode.ai) against a model
served by **NVIDIA's hosted API** ([build.nvidia.com](https://build.nvidia.com)).
It exercises the `AdditionalModelProvider` on `chia.models.opencode.OpenCodeLLM`:
NVIDIA's OpenAI-compatible endpoint is declared as a *custom opencode provider*,
and the loop sends one prompt through opencode to Nemotron and verifies the
round-trip.

This is the hosted-API sibling of `examples/vllm-opencode` (same loop shape, no
GPU instance to provision — just an API key).

## Topology

The model is hosted by NVIDIA, so the cluster is a single opencode worker on the
local machine:

```
┌───────── ${THIS_MACHINE} ─────────┐
│   opencode worker                 │   HTTPS   ┌──────────────────────────┐
│   `opencode run`  ────────────────┼──────────▶│ integrate.api.nvidia.com │
│                                   │           │ (Nemotron)               │
└───────────────────────────────────┘           └──────────────────────────┘
```

The driver runs on the head. `OpenCodeLLM.prompt` is a ChiaFunction gated on the
`opencode_creds` resource, so it dispatches onto the opencode worker, which
calls NVIDIA's endpoint over HTTPS.

## Components

| File | Role |
|------|------|
| `nvidia_opencode_loop.py` | The loop: declares NVIDIA's endpoint as an `AdditionalModelProvider`, builds an `OpenCodeLLM(model="nim/<model>", additional_providers=[...])`, prompts it, and reports PASS/FAIL. Writes collateral to `out/`. |
| `cluster.yaml` | One node type on the local machine: `opencode` (runs the CLI). |
| `env.yml` | Conda env (`nvidia_opencode`) for the head. |
| `out/` | Per-run `<ts>_summary.json` + `<ts>_transcript.txt` (git-ignored). |

## How the provider is wired

```python
provider = AdditionalModelProvider(
    id="nim",                                   # avoids any built-in "nvidia" id
    name="NVIDIA (hosted)",
    base_url="https://integrate.api.nvidia.com/v1",
    api_key="{env:NVIDIA_API_KEY}",             # opencode expands at runtime —
                                                # the secret stays out of the
                                                # on-disk config
    models={"nvidia/nemotron-3-super-120b-a12b":
            {"limit": {"context": 262144,       # Nemotron 3 Super's 256K
                       "output": 4096}}},
)
llm = OpenCodeLLM(model="nim/nvidia/nemotron-3-super-120b-a12b",
                  additional_providers=[provider])
```

`OpenCodeLLM._build_config` emits this under the config's `provider` block, so
opencode loads it via `@ai-sdk/openai-compatible` and routes
`nim/nvidia/nemotron-3-super-120b-a12b` to NVIDIA (opencode splits
`provider/model` on the first `/`, so the vendor-prefixed model id keeps its own
slash).

The explicit `limit` matters: without it opencode assumes a 32000-token output
cap for unknown custom models, and the API rejects the request (`max_tokens`
above the model's cap). Any model listed on build.nvidia.com should work — the
model id is the full `<vendor>/<name>` string (`nvidia/*`, `meta/*`, `qwen/*`,
...); pick one that supports tool calling if you plan to go beyond this smoke
test. The default is the Nemotron 3 flagship reasoning MoE; its hosted siblings
are `nvidia/nemotron-3-nano-30b-a3b` and `nvidia/nemotron-3-ultra-550b-a55b`
(set `NVIDIA_OPENCODE_MODEL`, and `NVIDIA_OPENCODE_CONTEXT` to match).

## How the API key travels

`chia job submit` forwards to `ray job submit`, and the Ray job agent does
**not** inherit your submitting shell's environment — so the key is passed
explicitly via the job's runtime env (see Run below). The driver:

1. reads `NVIDIA_API_KEY` from its own env and fails fast (exit 2) if unset;
2. forwards it to the opencode worker via `runtime_env.env_vars`;
3. references it in the provider config as `{env:NVIDIA_API_KEY}`, which
   opencode expands in-process — the literal key never lands in the temp config
   file opencode writes to disk.

## Prerequisites

- `chia` installed on the head (`pip install -e .` from the repo root, or use
  `env.yml`).
- An NVIDIA API key (`nvapi-...`) from [build.nvidia.com](https://build.nvidia.com).
- `THIS_MACHINE` exported to the head machine's IP (see `cluster.yaml`).

## Run

```bash
export THIS_MACHINE=$(hostname -I | awk '{print $1}')
export NVIDIA_API_KEY=nvapi-...        # do NOT commit a real key

chia up examples/opencode-nvidia/cluster.yaml
cd examples/opencode-nvidia
chia job submit --working-dir . \
    --runtime-env-json "{\"env_vars\":{\"NVIDIA_API_KEY\":\"$NVIDIA_API_KEY\"}}" \
    -- python nvidia_opencode_loop.py
chia down examples/opencode-nvidia/cluster.yaml
```

The job's `--working-dir .` uploads this example dir and runs the driver from
its snapshot. The loop's own runtime env ships the head's current `chia` package
to the workers via `py_modules` (resolved from the importable package, not
`__file__`, so it stays correct under the working-dir snapshot); `out/` likewise
resolves to the real example dir on the head.

The loop exits non-zero if the round-trip fails, so a failed `chia job submit`
job flags a broken setup.

## Knobs (environment variables)

| Var | Default | Meaning |
|-----|---------|---------|
| `NVIDIA_API_KEY` | — (required) | Your build.nvidia.com key (`nvapi-...`). |
| `NVIDIA_OPENCODE_MODEL` | `nvidia/nemotron-3-super-120b-a12b` | Model id on the NVIDIA API (full `<vendor>/<name>` string from build.nvidia.com). |
| `NVIDIA_OPENCODE_BASE_URL` | `https://integrate.api.nvidia.com/v1` | OpenAI-compatible endpoint the opencode worker calls. |
| `NVIDIA_OPENCODE_TIMEOUT` | `600` | Per-prompt timeout (seconds). |
| `NVIDIA_OPENCODE_CONTEXT` | `262144` | Context window opencode assumes. Shrink if you pick a smaller-context model. |
| `NVIDIA_OPENCODE_OUTPUT` | `4096` | Max output tokens opencode requests. |

## Expected output

```
========================================================================
NVIDIA + opencode smoke test: PASSED
  model     : nim/nvidia/nemotron-3-super-120b-a12b
  endpoint  : https://integrate.api.nvidia.com/v1
  returncode: 0
  sentinel  : found ('NVIDIA_OPENCODE_OK')
------------------------------------------------------------------------
Response:
NVIDIA_OPENCODE_OK
========================================================================
```

The **hard** pass condition is a successful, non-empty response (proves the
opencode → NVIDIA → generation path). The sentinel token is a soft check —
reasoning models may chatter around the requested text — so it's reported but
not required for PASS.
