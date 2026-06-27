#!/usr/bin/env bash
#
# Bring the local vLLM test cluster up/down using YOUR currently-active
# environment — no hardcoded venv. Activate any conda/venv that has ray +
# chia, then run this from anywhere:
#
#   conda activate myenv        # or: source /path/to/venv/bin/activate
#   ./vllm_cluster.sh up        # also: up --dry-run
#   ./vllm_cluster.sh down
#
# chia launches the head node via a fresh SSH shell that does NOT inherit your
# interactive env, so we derive an activation command from the active env and
# pass it through CHIA_HEAD_ENV_SETUP (the YAML's head_env_commands expands it).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# .../chia/models/tests/cluster -> repo root is 4 levels up
REPO_ROOT="$(cd "$HERE/../../../.." && pwd)"
# Which cluster config to use. Defaults to the local-GPU config; set
# CHIA_VLLM_YAML=vllm_aws.yaml to launch the vLLM worker on an AWS GPU instance.
YAML="$HERE/${CHIA_VLLM_YAML:-vllm_local.yaml}"

if [ "$#" -lt 1 ]; then
    echo "usage: $0 up|down [extra chia args]" >&2
    exit 1
fi

# Derive a head-node activation command from the active environment.
if [ -n "${VIRTUAL_ENV:-}" ]; then
    export CHIA_HEAD_ENV_SETUP="source ${VIRTUAL_ENV}/bin/activate"
elif [ -n "${CONDA_PREFIX:-}" ]; then
    export CHIA_HEAD_ENV_SETUP="source $(conda info --base)/etc/profile.d/conda.sh && conda activate ${CONDA_DEFAULT_ENV:-base}"
else
    echo "ERROR: no active venv/conda env detected." >&2
    echo "Activate an environment with ray + chia first, then re-run." >&2
    exit 1
fi

cd "$REPO_ROOT"

# Preflight (from the repo root, where `chia` resolves): confirm THIS env can
# import what the head/driver need.
if ! python -c "import ray, chia.cli.main" >/dev/null 2>&1; then
    echo "ERROR: the active env can't import ray + chia (from $REPO_ROOT)." >&2
    echo "Use an env with ray (2.54.0) + chia installed/importable." >&2
    exit 1
fi

echo "[vllm-cluster] head env activation: ${CHIA_HEAD_ENV_SETUP}"
exec python -m chia.cli.main "$@" "$YAML"
