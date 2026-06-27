#!/usr/bin/env bash
# Submit the circt_issue_solver flow as a job via `chia job submit` so its driver
# logs appear in the Ray dashboard (Jobs view) and via `chia job logs <id>`.
#
# Why this vs. `python circt_issue_loop.py`: running the driver directly also
# registers a Ray job, but as a DRIVER-type job whose stdout/stderr the job
# server does NOT capture. Only `chia job submit` (SUBMISSION) jobs get
# retrievable, dashboard-visible logs.
#
# Usage (on the HOST head, in the activated env, with GITHUB_TOKEN set):
#   conda activate circtissues
#   export GITHUB_TOKEN=...
#   ./fix_issues_submit.sh --max-issues 2
#   ./fix_issues_submit.sh --issue 10588
#   NO_WAIT=1 ./fix_issues_submit.sh --max-issues 20      # detach; watch in the dashboard
#
# Notes:
#   - The job server / dashboard binds to 127.0.0.1:8265 on the head, so run
#     this ON the head. View the dashboard at http://localhost:8265 (forward the
#     port to reach it from your laptop). Override with RAY_JOB_ADDR=... .
#   - GITHUB_TOKEN is injected via the job's runtime-env env_vars so the CURRENT
#     token is used (the Ray daemon's own env may hold a stale one). The token
#     value is therefore stored in the job's runtime_env metadata, visible in
#     `chia job` output / the dashboard — acceptable for a private dashboard.
#   - python/chia are taken from PATH (the activated conda env). Override with
#     CIRCT_SOLVER_PY / CIRCT_SOLVER_CHIA if they live elsewhere.
#   - Default tails logs until the job exits; Ctrl-C just detaches the tail (the
#     job keeps running). Set NO_WAIT=1 to return immediately.
set -euo pipefail

ADDR="${RAY_JOB_ADDR:-http://localhost:8265}"
FLOW_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYBIN="${CIRCT_SOLVER_PY:-python}"
CHIABIN="${CIRCT_SOLVER_CHIA:-chia}"

: "${GITHUB_TOKEN:?set GITHUB_TOKEN before submitting}"

WAIT_FLAG=()
[ "${NO_WAIT:-0}" = "1" ] && WAIT_FLAG=(--no-wait)

exec "$CHIABIN" job submit \
  --address "$ADDR" \
  "${WAIT_FLAG[@]}" \
  --runtime-env-json "{\"env_vars\": {\"GITHUB_TOKEN\": \"${GITHUB_TOKEN}\"}}" \
  -- "$PYBIN" "$FLOW_DIR/circt_issue_loop.py" "$@"
