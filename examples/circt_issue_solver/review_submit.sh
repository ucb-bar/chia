#!/usr/bin/env bash
# Submit the PR-review-feedback flow as a job via `chia job submit` (logs in the
# dashboard / via `chia job logs <id>`), same rationale as fix_issues_submit.sh.
#
# Usage (on the HOST head, in the activated env, with GITHUB_TOKEN set):
#   conda activate circtissues
#   export GITHUB_TOKEN=...
#   ./review_submit.sh --pr 5:7949
#   ./review_submit.sh --pr 5:7949 --pr 6:7388 --pr 7:10104
#   NO_WAIT=1 ./review_submit.sh --pr 5:7949        # detach; watch the dashboard
#
# --pr PR:ISSUE maps a GITHUB_REPO (config.py) PR number to the paired issue
# number. The PR's feedback, diff, and the issue are read from GitHub; only the
# (optional) repro is read locally from issue_logs_to_pr/issue_<N>/repro.
# GITHUB_TOKEN is injected via runtime-env so the current token is used.
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
  -- "$PYBIN" "$FLOW_DIR/review_loop.py" "$@"
