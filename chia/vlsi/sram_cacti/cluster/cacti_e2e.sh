#!/usr/bin/env bash
#
# One-shot end-to-end: bring the local cacti cluster UP, run the smoke test
# (real cacti characterization + OpenROAD/OpenSTA validation of the generated
# Liberty/LEF), then bring the cluster DOWN — always, even if the test fails or
# you Ctrl-C. The script's exit code is the smoke test's, so it's CI-friendly.
#
# Activate any env with ray + chia first, then run:
#   conda activate myenv        # or: source /path/to/venv/bin/activate
#   ./cacti_e2e.sh
#
# (Reuses cacti_cluster.sh for up/down, so it derives the head-node env from
#  your active environment the same way.)
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../../../.." && pwd)"

echo "==> [e2e] bringing cluster up"
if ! "$HERE/cacti_cluster.sh" up -y; then
    echo "==> [e2e] cluster bring-up FAILED; attempting teardown" >&2
    "$HERE/cacti_cluster.sh" down -y || true
    exit 1
fi

# From here on, always tear the cluster down on exit (pass, fail, or interrupt).
trap '"$HERE/cacti_cluster.sh" down -y || true' EXIT INT TERM

echo "==> [e2e] running smoke test"
( cd "$REPO_ROOT" && python "$HERE/cacti_smoke_test.py" )
rc=$?

echo "==> [e2e] smoke test exit=${rc}; tearing cluster down"
exit "$rc"
