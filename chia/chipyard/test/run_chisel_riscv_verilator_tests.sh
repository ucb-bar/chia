#!/usr/bin/env bash
# Bring up the test cluster, run the chisel+riscv -> verilator integration
# tests, and tear the cluster down — even if a test fails.
#
# Usage:
#   ./test/run_chisel_riscv_verilator_tests.sh
#
# Exit code is non-zero if any test failed.

set -u                                        # error on undefined vars
set -o pipefail                               # surface failures in pipelines

# Paths relative to the chia repo root (the script can be invoked from
# anywhere — `cd` to the repo root for consistent `ray job submit`'ed
# working-dir behavior).
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

CLUSTER_YAML="test/test_chisel_riscv_verilator_cluster.yaml"
RAY_HEAD_IP="${RAY_HEAD_IP}"             # override via env if your head moves
RAY_ADDRESS="${RAY_HEAD_IP}:6379"

TESTS=(
    "test/test_riscv_build_failure.py:RiscvBuildNode failure path (broken C source)"
    "test/test_chisel_riscv_verilator_e2e.py:Chisel + RISC-V parallel build -> Verilator e2e"
)

# ----------------------------------------------------------------------------

banner() {
    local title="$1"
    local bar
    bar=$(printf '=%.0s' $(seq 1 $((${#title} + 8))))
    echo
    echo "$bar"
    echo "==  $title  =="
    echo "$bar"
}

teardown() {
    banner "Tearing down cluster"
    chia down "$CLUSTER_YAML" -y || true
}

# Always tear down on exit, even if the script aborts.
trap teardown EXIT

# --- Bring cluster up -------------------------------------------------------

banner "Bringing up cluster ($CLUSTER_YAML)"
chia up "$CLUSTER_YAML" -y

# --- Run each test ----------------------------------------------------------

failures=0
for spec in "${TESTS[@]}"; do
    script="${spec%%:*}"
    title="${spec#*:}"

    banner "TEST: $title"
    echo "    script: $script"
    echo "    cluster: $RAY_ADDRESS"
    if ray job submit --address "$RAY_ADDRESS" --working-dir . -- python "$script"; then
        echo "    -> PASSED"
    else
        echo "    -> FAILED"
        failures=$((failures + 1))
    fi
done

# --- Summary ----------------------------------------------------------------

banner "Summary"
echo "  ${#TESTS[@]} tests run, $failures failed"
exit "$failures"
