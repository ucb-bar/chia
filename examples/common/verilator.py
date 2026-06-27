"""Shared verilator-run helpers
"""
from __future__ import annotations

import functools
import os
import time

import ray

from chia.base.ChiaFunction import (
    ChiaFunction,
    TrackedRef,
    chia_cancel,
    chia_wait,
    get,
)
from chia.chipyard.state_def import BuildArtifact, BuildTarget, RunResult
from chia.chipyard.verilator_run_node import VerilatorRunNode
from chia.trace.profiler import get_profiler

from common.common_nodes import VerilatorTestOutcome
from common.state_def import TestBinary


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

_DEFAULT_WORK_DIR = "/home/ray/verilator/"

# Where dramsim ini files live, relative to this file's directory. Same
# convention as run_verilator_test() — bytes get loaded once per call and
# shipped to the run node (see VerilatorRunNode._setup).
_DRAMSIM_DIRNAME = "dramsim_ini"


def _load_dramsim_ini() -> dict[str, bytes]:
    here = os.path.dirname(os.path.abspath(__file__))
    dramsim_dir = os.path.join(here, _DRAMSIM_DIRNAME)
    ini: dict[str, bytes] = {}
    if os.path.isdir(dramsim_dir):
        for fname in os.listdir(dramsim_dir):
            fpath = os.path.join(dramsim_dir, fname)
            if os.path.isfile(fpath):
                with open(fpath, "rb") as f:
                    ini[fname] = f.read()
    return ini


# ---------------------------------------------------------------------------
# Debug-build verilator runner with selective waveform capture
# ---------------------------------------------------------------------------


@ChiaFunction(resources={"verilator_run": 1.0})
def run_verilator_test(
    artifact: BuildArtifact,
    test_binary: TestBinary,
    work_dir: str = _DEFAULT_WORK_DIR,
) -> RunResult:
    """Run a single (non-debug) verilator test using a BuildArtifact.

    Dispatches the FireSim metasim runner for metasim artifacts, else the
    plain verilator runner with the dramsim ini files + ``+loadmem``. Always
    requests TMA-counter dumps via ``+dump-tma-counters``.
    """
    get_profiler().add_info({"branch_name": artifact.name})

    ini = _load_dramsim_ini()

    run_node = VerilatorRunNode()
    if artifact.target == BuildTarget.FIRESIM_METASIM_VERILATOR:
        return run_node.run_metasim(
            artifact=artifact,
            test_binary_content=test_binary.content,
            test_binary_name=test_binary.name,
            work_dir=work_dir,
            plusargs={"+dump-tma-counters": None},
            timeout_seconds=test_binary.timeout_seconds,
            timeout_cycles=test_binary.timeout_cycles,
        )
    return run_node.run(
        artifact=artifact,
        test_binary_content=test_binary.content,
        test_binary_name=test_binary.name,
        work_dir=work_dir,
        plusargs={"+dump-tma-counters": None, "+loadmem": test_binary.name},
        dramsim_ini_files=ini,
        timeout_seconds=test_binary.timeout_seconds,
        timeout_cycles=test_binary.timeout_cycles,
    )


def dispatch_verilator_tests(
    artifacts_by_threads: dict[int, BuildArtifact],
    test_binaries: list[TestBinary],
    max_failures: int = 4,
    failure_timeout_seconds: float = 600,  # 10 minutes after first failure
) -> VerilatorTestOutcome:
    """Dispatch (non-debug) verilator tests in parallel from the head node.

    Each test is dispatched with the BuildArtifact compiled for its thread
    count (``test.verilator_threads``) and consumes that many verilator_run
    slots. Early-exits if *max_failures* tests fail, or once at least one test
    has failed and *failure_timeout_seconds* have elapsed since the first
    failure; remaining Ray tasks are cancelled on early-exit.
    """
    print(f"Dispatching {len(test_binaries)} verilator tests")

    def _submit(test: TestBinary, n: int) -> "ray.ObjectRef":
        return (run_verilator_test
                .options(resources={"verilator_run": float(n)}, scheduling_strategy="SPREAD")
                .chia_remote(artifacts_by_threads[n], test))

    tracked: list[TrackedRef] = []
    for test in test_binaries:
        n = test.verilator_threads
        print(f"  Dispatching: {test.name} (threads={n})")
        tracked.append(TrackedRef(
            ref=_submit(test, n),
            submit_fn=functools.partial(_submit, test, n),
            label=test.name,
        ))

    total = len(tracked)
    results: list[RunResult] = []
    failed: list[RunResult] = []
    pending: list[TrackedRef] = list(tracked)
    first_failure_time: float | None = None

    while pending:
        ready, pending = chia_wait(pending, num_returns=1)

        # Check failure timeout even if nothing completed this tick
        if first_failure_time is not None and time.time() - first_failure_time >= failure_timeout_seconds:
            print(f"\n  {failure_timeout_seconds}s since first failure — cancelling {len(pending)} remaining tests...")
            for tr in pending:
                chia_cancel(tr.ref, force=True)
            return VerilatorTestOutcome(results=results, failed=failed, cancelled=True)

        if not ready:
            continue

        result = get(ready[0].ref)
        results.append(result)

        if result.success:
            print(f"  [{len(results)}/{total}] PASS: {result.test_binary_name}")
        else:
            failed.append(result)
            if first_failure_time is None:
                first_failure_time = time.time()
            print(f"  [{len(results)}/{total}] FAIL: {result.test_binary_name} (rc={result.returncode})")
            print(f"  --- {result.test_binary_name} log ---")
            print(result.log)
            print(f"  --- {result.test_binary_name} out ---")
            print(result.out)

            if len(failed) >= max_failures:
                print(f"\n  {max_failures} failures reached — cancelling {len(pending)} remaining tests...")
                for tr in pending:
                    chia_cancel(tr.ref, force=True)
                return VerilatorTestOutcome(results=results, failed=failed, cancelled=True)

    print("Finished all verilator tests")
    return VerilatorTestOutcome(results=results, failed=failed, cancelled=False)
