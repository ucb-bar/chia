"""Driver-side runner for the TortureRunNode end-to-end test.

Dispatches a torture run onto a `{"chipyard": 1}` worker via ``ray.remote``,
gets the ``TortureResult`` back as a Python object on this host, and
saves all artifacts (test.S, .dump, signatures, build stdout, summary).

Run:

    python chia/chipyard/test/torture_e2e_driver.py

Override knobs via env vars:
    TORTURE_CONFIG (default RocketConfig)
    TORTURE_CONFIG_PACKAGE (default chipyard)
    TORTURE_MODE (single | overnight | replay; default single)
    TORTURE_OVERNIGHT_MINUTES (default 30)
    TORTURE_OVERNIGHT_MAX_FAILURES (default 1; set high for full-duration soak)
    TORTURE_BUILD_TIMEOUT_S (default 3600 — fresh MegaBoom needs this)
    TORTURE_OUT_ROOT, TORTURE_TIMEOUT_S, TORTURE_NUM_CPUS
"""
from __future__ import annotations

import datetime
import json
import os
import sys
import time

import ray

CONFIG = os.environ.get("TORTURE_CONFIG", "RocketConfig")
CONFIG_PACKAGE = os.environ.get("TORTURE_CONFIG_PACKAGE", "chipyard")
OUT_ROOT = os.environ.get("TORTURE_OUT_ROOT", os.path.join(os.getcwd(), "torture_artifacts"))
MODE = os.environ.get("TORTURE_MODE", "single").lower()
OVERNIGHT_MINUTES = int(os.environ.get("TORTURE_OVERNIGHT_MINUTES", "30"))
OVERNIGHT_MAX_FAILURES = int(os.environ.get("TORTURE_OVERNIGHT_MAX_FAILURES", "1"))
BUILD_TIMEOUT_S = int(os.environ.get("TORTURE_BUILD_TIMEOUT_S", "3600"))
NUM_CPUS = int(os.environ.get("TORTURE_NUM_CPUS", "4"))

# Default torture timeout: cover (overnight_minutes + 10min slack) for OVERNIGHT,
# else 1hr for SINGLE/REPLAY.  TortureRunNode.torture() bumps this further if
# overnight_minutes implies a longer wall.
_default_timeout = OVERNIGHT_MINUTES * 60 + 600 if MODE == "overnight" else 3600
TIMEOUT_S = int(os.environ.get("TORTURE_TIMEOUT_S", str(_default_timeout)))


def _run_torture(num_cpus: int, config: str, config_package: str, mode_str: str,
                 timeout_seconds: int, build_timeout_seconds: int,
                 overnight_minutes: int, overnight_max_failures: int):
    """Dispatch TortureRunNode.torture_from_config (a @ChiaFunction reserving
    chipyard:1) onto a chia-chisel-build worker and return the TortureResult."""
    from chia.base.ChiaFunction import get
    from chia.chipyard.torture_run_node import TortureRunNode
    from chia.chipyard.state_def import TortureMode

    node = TortureRunNode(chipyard_path="/home/ray/chipyard",
                          timeout_seconds=timeout_seconds)
    return get(node.torture_from_config.options(num_cpus=num_cpus, max_retries=0).chia_remote(
        node,
        config=config,
        config_package=config_package,
        mode=TortureMode(mode_str),
        work_dir="/tmp/chia-torture-e2e",
        build_kwargs={"timeout_seconds": build_timeout_seconds, "make_jobs": num_cpus},
        overnight_minutes=overnight_minutes,
        overnight_max_failures=overnight_max_failures,
    ))


def _save(result, out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)

    summary = {
        "name": result.name,
        "config": result.config,
        "config_package": result.config_package,
        "mode": str(result.mode),
        "success": result.success,
        "num_tests": result.num_tests,
        "num_failures": result.num_failures,
        "returncode": result.returncode,
        "tests": [
            {"name": t.name, "success": t.success,
             "spike_sig_len": len(t.spike_sig),
             "rtlsim_sig_len": len(t.rtlsim_sig),
             "has_pseg": t.pseg_test_s is not None}
            for t in result.tests
        ],
    }
    if result.build_artifact is not None:
        summary["build"] = {
            "success": result.build_artifact.success,
            "config": result.build_artifact.config,
            "binary_name": result.build_artifact.simulator_binary_name,
            "returncode": result.build_artifact.returncode,
        }
    with open(os.path.join(out_dir, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)

    with open(os.path.join(out_dir, "torture.stdout.log"), "w") as f:
        f.write(result.stdout)
    with open(os.path.join(out_dir, "torture.stderr.log"), "w") as f:
        f.write(result.stderr)

    if result.build_artifact is not None:
        with open(os.path.join(out_dir, "build.stdout.log"), "w") as f:
            f.write(result.build_artifact.stdout)
        with open(os.path.join(out_dir, "build.stderr.log"), "w") as f:
            f.write(result.build_artifact.stderr)

    for t in result.tests:
        tdir = os.path.join(out_dir, "tests", t.name)
        os.makedirs(tdir, exist_ok=True)
        with open(os.path.join(tdir, "status"), "w") as f:
            f.write("PASS\n" if t.success else "FAIL\n")
        if t.test_s:
            with open(os.path.join(tdir, f"{t.name}.S"), "w") as f:
                f.write(t.test_s)
        if t.test_dump:
            with open(os.path.join(tdir, f"{t.name}.dump"), "w") as f:
                f.write(t.test_dump)
        if t.spike_sig:
            with open(os.path.join(tdir, f"{t.name}.spike.sig"), "w") as f:
                f.write(t.spike_sig)
        if t.rtlsim_sig:
            with open(os.path.join(tdir, f"{t.name}.rtlsim.sig"), "w") as f:
                f.write(t.rtlsim_sig)
        if t.pseg_test_s:
            with open(os.path.join(tdir, f"{t.name}_pseg.S"), "w") as f:
                f.write(t.pseg_test_s)


def main() -> int:
    chia_root = os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))))

    # The .venv has chia as a namespace package only when chia_root is on
    # sys.path; without this, get() can't unpickle TortureResult because
    # `from chia.chipyard.state_def import ...` fails on the driver.
    if chia_root not in sys.path:
        sys.path.insert(0, chia_root)

    print(f"[driver] connecting to ray cluster (working_dir={chia_root})")
    ray.init(
        address="auto",
        runtime_env={
            "working_dir": chia_root,
            "excludes": [".venv/**", ".git/**", "**/__pycache__/**",
                         "**/*.pyc", "**/.pytest_cache/**",
                         "**/HELLOLOG/**", "**/tags"],
        },
    )

    print(f"[driver] dispatching torture_from_config(config={CONFIG!r}, mode={MODE!r}, "
          f"overnight_minutes={OVERNIGHT_MINUTES}, max_failures={OVERNIGHT_MAX_FAILURES}, "
          f"num_cpus={NUM_CPUS}) onto a chipyard worker")
    t0 = time.time()
    result = _run_torture(
        NUM_CPUS, CONFIG, CONFIG_PACKAGE, MODE,
        TIMEOUT_S, BUILD_TIMEOUT_S,
        OVERNIGHT_MINUTES, OVERNIGHT_MAX_FAILURES,
    )
    elapsed = time.time() - t0

    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    out_dir = os.path.join(OUT_ROOT, f"{ts}-{result.config}")
    _save(result, out_dir)

    print(f"\n[driver] === RESULT ({elapsed:.0f}s) ===")
    print(f"  config       : {result.config} ({result.config_package})")
    print(f"  mode         : {result.mode}")
    print(f"  success      : {result.success}")
    print(f"  num_tests    : {result.num_tests}")
    print(f"  num_failures : {result.num_failures}")
    print(f"  artifacts    : {out_dir}")

    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
