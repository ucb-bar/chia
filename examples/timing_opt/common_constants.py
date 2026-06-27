"""Ray runtime-env config for running the timing-improvement example as a
standalone script.
"""

from pathlib import Path

# Repo checkout root, resolved relative to this file
# (examples/timing_opt/common_constants.py → parents[2]) so nothing is
# hardcoded to one clone location.
_REPO_ROOT = Path(__file__).resolve().parents[2]

RUNTIME_ENV = {
    # Ship the checkout as the job's working_dir, and the packages the
    # pipeline's ChiaFunctions are pickled against (chia.*, common.*,
    # timing_opt.*) as py_modules so they are importable top-level on every
    # worker — the repo-root working_dir alone does not put examples/ on the
    # workers' sys.path.
    "working_dir": str(_REPO_ROOT),
    "py_modules": [
        str(_REPO_ROOT / "chia"),
        str(_REPO_ROOT / "examples" / "common"),
        str(_REPO_ROOT / "examples" / "sky130_vlsi"),
        str(_REPO_ROOT / "examples" / "timing_opt"),
    ],
    # gitignore-style patterns, applied to working_dir and py_modules uploads
    # alike. DB/ (the head-only SQLite store + files tree), the large
    # verilatorbins/ subdirs (only asmtests/embench are consumed),
    # benchmarks/ and caches dominate the upload size.
    "excludes": [
        "/DB/",
        "/fpga_builds/",
        "verilatorbins/ubench/",
        "__pycache__",
        ".mypy_cache",
        "benchmarks/",
        "out/",
    ],
}
