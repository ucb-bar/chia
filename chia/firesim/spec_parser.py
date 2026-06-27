"""SPEC benchmark result parsing utilities.

Parses per-benchmark CSVs extracted from FireMarshal rootfs /output
and computes SPEC scores (ratio against reference times, geometric mean).
"""

from __future__ import annotations

import csv
import io
import math

from chia.cluster.log import get_logger

logger = get_logger("firesim.spec_parser")

# SPEC CPU 2017 Integer Speed reference times (ref dataset)
SPEC17_INTSPEED_REF_TIMES: dict[str, float] = {
    "600.perlbench_s": 1776.0,
    "602.gcc_s": 3981.0,
    "605.mcf_s": 4723.0,
    "620.omnetpp_s": 1622.0,
    "623.xalancbmk_s": 1398.0,
    "625.x264_s": 1761.0,
    "631.deepsjeng_s": 1446.0,
    "641.leela_s": 1710.0,
    "648.exchange2_s": 2612.0,
    "657.xz_s": 6153.0,
}


def parse_spec_csv(csv_content: str, benchmark_name: str = "") -> dict[str, float] | None:
    """Parse a single SPEC benchmark CSV.

    Expected format: name,RealTime,UserTime,KernelTime
    (one header row, one data row)

    Reads the first data row, looks up the benchmark's SPEC CPU 2017
    Integer Speed reference time (matching on the base name prefix so that
    sub-workload suffixes are stripped), and computes the SPEC score as
    ``ref_time / real_time``. The score is ``0.0`` when either the reference
    time or the measured real time is non-positive or unknown.

    Args:
        csv_content: Raw CSV text containing a header row and a data row.
        benchmark_name: Fallback benchmark name, used when the CSV row has no
            ``name`` field and for log messages on parse failure.

    Returns:
        Dict with ``RealTime``, ``UserTime``, and ``KernelTime`` (floats, in
        seconds) plus the computed ``score`` (float), or ``None`` if the CSV
        is empty or cannot be parsed.
    """
    try:
        reader = csv.DictReader(io.StringIO(csv_content))
        for row in reader:
            name = row.get("name", benchmark_name)
            real_time = float(row["RealTime"])
            user_time = float(row["UserTime"])
            kernel_time = float(row["KernelTime"])

            # Compute score: ref_time / real_time
            # For benchmarks like 657.xz_s with sub-workloads, strip the suffix
            base_name = name
            for ref_name in SPEC17_INTSPEED_REF_TIMES:
                if name.startswith(ref_name):
                    base_name = ref_name
                    break
            ref_time = SPEC17_INTSPEED_REF_TIMES.get(base_name, 0.0)
            score = ref_time / real_time if real_time > 0 and ref_time > 0 else 0.0

            return {
                "RealTime": real_time,
                "UserTime": user_time,
                "KernelTime": kernel_time,
                "score": score,
            }
    except Exception as e:
        logger.warning(f"Failed to parse SPEC CSV for {benchmark_name}: {e}")
    return None


def extract_spec_scores(rootfs_outputs: dict[str, str],
                        workload_name: str) -> dict[str, float] | None:
    """Extract SPEC scores from rootfs output files for a workload.

    Looks for ``<workload_name>.csv`` in the output files (matching either the
    exact relative path or any path ending in ``/<workload_name>.csv``) and
    delegates parsing to :func:`parse_spec_csv`.

    Args:
        rootfs_outputs: Dict of relative_path -> content from one sim slot.
        workload_name: The benchmark name (e.g., "600.perlbench_s").

    Returns:
        Dict with ``RealTime``, ``UserTime``, ``KernelTime``, and ``score``
        (all floats) as returned by :func:`parse_spec_csv`, or ``None`` if the
        workload's CSV is not present or fails to parse.
    """
    csv_filename = f"{workload_name}.csv"
    csv_content = None

    for relpath, content in rootfs_outputs.items():
        if relpath == csv_filename or relpath.endswith(f"/{csv_filename}"):
            csv_content = content
            break

    if csv_content is None:
        return None

    return parse_spec_csv(csv_content, workload_name)


def compute_aggregate_score(per_workload_scores: dict[str, dict[str, float]]) -> float:
    """Compute the geometric mean of individual benchmark scores (SPECspeed composite).

    Only workloads with a positive ``score`` contribute to the mean.

    Args:
        per_workload_scores: Mapping of workload name to its score dict (each
            containing at least a ``score`` key, e.g. the output of
            :func:`extract_spec_scores`).

    Returns:
        The geometric mean of the positive per-workload scores as a float, or
        ``0.0`` if no workload has a positive score.
    """
    scores = [s["score"] for s in per_workload_scores.values() if s.get("score", 0) > 0]
    if not scores:
        return 0.0
    return math.exp(sum(math.log(s) for s in scores) / len(scores))
