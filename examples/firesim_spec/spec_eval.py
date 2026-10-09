"""Build a design's bitstream and a SPEC workload, run each benchmark on its own F2
FPGA, and score the run as SPEC does.

    result = get(spec_eval.chia_remote(aws, "spec17-intspeed-test", recipe, run_config))
    result.score, result.ratios, result.jobs

A given ``bitstream`` or ``workload`` (from an earlier call or S3) is not built.
The cluster needs the workers of examples/spec_build/spec_sw_build_loop.py.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass, replace
from pathlib import Path

from ray import cloudpickle

from chia.aws.manager import AWSWorker
from chia.aws.s3 import S3Node
from chia.base.ChiaFunction import ChiaFunction, get
from chia.chipyard.state_def import FireMarshalArtifact
from chia.firesim.bitstream_build_node import BitstreamBuildNode
from chia.firesim.fs_bitstream import FSBitstream
from chia.firesim.manager_node import FireSimManagerNode
from chia.firesim.specs import F2_SIM, F2_VIVADO
from chia.firesim.state_def import BuildRecipe, RunConfig, SimJobResult

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "spec_build"))
import spec_sw_build_loop as spec_build  # noqa: E402

# Workers cannot import this example module, so its functions travel by value.
cloudpickle.register_pickle_by_value(sys.modules[__name__])

# A benchmark in the names of speckle's timing rows and files (657.xz_s_0 -> 657.xz_s).
_BENCHMARK = re.compile(r"\d{3}\.[A-Za-z0-9]+(_[rs])?")


@dataclass
class SpecEvalResult:
    """Per-benchmark ``seconds`` and SPEC ``ratios``; ``score`` is their geometric
    mean, or None if a benchmark has no ratio."""
    bitstream: FSBitstream
    workload: FireMarshalArtifact
    jobs: list[SimJobResult]
    seconds: dict[str, float]
    ratios: dict[str, float]
    score: float | None


# On the head, with no CPU (it only waits) and no retry (a retry rebuilds everything).
@ChiaFunction(num_cpus=0, max_retries=0, resources={"node:__internal_head__": 0.001})
def spec_eval(aws, spec: str, recipe: BuildRecipe, run_config: RunConfig | None = None,
              diffs: "list[str] | None" = None, bitstream: FSBitstream | None = None,
              workload: FireMarshalArtifact | None = None, spec_flags: str = "",
              cores: int = 1, upload_to: str | None = None,
              max_fpgas: int = 12, small_images: bool = False,
              build_worker: AWSWorker | None = F2_VIVADO,
              build_resource: str = "F2_VIVADO") -> SpecEvalResult:
    """Run ``spec`` on ``recipe`` with ``diffs``, and score it. The SPEC build and the
    bitstream build run at the same time.

    Args:
        aws: The AWS manager that launches the F2 machines.
        spec: e.g. ``"spec17-intspeed-test"``.
        recipe, diffs: The design, and the chipyard changes to build it with (diffs
            from the chipyard root, applied in order).
        run_config: FireSim runtime settings.
        bitstream, workload: Earlier results, so that they are not built.
        spec_flags, cores, small_images: As in ``spec_sw_build_loop.start_workload``.
        upload_to: ``s3://bucket/prefix`` for the bitstream and workload that it builds.
        max_fpgas: F2 machines at most.
        build_worker: The machine to launch for the bitstream build. ``None`` launches
            none: the build runs on a machine that the cluster already has.
        build_resource: The resource that the bitstream build asks for, for example
            ``"AWS_VIVADO"`` with ``build_worker=AWS_VIVADO``. A cluster can name its own,
            for example for a machine that builds every platform.

    Raises:
        ValueError: ``build_worker`` does not have ``build_resource``.
        RuntimeError: A build failed.
    """
    if bitstream is None and build_worker and build_resource not in build_worker[0].resources:
        raise ValueError(f"{build_worker[0].name} has no resource {build_resource!r}, "
                         f"so the bitstream build would never run")
    jobs = spec_build.jobs(spec, cores)
    workload_ref = None if workload else spec_build.start_workload(spec, cores, spec_flags,
                                                                   small_images, upload_to or "")
    built_bitstream = False
    if bitstream is None:
        ecad = get(aws.launch.chia_remote(build_worker, count=1)) if build_worker else None
        try:
            builder = BitstreamBuildNode()
            build_ref = builder.build_bitstream.options(resources={build_resource: 1}).chia_remote(
                builder, recipe=recipe, diffs=diffs)
            if workload_ref:
                workload = _checked(get(workload_ref))
            build = get(build_ref)
        finally:
            if ecad:
                ecad.teardown()
        if not build.success:
            raise RuntimeError(f"bitstream build failed:\n{build.log[-4000:]}")
        bitstream, built_bitstream = build.bitstream, True
    elif workload_ref:
        workload = _checked(get(workload_ref))

    stored_bitstream = bitstream
    if upload_to and built_bitstream:   # the workload is in S3 already
        bucket, prefix = upload_to.removeprefix("s3://").split("/", 1)
        # F2 names its bitstream with an AGFI; the other platforms have only the bytes.
        name = bitstream.agfi or hashlib.sha256(bitstream.bitstream_bytes).hexdigest()[:16]
        stored_bitstream = get(bitstream.publish.chia_remote(bitstream, bucket, f"{prefix}/{name}"))

    # F2 machines have no AWS credentials, so a driver in S3 travels by value.
    if bitstream.driver_uri and not bitstream.driver_bytes:
        bucket, key = bitstream.driver_uri.removeprefix("s3://").split("/", 1)
        bitstream = replace(bitstream, driver_uri=None, driver_bytes=S3Node(bucket).get_bytes(key))
    farm = get(aws.launch.chia_remote(F2_SIM, count=min(max_fpgas, len(jobs))))
    try:
        results = FireSimManagerNode().run_workload(farm, workload, bitstream, run_config)
    finally:
        farm.teardown()        # also when run_workload fails
    return SpecEvalResult(stored_bitstream, workload, results, *_score(results))


def _checked(workload: FireMarshalArtifact) -> FireMarshalArtifact:
    if not workload.success:
        raise RuntimeError(f"SPEC workload failed:\n{workload.stderr[-4000:]}")
    return workload


def _score(results: list[SimJobResult]) -> tuple[dict[str, float], dict[str, float], float | None]:
    """Seconds, ratios and score from the timing .csv files and reftimes.json in /output."""
    reftimes: dict[str, float] = {}
    times: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    failed = set()
    for result in results:
        for path, data in result.outputs.items():
            folder, _, file = path.rpartition("/")
            if not folder.endswith("/output") or not (file == "reftimes.json" or _BENCHMARK.match(file)):
                continue
            if file == "reftimes.json":
                reftimes.update(json.loads(data))
            elif file.endswith(".csv"):
                text = data.decode(errors="replace")
                if "Command exited with non-zero status" in text or "Command terminated by signal" in text:
                    failed.add(_BENCHMARK.match(file)[0])
                for row in csv.DictReader(io.StringIO(text)):
                    if row.get("RealTime") and row["name"] != "name":
                        times[_BENCHMARK.match(row["name"])[0]][row.get("copy") or "0"] += float(row["RealTime"])
    seconds = {b: max(t.values()) for b, t in times.items() if b not in failed}
    ratios = {b: len(times[b]) * reftimes[b] / s for b, s in seconds.items() if b in reftimes}
    score = statistics.geometric_mean(ratios.values()) if reftimes and ratios.keys() == reftimes.keys() else None
    return seconds, ratios, score
