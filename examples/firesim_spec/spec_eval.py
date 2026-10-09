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
import time
from collections import defaultdict
from dataclasses import dataclass, replace
from pathlib import Path

from ray import cloudpickle

from chia.aws.manager import AWSWorker, worker_resources
from chia.aws.s3 import S3Node
from chia.base.ChiaFunction import ChiaFunction, get
from chia.chipyard.state_def import FireMarshalArtifact
from chia.firesim.bitstream_build_node import BUILD_LOGS_NAME, BitstreamBuildNode
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
              build_resource: str | None = None,
              log_dir: str | None = None) -> SpecEvalResult:
    """Run ``spec`` on ``recipe`` with ``diffs``, and score it. The SPEC build and the
    bitstream build run at the same time.

    Args:
        aws: The AWS manager that launches the build machine and the F2 simulation machines.
        spec: The SPEC suite to build and run, for example ``"spec17-intspeed-test"``.
        recipe: The FireSim build recipe of the design that the bitstream build makes.
        run_config: FireSim runtime settings for the simulations; ``None`` keeps the
            image's FireSim settings.
        diffs: Chipyard changes, as diffs from the chipyard root, that the bitstream build
            applies in order.
        bitstream: An earlier bitstream to simulate; with it, no bitstream build runs.
        workload: An earlier SPEC workload to run; with it, no SPEC build runs.
        spec_flags: Flags added to each RISC-V compile and link of SPEC, for example
            ``"-march=rv64gc_zba"``.
        cores: The threads of a SPEC speed run, or the copies of a rate run.
        upload_to: The S3 location, ``s3://bucket/prefix``, for the workload, and for the
            bitstream that it builds with its build logs; ``None`` uploads nothing.
        max_fpgas: The most F2 machines that it launches for the simulations, one for each
            SPEC job.
        small_images: With ``True``, each job's disk image holds only its own benchmark,
            as spec26 needs.
        build_worker: The machine that it launches for the bitstream build and terminates
            after it; ``None`` uses a machine that the cluster already has.
        build_resource: The resource that the bitstream build asks for; by default,
            ``build_worker``'s resources, or the build node's ``VIVADO`` when
            ``build_worker`` is ``None``.
        log_dir: A folder on the head where it writes the bitstream build's logs and
            reports as a ``.tar.gz``, also when the build fails; ``None`` writes none.

    Raises:
        ValueError: ``build_worker`` does not have ``build_resource``.
        RuntimeError: A build failed.
    """
    if (bitstream is None and build_worker and build_resource
            and build_resource not in build_worker[0].resources):
        raise ValueError(f"{build_worker[0].name} has no resource {build_resource!r}, "
                         f"so the bitstream build would never run")
    # None keeps the build node's default resource (VIVADO).
    resources = ({build_resource: 1} if build_resource
                 else worker_resources(build_worker) if build_worker else None)
    jobs = spec_build.jobs(spec, cores)
    workload_ref = None if workload else spec_build.start_workload(spec, cores, spec_flags,
                                                                   small_images, upload_to or "")
    built_bitstream = False
    if bitstream is None:
        ecad = get(aws.launch.chia_remote(build_worker, count=1)) if build_worker else None
        try:
            builder = BitstreamBuildNode()
            build_ref = builder.build_bitstream.options(resources=resources).chia_remote(
                builder, recipe=recipe, diffs=diffs)
            if workload_ref:
                workload = _checked(get(workload_ref))
            build = get(build_ref)
        finally:
            if ecad:
                ecad.teardown()
        if log_dir:
            logs = Path(log_dir) / f"{recipe.name}-{time.strftime('%Y%m%d-%H%M%S')}-{BUILD_LOGS_NAME}"
            logs.parent.mkdir(parents=True, exist_ok=True)
            logs.write_bytes(build.logs)
        if not build.success:
            raise RuntimeError(f"bitstream build failed:\n{build.log[-4000:]}"
                               + (f"\nAll its logs: {logs}" if log_dir else ""))
        bitstream, built_bitstream = build.bitstream, True
    elif workload_ref:
        workload = _checked(get(workload_ref))

    stored_bitstream = bitstream
    if upload_to and built_bitstream:   # the workload is in S3 already
        bucket, prefix = upload_to.removeprefix("s3://").split("/", 1)
        # F2 names its bitstream with an AGFI; the other platforms have only the bytes.
        name = bitstream.agfi or hashlib.sha256(bitstream.bitstream_bytes).hexdigest()[:16]
        stored_bitstream = get(bitstream.publish.chia_remote(bitstream, bucket, f"{prefix}/{name}"))
        S3Node(bucket).put_bytes(f"{prefix}/{name}/{BUILD_LOGS_NAME}", build.logs)

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
