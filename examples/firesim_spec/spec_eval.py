"""Evaluate a design on SPEC CPU: build its bitstream and a SPEC workload, run each
benchmark on its own F2 FPGA, and score the run times as SPEC does.

    from spec_eval import spec_eval, spec_workload       # from this folder

    result = spec_eval(aws, "spec17-intspeed-test", recipe, run_config)
    result.score, result.ratios, result.jobs
    spec_workload("spec26-intrate-ref", upload_to="s3://bucket/prefix")

``spec`` names the suite and the input size: ``spec06-int-<size>`` (CINT2006), or
``spec17-`` or ``spec26-`` with ``intspeed-<size>`` or ``intrate-<size>``. The
size is ``test``, ``train`` or ``ref``.

The SPEC build's files and the workload go from worker to worker; the caller
gets only what it needs: the workload for a run, an S3 reference for an upload.

A step does not run when its result is given, from an earlier call or from S3
(``FireMarshalArtifact(archive_uri=...)``, ``FSBitstream(driver_uri=...)``):

    bitstream given   no bitstream build; ``recipe`` and ``diff`` are not used
    workload given    no SPEC build and no FireMarshal compose

The caller sets up the cluster for the steps that run:

    SPEC build   a ``riscv_build`` worker (chia-riscv-cross) that mounts each SPEC
                 install at its own path and sets SPEC_DIR_2006, SPEC_DIR_2017 and
                 SPEC_DIR_2026 to those paths, and a ``firemarshal`` worker
    bitstream    AWS access for ``aws`` to launch F2_ECAD
    run          AWS access for ``aws`` to launch F2_SIM
    S3           a worker with the ``aws_creds`` resource for ``upload_to``, and
                 AWS credentials in the calling process for s3:// inputs
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import re
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass, replace
from pathlib import Path

from ray import cloudpickle

import chia.firesim
from chia.aws.s3 import S3Node
from chia.base.ChiaFunction import ChiaFunction, get
from chia.chipyard.firemarshal_node import FireMarshalNode
from chia.chipyard.riscv_build_node import RiscvBuildNode
from chia.chipyard.state_def import FireMarshalArtifact, ProgramBuildArtifact
from chia.firesim.bitstream_build_node import BitstreamBuildNode
from chia.firesim.fs_bitstream import FSBitstream
from chia.firesim.manager_node import FireSimManagerNode
from chia.firesim.specs import F2_ECAD, F2_SIM
from chia.firesim.state_def import BuildRecipe, RunConfig, SimJobResult

# Workers cannot import this example module, so its functions travel by value.
cloudpickle.register_pickle_by_value(sys.modules[__name__])

# Speckle build scripts per SPEC version, and ucb-bar's job lists for 2017 and 2026
# (the marshal configs of ucb-bar/spec2017-workload and spec2026-workload).
SPEC_FILES = Path(chia.firesim.__path__[0]) / "spec"
CINT2006 = ["400.perlbench", "401.bzip2", "403.gcc", "429.mcf", "445.gobmk", "456.hmmer",
            "458.sjeng", "462.libquantum", "464.h264ref", "471.omnetpp", "473.astar",
            "483.xalancbmk"]
_FIREMARSHAL = {"resources": {"firemarshal": 1}}
# A benchmark in the names of speckle's timing rows and files (657.xz_s_0 -> 657.xz_s).
_BENCHMARK = re.compile(r"\d{3}\.[A-Za-z0-9]+(_[rs])?")

# {benchmark: reference seconds} of one input size of a SPEC install, as JSON. A
# speed benchmark with no data of its own uses its rate twin's (Spec/origin).
_REFTIMES = r"""
import json, sys
from pathlib import Path

spec, size, benchmarks = Path(sys.argv[1]), sys.argv[2], sys.argv[3:]
cpu = next((spec / "benchspec").glob("CPU*"))


def reftime(benchmark):
    d = cpu / benchmark
    if not (d / "data").is_dir():
        d = cpu / (d / "Spec" / "origin").read_text().split()[0]
    return float((d / "data" / size / "reftime").read_text().split()[-1])


json.dump({b: reftime(b) for b in benchmarks}, sys.stdout)
"""


@dataclass
class SpecEvalResult:
    """``jobs`` holds each job's result with all of its output files. ``seconds``
    and ``ratios`` are per benchmark, and ``score`` is the geometric mean of the
    ratios, or None if a benchmark has none. A ratio is SPEC's: reference time /
    run time for a speed run, and copies x reference time / the slowest copy's
    time for a rate run. A benchmark whose run script failed has neither."""
    bitstream: FSBitstream
    workload: FireMarshalArtifact
    jobs: list[SimJobResult]
    seconds: dict[str, float]
    ratios: dict[str, float]
    score: float | None


def spec_eval(aws, spec: str, recipe: BuildRecipe, run_config: RunConfig | None = None,
              diff: str = "", bitstream: FSBitstream | None = None,
              workload: FireMarshalArtifact | None = None, spec_cfg: str | None = None,
              cores: int = 1, upload_to: str | None = None,
              max_fpgas: int = 12) -> SpecEvalResult:
    """Run ``spec`` on the design ``recipe`` with ``diff``, and score it.

    The SPEC build and the bitstream build run at the same time; the F2 machines
    start once both are done, and each goes down when it has no job left.

    Args:
        aws: An AWSManager handle (:func:`chia.aws.manager.start_aws_manager`).
        spec: The suite and the input size, e.g. ``"spec17-intspeed-test"``.
        recipe: The design to build.
        run_config: FireSim runtime settings (``config_runtime.yaml``).
        diff: A chipyard change to build the design with.
        bitstream: A bitstream of the design, so that it is not built.
        workload: A workload of ``spec`` built for ``cores``, so that SPEC is not
            built.
        spec_cfg: The text of a SPEC config file for the RISC-V compile (compiler
            and flags), in place of speckle's.
        cores: Copies of a rate run and threads of a speed run, for 2017 and 2026
            (CINT2006 runs one copy). Match it to the design's cores.
        upload_to: ``s3://bucket/prefix`` to store the bitstream and the workload
            that this call builds; the result then refers to them there.
        max_fpgas: F2 machines at most.

    Raises:
        RuntimeError: A build failed; the message holds the end of its log.
    """
    jobs = _jobs(spec, cores)
    workload_ref = None if workload else _start_workload(spec, cores, spec_cfg)
    built_bitstream = False
    if bitstream is None:
        ecad = get(aws.launch.chia_remote(F2_ECAD, count=1))
        try:
            builder = BitstreamBuildNode()
            build_ref = builder.build_bitstream.chia_remote(builder, recipe=recipe, diff=diff)
            if workload_ref:
                workload = _checked(get(workload_ref))
            build = get(build_ref)
        finally:
            ecad.teardown()
        if not build.success:
            raise RuntimeError(f"bitstream build failed:\n{build.log[-4000:]}")
        bitstream, built_bitstream = build.bitstream, True
    elif workload_ref:
        workload = _checked(get(workload_ref))

    stored_bitstream, stored_workload = bitstream, workload
    if upload_to:
        bucket, prefix = upload_to.removeprefix("s3://").split("/", 1)
        if built_bitstream:
            stored_bitstream = get(bitstream.publish.chia_remote(
                bitstream, bucket, f"{prefix}/{bitstream.agfi}"))
        if workload_ref:
            stored_workload = get(_publish(workload_ref, upload_to))

    # F2 machines have no AWS credentials, so a driver in S3 travels by value.
    if bitstream.driver_uri and not bitstream.driver_bytes:
        bucket, key = bitstream.driver_uri.removeprefix("s3://").split("/", 1)
        bitstream = replace(bitstream, driver_uri=None, driver_bytes=S3Node(bucket).get_bytes(key))
    farm = get(aws.launch.chia_remote(F2_SIM, count=min(max_fpgas, len(jobs))))
    try:
        results = FireSimManagerNode().run_workload(farm, workload, bitstream, run_config)
    finally:
        farm.teardown()        # run_workload takes the farm down too; this covers its errors
    return SpecEvalResult(stored_bitstream, stored_workload, results, *_score(results))


def spec_workload(spec: str, cores: int = 1, spec_cfg: str | None = None,
                  upload_to: str | None = None) -> FireMarshalArtifact:
    """The SPEC workload of ``spec`` for ``cores``: speckle compiles it on a
    riscv_build worker, and FireMarshal composes it onto br-base, one job per
    run. With ``upload_to`` (``s3://bucket/prefix``) it goes to S3 from the
    workers, and the result refers to it there. ``spec_cfg`` and ``cores`` are as
    in :func:`spec_eval`.

    Raises:
        RuntimeError: A build failed; the message holds the end of its log.
    """
    ref = _start_workload(spec, cores, spec_cfg)
    return _checked(get(_publish(ref, upload_to) if upload_to else ref))


def _start_workload(spec: str, cores: int, spec_cfg: str | None):
    """Start the SPEC build, the FireMarshal base and the compose; return the
    workload's ref."""
    year, suite, size = _parse(spec)
    files = {p.name: p.read_bytes() for p in (SPEC_FILES / f"spec{year}").iterdir() if p.is_file()}
    files["reftimes.py"] = _REFTIMES.encode()
    if spec_cfg:
        files["riscv.cfg"] = spec_cfg.encode()
    benchmarks = sorted({run.split()[1] for run in _runs(year, suite, cores).values()})
    spec_ref = _build_spec(year, suite, size, cores, files, benchmarks)
    fm = FireMarshalNode(timeout_seconds=4 * 60 * 60)
    base_ref = fm.build_base.options(**_FIREMARSHAL).chia_remote(fm, name="br-base")
    jobs = _jobs(spec, cores)
    return _compose.chia_remote(_workload_name(spec, cores, files, jobs), spec_ref, base_ref, jobs)


def _publish(workload_ref, upload_to: str):
    """Upload the workload from the workers; return the ref of its S3 copy."""
    bucket, prefix = upload_to.removeprefix("s3://").split("/", 1)
    return FireMarshalArtifact.publish.chia_remote(workload_ref, bucket, prefix)


def _checked(workload: FireMarshalArtifact) -> FireMarshalArtifact:
    if not workload.success:
        raise RuntimeError(f"SPEC workload failed:\n{workload.stderr[-4000:]}")
    return workload


def _parse(spec: str) -> tuple[str, str, str]:
    """``"spec17-intspeed-test"`` -> ``("2017", "intspeed", "test")``; CINT2006's
    suite is ``cint2006``, speckle's name for it."""
    m = re.fullmatch(r"spec(06|17|26)-(int|intspeed|intrate)-(test|train|ref)", spec)
    if not m or (m[1] == "06") != (m[2] == "int"):
        raise ValueError(f"not a SPEC name: {spec!r}")
    year = f"20{m[1]}"
    return year, "cint2006" if year == "2006" else m[2], m[3]


def _runs(year: str, suite: str, cores: int) -> dict[str, str]:
    """Job name -> run command, in the folder of the suite's run scripts."""
    if year == "2006":
        return {b: f"./cint.sh {b}" for b in CINT2006}
    config = SPEC_FILES / f"spec{year}/marshal-configs/spec{year[2:]}-{suite}.json"
    return {j["name"]: re.sub(r"--(threads|copies) \d+", rf"--\1 {cores}", j["command"])
            for j in json.loads(config.read_text())["jobs"]}


def _jobs(spec: str, cores: int) -> list[dict]:
    """FireMarshal jobs, one per run; each copies the reference times to /output."""
    year, suite, size = _parse(spec)
    root = f"/root/spec/{suite}/{size}"
    return [{"name": name, "outputs": ["/output"],
             "command": f"mkdir -p /output && cp {root}/reftimes.json /output/ && cd {root} && {run}"}
            for name, run in _runs(year, suite, cores).items()]


def _build_spec(year: str, suite: str, size: str, cores: int, files: dict[str, bytes],
                benchmarks: list[str]):
    """Start the SPEC build on a riscv_build worker; return its ref. The overlay
    gets the suite's reference times as reftimes.json. SPEC builds inside its
    install, and a successful build then removes its build, run and exe folders
    there (a 2026 suite leaves about 40 GB): the overlay has what it needs."""
    script = "build-cint.sh" if year == "2006" else f"build-{suite}.sh"
    refsize = f"ref{suite[3:]}" if year != "2006" and size == "ref" else size   # refspeed/refrate
    overlay = f"speckle/build/overlay/{suite}/{size}"
    command = (f'export SPEC_DIR="$SPEC_DIR_{year}" THREADS={cores} && bash {script} {size} && '
               f'python3 reftimes.py "$SPEC_DIR" {refsize} {" ".join(benchmarks)} '
               f'> {overlay}/reftimes.json && '
               f'rm -rf "$SPEC_DIR"/benchspec/CPU*/*/build "$SPEC_DIR"/benchspec/CPU*/*/run '
               f'"$SPEC_DIR"/benchspec/CPU*/*/exe')
    rv = RiscvBuildNode(timeout_seconds=8 * 60 * 60)
    return rv.build_program.chia_remote(rv, input_files=files, command=["bash", "-c", command],
                                        work_dir="/tmp/spec_build", outputs=["speckle/build/overlay"])


def _workload_name(spec: str, cores: int, files: dict[str, bytes], jobs: list[dict]) -> str:
    """A name per set of build inputs: FireMarshal's store reuses a workload by name."""
    h = hashlib.sha256(f"{spec} {cores} {json.dumps(jobs)}".encode())
    for name in sorted(files):
        h.update(name.encode() + files[name])
    return f"{spec}-{h.hexdigest()[:8]}"


@ChiaFunction(**_FIREMARSHAL)
def _compose(name: str, spec: ProgramBuildArtifact, base, jobs: list[dict]) -> FireMarshalArtifact:
    """On the FireMarshal worker, which gets the build's files from the build
    worker: the SPEC overlay under /root/spec in br-base, one job per run. The
    rootfs holds twice the overlay plus 1 GiB, for the base and the outputs."""
    for step, artifact in (("SPEC build", spec), ("FireMarshal base", base)):
        if not artifact.success:
            return FireMarshalArtifact(success=False, stderr=f"{step} failed:\n{artifact.stderr[-4000:]}")
    rootfs = {p: f"root/spec/{p.split('overlay/', 1)[1]}" for p in spec.files if "overlay/" in p}
    gib = math.ceil(2 * sum(len(spec.files[p]) for p in rootfs) / 2**30) + 1
    return FireMarshalNode(timeout_seconds=4 * 60 * 60).compose(
        base_name="br-base", name=name, config={"jobs": jobs}, rootfs_size_mib=1024 * gib,
        overlay_files={rootfs[p]: spec.files[p] for p in rootfs},
        overlay_modes={rootfs[p]: spec.modes[p] for p in rootfs})


def _score(results: list[SimJobResult]) -> tuple[dict[str, float], dict[str, float], float | None]:
    """Seconds, ratios and score from the jobs' outputs: speckle's run scripts time
    each run (and each copy of a rate run) into a .csv in /output."""
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
