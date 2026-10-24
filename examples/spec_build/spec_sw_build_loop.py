"""Build a SPEC CPU FireSim workload: compile SPEC and a FireMarshal base in
parallel, then compose SPEC into the base rootfs, one image per job.

    build_program (SPEC via speckle) ─┐
    build_base    (br-base)          ─┴─> compose(overlay=SPEC, base=br-base,
                                              jobs=one per run)
                                          └─> workload (FireMarshalArtifact)

``spec`` is ``spec06-int-<size>``, or ``spec17-``/``spec26-`` with ``intspeed-<size>``
or ``intrate-<size>``; the size is ``test``, ``train`` or ``ref``. collateral/spec<year>
holds the build scripts and the job list (marshal-configs) of each SPEC version.

The SPEC files go from the build to the compose through Ray's memory, so they must
fit in it: a SPEC 2026 ref build is 39 GB.

The cluster (cluster_sw.yaml): a ``riscv_build`` worker that mounts each SPEC install
and sets SPEC_DIR_2006/2017/2026, and a ``firemarshal`` worker (with ``aws_creds``
for an upload).

Run (after `chia up cluster_sw.yaml -y`):
    chia job submit --working-dir . -- python spec_sw_build_loop.py spec06-int-test
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import shlex
import sys
from pathlib import Path

import ray
from ray import cloudpickle

from chia.base.ChiaFunction import ChiaFunction, get
from chia.chipyard.firemarshal_node import FireMarshalNode
from chia.chipyard.riscv_build_node import RiscvBuildNode
from chia.chipyard.state_def import FireMarshalArtifact, ProgramBuildArtifact

# Workers cannot import this example module, so its functions travel by value.
cloudpickle.register_pickle_by_value(sys.modules[__name__])

COLLATERAL = Path(__file__).resolve().parent / "collateral"
_FIREMARSHAL = {"resources": {"firemarshal": 1}}


def start_workload(spec: str, cores: int = 1, spec_flags: str = "", small_images: bool = False,
                   upload_to: str = "") -> ray.ObjectRef:
    """Start the SPEC build, the FireMarshal base and the compose of ``spec``; return
    the ref of the workload (a FireMarshalArtifact).

    Args:
        spec: e.g. ``"spec17-intspeed-test"``.
        cores: Threads of a speed run, copies of a rate run.
        spec_flags: Flags added to each RISC-V compile and link, e.g. ``"-march=rv64gc_zba"``.
        small_images: Each job's image holds only its own benchmark (for spec26).
        upload_to: ``s3://bucket/prefix``: upload the workload and return its
            ``archive_uri``, not its bytes.
    """
    year, suite, size = _parse(spec)
    files = {p.name: p.read_bytes() for p in (COLLATERAL / f"spec{year}").iterdir() if p.is_file()}
    files["reftimes.py"] = (COLLATERAL / "reftimes.py").read_bytes()
    workload_jobs = jobs(spec, cores)
    # FireMarshalNode reuses a stored image by name, so the name changes with the inputs.
    digest = hashlib.sha256(json.dumps([spec, cores, spec_flags, small_images, workload_jobs]).encode())
    for f in sorted(files):
        digest.update(f.encode() + files[f])
    name = f"{spec}-{digest.hexdigest()[:8]}"

    refsize = f"ref{suite[3:]}" if year != "2006" and size == "ref" else size   # refspeed/refrate
    overlay = f"speckle/build/overlay/{suite}/{size}"
    command = (f'export SPEC_DIR="$SPEC_DIR_{year}" THREADS={cores} SPEC_FLAGS={shlex.quote(spec_flags)} && '
               # SPEC builds in its install: one build at a time, then remove its build folders.
               f'exec 9<"$SPEC_DIR" && flock 9 && '
               f'bash {"build-cint.sh" if year == "2006" else f"build-{suite}.sh"} {size} && '
               f'python3 reftimes.py "$SPEC_DIR" {refsize} '
               f'{" ".join(sorted({_benchmark(job) for job in workload_jobs}))} > {overlay}/reftimes.json && '
               f'rm -rf "$SPEC_DIR"/benchspec/CPU*/*/build "$SPEC_DIR"/benchspec/CPU*/*/run '
               f'"$SPEC_DIR"/benchspec/CPU*/*/exe && '
               f'mkdir root && mv speckle/build/overlay root/spec')     # the files at their rootfs paths
    rv = RiscvBuildNode(timeout_seconds=8 * 60 * 60)
    spec_ref = rv.build_program.chia_remote(rv, input_files=files, command=["bash", "-c", command],
                                            work_dir="/tmp/spec_build", outputs=["root"])
    fm = FireMarshalNode(timeout_seconds=4 * 60 * 60)
    base_ref = fm.build_base.options(**_FIREMARSHAL).chia_remote(fm, name="br-base")
    # aws_creds: the compose uploads the workload.
    compose = _compose.options(resources={"firemarshal": 1, "aws_creds": 1}) if upload_to else _compose
    return compose.chia_remote(name, spec_ref, base_ref, workload_jobs, small_images, upload_to)


def jobs(spec: str, cores: int = 1) -> list[dict]:
    """The FireMarshal jobs of ``spec``, with ``cores`` threads or copies. Each job
    also copies the reference times to /output."""
    year, suite, size = _parse(spec)
    config = COLLATERAL / f"spec{year}" / "marshal-configs" / f"{spec.rsplit('-', 1)[0]}.json"
    root = f"/root/spec/{suite}/{size}"
    return [{"name": job["name"], "outputs": ["/output"],
             "command": f"mkdir -p /output && cp {root}/reftimes.json /output/ && cd {root} && "
                        + re.sub(r"--(threads|copies) \d+", rf"--\1 {cores}", job["command"])}
            for job in json.loads(config.read_text())["jobs"]]


@ChiaFunction(**_FIREMARSHAL)
def _compose(name: str, spec: ProgramBuildArtifact, base, jobs: list[dict], small_images: bool,
             upload_to: str) -> FireMarshalArtifact:
    """Compose the SPEC files onto br-base, one image per job, and upload the workload
    for ``upload_to``. With ``small_images``, a job's image holds only its benchmark's
    folder and the files of no benchmark."""
    for step, artifact in (("SPEC build", spec), ("FireMarshal base", base)):
        if not artifact.success:
            return FireMarshalArtifact(success=False, stderr=f"{step} failed:\n{artifact.stderr[-4000:]}")
    if small_images:
        benchmarks = {_benchmark(job) for job in jobs}

        def owner(path: str) -> str | None:      # root/spec/<suite>/<size>/<benchmark>/...
            parts = path.split("/")
            return parts[4] if len(parts) > 5 and parts[4] in benchmarks else None

        jobs = [{**job, "files": [p for p in spec.files if owner(p) in (None, _benchmark(job))]}
                for job in jobs]
    largest = max(sum(len(spec.files[p]) for p in job.get("files", spec.files)) for job in jobs)
    workload = FireMarshalNode(timeout_seconds=4 * 60 * 60).compose(
        base_name="br-base", name=name, overlay_files=spec.files, overlay_modes=spec.modes,
        config={"jobs": jobs}, rootfs_size_mib=1024 * (math.ceil(2 * largest / 2**30) + 1))
    if upload_to and workload.success:
        workload = workload.publish(*upload_to.removeprefix("s3://").split("/", 1))
    return workload


def _benchmark(job: dict) -> str:
    """The benchmark that a job runs: the argument of its run script."""
    return re.search(r"\.sh (\S+)", job["command"])[1]


def _parse(spec: str) -> tuple[str, str, str]:
    """``"spec17-intspeed-test"`` -> ``("2017", "intspeed", "test")``; 2006's suite is
    ``cint2006``, speckle's name for it."""
    m = re.fullmatch(r"spec(06|17|26)-(int|intspeed|intrate)-(test|train|ref)", spec)
    if not m or (m[1] == "06") != (m[2] == "int"):
        raise ValueError(f"not a SPEC name: {spec!r}")
    year = f"20{m[1]}"
    return year, "cint2006" if year == "2006" else m[2], m[3]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("spec", help="SPEC suite and input size, e.g. spec06-int-test or "
                                     "spec17-intspeed-train")
    args = parser.parse_args()

    ray.init(address="auto")
    wl = get(start_workload(args.spec))
    if not wl.success:
        print("workload failed\n" + wl.stderr[-2000:]); return 1
    print(f"OK: workload {len(wl.archive)} bytes, descriptor {wl.json_name}, {len(jobs(args.spec))} jobs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
