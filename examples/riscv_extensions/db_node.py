"""The `database` ray node: owns the durable VEXT log store.

Every remote is pinned to the `database` resource so Ray schedules it on the
one host with local disk for the store (the `database` host). Drivers tar
a directory on their side, ship the bytes through the Ray object store, and let
this node extract on local disk — no SSH, no rsync (field guide §11).

Layout produced under VEXT_DB_ROOT:

    <ext>/sweep_<N>/
    ├── summary.md                 # human-readable result table (write_text)
    ├── src/                       # snapshot of the loop code + prompts + specs
    ├── profiler/                  # chia profiler JSONL
    └── <ext>-<run_tag>/           # one per pipeline (archived work_root)
        ├── implementations/       #   per-iteration BOOM diffs
        ├── llm_logs/              #   claude session transcripts
        ├── sim_logs/              #   per-iteration DUT test logs
        ├── checklist.md  knowledge.md
        └── ...
"""

import io
import os
import re
import shutil
import sys
import tarfile

# chia on sys.path so `from chia...` resolves under ray's working_dir.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, _REPO_ROOT)

from chia.base.ChiaFunction import ChiaFunction
from chia.trace.profiler import get_profiler

# Overridable so a deployment can point elsewhere; the `database` node must have
# local write access here.
DB_ROOT = os.environ.get("VEXT_DB_ROOT", "/scratch/vext-db")


@ChiaFunction(resources={"database": 0.9})
def claim_sweep(extension: str) -> tuple[int, str]:
    """Allocate the next free sweep_<N> under DB_ROOT/<extension>/ and return
    (N, abs_path). Atomic via mkdir: racing callers retry the next integer."""
    if extension: get_profiler().add_info({"extension": extension})
    ext_root = os.path.join(DB_ROOT, extension)
    os.makedirs(ext_root, exist_ok=True)
    while True:
        used = [int(m.group(1)) for d in os.listdir(ext_root)
                if (m := re.match(r"sweep_(\d+)$", d))]
        n = max(used, default=0) + 1
        path = os.path.join(ext_root, f"sweep_{n}")
        try:
            os.mkdir(path)
            return n, path
        except FileExistsError:
            continue


@ChiaFunction(resources={"database": 0.9})
def archive_dir(sweep_path: str, name: str, tarball: bytes, extension: str = "") -> None:
    """Extract a tar blob into sweep_path/name/ (per-pipeline work_root,
    profiler dir, or src snapshot)."""
    if extension: get_profiler().add_info({"extension": extension})
    dst = os.path.join(sweep_path, name)
    os.makedirs(dst, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(tarball), mode="r") as t:
        t.extractall(path=dst)


@ChiaFunction(resources={"database": 0.9})
def write_text(sweep_path: str, filename: str, content: str, extension: str = "") -> None:
    """Write a text file (summary.md, etc.) at sweep_path/filename."""
    if extension: get_profiler().add_info({"extension": extension})
    with open(os.path.join(sweep_path, filename), "w") as f:
        f.write(content)


@ChiaFunction(resources={"database": 0.9})
def put(dest_dir: str, rel_path: str, content, extension: str = "") -> None:
    """Durably write ONE artifact at dest_dir/rel_path the moment the loop
    produces it (incremental archival — so a mid-run kill keeps everything
    logged so far, not just the end-of-run tar). `content` is str or bytes;
    parent dirs are created. Used by single_loop._mirror."""
    if extension: get_profiler().add_info({"extension": extension})
    path = os.path.join(dest_dir, rel_path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    mode = "wb" if isinstance(content, (bytes, bytearray)) else "w"
    with open(path, mode) as f:
        f.write(content)


# TODO(vext): ELFs under DB_ROOT/tests/<set>/ are staged by hand from the
# chipyard image's prebuilt riscv-tests (constants.RISCV_TESTS_ISA_DIR). The
# principled producer is a test-suite-builder node compiling from riscv-tests
# source for any extension; fetch_tests stays the read path either way.

@ChiaFunction(resources={"database": 0.9})
def fetch_tests(extension: str) -> list[tuple[str, bytes]]:
    """Return [(name, bytes)] for every staged test ELF of `extension`, read
    from DB_ROOT/tests/<extension>/ (empty if the dir is missing). The bytes
    are shipped to the Verilator (DUT) and Spike runners."""
    if extension: get_profiler().add_info({"extension": extension})
    d = os.path.join(DB_ROOT, "tests", extension)
    if not os.path.isdir(d):
        return []
    out: list[tuple[str, bytes]] = []
    for name in sorted(os.listdir(d)):
        p = os.path.join(d, name)
        if os.path.isfile(p):
            with open(p, "rb") as f:
                out.append((name, f.read()))
    return out


# ---------------------------------------------------------------------------
# Random-test pool (S3): generators register tests from run start; the soak
# streams them to cosims and marks pending -> passed. A divergence resets every
# status so the whole batch (failing test included) re-runs after the fix.
# The pool is SCRATCH, living under DB_ROOT/tmp/<run_id>; pool_finalize folds
# the .S record into the sweep and deletes it before the run's logging ends.
# ---------------------------------------------------------------------------

def pool_path(run_id: str) -> str:
    """Where a run's scratch pool lives (plain helper, callable anywhere)."""
    return os.path.join(DB_ROOT, "tmp", run_id)


@ChiaFunction(resources={"database": 0.9})
def archive_asm(pool_dir: str, dest_dir: str, extension: str = "") -> int:
    """Pack every generated .S into one gzip tarball (dest_dir/asm.tar.gz) the
    moment generation finishes. Idempotent — skips if already written; returns
    the count, or -1 if there is nothing to do. The .elf binaries are scratch
    and never archived; programs reproduce from the assembly."""
    if extension: get_profiler().add_info({"extension": extension})
    out = os.path.join(dest_dir, "asm.tar.gz")
    if os.path.exists(out) or not os.path.isdir(pool_dir):
        return -1
    srcs = sorted(f for f in os.listdir(pool_dir) if f.endswith(".S"))
    if not srcs:
        return -1
    os.makedirs(dest_dir, exist_ok=True)
    with tarfile.open(out, "w:gz") as t:
        for fn in srcs:
            t.add(os.path.join(pool_dir, fn), arcname=fn)
    return len(srcs)


@ChiaFunction(resources={"database": 0.9})
def pool_finalize(pool_dir: str, extension: str = "") -> None:
    """Delete the scratch pool. The .S are archived at generation completion
    (archive_asm); the .elf binaries are scratch and dropped."""
    if extension: get_profiler().add_info({"extension": extension})
    shutil.rmtree(pool_dir, ignore_errors=True)


@ChiaFunction(resources={"database": 0.9})
def pool_add(pool_dir: str, name: str, elf: bytes, asm: str, instr: int, extension: str = "") -> None:
    """Register one generated test: binary + source + instr count + 'pending'."""
    if extension: get_profiler().add_info({"extension": extension})
    os.makedirs(pool_dir, exist_ok=True)
    stem = os.path.join(pool_dir, name)
    with open(stem + ".elf", "wb") as f:
        f.write(elf)
    with open(stem + ".S", "w") as f:
        f.write(asm)
    with open(stem + ".instr", "w") as f:
        f.write(str(instr))
    with open(stem + ".status", "w") as f:
        f.write("pending")


@ChiaFunction(resources={"database": 0.9})
def pool_state(pool_dir: str, extension: str = "") -> dict[str, tuple[str, int]]:
    """name -> (status, instr count) for every pooled test (status: pending |
    passed; instr sizes the cosim's cycle budget)."""
    if extension: get_profiler().add_info({"extension": extension})
    if not os.path.isdir(pool_dir):
        return {}
    out: dict[str, tuple[str, int]] = {}
    for fn in os.listdir(pool_dir):
        if fn.endswith(".status"):
            stem = os.path.join(pool_dir, fn[: -len(".status")])
            with open(stem + ".status") as f:
                status = f.read().strip()
            with open(stem + ".instr") as f:
                out[os.path.basename(stem)] = (status, int(f.read().strip()))
    return out


@ChiaFunction(resources={"database": 0.9})
def pool_elf(pool_dir: str, name: str, extension: str = "") -> bytes:
    if extension: get_profiler().add_info({"extension": extension})
    with open(os.path.join(pool_dir, name + ".elf"), "rb") as f:
        return f.read()


@ChiaFunction(resources={"database": 0.9})
def pool_mark(pool_dir: str, name: str, status: str, extension: str = "") -> None:
    if extension: get_profiler().add_info({"extension": extension})
    with open(os.path.join(pool_dir, name + ".status"), "w") as f:
        f.write(status)


@ChiaFunction(resources={"database": 0.9})
def pool_reset(pool_dir: str, extension: str = "") -> int:
    """Every test back to 'pending'; returns how many."""
    if extension: get_profiler().add_info({"extension": extension})
    n = 0
    if os.path.isdir(pool_dir):
        for fn in os.listdir(pool_dir):
            if fn.endswith(".status"):
                with open(os.path.join(pool_dir, fn), "w") as f:
                    f.write("pending")
                n += 1
    return n
