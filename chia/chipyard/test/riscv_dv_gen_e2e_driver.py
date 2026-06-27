"""Standalone validation of the riscv-dv generator node. Two cases, run in turn:

  base      pyflow (free)               -> riscv_arithmetic_basic_test, rv64 base
  bitmanip  Xcelium SV (--simulator xlm) -> riscv_b_ext_test, rv64 bitmanip

Each case requests the Ray resources it needs: `base` needs `dv` (the generator
role); `bitmanip` needs `dv` + an `xcelium` license seat. Ray caps concurrency to
the seats the cluster declares — Ray is the floating-license throttle. Both run on
riscv_dv_gen_cluster (it exposes both resources):

    ray job submit --address IP:6379 --working-dir . -- \
      python chia/chipyard/test/riscv_dv_gen_e2e_driver.py

Env knobs:
    RVDV_CASE   run only this case (default: run all); base | bitmanip
    RVDV_NUM    iterations (default 1 — keep small)
    RVDV_OUT    where ELFs are saved on the head (default /tmp/riscv_dv_gen)
"""
from __future__ import annotations

import os
import sys

import ray

# A case = generator backend + the Ray resources it needs + the riscv-dv
# test/target/isa.
# NOTE (validate on first SV run): confirm riscv-dv's SV bitmanip is ratified
# Zba/Zbb/Zbc/Zbs (not draft) — if gcc_compile rejects ops, `isa`/the test's
# group args need adjusting.
CASES = {
    "base": dict(
        sim="pyflow", resources={"dv": 1},
        test="riscv_arithmetic_basic_test", target="rv64imafdc",
        isa="rv64gc", mabi="lp64d",
    ),
    "bitmanip": dict(
        sim="xlm", resources={"dv": 1, "xcelium": 1},
        test="riscv_b_ext_test", target="rv64imcb",
        isa="rv64gc_zba_zbb_zbc_zbs", mabi="lp64d",
    ),
}

CASE = os.environ.get("RVDV_CASE", "")  # empty -> run all
NUM = int(os.environ.get("RVDV_NUM", "1"))
OUT = os.environ.get("RVDV_OUT", "/tmp/riscv_dv_gen")


def _generate(case: dict, num: int):
    from chia.base.ChiaFunction import get
    from chia.chipyard.riscv_dv_gen_node import GenSpec, RiscvDvGenNode
    node = RiscvDvGenNode(
        isa=case["isa"], mabi=case["mabi"],
        target=case["target"], simulator=case["sim"],
    )
    return get(node.generate.options(resources=case["resources"], max_retries=0).chia_remote(
        node, GenSpec(test=case["test"], iterations=num), "/tmp/rvdv_work"))


def _run_case(name: str, available: dict) -> bool | None:
    """Run one case; return True/False on ELFs produced, or None if skipped
    (cluster lacks a required resource — avoids blocking on a pending task)."""
    case = CASES[name]
    missing = [r for r in case["resources"] if available.get(r, 0) < 1]
    if missing:
        print(f"[rvdv] SKIP {name}: cluster lacks resource(s) {missing}")
        return None
    print(f"[rvdv] {name}: sim={case['sim']} test={case['test']} "
          f"target={case['target']} x{NUM}, needs {case['resources']}")
    elfs = _generate(case, NUM)
    out_dir = os.path.join(OUT, name)
    os.makedirs(out_dir, exist_ok=True)
    for fn, data, asm in elfs:
        with open(os.path.join(out_dir, fn), "wb") as f:
            f.write(data)
        with open(os.path.join(out_dir, os.path.splitext(fn)[0] + ".S"), "w") as f:
            f.write(asm)
    summary = ", ".join(f"{fn} ({len(d)}B)" for fn, d, _ in elfs) or "none"
    print(f"[rvdv]   -> {len(elfs)} ELF(s) in {out_dir}: {summary}")
    return bool(elfs)


def main() -> int:
    if CASE and CASE not in CASES:
        print(f"RVDV_CASE must be one of {list(CASES)} (got {CASE!r})", file=sys.stderr)
        return 2

    chia_root = os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))))
    if chia_root not in sys.path:
        sys.path.insert(0, chia_root)
    ray.init(address="auto")  # job ships working_dir; no runtime_env here

    available = ray.cluster_resources()
    results = {n: _run_case(n, available) for n in ([CASE] if CASE else CASES)}

    print("\n[rvdv] === SUMMARY ===")
    for name, ok in results.items():
        print(f"  {name}: {'PASS' if ok else 'SKIP' if ok is None else 'FAIL'}")
    ran = [ok for ok in results.values() if ok is not None]
    return 0 if ran and all(ran) else 1


if __name__ == "__main__":
    sys.exit(main())
