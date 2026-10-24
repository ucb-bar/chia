"""Print {benchmark: reference seconds} of one input size of a SPEC install, as JSON:

    python3 reftimes.py <SPEC install> <size> <benchmark>...

A reftime file holds "<size> <seconds>" or "<size> ref <seconds>" lines. CPU2017 keeps
some speed times in the files of the rate twin (Spec/origin), so the search goes there too.
"""

import json, re, sys
from pathlib import Path

spec, size, benchmarks = Path(sys.argv[1]), sys.argv[2], sys.argv[3:]
cpu = next((spec / "benchspec").glob("CPU*"))


def reftime(benchmark):
    dirs = [cpu / benchmark]
    origin = cpu / benchmark / "Spec" / "origin"
    if origin.is_file():
        dirs.append(cpu / origin.read_text().split()[0])
    for d in dirs:
        for f in [d / "data" / size / "reftime", *sorted((d / "data").glob("*/reftime"))]:
            words = f.read_text().split() if f.is_file() else []
            for i, word in enumerate(words):
                if word == size:
                    return float(next(w for w in words[i + 1:] if re.fullmatch(r"[0-9.]+", w)))
    sys.exit(f"no {size} reference time for {benchmark}")


json.dump({b: reftime(b) for b in benchmarks}, sys.stdout)
