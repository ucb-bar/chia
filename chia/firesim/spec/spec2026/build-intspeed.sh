#!/bin/bash
# SPEC CPU2026 intspeed build. Clones speckle (pinned) and builds the suite
# against $SPEC_DIR.
set -ex
if [ "$1" != "ref" ] && [ "$1" != "test" ] && [ "$1" != "train" ]; then
    echo "Must specify ref/test/train"
    exit 1
fi

SPECKLE_REPO="${SPECKLE_REPO:-https://github.com/ucb-bar/Speckle.git}"
# The speckle submodule of ucb-bar/spec2026-workload, plus the vpr input fix
# (branch 2026-fix-vpr-xz).
SPECKLE_COMMIT="${SPECKLE_COMMIT:-8aeccf81fe702e961fc788bad5919568139a5ff2}"

if [ ! -d speckle ]; then
    git config --global url."https://github.com/".insteadOf "git@github.com:"
    git clone "$SPECKLE_REPO" speckle
    git -C speckle checkout "$SPECKLE_COMMIT"
    git -C speckle submodule update --init --recursive
fi

# A caller's SPEC config for the RISC-V compile replaces speckle's.
[ ! -f riscv.cfg ] || cp riscv.cfg speckle/riscv.cfg

echo "Building SPEC2026 Intspeed with $1 inputs"
# Thread/hart count baked into the generated benchmark commands; match it
# to the target machine.
cd speckle && ./gen_binaries.sh --compile --suite intspeed --input "$1" --threads "${THREADS:-4}"
