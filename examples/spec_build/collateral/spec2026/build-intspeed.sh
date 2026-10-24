#!/bin/bash
# SPEC CPU2026 intspeed build with speckle (pinned), against $SPEC_DIR.
set -ex
if [ "$1" != "ref" ] && [ "$1" != "test" ] && [ "$1" != "train" ]; then
    echo "Must specify ref/test/train"
    exit 1
fi

SPECKLE_REPO="${SPECKLE_REPO:-https://github.com/ucb-bar/Speckle.git}"
# ucb-bar/Speckle 2026, with the vpr input fix (branch 2026-fix-vpr-xz).
SPECKLE_COMMIT="${SPECKLE_COMMIT:-8aeccf81fe702e961fc788bad5919568139a5ff2}"

if [ ! -d speckle ]; then
    git config --global url."https://github.com/".insteadOf "git@github.com:"
    git clone "$SPECKLE_REPO" speckle
    git -C speckle checkout "$SPECKLE_COMMIT"
    git -C speckle submodule update --init --recursive
fi

# Add SPEC_FLAGS to the RISC-V compilers in speckle's config.
[ -z "$SPEC_FLAGS" ] || sed -i -E "s#^([[:space:]]*(CC|CXX|FC)[[:space:]]*=.*)\$#\1 $SPEC_FLAGS#" speckle/riscv.cfg

echo "Building SPEC2026 Intspeed with $1 inputs"
# Threads in the benchmark commands: match the target's cores.
cd speckle && ./gen_binaries.sh --compile --suite intspeed --input "$1" --threads "${THREADS:-4}"
