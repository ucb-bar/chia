#!/bin/bash
# SPEC CPU2017 intrate build. Clones speckle (pinned) and builds the suite
# against $SPEC_DIR.
set -ex
if [ "$1" != "ref" ] && [ "$1" != "test" ] && [ "$1" != "train" ]; then
    echo "Must specify ref/test/train"
    exit 1
fi

SPECKLE_REPO="${SPECKLE_REPO:-https://github.com/ucb-bar/Speckle.git}"
# ucb-bar/Speckle's firesim-2017: the speckle of ucb-bar/spec2017-workload plus the
# host-build fix for 502.gcc_r/602.gcc_s.
SPECKLE_COMMIT="${SPECKLE_COMMIT:-07f845d381965900b5c1c1f11db8980d1b238f20}"

if [ ! -d speckle ]; then
    git config --global url."https://github.com/".insteadOf "git@github.com:"
    git clone "$SPECKLE_REPO" speckle
    git -C speckle checkout "$SPECKLE_COMMIT"
    git -C speckle submodule update --init --recursive
fi

# A caller's SPEC config for the RISC-V compile replaces speckle's.
[ ! -f riscv.cfg ] || cp riscv.cfg speckle/riscv.cfg

echo "Building SPEC2017 Intrate with $1 inputs"
cd speckle && ./gen_binaries.sh --compile --suite intrate --input "$1"
