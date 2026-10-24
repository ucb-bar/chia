#!/bin/bash
# SPEC CPU2017 intspeed build with speckle (pinned), against $SPEC_DIR.
set -ex
if [ "$1" != "ref" ] && [ "$1" != "test" ] && [ "$1" != "train" ]; then
    echo "Must specify ref/test/train"
    exit 1
fi

SPECKLE_REPO="${SPECKLE_REPO:-https://github.com/ucb-bar/Speckle.git}"
# ucb-bar/Speckle firesim-2017, with the 502.gcc_r/602.gcc_s host-build fix.
SPECKLE_COMMIT="${SPECKLE_COMMIT:-07f845d381965900b5c1c1f11db8980d1b238f20}"

if [ ! -d speckle ]; then
    git config --global url."https://github.com/".insteadOf "git@github.com:"
    git clone "$SPECKLE_REPO" speckle
    git -C speckle checkout "$SPECKLE_COMMIT"
    git -C speckle submodule update --init --recursive
fi

# Add SPEC_FLAGS to the RISC-V compilers in speckle's config.
[ -z "$SPEC_FLAGS" ] || sed -i -E "s#^([[:space:]]*(CC|CXX|FC)[[:space:]]*=.*)\$#\1 $SPEC_FLAGS#" speckle/riscv.cfg

echo "Building SPEC2017 Intspeed with $1 inputs"
cd speckle && ./gen_binaries.sh --compile --suite intspeed --input "$1"
