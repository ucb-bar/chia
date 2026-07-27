#!/bin/bash
# SPEC CPU2006 CINT build. Clones speckle (pinned) and builds the CINT2006 suite
# against $SPEC_DIR. Run from a dir that also contains the vendored Makefile.
set -ex

if [ "$1" != "ref" ] && [ "$1" != "train" ] && [ "$1" != "test" ]; then
    echo "Must specify ref/train/test"
    exit 1
fi

SPECKLE_REPO="${SPECKLE_REPO:-https://github.com/ucb-bar/Speckle.git}"
# gen_binaries-cpu2006.sh lives only on branch chia_artifact (not master), so pin it.
SPECKLE_COMMIT="${SPECKLE_COMMIT:-16bc648f4472641a59b350426c069ffd1a88a7e9}"

if [ ! -d speckle ]; then
    git config --global url."https://github.com/".insteadOf "git@github.com:"
    git clone "$SPECKLE_REPO" speckle
    git -C speckle checkout "$SPECKLE_COMMIT"
    git -C speckle submodule update --init --recursive
fi

echo "Building SPEC CPU2006 CINT2006 with $1 inputs"
make spec06-cint2006 INPUT=$1
