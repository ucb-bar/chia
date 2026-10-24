# Build system for SPEC CPU2006. Drives speckle's SPEC2006 build script to
# cross-compile the CINT/CFP suites. TMA counter injection is OPT-IN: pass TMA=1
# to cross-compile tma_inject.o (ctor/dtor from tests/tmatests/, which requires
# this repo checked out as a submodule of its parent) and link it into every
# benchmark via speckle's TMA_INJECT_OBJ. Default TMA=0 builds plain SPEC.
# SPEC06 build resources (configs, run scripts, cmd files) live in speckle.

SPECKLE_DIR       ?= speckle
INPUT             ?= ref
CROSS_COMPILE     ?= riscv64-unknown-linux-gnu-
TMA               ?= 0
TMA_COUNTERS_DIR  ?= $(CURDIR)/../../tests/tmatests

spec06_suites = cint2006 cfp2006

# TMA=1 wires the injection object into the overlay build; TMA=0 leaves both empty.
ifeq ($(TMA),1)
TMA_DEP := $(SPECKLE_DIR)/tma_inject.o
TMA_ENV := TMA_INJECT_OBJ=$(abspath $(SPECKLE_DIR)/tma_inject.o)
endif

# Flags must match riscv-cpu2006.cfg (rv64imafdc / lp64d) so the .o is ABI-compatible.
$(SPECKLE_DIR)/tma_inject.o: $(TMA_COUNTERS_DIR)/tma_inject.c $(TMA_COUNTERS_DIR)/tma_counters.h
	$(CROSS_COMPILE)gcc -O2 -c -march=rv64imafdc -mabi=lp64d \
	    -I$(TMA_COUNTERS_DIR) -o $@ $<

# Per-suite overlay: $(SPECKLE_DIR)/build/overlay/<suite>/<input>/.
$(SPECKLE_DIR)/build/overlay/%/$(INPUT): $(TMA_DEP)
	cd $(SPECKLE_DIR) && $(TMA_ENV) ./gen_binaries-cpu2006.sh --compile --suite $* --input $(INPUT)

spec06-%: $(SPECKLE_DIR)/build/overlay/%/$(INPUT)
	@echo "Built overlay: $^"

clean:
	rm -rf $(SPECKLE_DIR)/build $(SPECKLE_DIR)/tma_inject.o

# Note: do NOT put spec06-* targets in .PHONY -- make skips implicit/pattern
# rule lookup for phony targets, which would defeat the spec06-%: pattern
# rule above. (The same latent bug lives in spec2017's Makefile but is
# masked by a typo: `$(spec17-suites)` with a hyphen is undefined.)
.PHONY: clean
.PRECIOUS: $(SPECKLE_DIR)/build/overlay/%/$(INPUT)
