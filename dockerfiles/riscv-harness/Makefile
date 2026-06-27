# Cross-compile harness for RISC-V ELFs.
#
# TARGET=verilator (default) -> baremetal ELF (<PROGRAM>.riscv) that loads
#                                into chipyard / MegaBoom Verilator via
#                                `+permissive-off <bin>`. Flags match
#                                chipyard/tests/CMakeLists.txt.
# TARGET=linux               -> userspace ELF (<PROGRAM>) runnable on a
#                                Linux RISC-V system (e.g. FireSim Linux).
#
# Usage:
#   make PROGRAM=hello
#   make PROGRAM=hello TARGET=linux
#   make PROGRAM=hello EXTRA_CFLAGS=-DDEBUG EXTRA_LDFLAGS=-lm
#   make PROGRAM=memcpy SRCS="memcpy.c helper.c"
#   make PROGRAM=hello clean

TARGET      ?= verilator
PROGRAM     ?=
SRCS        ?= $(PROGRAM).c
HARNESS_DIR ?= /opt/riscv-harness

ifeq ($(PROGRAM),)
$(error PROGRAM= must be set (e.g. `make PROGRAM=hello`))
endif

# ---- Per-target settings -----------------------------------------------------
ifeq ($(TARGET),verilator)
TOOL_PREFIX := riscv64-unknown-elf-
ARCH        := rv64imafd_zicsr_zifencei
ABI         := lp64d
TARGET_CF   := -mcmodel=medany -specs=htif_nano.specs \
               -fno-common -fno-builtin-printf \
               -ffunction-sections -fdata-sections
TARGET_LF   := -specs=htif_nano.specs
OUTPUT      := $(PROGRAM).riscv
else ifeq ($(TARGET),linux)
TOOL_PREFIX := riscv64-unknown-linux-gnu-
ARCH        := rv64gc
ABI         := lp64d
TARGET_CF   :=
TARGET_LF   :=
OUTPUT      := $(PROGRAM)
else
$(error TARGET must be 'verilator' or 'linux' (got '$(TARGET)'))
endif

# ---- Common flags ------------------------------------------------------------
ARCH_FLAGS := -march=$(ARCH) -mabi=$(ABI)
CFLAGS     := -std=gnu99 -O2 -Wall -Wextra \
              $(ARCH_FLAGS) $(TARGET_CF) \
              -I$(HARNESS_DIR)/include \
              $(EXTRA_CFLAGS)
LDFLAGS    := -static $(TARGET_LF) $(EXTRA_LDFLAGS)
OBJS       := $(addsuffix .o,$(basename $(SRCS)))

# ---- Rules -------------------------------------------------------------------
.PHONY: all clean dump
all: $(OUTPUT)

%.o: %.c
	$(TOOL_PREFIX)gcc $(CFLAGS) -c $< -o $@

%.o: %.S
	$(TOOL_PREFIX)gcc $(CFLAGS) -c $< -o $@

# Don't carry CFLAGS at link time — -specs=htif_nano.specs is already in
# LDFLAGS, and passing it twice triggers a duplicate-spec error.
$(OUTPUT): $(OBJS)
	$(TOOL_PREFIX)gcc $(ARCH_FLAGS) $(LDFLAGS) $(OBJS) -o $@

dump: $(OUTPUT)
	$(TOOL_PREFIX)objdump -D $< > $(PROGRAM).dump

clean:
	rm -f $(OBJS) $(PROGRAM).riscv $(PROGRAM) $(PROGRAM).dump
