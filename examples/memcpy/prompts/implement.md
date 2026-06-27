Design a RoCC (Rocket Custom Coprocessor) accelerator in Chisel that performs a
memory copy of an array of 64-bit elements, and attach it to the BOOM tile of
the `${BUILD_CONFIG}` configuration.

## Functional spec

- The accelerator copies a contiguous array of ${DATA_SIZE} 64-bit elements
  (`long long`) from a source region in memory to a destination region.
- It must read each source element from memory and write it to the
  corresponding destination element using the RoCC memory interface
  (the L1 cache port the RoCC interface exposes). It MUST NOT touch any
  memory outside the source and destination arrays.

## Custom-instruction ABI (opcode = OpcodeSet.custom1)

The accelerator is driven by exactly two RoCC instructions:

- `funct == 0` — load addresses. `rs1` = source base address,
  `rs2` = destination base address. Latch both. Write `rd = 1` to signal the
  instruction was accepted.
- `funct == 1` — start copy. `rs1` = the array length (number of 64-bit
  elements to copy). Perform the full copy from source to destination, then
  write `rd = 1` to signal completion.

Both instructions write their `rd` register, so the accelerator must drive the
RoCC response channel.

## Integration requirement

Your accelerator must be present when the build system compiles
`${BUILD_CONFIG}`. Wire it onto the BOOM RocketTile in that configuration
(add the RoCC parameter to the config, or to the tile params it uses) at
opcode `OpcodeSet.custom1`. Building `${BUILD_CONFIG}` must instantiate and
exercise your accelerator — do not create a separate config that the build
will not compile.

ONLY write the Chisel source and the configuration wiring. Do NOT build, run
tests, or run simulations — a later stage handles build and simulation.

Use the `chipyard_bash` tool to explore the chipyard repository and write your
files into it. The chipyard generators (BOOM, rocket-chip, chipyard configs)
live under `${CHIPYARD_SRC_PATH}`. Read any `CLAUDE.md` you find in the
chipyard repo for background before you start, and look at how existing RoCC
accelerators (e.g. the example accumulator / `OpcodeSet` usage) are written and
attached to a tile.
