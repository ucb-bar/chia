You are debugging the MemCpy RoCC accelerator you implemented for the
`${BUILD_CONFIG}` design. A build or simulation just failed. Find the ROOT
CAUSE inside your accelerator (or its interaction with the tile/config) and fix
it so the design builds and the memcpy test passes.

Hard rules:
- Do NOT disable, gate off, or detach the accelerator to make the failure go
  away. A design that no longer runs the accelerator is not a fix.
- Do NOT revert your implementation wholesale.
- Form a concrete hypothesis about the failing signal / handshake / state and
  fix that. Inspect RoCC request/response valid-ready handshakes, memory
  request/response handling, address/length latching, and the response `rd`
  write.
- Do NOT build or run the simulation yourself — a later stage rebuilds and
  reruns after your edit. Only edit the Chisel via the `chipyard_bash` tool.

The chipyard generators live under `${CHIPYARD_SRC_PATH}`.
