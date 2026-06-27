"""VEXT — LLM-driven RISC-V ISA-extension implementation loop.

An LLM extends the MegaBOOM core (Chisel, inside a chipyard build container)
with ISA extensions (bitmanip, crypto, vector, ...). Each iteration the core
is elaborated and the extension's test programs are run in parallel on the
Verilator DUT and on Spike (the golden ISA reference); mismatches are fed back
to the LLM, which carries a persistent session plus a knowledge log.

See vext/README.md for the design and the outer/inner contract.
"""
