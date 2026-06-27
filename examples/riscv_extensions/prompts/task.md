Implement the **{ext_name}** extension in the BOOM core.

> {ext_desc}

BOOM Chisel source is at `{boom_src}` (inside the build container; reach it with
the bash tool). **Call read_spec first and open the file(s) it names with your
Read tool — that spec is the only source of truth for the
instruction encodings and semantics (Rule #1); implement strictly to it and
never rely on memory or assumptions.** Then read_status for the instruction
list and per-instruction pass/FAIL.

The loop builds CONFIG **{config}** — create it per the system prompt (it does
not exist yet in a fresh tree), and append **{isa_suffix}** to the cosim ISA
string in IOBinders so the embedded spike decodes your instructions.

The loop runs **{num_tests}** directed per-instruction tests for this extension
after every turn (self-checking riscv-tests, or — where none exist — a Spike
differential where any DUT-vs-Spike mismatch is a failure). On the stock
(unmodified) core, **{num_failing}** fail — not yet (correctly) implemented:

{failing_tests}

First, explore: grep the BOOM source to learn how an existing, similar
instruction flows through decode → execute → writeback. Then implement the
missing instructions and append_knowledge with what you learn about BOOM's
internals. End your turn when you have a coherent set of edits; the loop will
rebuild the core, re-run all tests on the DUT and Spike, and update read_status
with which now pass.
