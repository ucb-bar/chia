A CIRCT issue is below. Your ONLY job this turn is to produce a MINIMAL,
self-contained reproduction — do NOT fix anything yet.

$issue

Write a script to /workspace/circt/.circtissues/repro.sh with this exact contract:

    repro.sh MUST exit 0 if and only if the bug is FIXED, and exit non-zero while
    the bug is still present.

How to satisfy the contract:
  - Crash / assertion / "UNREACHABLE" / verifier error: just run the tool on the
    minimal input. Its non-zero exit (or the crash) reproduces the bug today; a
    clean run after the fix exits 0. Nothing else needed.
  - Miscompile / wrong output (the tool exits 0 but produces incorrect IR/Verilog):
    capture the EXPECTED-CORRECT output as a FileCheck pattern, so the script
    fails on today's wrong output and passes once fixed. A clean way to do this is
    to write a proper lit test under /workspace/circt/test/... with `// RUN:` and
    `// CHECK:` lines, and have repro.sh run it via the lit tool (or run
    `circt-opt ... | FileCheck ...` directly inside repro.sh).

Keep the input as small as possible: reduce the issue's example down to the few
operations that actually trigger the behavior.

Steps:
  1. With the bash tool, create /workspace/circt/.circtissues/ and write any input
     MLIR there plus repro.sh (and/or a lit test under test/...). Use the
     source-tree binaries (/workspace/circt/build/bin/circt-opt, firtool, ...).
  2. Run repro.sh yourself and confirm it currently FAILS (non-zero) on the
     unmodified tree — that proves the bug reproduces at firtool-1.148.0.
  3. If you genuinely cannot reproduce it here (it needs a tool/feature not in
     this build, or it was already fixed upstream after firtool-1.148.0), instead
     make repro.sh exit 0 and write /workspace/circt/.circtissues/NOTES.md
     explaining why.

Report what the repro does and whether it reproduced.
