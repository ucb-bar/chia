Now fix the bug. A reproduction has already been prepared at
/workspace/circt/.circtissues/repro.sh (its inputs are in that directory). It
exits 0 only once the bug is fixed; on the unmodified tree it currently fails.
For reference, here is repro.sh:

~~~bash
$repro
~~~

$issue

Approach:
  1. Locate the root cause in /workspace/circt. The relevant pass/dialect is
     usually hinted by the issue title (e.g. lib/Dialect/<D>/..., lib/Conversion/
     <X>/..., or include/circt/Dialect/<D>/...). Use the bash tool to grep/read.
  2. Make the smallest correct fix. Edit files with the bash tool (sed, python, or
     `cat <<'EOF' > file`).
  3. Rebuild with the build tool: call build to start (it returns immediately),
     then poll build_status until it returns done=true to get the result. Target
     the tool your repro uses, e.g. ["circt-opt"] or ["firtool"].
  4. Re-run repro.sh (via the bash tool) and iterate until it exits 0.
  5. You should add a lit test that pins the fixed behavior, committed in the
     source tree under test/ (e.g. test/Dialect/<D>/..., test/Conversion/<X>/...),
     mirroring the conventions of the existing tests in that directory (a `// RUN:`
     line plus `// CHECK:` / `expected-error` directives). It should FAIL on the
     unmodified tree and PASS with your fix — i.e. encode the same scenario as
     repro.sh as a proper regression test, so the fixed behavior stays pinned
     after this change. (repro.sh itself is not committed; the lit test is the
     durable regression guard a reviewer will expect.) Prefer extending an
     existing test file in that directory over creating a new one when natural.
  6. Run the lit tool on the test dir(s) covering the code you touched (e.g.
     ["test/Dialect/FIRRTL"]): call run_lit to start, then poll lit_status until
     done=true. Confirm your new test passes and that you did not regress the
     others.

Do not stop until repro.sh exits 0 AND the relevant lit tests pass (ideally
including the test you added) — or until you are confident the issue cannot be
fixed at this commit, in which case explain why. Do not weaken assertions or skip
tests to pass.
