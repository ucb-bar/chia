You have finished working on the CIRCT issue below. Write the GitHub Pull Request
description you WOULD submit for your change. It will NOT be posted automatically —
a human will review it — so be precise and honest.

$issue

Your change (git diff against firtool-1.148.0):

~~~diff
$diff
~~~

Automated verification result:

~~~json
$verdict
~~~

Write a clear PR description in Markdown containing:
  - A title line of the form: `[Dialect/Area] short imperative summary`
  - **Root cause** — what was actually wrong and why it manifested as reported.
  - **Fix** — what you changed and why it is correct and minimal.
  - **Testing** — the repro, and which lit tests you ran or added, with results.
    Call out honestly anything left unverified or any remaining risk (e.g. if the
    diff is empty, the repro did not flip, or lit regressed, say so plainly).
  - **Existing-test changes** — REQUIRED if your diff modifies any test file that
    already existed (i.e. edits/deletes existing lines, not just appends a new
    test). For EACH such test, name the file and justify the change explicitly:
    why the old expectation was wrong or obsolete, what behavior the new
    expectation encodes, and why this is a legitimate update rather than masking
    a regression or weakening coverage. (Renaming/retargeting a CHECK, updating
    expected output, or editing input IR all count.) If you changed no existing
    tests — only added new ones — state "No existing tests were modified." Be
    candid: if a changed test no longer tests what its name implies, say so.
  - A closing `Fixes #<number>` line using the issue number above.

Output ONLY the PR description — no preamble, no surrounding code fence.
