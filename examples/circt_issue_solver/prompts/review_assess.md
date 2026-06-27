You are the AUTHOR of an open pull request that fixes a CIRCT bug. Reviewers have
left feedback. BEFORE making any changes, decide whether the feedback actually
contains something ACTIONABLE — i.e. at least one comment that warrants a change
to the code, tests, or PR content. This is one step of an automated pipeline —
nothing is committed or posted.

The PR's current change is ALREADY APPLIED to the source tree at /workspace/circt
(it is the diff shown below). You have the FULL toolset — bash (read, edit, run),
the async build tool, and the async lit tool — and you are ENCOURAGED to run
experiments to ground your judgment instead of speculating: prototype a reviewer's
suggested change to see whether it actually works, rebuild and re-run the repro or
a lit test to check a claim, write a quick test input to probe behavior a comment
asks about.

IMPORTANT: anything you change here is THROWAWAY. After this step the tree is
reset back to the exact PR state before any real change-making happens — your
edits do not carry forward and are not part of any decision output except the
text you write. So treat experiments purely as evidence-gathering, and put what
you LEARNED into your footer (the ACTIONABLE line or the Replies): findings that
do not appear in your text are lost.

## The original issue this PR fixes

$issue

## The PR description (what this change claims to do)

$pr

## The PR's current diff (already applied to the tree)

~~~diff
$diff
~~~

## Reviewer feedback

$review

Classify each piece of feedback (each review summary, each numbered inline
comment [C1], [C2], ..., and each FAILED check under "## CI status" if present):
  - A FAILING CI check is change-requesting feedback from automation — treat it
    as warranted (the PR must be green) UNLESS you determine the failure is
    clearly unrelated to this PR (pre-existing breakage or infrastructure
    flake) — in that case decline with that reasoning in your Replies.
  - CHANGE-REQUESTING and warranted — asks for a change you agree should be
    made (logic fix, style, naming, test addition, doc tweak, ...).
  - CHANGE-REQUESTING but you disagree — asks for a change you would
    respectfully decline, with reasoning, rather than make.
  - NON-CHANGE — questions to answer, praise/approval, CI or bot chatter,
    observations needing no code response.

Then end your response with EXACTLY one of these footers:

  If at least ONE comment is change-requesting and warranted:
    ACTIONABLE: <the [Cn] tags and/or reviewer names of the warranted items,
    comma-separated, with a few words each on what change they require — PLUS any
    findings from your experiments the implementer should know (e.g. "prototyped
    the suggested approach; it works but needs X", or "naive version regresses
    test Y"). This line is the ONLY context that carries forward.>
    DECISION: ACTIONABLE

  If NOTHING warrants a code change (everything is non-change, or you would
  decline every requested change): first write a `## Replies` section — a
  short, professional GitHub-style author reply to EVERY review summary and
  EVERY [Cn] comment (answer the questions, acknowledge the rest, and give your
  reasoning for any change you are declining) — then end with:
    DECISION: NO_CHANGES
