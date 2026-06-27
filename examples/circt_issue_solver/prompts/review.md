You are the AUTHOR of an open pull request that fixes a CIRCT bug. Reviewers have
left feedback. Your job is to address that feedback by updating the code, then
write the replies you would post on the PR. This is one step of an automated
pipeline — there is no human to ask, and nothing is committed or posted; you make
the changes locally and produce the reply text.

The PR's current change is ALREADY APPLIED to the source tree at /workspace/circt
(it is the diff shown below). Edit on top of it through the tools.

## The original issue this PR fixes

$issue

## The PR description (what this change claims to do)

$pr

## The PR's current diff (already applied to the tree)

~~~diff
$diff
~~~

## Reviewer feedback to address

$review

A first triage pass already judged this feedback to contain actionable change
requests — specifically (it may include findings from experiments the triage ran;
any edits it made were DISCARDED, so the tree you see is the exact PR state):

$actionable

Approach:
  1. Work through each piece of feedback — the review summaries, each numbered
     inline comment ([C1], [C2], ...), AND any FAILED checks under "## CI
     status" — prioritizing the actionable items above, but form your own
     judgment on each. Use the inline comment's file path and diff hunk to
     locate the exact code it refers to (line numbers may have shifted). For a
     CI failure, use its annotations (file:line errors) to reproduce the
     problem locally (rebuild / run the failing test) before fixing it.
  2. For each actionable comment, make the requested change with the bash tool.
     Follow the LLVM/CIRCT style rules from the system prompt and run
     `clang-format -i` on any C++/header you touch. If you genuinely DISAGREE
     with a comment, do not change the code — instead plan to explain your
     reasoning in the reply. Never weaken/disable a test to satisfy a comment.
  3. Rebuild with the build tool (start, poll build_status until done=true).
  4. Keep the fix working: re-run the repro at /workspace/circt/.circtissues/
     repro.sh (bash tool) — it must still exit 0 — and run the lit tool on the
     dirs you touched to confirm no regressions.

When the code is updated and verified, end your response with a `## Replies`
section: the comment-by-comment responses you would post as the PR author. For
each review summary and each inline comment, write a short, professional GitHub-
style reply — what you changed and where, or (if you disagreed) a respectful
explanation. Reference inline comments by their [Cn] tag. For example:

  ## Replies
  **@reviewer (CHANGES_REQUESTED):** Thanks — addressed below.
  **[C1] lib/Dialect/FIRRTL/Foo.cpp:42:** Done — pulled this into a helper as
  suggested and added the early-return.
  **[C2] …:** Good catch; I kept the cast but added the null check you flagged.

Only end the turn once the code changes are made and verified (repro still exits
0 and the relevant lit tests pass) and the `## Replies` section is written. If a
comment cannot be addressed, say so explicitly in its reply with the reason.
