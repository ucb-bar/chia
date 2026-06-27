You are triaging a CIRCT issue BEFORE any attempt to reproduce or fix it. Decide
whether this issue is something the automated pipeline should attempt at all —
there is no human to ask for clarification.

$issue

You have access to a circt_bash MCP tool that gives you the abillity to operate in a CIRCT code base.

Read the issue carefully. You MAY use the circt bash tool to investigate
/workspace/circt READ-ONLY to understand the intended behavior: grep and read the
relevant source (lib/, include/circt/), the dialect docs (docs/, and the op
`summary`/`description` fields in the *.td files), and `git log` / `git blame`.
Do not edit anything — this is analysis only.

Make the decision in two parts:

PART A — Is this actually a BUG?
  A bug is a defect where CIRCT behaves incorrectly: a crash, an assertion
  failure, a miscompile, wrong output, a missing or incorrect verifier, or a
  clear violation of the FIRRTL/MLIR/CIRCT spec. It is NOT a bug if it is a
  feature request or enhancement, a design proposal / RFC, a usage question, a
  build / CI / packaging / infrastructure issue, a documentation request, or
  behavior that is actually working as intended. If it is not a bug, STOP and
  report NOT_A_BUG — do not analyze further.

PART B — (only if it IS a bug) Are BOTH of these CLEAR?
  1. THE BUG — what is actually going wrong, concretely enough to reproduce it.
  2. THE CORRECT BEHAVIOR — what the tool SHOULD do instead, unambiguously
     enough to tell whether a candidate fix is right.

     A clear BUG does NOT imply a clear CORRECT BEHAVIOR — judge them
     separately. A crash, assertion, or illegal IR is an obvious defect, but
     what the tool should do *instead* is frequently a design decision, and that
     is the part most often wrongly assumed to be clear. Before you call the
     correct behavior clear, explicitly enumerate the materially-different ways a
     maintainer could reasonably resolve it — most importantly REJECT the input
     with a diagnostic vs. EXTEND the code to handle it, but also any choice of
     output, semantics, or which component should change. If more than one of
     those is defensible and the issue / code / docs / spec do not single one
     out, the correct behavior is NOT clear → UNCLEAR. Choose CLEAR only when
     ONE resolution is clearly the intended one, grounded in the issue text, the
     spec, the docs, or an unambiguous existing CIRCT convention — not merely a
     reasonable-sounding answer you inferred or your own preference among
     options. (A clearly-VALID input that crashes or miscompiles usually has one
     defensible answer — handle it correctly; rejecting valid input is not on the
     table — so that is typically CLEAR. The reject-vs-extend ambiguity bites
     mainly when the INPUT itself is unusual, questionable, or arguably invalid —
     e.g. a transform invoked on a kind of operation it was never designed for
     (such as a blackbox/extmodule where a normal module is assumed), where
     "diagnose and refuse" and "extend to support it" are both defensible.) Reporter hedging ("this is weird", "not sure if…", "should it…?",
     "I'd expect… but maybe") is a strong signal the expected behavior is
     unsettled. Also UNCLEAR if essential details (input, version, expected vs.
     actual) are missing and unrecoverable from the code.

This decision gates an automated pipeline: only DECISION: CLEAR proceeds to
reproduce + fix. NOT_A_BUG and UNCLEAR are LOGGED with your reason and skipped so
we move on to a different issue. Be honest — a confidently-wrong "clear" wastes a
fix attempt and can yield a misguided patch — but do not reject a genuinely
well-specified bug just because the fix looks hard.

End your response with EXACTLY one of these footers, each field on its own line:

  If it is a bug and both the bug and correct behavior are clear:
    BUG: <one sentence — what is wrong>
    EXPECTED: <one sentence — the correct behavior>
    DECISION: CLEAR

  If it is not a bug:
    DECISION: NOT_A_BUG
    REASON: <1-3 sentences: what kind of issue it actually is>

  If it is (or may be) a bug but the bug or the correct behavior is not clear:
    DECISION: UNCLEAR
    REASON: <2-4 sentences: which is unclear — the bug, the expected behavior,
    or both — and specifically what is missing or ambiguous>
