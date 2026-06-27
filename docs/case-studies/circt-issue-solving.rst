Autonomous CIRCT Issue Solving
==============================

A CHIA case study that uses an LLM-in-the-loop to triage the open-issue backlog
of `CIRCT <https://circt.llvm.org/>`_ — the MLIR-based hardware compiler
infrastructure — and, for each candidate, drive a sequence of agents through
**assess → reproduce → fix → verify → (regression repair) → writeup** inside a
real CIRCT checkout. The full flow lives in
``chia/examples/circt_issue_solver``.

Overview
--------

Large open-source projects are facing a new burden: AI tools have enabled 
a significant increase in outside contributions to large
open source GitHub repositories, including new issue reports and pull requests,
but has lowered the overall quality of the contributions.
As noted in the LLVM project's AI tool use policy,
when these contributions are 
done entirely by AI in an unprincipled way and without human review,
this "extracts work from [maintainers] in the form of design and code review". CHIA cannot make a contributor review their AI's work, but it can force the AI to make improvements in a principled, structured way, that leads naturally to high quality bug fixes and PRs.

``circt_issue_solver`` shows this. We take a multi-step approach to fixing an issue which ensures that fixes are only proposed for real bugs, and only for issues where discussion has converged on a clear solution. Furthermore, we run validation programmatically, including the entire regression test suite, and a reproduction script that confirms that the bug has gone from unfixed to fixed based on the LLMs changes. The pipeline works as follows:

First, the head node **triages** open issues down to a sample of plausible candidates, then runs
one independent pipeline per candidate across a pool of CIRCT containers. Each pipeline:

#. **assesses** the issue — is this actually a bug, and are *both* the bug and
   the correct behavior clear enough to act on autonomously? If not, it logs the
   reason and skips;
#. **reproduces** it — writes ``.circtissues/repro.sh`` with the contract *exit 0
   iff the bug is fixed*, and skips the issue if it doesn't actually reproduce on
   the pinned tree;
#. **fixes** it — edits ``/workspace/circt``, rebuilds, reruns the repro, and
   adds a lit regression test;
#. **verifies** deterministically — rebuilds, reruns the repro, and runs the full lit gate;
#. on a regression, gets **one repair turn** to fix the broken tests without un-fixing the bug; and
#. writes the **PR description it would submit**.

The output is local: a candidate diff and the PR writeup, persisted to
``issue_logs/issue_<N>/`` and a row in a SQLite database (``issues.db``). A
second flow reads **PR review feedback** (reviewer comments *and* failing CI) and
produces an updated diff plus the replies it would post. Crucially, **neither
flow writes to GitHub — both only read.** A human reviews and submits.

.. note:: **Result**

   We worked with the maintainers of CIRCT to target our flow at a manageable
   number of issues. It correctly assessed 16 issues, 5 of which were
   reproducible bugs with clear solutions. It solved all 5. Of those 5, 2 were
   not eligible for AI assisted PRs (because they were labelled as "good first
   issues"). We submitted PRs for the other 3 and they have all been upstreamed.
   Read section 5 of our `arXiv paper
   <https://arxiv.org/abs/2606.27350>`_ to hear more about our results.

How it works
------------

The per-issue pipeline
~~~~~~~~~~~~~~~~~~~~~~~~

Triage runs on the head; each surviving candidate is fanned out as one
``run_issue_remote`` task pinned to a single CIRCT container. Within that
container the phases run in sequence, and any phase can short-circuit the
pipeline (``not_a_bug`` / ``unclear`` / ``no_repro``) so a fix attempt is only
ever spent on an issue that has cleared the gates before it::

    HEAD (triage, read-only GithubIssuesNode)
      |
      +-- list the open backlog, drop: already attempted, feature requests,
      |   repro-less issues, and issues with an open PR attached
      +-- sample max_issues candidates at random
      |
      +-- fan out one run_issue_remote per candidate across the circt slots
            |
            v
    CIRCT WORKER (one issue per container, pinned)
      |
      +-- git reset --hard <tag> + warm incremental build
      +-- ASSESS      [llm]  bug? AND is the correct behavior clear?  -> skip: not_a_bug / unclear
      +-- REPRODUCE   [llm]  write .circtissues/repro.sh (exit 0 iff fixed)  -> skip: no_repro
      +-- FIX         [llm]  edit /workspace/circt, rebuild, rerun repro, add a lit test
      +-- VERIFY      [deterministic, no LLM]  rebuild, rerun repro, run the full lit gate
      +-- REGRESSION  [llm]  only if repro is green but the suite went red — one repair turn
      +-- WRITEUP     [llm]  the PR description it WOULD submit
      |
      +-- return result dict -> HEAD persists issue_logs/issue_<N>/ + a row in issues.db

Each LLM phase is a fresh, stateless ``claude --print`` session: the context a
later phase needs is inlined into its prompt (the ``repro.sh`` into the fix
prompt; the diff and verdict into the writeup), rather than carried via
``--resume``.

Triage on the head
~~~~~~~~~~~~~~~~~~~

Triage is a cheap heuristic filter over the open backlog using the read-only
:class:`~chia.github.github_issues_node.GithubIssuesNode`. It lists the most
recent open issues *without* fetching per-issue comments (the filter only needs
title/body/labels), keeps issues that carry a fenced code block plus either a
tool command or a failure signal (crash/assert/miscompile), and drops feature
requests, already-attempted issues, and anything with an open PR attached. There
is deliberately **no label gate** — whether an issue is really a bug is left to
the per-issue assess phase, which reads the issue *and* the source:

.. code-block:: python

    def has_repro(issue) -> bool:
        """A self-contained repro: a fenced code block + a tool command or a
        failure signal (crash/assert/miscompile)."""
        body = issue.body or ""
        return "```" in body and (bool(_CMD_RE.search(body)) or bool(_SIGNAL_RE.search(body)))

The qualifying set is shuffled and sampled, and only the chosen survivors are
re-fetched with their comments attached — so the expensive per-issue requests
are paid only for the handful that will actually run.

The deterministic verify gate
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To keep fixes principled, the fix is judged by a **verify step that uses no
LLM** — programmatic orchestration confirms the change works as well as the agent
claims, so the agent cannot talk its way past it or mark its own work ``fixed``.
Verify captures the diff, rebuilds the tool targets, reruns the
repro (which exits 0 only when the bug is fixed), and runs the whole lit suite
minus the categories that are red on the unmodified image (``test/CAPI`` and
``Tools/circt-tblgen`` need binaries this SDK-only build doesn't ship). An issue
is only marked ``fixed`` when the repro is green **and** the full lit gate is
green:

.. code-block:: python

    def circt_lit_gate_paths() -> list[str]:
        """Every top-level test/<category> except the baseline-red ones, so a
        gate failure means a real regression, not a missing-binary artifact."""
        ...

    # status = "fixed" iff repro green AND full lit gate green; else "attempted"

If the repro flips green but the suite goes red, the pipeline fires a single
**regression-repair** turn — the failing tests and their output inlined — to
mend the regression without un-fixing the bug, then re-verifies. Because the gate
runs the whole suite (not just the touched dialect), it catches collateral
damage anywhere in the tree and never vacuously passes.

Distributing work across the cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each per-issue task is a CHIA function that holds one whole ``circt`` slot, so
exactly one issue runs per CIRCT container. Within the task, every LLM phase is
dispatched onto an ``llm`` worker (``1.0`` per call, so the cluster's ``llm``
slots cap prompt concurrency) while the bash / build / lit MCP servers stay on
the CIRCT worker and are reached over HTTP — the heavy CIRCT checkout never moves:

.. code-block:: python

    @ChiaFunction(resources={"circt": 1})
    def run_issue_remote(issue_md: str, number: int, cfg: dict, ...) -> dict:
        ...
        cli = get(llm.prompt.options(resources={"llm": 1.0})
                            .chia_remote(llm, prompt, tools))

The flow ships **this repo's** ``chia`` — plus the head-side ``circt_util.py``
and ``issue_task.py`` — to every worker via Ray ``py_modules``, so edits to the
flow reach workers on the next submit with no image rebuild:

.. code-block:: python

    _CHIA_PKG = FLOW_DIR.parent.parent / "chia"
    _PY_MODULES = [str(FLOW_DIR / "circt_util.py"),
                   str(FLOW_DIR / "issue_task.py"),
                   str(_CHIA_PKG)]
    ...
    ray.init(address="auto",
             runtime_env={"py_modules": _PY_MODULES, "excludes": _RUNTIME_ENV_EXCLUDES})

The general CIRCT build/test primitives and the
:class:`~chia.chipyard.circt.BuildTool` / :class:`~chia.chipyard.circt.LitTool`
MCP wrappers live in the ``chia`` package (``chia.chipyard.circt``), so they ride
along with it.

The LLM in the loop
~~~~~~~~~~~~~~~~~~~~

Each phase runs on an ``llm`` worker via
:class:`~chia.models.claude.ClaudeCodeLLM`, under a shared system prompt that
casts it as a senior MLIR/CIRCT engineer and pins the rules that keep a fix
honest — make the smallest root-cause change, never disable a test or
special-case the repro, and treat LLVM/MLIR (the prebuilt SDK) as out of scope
rather than hacking around it. A fresh session is built per phase:

.. code-block:: python

    llm = ClaudeCodeLLM(
        model=cfg["model"], system_message=cfg["system_prompt"],
        timeout_seconds=cfg["timeouts"][phase],
        extra_cli_args=["--effort", "max"],
        resume_session=True, projects_cwd=None,
    )

Alongside the prompt the agent gets MCP tools that execute inside the CIRCT
container: a :class:`~chia.base.tools.BashTool.BashTool` to read, edit, and run
shell commands in ``/workspace/circt``; an async ``BuildTool`` that starts a
ninja rebuild and is polled to completion; and an async ``LitTool`` that runs a
lit regression set. The async build/lit tools return immediately and are polled,
so a long build can't stall the transport.

The assess phase is the one that most shapes quality, and it is where the
"principled" commitment starts. It separates "is this a bug?" from "is the
correct behavior clear?" — a crash is an obvious defect, but what the tool
*should* do instead is often a design decision (e.g. reject the input vs. extend the
code to handle it). The phase is asked to **enumerate the materially different
ways a maintainer could reasonably resolve the issue**; if more than one is
defensible and nothing in the issue, spec, or docs singles one out, it returns
``UNCLEAR`` and the pipeline skips the issue rather than producing a
confidently-wrong patch. That guard is exactly what caught issue #8508 in the
study (a transform run on an ``extmodule`` DUT where "diagnose and refuse" and
"extend to support it" were both defensible — see ``issue_logs/issue_8508/``):
the bug was crystal clear, but the *fix* was a maintainer's design call, so the
flow correctly declined to guess.

The review flow
~~~~~~~~~~~~~~~

A companion flow (``review_loop.py`` → ``review_task.py``) closes the human loop.
Given a ``PR:ISSUE`` pair, it reconstructs the PR's state on the pinned tree
(re-applying the PR's current diff fetched from GitHub via
:class:`~chia.github.github_pulls_node.GithubPullsNode`), then runs
**triage → (if actionable) fix → verify → replies** over the reviewer comments
*and* any failing CI checks. A PR that is simply red in CI — with no human
comments — is enough to trigger a round. It produces an updated diff and the
author replies it would post; as with the issue flow, nothing is written back to
GitHub.

Principled fixes and maintainer friendliness
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The two commitments from the overview are not afterthoughts — they are wired into
the pipeline and into how the flow was actually run, and together they are what
let every submitted PR be upstreamed.

**Fixes are principled by construction.** The shared system prompt holds the
agent to the smallest root-cause change and forbids the shortcuts that make an
automated "fix" worthless — or worse, harmful — to a reviewer: disabling or
weakening a test, special-casing the repro, or hacking around an LLVM/MLIR root
cause it cannot legitimately build. Two gates enforce this rather than trusting
the agent's word:

- the **assess phase** (above) refuses to act unless the bug *and* its correct
  resolution are unambiguous, by enumerating the reasonable resolutions and
  bailing to ``UNCLEAR`` when more than one stands — so a fix is only ever
  attempted where there is a single clear answer;
- the **verify phase** (above) runs with no LLM in the loop at all: programmatic
  orchestration rebuilds, reruns the repro, and runs the full regression suite,
  so a change is only ever called ``fixed`` when it provably is.

**The human — and the maintainers — stay in charge.** Neither flow writes to
GitHub; every diff and PR writeup is a *proposal*. In the study a human reviewed
each change in detail and hand-wrote the pull request before anything reached
upstream CIRCT. The load on the maintainers was bounded just as deliberately: the
flow was run on a small, randomly chosen set of issues — in close coordination
with the CIRCT maintainers on both the *quantity* and the *quality* of
contributions — rather than firing patches at the whole backlog. Issues labelled
*good first issue* were deliberately left for human newcomers, in keeping with the
project's norms and the broader LLVM concern that unprincipled AI contributions
mostly extract design- and code-review effort from maintainers. When the
submitted PRs drew review feedback, that feedback was addressed with the **review
flow** above — the same principled, human-in-the-loop machinery — before
resubmitting. See our `arXiv paper <https://arxiv.org/abs/2606.27350>`__ for the
full study.

Targeting a different repository
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The repo is the one thing you change to retarget both flows. In ``config.py``:

.. code-block:: python

    GITHUB_REPO = "llvm/circt"   # read by both flows

It defaults to ``llvm/circt``; the only supported change is pointing it at a
CIRCT **fork** (e.g. to review PRs on your own fork) — the rest of the flow still
assumes CIRCT's build and lit conventions.

.. note::

   The ``chia-circt`` image is pinned at **firtool-1.148.0**. Issues fixed
   upstream after that tag won't reproduce, so the reproduce gate correctly marks
   them ``no_repro`` and the pipeline moves on. Likewise, a root cause that lives
   in LLVM/MLIR (the prebuilt SDK / ``llvm`` submodule) is out of scope — only
   CIRCT's own tree is buildable here, so the agent reports such cases instead of
   working around them.

Setup
-----

These steps mirror the example's ``README.md``. Run them from the example
directory (``<repo>/chia/examples/circt_issue_solver``) unless noted.

**1. Head conda env** — only the head needs this; workers get ``chia`` via Ray
``py_modules`` and run the Docker images named in ``cluster.yaml``:

.. code-block:: bash

    conda env create -f env.yml
    conda activate circtissues

**2. Cluster** — ``cluster.yaml`` is single-machine by default: the head plus
four containers on one host, read from ``CHIA_HEAD``. The required worker
counts (``min_workers`` / ``max_workers``):

.. list-table::
   :header-rows: 1
   :widths: 22 12 66

   * - Node type
     - Workers
     - Role
   * - ``circt_llm``
     - 2
     - runs the assessing / fixing / reviewing LLM (light); image
       ``chia-claude-code``
   * - ``circt_worker``
     - 2
     - owns a ``/workspace/circt`` checkout, builds + runs lit (heavy); image
       ``chia-circt``

Each ``circt_llm`` container bind-mounts your Claude Code config into the
container. If your credentials live somewhere other than ``~/.claude``, edit the
mount in ``cluster.yaml`` to match:

.. code-block:: yaml

    run_options:
        - "-v ~/.claude:/home/ray/.claude"   # mount your Claude config into the container

Scale up by raising the per-type and cluster-wide ``min/max_workers`` and adding
IPs to ``compatible_ips``. The default ports (GCS 6379, dashboard 8265) mean only
one CHIA cluster should be up per host at a time. See
:doc:`/user_guides/cluster_config_reference` for the full schema.

**3. GitHub token** — both flows read issues/PRs through an authenticated client.
Set a token with read access to the repo (if using
public CIRCT this step can be skipped):

.. code-block:: bash

    export GITHUB_TOKEN=...            # read access to GITHUB_REPO
    export CHIA_HEAD=$(hostname)       # host to bring the cluster up on

**4. Config** — in ``config.py``, leave ``GITHUB_REPO`` at ``llvm/circt`` or
point it at a CIRCT fork.

**5. Bring up the cluster** — from the example directory with the env active:

.. code-block:: bash

    chia up cluster.yaml

The first run pulls the ``chia-circt`` image, which is large; on slow links the
pull may time out, so raise the pull timeout and retry.

**6. Run the issue flow** — ``fix_issues_submit.sh`` wraps ``chia job submit`` so
the driver's logs land in the Ray dashboard (``http://localhost:8265``) and in
``chia job logs <id>``:

.. code-block:: bash

    ./fix_issues_submit.sh --max-issues 2            # triage + attempt 2 candidates
    ./fix_issues_submit.sh --issue 10568             # one specific issue, skip triage
    NO_WAIT=1 ./fix_issues_submit.sh --max-issues 5  # detach; watch the dashboard

**7. (Optional) Review flow** — feed it a PR number paired with its issue number;
the PR's current diff and feedback are fetched from GitHub:

.. code-block:: bash

    ./review_submit.sh --pr 10648:7388

**8. Tear down** — when the run is done:

.. code-block:: bash

    chia down cluster.yaml

Outputs land in ``issue_logs/issue_<N>/``: the candidate ``fix.diff``, the
``pr_writeup.md`` it would submit, a ``verdict.json`` (status, repro/build/lit
results, diff counts), the saved repro, and per-phase LLM transcripts
(``llm_<phase>.md`` / ``.jsonl``) — plus one row per attempt in ``issues.db``.
Review-flow outputs land under ``review_logs/issue_<N>_pr_<M>/`` (the updated
diff and the replies it would post). Re-running skips issues already recorded in
``issues.db``.
