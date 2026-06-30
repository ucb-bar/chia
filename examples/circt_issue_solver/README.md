# circt_issue_solver — autonomous CIRCT GitHub-issue solver (CHIA example)

A CHIA loop which triages open CIRCT issues and, for each candidate, drives a sequence of agents through
**assess → reproduce → fix → verify → (regression repair) → writeup** inside a
real CIRCT checkout (the `chia-circt` image). 
The output is local: a candidate diff and the PR description it *would* submit. A
second flow reads **PR review feedback** (reviewer comments *and* failing CI) and
produces an updated diff + the replies it *would* post. **No GitHub writes — both
flows only read.**

## Setup

`config.py` pins the repo (both flows read it):

```python
GITHUB_REPO = "llvm/circt"   # used by both flows
```

It defaults to llvm/circt; the only supported change here is pointing it at a
CIRCT **fork** (e.g. to review PRs on your own fork) — the rest of the flow still
assumes CIRCT. Set `GITHUB_TOKEN` (read access to the repo) in the environment
before running.

If you have a non-default claude credential install location, change the first part of following line in the cluster.yaml accordingly:

```yaml
- "-v ~/.claude:/home/ray/.claude" # Mount claude configuration dir to container
```

## Flows

### Issue flow (`circt_issue_loop.py` → `issue_task.py`)
1. **Triage** (head, read-only `GithubIssuesNode`): sample open issues that carry
   a code-block repro, aren't obvious feature requests, aren't already attempted by the flow,
   and have no open PR attached. See `triage.py`.
2. **Per issue** (one `run_issue_remote` task per candidate across the CIRCT
   containers):
   - **assess** — is this actually a bug, and are the bug *and* the correct
     behavior clear? If not, log the reason and skip (`not_a_bug` / `unclear`).
   - **reproduce** — write `/workspace/circt/.circtissues/repro.sh` (contract:
     *exit 0 iff fixed*); skip if it doesn't reproduce (`no_repro`).
   - **fix** — edit `/workspace/circt`, rebuild, rerun repro, add a lit test.
   - **verify** — deterministic, no LLM: rebuild, rerun repro, run the full lit
     gate.
   - **regression repair** — if the fix broke other tests, one more turn with the
     failing tests to repair without un-fixing the bug.
   - **writeup** — the PR description it would submit.
3. **Persist** (head): `issue_logs/issue_<N>/` (`fix.diff`, `pr_writeup.md`,
   `verdict.json`, per-phase `llm_*.md` / `.jsonl`) + a row in `issues.db`.

### Review flow (`review_loop.py` → `review_task.py`)
`./review_submit.sh --pr <PR#>:<ISSUE#>` reconstructs the PR (its current diff
fetched from GitHub) and runs **triage → (if actionable) fix → verify → replies**
over the reviewer comments *and* failing CI checks. A PR that is simply red in CI
(no human comments) is enough to trigger a round. Output lands in
`review_logs/issue_<N>_pr_<M>/`.

Prompting uses `chia.models.claude`: each per-issue task dispatches
its `prompt` onto an `llm` worker (1.0/call), while the bash/build/lit MCP servers
stay on the CIRCT worker and are reached over HTTP.

## Layout

| File | Where it runs | Purpose |
|---|---|---|
| `config.py` | head | **`GITHUB_REPO` — pinned to `llvm/circt`, read by both flows** |
| `circt_issue_loop.py` | head | issue-flow driver: triage → fan-out → persist |
| `review_loop.py` | head | review-flow driver: PR feedback → fan-out → persist |
| `triage.py`, `db.py` | head | issue selection; SQLite results |
| `issue_task.py` | circt worker | `run_issue_remote` per-issue pipeline |
| `review_task.py` | circt worker | `run_review_round_remote` per-PR pipeline |
| `circt_util.py` | circt worker | flow-specific CIRCT ops (git reset/apply/diff, repro, lit-gate policy); re-exports the build/test primitives from `chia/chia/chipyard/circt.py` |
| `chia/chia/chipyard/circt.py` | circt worker (pkg) | canonical CIRCT primitives + the `BuildTool` / `LitTool` MCP tools (ships in the chia package) |
| `prompts/` | head (read) | per-phase prompts (assess/reproduce/fix/regression/writeup/review*) |
| `cluster.yaml` | — | single-machine: 2 LLM + 2 CIRCT containers |

`circt_util.py` (and the chia package itself) ship to workers via `runtime_env`
`py_modules`, so head-side edits reach workers on the next submit — no image
rebuild. The general build/test primitives it re-exports live in
`chia.chipyard.circt`.

## Run

```bash
conda env create -f env.yml          # first time
conda activate circtissues
export GITHUB_TOKEN=...               # read access to GITHUB_REPO
export CHIA_HEAD=$(hostname)          # the host to bring the cluster up on

chia up cluster.yaml                  # 2 LLM + 2 CIRCT containers on one host

# Issue flow (submit as a job so driver logs show in the dashboard):
./fix_issues_submit.sh --max-issues 2
./fix_issues_submit.sh --issue 10568             # one specific issue, skip triage
NO_WAIT=1 ./fix_issues_submit.sh --max-issues 5  # detach; watch the dashboard

# Review flow (PR number : paired issue number):
./review_submit.sh --pr 10648:7388

chia down cluster.yaml
```

`fix_issues_submit.sh` / `review_submit.sh` wrap `chia job submit` (dashboard at
http://localhost:8265, `chia job logs <id>`). Running the drivers with `python`
directly works for debugging but registers a DRIVER job whose logs the dashboard
doesn't capture. python/chia are taken from PATH (the activated env); override
with `CIRCT_SOLVER_PY` / `CIRCT_SOLVER_CHIA`.

## Notes

- **Single machine.** `cluster.yaml` puts the head + all 4 containers on one host
  (change `HOST` in head_ip and both
  `compatible_ips`). Scale up by raising the per-type and cluster-wide
  `min/max_workers` and adding IPs.
- The chia-circt image is pinned at **firtool-1.148.0**. Issues fixed upstream
  after that tag won't reproduce — the reproduce gate marks those `no_repro`. For
  a repo far ahead of the tag, the review flow's `git apply` of a PR diff onto the
  pinned tree may fail if the PR touches files changed since the tag.
- The regression gate runs the full lit suite minus baseline-red dirs (`CAPI`,
  `Tools/circt-tblgen`) — not `check-circt` (its integration tests need
  verilator/z3/sby, absent here).
- Root cause in LLVM/MLIR (the prebuilt SDK / `llvm` submodule) is out of scope —
  only CIRCT's own tree is buildable here; the agent reports such cases instead of
  hacking around them.
- Default GCS port 6379 / dashboard 8265 — bring only one chia cluster up per host
  at a time.
