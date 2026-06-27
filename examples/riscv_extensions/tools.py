"""Sealed MCP tools for the VEXT loop (field guide §6).

These all pin to `head_local` so their files land on the driver's disk and get
archived. The destination path is baked in at construction — the LLM only sees
the payload. The *editor* (chia.base.tools.BashTool, work_dir = chipyard) is
NOT here: it is built in the inner loop and pinned to the build placement group
because it must edit BOOM source inside the build container.

Tool surface the LLM sees:
    read_spec()                    – path(s) to the spec doc(s); open them with Read
    read_status()                  – per-instruction pass/fail from the latest run
    read_knowledge()               – durable cross-iteration notes
    append_knowledge(note)         – add a note (bug found/fixed, gotcha)
    finish(summary)                – declare the extension complete
"""

import os

from chia.base.tools.ChiaTool import ChiaTool


class SpecTool(ChiaTool):
    """Points the LLM at the extension's spec docs (specs/<ext>/) to open itself.

    read_spec returns the working-dir-relative path of every spec doc found —
    .pdf, .md, .txt, README — and the model opens each with its own Read tool: a
    PDF renders to pages natively (full fidelity, no pre-processing), text reads
    verbatim. Whichever form(s) the spec ships in — PDF only, text only, or both
    — all get listed. The files reach the LLM node through the job's working-dir
    upload; spec PDFs survive the `*.pdf` gitignore via the `!vext/specs/**/*.pdf`
    negation so they actually ship."""

    def __init__(self, name, spec_dir, spec_rel, task_options=None):
        super().__init__(name, task_options=task_options)
        self.spec_dir = spec_dir   # abs path on the head — to enumerate the docs
        self.spec_rel = spec_rel   # repo-relative — what the LLM opens in its workdir
        self.mcp.add_tool(self.read_spec, name=f"{name}_read_spec")
        super().__post_init__()

    def read_spec(self):
        """Name the ISA-extension spec doc(s) to implement; open each with your
        own Read tool (a PDF renders to its pages, .md/.txt/README read as text).
        Paths are relative to your working directory. Handles a PDF, text, or
        both."""
        if not os.path.isdir(self.spec_dir):
            return "No spec is available for this extension."
        docs = [n for n in sorted(os.listdir(self.spec_dir))
                if os.path.isfile(os.path.join(self.spec_dir, n))
                and (n.endswith((".pdf", ".md", ".txt")) or n.startswith("README"))]
        if not docs:
            return "No spec is available for this extension."
        listing = "\n".join(f"- {os.path.join(self.spec_rel, n)}" for n in docs)
        return ("The spec for this extension is in the file(s) below — open each "
                "with your Read tool (PDF renders to pages, text is verbatim):\n"
                + listing)


class StatusTool(ChiaTool):
    """Per-instruction pass/fail status, READ-ONLY. The loop recomputes it from
    the directed-test results after every build (self-checking ELFs or cospike
    match), so it is ground truth — the LLM cannot self-report. It is also the
    instruction checklist: every instruction the extension must implement, each
    marked pass / FAIL / not-yet-run."""

    def __init__(self, name, status_path, task_options=None):
        super().__init__(name, task_options=task_options)
        self.status_path = status_path
        self.mcp.add_tool(self.read_status, name=f"{name}_read_status")
        super().__post_init__()

    def read_status(self) -> str:
        """Return the per-instruction status from the latest build+test run:
        which instructions pass, which still FAIL (fix those), which haven't run."""
        if not os.path.exists(self.status_path):
            return "No directed tests have run yet."
        with open(self.status_path) as f:
            return f.read()


class KnowledgeTool(ChiaTool):
    """Durable cross-iteration scratch memory the LLM appends to."""

    def __init__(self, name, knowledge_path, task_options=None):
        super().__init__(name, task_options=task_options)
        self.knowledge_path = knowledge_path
        self.mcp.add_tool(self.read_knowledge, name=f"{name}_read_knowledge")
        self.mcp.add_tool(self.append_knowledge, name=f"{name}_append_knowledge")
        super().__post_init__()

    def read_knowledge(self) -> str:
        """Return your accumulated notes from earlier iterations."""
        if not os.path.exists(self.knowledge_path):
            return ""
        with open(self.knowledge_path) as f:
            return f.read()

    def append_knowledge(self, note: str) -> str:
        """Append a durable note (a bug's root cause + fix, a BOOM-internal
        gotcha, where an instruction's decode/exec lives). Survives iterations."""
        with open(self.knowledge_path, "a") as f:
            f.write("\n" + note.rstrip() + "\n")
        return "Noted."


class FinishTool(ChiaTool):
    """Sentinel the LLM drops to declare the extension complete; the loop
    verifies against the test results before accepting it."""

    def __init__(self, name, sentinel_path, task_options=None):
        super().__init__(name, task_options=task_options)
        self.sentinel_path = sentinel_path
        self.mcp.add_tool(self.finish, name=f"{name}_finish")
        super().__post_init__()

    def finish(self, summary: str) -> str:
        """Call when you believe every instruction in the extension is
        implemented and passing. `summary`: one paragraph on what you did."""
        os.makedirs(os.path.dirname(self.sentinel_path) or ".", exist_ok=True)
        with open(self.sentinel_path, "w") as f:
            f.write(summary)
        return "Recorded. The loop will re-verify all tests before exiting."

    def was_finished(self) -> bool:        # driver-side poll (head, same disk)
        return os.path.exists(self.sentinel_path)

    def reset(self) -> None:               # clear any prior sentinel
        if os.path.exists(self.sentinel_path):
            os.remove(self.sentinel_path)
