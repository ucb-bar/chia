import ray

from chia.base.ChiaFunction import get
from chia.base.tools.ChiaTool import ChiaTool
from chia.models.claude import ClaudeCodeLLM


class SubagentTool(ChiaTool):
    def setup(self):
        self.mcp.add_tool(self.ask_subagent, name="ask_subagent")

    def ask_subagent(self, task: str) -> str:
        """Ask a fresh, stateless Claude subagent to complete a task."""
        subagent = ClaudeCodeLLM(
            logging_name="claude_subagent",
            resume_session=False,
            projects_cwd=None,
        )
        result = subagent.prompt.chia_remote_blocking(subagent, task)
        return result.result


def main():
    ray.init(address="auto")

    subagent_tool = SubagentTool(
        "subagent",
        task_options={"num_cpus": 0.1},
    )
    main_agent = ClaudeCodeLLM(
        logging_name="claude_main",
        system_message=(
            "You are the persistent main agent. "
            "Delegate independent work using ask_subagent."
        ),
        resume_session=True,
        projects_cwd=None,
    )

    try:
        prompts = [
            "Ask the subagent for a project idea. Choose one and remember it.",
            "Recall your choice. Ask a fresh subagent to critique it, then refine it.",
        ]

        for prompt in prompts:
            ref = main_agent.prompt.chia_remote(
                main_agent,
                prompt,
                tools=[subagent_tool],
            )
            result = get(ref)
            print("Main:", result.result)
    finally:
        subagent_tool.stop()
        ray.shutdown()


if __name__ == "__main__":
    main()
