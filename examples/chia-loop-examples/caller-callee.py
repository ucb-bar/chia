import ray

from chia.base.ChiaFunction import get
from chia.base.tools.ChiaTool import ChiaTool
from chia.models.claude import ClaudeCodeLLM


class PersistentSubagentTool(ChiaTool):
    def setup(self):
        self.subagent = ClaudeCodeLLM(
            logging_name="claude_subagent",
            resume_session=True,
            projects_cwd=None,
        )
        self.mcp.add_tool(self.ask_subagent, name="ask_subagent")

    def ask_subagent(self, task: str) -> str:
        """Ask the persistent Claude subagent to complete a task."""
        ref = self.subagent.prompt.chia_remote(self.subagent, task)
        result = get(ref)
        return result.result


def main():
    ray.init(address="auto")

    subagent_tool = PersistentSubagentTool(
        "subagent",
        task_options={"num_cpus": 0.1},
    )
    main_agent = ClaudeCodeLLM(
        logging_name="claude_main",
        system_message=(
            "You are the persistent main agent. "
            "Delegate work using ask_subagent."
        ),
        resume_session=True,
        projects_cwd=None,
    )

    try:
        prompts = [
            (
                "Call ask_subagent and tell it to remember that its private "
                "codeword is ORBIT. You must remember that your codeword is COMET."
            ),
            (
                "Call ask_subagent and ask it for its private codeword. "
                "Then report both its codeword and yours."
            ),
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
