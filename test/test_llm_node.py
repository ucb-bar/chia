"""Simple test: instantiate one LLM node, prompt it, print the output."""

from chia.base.ChiaFunction import ChiaFunction, get
from chia.base.llm_call import QueryResult
from chia.models.claude import ClaudeCodeLLM


LLM_ENV = "/home/ray/llm_env"

@ChiaFunction(resources={"llm": 1.0})
def test_prompt(message: str) -> QueryResult:
    import os
    os.chdir(LLM_ENV)
    llm = ClaudeCodeLLM(
        model="claude-sonnet-4-6",
        timeout_seconds=120,
    )
    return llm.prompt(message)


if __name__ == "__main__":
    print("Sending prompt to LLM node...")
    ref = test_prompt.chia_remote("/test")
    cli = get(ref)
    print(f"Success: {cli.success}")
    print(f"Response: {cli.result}")
