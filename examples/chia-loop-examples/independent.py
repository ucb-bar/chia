import ray

from chia.base.ChiaFunction import get
from chia.models.claude import ClaudeCodeLLM


def main():
    ray.init(address="auto")

    claude_a = ClaudeCodeLLM(
        logging_name="claude_a",
        system_message="You are worker A.",
        resume_session=True,
        projects_cwd=None,
    )
    claude_b = ClaudeCodeLLM(
        logging_name="claude_b",
        system_message="You are worker B.",
        resume_session=True,
        projects_cwd=None,
    )

    prompts_a = [
        "Remember that your private codeword is ALPHA.",
        "What is your private codeword?",
    ]
    prompts_b = [
        "Remember that your private codeword is BETA.",
        "What is your private codeword?",
    ]

    for prompt_a, prompt_b in zip(prompts_a, prompts_b):
        ref_a = claude_a.prompt.chia_remote(claude_a, prompt_a)
        ref_b = claude_b.prompt.chia_remote(claude_b, prompt_b)

        result_a = get(ref_a)
        result_b = get(ref_b)

        print("A:", result_a.result)
        print("B:", result_b.result)

    ray.shutdown()


if __name__ == "__main__":
    main()
