# CHIA Loop Examples

Minimal examples of Claude agent relationships and conversation persistence in CHIA.

| Example | Description |
|---|---|
| `independent.py` | Two independent, persistent Claude nodes run concurrently. |
| `temporary-subagent.py` | A persistent caller delegates to a stateless callee. |
| `caller-callee.py` | A persistent caller delegates to a persistent callee. |

## Persistence

A persistent agent reuses one `ClaudeCodeLLM` instance with `resume_session=True`. Calling `get()` after each prompt synchronizes its transcript for the next turn.

## Requirements

- A running CHIA/Ray cluster
- Claude Code installed and authenticated on Claude workers
- At least `0.02` available `claude_creds` capacity for caller/callee examples
