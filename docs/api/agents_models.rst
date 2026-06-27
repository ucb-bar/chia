LLM servers, providers, and agent Nodes
=======================================

API reference for the LLM servers, providers, and agents library nodes in 
:mod:`chia.models`.

Base class
----------

.. automodule:: chia.base.llm_call

Providers
---------

We provide the prompt interface for each of the following providers. Many of our interfaces for these providers are lightly tested using free tokens. In particular, error handling is likely fragile.

CLIs
~~~~

Claude :mod:`chia.models.claude`

Codex :mod:`chia.models.codex`

OpenCode :mod:`chia.models.opencode`

Antigravity :mod:`chia.models.antigravity`


Remote providers
~~~~~~~~~~~~~~~~

Bedrock :mod:`chia.models.bedrock`

Vertex :mod:`chia.models.vertex`

OpenAI :mod:`chia.models.openai_providers` :mod:`chia.models.openai_compat`

OpenRouter (OpenAI compatible) :mod:`chia.models.openai_providers` :mod:`chia.models.openai_compat`

FireworkAI (OpenAI compatible) :mod:`chia.models.openai_providers` :mod:`chia.models.openai_compat`

Groq (OpenAI compatible) :mod:`chia.models.openai_providers` :mod:`chia.models.openai_compat`


On-prem servers
~~~~~~~~~~~~~~~

VLLM :mod:`chia.models.vllm`

Ollama :mod:`chia.models.ollama`
