Model Backends
==============

**NOTE:** We provide a single function to interface with each of these providers even though some are agents, some are LLM providers, and some are on-premises LLM servers. This interface is an agentic one, and for the non-agents, we turn the model into a (very primitive) agent, by placing it into a query->tool call->query loop. 

We plan to in the near future expose an interface to the non-agent models (providers and servers), which is, instead of a primitive agent, the interface you would use to build your own agents.

For most serious tasks that don't require on-premises LLM serving, we expect you will get better results using the agents (Claude Code, Codex, Antigravity, or OpenCode), with your preferred provider for credentials for the agent, as opposed to using the specific node for your provider (e.g. Claude Code with Bedrock credentials instead of the bedrock node).

API reference for :mod:`chia.models`. These pages are generated from the docstrings in the source, so they stay in sync with the code.

Claude
------

.. automodule:: chia.models.claude

Bedrock
-------

.. automodule:: chia.models.bedrock

Vertex
------

.. automodule:: chia.models.vertex

Antigravity
-----------

.. automodule:: chia.models.antigravity

Codex
-----

.. automodule:: chia.models.codex

Opencode
--------

.. automodule:: chia.models.opencode

Openai Compat
-------------

.. automodule:: chia.models.openai_compat

Openai Providers
----------------

.. automodule:: chia.models.openai_providers

Ollama
------

.. automodule:: chia.models.ollama

vLLM
----

.. automodule:: chia.models.vllm
