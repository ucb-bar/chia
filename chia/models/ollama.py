"""Ollama self-hosted LLM preset over :class:`OpenAICompatLLM`.

`Ollama <https://ollama.com>`_ serves open-weight models locally behind an
**OpenAI-compatible** Chat Completions endpoint at ``/v1``. From chia's point of
view a self-hosted Ollama is therefore just another OpenAI-compatible provider:
point ``base_url`` at the Ollama server and the entire
:class:`~chia.models.openai_compat.OpenAICompatLLM` loop/tool/error stack
applies unchanged. This is the same pattern as the cloud presets in
:mod:`chia.models.openai_providers`.

Auth
----
Ollama requires no credentials, but the ``openai`` SDK refuses to build a client
with an empty ``api_key``. We therefore default it to the conventional dummy
value ``"ollama"`` (overridable via ``api_key`` / ``token_provider`` /
``OPENAI_API_KEY``).
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, List, Optional

from chia.base.ChiaFunction import ChiaFunction
from chia.base.llm_call import QueryResult
from chia.models.openai_compat import OpenAICompatLLM

if TYPE_CHECKING:
    from chia.base.tools.ChiaTool import ChiaTool


class OllamaLLM(OpenAICompatLLM):
    """Self-hosted Ollama via its OpenAI-compatible ``/v1`` endpoint.

    ``model`` is the Ollama model tag to serve, e.g. ``"llama3.1:8b"`` or
    ``"qwen2.5:7b"`` (it must already be pulled on the server — see the
    ``OLLAMA_PULL`` build/run knob in ``dockerfiles/OllamaDockerfile``).
    """

    DEFAULT_BASE_URL = "http://localhost:11434/v1"
    DEFAULT_LOGGING_NAME = "ollama"

    def __init__(self, model: str, **kwargs):
        kwargs.setdefault(
            "base_url", os.environ.get("OLLAMA_BASE_URL", self.DEFAULT_BASE_URL)
        )
        kwargs.setdefault("logging_name", self.DEFAULT_LOGGING_NAME)
        # Ollama needs no auth, but the openai SDK requires a non-empty api_key.
        # Only inject the dummy when the caller and environment supply nothing.
        if (
            not kwargs.get("api_key")
            and not kwargs.get("token_provider")
            and not os.environ.get("OPENAI_API_KEY")
        ):
            kwargs["api_key"] = "ollama"
        super().__init__(model=model, **kwargs)

    @ChiaFunction(resources={"ollama_creds": 0.01})
    def prompt(self, user_message: str, tools: Optional[List[ChiaTool]] = []) -> QueryResult:
        return OpenAICompatLLM.prompt(self, user_message, tools)
