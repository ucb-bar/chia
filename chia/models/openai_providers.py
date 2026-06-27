"""Lightweight per-provider presets over :class:`OpenAICompatLLM`.

Each big OpenAI-compatible provider differs only in its endpoint (``base_url``),
a default logging name, and the Ray resource its calls require. The providers
are thin subclasses that set the first two as class defaults and **re-decorate
``prompt``** with their own ``@ChiaFunction(resources=...)`` so that, when
dispatched via ``prompt.chia_remote(self, ...)``, each lands only on workers
advertising that provider's credential resource. The decorated body just defers
to :meth:`OpenAICompatLLM.prompt`.

Auth is environment-driven (see :class:`OpenAICompatLLM`). The SDK only reads
``OPENAI_API_KEY`` automatically — for non-OpenAI providers export the provider
key as ``OPENAI_API_KEY``, or pass ``api_key=`` / a ``token_provider``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

from chia.base.ChiaFunction import ChiaFunction
from chia.base.llm_call import QueryResult
from chia.models.openai_compat import OpenAICompatLLM

if TYPE_CHECKING:
    from chia.base.tools.ChiaTool import ChiaTool


class _OpenAICompatProvider(OpenAICompatLLM):
    """Base for the provider presets: applies endpoint/logging defaults.

    Deliberately does NOT define ``prompt`` — each concrete provider
    re-decorates it so its own resource is captured, and the body defers to
    :meth:`OpenAICompatLLM.prompt`.
    """

    DEFAULT_BASE_URL: Optional[str] = None  # None -> OpenAI default endpoint
    DEFAULT_LOGGING_NAME: str = "openai_compat_llm"

    def __init__(self, model: str, **kwargs):
        kwargs.setdefault("base_url", self.DEFAULT_BASE_URL)
        kwargs.setdefault("logging_name", self.DEFAULT_LOGGING_NAME)
        super().__init__(model=model, **kwargs)


class OpenAILLM(_OpenAICompatProvider):
    """OpenAI itself (default endpoint)."""

    DEFAULT_BASE_URL = None
    DEFAULT_LOGGING_NAME = "openai"

    @ChiaFunction(resources={"openai_creds": 0.01})
    def prompt(self, user_message: str, tools: Optional[List[ChiaTool]] = []) -> QueryResult:
        return OpenAICompatLLM.prompt(self, user_message, tools)


class FireworksLLM(_OpenAICompatProvider):
    """Fireworks AI."""

    DEFAULT_BASE_URL = "https://api.fireworks.ai/inference/v1"
    DEFAULT_LOGGING_NAME = "fireworks"

    @ChiaFunction(resources={"fireworks_creds": 0.01})
    def prompt(self, user_message: str, tools: Optional[List[ChiaTool]] = []) -> QueryResult:
        return OpenAICompatLLM.prompt(self, user_message, tools)


class GroqLLM(_OpenAICompatProvider):
    """Groq."""

    DEFAULT_BASE_URL = "https://api.groq.com/openai/v1"
    DEFAULT_LOGGING_NAME = "groq"

    @ChiaFunction(resources={"groq_creds": 0.01})
    def prompt(self, user_message: str, tools: Optional[List[ChiaTool]] = []) -> QueryResult:
        return OpenAICompatLLM.prompt(self, user_message, tools)


class OpenRouterLLM(_OpenAICompatProvider):
    """OpenRouter (itself a multi-provider router)."""

    DEFAULT_BASE_URL = "https://openrouter.ai/api/v1"
    DEFAULT_LOGGING_NAME = "openrouter"

    @ChiaFunction(resources={"openrouter_creds": 0.01})
    def prompt(self, user_message: str, tools: Optional[List[ChiaTool]] = []) -> QueryResult:
        return OpenAICompatLLM.prompt(self, user_message, tools)
