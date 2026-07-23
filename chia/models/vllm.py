"""vLLM self-hosted LLM preset over :class:`OpenAICompatLLM`.

`vLLM <https://docs.vllm.ai>`_ serves open-weight models on GPUs behind an
**OpenAI-compatible** Chat Completions endpoint (``vllm serve <model>`` →
``/v1`` on port 8000). From chia's point of view a self-hosted vLLM is just
another OpenAI-compatible provider: point ``base_url`` at the vLLM server and the
whole :class:`~chia.models.openai_compat.OpenAICompatLLM` loop/tool/error stack
applies unchanged — the same pattern as :class:`~chia.models.ollama.OllamaLLM`.

Default hosting to port 8200 (not vLLM's usual 8000): chia uses the low 8000s heavily — on workers
using the SSH-tunnel fallback its SSH tunnels reserve 8000-8010 (``head_tool_port``), and
ChiaTool MCP servers probe ports 8000-8099 (``start_router`` base 8000 + 100
tries; Ray Serve's proxy also defaults to 8000). Since chia runs containers
``--net=host``, vLLM on 8000 would collide. 8200 clears the tool range with
margin and stays below the Ray dashboard (8265) and all tunnel/ray ranges.

One model per server
--------------------
Unlike Ollama (which serves many models and switches per request), a vLLM server
serves **exactly one model**, fixed when ``vllm serve`` is launched. So ``model``
here must match the model the target server was started with (its HF id, or the
``--served-model-name`` if one was set). Serving multiple models means running
multiple vLLM servers/workers.

Auth
----
vLLM started without ``--api-key`` ignores the credential, but the ``openai`` SDK
refuses to build a client with an empty ``api_key``, so we default it to the
dummy ``"vllm"`` (overridable via ``api_key`` / ``token_provider`` /
``OPENAI_API_KEY`` — set a real one if the server was started with ``--api-key``).
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, List, Optional

from chia.base.ChiaFunction import ChiaFunction
from chia.base.llm_call import QueryResult
from chia.models.openai_compat import OpenAICompatLLM

if TYPE_CHECKING:
    from chia.base.tools.ChiaTool import ChiaTool


class VLLMLLM(OpenAICompatLLM):
    """Self-hosted vLLM via its OpenAI-compatible ``/v1`` endpoint.

    ``model`` is the model the target vLLM server is serving, e.g.
    ``"Qwen/Qwen2.5-3B-Instruct"`` (must match the server's launch model /
    ``--served-model-name`` — see the ``VLLM_MODEL`` knob in
    ``dockerfiles/VLLMDockerfile``).
    """

    DEFAULT_BASE_URL = "http://localhost:8200/v1"
    DEFAULT_LOGGING_NAME = "vllm"

    def __init__(self, model: str, **kwargs):
        kwargs.setdefault(
            "base_url", os.environ.get("VLLM_BASE_URL", self.DEFAULT_BASE_URL)
        )
        kwargs.setdefault("logging_name", self.DEFAULT_LOGGING_NAME)
        # vLLM (no --api-key) needs no auth, but the openai SDK requires a
        # non-empty api_key. Only inject the dummy when nothing else supplies one.
        if (
            not kwargs.get("api_key")
            and not kwargs.get("token_provider")
            and not os.environ.get("OPENAI_API_KEY")
        ):
            kwargs["api_key"] = "vllm"
        super().__init__(model=model, **kwargs)

    @ChiaFunction(resources={"vllm_creds": 0.01})
    def prompt(self, user_message: str, tools: Optional[List[ChiaTool]] = []) -> QueryResult:
        return OpenAICompatLLM.prompt(self, user_message, tools)
