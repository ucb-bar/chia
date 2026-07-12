
import warnings
from typing import List, Optional
from dataclasses import dataclass
from abc import ABC, abstractmethod
from chia.base.tools.ChiaTool import ChiaTool


# Sentinel for "argument not provided". Lets LLMCallBase tell an explicit value
# (which warrants a warning on a backend that ignores it) from the unset default.
UNSET = object()


@dataclass
class QueryResult:
    """
    Structured result from prompting an LLM or agent.
    
    :param result: The final response from the LLM or agent
    :param returncode: The returncode from running the prompt
    :param stderr: The stderr output from running the prompt (clis only)
    :param stream_result: The full transcript of all turns of the LLM or agent
    :param success: Whether the prompt completed successfully
    :type result: str
    :type returncode: int
    :type stderr: str
    :type stream_result: str
    :type success: bool
    """

    result: str
    returncode: int
    stderr: str
    stream_result: str
    success: bool = False

class LLMCallBase(ABC):
    """
    Polymorphic base container for generic LLM and agent
    configuration traits and behavior. Easy to switch between
    different backing providers, servers, and CLIs
    """

    # Capability flags — subclasses that honor these permission controls
    # override them to True. When False (the default), passing the corresponding
    # argument emits a warning that it will be ignored (see __init__).
    supports_dangerously_skip_permissions: bool = False
    supports_config: bool = False

    def __init__(
        self,
        system_message: str,
        dangerously_skip_permissions=UNSET,
        config=UNSET,
    ):
        self.system_message = system_message
        cls = type(self).__name__
        if (dangerously_skip_permissions is not UNSET
                and not self.supports_dangerously_skip_permissions):
            warnings.warn(
                f"{cls} does not support 'dangerously_skip_permissions'; the "
                f"argument is ignored (this backend has no permission gate).",
                stacklevel=2,
            )
        if config is not UNSET and not self.supports_config:
            warnings.warn(
                f"{cls} does not support a 'config' block; the "
                f"argument is ignored.",
                stacklevel=2,
            )
        # Maps ONLY to the backend's "dangerously skip permissions" CLI flag
        # (claude/opencode/antigravity --dangerously-skip-permissions, codex
        # --dangerously-bypass-approvals-and-sandbox, copilot --allow-all).
        # Honored only where supports_dangerously_skip_permissions is True.
        self.dangerously_skip_permissions = (
            True if dangerously_skip_permissions is UNSET else dangerously_skip_permissions
        )
        # The backend's config block (e.g. opencode's `permission`
        # object). ``None`` means "allow all". Honored only where
        # supports_config is True.
        self.config = None if config is UNSET else config

    @abstractmethod
    def prompt(self, user_message: str, tools: Optional[List[ChiaTool]] = []) -> QueryResult:
        """
        Send a prompt to this LLM

        :param user_message: Message used to prompt the LLM
        :param tools: Tools available to the LLM during the call
        :type user_message: str
        :type tools: Optional[List[ChiaTool]]
        """
        pass
