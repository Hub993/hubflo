"""Industry-neutral Core Conversation orchestration."""

from core.industry import IndustryModule, IndustryRequest, IndustryResult


class CoreConversation:
    """Delegate structured interpretation through an injected Industry Module."""

    def __init__(self, industry: IndustryModule):
        self._industry = industry

    def interpret(self, request: IndustryRequest) -> IndustryResult:
        """Return the structured Industry Module interpretation unchanged."""
        return self._industry.interpret(request)
