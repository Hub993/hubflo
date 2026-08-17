"""Construction Industry Module foundation."""

from core.industry import IndustryRequest, IndustryResult


class ConstructionIndustryModule:
    """Construction implementation of the generic Industry Module boundary."""

    name = "construction"

    def interpret(self, request: IndustryRequest) -> IndustryResult:
        """Patch 1 foundation only; Construction interpretation is wired in Patch 2."""
        return IndustryResult()
