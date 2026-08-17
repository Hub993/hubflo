"""Construction Industry Module."""

import re

from core.industry import IndustryRequest, IndustryResult


class ConstructionIndustryModule:
    """Construction implementation of the generic Industry Module boundary."""

    name = "construction"

    def interpret(self, request: IndustryRequest) -> IndustryResult:
        """Return Construction-specific recognition without executing lifecycles."""
        if request.capability != "domain_recognition":
            return IndustryResult()

        candidate = str(
            request.context.get("candidate") or ""
        ).strip().lower()

        if candidate == "inspection":
            return self._recognize_inspection(request.text)

        if candidate == "critical_path_delay":
            return self._recognize_critical_path_delay(request.text)

        return IndustryResult()

    @staticmethod
    def _recognize_inspection(text: str) -> IndustryResult:
        t = (text or "").lower()
        handled = (
            "inspection" in t
            and ("book" in t or "schedule" in t)
        )

        return IndustryResult(
            handled=handled,
            classification="inspection" if handled else "",
            metadata=(
                {"domain": "construction", "concept": "inspection"}
                if handled
                else {}
            ),
        )

    @staticmethod
    def _recognize_critical_path_delay(text: str) -> IndustryResult:
        t = (text or "").lower().strip()

        if not t:
            return IndustryResult()

        negative_delay_patterns = [
            r"\bno\s+delay\b",
            r"\bno\s+delays\b",
            r"\bnot\s+delayed\b",
            r"\bnot\s+delay(?:ed|ing)?\b",
            r"\bwithout\s+delay\b",
            r"\bzero\s+delay\b",
            r"\b0\s+days?\s+(?:of\s+)?delay\b",
            r"\bnot\s+running\s+late\b",
        ]

        if any(
            re.search(pattern, t)
            for pattern in negative_delay_patterns
        ):
            return IndustryResult()

        positive_delay_patterns = [
            r"\bdelay(?:ed|ing|s)?\b",
            r"\brunning\s+late\b",
            r"\bbehind\s+schedule\b",
            r"\blate\s+by\b",
        ]

        handled = any(
            re.search(pattern, t)
            for pattern in positive_delay_patterns
        )

        return IndustryResult(
            handled=handled,
            classification=(
                "critical_path_delay"
                if handled
                else ""
            ),
            metadata=(
                {
                    "domain": "construction",
                    "concept": "critical_path_delay",
                }
                if handled
                else {}
            ),
        )
