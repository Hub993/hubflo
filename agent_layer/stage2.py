"""Narrow composition adapter for accepted Stage 2 capabilities.

This module owns no recognition, authorization, lifecycle, business mutation or
Industry Module.  The application may inject an already-authoritative handler
and its outcome inspector into `AgentRuntime.register_handler`.
"""

from typing import Mapping


class Stage2CapabilityAdapter:
    def __init__(self, authoritative_handler, outcome_inspector):
        self._handler = authoritative_handler
        self._outcome_inspector = outcome_inspector

    def execute(self, payload: Mapping):
        return self._handler(payload)

    def inspect_outcome(self, invocation):
        return self._outcome_inspector(invocation)
