"""HUBFLO Agent Layer 2.0.

The package composes intelligence and governed execution over the accepted
Stage 2 application.  Importing it registers only Agent Layer persistence
models; it does not enable a capability, grant authority, call a provider, or
alter a Stage 2 handler.
"""

from .runtime import AgentRuntime

__all__ = ["AgentRuntime"]
