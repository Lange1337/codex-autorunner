"""OpenCode server integration (HTTP + SSE).

Codex and Hermes harnesses consume supervisor-managed turn event streams. This
agent talks to ``opencode serve`` directly: raw SSE (sometimes wrapped JSON;
see ``_normalize_sse_event`` in ``client``), relevance filtering in
``opencode_event_is_progress_signal`` in ``runtime``, and optional
``list_messages`` polling on ``OpenCodeHarness`` when the SSE tail is quiet.
"""

from .client import OpenCodeClient
from .harness import OpenCodeHarness
from .run_prompt import OpenCodeRunConfig, OpenCodeRunResult, run_opencode_prompt
from .supervisor import OpenCodeSupervisor

__all__ = [
    "OpenCodeClient",
    "OpenCodeHarness",
    "OpenCodeRunConfig",
    "OpenCodeRunResult",
    "OpenCodeSupervisor",
    "run_opencode_prompt",
]
