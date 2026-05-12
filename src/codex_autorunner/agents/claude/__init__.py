"""Claude Code agent integration for CAR.

The Claude harness wraps the ``claude`` CLI (Anthropic's Claude Code) via its
``--print --output-format=stream-json`` mode. Each turn spawns one subprocess;
session persistence is delegated to Claude's on-disk session storage under
``~/.claude/projects/``.
"""

from .harness import CLAUDE_CAPABILITIES, CLAUDE_RUNTIME_ID, ClaudeHarness
from .supervisor import (
    ClaudeRuntimePreflightResult,
    ClaudeSessionHandle,
    ClaudeSupervisor,
    ClaudeSupervisorError,
    build_claude_supervisor_from_config,
    claude_binary_available,
    claude_runtime_preflight,
)

__all__ = [
    "CLAUDE_CAPABILITIES",
    "CLAUDE_RUNTIME_ID",
    "ClaudeHarness",
    "ClaudeRuntimePreflightResult",
    "ClaudeSessionHandle",
    "ClaudeSupervisor",
    "ClaudeSupervisorError",
    "build_claude_supervisor_from_config",
    "claude_binary_available",
    "claude_runtime_preflight",
]
