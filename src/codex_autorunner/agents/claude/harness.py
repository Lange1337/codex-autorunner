"""Claude Code harness: AgentHarness implementation backed by a ClaudeSupervisor."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, AsyncIterator, Optional

from ...core.text_utils import _normalize_optional_text
from ..base import AgentHarness
from ..types import (
    AgentId,
    ConversationRef,
    ModelCatalog,
    ModelSpec,
    RuntimeCapability,
    RuntimeCapabilityReport,
    TerminalTurnResult,
    TurnRef,
)
from .supervisor import ClaudeSessionHandle, ClaudeSupervisor

_logger = logging.getLogger(__name__)

CLAUDE_RUNTIME_ID = "claude"

CLAUDE_CAPABILITIES: frozenset[RuntimeCapability] = frozenset(
    [
        RuntimeCapability("durable_threads"),
        RuntimeCapability("message_turns"),
        RuntimeCapability("interrupt"),
        RuntimeCapability("active_thread_discovery"),
        RuntimeCapability("event_streaming"),
        RuntimeCapability("model_listing"),
    ]
)

# Claude Code's well-known model aliases. The catalog is static here because
# Claude's CLI does not advertise its model list over the wire; we accept any
# string anyway (the CLI validates on launch).
_CLAUDE_MODELS: tuple[ModelSpec, ...] = (
    ModelSpec(
        id="sonnet",
        display_name="Claude Sonnet (latest)",
        supports_reasoning=False,
        reasoning_options=[],
    ),
    ModelSpec(
        id="opus",
        display_name="Claude Opus (latest)",
        supports_reasoning=False,
        reasoning_options=[],
    ),
    ModelSpec(
        id="haiku",
        display_name="Claude Haiku (latest)",
        supports_reasoning=False,
        reasoning_options=[],
    ),
)
_CLAUDE_DEFAULT_MODEL = "sonnet"


class ClaudeHarness(AgentHarness):
    agent_id: AgentId = AgentId(CLAUDE_RUNTIME_ID)
    display_name = "Claude"
    capabilities = CLAUDE_CAPABILITIES

    def __init__(self, supervisor: ClaudeSupervisor) -> None:
        self._supervisor = supervisor

    # ----- discovery -----

    async def ensure_ready(self, workspace_root: Path) -> None:
        await self._supervisor.ensure_ready(workspace_root)

    async def runtime_capability_report(
        self, workspace_root: Path
    ) -> RuntimeCapabilityReport:
        _ = workspace_root
        return RuntimeCapabilityReport(capabilities=self.capabilities)

    async def model_catalog(self, workspace_root: Path) -> ModelCatalog:
        _ = workspace_root
        return ModelCatalog(
            default_model=_CLAUDE_DEFAULT_MODEL,
            models=list(_CLAUDE_MODELS),
        )

    # ----- conversation lifecycle -----

    def _conversation_ref(self, session: ClaudeSessionHandle) -> ConversationRef:
        return ConversationRef(
            agent=self.agent_id,
            id=session.session_id,
            title=_normalize_optional_text(session.title),
            summary=_normalize_optional_text(session.summary),
        )

    async def new_conversation(
        self, workspace_root: Path, title: Optional[str] = None
    ) -> ConversationRef:
        session = await self._supervisor.create_session(workspace_root, title=title)
        return self._conversation_ref(session)

    async def list_conversations(self, workspace_root: Path) -> list[ConversationRef]:
        sessions = await self._supervisor.list_sessions(workspace_root)
        return [self._conversation_ref(session) for session in sessions]

    async def resume_conversation(
        self, workspace_root: Path, conversation_id: str
    ) -> ConversationRef:
        session = await self._supervisor.resume_session(workspace_root, conversation_id)
        return self._conversation_ref(session)

    # ----- turn lifecycle -----

    async def start_turn(
        self,
        workspace_root: Path,
        conversation_id: str,
        prompt: str,
        model: Optional[str],
        reasoning: Optional[str],
        *,
        approval_mode: Optional[str],
        sandbox_policy: Optional[Any],
        input_items: Optional[list[dict[str, Any]]] = None,
    ) -> TurnRef:
        # Claude CLI does not consume CAR's reasoning/sandbox/input_items shape
        # directly. Reasoning is implicit in the model choice; sandbox is the
        # caller's responsibility (the binary itself does not sandbox).
        _ = reasoning, sandbox_policy, input_items
        turn_id = await self._supervisor.start_turn(
            workspace_root,
            conversation_id,
            prompt,
            model=model,
            approval_mode=approval_mode,
        )
        return TurnRef(conversation_id=conversation_id, turn_id=turn_id)

    async def wait_for_turn(
        self,
        workspace_root: Path,
        conversation_id: str,
        turn_id: Optional[str],
        *,
        timeout: Optional[float] = None,
    ) -> TerminalTurnResult:
        resolved_turn_id = str(turn_id or "").strip()
        if not resolved_turn_id:
            raise ValueError("Claude wait_for_turn requires a turn id")
        return await self._supervisor.wait_for_turn(
            workspace_root,
            conversation_id,
            resolved_turn_id,
            timeout=timeout,
        )

    async def interrupt(
        self, workspace_root: Path, conversation_id: str, turn_id: Optional[str]
    ) -> None:
        await self._supervisor.interrupt_turn(workspace_root, conversation_id, turn_id)

    async def stream_events(
        self, workspace_root: Path, conversation_id: str, turn_id: str
    ) -> AsyncIterator[dict[str, Any]]:
        async for event in self._supervisor.stream_turn_events(
            workspace_root, conversation_id, turn_id
        ):
            yield event

    async def list_progress_events(
        self, conversation_id: str, turn_id: str, **kwargs: Any
    ) -> list[dict[str, Any]]:
        _ = conversation_id, kwargs
        return await self._supervisor.list_turn_events_snapshot(turn_id)


__all__ = [
    "CLAUDE_CAPABILITIES",
    "CLAUDE_RUNTIME_ID",
    "ClaudeHarness",
]
