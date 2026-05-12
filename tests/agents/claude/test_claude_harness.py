from __future__ import annotations

from pathlib import Path
from typing import Any, AsyncIterator, Optional

import pytest

from codex_autorunner.agents.claude.harness import (
    CLAUDE_CAPABILITIES,
    CLAUDE_RUNTIME_ID,
    ClaudeHarness,
)
from codex_autorunner.agents.claude.supervisor import ClaudeSessionHandle
from codex_autorunner.agents.registry import _BUILTIN_AGENTS
from codex_autorunner.agents.types import TerminalTurnResult


class _StubSupervisor:
    def __init__(self) -> None:
        self.created: list[tuple[Path, Optional[str]]] = []
        self.resumed: list[tuple[Path, str]] = []
        self.started: list[tuple[Path, str, str, Optional[str], Optional[str]]] = []
        self.waited: list[tuple[Path, str, str, Optional[float]]] = []
        self.interrupted: list[tuple[Path, str, Optional[str]]] = []
        self.streamed: list[tuple[Path, str, str]] = []
        self.ready_workspace: Optional[Path] = None
        self.snapshot_turn_ids: list[str] = []

    async def ensure_ready(self, workspace_root: Path) -> None:
        self.ready_workspace = workspace_root

    async def create_session(
        self,
        workspace_root: Path,
        *,
        title: Optional[str] = None,
    ) -> ClaudeSessionHandle:
        self.created.append((workspace_root, title))
        return ClaudeSessionHandle(
            session_id="claude-session-1",
            title=title,
            summary=None,
            raw={},
        )

    async def resume_session(
        self, workspace_root: Path, session_id: str
    ) -> ClaudeSessionHandle:
        self.resumed.append((workspace_root, session_id))
        return ClaudeSessionHandle(
            session_id=session_id,
            title="Resumed",
            summary="resumed summary",
            raw={"first_user_message": "hi"},
        )

    async def list_sessions(self, workspace_root: Path) -> list[ClaudeSessionHandle]:
        return [
            ClaudeSessionHandle(session_id="claude-session-1", title="One"),
            ClaudeSessionHandle(session_id="claude-session-2", title="Two"),
        ]

    async def start_turn(
        self,
        workspace_root: Path,
        session_id: str,
        prompt: str,
        *,
        model: Optional[str] = None,
        approval_mode: Optional[str] = None,
        extra_args: Any = (),
    ) -> str:
        _ = extra_args
        self.started.append((workspace_root, session_id, prompt, model, approval_mode))
        return "turn-1"

    async def wait_for_turn(
        self,
        workspace_root: Path,
        session_id: str,
        turn_id: str,
        *,
        timeout: Optional[float] = None,
    ) -> TerminalTurnResult:
        self.waited.append((workspace_root, session_id, turn_id, timeout))
        return TerminalTurnResult(
            status="ok",
            assistant_text="hello",
            errors=[],
            raw_events=[{"type": "result", "subtype": "success"}],
        )

    async def interrupt_turn(
        self,
        workspace_root: Path,
        session_id: str,
        turn_id: Optional[str],
    ) -> None:
        self.interrupted.append((workspace_root, session_id, turn_id))

    async def stream_turn_events(
        self,
        workspace_root: Path,
        session_id: str,
        turn_id: str,
    ) -> AsyncIterator[dict[str, Any]]:
        self.streamed.append((workspace_root, session_id, turn_id))
        for event in ({"message": {"method": "claude.system"}},):
            yield event

    async def list_turn_events_snapshot(self, turn_id: str) -> list[dict[str, Any]]:
        self.snapshot_turn_ids.append(turn_id)
        return [{"message": {"method": "claude.user"}}]


@pytest.mark.asyncio
async def test_harness_round_trip_through_stub_supervisor(tmp_path: Path) -> None:
    sup = _StubSupervisor()
    harness = ClaudeHarness(sup)

    await harness.ensure_ready(tmp_path)
    assert sup.ready_workspace == tmp_path

    conv = await harness.new_conversation(tmp_path, title="hello")
    assert conv.agent == CLAUDE_RUNTIME_ID
    assert conv.id == "claude-session-1"
    assert conv.title == "hello"

    listed = await harness.list_conversations(tmp_path)
    assert [c.id for c in listed] == ["claude-session-1", "claude-session-2"]

    resumed = await harness.resume_conversation(tmp_path, "claude-session-9")
    assert resumed.id == "claude-session-9"
    assert resumed.title == "Resumed"

    turn = await harness.start_turn(
        tmp_path,
        conv.id,
        "list the contracts",
        model="sonnet",
        reasoning=None,
        approval_mode="trusted_mode",
        sandbox_policy=None,
        input_items=None,
    )
    assert turn.conversation_id == "claude-session-1"
    assert turn.turn_id == "turn-1"
    assert sup.started[-1] == (
        tmp_path,
        "claude-session-1",
        "list the contracts",
        "sonnet",
        "trusted_mode",
    )

    result = await harness.wait_for_turn(tmp_path, conv.id, turn.turn_id, timeout=30.0)
    assert result.status == "ok"
    assert result.assistant_text == "hello"

    events: list[dict[str, Any]] = []
    async for event in harness.stream_events(tmp_path, conv.id, turn.turn_id):
        events.append(event)
    assert events == [{"message": {"method": "claude.system"}}]

    await harness.interrupt(tmp_path, conv.id, turn.turn_id)
    assert sup.interrupted[-1] == (tmp_path, "claude-session-1", "turn-1")


def test_claude_is_registered_with_expected_capabilities() -> None:
    descriptor = _BUILTIN_AGENTS["claude"]
    assert descriptor.id == "claude"
    assert descriptor.name == "Claude"
    assert descriptor.runtime_kind == "claude"
    # Sanity: capabilities frozen set matches the harness module declaration.
    assert descriptor.capabilities == CLAUDE_CAPABILITIES


@pytest.mark.asyncio
async def test_model_catalog_returns_static_aliases(tmp_path: Path) -> None:
    sup = _StubSupervisor()
    harness = ClaudeHarness(sup)
    catalog = await harness.model_catalog(tmp_path)
    ids = [spec.id for spec in catalog.models]
    assert "sonnet" in ids and "opus" in ids and "haiku" in ids
    assert catalog.default_model == "sonnet"


@pytest.mark.asyncio
async def test_wait_for_turn_requires_turn_id(tmp_path: Path) -> None:
    harness = ClaudeHarness(_StubSupervisor())
    with pytest.raises(ValueError):
        await harness.wait_for_turn(tmp_path, "session", None)
