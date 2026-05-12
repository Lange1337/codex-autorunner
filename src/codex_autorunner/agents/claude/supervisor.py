"""Claude session/turn supervisor.

Responsibilities:
  * Generate / track Claude session UUIDs (used as ``conversation_id``).
  * Spawn one ``claude --print`` subprocess per turn (via ``client``).
  * Drive the stream-json line iterator into a TurnEventBuffer so surfaces can
    consume progress + final result identically to the other harnesses.
  * Expose interrupt (SIGTERM) on the in-flight turn.
  * Enumerate persisted sessions by reading the Claude project directory.

Unlike hermes/opencode there is no long-lived agent process to supervise: each
turn is a fresh subprocess. The supervisor is mostly bookkeeping.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator, Iterable, Mapping, Optional, Sequence

from ...core.logging_utils import log_event
from ...core.orchestration.turn_event_buffer import TurnEventBuffer
from ...core.text_utils import _normalize_optional_text
from ..types import TerminalTurnResult
from . import event_decoder
from .client import ClaudeLaunchSpec, ClaudeProcessError, ClaudeProcessHandle, spawn_claude

_logger = logging.getLogger(__name__)

CLAUDE_RUNTIME_ID = "claude"
DEFAULT_TURN_TIMEOUT_SECONDS = 3600.0


@dataclass(frozen=True)
class ClaudeSessionHandle:
    """Plain-text view of a Claude conversation."""

    session_id: str
    title: Optional[str] = None
    summary: Optional[str] = None
    raw: dict[str, Any] = field(default_factory=dict)


class ClaudeSupervisorError(RuntimeError):
    pass


@dataclass
class _ClaudeTurnState:
    session_id: str
    turn_id: str
    handle: ClaudeProcessHandle
    event_buffer: TurnEventBuffer = field(default_factory=TurnEventBuffer)
    pump_task: Optional[asyncio.Task[None]] = None
    assistant_text_chunks: list[str] = field(default_factory=list)
    final_result_text: Optional[str] = None
    errors: list[str] = field(default_factory=list)
    raw_events: list[dict[str, Any]] = field(default_factory=list)
    terminal_status: Optional[str] = None
    closed: bool = False


def _encode_workspace_for_claude_projects(workspace_root: Path) -> str:
    """Mirror Claude Code's project-dir naming: leading and embedded ``/`` → ``-``.

    e.g. ``/home/kell/car-hub`` → ``-home-kell-car-hub``.
    """
    return str(workspace_root).replace("/", "-")


def _claude_projects_root() -> Path:
    return Path.home() / ".claude" / "projects"


class ClaudeSupervisor:
    """Tracks Claude sessions/turns and dispatches subprocess invocations."""

    def __init__(
        self,
        binary: str,
        *,
        base_env: Optional[Mapping[str, str]] = None,
        default_model: Optional[str] = None,
        default_permission_mode: Optional[str] = None,
        turn_timeout_seconds: float = DEFAULT_TURN_TIMEOUT_SECONDS,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        if not binary:
            raise ValueError("Claude binary must not be empty")
        self._binary = binary
        self._base_env = dict(base_env or {})
        self._default_model = _normalize_optional_text(default_model)
        self._default_permission_mode = _normalize_optional_text(default_permission_mode)
        self._turn_timeout_seconds = max(float(turn_timeout_seconds or 0.0), 60.0)
        self._logger = logger or _logger
        # Session id → on-disk indicator (we never load the file unless asked).
        self._known_sessions: set[str] = set()
        # (session_id, turn_id) → state.
        self._turn_states: dict[tuple[str, str], _ClaudeTurnState] = {}
        # session_id → in-flight turn_id (only one in-flight turn per session).
        self._session_active_turn: dict[str, str] = {}
        self._lock = asyncio.Lock()

    @property
    def binary(self) -> str:
        return self._binary

    async def ensure_ready(self, workspace_root: Path) -> None:
        """Verify the binary is invocable. Cheap probe — no long-lived process."""
        _ = workspace_root
        # We deliberately do NOT run ``claude --version`` here on every readiness
        # check; that would balloon latency. The runtime preflight in the
        # registry handles that once at startup.
        return None

    # ----- session lifecycle -----

    async def create_session(
        self,
        workspace_root: Path,
        *,
        title: Optional[str] = None,
    ) -> ClaudeSessionHandle:
        session_id = str(uuid.uuid4())
        async with self._lock:
            self._known_sessions.add(session_id)
        log_event(
            self._logger,
            logging.INFO,
            "claude.session.created",
            session_id=session_id,
            workspace_root=str(workspace_root),
            title=title,
        )
        return ClaudeSessionHandle(
            session_id=session_id,
            title=_normalize_optional_text(title),
        )

    async def resume_session(
        self,
        workspace_root: Path,
        session_id: str,
    ) -> ClaudeSessionHandle:
        sid = (session_id or "").strip()
        if not sid:
            raise ClaudeSupervisorError("Cannot resume Claude session: id is empty")
        async with self._lock:
            self._known_sessions.add(sid)
        title: Optional[str] = None
        summary: Optional[str] = None
        raw: dict[str, Any] = {}
        session_path = self._session_jsonl_path(workspace_root, sid)
        if session_path is not None and session_path.exists():
            title, summary, raw = _read_session_metadata(session_path)
        return ClaudeSessionHandle(
            session_id=sid,
            title=title,
            summary=summary,
            raw=raw,
        )

    async def list_sessions(self, workspace_root: Path) -> list[ClaudeSessionHandle]:
        project_dir = self._project_dir(workspace_root)
        if project_dir is None or not project_dir.exists():
            return []
        handles: list[ClaudeSessionHandle] = []
        for entry in sorted(project_dir.glob("*.jsonl")):
            sid = entry.stem
            if not _looks_like_uuid(sid):
                continue
            title, summary, raw = _read_session_metadata(entry)
            handles.append(
                ClaudeSessionHandle(
                    session_id=sid,
                    title=title,
                    summary=summary,
                    raw=raw,
                )
            )
        return handles

    def _project_dir(self, workspace_root: Path) -> Optional[Path]:
        encoded = _encode_workspace_for_claude_projects(workspace_root)
        if not encoded:
            return None
        return _claude_projects_root() / encoded

    def _session_jsonl_path(
        self, workspace_root: Path, session_id: str
    ) -> Optional[Path]:
        project_dir = self._project_dir(workspace_root)
        if project_dir is None:
            return None
        return project_dir / f"{session_id}.jsonl"

    # ----- turn lifecycle -----

    async def start_turn(
        self,
        workspace_root: Path,
        session_id: str,
        prompt: str,
        *,
        model: Optional[str] = None,
        approval_mode: Optional[str] = None,
        extra_args: Sequence[str] = (),
    ) -> str:
        if not prompt or not prompt.strip():
            raise ClaudeSupervisorError("Claude turn requires a non-empty prompt")
        sid = (session_id or "").strip()
        if not sid:
            raise ClaudeSupervisorError("Claude turn requires a session id")
        spec = ClaudeLaunchSpec(
            binary=self._binary,
            session_id=sid,
            prompt=prompt,
            workspace_root=workspace_root,
            model=_normalize_optional_text(model) or self._default_model,
            permission_mode=(
                _map_approval_mode(approval_mode) or self._default_permission_mode
            ),
            extra_args=tuple(extra_args),
            env=self._base_env,
        )
        try:
            handle = await spawn_claude(spec)
        except ClaudeProcessError as exc:
            raise ClaudeSupervisorError(str(exc)) from exc

        turn_id = str(uuid.uuid4())
        state = _ClaudeTurnState(session_id=sid, turn_id=turn_id, handle=handle)
        async with self._lock:
            self._turn_states[(sid, turn_id)] = state
            self._session_active_turn[sid] = turn_id
            self._known_sessions.add(sid)

        state.pump_task = asyncio.create_task(self._pump_events(state))
        log_event(
            self._logger,
            logging.INFO,
            "claude.turn.started",
            session_id=sid,
            turn_id=turn_id,
            pid=handle.pid,
        )
        return turn_id

    async def _pump_events(self, state: _ClaudeTurnState) -> None:
        """Read the subprocess stdout stream and translate it into envelopes."""
        try:
            async for line in state.handle.iter_stdout_lines():
                event = event_decoder.decode_stream_json_line(line)
                if event is None:
                    continue
                state.raw_events.append(event)
                envelope = event_decoder.to_envelope(event)
                await state.event_buffer.append(envelope)

                if event_decoder.event_kind(event) == "assistant":
                    text = event_decoder.extract_assistant_delta_text(event)
                    if text:
                        state.assistant_text_chunks.append(text)
                if event_decoder.is_terminal(event):
                    state.final_result_text = event_decoder.extract_result_text(event)
                    state.terminal_status = (
                        "error" if event_decoder.is_error_result(event) else "ok"
                    )
                    err = _extract_terminal_error_message(event)
                    if err:
                        state.errors.append(err)
            rc = await state.handle.wait()
            if rc != 0 and state.terminal_status is None:
                state.terminal_status = "error"
                stderr = state.handle.stderr_text()
                if stderr.strip():
                    state.errors.append(stderr.strip())
                else:
                    state.errors.append(f"claude exited with code {rc}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - safety net for pump loop
            log_event(
                self._logger,
                logging.WARNING,
                "claude.turn.pump_failed",
                session_id=state.session_id,
                turn_id=state.turn_id,
                error=str(exc),
            )
            state.terminal_status = state.terminal_status or "error"
            state.errors.append(str(exc))
        finally:
            await state.event_buffer.close()
            state.closed = True
            async with self._lock:
                if self._session_active_turn.get(state.session_id) == state.turn_id:
                    self._session_active_turn.pop(state.session_id, None)

    async def wait_for_turn(
        self,
        workspace_root: Path,
        session_id: str,
        turn_id: str,
        *,
        timeout: Optional[float] = None,
    ) -> TerminalTurnResult:
        _ = workspace_root
        state = await self._get_state(session_id, turn_id)
        if state.pump_task is None:
            raise ClaudeSupervisorError(
                f"Claude turn {turn_id} has no active pump task"
            )
        effective_timeout = (
            float(timeout) if timeout is not None else self._turn_timeout_seconds
        )
        try:
            await asyncio.wait_for(asyncio.shield(state.pump_task), timeout=effective_timeout)
        except asyncio.TimeoutError as exc:
            await state.handle.terminate()
            raise ClaudeSupervisorError(
                f"Claude turn {turn_id} timed out after {effective_timeout:.0f}s"
            ) from exc
        return self._build_terminal_result(state)

    async def interrupt_turn(
        self,
        workspace_root: Path,
        session_id: str,
        turn_id: Optional[str],
    ) -> None:
        _ = workspace_root
        sid = (session_id or "").strip()
        if not sid:
            return
        async with self._lock:
            resolved_turn_id = (turn_id or "").strip() or self._session_active_turn.get(sid)
        if not resolved_turn_id:
            return
        state = self._turn_states.get((sid, resolved_turn_id))
        if state is None:
            return
        await state.handle.terminate()

    async def stream_turn_events(
        self,
        workspace_root: Path,
        session_id: str,
        turn_id: str,
    ) -> AsyncIterator[dict[str, Any]]:
        _ = workspace_root
        state = await self._get_state(session_id, turn_id)
        async for event in state.event_buffer.tail():
            yield event

    async def list_turn_events_snapshot(self, turn_id: str) -> list[dict[str, Any]]:
        for (_, tid), state in self._turn_states.items():
            if tid == turn_id:
                return state.event_buffer.snapshot()
        return []

    async def _get_state(self, session_id: str, turn_id: str) -> _ClaudeTurnState:
        state = self._turn_states.get((session_id, turn_id))
        if state is None:
            raise ClaudeSupervisorError(
                f"Unknown Claude turn: session={session_id!r} turn={turn_id!r}"
            )
        return state

    def _build_terminal_result(self, state: _ClaudeTurnState) -> TerminalTurnResult:
        assistant_text = state.final_result_text or "".join(state.assistant_text_chunks)
        return TerminalTurnResult(
            status=state.terminal_status or "ok",
            assistant_text=assistant_text,
            errors=list(state.errors),
            raw_events=list(state.raw_events),
        )


# ----- helpers -----


def _map_approval_mode(value: Optional[str]) -> Optional[str]:
    """Map CAR's approval_mode vocabulary onto Claude's --permission-mode choices."""
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if not normalized:
        return None
    # CAR vocabulary → Claude vocabulary. Conservative defaults:
    mapping = {
        "untrusted_mode": "default",
        "trusted_mode": "acceptEdits",
        "danger_full_access": "bypassPermissions",
        "dangerfullaccess": "bypassPermissions",
        "ask": "default",
        "auto": "auto",
        "plan": "plan",
        "default": "default",
        "acceptedits": "acceptEdits",
        "bypasspermissions": "bypassPermissions",
        "dontask": "dontAsk",
    }
    return mapping.get(normalized, normalized)


def _looks_like_uuid(value: str) -> bool:
    try:
        uuid.UUID(value)
    except (ValueError, AttributeError, TypeError):
        return False
    return True


def _read_session_metadata(
    path: Path,
) -> tuple[Optional[str], Optional[str], dict[str, Any]]:
    """Pull a best-effort title/summary from the first user message in a Claude session file."""
    title: Optional[str] = None
    summary: Optional[str] = None
    raw: dict[str, Any] = {}
    try:
        with path.open("r", encoding="utf-8", errors="replace") as fp:
            for line in fp:
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    record = json.loads(stripped)
                except (json.JSONDecodeError, TypeError):
                    continue
                if not isinstance(record, dict):
                    continue
                if record.get("type") == "user":
                    message = record.get("message")
                    if isinstance(message, dict):
                        content = message.get("content")
                        if isinstance(content, str):
                            title = _truncate(content, 80)
                            summary = _truncate(content, 240)
                            raw = {"first_user_message": content}
                            break
                        if isinstance(content, list):
                            for block in content:
                                if (
                                    isinstance(block, dict)
                                    and block.get("type") == "text"
                                    and isinstance(block.get("text"), str)
                                ):
                                    title = _truncate(block["text"], 80)
                                    summary = _truncate(block["text"], 240)
                                    raw = {"first_user_message": block["text"]}
                                    break
                            if title is not None:
                                break
    except OSError:
        return title, summary, raw
    return title, summary, raw


def _truncate(text: str, limit: int) -> Optional[str]:
    stripped = text.strip()
    if not stripped:
        return None
    if len(stripped) <= limit:
        return stripped
    return stripped[: max(limit - 1, 1)].rstrip() + "…"


def _extract_terminal_error_message(event: Mapping[str, Any]) -> Optional[str]:
    if not event_decoder.is_error_result(event):
        return None
    for key in ("error", "message", "result"):
        value = event.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    subtype = event.get("subtype")
    if isinstance(subtype, str) and subtype.strip():
        return f"claude turn failed: {subtype.strip()}"
    return "claude turn failed"


# ----- public preflight helpers (mirrors hermes_runtime_preflight) -----


@dataclass(frozen=True)
class ClaudeRuntimePreflightResult:
    runtime_id: str
    status: str
    version: Optional[str]
    binary: Optional[str]
    message: str
    fix: str


def claude_binary_available(config: Any) -> bool:
    """Cheap availability check used by the agent registry healthcheck."""
    binary = _resolve_binary_from_config(config)
    if not binary:
        return False
    from ...core.utils import resolve_executable

    return bool(resolve_executable(binary))


def claude_runtime_preflight(config: Any, **_: Any) -> ClaudeRuntimePreflightResult:
    """Resolve the configured Claude binary and probe ``--version``."""
    from ...core.utils import resolve_executable

    binary = _resolve_binary_from_config(config)
    if not binary:
        return ClaudeRuntimePreflightResult(
            runtime_id=CLAUDE_RUNTIME_ID,
            status="missing",
            version=None,
            binary=None,
            message="No claude binary configured under agents.claude.binary",
            fix="Set agents.claude.binary in your CAR config to the path of the `claude` CLI.",
        )
    resolved = resolve_executable(binary)
    if not resolved:
        return ClaudeRuntimePreflightResult(
            runtime_id=CLAUDE_RUNTIME_ID,
            status="missing",
            version=None,
            binary=binary,
            message=f"Claude binary not found on PATH: {binary!r}",
            fix=(
                "Install Claude Code (https://docs.anthropic.com/claude/docs/claude-code) "
                "or update agents.claude.binary to an absolute path."
            ),
        )
    version = _probe_version(resolved)
    return ClaudeRuntimePreflightResult(
        runtime_id=CLAUDE_RUNTIME_ID,
        status="ready",
        version=version,
        binary=resolved,
        message=f"Claude {version or 'available'} at {resolved}",
        fix="",
    )


def build_claude_supervisor_from_config(
    config: Any,
    *,
    logger: Optional[logging.Logger] = None,
) -> Optional[ClaudeSupervisor]:
    """Build a ClaudeSupervisor from a CAR config object, or return None if disabled."""
    binary = _resolve_binary_from_config(config)
    if not binary:
        return None
    return ClaudeSupervisor(binary=binary, logger=logger)


def _resolve_binary_from_config(config: Any) -> Optional[str]:
    if config is None:
        return None
    resolver = getattr(config, "agent_binary", None)
    if callable(resolver):
        try:
            value = resolver(CLAUDE_RUNTIME_ID)
        except (TypeError, KeyError, AttributeError):
            value = None
        normalized = _normalize_optional_text(value)
        if normalized:
            return normalized
    agents = getattr(config, "agents", None)
    if isinstance(agents, Mapping):
        section = agents.get(CLAUDE_RUNTIME_ID)
        if isinstance(section, Mapping):
            return _normalize_optional_text(section.get("binary"))
        if hasattr(section, "binary"):
            return _normalize_optional_text(getattr(section, "binary", None))
    return None


def _probe_version(binary: str) -> Optional[str]:
    """Synchronous best-effort ``claude --version`` probe used by preflight."""
    import subprocess

    try:
        result = subprocess.run(
            [binary, "--version"],
            capture_output=True,
            text=True,
            timeout=5.0,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    output = (result.stdout or result.stderr or "").strip()
    return output.splitlines()[0] if output else None


__all__ = [
    "CLAUDE_RUNTIME_ID",
    "ClaudeRuntimePreflightResult",
    "ClaudeSessionHandle",
    "ClaudeSupervisor",
    "ClaudeSupervisorError",
    "build_claude_supervisor_from_config",
    "claude_binary_available",
    "claude_runtime_preflight",
]
