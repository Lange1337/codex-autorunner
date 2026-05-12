"""Stream-json event decoder for the Claude Code CLI.

Claude Code emits line-delimited JSON when invoked with
``--print --output-format=stream-json --verbose``. Each line is a self-contained
JSON object. The schemas relevant to a turn lifecycle are::

    {"type": "system", "subtype": "init", "session_id": ..., ...}
    {"type": "user", "message": {"role": "user", "content": ...}, ...}
    {"type": "assistant", "message": {"role": "assistant", "content": [...]}, ...}
    {"type": "result", "subtype": "success" | "error_during_execution" | ...,
     "result": "<final assistant text>",
     "total_cost_usd": ..., "duration_ms": ..., "is_error": bool, ...}

The decoder normalizes those into the dict-shaped event envelopes the rest of
CAR consumes (``{"message": {"method", "params"}}``) so progress streams and
terminal-result extraction work identically to the other harnesses.
"""

from __future__ import annotations

import json
from typing import Any, Optional


def decode_stream_json_line(line: str) -> Optional[dict[str, Any]]:
    """Parse a single stream-json line into a raw event dict.

    Returns ``None`` for blank lines or malformed JSON; callers should skip
    those without aborting the stream.
    """
    if not line:
        return None
    stripped = line.strip()
    if not stripped:
        return None
    try:
        payload = json.loads(stripped)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def event_kind(event: dict[str, Any]) -> str:
    """Coarse event type used for routing (``system``/``user``/``assistant``/``result``)."""
    kind = event.get("type")
    if isinstance(kind, str):
        return kind.strip().lower()
    return ""


def extract_session_id(event: dict[str, Any]) -> Optional[str]:
    """Pull the Claude session id from a system init event (or any event)."""
    value = event.get("session_id")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def extract_assistant_delta_text(event: dict[str, Any]) -> Optional[str]:
    """Best-effort extraction of assistant-output text from an event.

    Claude streams assistant messages as ``{"type":"assistant","message":{...}}``
    where ``message.content`` is a list of content blocks. We concatenate text
    blocks; tool-use blocks are returned as ``None`` so callers can skip them.
    """
    if event_kind(event) != "assistant":
        return None
    message = event.get("message")
    if not isinstance(message, dict):
        return None
    content = message.get("content")
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return None
    parts: list[str] = []
    for block in content:
        if not isinstance(block, dict):
            continue
        if block.get("type") != "text":
            continue
        text = block.get("text")
        if isinstance(text, str):
            parts.append(text)
    if not parts:
        return None
    return "".join(parts)


def extract_result_text(event: dict[str, Any]) -> Optional[str]:
    """Pull the final ``result`` string from a terminal result event."""
    if event_kind(event) != "result":
        return None
    value = event.get("result")
    if isinstance(value, str):
        return value
    return None


def is_terminal(event: dict[str, Any]) -> bool:
    """Whether this event closes the turn."""
    return event_kind(event) == "result"


def is_error_result(event: dict[str, Any]) -> bool:
    """Whether a terminal ``result`` event indicates a failure."""
    if not is_terminal(event):
        return False
    if event.get("is_error") is True:
        return True
    subtype = event.get("subtype")
    if isinstance(subtype, str):
        return subtype.strip().lower() != "success"
    return False


def to_envelope(event: dict[str, Any]) -> dict[str, Any]:
    """Wrap a raw stream-json event in CAR's ``{message:{method,params}}`` envelope.

    Surfaces consume this shape (see opencode/harness.py docstring); keeping the
    method name namespaced under ``claude.*`` avoids collisions with
    opencode/codex/hermes event vocabularies.
    """
    kind = event_kind(event) or "unknown"
    return {
        "message": {
            "method": f"claude.{kind}",
            "params": dict(event),
        },
    }


__all__ = [
    "decode_stream_json_line",
    "event_kind",
    "extract_assistant_delta_text",
    "extract_result_text",
    "extract_session_id",
    "is_error_result",
    "is_terminal",
    "to_envelope",
]
