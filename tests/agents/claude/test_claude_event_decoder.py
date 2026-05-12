from __future__ import annotations

import json

from codex_autorunner.agents.claude import event_decoder


def test_decode_blank_or_garbage_lines_returns_none() -> None:
    assert event_decoder.decode_stream_json_line("") is None
    assert event_decoder.decode_stream_json_line("   \n") is None
    assert event_decoder.decode_stream_json_line("not json") is None
    # Non-object JSON (e.g. a bare array) is also rejected.
    assert event_decoder.decode_stream_json_line("[1,2,3]") is None


def test_decode_system_init_event_round_trips() -> None:
    payload = {"type": "system", "subtype": "init", "session_id": "abc-123"}
    decoded = event_decoder.decode_stream_json_line(json.dumps(payload))
    assert decoded == payload
    assert event_decoder.event_kind(decoded) == "system"
    assert event_decoder.extract_session_id(decoded) == "abc-123"
    assert not event_decoder.is_terminal(decoded)


def test_extract_assistant_delta_text_concatenates_text_blocks() -> None:
    event = {
        "type": "assistant",
        "message": {
            "role": "assistant",
            "content": [
                {"type": "text", "text": "Hello "},
                {"type": "tool_use", "name": "Bash"},  # should be ignored
                {"type": "text", "text": "world."},
            ],
        },
    }
    assert event_decoder.extract_assistant_delta_text(event) == "Hello world."


def test_extract_assistant_delta_text_returns_none_when_no_text() -> None:
    event = {
        "type": "assistant",
        "message": {"role": "assistant", "content": [{"type": "tool_use"}]},
    }
    assert event_decoder.extract_assistant_delta_text(event) is None


def test_terminal_result_event_classification() -> None:
    ok = {"type": "result", "subtype": "success", "result": "All done"}
    err = {
        "type": "result",
        "subtype": "error_during_execution",
        "is_error": True,
        "result": "boom",
    }
    assert event_decoder.is_terminal(ok)
    assert event_decoder.is_terminal(err)
    assert not event_decoder.is_error_result(ok)
    assert event_decoder.is_error_result(err)
    assert event_decoder.extract_result_text(ok) == "All done"


def test_to_envelope_wraps_under_claude_method_namespace() -> None:
    event = {"type": "user", "message": {"role": "user", "content": "hi"}}
    envelope = event_decoder.to_envelope(event)
    assert envelope["message"]["method"] == "claude.user"
    # Params copies the event (callers may keep it; we still produce a dict here).
    assert envelope["message"]["params"] == event
