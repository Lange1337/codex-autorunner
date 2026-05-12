"""Subprocess client for the Claude Code CLI.

Each turn spawns one ``claude --print --output-format=stream-json --verbose
--session-id=<uuid>`` process. Stdout is consumed line-by-line and each line is
fed to ``event_decoder`` to produce envelopes. The session UUID gives us
``durable_threads`` semantics: subsequent turns with the same UUID resume the
on-disk session under ``~/.claude/projects/``.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import AsyncIterator, Mapping, Optional, Sequence

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ClaudeLaunchSpec:
    """All the inputs required to spawn a single ``claude --print`` invocation."""

    binary: str
    session_id: str
    prompt: str
    workspace_root: Path
    model: Optional[str] = None
    permission_mode: Optional[str] = None
    extra_args: Sequence[str] = field(default_factory=tuple)
    env: Mapping[str, str] = field(default_factory=dict)

    def build_argv(self) -> list[str]:
        """Compose the argv array the supervisor will hand to the subprocess.

        Stream-json output requires ``--verbose`` to be set (Claude refuses
        otherwise). Session-id is set so resume works across turns.
        """
        argv: list[str] = [
            self.binary,
            "--print",
            "--verbose",
            "--output-format",
            "stream-json",
            "--input-format",
            "text",
            "--session-id",
            self.session_id,
        ]
        if self.model:
            argv.extend(["--model", self.model])
        if self.permission_mode:
            argv.extend(["--permission-mode", self.permission_mode])
        if self.extra_args:
            argv.extend(str(arg) for arg in self.extra_args)
        argv.append(self.prompt)
        return argv


class ClaudeProcessError(RuntimeError):
    """Raised when the Claude subprocess fails to start or exits non-zero."""

    def __init__(
        self,
        message: str,
        *,
        returncode: Optional[int] = None,
        stderr: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.returncode = returncode
        self.stderr = stderr


class ClaudeProcessHandle:
    """Live handle to a running ``claude --print`` subprocess."""

    def __init__(
        self,
        process: asyncio.subprocess.Process,
        spec: ClaudeLaunchSpec,
    ) -> None:
        self._process = process
        self._spec = spec
        self._stderr_buf: list[str] = []
        self._stderr_task: Optional[asyncio.Task[None]] = None

    @property
    def pid(self) -> Optional[int]:
        return self._process.pid

    @property
    def session_id(self) -> str:
        return self._spec.session_id

    @property
    def returncode(self) -> Optional[int]:
        return self._process.returncode

    async def iter_stdout_lines(self) -> AsyncIterator[str]:
        """Yield decoded stdout lines as they arrive.

        The Claude binary uses UTF-8; we surface decode errors lazily by
        replacing invalid bytes so a single garbled line does not crash the
        turn loop.
        """
        stdout = self._process.stdout
        if stdout is None:
            return
        while True:
            chunk = await stdout.readline()
            if not chunk:
                break
            yield chunk.decode("utf-8", errors="replace")

    async def _drain_stderr(self) -> None:
        stderr = self._process.stderr
        if stderr is None:
            return
        while True:
            line = await stderr.readline()
            if not line:
                break
            decoded = line.decode("utf-8", errors="replace").rstrip()
            self._stderr_buf.append(decoded)
            # Logging at debug level: callers can opt in via standard logging conf.
            _logger.debug("claude.stderr session=%s %s", self._spec.session_id, decoded)

    def start_stderr_drain(self) -> None:
        if self._stderr_task is None:
            self._stderr_task = asyncio.create_task(self._drain_stderr())

    async def wait(self) -> int:
        rc = await self._process.wait()
        if self._stderr_task is not None:
            try:
                await self._stderr_task
            except asyncio.CancelledError:
                pass
        return rc

    def stderr_text(self) -> str:
        return "\n".join(self._stderr_buf)

    async def terminate(self, *, grace_seconds: float = 5.0) -> None:
        """Interrupt the running turn: SIGTERM then SIGKILL after a grace period."""
        if self._process.returncode is not None:
            return
        try:
            self._process.terminate()
        except ProcessLookupError:
            return
        try:
            await asyncio.wait_for(self._process.wait(), timeout=max(grace_seconds, 0.1))
            return
        except asyncio.TimeoutError:
            pass
        try:
            self._process.kill()
        except ProcessLookupError:
            return
        try:
            await self._process.wait()
        except asyncio.CancelledError:
            raise


async def spawn_claude(spec: ClaudeLaunchSpec) -> ClaudeProcessHandle:
    """Start a ``claude --print`` subprocess for one turn.

    Raises ``ClaudeProcessError`` if the executable is missing or unspawnable.
    Note we do NOT shell-quote the prompt — it is passed as the final argv slot.
    """
    argv = spec.build_argv()
    env = dict(os.environ)
    if spec.env:
        env.update({str(k): str(v) for k, v in spec.env.items()})

    try:
        process = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(spec.workspace_root),
            env=env,
        )
    except FileNotFoundError as exc:
        raise ClaudeProcessError(
            f"Claude binary not found: {spec.binary!r}",
        ) from exc
    except OSError as exc:
        raise ClaudeProcessError(
            f"Failed to spawn Claude: {exc}",
        ) from exc

    handle = ClaudeProcessHandle(process, spec)
    handle.start_stderr_drain()
    return handle


__all__ = [
    "ClaudeLaunchSpec",
    "ClaudeProcessError",
    "ClaudeProcessHandle",
    "spawn_claude",
]
