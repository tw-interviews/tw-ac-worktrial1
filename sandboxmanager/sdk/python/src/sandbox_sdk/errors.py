from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from ._models import CommandBase


class SandboxError(Exception):
    """Base class for every error raised by this SDK."""


class SandboxConnectionError(SandboxError):
    """The sandbox API could not be reached."""


class SandboxTimeoutError(SandboxError, TimeoutError):
    """A client-side wait (e.g. for a sandbox to become ready) ran out of time."""


class APIError(SandboxError):
    """The sandbox API returned an error response."""

    def __init__(self, status_code: int, message: str, body: Any = None):
        super().__init__(f"{status_code}: {message}")
        self.status_code = status_code
        self.message = message
        self.body = body


class BadRequestError(APIError):
    """400: the request was invalid (bad image, env name, resources, ...)."""


class NotFoundError(APIError):
    """404: the sandbox or execution does not exist."""


class ConflictError(APIError):
    """409: the sandbox can't take the request right now, e.g. it isn't ready."""


class SandboxPausedError(ConflictError):
    """409: the sandbox is paused (or pausing); ``resume()`` it first."""


class QuotaExceededError(APIError):
    """429: the sandbox namespace's resource quota is full. Delete sandboxes
    (or request fewer resources) and retry."""


class ExecutionFailedError(APIError):
    """502 from an execution: the exec stream to the sandbox failed.

    The execution was still recorded; ``execution_id`` identifies it.
    """

    def __init__(self, status_code: int, message: str, body: Any = None):
        super().__init__(status_code, message, body)
        self.execution_id: Optional[str] = body.get("execution_id") if isinstance(body, dict) else None


class OutputNotLoadedError(SandboxError):
    """An AsyncCommand's output was read before it was fetched.

    Commands listed by ``AsyncSandbox.commands()`` don't include output; call
    ``await cmd.refresh()`` first, or list with ``with_output=True``.
    """


class CommandError(SandboxError):
    """Raised by ``Command.check()`` when a command didn't exit 0."""

    def __init__(self, command: CommandBase):
        if command.timed_out:
            reason = "timed out"
        else:
            reason = f"exited with code {command.exit_code}"
        super().__init__(f"command {command.argv!r} {reason}")
        self.command = command


_STATUS_ERRORS = {400: BadRequestError, 404: NotFoundError, 409: ConflictError, 429: QuotaExceededError}


def error_for_response(status_code: int, body: Any, path: str) -> APIError:
    message = body.get("error") if isinstance(body, dict) else None
    message = message or f"request to {path} failed"
    if status_code == 502 and isinstance(body, dict) and "execution_id" in body:
        return ExecutionFailedError(status_code, message, body)
    if isinstance(body, dict) and body.get("code") == "sandbox_paused":
        return SandboxPausedError(status_code, message, body)
    return _STATUS_ERRORS.get(status_code, APIError)(status_code, message, body)
