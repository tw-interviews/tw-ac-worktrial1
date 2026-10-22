"""State and validation shared by the sync and async Sandbox/Command classes.

Nothing here does I/O; subclasses add the methods that call the API.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

from ._time import parse_time
from .errors import CommandError, SandboxError, SandboxTimeoutError

# Sandbox statuses reported by the API.
STAGING = "Staging"  # waiting for files= to be uploaded before the command starts
READY = "Ready"
NOT_READY = "NotReady"
PAUSING = "Pausing"  # pause requested; the pod is stopping
PAUSED = "Paused"  # pod stopped; /workspace kept and its files still accessible
RESUMING = "Resuming"  # resume requested; a new pod is starting and restoring /workspace
DELETING = "Deleting"
DELETED = "Deleted"  # deleted; files can be downloaded until files_retained_until
EXPIRED = "Expired"  # TTL passed with no commands; the cluster deleted it
MISSING = "Missing"  # gone from the cluster for another reason

# Execution statuses reported by the API.
RUNNING = "running"
COMPLETED = "completed"
TIMED_OUT = "timed_out"
ERROR = "error"


@dataclass
class Resources:
    """Kubernetes-style resource quantities, e.g. ``{"cpu": "500m", "memory": "256Mi"}``."""

    requests: Dict[str, str] = field(default_factory=dict)
    limits: Dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class FileInfo:
    """One entry in a sandbox directory listing."""

    name: str
    type: str  # "file", "directory", "symlink" or "other"
    size: int
    mode: str  # octal permission bits, e.g. "644"
    modified: Optional[datetime]

    @property
    def is_dir(self) -> bool:
        return self.type == "directory"

    @classmethod
    def from_api(cls, data: Dict[str, Any]) -> "FileInfo":
        return cls(data["name"], data["type"], data["size"], data["mode"], parse_time(data.get("modified")))


@dataclass(frozen=True)
class UploadResult:
    """What an ``upload()`` (or ``files=`` at creation) wrote."""

    files: int
    bytes: int


@dataclass(frozen=True)
class WrittenFile:
    """What a ``write_file()`` wrote: the absolute path, size and SHA-256."""

    path: str
    bytes: int
    sha256: str


def exec_argv(command: Union[str, Sequence[str]]) -> List[str]:
    """A string runs via ``sh -c``; a sequence is used as argv as-is."""
    return ["sh", "-c", command] if isinstance(command, str) else list(command)


class CommandBase:
    def __init__(self, data: Dict[str, Any], *, output_loaded: bool = True):
        self._output_loaded = output_loaded
        self._apply(data)

    @property
    def stdout(self) -> str:
        self._ensure_output()
        return self._stdout

    @property
    def stderr(self) -> str:
        self._ensure_output()
        return self._stderr

    @property
    def stdout_truncated(self) -> bool:
        self._ensure_output()
        return self._stdout_truncated

    @property
    def stderr_truncated(self) -> bool:
        self._ensure_output()
        return self._stderr_truncated

    @property
    def output_loaded(self) -> bool:
        return self._output_loaded

    @property
    def running(self) -> bool:
        return self.status == RUNNING

    @property
    def timed_out(self) -> bool:
        return self.status == TIMED_OUT

    @property
    def ok(self) -> bool:
        """True if the command ran to completion and exited 0."""
        return self.status == COMPLETED and self.exit_code == 0

    def check(self):
        """Raise :class:`CommandError` unless the command exited 0; returns self."""
        if not self.ok:
            raise CommandError(self)
        return self

    def _ensure_output(self) -> None:
        raise NotImplementedError

    def _apply(self, data: Dict[str, Any], *, output_loaded: Optional[bool] = None) -> None:
        if output_loaded is not None:
            self._output_loaded = output_loaded
        self.execution_id: str = data["execution_id"]
        self.sandbox_id: str = data["sandbox_id"]
        self.argv: List[str] = list(data.get("command") or [])
        self.pod: Optional[str] = data.get("pod")
        self.status: str = data["status"]
        self.exit_code: Optional[int] = data.get("exit_code")
        self.started_at: Optional[datetime] = parse_time(data.get("started_at"))
        self.finished_at: Optional[datetime] = parse_time(data.get("finished_at"))
        self._stdout: str = data.get("stdout") or ""
        self._stderr: str = data.get("stderr") or ""
        self._stdout_truncated: bool = bool(data.get("stdout_truncated"))
        self._stderr_truncated: bool = bool(data.get("stderr_truncated"))

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(execution_id={self.execution_id!r}, argv={self.argv!r}, "
            f"status={self.status!r}, exit_code={self.exit_code!r})"
        )


class SandboxBase:
    def __init__(
        self,
        image: Optional[str] = None,
        *,
        command: Optional[Sequence[str]] = None,
        args: Optional[Sequence[str]] = None,
        env: Optional[Dict[str, str]] = None,
        resources: Optional[Resources] = None,
        ttl: Optional[int] = None,
        paused_ttl: Optional[int] = None,
        files: Optional[Mapping[str, str]] = None,
    ):
        self.image = image
        self.ttl = ttl
        self.paused_ttl = paused_ttl
        self.files = dict(files) if files else {}
        self.command = list(command) if command is not None else None
        self.args = list(args) if args is not None else None
        self.env = dict(env) if env else {}
        self.resources = resources or Resources()

        self.sandbox_id: Optional[str] = None
        self.name: Optional[str] = None
        self.namespace: Optional[str] = None
        self.definition: Optional[str] = None
        self.created_at: Optional[datetime] = None
        self.ttl_seconds: Optional[int] = None
        self.expires_at: Optional[datetime] = None
        self.paused_ttl_seconds: Optional[int] = None
        self.paused_at: Optional[datetime] = None
        self.volume: Optional[str] = None
        self.volume_size: Optional[str] = None
        # The latest saved copy of /workspace (pause, delete, expiry).
        self.snapshot_at: Optional[datetime] = None
        self.snapshot_bytes: Optional[int] = None
        self.deleted_at: Optional[datetime] = None
        self.files_retained_until: Optional[datetime] = None
        self.status: Optional[str] = None
        self.deleted = False

    @property
    def created(self) -> bool:
        return self.sandbox_id is not None and not self.deleted

    @property
    def ready(self) -> bool:
        return self.status == READY

    @property
    def paused(self) -> bool:
        """True once a pause has finished (status ``Paused``)."""
        return self.status == PAUSED

    def _spec_for_create(self) -> Dict[str, Any]:
        if self.sandbox_id is not None:
            raise SandboxError(f"sandbox {self.sandbox_id} was already created")
        if not self.image:
            raise SandboxError("image is required to create a sandbox")
        spec: Dict[str, Any] = {"image": self.image}
        if self.command is not None:
            spec["command"] = self.command
        if self.args is not None:
            spec["args"] = self.args
        if self.env:
            spec["env"] = self.env
        if self.ttl is not None:
            spec["ttl_seconds"] = self.ttl
        if self.paused_ttl is not None:
            spec["paused_ttl_seconds"] = self.paused_ttl
        if self.files:
            spec["stage_files"] = True
        resources = {k: v for k, v in (("requests", self.resources.requests), ("limits", self.resources.limits)) if v}
        if resources:
            spec["resources"] = resources
        return spec

    def _deadline(self, timeout: float) -> float:
        return time.monotonic() + timeout

    def _ready_after_refresh(self, deadline: float, timeout: float) -> bool:
        """After a refresh: True if ready, False to keep polling, else raise."""
        if self.ready:
            return True
        if self.status in (MISSING, EXPIRED, DELETING, DELETED, PAUSING, PAUSED):
            hint = "; call resume() first" if self.status in (PAUSING, PAUSED) else ""
            raise SandboxError(f"sandbox {self.sandbox_id} is {self.status}{hint}")
        if time.monotonic() >= deadline:
            raise SandboxTimeoutError(f"sandbox {self.sandbox_id} not ready after {timeout}s (status {self.status})")
        return False

    def _paused_after_refresh(self, deadline: float, timeout: float) -> bool:
        """After a refresh: True if paused, False to keep polling, else raise."""
        if self.paused:
            return True
        if self.status != PAUSING:
            raise SandboxError(f"sandbox {self.sandbox_id} is {self.status}, not pausing")
        if time.monotonic() >= deadline:
            raise SandboxTimeoutError(f"sandbox {self.sandbox_id} not paused after {timeout}s")
        return False

    def _note_execution(self, data: Dict[str, Any]) -> None:
        """Every execution refreshes the sandbox's TTL; record the new expiry."""
        if data.get("sandbox_expires_at"):
            self.expires_at = parse_time(data["sandbox_expires_at"])

    def _mark_deleted(self, *, purged: bool) -> None:
        self.deleted = True
        self.status = None if purged else DELETED

    def _check_owns(self, command: CommandBase) -> None:
        if command.sandbox_id != self.sandbox_id:
            raise SandboxError(f"execution {command.execution_id} belongs to sandbox {command.sandbox_id}")

    def _known_id(self) -> str:
        """The id, for calls that still work once the sandbox is deleted
        (reading it, and its files during the recovery window)."""
        if self.sandbox_id is None:
            raise SandboxError("sandbox has not been created yet; call create() first")
        return self.sandbox_id

    def _require_id(self) -> str:
        if self.sandbox_id is None:
            raise SandboxError("sandbox has not been created yet; call create() first")
        if self.deleted:
            raise SandboxError(f"sandbox {self.sandbox_id} has been deleted")
        return self.sandbox_id

    def _apply(self, data: Dict[str, Any]) -> None:
        self.sandbox_id = data["sandbox_id"]
        self.name = data.get("name")
        self.namespace = data.get("namespace")
        self.definition = data.get("definition")
        self.created_at = parse_time(data.get("created_at"))
        self.ttl_seconds = data.get("ttl_seconds")
        self.expires_at = parse_time(data.get("expires_at"))
        self.paused_ttl_seconds = data.get("paused_ttl_seconds")
        self.paused_at = parse_time(data.get("paused_at"))
        self.volume = data.get("volume")
        self.volume_size = data.get("volume_size")
        self.snapshot_at = parse_time(data.get("snapshot_at"))
        self.snapshot_bytes = data.get("snapshot_bytes")
        self.deleted_at = parse_time(data.get("deleted_at"))
        self.files_retained_until = parse_time(data.get("files_retained_until"))
        if self.deleted_at is not None:
            self.deleted = True
        # POST responses carry no live status; leave the last known value.
        if data.get("status"):
            self.status = data["status"]

    def __repr__(self) -> str:
        return f"{type(self).__name__}(sandbox_id={self.sandbox_id!r}, image={self.image!r}, status={self.status!r})"
