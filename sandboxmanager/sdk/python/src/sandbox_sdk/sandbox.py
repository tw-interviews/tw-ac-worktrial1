from __future__ import annotations

import os
import tempfile
import time
from typing import Any, BinaryIO, Dict, List, Mapping, Optional, Sequence, Union

from . import _files
from ._models import (  # noqa: F401  (re-exported)
    MISSING,
    NOT_READY,
    READY,
    FileInfo,
    Resources,
    SandboxBase,
    UploadResult,
    WrittenFile,
    exec_argv,
)
from .client import Client, default_client
from .command import Command
from .errors import NotFoundError, SandboxError


class Sandbox(SandboxBase):
    """A sandbox: one user container running ``image`` in the cluster.

    Describe it, then ``create()`` it::

        sb = Sandbox("python:3.12-slim", command=["sleep", "infinity"], env={"MODE": "dev"})
        sb.create()
        cmd = sb.exec("python -c 'print(1 + 1)'")
        print(cmd.stdout)
        sb.delete()

    or use it as a context manager, which creates on entry and deletes on exit.

    Every sandbox has a workspace at ``/workspace``, saved (as a snapshot)
    when it's paused, deleted or about to expire. ``files`` maps
    local paths (files or directories) to destinations there, and they're
    uploaded before the sandbox's command starts::

        Sandbox("python:3.12-slim", command=["python", "/workspace/main.py"],
                files={"./main.py": "main.py", "./data/": "data/"})

    After a ``delete()`` (or TTL expiry) the files can still be downloaded for
    a recovery window (12h by default; see ``files_retained_until``).

    The object keeps the sandbox's runtime data (``sandbox_id``, ``name``,
    ``namespace``, ``definition``, ``status``, ...) up to date as you call it.
    See :class:`~sandbox_sdk.AsyncSandbox` for the asyncio version.
    """

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
        client: Optional[Client] = None,
    ):
        super().__init__(image, command=command, args=args, env=env, resources=resources, ttl=ttl,
                         paused_ttl=paused_ttl, files=files)
        self._client = client

    @classmethod
    def get(cls, sandbox_id: str, *, client: Optional[Client] = None) -> Sandbox:
        """Attach to an existing sandbox by id."""
        client = client or default_client()
        return cls._from_api(client.get_sandbox(sandbox_id), client)

    @classmethod
    def _from_api(cls, data: Dict[str, Any], client: Client) -> Sandbox:
        sb = cls(client=client)
        sb._apply(data)
        return sb

    @property
    def client(self) -> Client:
        if self._client is None:
            self._client = default_client()
        return self._client

    def create(self, *, wait: bool = True, timeout: float = 120.0) -> Sandbox:
        """Create the sandbox (``POST /sandboxes``).

        With ``wait=True`` (the default) this blocks until the sandbox is ready
        to run commands, raising :class:`SandboxTimeoutError` after ``timeout``
        seconds.

        If ``files`` was given, they're uploaded before the user container
        starts (even with ``wait=False``). Missing local paths raise before
        anything is created; if the upload fails, the sandbox is deleted
        (purged) and the error re-raised.
        """
        entries = _files.plan_upload(self.files) if self.files else None
        self._apply(self.client.create_sandbox(self._spec_for_create()))
        if entries is not None:
            try:
                self._upload_entries(entries)
                self.client.complete_staging(self._known_id())
            except BaseException:
                self._discard()
                raise
        if wait:
            self.wait_ready(timeout=timeout)
        return self

    def refresh(self) -> Sandbox:
        """Re-read the sandbox, including its live status, from the API."""
        self._apply(self.client.get_sandbox(self._known_id()))
        return self

    def wait_ready(self, *, timeout: float = 120.0, poll_interval: float = 0.5) -> Sandbox:
        """Poll until the sandbox is ready to run commands."""
        deadline = self._deadline(timeout)
        while not self.refresh()._ready_after_refresh(deadline, timeout):
            time.sleep(poll_interval)
        return self

    def pause(self, *, wait: bool = True, timeout: float = 60.0, force: bool = False) -> Sandbox:
        """Pause the sandbox (``POST /sandboxes/{id}/pause``): its pod stops,
        freeing its CPU and memory quota, while ``/workspace`` is kept. Files
        stay readable and writable while paused; ``exec()`` raises
        :class:`SandboxPausedError` until ``resume()``. Memory, processes and
        everything outside ``/workspace`` are lost.

        Raises :class:`ConflictError` while commands are running, unless
        ``force=True``, which stops them. The sandbox then expires after its
        paused TTL (``paused_ttl_seconds``, 24h by default) instead of its TTL.
        With ``wait=True`` this returns once the pod has stopped.
        """
        self._apply(self.client.pause_sandbox(self._require_id(), force=force))
        if wait:
            self.wait_paused(timeout=timeout)
        return self

    def resume(self, *, wait: bool = True, timeout: float = 120.0) -> Sandbox:
        """Resume a paused sandbox (``POST /sandboxes/{id}/resume``): a new pod
        starts with ``/workspace`` restored from the pause's snapshot, and the
        TTL restarts. Raises
        :class:`QuotaExceededError` if the pod wouldn't fit, or
        :class:`ConflictError` if the sandbox expired while paused (its files
        can still be downloaded until ``files_retained_until``). With
        ``wait=True`` this returns once it's ready. Resuming a running sandbox
        is a no-op.
        """
        self._apply(self.client.resume_sandbox(self._require_id()))
        if wait:
            self.wait_ready(timeout=timeout)
        return self

    def wait_paused(self, *, timeout: float = 60.0, poll_interval: float = 0.5) -> Sandbox:
        """Poll until a pause has finished."""
        deadline = self._deadline(timeout)
        while not self.refresh()._paused_after_refresh(deadline, timeout):
            time.sleep(poll_interval)
        return self

    def delete(self, *, purge: bool = False) -> None:
        """Delete the sandbox (``DELETE /sandboxes/{id}``).

        ``/workspace`` is saved first, and stays downloadable for the recovery
        window, after which it and the execution history are deleted. ``purge=True`` deletes them
        immediately. Deleting a sandbox that's already gone is not an error.
        """
        try:
            self.client.delete_sandbox(self._known_id(), purge=purge)
        except NotFoundError:
            purge = True
        self._mark_deleted(purged=purge)

    def exec(
        self,
        command: Union[str, Sequence[str]],
        *,
        stdin: Optional[str] = None,
        timeout: Optional[int] = None,
    ) -> Command:
        """Run a command in the sandbox's user container and wait for it.

        ``command`` is either an argv list (``["ls", "-l"]``) or a string, which
        runs via ``sh -c`` so pipes and ``$VARS`` work (the image needs ``sh``).
        ``timeout`` is in seconds (server default 30, max 300).

        A non-zero exit code does not raise; check ``Command.exit_code`` or call
        ``Command.check()``.
        """
        data = self.client.create_execution(
            self._require_id(), exec_argv(command), stdin=stdin, timeout_seconds=timeout
        )
        self._note_execution(data)
        return Command(data, self.client)

    def commands(self) -> List[Command]:
        """Every command run in this sandbox, newest first.

        Output is fetched lazily per command on first access to ``stdout``/``stderr``.
        """
        return [
            Command(data, self.client, output_loaded=False)
            for data in self.client.list_executions(self._require_id())
        ]

    def get_command(self, execution_id: str) -> Command:
        """Look up one of this sandbox's commands by execution id."""
        cmd = Command(self.client.get_execution(execution_id), self.client)
        self._check_owns(cmd)
        return cmd

    # Files in /workspace. Paths are relative to /workspace (or absolute inside
    # it). These keep working after delete() during the recovery window,
    # except that a deleted sandbox can't be written to.

    def upload(self, files: Mapping[str, str]) -> UploadResult:
        """Upload ``{local path: destination}`` into the sandbox, streamed as
        one tar archive. Local directories are uploaded recursively."""
        return self._upload_entries(_files.plan_upload(files))

    def write_file(self, path: str, data: Union[bytes, str, BinaryIO]) -> WrittenFile:
        """Write one file from bytes, text (UTF-8) or a binary file object
        (streamed). Replaces it atomically; creates parent directories."""
        r = self.client.put_file(self._require_id(), path, _files.content_chunks(data))
        return WrittenFile(r["path"], r["bytes"], r["sha256"])

    def read_file(self, path: str) -> bytes:
        """Read a whole file into memory. Use ``download()`` for large files."""
        with self.client.open_file(self._known_id(), path) as resp:
            return resp.read()

    def download(self, path: str, local: str) -> str:
        """Stream a file or directory out of the sandbox; returns where it went.

        A file goes to ``local``, or into it if that's an existing directory.
        A directory is extracted into ``local``.
        """
        with self.client.open_file(self._known_id(), path) as resp:
            is_dir = resp.headers.get("content-type", "").startswith("application/x-tar")
            target = _files.download_target(path, local, is_dir)
            if is_dir:
                with tempfile.TemporaryFile() as spool:
                    for chunk in resp.iter_bytes(_files.CHUNK_SIZE):
                        spool.write(chunk)
                    spool.seek(0)
                    _files.extract_tar(spool, target)
                return target
            parent = os.path.dirname(os.path.abspath(target))
            os.makedirs(parent, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=parent, prefix=".download-")
            try:
                with os.fdopen(fd, "wb") as out:
                    for chunk in resp.iter_bytes(_files.CHUNK_SIZE):
                        out.write(chunk)
                os.replace(tmp, target)
            except BaseException:
                os.unlink(tmp)
                raise
            return target

    def list_files(self, path: str = "") -> List[FileInfo]:
        """List a directory (the workspace root by default)."""
        return [FileInfo.from_api(e) for e in self.client.list_files(self._known_id(), path)]

    def remove_file(self, path: str) -> None:
        """Delete a file, or a directory recursively."""
        self.client.delete_file(self._require_id(), path)

    def _upload_entries(self, entries: List[_files.UploadEntry]) -> UploadResult:
        r = self.client.upload_tar(self._require_id(), _files.tar_chunks(entries))
        return UploadResult(r["files"], r["bytes"])

    def _discard(self) -> None:
        """Purge a sandbox that failed to start, ignoring errors."""
        try:
            self.delete(purge=True)
        except SandboxError:
            pass

    def __enter__(self) -> Sandbox:
        if self.sandbox_id is None:
            try:
                self.create()
            except BaseException:
                # Created but never became ready: don't leak it.
                if self.created:
                    self._discard()
                raise
        return self

    def __exit__(self, *exc: Any) -> None:
        if self.created:
            self.delete()
