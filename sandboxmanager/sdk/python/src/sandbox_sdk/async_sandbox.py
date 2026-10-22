from __future__ import annotations

import asyncio
import os
import tempfile
from typing import Any, AsyncIterator, BinaryIO, Dict, Iterator, List, Mapping, Optional, Sequence, Union

from . import _files
from ._models import FileInfo, Resources, SandboxBase, UploadResult, WrittenFile, exec_argv
from .async_client import AsyncClient
from .async_command import AsyncCommand
from .errors import NotFoundError, SandboxError


async def _in_thread(chunks: Iterator[bytes]) -> AsyncIterator[bytes]:
    """Drive a blocking (disk-reading) chunk generator from a worker thread."""
    done = object()
    while True:
        chunk = await asyncio.to_thread(next, chunks, done)
        if chunk is done:
            return
        yield chunk


class AsyncSandbox(SandboxBase):
    """A sandbox, driven from asyncio; the async twin of :class:`Sandbox`.

    ::

        async with AsyncClient() as client:
            async with AsyncSandbox("busybox:1.36", command=["sleep", "infinity"], client=client) as sb:
                cmd = await sb.exec("echo hi")
                print(cmd.stdout)
                # several at once
                cmds = await asyncio.gather(*(sb.exec(f"echo {i}") for i in range(5)))

    Pass a shared :class:`AsyncClient` when you manage several sandboxes. Without
    one, the sandbox opens its own client and closes it on ``delete()``,
    ``aclose()`` or leaving ``async with``; commands it returned can't be
    refreshed after that (their history is deleted with the sandbox anyway).

    Cancelling a task that's awaiting ``exec()`` closes the request; the server
    still records the result, but the process may keep running in the container.
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
        client: Optional[AsyncClient] = None,
    ):
        super().__init__(image, command=command, args=args, env=env, resources=resources, ttl=ttl,
                         paused_ttl=paused_ttl, files=files)
        self._client = client
        self._owns_client = client is None

    @classmethod
    async def get(cls, sandbox_id: str, *, client: Optional[AsyncClient] = None) -> AsyncSandbox:
        """Attach to an existing sandbox by id."""
        sb = cls(client=client)
        try:
            sb._apply(await sb.client.get_sandbox(sandbox_id))
        except BaseException:
            await sb.aclose()
            raise
        return sb

    @classmethod
    def _from_api(cls, data: Dict[str, Any], client: AsyncClient) -> AsyncSandbox:
        sb = cls(client=client)
        sb._apply(data)
        return sb

    @property
    def client(self) -> AsyncClient:
        if self._client is None:
            self._client = AsyncClient()
        return self._client

    async def aclose(self) -> None:
        """Close the HTTP client if this sandbox opened it. Doesn't delete the sandbox."""
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def create(self, *, wait: bool = True, timeout: float = 120.0) -> AsyncSandbox:
        """Create the sandbox (``POST /sandboxes``).

        With ``wait=True`` (the default) this waits until the sandbox is ready
        to run commands, raising :class:`SandboxTimeoutError` after ``timeout``
        seconds.

        If ``files`` was given, they're uploaded before the user container
        starts (even with ``wait=False``). Missing local paths raise before
        anything is created; if the upload fails, the sandbox is deleted
        (purged) and the error re-raised.
        """
        entries = _files.plan_upload(self.files) if self.files else None
        self._apply(await self.client.create_sandbox(self._spec_for_create()))
        if entries is not None:
            try:
                await self._upload_entries(entries)
                await self.client.complete_staging(self._known_id())
            except BaseException:
                await self._discard()
                raise
        if wait:
            await self.wait_ready(timeout=timeout)
        return self

    async def refresh(self) -> AsyncSandbox:
        """Re-read the sandbox, including its live status, from the API."""
        self._apply(await self.client.get_sandbox(self._known_id()))
        return self

    async def wait_ready(self, *, timeout: float = 120.0, poll_interval: float = 0.5) -> AsyncSandbox:
        """Poll until the sandbox is ready to run commands."""
        deadline = self._deadline(timeout)
        while not (await self.refresh())._ready_after_refresh(deadline, timeout):
            await asyncio.sleep(poll_interval)
        return self

    async def pause(self, *, wait: bool = True, timeout: float = 60.0, force: bool = False) -> AsyncSandbox:
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
        self._apply(await self.client.pause_sandbox(self._require_id(), force=force))
        if wait:
            await self.wait_paused(timeout=timeout)
        return self

    async def resume(self, *, wait: bool = True, timeout: float = 120.0) -> AsyncSandbox:
        """Resume a paused sandbox (``POST /sandboxes/{id}/resume``): a new pod
        starts with ``/workspace`` restored from the pause's snapshot, and the
        TTL restarts. Raises
        :class:`QuotaExceededError` if the pod wouldn't fit, or
        :class:`ConflictError` if the sandbox expired while paused (its files
        can still be downloaded until ``files_retained_until``). With
        ``wait=True`` this returns once it's ready. Resuming a running sandbox
        is a no-op.
        """
        self._apply(await self.client.resume_sandbox(self._require_id()))
        if wait:
            await self.wait_ready(timeout=timeout)
        return self

    async def wait_paused(self, *, timeout: float = 60.0, poll_interval: float = 0.5) -> AsyncSandbox:
        """Poll until a pause has finished."""
        deadline = self._deadline(timeout)
        while not (await self.refresh())._paused_after_refresh(deadline, timeout):
            await asyncio.sleep(poll_interval)
        return self

    async def delete(self, *, purge: bool = False) -> None:
        """Delete the sandbox (``DELETE /sandboxes/{id}``).

        ``/workspace`` is saved first, and stays downloadable for the recovery
        window, after which it and the execution history are deleted. ``purge=True`` deletes them
        immediately. Deleting a sandbox that's already gone is not an error.
        """
        try:
            await self.client.delete_sandbox(self._known_id(), purge=purge)
        except NotFoundError:
            purge = True
        self._mark_deleted(purged=purge)
        await self.aclose()

    async def exec(
        self,
        command: Union[str, Sequence[str]],
        *,
        stdin: Optional[str] = None,
        timeout: Optional[int] = None,
    ) -> AsyncCommand:
        """Run a command in the sandbox's user container and wait for it.

        ``command`` is either an argv list (``["ls", "-l"]``) or a string, which
        runs via ``sh -c`` so pipes and ``$VARS`` work (the image needs ``sh``).
        ``timeout`` is in seconds (server default 30, max 300).

        A non-zero exit code does not raise; check ``AsyncCommand.exit_code`` or
        call ``AsyncCommand.check()``. Calls may run concurrently.
        """
        data = await self.client.create_execution(
            self._require_id(), exec_argv(command), stdin=stdin, timeout_seconds=timeout
        )
        self._note_execution(data)
        return AsyncCommand(data, self.client)

    async def commands(self, *, with_output: bool = False) -> List[AsyncCommand]:
        """Every command run in this sandbox, newest first.

        Output isn't included unless ``with_output=True``, which fetches each
        command's output concurrently.
        """
        cmds = [
            AsyncCommand(data, self.client, output_loaded=False)
            for data in await self.client.list_executions(self._require_id())
        ]
        if with_output:
            await asyncio.gather(*(cmd.refresh() for cmd in cmds))
        return cmds

    async def get_command(self, execution_id: str) -> AsyncCommand:
        """Look up one of this sandbox's commands by execution id."""
        cmd = AsyncCommand(await self.client.get_execution(execution_id), self.client)
        self._check_owns(cmd)
        return cmd

    # Files in /workspace. Paths are relative to /workspace (or absolute inside
    # it). These keep working after delete() during the recovery window,
    # except that a deleted sandbox can't be written to.

    async def upload(self, files: Mapping[str, str]) -> UploadResult:
        """Upload ``{local path: destination}`` into the sandbox, streamed as
        one tar archive. Local directories are uploaded recursively."""
        return await self._upload_entries(_files.plan_upload(files))

    async def write_file(self, path: str, data: Union[bytes, str, BinaryIO]) -> WrittenFile:
        """Write one file from bytes, text (UTF-8) or a binary file object
        (streamed). Replaces it atomically; creates parent directories."""
        content = _files.content_chunks(data)
        if not isinstance(content, bytes):
            content = _in_thread(content)
        r = await self.client.put_file(self._require_id(), path, content)
        return WrittenFile(r["path"], r["bytes"], r["sha256"])

    async def read_file(self, path: str) -> bytes:
        """Read a whole file into memory. Use ``download()`` for large files."""
        async with self.client.open_file(self._known_id(), path) as resp:
            return await resp.aread()

    async def download(self, path: str, local: str) -> str:
        """Stream a file or directory out of the sandbox; returns where it went.

        A file goes to ``local``, or into it if that's an existing directory.
        A directory is extracted into ``local``.
        """
        async with self.client.open_file(self._known_id(), path) as resp:
            is_dir = resp.headers.get("content-type", "").startswith("application/x-tar")
            target = _files.download_target(path, local, is_dir)
            if is_dir:
                with tempfile.TemporaryFile() as spool:
                    async for chunk in resp.aiter_bytes(_files.CHUNK_SIZE):
                        await asyncio.to_thread(spool.write, chunk)
                    spool.seek(0)
                    await asyncio.to_thread(_files.extract_tar, spool, target)
                return target
            parent = os.path.dirname(os.path.abspath(target))
            os.makedirs(parent, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=parent, prefix=".download-")
            try:
                with os.fdopen(fd, "wb") as out:
                    async for chunk in resp.aiter_bytes(_files.CHUNK_SIZE):
                        await asyncio.to_thread(out.write, chunk)
                os.replace(tmp, target)
            except BaseException:
                os.unlink(tmp)
                raise
            return target

    async def list_files(self, path: str = "") -> List[FileInfo]:
        """List a directory (the workspace root by default)."""
        return [FileInfo.from_api(e) for e in await self.client.list_files(self._known_id(), path)]

    async def remove_file(self, path: str) -> None:
        """Delete a file, or a directory recursively."""
        await self.client.delete_file(self._require_id(), path)

    async def _upload_entries(self, entries: List[_files.UploadEntry]) -> UploadResult:
        r = await self.client.upload_tar(self._require_id(), _in_thread(_files.tar_chunks(entries)))
        return UploadResult(r["files"], r["bytes"])

    async def _discard(self) -> None:
        """Purge a sandbox that failed to start, ignoring errors."""
        try:
            await self.delete(purge=True)
        except SandboxError:
            pass

    async def __aenter__(self) -> AsyncSandbox:
        if self.sandbox_id is None:
            try:
                await self.create()
            except BaseException:
                # Created but never became ready: don't leak it.
                if self.created:
                    await self._discard()
                await self.aclose()
                raise
        return self

    async def __aexit__(self, *exc: Any) -> None:
        try:
            if self.created:
                await self.delete()
        finally:
            await self.aclose()
