from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncIterator, Dict, List, Optional

import httpx

from . import _api

if TYPE_CHECKING:
    from .async_sandbox import AsyncSandbox


class AsyncClient:
    """asyncio HTTP client for the sandbox API; the async twin of :class:`Client`.

    Bound to the event loop it's first used on, so create it inside your async
    code (``async with AsyncClient() as client: ...``) rather than at import time.
    Safe to share between concurrent tasks on that loop.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        *,
        timeout: float = 60.0,
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ):
        self.base_url = _api.resolve_base_url(base_url)
        self._http = httpx.AsyncClient(base_url=self.base_url, timeout=timeout, transport=transport)

    @property
    def closed(self) -> bool:
        return self._http.is_closed

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> AsyncClient:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    async def sandbox(self, sandbox_id: str) -> AsyncSandbox:
        """Attach to an existing sandbox by id."""
        from .async_sandbox import AsyncSandbox

        return await AsyncSandbox.get(sandbox_id, client=self)

    async def sandboxes(self) -> List[AsyncSandbox]:
        """All sandboxes known to the API."""
        from .async_sandbox import AsyncSandbox

        return [AsyncSandbox._from_api(data, self) for data in await self.list_sandboxes()]

    # Raw API calls. Each returns the decoded JSON body.

    async def create_sandbox(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        return await self._send(_api.create_sandbox(spec))

    async def get_sandbox(self, sandbox_id: str) -> Dict[str, Any]:
        return await self._send(_api.get_sandbox(sandbox_id))

    async def list_sandboxes(self) -> List[Dict[str, Any]]:
        return await self._send(_api.list_sandboxes())

    async def delete_sandbox(self, sandbox_id: str, *, purge: bool = False) -> None:
        await self._send(_api.delete_sandbox(sandbox_id, purge=purge))

    async def pause_sandbox(self, sandbox_id: str, *, force: bool = False) -> Dict[str, Any]:
        return await self._send(_api.pause_sandbox(sandbox_id, force=force))

    async def resume_sandbox(self, sandbox_id: str) -> Dict[str, Any]:
        return await self._send(_api.resume_sandbox(sandbox_id))

    async def create_execution(
        self,
        sandbox_id: str,
        command: List[str],
        *,
        stdin: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        return await self._send(
            _api.create_execution(sandbox_id, command, stdin=stdin, timeout_seconds=timeout_seconds)
        )

    async def get_execution(self, execution_id: str) -> Dict[str, Any]:
        return await self._send(_api.get_execution(execution_id))

    async def list_executions(self, sandbox_id: Optional[str] = None) -> List[Dict[str, Any]]:
        return await self._send(_api.list_executions(sandbox_id))

    async def upload_tar(self, sandbox_id: str, content: Any) -> Dict[str, Any]:
        return await self._send(_api.upload_tar(sandbox_id, content))

    async def put_file(self, sandbox_id: str, path: str, content: Any) -> Dict[str, Any]:
        return await self._send(_api.put_file(sandbox_id, path, content))

    async def list_files(self, sandbox_id: str, path: str = "") -> List[Dict[str, Any]]:
        return await self._send(_api.list_files(sandbox_id, path))

    async def delete_file(self, sandbox_id: str, path: str) -> None:
        await self._send(_api.delete_file(sandbox_id, path))

    async def complete_staging(self, sandbox_id: str) -> None:
        await self._send(_api.complete_staging(sandbox_id))

    @asynccontextmanager
    async def open_file(self, sandbox_id: str, path: str) -> AsyncIterator[httpx.Response]:
        """Stream a file (or a directory, as a tar archive) out of the sandbox.

        Yields the response; read it with ``aiter_bytes()``. A directory has
        ``Content-Type: application/x-tar``.
        """
        req = _api.get_file(sandbox_id, path)
        try:
            async with self._http.stream(req.method, req.path, **req.httpx_kwargs()) as resp:
                if resp.is_error:
                    await resp.aread()
                    _api.parse_response(resp, req)
                yield resp
        except httpx.TransportError as e:
            raise _api.connection_error(self.base_url, e) from e

    async def _send(self, req: _api.APIRequest) -> Any:
        try:
            resp = await self._http.request(req.method, req.path, **req.httpx_kwargs())
        except httpx.TransportError as e:
            raise _api.connection_error(self.base_url, e) from e
        return _api.parse_response(resp, req)
