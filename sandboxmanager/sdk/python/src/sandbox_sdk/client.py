from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional

import httpx

from . import _api
from ._api import DEFAULT_BASE_URL  # noqa: F401  (re-exported)

if TYPE_CHECKING:
    from .sandbox import Sandbox


class Client:
    """HTTP client for the sandbox API.

    The base URL defaults to ``$SANDBOX_API_URL``, then ``http://localhost:8080``
    (e.g. ``kubectl -n webapp port-forward svc/sandbox-manager 8080:80``).

    Most code uses :class:`~sandbox_sdk.Sandbox` and never touches this
    directly; the methods here are the thin, dict-in/dict-out API layer.
    See :class:`~sandbox_sdk.AsyncClient` for the asyncio version.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        *,
        timeout: float = 60.0,
        transport: Optional[httpx.BaseTransport] = None,
    ):
        self.base_url = _api.resolve_base_url(base_url)
        self._http = httpx.Client(base_url=self.base_url, timeout=timeout, transport=transport)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def sandbox(self, sandbox_id: str) -> Sandbox:
        """Attach to an existing sandbox by id."""
        from .sandbox import Sandbox

        return Sandbox.get(sandbox_id, client=self)

    def sandboxes(self) -> List[Sandbox]:
        """All sandboxes known to the API."""
        from .sandbox import Sandbox

        return [Sandbox._from_api(data, self) for data in self.list_sandboxes()]

    # Raw API calls. Each returns the decoded JSON body.

    def create_sandbox(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        return self._send(_api.create_sandbox(spec))

    def get_sandbox(self, sandbox_id: str) -> Dict[str, Any]:
        return self._send(_api.get_sandbox(sandbox_id))

    def list_sandboxes(self) -> List[Dict[str, Any]]:
        return self._send(_api.list_sandboxes())

    def delete_sandbox(self, sandbox_id: str, *, purge: bool = False) -> None:
        self._send(_api.delete_sandbox(sandbox_id, purge=purge))

    def pause_sandbox(self, sandbox_id: str, *, force: bool = False) -> Dict[str, Any]:
        return self._send(_api.pause_sandbox(sandbox_id, force=force))

    def resume_sandbox(self, sandbox_id: str) -> Dict[str, Any]:
        return self._send(_api.resume_sandbox(sandbox_id))

    def create_execution(
        self,
        sandbox_id: str,
        command: List[str],
        *,
        stdin: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        return self._send(_api.create_execution(sandbox_id, command, stdin=stdin, timeout_seconds=timeout_seconds))

    def get_execution(self, execution_id: str) -> Dict[str, Any]:
        return self._send(_api.get_execution(execution_id))

    def list_executions(self, sandbox_id: Optional[str] = None) -> List[Dict[str, Any]]:
        return self._send(_api.list_executions(sandbox_id))

    def upload_tar(self, sandbox_id: str, content: Any) -> Dict[str, Any]:
        return self._send(_api.upload_tar(sandbox_id, content))

    def put_file(self, sandbox_id: str, path: str, content: Any) -> Dict[str, Any]:
        return self._send(_api.put_file(sandbox_id, path, content))

    def list_files(self, sandbox_id: str, path: str = "") -> List[Dict[str, Any]]:
        return self._send(_api.list_files(sandbox_id, path))

    def delete_file(self, sandbox_id: str, path: str) -> None:
        self._send(_api.delete_file(sandbox_id, path))

    def complete_staging(self, sandbox_id: str) -> None:
        self._send(_api.complete_staging(sandbox_id))

    @contextmanager
    def open_file(self, sandbox_id: str, path: str) -> Iterator[httpx.Response]:
        """Stream a file (or a directory, as a tar archive) out of the sandbox.

        Yields the response; read it with ``iter_bytes()``. A directory has
        ``Content-Type: application/x-tar``.
        """
        req = _api.get_file(sandbox_id, path)
        try:
            with self._http.stream(req.method, req.path, **req.httpx_kwargs()) as resp:
                if resp.is_error:
                    resp.read()
                    _api.parse_response(resp, req)
                yield resp
        except httpx.TransportError as e:
            raise _api.connection_error(self.base_url, e) from e

    def _send(self, req: _api.APIRequest) -> Any:
        try:
            resp = self._http.request(req.method, req.path, **req.httpx_kwargs())
        except httpx.TransportError as e:
            raise _api.connection_error(self.base_url, e) from e
        return _api.parse_response(resp, req)


_default_client: Optional[Client] = None


def default_client() -> Client:
    """The shared client used when a Sandbox isn't given one explicitly."""
    global _default_client
    if _default_client is None:
        _default_client = Client()
    return _default_client
