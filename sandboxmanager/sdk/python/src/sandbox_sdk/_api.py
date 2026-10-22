"""The HTTP API as data, shared by the sync and async clients.

Each function returns an APIRequest describing one call; the clients only
send it and pass the response to parse_response. Keeping this free of I/O
means Client and AsyncClient can't drift apart.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx

from . import _files
from .errors import SandboxConnectionError, error_for_response

DEFAULT_BASE_URL = "http://localhost:8080"

# The execution endpoint holds the request open until the command finishes
# (30s server default), so allow that plus some slack.
DEFAULT_EXEC_TIMEOUT = 30
EXEC_HTTP_SLACK = 30


def resolve_base_url(base_url: Optional[str]) -> str:
    return base_url or os.environ.get("SANDBOX_API_URL") or DEFAULT_BASE_URL


@dataclass(frozen=True)
class APIRequest:
    method: str
    path: str
    json: Any = None
    params: Optional[Dict[str, str]] = None
    timeout: Optional[float] = None
    # Raw request body (bytes, or an iterator of bytes to stream it).
    content: Any = None
    headers: Optional[Dict[str, str]] = None

    def httpx_kwargs(self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {"json": self.json, "params": self.params}
        if self.timeout is not None:
            kwargs["timeout"] = self.timeout
        if self.content is not None:
            kwargs["content"] = self.content
        if self.headers:
            kwargs["headers"] = self.headers
        return kwargs


def create_sandbox(spec: Dict[str, Any]) -> APIRequest:
    return APIRequest("POST", "/sandboxes", json=spec)


def get_sandbox(sandbox_id: str) -> APIRequest:
    return APIRequest("GET", f"/sandboxes/{sandbox_id}")


def list_sandboxes() -> APIRequest:
    return APIRequest("GET", "/sandboxes")


def delete_sandbox(sandbox_id: str, *, purge: bool = False) -> APIRequest:
    return APIRequest("DELETE", f"/sandboxes/{sandbox_id}", params={"purge": "true"} if purge else None)


def pause_sandbox(sandbox_id: str, *, force: bool = False) -> APIRequest:
    return APIRequest("POST", f"/sandboxes/{sandbox_id}/pause", params={"force": "true"} if force else None)


def resume_sandbox(sandbox_id: str) -> APIRequest:
    return APIRequest("POST", f"/sandboxes/{sandbox_id}/resume")


# Files in a sandbox's /workspace. Paths are relative to /workspace.


def upload_tar(sandbox_id: str, content: Any) -> APIRequest:
    return APIRequest("POST", f"/sandboxes/{sandbox_id}/files", content=content,
                      headers={"content-type": "application/x-tar"})


def put_file(sandbox_id: str, path: str, content: Any) -> APIRequest:
    return APIRequest("PUT", _files.url_path(sandbox_id, path), content=content,
                      headers={"content-type": "application/octet-stream"})


def get_file(sandbox_id: str, path: str) -> APIRequest:
    return APIRequest("GET", _files.url_path(sandbox_id, path))


def list_files(sandbox_id: str, path: str) -> APIRequest:
    return APIRequest("GET", _files.url_path(sandbox_id, path), params={"list": ""})


def delete_file(sandbox_id: str, path: str) -> APIRequest:
    return APIRequest("DELETE", _files.url_path(sandbox_id, path))


def complete_staging(sandbox_id: str) -> APIRequest:
    return APIRequest("POST", f"/sandboxes/{sandbox_id}/staging/complete")


def create_execution(
    sandbox_id: str,
    command: List[str],
    *,
    stdin: Optional[str] = None,
    timeout_seconds: Optional[int] = None,
) -> APIRequest:
    body: Dict[str, Any] = {"sandbox_id": sandbox_id, "command": command}
    if stdin is not None:
        body["stdin"] = stdin
    if timeout_seconds is not None:
        body["timeout_seconds"] = timeout_seconds
    http_timeout = (timeout_seconds or DEFAULT_EXEC_TIMEOUT) + EXEC_HTTP_SLACK
    return APIRequest("POST", "/sandbox/executions", json=body, timeout=http_timeout)


def get_execution(execution_id: str) -> APIRequest:
    return APIRequest("GET", f"/sandbox/executions/{execution_id}")


def list_executions(sandbox_id: Optional[str] = None) -> APIRequest:
    params = {"sandbox_id": sandbox_id} if sandbox_id else None
    return APIRequest("GET", "/sandbox/executions", params=params)


def parse_response(resp: httpx.Response, req: APIRequest) -> Any:
    """Return the decoded JSON body, or raise the matching APIError."""
    if resp.status_code == 204:
        return None
    try:
        body = resp.json()
    except ValueError:
        body = None
    if resp.is_error:
        raise error_for_response(resp.status_code, body, req.path)
    return body


def connection_error(base_url: str, err: Exception) -> SandboxConnectionError:
    return SandboxConnectionError(f"cannot reach sandbox API at {base_url}: {err}")
