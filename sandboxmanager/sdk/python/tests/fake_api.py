"""In-memory stand-in for the Go sandbox API, served through httpx.MockTransport."""

from __future__ import annotations

import asyncio
import io
import json
import posixpath
import tarfile
import uuid
from typing import Any, Dict, List, Tuple

import httpx

NOW = "2026-10-21T20:23:38.353954123Z"
EXTENDED = "2026-10-21T20:30:00Z"
RETAINED = "2026-10-22T08:23:38Z"


class FakeAPI:
    def __init__(self, ready_after_polls: int = 1, quota: int = 100):
        self.quota = quota
        self.sandboxes: Dict[str, Dict[str, Any]] = {}
        self.executions: Dict[str, Dict[str, Any]] = {}
        self.ready_after_polls = ready_after_polls
        self.polls: Dict[str, int] = {}
        self.requests: List[Tuple[str, str, Any]] = []
        self.in_flight = 0
        self.max_in_flight = 0
        self.fail_uploads = False  # simulate a full volume (507) on uploads
        self.resume_quota_full = False  # simulate a full quota (429) on resume

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle)

    def async_transport(self, exec_delay: float = 0.0) -> httpx.MockTransport:
        """For AsyncClient. Each execution takes exec_delay seconds, so tests can
        tell concurrent execs (total ~delay) from serial ones (~n * delay)."""

        async def handle(request: httpx.Request) -> httpx.Response:
            await request.aread()
            if request.method == "POST" and request.url.path == "/sandbox/executions":
                self.in_flight += 1
                self.max_in_flight = max(self.max_in_flight, self.in_flight)
                try:
                    await asyncio.sleep(exec_delay)
                finally:
                    self.in_flight -= 1
            return self.handle(request)

        return httpx.MockTransport(handle)

    def handle(self, request: httpx.Request) -> httpx.Response:
        raw = request.read()
        is_json = request.headers.get("content-type", "").startswith("application/json")
        body = json.loads(raw) if raw and is_json else raw or None
        path = request.url.path
        self.requests.append((request.method, path, body))
        parts = path.strip("/").split("/")

        if parts[0] == "sandboxes" and len(parts) == 3 and parts[2] in ("pause", "resume") and request.method == "POST":
            return self._mode(request, parts[1], parts[2])

        if parts[0] == "sandboxes" and len(parts) >= 3 and parts[2] in ("files", "staging"):
            return self._files(request, parts[1], parts[2:], raw)

        if request.method == "POST" and path == "/sandboxes":
            if not body.get("image"):
                return _error(400, "image is required and must not contain whitespace")
            if len(self.sandboxes) >= self.quota:
                return _error(429, "sandbox quota exceeded; delete sandboxes or request fewer resources")
            sid = str(uuid.uuid4())
            self.sandboxes[sid] = {
                "sandbox_id": sid,
                "name": f"sbx-{sid}",
                "namespace": "sandboxes",
                "definition": f"kind: Sandbox\nimage: {body['image']}\n",
                "created_at": NOW,
                "ttl_seconds": body.get("ttl_seconds", 300),
                "paused_ttl_seconds": body.get("paused_ttl_seconds", 86400),
                "paused_at": None,
                "mode": "Running",
                "resumed": False,
                "mode_polls": 0,  # status polls since the last pause/resume
                "running": 0,  # tests set this to simulate in-flight commands
                "expires_at": "2026-10-21T20:28:39Z",
                "volume": None,
                "volume_size": "1Gi",
                "snapshot_at": None,
                "snapshot_bytes": None,
                "deleted_at": None,
                "files_retained_until": None,
                "spec": body,
                "staged": not body.get("stage_files"),
                "files": {},  # path relative to /workspace -> (bytes, mode)
                "dirs": set(),
            }
            self.polls[sid] = 0
            return httpx.Response(201, json=self._public(sid))

        if request.method == "GET" and path == "/sandboxes":
            return httpx.Response(200, json=[self._public(sid, with_status=True) for sid in self.sandboxes])

        if parts[0] == "sandboxes" and len(parts) == 2:
            sid = parts[1]
            if sid not in self.sandboxes:
                return _error(404, "sandbox not found")
            if request.method == "GET":
                self.polls[sid] += 1
                out = self._public(sid, with_status=True)
                self.sandboxes[sid]["mode_polls"] += 1
                return httpx.Response(200, json=out)
            if request.method == "DELETE":
                if request.url.params.get("purge") == "true":
                    del self.sandboxes[sid]
                    self.executions = {k: v for k, v in self.executions.items() if v["sandbox_id"] != sid}
                else:
                    self.sandboxes[sid]["deleted_at"] = NOW
                    self.sandboxes[sid]["files_retained_until"] = RETAINED
                return httpx.Response(204)

        if path == "/sandbox/executions" and request.method == "POST":
            sid = body["sandbox_id"]
            if sid not in self.sandboxes:
                return _error(404, "sandbox not found")
            if self.sandboxes[sid]["deleted_at"]:
                return _error(409, "sandbox no longer exists in the cluster (expired or deleted)")
            if self.sandboxes[sid]["mode"] == "Suspended":
                return httpx.Response(409, json={"error": "sandbox is paused; resume it with POST /sandboxes/{id}/resume",
                                                 "code": "sandbox_paused"})
            if self.polls[sid] < self.ready_after_polls or not self.sandboxes[sid]["staged"]:
                return _error(409, "sandbox is not ready")
            argv = body["command"]
            eid = str(uuid.uuid4())
            stdout, stderr, code, status = _run(argv, body.get("stdin"))
            self.executions[eid] = {
                "execution_id": eid,
                "sandbox_id": sid,
                "pod": f"sbx-{sid}",
                "command": argv,
                "status": status,
                "exit_code": code,
                "stdout": stdout,
                "stderr": stderr,
                "stdout_truncated": False,
                "stderr_truncated": False,
                "started_at": NOW,
                "finished_at": NOW,
            }
            if status == "error":
                return httpx.Response(502, json={"error": "failed to execute command in sandbox", "execution_id": eid})
            # Each execution refreshes the TTL; the POST response reports the new expiry.
            self.sandboxes[sid]["expires_at"] = EXTENDED
            return httpx.Response(200, json=dict(self.executions[eid], sandbox_expires_at=EXTENDED))

        if path == "/sandbox/executions" and request.method == "GET":
            sid = request.url.params.get("sandbox_id")
            items = [dict(e, stdout="", stderr="") for e in self.executions.values() if sid in (None, e["sandbox_id"])]
            return httpx.Response(200, json=list(reversed(items)))

        if parts[:2] == ["sandbox", "executions"] and len(parts) == 3 and request.method == "GET":
            if parts[2] not in self.executions:
                return _error(404, "execution not found")
            return httpx.Response(200, json=self.executions[parts[2]])

        return _error(404, "404 page not found")

    def _public(self, sid: str, with_status: bool = False) -> Dict[str, Any]:
        sb = self.sandboxes[sid]
        hidden = ("spec", "staged", "files", "dirs", "mode", "resumed", "mode_polls", "running")
        out = {k: v for k, v in sb.items() if k not in hidden}
        if with_status:
            if sb["deleted_at"]:
                out["status"] = "Deleted"
            elif sb["mode"] == "Suspended":
                out["status"] = "Paused" if sb["mode_polls"] >= 1 else "Pausing"
            elif sb["resumed"] and sb["mode_polls"] < 1:
                out["status"] = "Resuming"
            elif not sb["staged"]:
                out["status"] = "Staging"
            else:
                out["status"] = "Ready" if self.polls[sid] >= self.ready_after_polls else "NotReady"
        elif not sb["staged"]:
            out["status"] = "Staging"
        return out

    def _mode(self, request: httpx.Request, sid: str, action: str) -> httpx.Response:
        """POST /sandboxes/{id}/pause and /resume. A pause finishes (and a
        resumed sandbox becomes Ready) after one status poll."""
        if sid not in self.sandboxes:
            return _error(404, "sandbox not found")
        sb = self.sandboxes[sid]
        if sb["deleted_at"]:
            return _error(409, "sandbox no longer exists in the cluster (expired or deleted)")
        want = "Suspended" if action == "pause" else "Running"
        if sb["mode"] != want:
            if action == "pause":
                if sb["running"] and request.url.params.get("force") != "true":
                    return _error(409, f"{sb['running']} command(s) still running in the sandbox")
                sb["paused_at"], sb["expires_at"], sb["running"] = NOW, RETAINED, 0
            else:
                if self.resume_quota_full:
                    return _error(429, "sandbox quota exceeded; delete sandboxes or request fewer resources")
                sb["paused_at"], sb["expires_at"], sb["resumed"] = None, EXTENDED, True
            sb["mode"], sb["mode_polls"] = want, 0
        return httpx.Response(202, json=self._public(sid, with_status=True))

    def _files(self, request: httpx.Request, sid: str, rest: List[str], raw: bytes) -> httpx.Response:
        """/sandboxes/{id}/files[/{path}] and /sandboxes/{id}/staging/complete."""
        if sid not in self.sandboxes:
            return _error(404, "sandbox not found")
        sb = self.sandboxes[sid]
        files, dirs = sb["files"], sb["dirs"]
        if rest == ["staging", "complete"]:
            sb["staged"] = True
            return httpx.Response(204)
        rel = posixpath.normpath("/" + "/".join(rest[1:])).lstrip("/")
        method = request.method

        if method == "POST" and not rel:
            if self.fail_uploads:
                return _error(507, "sandbox volume is full")
            n = total = 0
            with tarfile.open(fileobj=io.BytesIO(raw), mode="r|") as tf:
                for m in tf:
                    name = posixpath.normpath("/" + m.name).lstrip("/")
                    if m.isdir():
                        dirs.add(name)
                    elif m.isfile():
                        data = tf.extractfile(m).read()
                        files[name] = (data, m.mode)
                        n, total = n + 1, total + len(data)
                    else:
                        return _error(400, "only regular files and directories can be uploaded")
            return httpx.Response(200, json={"files": n, "bytes": total})
        if method == "PUT":
            if self.fail_uploads:
                return _error(507, "sandbox volume is full")
            files[rel] = (raw, 0o666)
            import hashlib
            return httpx.Response(200, json={"path": "/workspace/" + rel, "bytes": len(raw),
                                             "sha256": hashlib.sha256(raw).hexdigest()})
        if method == "DELETE":
            hit = [k for k in files if k == rel or k.startswith(rel + "/")]
            if not hit and rel not in dirs:
                return _error(404, "no such file or directory")
            for k in hit:
                del files[k]
            dirs.discard(rel)
            return httpx.Response(204)
        if method == "GET":
            is_dir = rel == "" or rel in dirs or any(k.startswith(rel + "/") for k in files)
            if "list" in request.url.params:
                if not is_dir:
                    return _error(404 if rel not in files else 400, "not a directory")
                prefix = rel + "/" if rel else ""
                names = {}
                for k, (data, mode) in files.items():
                    if k.startswith(prefix):
                        head = k[len(prefix):].split("/")
                        names[head[0]] = ("file", len(data), oct(mode)[2:]) if len(head) == 1 else ("directory", 4096, "777")
                for d in dirs:
                    if d.startswith(prefix) and "/" not in d[len(prefix):]:
                        names.setdefault(d[len(prefix):], ("directory", 4096, "777"))
                return httpx.Response(200, json=[
                    {"name": k, "type": t, "size": sz, "mode": md, "modified": "2026-10-21T20:23:38Z"}
                    for k, (t, sz, md) in sorted(names.items())])
            if rel in files:
                return httpx.Response(200, content=files[rel][0], headers={"content-type": "application/octet-stream"})
            if is_dir:
                buf = io.BytesIO()
                prefix = rel + "/" if rel else ""
                with tarfile.open(fileobj=buf, mode="w") as tf:
                    for k, (data, mode) in sorted(files.items()):
                        if k.startswith(prefix):
                            ti = tarfile.TarInfo("./" + k[len(prefix):])
                            ti.size, ti.mode = len(data), mode
                            tf.addfile(ti, io.BytesIO(data))
                return httpx.Response(200, content=buf.getvalue(), headers={"content-type": "application/x-tar"})
            return _error(404, "no such file or directory")
        return _error(405, "method not allowed")


def _run(argv: List[str], stdin: Any) -> Tuple[str, str, Any, str]:
    """Pretend to run a few shell-ish commands."""
    if argv == ["sh", "-c", "exit 3"]:
        return "", "boom\n", 3, "completed"
    if argv == ["sleep", "999"]:
        return "", "", None, "timed_out"
    if argv == ["explode"]:
        return "", "", None, "error"
    if argv and argv[0] == "cat":
        return stdin or "", "", 0, "completed"
    return " ".join(argv) + "\n", "", 0, "completed"


def _error(status: int, message: str) -> httpx.Response:
    return httpx.Response(status, json={"error": message})
