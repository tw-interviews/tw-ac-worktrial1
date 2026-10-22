"""Local-file handling shared by the sync and async SDKs.

Turns a ``{local source: sandbox destination}`` mapping into tar entries,
streams them as a tar archive without reading whole files into memory, and
safely extracts downloaded archives. Touches the local disk, never the network.
"""

from __future__ import annotations

import os
import posixpath
import tarfile
from dataclasses import dataclass
from typing import BinaryIO, Iterator, List, Mapping, Union
from urllib.parse import quote

from .errors import SandboxError

WORKSPACE = "/workspace"
CHUNK_SIZE = 1 << 20


def workspace_path(dest: str) -> str:
    """Normalize a sandbox path to one relative to /workspace ("" is the root).

    Relative paths are relative to /workspace; absolute ones must be inside it.
    """
    if dest.startswith("/"):
        p = posixpath.normpath(dest)
        if p != WORKSPACE and not p.startswith(WORKSPACE + "/"):
            raise ValueError(f"sandbox path {dest!r} must be inside {WORKSPACE} (or relative to it)")
        return p[len(WORKSPACE):].lstrip("/")
    p = posixpath.normpath(dest) if dest else "."
    if p == ".." or p.startswith("../"):
        raise ValueError(f"sandbox path {dest!r} escapes {WORKSPACE}")
    return "" if p == "." else p


def url_path(sandbox_id: str, path: str) -> str:
    return f"/sandboxes/{sandbox_id}/files/" + quote(workspace_path(path), safe="/")


@dataclass(frozen=True)
class UploadEntry:
    local: str
    arcname: str  # relative to /workspace
    is_dir: bool


def plan_upload(files: Mapping[str, str]) -> List[UploadEntry]:
    """Expand ``{local source: destination}`` into tar entries.

    A local directory is uploaded recursively to the destination directory. A
    file goes to the destination path, or into it if the destination ends with
    "/". Raises FileNotFoundError / ValueError before anything is sent.
    """
    entries: List[UploadEntry] = []
    for src, dest in files.items():
        local = os.path.expanduser(os.fspath(src))
        if not os.path.exists(local):
            raise FileNotFoundError(f"local path not found: {src}")
        rel = workspace_path(dest)
        if os.path.isdir(local):
            if rel:
                entries.append(UploadEntry(local, rel, True))
            for root, dirs, names in os.walk(local):
                dirs.sort()
                sub = os.path.relpath(root, local)
                base = rel if sub == "." else posixpath.join(rel, *sub.split(os.sep))
                for d in dirs:
                    entries.append(UploadEntry(os.path.join(root, d), posixpath.join(base, d), True))
                for name in sorted(names):
                    path = os.path.join(root, name)
                    if os.path.isfile(path):  # skips sockets, broken symlinks, ...
                        entries.append(UploadEntry(path, posixpath.join(base, name), False))
        else:
            if not rel or dest.endswith("/"):
                rel = posixpath.join(rel, os.path.basename(local))
            entries.append(UploadEntry(local, rel, False))
    return entries


def tar_chunks(entries: List[UploadEntry]) -> Iterator[bytes]:
    """Yield a tar archive of ``entries`` piece by piece (at most CHUNK_SIZE)."""
    for e in entries:
        st = os.stat(e.local)
        info = tarfile.TarInfo(e.arcname)
        info.mode = st.st_mode & 0o7777
        info.mtime = int(st.st_mtime)
        if e.is_dir:
            info.type = tarfile.DIRTYPE
            yield info.tobuf(format=tarfile.PAX_FORMAT)
            continue
        info.size = st.st_size
        yield info.tobuf(format=tarfile.PAX_FORMAT)
        with open(e.local, "rb") as f:
            remaining = st.st_size
            while remaining:
                chunk = f.read(min(CHUNK_SIZE, remaining))
                if not chunk:
                    raise SandboxError(f"{e.local} shrank while it was being uploaded")
                remaining -= len(chunk)
                yield chunk
        if st.st_size % tarfile.BLOCKSIZE:
            yield b"\0" * (tarfile.BLOCKSIZE - st.st_size % tarfile.BLOCKSIZE)
    yield b"\0" * (2 * tarfile.BLOCKSIZE)


def content_chunks(data: Union[bytes, str, BinaryIO]) -> Union[bytes, Iterator[bytes]]:
    """Request content for a single-file upload: bytes as-is, text as UTF-8,
    a binary file object streamed in chunks."""
    if isinstance(data, str):
        return data.encode()
    if isinstance(data, (bytes, bytearray, memoryview)):
        return bytes(data)

    def read() -> Iterator[bytes]:
        while True:
            chunk = data.read(CHUNK_SIZE)
            if not chunk:
                return
            yield chunk

    return read()


def download_target(remote: str, local: str, is_dir: bool) -> str:
    """Where a download lands: ``local`` itself, or for a file, inside
    ``local`` if that's an existing directory or ends with a separator."""
    local = os.path.expanduser(os.fspath(local))
    if not is_dir and (os.path.isdir(local) or local.endswith(os.sep)):
        name = posixpath.basename(workspace_path(remote)) or "workspace"
        return os.path.join(local, name)
    return local


def extract_tar(fileobj: BinaryIO, dest_dir: str) -> None:
    """Extract a tar stream into dest_dir, refusing entries that would land
    outside it. Only regular files and directories are written."""
    os.makedirs(dest_dir, exist_ok=True)
    root = os.path.realpath(dest_dir)
    with tarfile.open(fileobj=fileobj, mode="r|") as tf:
        for member in tf:
            name = posixpath.normpath(member.name)
            if name in ("", "."):
                continue
            if name.startswith("/") or name == ".." or name.startswith("../"):
                raise SandboxError(f"refusing to extract {member.name!r}: outside the destination")
            target = os.path.join(root, *name.split("/"))
            parent = os.path.dirname(target)
            os.makedirs(parent, exist_ok=True)
            if os.path.commonpath([root, os.path.realpath(parent)]) != root:
                raise SandboxError(f"refusing to extract {member.name!r}: outside the destination")
            if member.isdir():
                os.makedirs(target, exist_ok=True)
            elif member.isfile():
                src = tf.extractfile(member)
                with open(target, "wb") as out:
                    while True:
                        chunk = src.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        out.write(chunk)
                os.chmod(target, member.mode & 0o777)
