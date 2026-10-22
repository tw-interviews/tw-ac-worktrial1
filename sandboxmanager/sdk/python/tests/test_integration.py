"""End-to-end tests against a running sandbox API.

    kubectl -n webapp port-forward svc/sandbox-manager 8080:80
    SANDBOX_API_URL=http://localhost:8080 uv run pytest -m integration
"""

import asyncio
import hashlib
import os
import time

import pytest

from sandbox_sdk import AsyncClient, AsyncSandbox, ConflictError, NotFoundError, Resources, Sandbox, SandboxPausedError

pytestmark = pytest.mark.integration


def test_lifecycle():
    sb = Sandbox(
        "busybox:1.36",
        command=["sleep", "infinity"],
        env={"GREETING": "hello"},
        resources=Resources(requests={"cpu": "50m", "memory": "32Mi"}, limits={"memory": "64Mi"}),
    )
    with sb:
        assert sb.ready and sb.name == f"sbx-{sb.sandbox_id}"
        assert "agents.x-k8s.io/v1beta1" in sb.definition

        cmd = sb.exec("echo $GREETING world")
        assert cmd.check().stdout == "hello world\n"

        failing = sb.exec("echo nope >&2; exit 7")
        assert failing.exit_code == 7 and failing.stderr == "nope\n" and not failing.ok

        assert sb.exec(["tr", "a-z", "A-Z"], stdin="shout").stdout == "SHOUT"
        assert sb.exec(["sleep", "5"], timeout=1).timed_out

        history = sb.commands()
        assert len(history) == 4
        assert history[-1].execution_id == cmd.execution_id
        assert history[-1].stdout == "hello world\n"  # lazily fetched
        assert sb.get_command(failing.execution_id).exit_code == 7

    assert sb.deleted
    gone = Sandbox.get(sb.sandbox_id)  # kept for the recovery window
    assert gone.status == "Deleted" and gone.files_retained_until is not None
    gone.delete(purge=True)
    with pytest.raises(NotFoundError):
        Sandbox.get(sb.sandbox_id)


async def test_async_lifecycle_and_concurrency():
    async with AsyncClient() as client:
        async with AsyncSandbox("busybox:1.36", command=["sleep", "infinity"], client=client) as sb:
            assert sb.ready
            start = time.monotonic()
            cmds = await asyncio.gather(*(sb.exec(f"sleep 2; echo {i}") for i in range(3)))
            assert time.monotonic() - start < 5  # serial would take >= 6s
            assert sorted(c.stdout for c in cmds) == ["0\n", "1\n", "2\n"]

            history = await sb.commands(with_output=True)
            assert sorted(c.stdout for c in history) == ["0\n", "1\n", "2\n"]
        assert sb.deleted
        await sb.delete(purge=True)


def test_exec_before_ready_conflicts():
    sb = Sandbox("busybox:1.36", command=["sleep", "infinity"]).create(wait=False)
    try:
        with pytest.raises(ConflictError):
            sb.exec(["true"])
    finally:
        sb.delete(purge=True)


def test_files(tmp_path):
    (tmp_path / "app").mkdir()
    (tmp_path / "app" / "main.py").write_text("print('hello')\n")
    (tmp_path / "app" / "run.sh").write_text("#!/bin/sh\necho ran\n")
    (tmp_path / "app" / "run.sh").chmod(0o755)
    blob = os.urandom(20 << 20)
    (tmp_path / "blob.bin").write_bytes(blob)

    # The command copies a staged file at startup, proving files= lands first.
    sb = Sandbox(
        "busybox:1.36",
        command=["sh", "-c", "cp /workspace/app/main.py /workspace/seen-at-start.txt; exec sleep infinity"],
        files={str(tmp_path / "app"): "app", str(tmp_path / "blob.bin"): "/workspace/data/"},
    )
    try:
        sb.create()
        assert sb.volume is None and sb.volume_size == "1Gi"
        assert sb.read_file("seen-at-start.txt") == b"print('hello')\n"
        assert sb.exec("/workspace/app/run.sh").check().stdout == "ran\n"  # mode kept
        assert sb.exec("sha256sum /workspace/data/blob.bin").stdout.split()[0] == hashlib.sha256(blob).hexdigest()

        sb.exec("mkdir -p /workspace/out && echo made-inside > /workspace/out/r.txt").check()
        assert {e.name for e in sb.list_files()} == {"app", "data", "out", "seen-at-start.txt"}
        assert sb.download("out", str(tmp_path / "out")) == str(tmp_path / "out")
        assert (tmp_path / "out" / "r.txt").read_text() == "made-inside\n"
        got = sb.download("data/blob.bin", str(tmp_path / "blob-copy.bin"))
        assert open(got, "rb").read() == blob

        w = sb.write_file("note.txt", "hi")
        assert w.sha256 == hashlib.sha256(b"hi").hexdigest()
        sb.remove_file("note.txt")
        with pytest.raises(NotFoundError):
            sb.read_file("note.txt")

        sb.delete()  # files stay recoverable
        assert sb.read_file("out/r.txt") == b"made-inside\n"
        assert Sandbox.get(sb.sandbox_id).status == "Deleted"
    finally:
        sb.delete(purge=True)


async def test_async_files(tmp_path):
    (tmp_path / "in.txt").write_text("async in\n")
    async with AsyncClient() as client:
        sbs = [
            AsyncSandbox("busybox:1.36", command=["sleep", "infinity"], files={str(tmp_path / "in.txt"): f"in-{i}.txt"},
                         client=client)
            for i in range(3)
        ]
        await asyncio.gather(*(sb.create() for sb in sbs))
        try:
            for i, sb in enumerate(sbs):
                assert await sb.read_file(f"in-{i}.txt") == b"async in\n"
            await asyncio.gather(*(sb.write_file("out.txt", sb.sandbox_id) for sb in sbs))
            outs = await asyncio.gather(*(sb.read_file("out.txt") for sb in sbs))
            assert [o.decode() for o in outs] == [sb.sandbox_id for sb in sbs]  # each has its own workspace
        finally:
            await asyncio.gather(*(sb.delete(purge=True) for sb in sbs))


def test_pause_and_resume():
    import threading

    sb = Sandbox("busybox:1.36", command=["sleep", "infinity"], paused_ttl=600)
    try:
        sb.create()
        assert sb.paused_ttl_seconds == 600
        sb.exec("echo kept > /workspace/f; echo lost > /tmp/f").check()

        sb.pause()
        assert sb.paused and sb.paused_at is not None
        with pytest.raises(SandboxPausedError):
            sb.exec("true")
        assert sb.read_file("f") == b"kept\n"  # served from the pause's snapshot
        sb.write_file("while-paused.txt", "hi")

        sb.resume()
        assert sb.ready and sb.paused_at is None
        out = sb.exec("cat /workspace/f /workspace/while-paused.txt; ls /tmp/f 2>/dev/null || echo tmp-gone").stdout
        assert out == "kept\nhitmp-gone\n"  # /workspace survives, the rest of the container doesn't

        # A pause waits for running commands unless forced, which kills them.
        result = {}
        t = threading.Thread(target=lambda: result.setdefault("cmd", sb.exec("sleep 30", timeout=60)))
        t.start()
        time.sleep(2)
        with pytest.raises(ConflictError, match="still running"):
            sb.pause()
        sb.pause(force=True)
        t.join()
        assert result["cmd"].exit_code == 137
        assert sb.paused
    finally:
        sb.delete(purge=True)


async def test_async_pause_many():
    async with AsyncClient() as client:
        sbs = [AsyncSandbox("busybox:1.36", command=["sleep", "infinity"], client=client) for _ in range(3)]
        await asyncio.gather(*(sb.create() for sb in sbs))
        try:
            await asyncio.gather(*(sb.write_file("id.txt", sb.sandbox_id) for sb in sbs))
            await asyncio.gather(*(sb.pause() for sb in sbs))
            assert all(sb.paused for sb in sbs)
            await asyncio.gather(*(sb.resume() for sb in sbs))
            outs = await asyncio.gather(*(sb.exec("cat /workspace/id.txt") for sb in sbs))
            assert [o.stdout for o in outs] == [sb.sandbox_id for sb in sbs]
        finally:
            await asyncio.gather(*(sb.delete(purge=True) for sb in sbs))
