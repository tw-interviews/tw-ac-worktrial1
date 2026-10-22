from datetime import datetime, timezone

import pytest

from sandbox_sdk import (
    AsyncClient,
    AsyncSandbox,
    Client,
    ConflictError,
    QuotaExceededError,
    Sandbox,
    SandboxError,
    SandboxPausedError,
)

from .fake_api import FakeAPI


@pytest.fixture
def api():
    return FakeAPI()


@pytest.fixture
def client(api):
    with Client("http://sandbox.test", transport=api.transport()) as c:
        yield c


@pytest.fixture
async def aclient(api):
    async with AsyncClient("http://sandbox.test", transport=api.async_transport()) as c:
        yield c


def test_paused_ttl_sent(api, client):
    sb = Sandbox("busybox", paused_ttl=3600, client=client).create()
    assert api.requests[0][2] == {"image": "busybox", "paused_ttl_seconds": 3600}
    assert sb.paused_ttl_seconds == 3600 and sb.paused_at is None


def test_pause_and_resume(api, client):
    sb = Sandbox("busybox", client=client).create()
    assert sb.pause(timeout=5) is sb
    assert sb.paused and sb.status == "Paused"
    assert sb.paused_at == datetime(2026, 10, 21, 20, 23, 38, 353954, tzinfo=timezone.utc)
    assert ("POST", f"/sandboxes/{sb.sandbox_id}/pause", None) in api.requests

    with pytest.raises(SandboxPausedError, match="paused") as e:
        sb.exec(["true"])
    assert isinstance(e.value, ConflictError) and e.value.status_code == 409

    assert sb.resume(timeout=5) is sb
    assert sb.ready and not sb.paused and sb.paused_at is None
    assert sb.exec(["true"]).ok


def test_pause_without_wait_reports_pausing(client):
    sb = Sandbox("busybox", client=client).create()
    sb.pause(wait=False)
    assert sb.status == "Pausing" and not sb.paused
    sb.wait_paused(poll_interval=0)
    assert sb.paused


def test_resume_without_wait_reports_resuming(client):
    sb = Sandbox("busybox", client=client).create().pause()
    sb.resume(wait=False)
    assert sb.status == "Resuming"
    sb.wait_ready(poll_interval=0)
    assert sb.ready


def test_pause_and_resume_are_idempotent(client):
    sb = Sandbox("busybox", client=client).create()
    sb.pause().pause()
    assert sb.paused
    sb.resume().resume()
    assert sb.ready


def test_pause_refuses_running_commands_unless_forced(api, client):
    sb = Sandbox("busybox", client=client).create()
    api.sandboxes[sb.sandbox_id]["running"] = 2
    with pytest.raises(ConflictError, match="still running") as e:
        sb.pause()
    assert not isinstance(e.value, SandboxPausedError)
    sb.pause(force=True)
    assert sb.paused and api.sandboxes[sb.sandbox_id]["mode"] == "Suspended"


def test_resume_quota_full(api, client):
    sb = Sandbox("busybox", client=client).create().pause()
    api.resume_quota_full = True
    with pytest.raises(QuotaExceededError):
        sb.resume()
    api.resume_quota_full = False
    assert sb.resume().ready


def test_wait_ready_on_paused_sandbox_raises(client):
    sb = Sandbox("busybox", client=client).create().pause()
    with pytest.raises(SandboxError, match="Paused; call resume"):
        sb.wait_ready(timeout=5)


def test_wait_paused_on_running_sandbox_raises(client):
    sb = Sandbox("busybox", client=client).create()
    with pytest.raises(SandboxError, match="not pausing"):
        sb.wait_paused(timeout=5)


def test_files_work_while_paused(client):
    sb = Sandbox("busybox", client=client).create()
    sb.write_file("a.txt", "before")
    sb.pause()
    assert sb.read_file("a.txt") == b"before"
    sb.write_file("b.txt", "while paused")
    sb.resume()
    assert sb.read_file("b.txt") == b"while paused"


def test_pause_deleted_sandbox_fails(client):
    sb = Sandbox("busybox", client=client).create()
    sb.delete()
    with pytest.raises(SandboxError, match="has been deleted"):
        sb.pause()


async def test_async_pause_and_resume(api, aclient):
    sb = await AsyncSandbox("busybox", paused_ttl=600, client=aclient).create()
    await sb.pause(timeout=5)
    assert sb.paused
    with pytest.raises(SandboxPausedError):
        await sb.exec(["true"])
    await sb.write_file("while-paused.txt", "x")
    assert await sb.read_file("while-paused.txt") == b"x"
    await sb.resume(timeout=5)
    assert sb.ready and (await sb.exec(["true"])).ok


async def test_async_pause_many_concurrently(api, aclient):
    import asyncio

    sbs = await asyncio.gather(*(AsyncSandbox("busybox", client=aclient).create() for _ in range(4)))
    await asyncio.gather(*(sb.pause() for sb in sbs))
    assert all(sb.paused for sb in sbs)
    await asyncio.gather(*(sb.resume() for sb in sbs))
    assert all(sb.ready for sb in sbs)


async def test_async_force_and_quota(api, aclient):
    sb = await AsyncSandbox("busybox", client=aclient).create()
    api.sandboxes[sb.sandbox_id]["running"] = 1
    with pytest.raises(ConflictError):
        await sb.pause()
    await sb.pause(force=True)
    api.resume_quota_full = True
    with pytest.raises(QuotaExceededError):
        await sb.resume()
