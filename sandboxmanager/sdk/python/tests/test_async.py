import asyncio
import time

import httpx
import pytest

from sandbox_sdk import (
    AsyncClient,
    AsyncCommand,
    AsyncSandbox,
    CommandError,
    ConflictError,
    ExecutionFailedError,
    NotFoundError,
    OutputNotLoadedError,
    Resources,
    SandboxConnectionError,
    SandboxError,
    SandboxTimeoutError,
)

from .fake_api import FakeAPI


@pytest.fixture
def api():
    return FakeAPI()


@pytest.fixture
async def client(api):
    async with AsyncClient("http://sandbox.test", transport=api.async_transport()) as c:
        yield c


async def test_create_sends_spec_and_waits_for_ready(api, client):
    sb = AsyncSandbox(
        "busybox:1.36",
        command=["sleep", "infinity"],
        env={"MODE": "test"},
        resources=Resources(limits={"memory": "64Mi"}),
        client=client,
    )
    assert await sb.create(timeout=5) is sb
    assert api.requests[0] == (
        "POST",
        "/sandboxes",
        {
            "image": "busybox:1.36",
            "command": ["sleep", "infinity"],
            "env": {"MODE": "test"},
            "resources": {"limits": {"memory": "64Mi"}},
        },
    )
    assert sb.created and sb.ready and sb.name == f"sbx-{sb.sandbox_id}"


async def test_ttl_and_concurrent_start_delete(api, client):
    # Start several sandboxes without waiting, await readiness together, delete together.
    sbs = [AsyncSandbox("busybox", ttl=60, client=client) for _ in range(5)]
    await asyncio.gather(*(sb.create(wait=False) for sb in sbs))
    assert all(sb.created and sb.ttl_seconds == 60 for sb in sbs)
    await asyncio.gather(*(sb.wait_ready(poll_interval=0) for sb in sbs))
    cmd = await sbs[0].exec(["true"])
    assert cmd.ok and sbs[0].expires_at.minute == 30  # refreshed by the exec
    await asyncio.gather(*(sb.delete() for sb in sbs))
    assert all(sb.deleted and api.sandboxes[sb.sandbox_id]["deleted_at"] for sb in sbs)


async def test_wait_ready_polls_then_times_out():
    api = FakeAPI(ready_after_polls=3)
    async with AsyncClient("http://sandbox.test", transport=api.async_transport()) as c:
        sb = await AsyncSandbox("busybox", client=c).create(wait=False)
        await sb.wait_ready(timeout=5, poll_interval=0)
        assert sb.ready and api.polls[sb.sandbox_id] == 3

    api = FakeAPI(ready_after_polls=10**9)
    async with AsyncClient("http://sandbox.test", transport=api.async_transport()) as c:
        sb = await AsyncSandbox("busybox", client=c).create(wait=False)
        with pytest.raises(SandboxTimeoutError):
            await sb.wait_ready(timeout=0.05, poll_interval=0.01)


async def test_exec_returns_loaded_command(client):
    sb = await AsyncSandbox("busybox", client=client).create()
    cmd = await sb.exec(["echo", "hi"])
    assert isinstance(cmd, AsyncCommand)
    assert cmd.ok and cmd.stdout == "echo hi\n" and cmd.argv == ["echo", "hi"]

    failed = await sb.exec("exit 3")
    assert failed.exit_code == 3 and failed.stderr == "boom\n"
    with pytest.raises(CommandError):
        failed.check()
    assert (await sb.exec(["sleep", "999"])).timed_out


async def test_concurrent_execs_overlap():
    api = FakeAPI()
    async with AsyncClient("http://sandbox.test", transport=api.async_transport(exec_delay=0.2)) as c:
        sb = await AsyncSandbox("busybox", client=c).create()
        start = time.monotonic()
        cmds = await asyncio.gather(*(sb.exec(["echo", str(i)]) for i in range(5)))
        elapsed = time.monotonic() - start

    assert [c.stdout for c in cmds] == [f"echo {i}\n" for i in range(5)]
    assert api.max_in_flight == 5
    assert elapsed < 0.6  # serial would be >= 1.0s


async def test_commands_need_explicit_output_load(api, client):
    sb = await AsyncSandbox("busybox", client=client).create()
    await sb.exec(["echo", "one"])
    await sb.exec(["cat"], stdin="two")

    cmds = await sb.commands()
    assert [c.argv for c in cmds] == [["cat"], ["echo", "one"]]
    assert not cmds[0].output_loaded
    assert cmds[0].status == "completed"  # metadata is available without output
    with pytest.raises(OutputNotLoadedError, match="await cmd.refresh"):
        cmds[0].stdout
    assert (await cmds[0].refresh()).stdout == "two"

    loaded = await sb.commands(with_output=True)
    assert [c.stdout for c in loaded] == ["two", "echo one\n"]


async def test_get_command_and_ownership(client):
    a = await AsyncSandbox("busybox", client=client).create()
    b = await AsyncSandbox("busybox", client=client).create()
    cmd = await a.exec(["true"])
    assert (await a.get_command(cmd.execution_id)).ok
    with pytest.raises(SandboxError, match="belongs to sandbox"):
        await b.get_command(cmd.execution_id)


async def test_errors_map_like_sync(api, client):
    sb = await AsyncSandbox("busybox", client=client).create(wait=False)
    with pytest.raises(ConflictError):
        await sb.exec(["true"])
    await sb.wait_ready(poll_interval=0)
    with pytest.raises(ExecutionFailedError) as e:
        await sb.exec(["explode"])
    assert e.value.execution_id in api.executions
    with pytest.raises(NotFoundError):
        await AsyncSandbox.get("00000000-0000-0000-0000-000000000000", client=client)


async def test_delete_is_idempotent(api, client):
    sb = await AsyncSandbox("busybox", client=client).create()
    del api.sandboxes[sb.sandbox_id]
    await sb.delete()
    assert sb.deleted
    with pytest.raises(SandboxError, match="has been deleted"):
        await sb.exec(["true"])
    assert not client.closed  # a shared client is never closed by the sandbox


async def test_async_with_creates_and_deletes(api, client):
    async with AsyncSandbox("busybox", client=client) as sb:
        assert sb.ready
        sid = sb.sandbox_id
    assert api.sandboxes[sid]["deleted_at"] and sb.deleted


async def test_async_with_cleans_up_if_never_ready(monkeypatch):
    api = FakeAPI(ready_after_polls=10**9)
    async with AsyncClient("http://sandbox.test", transport=api.async_transport()) as c:
        sb = AsyncSandbox("busybox", client=c)

        async def fast_create(**kw):
            return await AsyncSandbox.create(sb, wait=True, timeout=0.01)

        monkeypatch.setattr(sb, "create", fast_create)
        with pytest.raises(SandboxTimeoutError):
            async with sb:
                pass
        assert sb.deleted and api.sandboxes == {}


async def test_owned_client_is_closed(monkeypatch, api):
    # A sandbox given no client opens its own, and closes it on delete.
    monkeypatch.setattr(
        "sandbox_sdk.async_sandbox.AsyncClient",
        lambda: AsyncClient("http://sandbox.test", transport=api.async_transport()),
    )
    async with AsyncSandbox("busybox") as sb:
        owned = sb.client
        assert (await sb.exec(["true"])).ok
    assert owned.closed


async def test_client_helpers(client):
    sb = await AsyncSandbox("busybox", client=client).create()
    assert (await client.sandbox(sb.sandbox_id)).ready
    assert [s.sandbox_id for s in await client.sandboxes()] == [sb.sandbox_id]


async def test_connection_error():
    def refuse(request):
        raise httpx.ConnectError("connection refused")

    async with AsyncClient("http://sandbox.test", transport=httpx.MockTransport(refuse)) as c:
        with pytest.raises(SandboxConnectionError):
            await AsyncSandbox("busybox", client=c).create()
