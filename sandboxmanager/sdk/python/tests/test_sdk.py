from datetime import datetime, timezone

import httpx
import pytest

from sandbox_sdk import (
    BadRequestError,
    Client,
    CommandError,
    ConflictError,
    ExecutionFailedError,
    NotFoundError,
    QuotaExceededError,
    Resources,
    Sandbox,
    SandboxConnectionError,
    SandboxError,
    SandboxTimeoutError,
)

from .fake_api import FakeAPI


@pytest.fixture
def api():
    return FakeAPI()


@pytest.fixture
def client(api):
    with Client("http://sandbox.test", transport=api.transport()) as c:
        yield c


def test_create_sends_spec_and_waits_for_ready(api, client):
    sb = Sandbox(
        "busybox:1.36",
        command=["sleep", "infinity"],
        env={"MODE": "test"},
        resources=Resources(requests={"cpu": "50m"}, limits={"memory": "64Mi"}),
        client=client,
    )
    assert not sb.created
    assert sb.create(timeout=5) is sb

    assert api.requests[0] == (
        "POST",
        "/sandboxes",
        {
            "image": "busybox:1.36",
            "command": ["sleep", "infinity"],
            "env": {"MODE": "test"},
            "resources": {"requests": {"cpu": "50m"}, "limits": {"memory": "64Mi"}},
        },
    )
    assert sb.created and sb.ready
    assert sb.name == f"sbx-{sb.sandbox_id}"
    assert sb.namespace == "sandboxes"
    assert "kind: Sandbox" in sb.definition
    assert sb.created_at == datetime(2026, 10, 21, 20, 23, 38, 353954, tzinfo=timezone.utc)


def test_ttl_sent_and_refreshed_by_exec(api, client):
    sb = Sandbox("busybox", ttl=60, client=client).create()
    assert api.requests[0][2] == {"image": "busybox", "ttl_seconds": 60}
    assert sb.ttl_seconds == 60
    assert sb.expires_at == datetime(2026, 10, 21, 20, 28, 39, tzinfo=timezone.utc)
    sb.exec(["true"])
    assert sb.expires_at == datetime(2026, 10, 21, 20, 30, tzinfo=timezone.utc)


def test_quota_exceeded(client):
    api = FakeAPI(quota=1)
    with Client("http://sandbox.test", transport=api.transport()) as c:
        Sandbox("busybox", client=c).create()
        with pytest.raises(QuotaExceededError) as e:
            Sandbox("busybox", client=c).create()
        assert e.value.status_code == 429


def test_create_omits_unset_fields(api, client):
    Sandbox("busybox", client=client).create(wait=False)
    assert api.requests[0][2] == {"image": "busybox"}


def test_create_twice_or_without_image_fails(client):
    sb = Sandbox("busybox", client=client).create(wait=False)
    with pytest.raises(SandboxError, match="already created"):
        sb.create()
    with pytest.raises(SandboxError, match="image is required"):
        Sandbox(client=client).create()


def test_api_400_raises_bad_request(client):
    with pytest.raises(BadRequestError, match="image is required") as e:
        client.create_sandbox({"image": ""})
    assert e.value.status_code == 400


def test_wait_ready_polls_until_ready(client):
    api = FakeAPI(ready_after_polls=3)
    with Client("http://sandbox.test", transport=api.transport()) as c:
        sb = Sandbox("busybox", client=c).create(wait=False)
        sb.wait_ready(timeout=5, poll_interval=0)
        assert sb.ready and api.polls[sb.sandbox_id] == 3


def test_wait_ready_times_out():
    api = FakeAPI(ready_after_polls=10**9)
    with Client("http://sandbox.test", transport=api.transport()) as c:
        sb = Sandbox("busybox", client=c).create(wait=False)
        with pytest.raises(SandboxTimeoutError):
            sb.wait_ready(timeout=0.05, poll_interval=0.01)


def test_exec_returns_command_with_output(api, client):
    sb = Sandbox("busybox", client=client).create()
    cmd = sb.exec(["echo", "hi"])
    assert cmd.ok and cmd.exit_code == 0 and cmd.stdout == "echo hi\n"
    assert cmd.sandbox_id == sb.sandbox_id
    assert cmd.argv == ["echo", "hi"]
    assert cmd.execution_id in api.executions


def test_exec_string_runs_through_shell(api, client):
    sb = Sandbox("busybox", client=client).create()
    sb.exec("echo $HOME | wc -c", stdin="x", timeout=5)
    assert api.requests[-1][2] == {
        "sandbox_id": sb.sandbox_id,
        "command": ["sh", "-c", "echo $HOME | wc -c"],
        "stdin": "x",
        "timeout_seconds": 5,
    }


def test_nonzero_exit_does_not_raise_until_check(client):
    sb = Sandbox("busybox", client=client).create()
    cmd = sb.exec("exit 3")
    assert not cmd.ok and cmd.exit_code == 3 and cmd.stderr == "boom\n"
    with pytest.raises(CommandError, match="exited with code 3"):
        cmd.check()
    assert sb.exec(["true"]).check().ok


def test_timed_out_command(client):
    cmd = Sandbox("busybox", client=client).create().exec(["sleep", "999"])
    assert cmd.timed_out and cmd.exit_code is None and not cmd.ok
    with pytest.raises(CommandError, match="timed out"):
        cmd.check()


def test_exec_stream_failure_carries_execution_id(api, client):
    sb = Sandbox("busybox", client=client).create()
    with pytest.raises(ExecutionFailedError) as e:
        sb.exec(["explode"])
    assert e.value.execution_id in api.executions


def test_exec_before_ready_is_conflict(client):
    sb = Sandbox("busybox", client=client).create(wait=False)
    with pytest.raises(ConflictError, match="not ready"):
        sb.exec(["true"])


def test_exec_requires_created_sandbox(client):
    with pytest.raises(SandboxError, match="not been created"):
        Sandbox("busybox", client=client).exec(["true"])


def test_commands_fetch_output_lazily(api, client):
    sb = Sandbox("busybox", client=client).create()
    first = sb.exec(["echo", "one"])
    sb.exec(["cat"], stdin="two")

    cmds = sb.commands()
    assert [c.argv for c in cmds] == [["cat"], ["echo", "one"]]
    gets_before = sum(1 for m, p, _ in api.requests if m == "GET" and p.startswith("/sandbox/executions/"))
    assert cmds[0].stdout == "two"  # triggers GET /sandbox/executions/{id}
    assert cmds[1].stdout == "echo one\n"
    gets_after = sum(1 for m, p, _ in api.requests if m == "GET" and p.startswith("/sandbox/executions/"))
    assert gets_after - gets_before == 2
    cmds[0].stdout  # already loaded, no extra request
    assert sum(1 for m, p, _ in api.requests if m == "GET" and p.startswith("/sandbox/executions/")) == gets_after

    assert sb.get_command(first.execution_id).stdout == "echo one\n"


def test_get_command_rejects_other_sandboxes_execution(client):
    a = Sandbox("busybox", client=client).create()
    b = Sandbox("busybox", client=client).create()
    cmd = a.exec(["true"])
    with pytest.raises(SandboxError, match="belongs to sandbox"):
        b.get_command(cmd.execution_id)


def test_delete_is_idempotent_and_blocks_further_use(api, client):
    sb = Sandbox("busybox", client=client).create()
    sb.delete()
    assert sb.deleted and not sb.created and sb.status == "Deleted"
    assert api.sandboxes[sb.sandbox_id]["deleted_at"]  # kept for the recovery window
    with pytest.raises(SandboxError, match="has been deleted"):
        sb.exec(["true"])

    other = Sandbox("busybox", client=client).create()
    del api.sandboxes[other.sandbox_id]  # removed behind the SDK's back
    other.delete()
    assert other.deleted


def test_context_manager_creates_and_deletes(api, client):
    with Sandbox("busybox", client=client) as sb:
        assert sb.ready
        sid = sb.sandbox_id
    assert api.sandboxes[sid]["deleted_at"] and sb.deleted


def test_attach_to_existing(client):
    sb = Sandbox("busybox", client=client).create()
    again = client.sandbox(sb.sandbox_id)
    assert again.sandbox_id == sb.sandbox_id and again.ready
    assert [s.sandbox_id for s in client.sandboxes()] == [sb.sandbox_id]
    with pytest.raises(NotFoundError):
        Sandbox.get("00000000-0000-0000-0000-000000000000", client=client)


def test_connection_error():
    def refuse(request):
        raise httpx.ConnectError("connection refused")

    with Client("http://sandbox.test", transport=httpx.MockTransport(refuse)) as c:
        with pytest.raises(SandboxConnectionError, match="cannot reach sandbox API"):
            Sandbox("busybox", client=c).create()
