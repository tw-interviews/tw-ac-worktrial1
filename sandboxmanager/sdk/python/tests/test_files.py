import io
import os

import pytest

from sandbox_sdk import (
    APIError,
    AsyncClient,
    AsyncSandbox,
    Client,
    NotFoundError,
    Sandbox,
    SandboxError,
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
def local_tree(tmp_path):
    (tmp_path / "main.py").write_text("print('hi')\n")
    (tmp_path / "run.sh").write_text("#!/bin/sh\n")
    (tmp_path / "run.sh").chmod(0o755)
    data = tmp_path / "data"
    (data / "nested").mkdir(parents=True)
    (data / "a.csv").write_text("a,b\n1,2\n")
    (data / "nested" / "b.bin").write_bytes(os.urandom(3000))
    (data / "empty").mkdir()
    return tmp_path


def requests_to(api, method, suffix=""):
    return [r for r in api.requests if r[0] == method and r[1].endswith(suffix)]


def test_files_uploaded_before_command_starts(api, client, local_tree):
    sb = Sandbox(
        "busybox",
        command=["sh", "/workspace/run.sh"],
        files={
            str(local_tree / "main.py"): "app/main.py",
            str(local_tree / "run.sh"): "/workspace/",  # into the root, keeping its name
            str(local_tree / "data"): "data",  # a directory, recursively
        },
        client=client,
    ).create()

    assert api.requests[0][2]["stage_files"] is True
    order = [(m, p.split("/")[-1]) for m, p, _ in api.requests[:4]]
    assert order[:3] == [("POST", "sandboxes"), ("POST", "files"), ("POST", "complete")]
    files = api.sandboxes[sb.sandbox_id]["files"]
    assert files["app/main.py"][0] == b"print('hi')\n"
    assert files["run.sh"][1] & 0o777 == 0o755  # permissions kept
    assert files["data/nested/b.bin"][0] == (local_tree / "data/nested/b.bin").read_bytes()
    assert "data/empty" in api.sandboxes[sb.sandbox_id]["dirs"]
    assert sb.ready and sb.volume is None and sb.volume_size == "1Gi"


def test_bad_local_path_or_destination_fails_before_creating(api, client, local_tree):
    with pytest.raises(FileNotFoundError):
        Sandbox("busybox", files={str(local_tree / "nope"): "x"}, client=client).create()
    with pytest.raises(ValueError, match="inside /workspace"):
        Sandbox("busybox", files={str(local_tree / "main.py"): "/etc/passwd"}, client=client).create()
    with pytest.raises(ValueError, match="escapes"):
        Sandbox("busybox", files={str(local_tree / "main.py"): "../x"}, client=client).create()
    assert api.requests == []


def test_failed_staging_purges_the_sandbox(api, client, local_tree):
    api.fail_uploads = True
    sb = Sandbox("busybox", files={str(local_tree / "main.py"): "main.py"}, client=client)
    with pytest.raises(APIError) as e:
        sb.create()
    assert e.value.status_code == 507
    assert api.sandboxes == {} and sb.deleted
    assert requests_to(api, "DELETE")[0][1] == f"/sandboxes/{sb.sandbox_id}"


def test_write_read_list_remove(client, local_tree):
    sb = Sandbox("busybox", client=client).create()
    w = sb.write_file("notes/todo.txt", "buy milk\n")
    assert (w.path, w.bytes, len(w.sha256)) == ("/workspace/notes/todo.txt", 9, 64)
    sb.write_file("/workspace/blob.bin", b"\x00\x01")
    with open(local_tree / "main.py", "rb") as f:
        sb.write_file("main.py", f)  # a file object is streamed
    assert sb.read_file("notes/todo.txt") == b"buy milk\n"
    assert sb.read_file("main.py") == b"print('hi')\n"

    root = {e.name: e for e in sb.list_files()}
    assert set(root) == {"notes", "blob.bin", "main.py"}
    assert root["notes"].is_dir and root["blob.bin"].size == 2
    assert [e.name for e in sb.list_files("notes")] == ["todo.txt"]

    sb.remove_file("notes")
    with pytest.raises(NotFoundError):
        sb.read_file("notes/todo.txt")


def test_upload_and_download(client, local_tree, tmp_path):
    sb = Sandbox("busybox", client=client).create()
    r = sb.upload({str(local_tree / "data"): "in/data", str(local_tree / "main.py"): "in/"})
    assert (r.files, r.bytes) == (3, 8 + 3000 + 12)

    out = tmp_path / "out"
    out.mkdir()
    assert sb.download("in/main.py", str(out)) == str(out / "main.py")  # into a directory
    assert sb.download("in/main.py", str(out / "renamed.py")) == str(out / "renamed.py")
    assert (out / "renamed.py").read_text() == "print('hi')\n"

    got = sb.download("in/data", str(tmp_path / "data-copy"))  # a directory is extracted
    assert open(os.path.join(got, "nested", "b.bin"), "rb").read() == (local_tree / "data/nested/b.bin").read_bytes()
    assert not any(n.startswith(".download-") for n in os.listdir(out))


def test_files_recoverable_after_delete_until_purged(api, client):
    sb = Sandbox("busybox", client=client).create()
    sb.write_file("result.txt", "42")
    sb.delete()
    assert sb.deleted and sb.status == "Deleted"
    sb.refresh()
    assert sb.files_retained_until is not None and sb.deleted_at is not None

    assert sb.read_file("result.txt") == b"42"  # recovery window
    assert [e.name for e in sb.list_files()] == ["result.txt"]
    with pytest.raises(SandboxError, match="has been deleted"):
        sb.write_file("more.txt", "x")
    with pytest.raises(SandboxError, match="has been deleted"):
        sb.exec("true")

    again = Sandbox.get(sb.sandbox_id, client=client)  # e.g. from another process
    assert again.deleted and again.read_file("result.txt") == b"42"

    sb.delete(purge=True)
    with pytest.raises(NotFoundError):
        sb.read_file("result.txt")


async def test_async_files(local_tree, tmp_path):
    api = FakeAPI()
    async with AsyncClient("http://sandbox.test", transport=api.async_transport()) as c:
        sb = await AsyncSandbox("busybox", files={str(local_tree / "data"): "data"}, client=c).create()
        assert sb.ready and api.sandboxes[sb.sandbox_id]["files"]["data/a.csv"][0] == b"a,b\n1,2\n"

        w = await sb.write_file("big.bin", io.BytesIO(os.urandom(5 << 20)))  # streamed from a thread
        assert w.bytes == 5 << 20
        r = await sb.upload({str(local_tree / "main.py"): "main.py"})
        assert r.files == 1
        assert await sb.read_file("main.py") == b"print('hi')\n"
        assert {e.name for e in await sb.list_files()} == {"data", "big.bin", "main.py"}

        got = await sb.download("data", str(tmp_path / "copy"))
        assert open(os.path.join(got, "a.csv")).read() == "a,b\n1,2\n"

        await sb.delete()
        assert await sb.read_file("main.py") == b"print('hi')\n"  # recovery window
        await sb.delete(purge=True)
        assert sb.sandbox_id not in api.sandboxes


async def test_async_failed_staging_purges(local_tree):
    api = FakeAPI()
    api.fail_uploads = True
    async with AsyncClient("http://sandbox.test", transport=api.async_transport()) as c:
        with pytest.raises(APIError):
            async with AsyncSandbox("busybox", files={str(local_tree / "main.py"): "m.py"}, client=c):
                pass
    assert api.sandboxes == {}
