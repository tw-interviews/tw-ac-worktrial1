# sandbox-sdk

Python SDK for the SandboxManager API.

```sh
pip install -e ~/k3d-webapp/sdk/python   # or: uv add --editable ~/k3d-webapp/sdk/python
kubectl -n webapp port-forward svc/sandbox-manager 8080:80
```

The SDK talks to `$SANDBOX_API_URL`, defaulting to `http://localhost:8080`.

## Quick start

```python
from sandbox_sdk import Sandbox, Resources

sb = Sandbox(
    "busybox:1.36",
    command=["sleep", "infinity"],          # keep the container alive
    env={"MODE": "dev"},
    resources=Resources(requests={"cpu": "100m", "memory": "64Mi"},
                        limits={"cpu": "500m", "memory": "128Mi"}),
)
sb.create()                                 # POST /sandboxes, waits until Ready
print(sb.sandbox_id, sb.status)

cmd = sb.exec("echo $MODE | tr a-z A-Z")    # POST /sandbox/executions
print(cmd.execution_id, cmd.exit_code, cmd.stdout)

sb.delete()                                 # DELETE /sandboxes/{id}
```

Or let a `with` block create and delete it:

```python
with Sandbox("busybox:1.36", command=["sleep", "infinity"]) as sb:
    sb.exec(["ls", "-l", "/"]).check()
```

## Async

`AsyncClient`, `AsyncSandbox` and `AsyncCommand` mirror the sync classes with awaitable methods,
so independent commands and sandboxes can run concurrently:

```python
import asyncio
from sandbox_sdk import AsyncClient, AsyncSandbox

async def main():
    async with AsyncClient() as client:
        async with AsyncSandbox("busybox:1.36", command=["sleep", "infinity"], client=client) as sb:
            cmd = await sb.exec("echo hi")
            print(cmd.stdout)
            results = await asyncio.gather(*(sb.exec(f"sleep 1; echo {i}") for i in range(5)))

asyncio.run(main())
```

A full runnable example is in `examples/async_quickstart.py` (`uv run examples/async_quickstart.py`).

Differences from the sync API:

- **Output of listed commands must be loaded explicitly.** A property can't be awaited, so
  `cmd.stdout` on a command from `await sb.commands()` raises `OutputNotLoadedError`. Call
  `await cmd.refresh()` first, or use `await sb.commands(with_output=True)` to fetch every
  command's output concurrently. Commands from `exec()`/`get_command()` are always loaded.
- **No shared default client.** An `AsyncClient` belongs to one event loop. Pass one in (best
  when you manage several sandboxes), or let the sandbox open its own, which it closes on
  `delete()`, `aclose()` or leaving `async with`.
- **Cancellation.** Cancelling a task awaiting `exec()` closes the request; the server still
  records the result, but the process may keep running in the container.

## Sandbox

`Sandbox(image, *, command=None, args=None, env=None, resources=None, ttl=None, paused_ttl=None,
files=None, client=None)` describes a sandbox; nothing happens on the server until `create()`. The
object then holds its runtime data: `sandbox_id`, `name`, `namespace`, `definition` (the Sandbox
YAML), `created_at`, `ttl_seconds`, `expires_at`, `paused_ttl_seconds`, `paused_at`, `volume`,
`volume_size`, `deleted_at`, `files_retained_until`, `status`, `ready`, `paused`, `created`,
`deleted`.

`status` is one of `Staging` (waiting for `files` to upload), `Ready`, `NotReady`, `Pausing`,
`Paused`, `Resuming` (see [Pausing](#pausing)), `Deleting`,
`Deleted` (deleted; files still downloadable until `files_retained_until`), `Expired` (its TTL ran
out and the cluster deleted it) or `Missing` (gone from the cluster for another reason).

| Method | API | Notes |
|---|---|---|
| `create(wait=True, timeout=120)` | `POST /sandboxes` | Waits for `Ready` unless `wait=False`. |
| `exec(command, stdin=None, timeout=None)` | `POST /sandbox/executions` | `command` is an argv list, or a string run via `sh -c`. `timeout` in seconds (server default 30, max 300). Returns a `Command`. A non-zero exit does **not** raise. |
| `delete(purge=False)` | `DELETE /sandboxes/{id}` | Stops the sandbox. Its files and execution history are kept for a 12h recovery window, then deleted; `purge=True` deletes them now. Already-gone sandboxes are fine. |
| `pause(wait=True, timeout=60, force=False)` | `POST /sandboxes/{id}/pause` | See [Pausing](#pausing). |
| `resume(wait=True, timeout=120)` | `POST /sandboxes/{id}/resume` | |
| `refresh()` / `wait_ready()` / `wait_paused()` | `GET /sandboxes/{id}` | Update `status`. |
| `commands()` | `GET /sandbox/executions?sandbox_id=` | Every command run in this sandbox, newest first. |
| `get_command(execution_id)` | `GET /sandbox/executions/{id}` | One command by id. |
| `Sandbox.get(id)` / `client.sandbox(id)` | `GET /sandboxes/{id}` | Attach to an existing sandbox. |
| `upload({local: dest})` | `POST /sandboxes/{id}/files` | See [Files](#files). |
| `write_file(path, data)` / `read_file(path)` | `PUT` / `GET /sandboxes/{id}/files/{path}` | |
| `download(path, local)` / `list_files(path="")` / `remove_file(path)` | `GET` / `GET ?list` / `DELETE …/files/{path}` | |

### Files

Every sandbox has its own persistent volume (1Gi), mounted at `/workspace`. You never manage the
volume: map local paths to destinations with `files=`, and they're in place **before the
sandbox's command starts**:

```python
sb = Sandbox(
    "python:3.12-slim",
    command=["python", "/workspace/src/train.py"],
    files={
        "./src/": "src/",                     # a directory, recursively
        "./data/train.csv": "data/train.csv",  # a file
        "./config.yaml": "/workspace/",        # into a directory, keeping its name
    },
).create()
```

A runnable walkthrough of every pattern below is in `examples/storage_quickstart.py`
(`uv run examples/storage_quickstart.py`).

Destinations are relative to `/workspace` (absolute paths must be inside it). Missing local paths
or bad destinations raise before anything is created; if the upload fails, the half-made sandbox is
purged and the error re-raised.

While the sandbox runs:

```python
sb.upload({"./more/": "more/"})                # add files any time (one streamed tar archive)
sb.write_file("notes.txt", "hello")             # bytes, str or a binary file object
print(sb.read_file("results/out.csv"))          # whole file in memory
sb.download("results/", "./results")            # a file or a whole directory, streamed to disk
for f in sb.list_files("results"):              # FileInfo(name, type, size, mode, modified)
    print(f.name, f.size)
sb.remove_file("tmp/")                          # a file, or a directory recursively
```

- **Everything streams.** Uploads are built as a tar stream from disk and downloads are written to
  disk as they arrive, so file size doesn't affect memory use on either side.
- **Files survive the container restarting.** They live on the volume, not in the container.
- **The recovery window.** After `delete()` (or TTL expiry) the files stay downloadable for 12 hours:
  `read_file`, `download` and `list_files` keep working (also via `Sandbox.get(id)` from another
  process), while writes raise `SandboxError`. Then the volume is deleted. `delete(purge=True)`
  deletes it immediately.
- **Permissions.** Uploaded files keep their mode; files are owned by root (as are files written
  by most images), and `sb.list_files()` shows what's there.
- **Limits.** The 1Gi size isn't enforced by the local storage driver, so a sandbox can write past
  it; uploads that hit a full disk fail with HTTP 507. Paths that escape `/workspace`, including
  through symlinks, are rejected. Only regular files and directories can be uploaded.

### Pausing

Pause a sandbox you'll come back to: its pod stops, freeing its CPU and memory quota, while its
`/workspace` volume is kept. Resuming starts a new pod on the same volume.

```python
sb = Sandbox("python:3.12-slim", command=["sleep", "infinity"], paused_ttl=3600).create()
sb.exec("pip install --target /workspace/lib requests").check()
sb.pause()                      # waits until the pod has stopped (status "Paused")
sb.read_file("notes.txt")       # files still work while paused
sb.resume()                     # waits until it's Ready again
sb.exec("PYTHONPATH=/workspace/lib python -c 'import requests'").check()
```

- **Only `/workspace` survives.** Memory, running processes, and everything else in the container
  (`/tmp`, packages installed outside `/workspace`, ...) are gone after a resume, as after a restart.
  This follows the upstream Sandbox's `operatingMode: Suspended`; memory snapshots are GKE-only.
- **Files keep working while paused.** `read_file`, `write_file`, `upload`, `download`,
  `list_files` and `remove_file` go through a small temporary pod that mounts the volume.
- **`exec()` raises `SandboxPausedError`** (a `ConflictError`) while paused or pausing.
- **Running commands block a pause.** `pause()` raises `ConflictError` while any `exec()` is still
  running, because stopping the pod kills them. `pause(force=True)` pauses anyway; the killed
  commands report exit code 137. File transfers in progress are not waited for.
- **A paused sandbox has its own TTL**, `paused_ttl` (default 24 hours, 60 seconds to 7 days),
  counted from the pause. If it runs out, the sandbox expires: it can't be resumed, and its files
  stay downloadable for the 12h recovery window. `resume()` restarts the normal `ttl`.
- **Resuming can hit the quota.** `resume()` raises `QuotaExceededError` if the new pod wouldn't
  fit; the sandbox stays paused, so retry once there's room.
- **Both are asynchronous and idempotent.** With `wait=False` they return at once (status
  `Pausing` / `Resuming`); pausing a paused sandbox or resuming a running one does nothing.
  `AsyncSandbox` has the same methods, so you can pause or resume many at once with
  `asyncio.gather`.

### TTL

Every sandbox expires once it has gone `ttl` seconds without running a command, and the cluster
then deletes it. The default is 5 minutes (300 seconds); `ttl` can be 30 to 86400 seconds.

```python
sb = Sandbox("busybox:1.36", command=["sleep", "infinity"], ttl=600).create()
print(sb.expires_at)            # about 10 minutes from now
sb.exec("true")
print(sb.expires_at)            # pushed out again
```

- **Each `exec()` refreshes the TTL**, and `sb.expires_at` is updated from the response. The new
  expiry is the exec's start time + its `timeout` + `ttl`, so a sandbox can't expire while a
  command is still running. It never moves earlier, so parallel commands are safe.
- **File transfers count as activity too** (`upload`, `write_file`, `download`, ...), and refresh it
  throughout long transfers.
- **Nothing else refreshes it.** `refresh()`, `wait_ready()` and `commands()` only read. To keep an
  idle sandbox alive, run a cheap command such as `sb.exec("true")`.
- **While paused**, the paused TTL applies instead; see [Pausing](#pausing).
- **After expiry**, `refresh()` shows `status == "Expired"` and `exec()` raises `ConflictError`.
  Its files stay downloadable for 12 hours after the expiry, then the volume and record are deleted.

### Capacity

Sandboxes share a resource quota (sized for the local 2 CPU / 2 GiB VM; about 9 running sandboxes
at default resources). A sandbox that doesn't set `resources` gets a 25m CPU / 32Mi request and a
500m / 64Mi limit; one sandbox may use at most 1 CPU / 512Mi. Each sandbox pod also runs a small
file-transfer container (16Mi limit).

Volumes have their own quota: 40 volumes / 40Gi. Deleted and expired sandboxes keep theirs during
the 12h recovery window, so if you create many short-lived sandboxes, `delete(purge=True)` the ones
whose files you don't need.

When the quota is full, `create()` raises `QuotaExceededError` (HTTP 429) right away; nothing is
created. Delete sandboxes or ask for fewer resources, then retry:

```python
from sandbox_sdk import QuotaExceededError

try:
    sb = Sandbox("busybox:1.36", command=["sleep", "infinity"]).create()
except QuotaExceededError as e:
    print("no room:", e.message)
```

Asking for more than the per-sandbox maximum raises `BadRequestError` (400) instead.

## Command

Each `exec()` is one execution; a sandbox can have many. A `Command` exposes `execution_id`,
`sandbox_id`, `argv`, `status` (`completed`, `timed_out`, `running`, `error`), `exit_code`,
`stdout`, `stderr`, `stdout_truncated`, `stderr_truncated` (output is capped at 1 MiB per
stream), `started_at`, `finished_at`, plus:

- `ok` — completed with exit code 0.
- `check()` — raise `CommandError` unless `ok`; returns the command, so `sb.exec(...).check().stdout` works.
- `refresh()` — re-read from the API.

Commands from `sandbox.commands()` load `stdout`/`stderr` on first access, since the list
endpoint leaves output out.

## Errors

All inherit from `SandboxError`:
`APIError` (with `status_code`, `message`) → `BadRequestError` (400, e.g. invalid `ttl` or
resources above the per-sandbox maximum), `NotFoundError` (404), `ConflictError` (409, e.g. sandbox
not ready, being deleted or expired, or commands still running when pausing) and its subclass
`SandboxPausedError` (409, `exec()` on a paused sandbox), `QuotaExceededError` (429, sandbox quota
full, also on `resume()`),
`ExecutionFailedError` (502, has `execution_id`);
plus `SandboxConnectionError`, `SandboxTimeoutError`, `CommandError`, `OutputNotLoadedError` (async only).

## Development

```sh
uv run pytest                                      # unit tests (sync + async) against an in-memory fake API
SANDBOX_API_URL=http://localhost:8080 uv run pytest -m integration   # against the real cluster
```
