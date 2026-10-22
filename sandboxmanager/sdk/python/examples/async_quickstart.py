"""Async quickstart for the sandbox SDK.

Run (from sdk/python, with the API port-forwarded to localhost:8080):

    uv run examples/async_quickstart.py
"""

import asyncio
import tempfile
import time

from sandbox_sdk import AsyncClient, AsyncSandbox, QuotaExceededError, Resources

FLEET_SIZE = 5  # stays well inside the sandbox quota (about 12 at default resources)


async def single_sandbox(client: AsyncClient) -> None:
    # `async with` waits until the sandbox is Ready and deletes it at the end.
    async with AsyncSandbox(
        "busybox:1.36",
        command=["sleep", "infinity"],  # keep the container alive
        env={"GREETING": "hello"},
        resources=Resources(limits={"cpu": "500m", "memory": "64Mi"}),
        ttl=120,  # deleted by the cluster after 2 idle minutes, even if this script dies
        files={__file__: "scripts/quickstart.py"},  # uploaded to /workspace before the command starts
        client=client,
    ) as sb:
        print(f"sandbox {sb.sandbox_id} is {sb.status}, expires {sb.expires_at:%H:%M:%S}")
        print(f"  volume {sb.volume} ({sb.volume_size}): {[f.name for f in await sb.list_files('scripts')]}")

        # 1. Run one command and read its output. Each exec refreshes the TTL.
        cmd = await sb.exec("echo $GREETING from $(hostname)")
        print(f"[{cmd.execution_id}] exit={cmd.exit_code} stdout={cmd.stdout!r}")
        print(f"  ttl refreshed: expires {sb.expires_at:%H:%M:%S}")

        # 2. Non-zero exits don't raise; check exit_code or call check().
        failed = await sb.exec("echo oops >&2; exit 2")
        print(f"exit={failed.exit_code} stderr={failed.stderr!r} ok={failed.ok}")

        # 3. Run several commands at once. Each sleeps 2s; together they take ~2s, not 6s.
        start = time.monotonic()
        results = await asyncio.gather(*(sb.exec(f"sleep 2; echo job {i} done") for i in range(3)))
        print(f"3 jobs in {time.monotonic() - start:.1f}s:", [r.stdout.strip() for r in results])

        # 4. Command history, with output fetched concurrently.
        for c in await sb.commands(with_output=True):
            print(f"  {c.status:<9} exit={c.exit_code} {c.argv[-1]!r} -> {c.stdout.strip()!r}")

        # 5. Files the sandbox wrote come back out with download().
        await sb.exec("wc -l /workspace/scripts/quickstart.py > /workspace/report.txt")
        with tempfile.TemporaryDirectory() as out:
            path = await sb.download("report.txt", out)
            print(f"downloaded report: {open(path).read().strip()!r}")

    # Leaving `async with` deletes the sandbox but keeps its files for the
    # recovery window, so this still works:
    print(f"deleted: {sb.deleted}, files kept until {sb.files_retained_until or (await sb.refresh()).files_retained_until:%H:%M}")
    print(f"  recovered: {(await sb.read_file('report.txt')).decode().strip()!r}")
    await sb.delete(purge=True)  # a demo; free the volume now


async def sandbox_fleet(client: AsyncClient) -> None:
    """Start and delete several sandboxes concurrently."""
    sandboxes = [
        AsyncSandbox("busybox:1.36", command=["sleep", "infinity"], ttl=120, client=client)
        for _ in range(FLEET_SIZE)
    ]

    # 6. Start them all at once. With wait=False, create() returns as soon as the
    #    server has accepted the sandbox, without waiting for its pod to start.
    start = time.monotonic()
    results = await asyncio.gather(*(sb.create(wait=False) for sb in sandboxes), return_exceptions=True)
    started = [sb for sb, r in zip(sandboxes, results) if not isinstance(r, BaseException)]
    for r in results:
        if isinstance(r, QuotaExceededError):
            print(f"  quota full, skipped one: {r.message[:60]}...")
        elif isinstance(r, BaseException):
            raise r
    print(f"\nstarted {len(started)} sandboxes in {time.monotonic() - start:.2f}s")
    await asyncio.gather(*(sb.refresh() for sb in started))
    print(f"  statuses right after starting: {[sb.status for sb in started]}")

    try:
        # 7. Wait for all of them to become Ready, concurrently.
        await asyncio.gather(*(sb.wait_ready() for sb in started))
        print(f"all Ready after {time.monotonic() - start:.1f}s")

        # 8. One command in each sandbox, all at the same time.
        cmds = await asyncio.gather(*(sb.exec("hostname") for sb in started))
        for sb, c in zip(started, cmds):
            print(f"  {sb.sandbox_id[:8]} -> {c.stdout.strip()}")
    finally:
        # 9. Delete them all at once. delete() returns once the server accepts it;
        #    the cluster stops the pods in the background (within ~5s). purge=True
        #    also deletes their volumes now, rather than after the recovery window.
        start = time.monotonic()
        await asyncio.gather(*(sb.delete(purge=True) for sb in started))
        print(f"deleted {len(started)} sandboxes in {time.monotonic() - start:.2f}s: "
              f"{all(sb.deleted for sb in started)}")


async def main() -> None:
    # One client shared by everything below.
    async with AsyncClient() as client:
        await single_sandbox(client)
        await sandbox_fleet(client)


if __name__ == "__main__":
    asyncio.run(main())
