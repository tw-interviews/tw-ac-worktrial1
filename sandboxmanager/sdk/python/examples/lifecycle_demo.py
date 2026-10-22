"""Step-by-step demo of the sandbox lifecycle through the SandboxManager API.

Starts 3 sandboxes, uploads 3 files to each, runs a command that prints the
files, pauses them, resumes them, runs the command again, and deletes them.
Press Enter to move on after each step.

Run (from sdk/python, with the API port-forwarded to localhost:8080):

    kubectl -n webapp port-forward svc/sandbox-manager 8080:80
    uv run examples/lifecycle_demo.py
"""

import asyncio

from sandbox_sdk import AsyncClient, AsyncSandbox

FILES = ["file1.txt", "file2.txt", "file3.txt"]
COMMAND = "cat " + " ".join(f"/workspace/{f}" for f in FILES)


async def wait_for_enter(next_step: str) -> None:
    await asyncio.to_thread(input, f"\nPress Enter to {next_step}... ")


async def run_command(sandboxes: list) -> None:
    print(f"$ {COMMAND}")
    results = await asyncio.gather(*(sb.exec(COMMAND) for sb in sandboxes))
    for i, (sb, cmd) in enumerate(zip(sandboxes, results), 1):
        print(f"\n--- sandbox {i} ({sb.sandbox_id[:8]}), exit code {cmd.exit_code} ---")
        print(cmd.stdout, end="")


async def main() -> None:
    async with AsyncClient() as client:
        sandboxes = [
            AsyncSandbox("busybox:1.36", command=["sleep", "infinity"], client=client)
            for _ in range(3)
        ]
        try:
            await wait_for_enter("start 3 sandboxes")
            print("Starting 3 sandboxes...")
            await asyncio.gather(*(sb.create() for sb in sandboxes))
            for i, sb in enumerate(sandboxes, 1):
                print(f"  sandbox {i}: {sb.sandbox_id}  status={sb.status}")

            await wait_for_enter("upload 3 files to each sandbox")
            for i, sb in enumerate(sandboxes, 1):
                for f in FILES:
                    await sb.write_file(f, f"hello from {f} in sandbox {i}\n")
                print(f"  sandbox {i}: uploaded {', '.join(FILES)}")

            await wait_for_enter("run the command in each sandbox")
            await run_command(sandboxes)

            await wait_for_enter("pause the sandboxes")
            print("Pausing (their pods stop; /workspace is kept)...")
            await asyncio.gather(*(sb.pause() for sb in sandboxes))
            for i, sb in enumerate(sandboxes, 1):
                print(f"  sandbox {i}: status={sb.status}")

            await wait_for_enter("resume the sandboxes")
            print("Resuming (new pods start on the same volumes)...")
            await asyncio.gather(*(sb.resume() for sb in sandboxes))
            for i, sb in enumerate(sandboxes, 1):
                print(f"  sandbox {i}: status={sb.status}")

            await wait_for_enter("run the command again")
            await run_command(sandboxes)

            await wait_for_enter("delete the sandboxes")
        finally:
            # Also runs on Ctrl-C or an error, so nothing is left behind.
            created = [sb for sb in sandboxes if sb.created]
            if created:
                print("Deleting the sandboxes...")
                await asyncio.gather(*(sb.delete(purge=True) for sb in created))
                print(f"  deleted {len(created)} sandboxes")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
