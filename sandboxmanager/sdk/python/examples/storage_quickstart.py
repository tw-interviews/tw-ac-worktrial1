"""Storage quickstart: getting files into and out of a sandbox.

Every sandbox has its own persistent volume at /workspace. This walks through
the ways to use it:

  1. files= at creation: files are in place before the command starts
  2. upload / write_file while it runs
  3. list_files / read_file / download (a file or a whole directory)
  4. files the sandbox itself produces
  5. remove_file
  6. the recovery window: reading files after delete(), then purging

Run (from sdk/python, with the API port-forwarded to localhost:8080):

    uv run examples/storage_quickstart.py
"""

import os
import tempfile

from sandbox_sdk import NotFoundError, Sandbox, SandboxError


def make_local_project(root: str) -> None:
    """A small local project to upload: a script, a data folder and a config file."""
    os.makedirs(os.path.join(root, "data", "raw"))
    with open(os.path.join(root, "process.sh"), "w") as f:
        f.write("#!/bin/sh\n"
                "# Count the lines in every input file into /workspace/output/report.txt\n"
                "mkdir -p /workspace/output\n"
                "wc -l /workspace/data/raw/*.csv > /workspace/output/report.txt\n")
    os.chmod(os.path.join(root, "process.sh"), 0o755)
    for name, rows in [("a.csv", 3), ("b.csv", 5)]:
        with open(os.path.join(root, "data", "raw", name), "w") as f:
            f.write("id,value\n" + "".join(f"{i},{i * i}\n" for i in range(rows)))
    with open(os.path.join(root, "config.yaml"), "w") as f:
        f.write("mode: demo\n")


def show(title: str) -> None:
    print(f"\n== {title}")


def main() -> None:
    with tempfile.TemporaryDirectory() as local:
        make_local_project(local)

        # 1. files= maps {local path: destination}. Destinations are relative to
        #    /workspace (or absolute inside it). A directory uploads recursively;
        #    a destination ending in "/" keeps the file's name. The command starts
        #    only after everything is there, so it can rely on the files at once.
        show("1. files= at creation")
        sb = Sandbox(
            "busybox:1.36",
            command=["sh", "-c", "ls -R /workspace > /workspace/seen-at-start.txt; exec sleep infinity"],
            files={
                os.path.join(local, "process.sh"): "bin/process.sh",
                os.path.join(local, "data"): "data",
                os.path.join(local, "config.yaml"): "/workspace/",
            },
            ttl=300,
        )
        sb.create()
        print(f"sandbox {sb.sandbox_id} is {sb.status}; volume {sb.volume} ({sb.volume_size})")
        print("what the command saw on its first line:")
        print("  " + sb.read_file("seen-at-start.txt").decode().strip().replace("\n", "\n  "))

        try:
            # 2. Add files while it runs: many at once with upload() (one streamed
            #    tar archive), or one at a time with write_file() (bytes, text, or a
            #    file object, streamed; replaced atomically, parents created).
            show("2. upload and write_file")
            extra = os.path.join(local, "c.csv")
            with open(extra, "w") as f:
                f.write("id,value\n7,49\n")
            r = sb.upload({extra: "data/raw/"})
            print(f"upload: {r.files} file(s), {r.bytes} bytes")
            w = sb.write_file("notes/todo.txt", "check the report\n")
            print(f"write_file: {w.path}, {w.bytes} bytes, sha256 {w.sha256[:12]}...")

            # 3. Look around and read.
            show("3. list_files and read_file")
            for entry in sb.list_files():
                kind = "dir " if entry.is_dir else "file"
                print(f"  {kind} {entry.mode} {entry.size:>6}  {entry.name}")
            print(f"config.yaml: {sb.read_file('config.yaml').decode().strip()!r}")

            # 4. The sandbox produces output on the same volume; download it.
            #    download() streams to disk: a file goes to the given path (or into
            #    it, if that's a directory); a directory is extracted.
            show("4. files the sandbox writes")
            sb.exec("/workspace/bin/process.sh").check()  # its executable bit survived the upload
            out_dir = os.path.join(local, "results")
            os.makedirs(out_dir)
            report = sb.download("output/report.txt", out_dir)
            print(f"downloaded {report}:")
            for line in open(report):
                print("  " + line.strip())
            data_copy = sb.download("data", os.path.join(local, "data-copy"))
            copied = sorted(os.path.relpath(os.path.join(d, f), data_copy)
                            for d, _, fs in os.walk(data_copy) for f in fs)
            print(f"downloaded the data directory: {copied}")

            # 5. Remove a file or a whole directory.
            show("5. remove_file")
            sb.remove_file("notes")
            print(f"after removing notes/: {[e.name for e in sb.list_files()]}")

            # 6. delete() stops the sandbox but keeps its volume for the recovery
            #    window. Reads keep working (also from another process, via
            #    Sandbox.get(id)); writes and commands don't.
            show("6. recovery window")
            sb.delete()
            sb.refresh()
            print(f"status {sb.status}; files kept until {sb.files_retained_until:%Y-%m-%d %H:%M} UTC")
            later = Sandbox.get(sb.sandbox_id)  # e.g. a different process, later on
            print(f"recovered report: {later.read_file('output/report.txt').decode().splitlines()[-1]!r}")
            try:
                later.write_file("too-late.txt", "x")
            except SandboxError as e:
                print(f"writing after delete fails: {e}")
        finally:
            # A demo: free the volume now instead of after the recovery window.
            sb.delete(purge=True)

        try:
            sb.read_file("output/report.txt")
        except NotFoundError:
            print("after delete(purge=True) the files are gone")


if __name__ == "__main__":
    main()
