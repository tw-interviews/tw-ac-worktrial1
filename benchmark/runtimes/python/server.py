"""Sandbox runtime for the tool-call benchmark.

Speaks the upstream agent-sandbox SDK's "legacy python-runtime" protocol, which
`k8s_agent_sandbox`'s `sandbox.commands.run()` uses:

    POST /execute  {"command": "...", "timeout_seconds": 1.5}
                -> {"stdout": "...", "stderr": "...", "exit_code": 0, "timed_out": false}

plus GET / for the readiness probe. The upstream runtime's file endpoints aren't
needed for the benchmark and aren't implemented.

The image also carries numpy, pandas, scipy and requests so it's a realistic
"medium" tool image; the server itself doesn't import them, so they cost disk
and pull time but not startup time.
"""

import asyncio
from typing import Optional

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()


class ExecuteRequest(BaseModel):
    command: str
    timeout_seconds: Optional[float] = None


class ExecutionResult(BaseModel):
    stdout: str = ""
    stderr: str = ""
    exit_code: int = -1
    timed_out: bool = False


@app.get("/")
async def health() -> dict:
    return {"status": "ok"}


@app.post("/execute")
async def execute(req: ExecuteRequest) -> ExecutionResult:
    proc = await asyncio.create_subprocess_exec(
        "/bin/sh", "-c", req.command,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=req.timeout_seconds)
    except asyncio.TimeoutError:
        proc.kill()
        out, err = await proc.communicate()
        return ExecutionResult(stdout=out.decode(errors="replace"), stderr=err.decode(errors="replace"),
                               exit_code=124, timed_out=True)
    return ExecutionResult(stdout=out.decode(errors="replace"), stderr=err.decode(errors="replace"),
                           exit_code=proc.returncode)
