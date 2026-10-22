"""Tool-call throughput benchmark: one sandbox per tool call, three ways.

Each tool call acquires a sandbox, runs a tool in it, and releases it. The tool
is `sleep` for a duration drawn from N(300 ms, 75 ms), clamped to >= 50 ms.
Durations come from a per-worker seeded RNG, so worker k's i-th call sleeps
the same time in every arm.

Arms:
  sandboxmanager  the SandboxManager API, via sandbox_sdk: create (wait for
                  Ready), exec, delete(purge=True)
  agent-sandbox   the upstream kubernetes-sigs agent-sandbox SDK
                  (k8s-agent-sandbox): create_sandbox from the warm pool,
                  commands.run, delete_sandbox. The pool's depth is set with
                  `pool`: 0 means every claim is a cold start
  reuse           agent-sandbox sandboxes created once per worker up front and
                  reused for every call: the no-acquire-cost upper bound

Modes:
  closed  --workers W --duration S [--warmup S]   W workers loop back-to-back
  burst   --calls N                               N calls started at once
  open    --rate R --duration S [--warmup S]      Poisson arrivals at R calls/s

Stop rules (all modes): a run stops issuing calls when more than
--max-error-rate of the calls finished in the last 30 s failed, or a call has
been in flight longer than --max-latency; the reason goes to <out>.stop. Open
mode also caps concurrent calls at --max-inflight (extra arrivals are recorded
as dropped). Rules are checked between calls.

Every call is written as one JSON line to --out (times are wall-clock epoch
seconds, so lines from several processes merge). `summarize` reads them back.

Runs inside the cluster (the loadgen Deployment), usually via run-matrix.sh:
  python toolcall_bench.py pool --depth 32
  python toolcall_bench.py run --arm agent-sandbox --mode closed --workers 64 --duration 180 --out c.jsonl
  python toolcall_bench.py summarize c.jsonl
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import multiprocessing
import os
import random
import sys
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

NAMESPACE = os.environ.get("BENCH_NAMESPACE", "toolcall-bench")
POOL = os.environ.get("BENCH_POOL", "toolcall-pool")
TOOL_MEAN, TOOL_SD, TOOL_MIN = 0.300, 0.075, 0.050
RUNTIME_IMAGE = os.environ.get("RUNTIME_IMAGE", "")


def tool_durations(seed: int, worker: int):
    """Worker `worker`'s sequence of tool durations, identical across arms."""
    rng = random.Random(f"{seed}/{worker}")
    while True:
        yield max(TOOL_MIN, rng.gauss(TOOL_MEAN, TOOL_SD))


@dataclass
class Call:
    arm: str
    mode: str
    proc: int
    worker: int
    seq: int
    tool_s: float
    t_start: float = 0.0  # acquire begins
    t_acquired: float = 0.0  # sandbox ready to run a tool
    t_tool_done: float = 0.0  # tool finished
    t_released: float = 0.0  # release finished
    ok: bool = False
    error: Optional[str] = None
    warmup: bool = False
    extra: Dict[str, Any] = field(default_factory=dict)


# --- Arms: acquire() -> handle, run(handle, seconds), release(handle) ----------


class SandboxManagerArm:
    """The SandboxManager API, through its Python SDK (sandbox_sdk)."""

    name = "sandboxmanager"

    async def setup(self, workers: int) -> None:
        import httpx
        from sandbox_sdk import AsyncClient

        if not RUNTIME_IMAGE:
            sys.exit("RUNTIME_IMAGE is required for the sandboxmanager arm")
        # One shared HTTP client; no connection cap, so W workers don't queue on it.
        transport = httpx.AsyncHTTPTransport(limits=httpx.Limits(max_connections=None, max_keepalive_connections=None))
        self.client = AsyncClient(timeout=600, transport=transport)

    async def acquire(self):
        from sandbox_sdk import AsyncSandbox

        sb = AsyncSandbox(RUNTIME_IMAGE, ttl=600, client=self.client)
        await sb.create(timeout=600)
        return sb

    async def run(self, sb, seconds: float) -> None:
        (await sb.exec(["sleep", f"{seconds:.3f}"], timeout=60)).check()

    async def release(self, sb) -> None:
        await sb.delete(purge=True)

    async def teardown(self) -> None:
        await self.client.aclose()


class AgentSandboxArm:
    """The upstream agent-sandbox SDK, claiming from the warm pool: cold starts
    when the pool is at depth 0, warm claims otherwise."""

    name = "agent-sandbox"

    async def setup(self, workers: int) -> None:
        from k8s_agent_sandbox import AsyncSandboxClient
        from k8s_agent_sandbox.models import SandboxInClusterConnectionConfig
        from kubernetes_asyncio import client as kclient, config as kconfig

        cfg = kclient.Configuration()
        kconfig.load_incluster_config(client_configuration=cfg)
        cfg.connection_pool_maxsize = max(100, 2 * workers)
        self.api = kclient.ApiClient(cfg)
        self.client = AsyncSandboxClient(
            connection_config=SandboxInClusterConnectionConfig(server_port=8888),
            cleanup=False, api_client=self.api)

    async def acquire(self):
        return await self.client.create_sandbox(warmpool=POOL, namespace=NAMESPACE, sandbox_ready_timeout=600)

    async def run(self, sb, seconds: float) -> None:
        r = await sb.commands.run(f"sleep {seconds:.3f}", timeout=60)
        if r.exit_code != 0:
            raise RuntimeError(f"tool exited {r.exit_code}: {r.stderr[:200]}")

    async def release(self, sb) -> None:
        await self.client.delete_sandbox(sb.claim_name, namespace=NAMESPACE)

    async def teardown(self) -> None:
        await self.client.close()
        await self.api.close()


class ReuseArm(AgentSandboxArm):
    """Each worker acquires one agent-sandbox sandbox before the run and reuses
    it; acquire/release inside the measured loop cost nothing. The upper bound."""

    name = "reuse"

    async def setup(self, workers: int) -> None:
        await super().setup(workers)
        self.mine: List[Any] = list(await asyncio.gather(
            *(AgentSandboxArm.acquire(self) for _ in range(workers))))
        self.free: asyncio.Queue = asyncio.Queue()
        for sb in self.mine:
            self.free.put_nowait(sb)

    async def acquire(self):
        return await self.free.get()

    async def release(self, sb) -> None:
        self.free.put_nowait(sb)

    async def teardown(self) -> None:
        await asyncio.gather(*(AgentSandboxArm.release(self, sb) for sb in self.mine), return_exceptions=True)
        await super().teardown()


ARMS = {"sandboxmanager": SandboxManagerArm, "agent-sandbox": AgentSandboxArm, "reuse": ReuseArm}


# --- One tool call -------------------------------------------------------------


async def tool_call(arm, call: Call, out) -> None:
    call.t_start = time.time()
    sb = None
    try:
        sb = await arm.acquire()
        call.t_acquired = time.time()
        await arm.run(sb, call.tool_s)
        call.t_tool_done = time.time()
        call.ok = True
    except Exception as e:  # recorded, not fatal: errors are part of the result
        call.error = f"{type(e).__name__}: {e}"[:300]
    finally:
        if sb is not None:
            try:
                await arm.release(sb)
            except Exception as e:
                call.ok = False
                call.error = call.error or f"release {type(e).__name__}: {e}"[:300]
        call.t_released = time.time()
        out.write(json.dumps(asdict(call)) + "\n")
        out.flush()


# --- Modes (one process's share) ----------------------------------------------


async def run_proc(args, proc: int, n_procs: int) -> None:
    arm = ARMS[args.arm]()
    share = lambda total: total // n_procs + (1 if proc < total % n_procs else 0)
    workers = share(args.workers) if args.mode == "closed" else 0
    await arm.setup(max(workers, 1) if args.arm == "reuse" else max(workers, share(args.calls or 0), 1))
    path = f"{args.out}.p{proc}" if n_procs > 1 else args.out
    # Stop rules (small-scale copy): stop issuing calls once the last 30 s of
    # finished calls are > max_error_rate failures (min 10 calls), or any call
    # has been in flight longer than max_latency. The reason goes to <out>.stop.
    finished: List[tuple] = []  # (t_released, ok)
    inflight: Dict[int, float] = {}
    stopped = {"reason": None}

    def should_stop() -> bool:
        if stopped["reason"]:
            return True
        now = time.time()
        recent = [ok for t, ok in finished if now - t < 30]
        if len(recent) >= 10 and recent.count(False) / len(recent) > args.max_error_rate:
            stopped["reason"] = f"error rate {recent.count(False)}/{len(recent)} in last 30s"
        elif inflight and now - min(inflight.values()) > args.max_latency:
            stopped["reason"] = f"a call in flight over {args.max_latency:.0f}s"
        if stopped["reason"]:
            with open(args.out + ".stop", "a") as f:
                f.write(f"proc {proc}: {stopped['reason']}\n")
        return bool(stopped["reason"])

    async def tracked(c: Call, out) -> None:
        inflight[id(c)] = time.time()
        try:
            await tool_call(arm, c, out)
        finally:
            inflight.pop(id(c), None)
            finished.append((c.t_released, c.ok))

    with open(path, "w") as out:
        if args.mode == "closed":
            t0 = time.time()
            measure_from, stop_at = t0 + args.warmup, t0 + args.warmup + args.duration

            async def worker(w: int) -> None:
                gid = proc + w * n_procs  # global worker id, stable for a given W
                durations = tool_durations(args.seed, gid)
                seq = 0
                while time.time() < stop_at and not should_stop():
                    c = Call(args.arm, "closed", proc, gid, seq, next(durations), warmup=time.time() < measure_from)
                    await tracked(c, out)
                    seq += 1

            await asyncio.gather(*(worker(w) for w in range(workers)))
        elif args.mode == "burst":
            n = share(args.calls)
            calls = [Call(args.arm, "burst", proc, proc + i * n_procs, 0, next(tool_durations(args.seed, proc + i * n_procs)))
                     for i in range(n)]
            await asyncio.gather(*(tool_call(arm, c, out) for c in calls))
        else:  # open loop: Poisson arrivals at rate/n_procs per process
            rate = args.rate / n_procs
            rng = random.Random(f"{args.seed}/arrivals/{proc}")
            t0 = time.time()
            measure_from, stop_at = t0 + args.warmup, t0 + args.warmup + args.duration
            pending, i, t_next = set(), 0, t0
            while t_next < stop_at and not should_stop():
                await asyncio.sleep(max(0.0, t_next - time.time()))
                gid = proc + i * n_procs
                c = Call(args.arm, "open", proc, gid, 0, next(tool_durations(args.seed, gid)), warmup=t_next < measure_from)
                c.extra["scheduled"] = t_next
                if len(inflight) >= args.max_inflight // n_procs:
                    # In-flight cap: count the arrival as dropped instead of piling up sandboxes.
                    c.t_start = c.t_released = time.time()
                    c.error = "dropped: in-flight cap reached"
                    out.write(json.dumps(asdict(c)) + "\n")
                    finished.append((c.t_released, False))
                    i += 1
                    t_next += rng.expovariate(rate)
                    continue
                task = asyncio.ensure_future(tracked(c, out))
                pending.add(task)
                task.add_done_callback(pending.discard)
                i += 1
                t_next += rng.expovariate(rate)
            if pending:
                await asyncio.gather(*pending)
    await arm.teardown()


def proc_main(args, proc: int, n_procs: int) -> None:
    asyncio.run(run_proc(args, proc, n_procs))


def cmd_run(args) -> None:
    if args.mode == "closed" and not args.workers:
        sys.exit("closed mode needs --workers")
    if args.mode == "burst" and not args.calls:
        sys.exit("burst mode needs --calls")
    if args.mode == "open" and not args.rate:
        sys.exit("open mode needs --rate")
    if args.arm == "reuse" and args.mode != "closed":
        sys.exit("the reuse arm only makes sense in closed mode (one sandbox per worker)")
    n = max(1, min(args.procs, args.workers or args.calls or args.procs))
    meta = {k: v for k, v in vars(args).items() if k != "func"} | {"procs": n, "started": time.time()}
    with open(args.out + ".meta.json", "w") as f:
        json.dump(meta, f)
    if n == 1:
        proc_main(args, 0, 1)
    else:
        ps = [multiprocessing.Process(target=proc_main, args=(args, p, n)) for p in range(n)]
        for p in ps:
            p.start()
        for p in ps:
            p.join()
        with open(args.out, "w") as merged:
            for p in range(n):
                part = f"{args.out}.p{p}"
                with open(part) as f:
                    merged.write(f.read())
                os.remove(part)
    summarize([args.out])


# --- Warm pool control ----------------------------------------------------------


def cmd_pool(args) -> None:
    """Set the warm pool's depth and wait until that many sandboxes are ready."""
    from kubernetes import client, config

    config.load_incluster_config()
    api = client.CustomObjectsApi()
    group, version, plural = "extensions.agents.x-k8s.io", "v1beta1", "sandboxwarmpools"
    api.patch_namespaced_custom_object(group, version, NAMESPACE, plural, POOL, {"spec": {"replicas": args.depth}})
    t0 = time.time()
    while True:
        st = api.get_namespaced_custom_object(group, version, NAMESPACE, plural, POOL).get("status", {})
        ready = st.get("readyReplicas", 0) or 0
        if ready >= args.depth and (st.get("replicas", 0) or 0) <= args.depth:
            print(f"pool depth {args.depth}: {ready} ready after {time.time() - t0:.1f}s")
            return
        if time.time() - t0 > args.timeout:
            sys.exit(f"pool not ready after {args.timeout}s: {st}")
        time.sleep(1)


# --- Summaries -------------------------------------------------------------------


def pct(xs: List[float], p: float) -> float:
    if not xs:
        return float("nan")
    xs = sorted(xs)
    return xs[min(len(xs) - 1, max(0, math.ceil(p / 100 * len(xs)) - 1))]


def summarize(paths: List[str]) -> Dict[str, Any]:
    calls = []
    for path in paths:
        with open(path) as f:
            calls.extend(json.loads(line) for line in f if line.strip())
    measured = [c for c in calls if not c["warmup"]]
    ok = [c for c in measured if c["ok"]]
    if measured and measured[0]["mode"] == "burst":
        window = max(c["t_released"] for c in measured) - min(c["t_start"] for c in measured)
    else:
        window = (max(c["t_released"] for c in measured) - min(c["t_start"] for c in measured)) if measured else 0
    e2e = [c["t_released"] - c["t_start"] for c in ok]
    acq = [c["t_acquired"] - c["t_start"] for c in ok]
    tool = [c["t_tool_done"] - c["t_acquired"] for c in ok]
    rel = [c["t_released"] - c["t_tool_done"] for c in ok]
    s = {
        "files": paths, "calls": len(measured), "ok": len(ok), "errors": len(measured) - len(ok),
        "window_s": round(window, 2), "tool_calls_per_s": round(len(ok) / window, 2) if window else 0,
        "e2e_s": {p: round(pct(e2e, p), 3) for p in (50, 95, 99)},
        "acquire_s": {p: round(pct(acq, p), 3) for p in (50, 95, 99)},
        "tool_s": {p: round(pct(tool, p), 3) for p in (50, 95)},
        "release_s": {p: round(pct(rel, p), 3) for p in (50, 95)},
        "overhead_ratio_p50": round((pct(e2e, 50) - pct(tool, 50)) / pct(tool, 50), 2) if ok else None,
    }
    errs: Dict[str, int] = {}
    for c in measured:
        if not c["ok"]:
            key = (c["error"] or "?").split(":")[0]
            errs[key] = errs.get(key, 0) + 1
    s["error_kinds"] = errs
    print(json.dumps(s, indent=1))
    return s


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--arm", choices=sorted(ARMS), required=True)
    r.add_argument("--mode", choices=["closed", "burst", "open"], required=True)
    r.add_argument("--workers", type=int, default=0)
    r.add_argument("--calls", type=int, default=0)
    r.add_argument("--rate", type=float, default=0.0)
    r.add_argument("--duration", type=float, default=180)
    r.add_argument("--warmup", type=float, default=30)
    r.add_argument("--procs", type=int, default=max(1, (os.cpu_count() or 2) - 1), help="worker processes")
    r.add_argument("--seed", type=int, default=1)
    r.add_argument("--max-error-rate", type=float, default=0.05, help="stop rule: error share over the last 30s")
    r.add_argument("--max-latency", type=float, default=60, help="stop rule: seconds any call may stay in flight")
    r.add_argument("--max-inflight", type=int, default=64, help="open mode: cap on concurrent calls (extra arrivals are dropped)")
    r.add_argument("--out", required=True)
    r.set_defaults(func=cmd_run)
    q = sub.add_parser("pool")
    q.add_argument("--depth", type=int, required=True)
    q.add_argument("--timeout", type=float, default=1800)
    q.set_defaults(func=cmd_pool)
    s = sub.add_parser("summarize")
    s.add_argument("files", nargs="+")
    s.set_defaults(func=lambda a: summarize(a.files))
    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
