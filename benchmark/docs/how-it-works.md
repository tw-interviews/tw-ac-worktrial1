# How the benchmark works

How a run is driven, how load is generated, the life of one tool call through
each way of getting a sandbox, and a measured timeline of one worker's calls.

## 1. How a run is driven

`harness/run-matrix.sh` runs on your machine and drives everything through
`kubectl`. Each cell of the grid runs inside the cluster, detached, so a dropped
`kubectl exec` connection can't lose it.

```mermaid
flowchart TD
    A["run-matrix.sh PROFILE MODE<br/>(your machine)"] --> B["Setup: copy toolcall_bench.py into the loadgen pod,<br/>scale the benchmark SandboxManager,<br/>tune the agent-sandbox controller"]
    B --> C{"Next cell in<br/>the profile's grid?"}
    C -- yes --> D["Cooldown: set the warm pool to the cell's depth,<br/>wait until it's full and no sandbox pods<br/>are running or terminating"]
    D --> E["Start the harness detached in the loadgen pod<br/>(setsid; output and exit code go to files)"]
    E --> F["Poll every 5 s for the exit-code file"]
    F --> G["Copy the cell's per-call .jsonl and summary back,<br/>append to summary.jsonl"]
    G --> H{"Rate sweep: delivered below 90% of offered,<br/>or a stop rule tripped?"}
    H -- "yes: ceiling found" --> C
    H -- no --> C
    C -- no --> I["Cooldown to pool 0, write summary.tsv"]
```

## 2. How load is generated

The harness runs in the `loadgen` pod in `toolcall-loadgen`. It splits the
workers (closed loop), the calls (burst) or the arrival rate (open loop) across
several processes, each an asyncio event loop. Every worker reads its tool
durations from its own seeded random sequence, so worker *k*'s *i*-th call
sleeps the same time in every arm.

```mermaid
flowchart LR
    subgraph LG["loadgen pod (namespace toolcall-loadgen)"]
        H["toolcall_bench.py run"] --> P1["process 1<br/>asyncio loop"]
        H --> P2["process 2<br/>asyncio loop"]
        H --> PN["process N"]
        P1 --> W1["workers / arrivals"]
        P2 --> W2["workers / arrivals"]
    end
    W1 & W2 -- "sandboxmanager arm<br/>(HTTP, sandbox_sdk)" --> SM["Benchmark SandboxManager API<br/>sandboxmanager-benchmark (webapp)"]
    W1 & W2 -- "agent-sandbox and reuse arms<br/>(Kubernetes API, k8s-agent-sandbox)" --> KA["Kubernetes API server"]
    W1 & W2 -- "agent-sandbox tool calls<br/>(HTTP to the pod IP, :8888)" --> SB["Sandbox pods<br/>(namespace toolcall-bench)"]
    SM -- "create / delete Sandbox,<br/>exec into pods" --> KA
    SM --- DB[("Postgres")]
    KA --> CTRL["agent-sandbox controller"]
    CTRL -- "creates / deletes pods,<br/>keeps the warm pool full" --> SB
    KA -- "exec stream<br/>(via the kubelet)" --> SB
```

The three load modes:

| Mode | What drives it | Measures |
|---|---|---|
| Closed loop | W workers, each starting its next call as soon as the last finishes | Throughput and latency at a fixed concurrency |
| Burst | N calls started at the same instant | How fast a fan-out is absorbed (e.g. against a pool of N) |
| Open loop | Poisson arrivals at R calls/s, capped at MAX_INFLIGHT in flight | The highest rate an arm sustains; the sweep stops at the first rate delivered below 90% |

## 3. One tool call: SandboxManager

Every tool call records four timestamps: **t_start** (acquire begins),
**t_acquired** (a sandbox is ready), **t_tool_done** (the tool finished) and
**t_released** (release finished). Acquire is `t_acquired − t_start`;
end-to-end is `t_released − t_start`.

```mermaid
sequenceDiagram
    autonumber
    participant W as Worker (sandbox_sdk)
    participant API as SandboxManager API
    participant DB as Postgres
    participant K as Kubernetes API
    participant C as agent-sandbox controller
    participant P as Sandbox pod

    Note over W: t_start
    W->>API: POST /sandboxes {image, ttl}
    API->>K: dry-run the pod against quota / LimitRange
    API->>DB: insert the sandbox row (transaction)
    API->>K: create Sandbox
    API->>DB: commit
    API-->>W: 201 {sandbox_id}
    K->>C: Sandbox created
    C->>K: create pod
    K->>P: schedule, start the container
    loop every 0.5 s until Ready
        W->>API: GET /sandboxes/{id}
        API->>K: get Sandbox (Ready condition)
        API-->>W: status
    end
    Note over W: t_acquired<br/>(Ready = container running: no readiness probe,<br/>since tools run through Kubernetes exec)
    W->>API: POST /sandbox/executions {sleep 0.3}
    API->>K: find the sandbox's pod
    API->>DB: refresh the TTL, record the execution
    API->>K: exec into the pod (WebSocket)
    K->>P: run sleep via the kubelet
    P-->>API: exit code, output
    API->>DB: record the result
    API-->>W: 200 {exit_code, stdout}
    Note over W: t_tool_done
    W->>API: DELETE /sandboxes/{id}?purge=true
    API->>K: delete Sandbox (pod drains in the background)
    API->>DB: delete the row
    API-->>W: 204
    Note over W: t_released
```

## 4. One tool call: agent-sandbox, cold start

The warm pool is at depth 0, so the claim makes the controller create a sandbox
from the template and wait for its runtime to answer.

```mermaid
sequenceDiagram
    autonumber
    participant W as Worker (k8s-agent-sandbox)
    participant K as Kubernetes API
    participant C as agent-sandbox controller
    participant P as Sandbox pod

    Note over W: t_start
    W->>K: create SandboxClaim (warmPoolRef: toolcall-pool, depth 0)
    K->>C: claim created
    C->>K: create Sandbox from template toolcall-runtime
    C->>K: create pod
    K->>P: schedule, start the container
    loop readiness probe, every 1 s, until the runtime answers
        K->>P: GET / on :8888
    end
    C->>K: claim Ready, with the pod IP
    K-->>W: watch event: Ready
    Note over W: t_acquired<br/>(Ready = the in-sandbox runtime answers)
    W->>P: POST http://pod-ip:8888/execute {sleep 0.3}
    P-->>W: {exit_code, stdout}
    Note over W: t_tool_done
    W->>K: delete SandboxClaim
    K->>C: claim deleted
    C->>K: delete the sandbox and pod (in the background)
    Note over W: t_released
```

## 5. One tool call: agent-sandbox, warm pool

The pool keeps N sandboxes already running and Ready. A claim adopts one, so
acquiring skips pod creation and startup; the controller then starts a
replacement. When workers outpace the refill, the pool runs dry and claims fall
back to the cold path above.

```mermaid
sequenceDiagram
    autonumber
    participant W as Worker (k8s-agent-sandbox)
    participant K as Kubernetes API
    participant C as agent-sandbox controller
    participant WP as Warm pool (N running sandboxes)
    participant P as Adopted sandbox pod

    Note over WP: kept full ahead of time:<br/>pods created, started and Ready
    Note over W: t_start
    W->>K: create SandboxClaim (warmPoolRef: toolcall-pool)
    K->>C: claim created
    C->>WP: adopt a Ready sandbox
    WP-->>C: running pod (P)
    C->>K: claim Ready, with P's IP
    K-->>W: watch event: Ready
    Note over W: t_acquired
    par refill the pool
        C->>K: create a replacement sandbox and pod
        K->>WP: new pod starts, becomes Ready
    and run the tool
        W->>P: POST http://pod-ip:8888/execute {sleep 0.3}
        P-->>W: {exit_code, stdout}
    end
    Note over W: t_tool_done
    W->>K: delete SandboxClaim
    K->>C: claim deleted
    C->>K: delete the adopted sandbox and pod
    Note over W: t_released
```

## 6. One tool call: reused sandbox

The upper bound. Each worker claims one sandbox before the measured run starts
and keeps it; every tool call is a single HTTP request. Acquire and release are
a handoff inside the worker, so they take no measurable time.

```mermaid
sequenceDiagram
    autonumber
    participant W as Worker
    participant Q as Worker's own sandbox (held)
    participant P as Sandbox pod

    Note over W,P: before the run: one cold claim per worker (as in section 4)
    loop every tool call
        Note over W: t_start
        W->>Q: take the held sandbox
        Note over W: t_acquired (≈ t_start)
        W->>P: POST http://pod-ip:8888/execute {sleep 0.3}
        P-->>W: {exit_code, stdout}
        Note over W: t_tool_done
        W->>Q: hand it back
        Note over W: t_released (≈ t_tool_done)
    end
    Note over W,P: after the run: delete every held claim
```

## 7. One worker's request pipeline, measured

Real tool calls from the closed-loop runs with 4 workers (small profile, Python
runtime, `results/2026-10-22_small_python-runtime/`), showing **worker 1** only:
the first 12 s after warm-up. A worker starts its next call as soon as the last
is released, so its row is a back-to-back chain of **acquire** (red), **tool**
(blue) and **release** (grey); the other three workers run the same kind of chain
alongside it. Times are seconds from the first measured call in the run.
Generated with `tools/timing-diagram.py --worker 1`.

SandboxManager: acquire is ~1 s while pod IPs last; some calls wait much longer
for one.

```mermaid
gantt
    title SandboxManager, worker 1 of 4
    dateFormat YYYY-MM-DD HH:mm:ss.SSS
    axisFormat %S s
    tickInterval 2second
    section worker 1
    acquire :crit, 2026-01-01 00:00:00.445, 2026-01-01 00:00:02.010
    tool :active, 2026-01-01 00:00:02.010, 2026-01-01 00:00:02.628
    release :done, 2026-01-01 00:00:02.628, 2026-01-01 00:00:02.657
    acquire :crit, 2026-01-01 00:00:02.658, 2026-01-01 00:00:04.226
    tool :active, 2026-01-01 00:00:04.226, 2026-01-01 00:00:04.855
    release :done, 2026-01-01 00:00:04.855, 2026-01-01 00:00:04.884
    acquire :crit, 2026-01-01 00:00:04.884, 2026-01-01 00:00:05.933
    tool :active, 2026-01-01 00:00:05.933, 2026-01-01 00:00:06.574
    release :done, 2026-01-01 00:00:06.574, 2026-01-01 00:00:06.599
    acquire :crit, 2026-01-01 00:00:06.599, 2026-01-01 00:00:08.205
    tool :active, 2026-01-01 00:00:08.205, 2026-01-01 00:00:08.692
    release :done, 2026-01-01 00:00:08.692, 2026-01-01 00:00:08.726
    acquire :crit, 2026-01-01 00:00:08.726, 2026-01-01 00:00:10.325
    tool :active, 2026-01-01 00:00:10.325, 2026-01-01 00:00:10.888
    release :done, 2026-01-01 00:00:10.888, 2026-01-01 00:00:10.917
    acquire :crit, 2026-01-01 00:00:10.917, 2026-01-01 00:00:13.525
    tool :active, 2026-01-01 00:00:13.525, 2026-01-01 00:00:14.067
    release :done, 2026-01-01 00:00:14.067, 2026-01-01 00:00:14.094
```

agent-sandbox, cold start: every call waits ~5–6 s for a sandbox whose runtime
answers.

```mermaid
gantt
    title agent-sandbox cold start, worker 1 of 4
    dateFormat YYYY-MM-DD HH:mm:ss.SSS
    axisFormat %S s
    tickInterval 2second
    section worker 1
    acquire :crit, 2026-01-01 00:00:00.059, 2026-01-01 00:00:05.505
    tool :active, 2026-01-01 00:00:05.505, 2026-01-01 00:00:05.685
    release :done, 2026-01-01 00:00:05.685, 2026-01-01 00:00:05.696
    acquire :crit, 2026-01-01 00:00:05.696, 2026-01-01 00:00:11.844
    tool :active, 2026-01-01 00:00:11.844, 2026-01-01 00:00:12.115
    release :done, 2026-01-01 00:00:12.115, 2026-01-01 00:00:12.130
```

agent-sandbox with a warm pool of 32: while the pool lasts, acquire shrinks to a
sliver and the tool dominates; later calls wait for the pool to refill.

```mermaid
gantt
    title agent-sandbox, warm pool of 32, worker 1 of 4 (1 call cut off at 15 s)
    dateFormat YYYY-MM-DD HH:mm:ss.SSS
    axisFormat %S s
    tickInterval 2second
    section worker 1
    acquire :crit, 2026-01-01 00:00:00.000, 2026-01-01 00:00:00.051
    tool :active, 2026-01-01 00:00:00.051, 2026-01-01 00:00:00.297
    release :done, 2026-01-01 00:00:00.297, 2026-01-01 00:00:00.308
    acquire :crit, 2026-01-01 00:00:00.309, 2026-01-01 00:00:00.415
    tool :active, 2026-01-01 00:00:00.415, 2026-01-01 00:00:00.775
    release :done, 2026-01-01 00:00:00.775, 2026-01-01 00:00:00.788
    acquire :crit, 2026-01-01 00:00:00.788, 2026-01-01 00:00:01.022
    tool :active, 2026-01-01 00:00:01.022, 2026-01-01 00:00:01.450
    release :done, 2026-01-01 00:00:01.450, 2026-01-01 00:00:01.466
    acquire :crit, 2026-01-01 00:00:01.466, 2026-01-01 00:00:01.662
    tool :active, 2026-01-01 00:00:01.662, 2026-01-01 00:00:01.887
    release :done, 2026-01-01 00:00:01.887, 2026-01-01 00:00:01.903
    acquire :crit, 2026-01-01 00:00:01.903, 2026-01-01 00:00:02.137
    tool :active, 2026-01-01 00:00:02.137, 2026-01-01 00:00:02.520
    release :done, 2026-01-01 00:00:02.520, 2026-01-01 00:00:02.536
    acquire :crit, 2026-01-01 00:00:02.537, 2026-01-01 00:00:02.691
    tool :active, 2026-01-01 00:00:02.691, 2026-01-01 00:00:02.984
    release :done, 2026-01-01 00:00:02.984, 2026-01-01 00:00:03.000
    acquire :crit, 2026-01-01 00:00:03.000, 2026-01-01 00:00:03.109
    tool :active, 2026-01-01 00:00:03.109, 2026-01-01 00:00:03.657
    release :done, 2026-01-01 00:00:03.657, 2026-01-01 00:00:03.671
    acquire :crit, 2026-01-01 00:00:03.671, 2026-01-01 00:00:03.729
    tool :active, 2026-01-01 00:00:03.729, 2026-01-01 00:00:04.119
    release :done, 2026-01-01 00:00:04.119, 2026-01-01 00:00:04.135
    acquire :crit, 2026-01-01 00:00:04.135, 2026-01-01 00:00:04.257
    tool :active, 2026-01-01 00:00:04.257, 2026-01-01 00:00:04.542
    release :done, 2026-01-01 00:00:04.542, 2026-01-01 00:00:04.561
    acquire :crit, 2026-01-01 00:00:04.561, 2026-01-01 00:00:04.643
    tool :active, 2026-01-01 00:00:04.643, 2026-01-01 00:00:04.964
    release :done, 2026-01-01 00:00:04.964, 2026-01-01 00:00:05.008
    acquire :crit, 2026-01-01 00:00:05.009, 2026-01-01 00:00:05.185
    tool :active, 2026-01-01 00:00:05.185, 2026-01-01 00:00:05.466
    release :done, 2026-01-01 00:00:05.466, 2026-01-01 00:00:05.510
    acquire :crit, 2026-01-01 00:00:05.511, 2026-01-01 00:00:05.794
    tool :active, 2026-01-01 00:00:05.794, 2026-01-01 00:00:06.142
    release :done, 2026-01-01 00:00:06.142, 2026-01-01 00:00:06.156
    acquire :crit, 2026-01-01 00:00:06.156, 2026-01-01 00:00:06.374
    tool :active, 2026-01-01 00:00:06.374, 2026-01-01 00:00:06.768
    release :done, 2026-01-01 00:00:06.768, 2026-01-01 00:00:06.799
    acquire :crit, 2026-01-01 00:00:06.799, 2026-01-01 00:00:06.935
    tool :active, 2026-01-01 00:00:06.935, 2026-01-01 00:00:07.299
    release :done, 2026-01-01 00:00:07.299, 2026-01-01 00:00:07.315
    acquire :crit, 2026-01-01 00:00:07.315, 2026-01-01 00:00:07.484
    tool :active, 2026-01-01 00:00:07.484, 2026-01-01 00:00:07.916
    release :done, 2026-01-01 00:00:07.916, 2026-01-01 00:00:07.953
    acquire :crit, 2026-01-01 00:00:07.953, 2026-01-01 00:00:08.087
    tool :active, 2026-01-01 00:00:08.087, 2026-01-01 00:00:08.522
    release :done, 2026-01-01 00:00:08.522, 2026-01-01 00:00:08.545
    acquire :crit, 2026-01-01 00:00:08.545, 2026-01-01 00:00:14.724
    tool :active, 2026-01-01 00:00:14.724, 2026-01-01 00:00:15.000
```

Reused sandbox (first 3 s; the chain is too dense to read at 12 s): no acquire
at all, so the worker runs a tool every ~0.3 s.

```mermaid
gantt
    title Reused sandbox, worker 1 of 4
    dateFormat YYYY-MM-DD HH:mm:ss.SSS
    axisFormat %S s
    tickInterval 1second
    section worker 1
    acquire :crit, 2026-01-01 00:00:00.064, 2026-01-01 00:00:00.065
    tool :active, 2026-01-01 00:00:00.064, 2026-01-01 00:00:00.418
    release :done, 2026-01-01 00:00:00.418, 2026-01-01 00:00:00.419
    acquire :crit, 2026-01-01 00:00:00.418, 2026-01-01 00:00:00.419
    tool :active, 2026-01-01 00:00:00.418, 2026-01-01 00:00:00.858
    release :done, 2026-01-01 00:00:00.858, 2026-01-01 00:00:00.859
    acquire :crit, 2026-01-01 00:00:00.858, 2026-01-01 00:00:00.859
    tool :active, 2026-01-01 00:00:00.858, 2026-01-01 00:00:01.153
    release :done, 2026-01-01 00:00:01.153, 2026-01-01 00:00:01.154
    acquire :crit, 2026-01-01 00:00:01.153, 2026-01-01 00:00:01.154
    tool :active, 2026-01-01 00:00:01.153, 2026-01-01 00:00:01.399
    release :done, 2026-01-01 00:00:01.399, 2026-01-01 00:00:01.400
    acquire :crit, 2026-01-01 00:00:01.399, 2026-01-01 00:00:01.400
    tool :active, 2026-01-01 00:00:01.399, 2026-01-01 00:00:01.628
    release :done, 2026-01-01 00:00:01.628, 2026-01-01 00:00:01.629
    acquire :crit, 2026-01-01 00:00:01.628, 2026-01-01 00:00:01.629
    tool :active, 2026-01-01 00:00:01.628, 2026-01-01 00:00:01.992
    release :done, 2026-01-01 00:00:01.992, 2026-01-01 00:00:01.993
    acquire :crit, 2026-01-01 00:00:01.992, 2026-01-01 00:00:01.993
    tool :active, 2026-01-01 00:00:01.992, 2026-01-01 00:00:02.255
    release :done, 2026-01-01 00:00:02.255, 2026-01-01 00:00:02.256
    acquire :crit, 2026-01-01 00:00:02.255, 2026-01-01 00:00:02.256
    tool :active, 2026-01-01 00:00:02.255, 2026-01-01 00:00:02.546
    release :done, 2026-01-01 00:00:02.546, 2026-01-01 00:00:02.547
    acquire :crit, 2026-01-01 00:00:02.546, 2026-01-01 00:00:02.547
    tool :active, 2026-01-01 00:00:02.546, 2026-01-01 00:00:02.888
    release :done, 2026-01-01 00:00:02.888, 2026-01-01 00:00:02.889
    acquire :crit, 2026-01-01 00:00:02.888, 2026-01-01 00:00:02.889
    tool :active, 2026-01-01 00:00:02.888, 2026-01-01 00:00:03.196
    release :done, 2026-01-01 00:00:03.196, 2026-01-01 00:00:03.197
```

## Where the arms differ

| | SandboxManager | agent-sandbox |
|---|---|---|
| Acquire is done when | the container is running (no readiness probe) | the in-sandbox runtime answers on :8888 |
| A tool runs through | the API, then a Kubernetes exec stream via the kubelet | one HTTP request straight to the pod |
| Waiting for ready | client polls `GET /sandboxes/{id}` every 0.5 s | the SDK watches the claim |
| State it keeps | a Postgres row per sandbox and execution | Kubernetes objects only |

These differences show up in the results: SandboxManager is ready sooner with a
slow-starting runtime (it never waits for the runtime), while agent-sandbox's
run step is shorter (one HTTP request instead of an exec stream).
