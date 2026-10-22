# Sandbox tool-call benchmark

How many tool calls per second can you run when every tool call gets its own
Kubernetes sandbox? This repo compares four ways of getting that sandbox:

| Arm | How a sandbox is acquired | How the tool runs |
|---|---|---|
| **SandboxManager** (`sandboxmanager`) | An HTTP API (in [`sandboxmanager/`](sandboxmanager/)) that creates an agent-sandbox `Sandbox` and records it in Postgres | Kubernetes exec, through the API |
| **agent-sandbox, cold start** (`agent-sandbox`, pool 0) | The upstream [kubernetes-sigs/agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox) SDK claims a sandbox; none are pre-started | HTTP straight to the pod |
| **agent-sandbox, warm pool** (`agent-sandbox`, pool N) | The same SDK claims a pre-started sandbox from a `SandboxWarmPool` of N | HTTP straight to the pod |
| **agent-sandbox, reused** (`reuse`) | One sandbox per worker, created before the run and reused: the no-acquire-cost upper bound | HTTP straight to the pod |

A tool call is `sleep` for a duration drawn from N(300 ms, 75 ms), clamped to
≥ 50 ms; worker *k*'s *i*-th call sleeps the same time in every arm.

## Results so far

Small profile, 5 × c7i-flex.large on EKS 1.37, one run per cell, 0 errors. Full
tables and charts: [`benchmark/results/`](benchmark/results/).

- **The cluster's ceiling, ~2–2.5 calls/s, applied to every arm that creates a sandbox per
  call.** Those nodes were created before VPC CNI prefix delegation was enabled and run out of
  pod IPs under churn (a deleted pod's IP waits 30 s before reuse). The large profile, on fresh
  nodes, is where the designs will separate.
- **Reuse scales linearly** (104 calls/s at 32 workers): creating sandboxes is the whole cost.
- **Warm pools absorb bursts.** A pool of 32 served a 32-call burst at 13.45 calls/s with
  0.67 s acquires; with no pool the same burst ran at 1.39 calls/s.
- **"Ready" differs between the arms.** agent-sandbox waits for the in-sandbox runtime to
  answer; SandboxManager only for the container to start, since it runs tools through exec.
  With the ~440 MB Python runtime that made cold agent-sandbox acquires ~5.6 s against
  ~1.05 s; the minimal runtime removes most of the difference.

## Layout

```
benchmark/
  harness/        toolcall_bench.py (the load generator), run-matrix.sh, profiles/{small,large}.sh
  runtimes/       python/ (~440 MB "medium" runtime), minimal/ (~10 MB Go on busybox)
  deploy/         k8s/benchmark.yaml, deploy.sh, build-images.sh, eks/ (cluster setup)
  results/        published runs: summary.tsv, raw per-call data (gzipped), report pages
  docs/           how-it-works.md: how requests are submitted, each arm's request lifetime,
                  and a measured timeline of one worker's calls (Mermaid)
  tools/          import-results.py, timing-diagram.py (Mermaid timeline from a run's raw data)
sandboxmanager/   the SandboxManager service, node agent, Python SDK and its own docs
```

## Reproducing

You need an EKS cluster running SandboxManager and the agent-sandbox controller
v1.0.7 with extensions; see [`benchmark/deploy/eks/README.md`](benchmark/deploy/eks/README.md).

```sh
cp .env.example .env    # fill in KUBE_CONTEXT, AWS_REGION, ...
set -a; source .env; set +a

sandboxmanager/deploy-eks.sh                     # SandboxManager itself
benchmark/deploy/build-images.sh all             # runtimes, loadgen, the benchmark's SandboxManager
SANDBOX_MANAGER_IMAGE=<sandbox-manager:benchmark-…> \
  benchmark/deploy/deploy.sh up <runtime image> <loadgen image>
benchmark/deploy/deploy.sh pin                   # large profile only, once its node groups exist

benchmark/harness/run-matrix.sh small quick      # ~10 min: does every arm work?
benchmark/harness/run-matrix.sh small full       # ~1.5–2 h
```

`benchmark/deploy/deploy.sh down` removes the benchmark; `untune-upstream`
restores the agent-sandbox controller's defaults.

## Notes

- The harness stops a cell when > 5% of calls in the last 30 s failed or a call has
  been in flight over 60 s (checked between calls), and open-loop runs cap calls in
  flight; see `harness/toolcall_bench.py`.
- The benchmark's SandboxManager copy is tuned (more replicas, no client-side
  Kubernetes rate limit, HTTP/1.1, a larger DB pool), as is the agent-sandbox
  controller (more API connections, resource requests); see `deploy/deploy.sh`.
  Untuned, SandboxManager's default 5 requests/s Kubernetes client limit capped it at
  ~1.7 calls/s regardless of design.
- NetworkPolicy enforcement was off on the test cluster; the sandboxes only sleep.
