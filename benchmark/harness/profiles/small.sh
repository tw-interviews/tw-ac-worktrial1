# Small profile: the benchmark on a handful of small nodes (it was run on
# 5 x c7i-flex.large, 2 vCPU / 29 pods each). About 88 pod slots are free for
# sandboxes, and on nodes without prefix delegation the per-node pod-IP supply
# caps one-sandbox-per-call at ~2-2.5 calls/s, so the grid stops at 32 workers,
# pools of 32 and rates of 6/s.

API_REPLICAS=4       # the benchmark's SandboxManager; fewer replicas free pod slots
MAX_INFLIGHT=64      # open-loop cap on concurrent calls

grid_quick() {       # ~10 minutes: does every arm work end to end?
  DUR=15 WARM=5
  for arm in sandboxmanager agent-sandbox reuse; do closed "$arm" 0 2; done
  closed agent-sandbox 4 2
  burst agent-sandbox 4 4
  open_sweep agent-sandbox 0 0.5 1
}

grid_full() {        # ~1.5-2 hours
  DUR=60 WARM=15
  for w in 1 2 4 8 16 32; do
    for arm in sandboxmanager agent-sandbox reuse; do closed "$arm" 0 "$w"; done
  done
  for d in 8 16 32; do
    for w in 4 8 16; do closed agent-sandbox "$d" "$w"; done
  done
  for d in 8 16 32; do burst agent-sandbox "$d" "$d"; done
  burst agent-sandbox 0 32
  burst sandboxmanager 0 32
  open_sweep sandboxmanager 0 0.5 1 2 3 4 6
  open_sweep agent-sandbox 0 0.5 1 2 3 4 6
  open_sweep agent-sandbox 32 0.5 1 2 3 4 6
}

grid_startup() {     # ~35 minutes: the cells sandbox startup time affects
  DUR=60 WARM=15
  for w in 1 4 16 32; do
    for arm in sandboxmanager agent-sandbox; do closed "$arm" 0 "$w"; done
  done
  for w in 4 16; do closed agent-sandbox 32 "$w"; done
  burst agent-sandbox 0 32
  burst sandboxmanager 0 32
  burst agent-sandbox 32 32
  open_sweep sandboxmanager 0 0.5 1 2 3 4 6
  open_sweep agent-sandbox 0 0.5 1 2 3 4 6
}
