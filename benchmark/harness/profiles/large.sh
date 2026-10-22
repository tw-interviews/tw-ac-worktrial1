# Large profile: the benchmark on dedicated node groups (planned: 10, then 20,
# c7i.2xlarge with prefix delegation and kubelet maxPods 110, plus one
# c7i.4xlarge load generator; see ../deploy/eks/README.md). NOT YET RUN.
#
# With one sandbox per call, calls/s equals pods created per second, which the
# cluster caps far below workers / 0.3 s. So the grid finds each design's
# ceiling and compares them below it, rather than pushing every arm to 2048
# workers; only reuse, which creates nothing, goes that far. Stop rules and
# MAX_INFLIGHT bound each cell.

API_REPLICAS=10
SWEEP_FLOOR=0.5      # open-loop sweeps run until delivered < 50% of offered
MAX_INFLIGHT=600

grid_quick() {
  DUR=30 WARM=10
  for arm in sandboxmanager agent-sandbox reuse; do closed "$arm" 0 8; done
  closed agent-sandbox 32 8
  burst agent-sandbox 32 32
  open_sweep agent-sandbox 0 2 5
}

grid_full() {
  DUR=120 WARM=30
  for w in 1 8 32 128 512; do
    for arm in sandboxmanager agent-sandbox; do closed "$arm" 0 "$w"; done
  done
  for w in 1 32 128 512 2048; do closed reuse 0 "$w"; done
  for d in 32 128 512; do
    for w in 32 128 512; do closed agent-sandbox "$d" "$w"; done
  done
  for d in 128 512; do burst agent-sandbox "$d" "$d"; done
  burst agent-sandbox 0 512
  burst sandboxmanager 0 512
  burst agent-sandbox 512 2048        # the stress point: a burst four times the pool
  open_sweep sandboxmanager 0 5 10 20 40 80 160 320
  open_sweep agent-sandbox 0 5 10 20 40 80 160 320
  open_sweep agent-sandbox 512 5 10 20 40 80 160 320
}

grid_startup() {
  DUR=120 WARM=30
  for w in 8 128 512; do
    for arm in sandboxmanager agent-sandbox; do closed "$arm" 0 "$w"; done
  done
  burst agent-sandbox 0 512
  burst sandboxmanager 0 512
  open_sweep sandboxmanager 0 5 10 20 40 80 160 320
  open_sweep agent-sandbox 0 5 10 20 40 80 160 320
}
