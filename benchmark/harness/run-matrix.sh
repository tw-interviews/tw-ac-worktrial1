#!/bin/bash
# Run a benchmark grid against a deployed benchmark (../deploy/deploy.sh up).
#
#   harness/run-matrix.sh PROFILE MODE
#
# PROFILE is a file in profiles/ (small: 5 small nodes; large: ~10-20 nodes
# after deploy.sh pin). MODE is a grid that profile defines:
#   quick    a short validation pass
#   full     the profile's matrix
#   startup  only the cells where sandbox startup matters (rerun after
#            changing the runtime image)
#   open     only the open-loop (sustained load) sweeps
#
# Copies toolcall_bench.py into the loadgen pod, scales the benchmark's
# SandboxManager to the profile's API_REPLICAS, applies the agent-sandbox
# controller tuning, then runs each cell with a cooldown in between (warm pool
# at the cell's depth, nothing else running or terminating). Each cell runs
# detached in the pod, so a dropped `kubectl exec` stream doesn't lose it.
# Results: harness/results/<time>_<profile>_<mode>/ (one .jsonl per cell,
# summary.jsonl, summary.tsv, run.log).
set -euo pipefail
cd "$(dirname "$0")"
PROFILE=${1:-} MODE=${2:-}
[ -f "profiles/$PROFILE.sh" ] && [ -n "$MODE" ] || { sed -n '2,20p' "$0"; exit 1; }
# shellcheck source=/dev/null
source "profiles/$PROFILE.sh"
declare -F "grid_$MODE" >/dev/null || { echo "profile $PROFILE has no '$MODE' grid"; exit 1; }

CONTEXT=${KUBE_CONTEXT:?set KUBE_CONTEXT to the cluster kubeconfig context}
k() { kubectl --context="$CONTEXT" "$@"; }
OUT=results/$(date +%Y%m%d-%H%M%S)_${PROFILE}_$MODE
mkdir -p "$OUT"
POD=$(k -n toolcall-loadgen get pods -l app=loadgen -o jsonpath='{.items[0].metadata.name}')
lg() { k -n toolcall-loadgen exec "$POD" -- "$@"; }
RUNTIME_IMAGE=$(k -n toolcall-bench get sandboxtemplate toolcall-runtime -o jsonpath='{.spec.podTemplate.spec.containers[0].image}')
log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT/run.log"; }
# retry CMD...: up to 5 attempts, for kubectl calls that hit a reset connection.
retry() { local i; for i in 1 2 3 4 5; do "$@" && return 0; sleep 3; done; return 1; }

# --- setup ----------------------------------------------------------------------
log "profile $PROFILE, mode $MODE; runtime image ${RUNTIME_IMAGE##*/}; results in $OUT"
k cp toolcall_bench.py "toolcall-loadgen/$POD:/opt/bench/toolcall_bench.py"
k -n webapp scale deploy/sandboxmanager-benchmark --replicas="$API_REPLICAS" >/dev/null
k -n webapp rollout status deploy/sandboxmanager-benchmark --timeout=180s >/dev/null
../deploy/deploy.sh tune-upstream >/dev/null
log "setup done: harness copied, benchmark SandboxManager at $API_REPLICAS replicas, agent-sandbox controller tuned"

# Pods in toolcall-bench other than the pre-pull DaemonSet's (which has the app label).
sandbox_pods() { k -n toolcall-bench get pods -l '!app' --no-headers 2>/dev/null | grep -c . || true; }
terminating() { k -n toolcall-bench get pods -l '!app' -o jsonpath='{range .items[*]}{.metadata.deletionTimestamp}{"\n"}{end}' 2>/dev/null | grep -c . || true; }

# cooldown DEPTH: set the warm pool to DEPTH and wait until it's full and
# nothing else is running or terminating.
cooldown() {
  local depth=$1 t0=$SECONDS
  retry lg python toolcall_bench.py pool --depth "$depth" --timeout 600 >/dev/null
  while :; do
    local n t
    n=$(sandbox_pods); t=$(terminating)
    [ "$t" -eq 0 ] && [ "$n" -le "$depth" ] && break
    [ $((SECONDS - t0)) -gt 600 ] && { log "cooldown: still $n pods ($t terminating) after 600s; continuing"; break; }
    sleep 3
  done
}

# label ARM DEPTH: how a cell names its sandbox source.
label() {
  case "$1" in
    agent-sandbox) [ "$2" -eq 0 ] && echo agent-sandbox-cold || echo "agent-sandbox-pool$2" ;;
    *) echo "$1" ;;
  esac
}

# cell NAME DEPTH ARGS...: run one cell against a pool of DEPTH, save its calls
# and append its summary (plus the stop-rule reason, if any) to summary.jsonl.
cell() {
  local name=$1 depth=$2; shift 2
  cooldown "$depth"
  log "cell $name (pool $depth): $*"
  local cmd="cd /opt/bench && RUNTIME_IMAGE=$RUNTIME_IMAGE python toolcall_bench.py run $* --out /tmp/$name.jsonl > /tmp/$name.out 2>&1; echo \$? > /tmp/$name.rc"
  retry lg sh -c "rm -f /tmp/$name.rc; setsid sh -c '$cmd' > /dev/null 2>&1 &"
  local rc=""
  until [ -n "$rc" ]; do
    sleep 5
    rc=$(lg cat "/tmp/$name.rc" 2>/dev/null || true)
  done
  [ "$rc" = 0 ] || log "cell $name: harness exited $rc"
  retry lg cat "/tmp/$name.out" >"$OUT/$name.out"
  retry lg cat "/tmp/$name.jsonl" >"$OUT/$name.jsonl" || true
  local stop; stop=$(lg sh -c "cat /tmp/$name.jsonl.stop 2>/dev/null; rm -f /tmp/$name.jsonl* /tmp/$name.out /tmp/$name.rc" | tr '\n' ' ')
  python3 - "$OUT" "$name" "$depth" "$stop" "$*" <<'PY'
import json, sys
out, name, depth, stop, args = sys.argv[1:]
text = open(f"{out}/{name}.out").read()
i = text.find("{\n")
s = json.loads(text[i:]) if i >= 0 else {"error": text[-500:]}
s.update(cell=name, pool_depth=int(depth), args=args, stopped=stop.strip() or None)
s.pop("files", None)
with open(f"{out}/summary.jsonl", "a") as f:
    f.write(json.dumps(s) + "\n")
print(f"    -> {s.get('tool_calls_per_s')} calls/s, acquire p50 {s.get('acquire_s', {}).get('50')} s, "
      f"e2e p95 {s.get('e2e_s', {}).get('95')} s, errors {s.get('errors')}" + (f", STOPPED: {s['stopped']}" if s['stopped'] else ""))
PY
}

# Building blocks for profiles. W = workers, N = calls, R = calls/s.
closed() { # ARM DEPTH W
  cell "closed-loop_$(label "$1" "$2")_w$(printf %02d "$3")" "$2" --arm "$1" --mode closed --workers "$3" --duration "$DUR" --warmup "$WARM"
}
burst() { # ARM DEPTH N
  cell "burst_$(label "$1" "$2")_n$(printf %02d "$3")" "$2" --arm "$1" --mode burst --calls "$3"
}
# open_sweep ARM DEPTH RATES...: step the arrival rate up until delivered
# throughput falls below SWEEP_FLOOR (default 0.5, i.e. 50% degradation) of the
# offered rate. A cell a stop rule ended early still counts, judged on the
# calls it delivered before the stop; it doesn't end the sweep by itself.
open_sweep() {
  local arm=$1 depth=$2; shift 2
  for rate in "$@"; do
    cell "open-loop_$(label "$arm" "$depth")_r$rate" "$depth" --arm "$arm" --mode open --rate "$rate" \
      --duration "$DUR" --warmup "$WARM" --max-inflight "$MAX_INFLIGHT"
    if python3 - "$OUT/summary.jsonl" "$rate" "${SWEEP_FLOOR:-0.5}" <<'PY'
import json, sys
s = json.loads(open(sys.argv[1]).read().strip().splitlines()[-1])
sys.exit(0 if (s.get("tool_calls_per_s") or 0) < float(sys.argv[3]) * float(sys.argv[2]) else 1)
PY
    then log "sweep $(label "$arm" "$depth"): delivered below ${SWEEP_FLOOR:-0.5} x offered at rate $rate"; break; fi
  done
}

"grid_$MODE"

cooldown 0
python3 - "$OUT" <<'PY'
import json, sys
out = sys.argv[1]
rows = [json.loads(l) for l in open(f"{out}/summary.jsonl")]
cols = ["cell", "pool_depth", "calls", "errors", "tool_calls_per_s", "acquire_p50", "acquire_p95", "e2e_p50", "e2e_p95", "stopped"]
with open(f"{out}/summary.tsv", "w") as f:
    f.write("\t".join(cols) + "\n")
    for r in rows:
        f.write("\t".join(str(x) for x in [r["cell"], r["pool_depth"], r.get("calls"), r.get("errors"), r.get("tool_calls_per_s"),
            r.get("acquire_s", {}).get("50"), r.get("acquire_s", {}).get("95"), r.get("e2e_s", {}).get("50"), r.get("e2e_s", {}).get("95"),
            r.get("stopped") or ""]) + "\n")
print(open(f"{out}/summary.tsv").read())
PY
log "done: $OUT"
