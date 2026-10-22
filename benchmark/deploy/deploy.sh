#!/bin/bash
# Deploy the tool-call benchmark to an EKS cluster that already runs
# SandboxManager (../../sandboxmanager/deploy-eks.sh) and the upstream
# agent-sandbox controller with its extensions (v1.0.7).
#
#   deploy/deploy.sh up RUNTIME_IMAGE LOADGEN_IMAGE
#       Apply k8s/benchmark.yaml and create sandboxmanager-benchmark: a copy of
#       the SandboxManager Deployment (namespace webapp) that works only in
#       toolcall-bench. It uses SANDBOX_MANAGER_IMAGE if set (build one with
#       `build-images.sh sandboxmanager`), else the running one's image.
#   deploy/deploy.sh pin
#       Once the benchmark node groups exist (../deploy/eks/README.md): keep
#       sandboxes on bench-role=sandbox nodes, the load generator on the
#       bench-role=loadgen node.
#   deploy/deploy.sh tune-upstream | untune-upstream
#       Give the agent-sandbox controller 4 API server connections, a separate
#       watch connection and 1 CPU / 1Gi requested (or restore its defaults).
#   deploy/deploy.sh down
#       Delete everything above (SandboxManager itself is untouched).
#
# Needs KUBE_CONTEXT. `up` runs the benchmark's SandboxManager with
# API_REPLICAS (default 10) replicas, no client-side Kubernetes rate limit,
# HTTP/1.1 to the API server, and 8 DB connections per replica.
set -euo pipefail
cd "$(dirname "$0")"
CONTEXT=${KUBE_CONTEXT:?set KUBE_CONTEXT to the cluster kubeconfig context}
k() { kubectl --context="$CONTEXT" "$@"; }

case "${1:-}" in
up)
  RUNTIME=${2:?runtime image} LOADGEN=${3:?loadgen image}
  sed -e "s#RUNTIME_IMAGE#$RUNTIME#" -e "s#LOADGEN_IMAGE#$LOADGEN#" k8s/benchmark.yaml | k apply -f -

  # sandboxmanager-benchmark: the running SandboxManager Deployment, renamed
  # and pointed at toolcall-bench.
  IMAGE=${SANDBOX_MANAGER_IMAGE:-$(k -n webapp get deploy sandbox-manager -o jsonpath='{.spec.template.spec.containers[0].image}')}
  k -n webapp get deploy sandbox-manager -o json | python3 -c '
import json, sys
d = json.load(sys.stdin); image = sys.argv[1]
d.pop("status", None)
d["metadata"] = {"name": "sandboxmanager-benchmark", "namespace": "webapp", "labels": {"app": "sandboxmanager-benchmark"}}
spec = d["spec"]
spec["selector"] = {"matchLabels": {"app": "sandboxmanager-benchmark"}}
spec["template"]["metadata"]["labels"] = {"app": "sandboxmanager-benchmark"}
spec["template"]["metadata"].pop("annotations", None)
c = spec["template"]["spec"]["containers"][0]
c["image"] = image
tuned = {"SANDBOX_NAMESPACE": "toolcall-bench",
         # Throughput tuning (see newKubeClients in sandboxmanager/sandbox.go):
         # no client-side rate limit, a connection per in-flight request.
         "KUBE_QPS": "-1", "KUBE_HTTP1": "true"}
c["env"] = [e for e in c["env"] if e["name"] not in tuned and e["name"] != "SANDBOX_NODE_SELECTOR"]
for e in c["env"]:
    # 8 DB connections per replica (pgxpool defaults to max(4, CPUs)). With 10
    # replicas that is 80, plus 8 for SandboxManager itself, under Postgres max_connections=100.
    if e["name"] == "DATABASE_URL" and "pool_max_conns" not in e["value"]:
        e["value"] += "&pool_max_conns=8"
c["env"] += [{"name": k, "value": v} for k, v in tuned.items()]
spec["replicas"] = int(sys.argv[2])
print(json.dumps(d))
' "$IMAGE" "${API_REPLICAS:-10}" | k apply -f -
  k -n webapp apply -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: sandboxmanager-benchmark
  namespace: webapp
spec:
  selector:
    app: sandboxmanager-benchmark
  ports:
    - port: 80
      targetPort: 8080
EOF
  k -n webapp rollout status deploy/sandboxmanager-benchmark --timeout=180s
  k -n toolcall-loadgen rollout status deploy/loadgen --timeout=300s
  ;;
pin)
  k -n webapp set env deploy/sandboxmanager-benchmark SANDBOX_NODE_SELECTOR=bench-role=sandbox
  k -n toolcall-bench patch sandboxtemplate toolcall-runtime --type=merge \
    -p '{"spec":{"podTemplate":{"spec":{"nodeSelector":{"bench-role":"sandbox"}}}}}'
  k -n toolcall-bench patch daemonset prepull-runtime --type=merge \
    -p '{"spec":{"template":{"spec":{"nodeSelector":{"bench-role":"sandbox"}}}}}'
  k -n toolcall-loadgen patch deploy loadgen --type=merge -p '{"spec":{"template":{"spec":{
    "nodeSelector":{"bench-role":"loadgen"},
    "tolerations":[{"key":"bench-role","operator":"Equal","value":"loadgen","effect":"NoSchedule"}],
    "containers":[{"name":"loadgen","resources":{"requests":{"cpu":"12","memory":"24Gi"}}}]}}}}'
  k -n webapp rollout status deploy/sandboxmanager-benchmark --timeout=180s
  k -n toolcall-loadgen rollout status deploy/loadgen --timeout=600s
  ;;
tune-upstream)
  # The agent-sandbox controller already has no client-side QPS limit and
  # 100/50 Sandbox/Claim workers. At thousands of sandboxes its single HTTP/2
  # connection (~100 concurrent requests) and lack of resource requests are
  # what is left, matching what is tuned on the SandboxManager side.
  k -n agent-sandbox-system patch deploy agent-sandbox-controller --type=json -p '[
    {"op":"replace","path":"/spec/template/spec/containers/0/args","value":["--leader-elect=true","--extensions","--api-connections=4","--separate-watch-connection"]},
    {"op":"replace","path":"/spec/template/spec/containers/0/resources","value":{"requests":{"cpu":"1","memory":"1Gi"}}}]'
  k -n agent-sandbox-system rollout status deploy/agent-sandbox-controller --timeout=300s
  ;;
untune-upstream)
  k -n agent-sandbox-system patch deploy agent-sandbox-controller --type=json -p '[
    {"op":"replace","path":"/spec/template/spec/containers/0/args","value":["--leader-elect=true","--extensions"]},
    {"op":"replace","path":"/spec/template/spec/containers/0/resources","value":{}}]'
  k -n agent-sandbox-system rollout status deploy/agent-sandbox-controller --timeout=300s
  ;;
down)
  k -n webapp delete deploy/sandboxmanager-benchmark svc/sandboxmanager-benchmark --ignore-not-found
  k delete namespace toolcall-bench toolcall-loadgen --ignore-not-found
  ;;
*)
  sed -n '2,24p' "$0"; exit 1 ;;
esac
