#!/bin/bash
# Build the image, load it into the k3d cluster, and apply the manifests.
set -euo pipefail
cd "$(dirname "$0")"

CLUSTER=${CLUSTER:-mycluster}
IMAGE=sandbox-manager:dev

# Cross-compile a static Linux binary for the Docker daemon's (Colima VM's) architecture.
GOOS=linux GOARCH="$(docker version -f '{{.Server.Arch}}')" CGO_ENABLED=0 \
  go build -trimpath -ldflags="-s -w" -o bin/sandbox-manager .

docker build -t "$IMAGE" .
k3d image import "$IMAGE" -c "$CLUSTER"

kubectl create namespace webapp --dry-run=client -o yaml | kubectl apply -f -
# Generate the DB password once; keep it on later runs so existing data stays reachable.
if ! kubectl -n webapp get secret postgres >/dev/null 2>&1; then
  kubectl -n webapp create secret generic postgres --from-literal=password="$(openssl rand -hex 16)"
fi

# Cluster-scoped objects (PriorityClass, sandboxes namespace) first, since the rest refer to them.
kubectl apply -f k8s/cluster.yaml
kubectl apply -f k8s/
kubectl -n webapp rollout status statefulset/postgres --timeout=180s
kubectl -n webapp rollout restart deployment/sandbox-manager
kubectl -n webapp rollout status deployment/sandbox-manager --timeout=180s
