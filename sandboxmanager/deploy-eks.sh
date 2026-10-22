#!/bin/bash
# Build the API and the node agent for EKS (amd64), push them to ECR, and
# apply k8s/ to the EKS cluster. One-time AWS setup (bucket, IAM role, Pod
# Identity associations, ECR repos) is described in CLAUDE.md.
#
# Images are assembled with crane (binary layer on distroless) instead of
# `docker build`: the local Docker's legacy builder pushes layers EKS's
# containerd rejects ("wrong diff id").
set -euo pipefail
cd "$(dirname "$0")"

CONTEXT=${KUBE_CONTEXT:?set KUBE_CONTEXT to your EKS kubeconfig context}
REGION=${AWS_REGION:?set AWS_REGION}
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET=${SNAPSHOT_BUCKET:?set SNAPSHOT_BUCKET to the workspace-snapshot S3 bucket}
REG=$ACCOUNT.dkr.ecr.$REGION.amazonaws.com
TAG=$(date +%Y%m%d%H%M%S)
CRANE="go run github.com/google/go-containerregistry/cmd/crane@v0.20.2"
OUT=$(mktemp -d)
trap 'rm -rf "$OUT"' EXIT
k() { kubectl --context="$CONTEXT" "$@"; }

aws ecr get-login-password --region "$REGION" | $CRANE auth login "$REG" -u AWS --password-stdin

# build NAME PACKAGE BASE: a static linux/amd64 binary as one layer on BASE.
build() {
  mkdir -p "$OUT/$1"
  GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o "$OUT/$1/$1" "$2"
  tar -C "$OUT/$1" --uid 0 --gid 0 -cf "$OUT/$1.tar" "$1"
  $CRANE append --platform linux/amd64 -b "$3" -f "$OUT/$1.tar" -t "$REG/$1:$TAG-base" >/dev/null
  $CRANE mutate "$REG/$1:$TAG-base" --entrypoint "/$1" -t "$REG/$1:$TAG" >/dev/null
}
build sandbox-manager . gcr.io/distroless/static-debian12:nonroot
build sandbox-agent ./agent gcr.io/distroless/static-debian12 # root: reads any sandbox user's files

mkdir -p "$OUT/k8s"
cp k8s/*.yaml "$OUT/k8s/"
sed -i.bak -e "s#image: sandbox-manager:dev#image: $REG/sandbox-manager:$TAG#" \
  -e "s#image: sandbox-agent:dev#image: $REG/sandbox-agent:$TAG#" \
  -e "s#imagePullPolicy: Never .*#imagePullPolicy: IfNotPresent#" "$OUT/k8s/sandbox-manager.yaml" "$OUT/k8s/agent.yaml"
rm "$OUT"/k8s/*.bak

k create namespace webapp --dry-run=client -o yaml | k apply -f -
# Generated once; kept on later runs.
k -n webapp get secret postgres >/dev/null 2>&1 ||
  k -n webapp create secret generic postgres --from-literal=password="$(openssl rand -hex 16)"
k -n webapp get secret sandbox-agent >/dev/null 2>&1 ||
  k -n webapp create secret generic sandbox-agent --from-literal=token="$(openssl rand -hex 32)"
k -n webapp create configmap snapshots --from-literal=bucket="$BUCKET" --from-literal=region="$REGION" \
  --dry-run=client -o yaml | k apply -f -

k apply -f "$OUT/k8s/cluster.yaml"
k apply -f "$OUT/k8s/"
k -n webapp rollout status statefulset/postgres --timeout=180s
k -n webapp rollout status daemonset/sandbox-agent --timeout=180s
k -n webapp rollout status deployment/sandbox-manager --timeout=180s
echo "deployed $TAG"
