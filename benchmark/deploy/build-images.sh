#!/bin/bash
# Build and push the benchmark's images to ECR (linux/amd64) without Docker.
# Python packages are downloaded as prebuilt manylinux wheels and appended as
# one layer with crane; Go programs are static binaries appended the same way.
# (A local `docker build` with the legacy builder pushes layers EKS's containerd
# rejects.)
#
#   toolcall-runtime-python   ../runtimes/python/server.py + numpy, pandas, scipy,
#                             requests on python:3.12-slim (~440 MB, "medium")
#   toolcall-runtime-minimal  ../runtimes/minimal as a static binary on busybox (~10 MB)
#   toolcall-loadgen          ../harness/toolcall_bench.py + both SDKs
#   sandbox-manager:benchmark-<tag>  ../../sandboxmanager, for the benchmark's copy of the API
#
# Usage: deploy/build-images.sh [python|minimal|loadgen|sandboxmanager|all]
# Needs AWS_REGION, AWS credentials for the account's ECR, Go and Python 3.
# The ECR repositories must exist. Prints the pushed image references.
set -euo pipefail
cd "$(dirname "$0")"

WHAT=${1:-all}
REGION=${AWS_REGION:?set AWS_REGION}
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
REG=${REGISTRY:-$ACCOUNT.dkr.ecr.$REGION.amazonaws.com}
TAG=$(date +%Y%m%d%H%M%S)
CRANE="go run github.com/google/go-containerregistry/cmd/crane@v0.20.2"
OUT=$(mktemp -d)
trap 'rm -rf "$OUT"' EXIT

aws ecr get-login-password --region "$REGION" | $CRANE auth login "$REG" -u AWS --password-stdin >/dev/null

# site_packages DIR PKG...: install wheels for CPython 3.12 on linux/amd64 into DIR.
site_packages() {
  local dir=$1; shift
  python3 -m pip install --quiet --disable-pip-version-check \
    --platform manylinux2014_x86_64 --platform manylinux_2_17_x86_64 --platform manylinux_2_28_x86_64 \
    --implementation cp --python-version 3.12 --only-binary=:all: \
    --target "$dir" "$@"
}

# python_image NAME ROOTDIR CMD...: ROOTDIR's contents as one layer on python:3.12-slim.
python_image() {
  local name=$1 root=$2; shift 2
  tar -C "$root" --uid 0 --gid 0 -cf "$OUT/$name.tar" .
  $CRANE append --platform linux/amd64 -b python:3.12-slim -f "$OUT/$name.tar" -t "$REG/$name:$TAG-base" >/dev/null
  local cmd; cmd=$(printf '%s,' "$@"); cmd=${cmd%,}
  $CRANE mutate "$REG/$name:$TAG-base" --env PYTHONPATH=/opt/bench/site --env PYTHONUNBUFFERED=1 \
    --workdir /opt/bench --entrypoint "$cmd" -t "$REG/$name:$TAG" >/dev/null
  echo "$REG/$name:$TAG"
}

# go_image NAME:TAG BASE DIR: the Go program in DIR (built in its own module
# directory) as a static binary /app on BASE.
go_image() {
  local ref=$1 base=$2 dir=$3 bin
  bin=$OUT/$(basename "$dir")
  mkdir -p "$bin"
  (cd "$dir" && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o "$bin/app" .)
  tar -C "$bin" --uid 0 --gid 0 -cf "$bin.tar" app
  $CRANE append --platform linux/amd64 -b "$base" -f "$bin.tar" -t "$REG/$ref-base" >/dev/null
  $CRANE mutate "$REG/$ref-base" --entrypoint /app -t "$REG/$ref" >/dev/null
  echo "$REG/$ref"
}

if [ "$WHAT" = python ] || [ "$WHAT" = all ]; then
  R=$OUT/python/opt/bench
  mkdir -p "$R"
  site_packages "$R/site" fastapi==0.115.6 uvicorn==0.34.0 numpy==2.2.1 pandas==2.2.3 scipy==1.15.0 requests==2.32.3
  cp ../runtimes/python/server.py "$R/"
  python_image toolcall-runtime-python "$OUT/python" python -m uvicorn server:app --host 0.0.0.0 --port 8888 --log-level warning
fi
if [ "$WHAT" = minimal ] || [ "$WHAT" = all ]; then
  go_image "toolcall-runtime-minimal:$TAG" busybox:1.36 ../runtimes/minimal
fi
if [ "$WHAT" = sandboxmanager ] || [ "$WHAT" = all ]; then
  go_image "sandbox-manager:benchmark-$TAG" gcr.io/distroless/static-debian12:nonroot ../../sandboxmanager
fi
if [ "$WHAT" = loadgen ] || [ "$WHAT" = all ]; then
  L=$OUT/loadgen/opt/bench
  mkdir -p "$L"
  site_packages "$L/site" 'k8s-agent-sandbox[async]==1.0.7' 'httpx>=0.27,<1' ../../sandboxmanager/sdk/python
  cp ../harness/toolcall_bench.py "$L/"
  python_image toolcall-loadgen "$OUT/loadgen" sleep infinity
fi
