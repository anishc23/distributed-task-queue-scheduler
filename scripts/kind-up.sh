#!/usr/bin/env bash
# Create a local kind cluster, build and load the image, and deploy the stack.
#
# Usage:
#   scripts/kind-up.sh [--cluster NAME] [--overlay PATH] [--workers N]
#
# Requires: kind, kubectl, docker. No cloud account is involved.
set -euo pipefail

CLUSTER="${KIND_CLUSTER:-taskqueue}"
IMAGE="${TQ_IMAGE:-distributed-task-queue:dev}"
OVERLAY="deploy/kubernetes/base"
WORKERS="${WORKER_REPLICAS:-2}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cluster) CLUSTER="$2"; shift 2 ;;
    --overlay) OVERLAY="$2"; shift 2 ;;
    --workers) WORKERS="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,12p' "$0"
      exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

# Resolve the repository root so the script works from any directory.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

require() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: '$1' is required but not installed." >&2
    case "$1" in
      kind)    echo "  install: https://kind.sigs.k8s.io/docs/user/quick-start/#installation" >&2 ;;
      kubectl) echo "  install: https://kubernetes.io/docs/tasks/tools/" >&2 ;;
      docker)  echo "  install: https://docs.docker.com/get-docker/" >&2 ;;
    esac
    exit 1
  }
}
require kind
require kubectl
require docker

if ! docker info >/dev/null 2>&1; then
  echo "error: the Docker daemon is not running; start Docker and retry." >&2
  exit 1
fi

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  echo "==> kind cluster '$CLUSTER' already exists, reusing it"
else
  echo "==> creating kind cluster '$CLUSTER'"
  kind create cluster --name "$CLUSTER" --wait 120s
fi

echo "==> building image $IMAGE"
docker build -f deploy/docker/Dockerfile -t "$IMAGE" .

echo "==> loading image into the cluster"
kind load docker-image "$IMAGE" --name "$CLUSTER"

echo "==> applying manifests from $OVERLAY"
kubectl --context "kind-$CLUSTER" apply -k "$OVERLAY"

if [[ "$WORKERS" != "2" ]]; then
  echo "==> scaling workers to $WORKERS"
  kubectl --context "kind-$CLUSTER" -n taskqueue scale deployment/worker --replicas="$WORKERS"
fi

echo "==> waiting for Redis"
kubectl --context "kind-$CLUSTER" -n taskqueue rollout status statefulset/redis --timeout=180s
echo "==> waiting for the scheduler"
kubectl --context "kind-$CLUSTER" -n taskqueue rollout status deployment/scheduler --timeout=180s
echo "==> waiting for workers"
kubectl --context "kind-$CLUSTER" -n taskqueue rollout status deployment/worker --timeout=180s

cat <<MSG

Cluster '$CLUSTER' is ready.

  Submit a workload:
    kubectl --context kind-$CLUSTER -n taskqueue delete job producer --ignore-not-found
    kubectl --context kind-$CLUSTER -n taskqueue apply -f deploy/kubernetes/base/producer-job.yaml

  Scale workers:
    kubectl --context kind-$CLUSTER -n taskqueue scale deployment/worker --replicas=6

  Scheduler metrics:
    kubectl --context kind-$CLUSTER -n taskqueue port-forward svc/scheduler 9101:9101
    curl -s localhost:9101/metrics | grep '^tq_'

  Prometheus UI:
    kubectl --context kind-$CLUSTER -n taskqueue port-forward svc/prometheus 9090:9090
    open http://localhost:9090

  Inspect the dead-letter stream:
    kubectl --context kind-$CLUSTER -n taskqueue exec deploy/scheduler -- \\
      tqctl dlq --config /etc/taskqueue/config.yaml

  Tear down (this cluster only):
    scripts/kind-down.sh --cluster $CLUSTER
MSG
