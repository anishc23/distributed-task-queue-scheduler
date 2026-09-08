#!/usr/bin/env bash
# Delete only this project's kind cluster. Other kind clusters are untouched.
#
# Usage:
#   scripts/kind-down.sh [--cluster NAME]
set -euo pipefail

CLUSTER="${KIND_CLUSTER:-taskqueue}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cluster) CLUSTER="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,7p' "$0"
      exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

command -v kind >/dev/null 2>&1 || {
  echo "error: 'kind' is required but not installed." >&2
  exit 1
}

if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  echo "kind cluster '$CLUSTER' does not exist; nothing to do."
  exit 0
fi

echo "==> deleting kind cluster '$CLUSTER'"
kind delete cluster --name "$CLUSTER"
echo "done. Other kind clusters were not touched."
