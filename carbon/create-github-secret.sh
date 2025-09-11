#!/usr/bin/env bash
# Create or update the Kubernetes secret `github-token` with the env var GITHUB_TOKEN
# Usage:
#   export GITHUB_TOKEN=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
#   ./carbon/create-github-secret.sh [namespace]
# If namespace is omitted, the current kubectl context namespace will be used.
set -euo pipefail

NAMESPACE=${1:-}
if [[ -n "${NAMESPACE}" ]]; then
  NS_ARGS=("-n" "${NAMESPACE}")
else
  NS_ARGS=()
fi

if [[ -z "${GITHUB_TOKEN:-}" ]]; then
  echo "GITHUB_TOKEN environment variable is not set" >&2
  exit 1
fi

# Try to create, if exists then replace
if kubectl get secret github-token "${NS_ARGS[@]}" >/dev/null 2>&1; then
  echo "Updating existing secret 'github-token'${NAMESPACE:+ in namespace ${NAMESPACE}}"
  kubectl delete secret github-token "${NS_ARGS[@]}" >/dev/null 2>&1 || true
fi

kubectl create secret generic github-token \
  "${NS_ARGS[@]}" \
  --from-literal=GITHUB_TOKEN="${GITHUB_TOKEN}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "Secret 'github-token' is configured${NAMESPACE:+ in namespace ${NAMESPACE}}."
