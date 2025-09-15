#!/usr/bin/env bash

# A helper to (re)run the Kubernetes Job defined in carbon/job.yaml.
# - Deletes any existing job with the same name
# - Re-applies the manifest
# - Waits for pod scheduling
# - Streams logs
# - On failure/timeout, prints diagnostics and exits non-zero

set -euo pipefail

# Config (override with env vars):
JOB_NAME="${JOB_NAME:-python-job}"
JOB_FILE="${JOB_FILE:-}"  # if empty, will default relative to this script
NAMESPACE="${NAMESPACE:-}"
TIMEOUT="${TIMEOUT:-2h}"  # timeout for job completion wait 

# Derive JOB_FILE default relative to this script's directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -z "${JOB_FILE}" ]]; then
  JOB_FILE="${SCRIPT_DIR}/job.yaml"
fi

LABEL="k8s-app=${JOB_NAME}"

# kubectl wrapper with optional namespace
NS_ARGS=()
if [[ -n "${NAMESPACE}" ]]; then
  NS_ARGS=("-n" "${NAMESPACE}")
fi
k() { kubectl "${NS_ARGS[@]}" "$@"; }

echo "[run-job] Deleting any existing job: ${JOB_NAME}"
k delete "job/${JOB_NAME}" --ignore-not-found=true >/dev/null 2>&1 || true

echo "[run-job] Applying manifest: ${JOB_FILE}"
k apply -f "${JOB_FILE}"

# Get the pod name created by the job, waiting until it exists
echo "[run-job] Waiting for pod to be created (label: ${LABEL})"
POD_NAME=""
for i in {1..120}; do # up to ~2 minutes
  set +e
  POD_NAME=$(k get pods -l "${LABEL}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  RC=$?
  set -e
  if [[ ${RC} -eq 0 && -n "${POD_NAME}" ]]; then
    break
  fi
  sleep 1
done

if [[ -z "${POD_NAME}" ]]; then
  echo "[run-job] Timed out waiting for pod. Current pods:"
  k get pods -l "${LABEL}" -o wide || true
  exit 1
fi

echo "[run-job] Pod: ${POD_NAME}"

# Function to print helpful diagnostics
diagnose() {
  echo "\n[run-job] Diagnostics for job/${JOB_NAME} and pod/${POD_NAME}:"
  k get job "${JOB_NAME}" -o wide || true
  echo
  k describe job "${JOB_NAME}" || true
  echo
  k get pods -l "${LABEL}" -o wide || true
  echo
  k describe pod "${POD_NAME}" || true
  echo
  echo "[run-job] Tail of main container logs (python-job):"
  k logs "pod/${POD_NAME}" -c python-job --tail=200 || true
  echo
  echo "[run-job] Tail of init container logs (git-clone):"
  k logs "pod/${POD_NAME}" -c git-clone --tail=200 || true
}

# Ensure we show diagnostics on exit if something goes wrong
trap 'STATUS=$?; if [[ ${STATUS} -ne 0 ]]; then diagnose; fi' EXIT

# If the pod is stuck in init, surface that quickly
PHASE=$(k get pod "${POD_NAME}" -o jsonpath='{.status.phase}' || true)
echo "[run-job] Pod phase: ${PHASE}"

# Stream logs from the main container; retry briefly while container is not ready yet
echo "[run-job] Streaming logs from container: python-job (Ctrl-C to stop viewing)"
set +e
for i in {1..60}; do
  k logs "pod/${POD_NAME}" -c python-job --tail=10 >/dev/null 2>&1 && break
  # If the main container hasn't started, try to show init container logs once in a while
  if (( i % 10 == 0 )); then
    echo "[run-job] Waiting for main container to start; showing recent init logs (git-clone):"
    k logs "pod/${POD_NAME}" -c git-clone --tail=50 || true
  fi
  sleep 2
done

# Follow logs until completion/termination of the container
k logs -f "pod/${POD_NAME}" -c python-job --timestamps=true
LOGS_RC=$?
set -e

# Wait for Job completion (or detect failure/timeout)
echo "[run-job] Waiting for job completion (timeout: ${TIMEOUT})"
if k wait --for=condition=complete "job/${JOB_NAME}" --timeout="${TIMEOUT}"; then
  echo "[run-job] Job completed successfully."
  exit 0
fi

echo "[run-job] Job did not complete successfully (logs rc=${LOGS_RC}). Checking for failure condition..."
set +e
k wait --for=condition=failed "job/${JOB_NAME}" --timeout=1s >/dev/null 2>&1
FAILED_RC=$?
set -e

if [[ ${FAILED_RC} -eq 0 ]]; then
  echo "[run-job] Job failed. See diagnostics below."
  exit 1
else
  echo "[run-job] Job neither completed nor marked failed within timeout. See diagnostics below."
  exit 1
fi
