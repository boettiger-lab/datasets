#!/bin/bash
# Capture current job state before Kubernetes cleans it up
# Safe to run while job is running

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_DIR="job_snapshots"
mkdir -p "$OUTPUT_DIR"

echo "Capturing job state at $TIMESTAMP..."

# Capture job manifest and status
kubectl get jobs -n biodiversity -l app=wetlands -o json > "$OUTPUT_DIR/jobs_${TIMESTAMP}.json"

# Capture pod list and status
kubectl get pods -n biodiversity -l app=wetlands -o json > "$OUTPUT_DIR/pods_${TIMESTAMP}.json"

# Capture events
kubectl get events -n biodiversity --sort-by='.lastTimestamp' -o json > "$OUTPUT_DIR/events_${TIMESTAMP}.json"

# Extract summary info
echo "=== Job Summary ===" > "$OUTPUT_DIR/summary_${TIMESTAMP}.txt"
kubectl get jobs -n biodiversity -l app=wetlands -o json | jq -r '.items[] | "Job: \(.metadata.name)\nTier: \(.metadata.labels.tier)\nCompleted: \(.status.succeeded // 0)/\(.spec.completions)\nFailed: \(.status.failed // 0)\nActive: \(.status.active // 0)\nFailed Indexes: \(.status.failedIndexes // "none")\n"' >> "$OUTPUT_DIR/summary_${TIMESTAMP}.txt"

echo "Snapshot saved to $OUTPUT_DIR/"
ls -lh "$OUTPUT_DIR"/*${TIMESTAMP}*
