#!/bin/bash
# Workflow for mappinginequality
set -e

echo "Starting mappinginequality workflow..."
echo "Make sure you've already applied workflow-rbac.yaml!"
echo ""

# Run convert and pmtiles in parallel
echo "Step 1: Converting to GeoParquet and PMTiles (parallel)..."
kubectl apply -f convert-job.yaml -n biodiversity
kubectl apply -f pmtiles-job.yaml -n biodiversity

echo "Waiting for conversion jobs to complete..."
kubectl wait --for=condition=complete --timeout=3600s job/mappinginequality-convert -n biodiversity
kubectl wait --for=condition=complete --timeout=3600s job/mappinginequality-pmtiles -n biodiversity

echo "Step 2: H3 hexagonal tiling (50 chunks, 20 parallel)..."
kubectl apply -f hex-job.yaml -n biodiversity

echo "Waiting for hex tiling to complete..."
kubectl wait --for=condition=complete --timeout=7200s job/mappinginequality-hex -n biodiversity

echo "Step 3: Repartitioning by h0..."
kubectl apply -f repartition-job.yaml -n biodiversity

echo "Waiting for repartition to complete..."
kubectl wait --for=condition=complete --timeout=3600s job/mappinginequality-repartition -n biodiversity

echo "✓ Workflow complete!"
echo ""
echo "Clean up jobs with:"
echo "  kubectl delete jobs mappinginequality-convert mappinginequality-pmtiles mappinginequality-hex mappinginequality-repartition -n biodiversity"
