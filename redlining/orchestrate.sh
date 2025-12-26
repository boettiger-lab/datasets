#!/bin/bash
# Orchestrate Redlining data processing workflow
# Usage: ./orchestrate.sh [all|parquet|pmtiles|hex|repartition|cleanup]

set -e

NAMESPACE="biodiversity"

# Function to wait for a job to complete
wait_for_job() {
    local job_name=$1
    local timeout=${2:-3600}  # Default 1 hour timeout
    local elapsed=0
    local interval=30
    
    echo "Waiting for job $job_name to complete..."
    
    while [ $elapsed -lt $timeout ]; do
        status=$(kubectl get job "$job_name" -n "$NAMESPACE" -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null || echo "")
        failed=$(kubectl get job "$job_name" -n "$NAMESPACE" -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null || echo "")
        
        if [ "$status" = "True" ]; then
            echo "✓ Job $job_name completed successfully"
            return 0
        elif [ "$failed" = "True" ]; then
            echo "✗ Job $job_name failed"
            kubectl logs "job/$job_name" -n "$NAMESPACE" --tail=50
            return 1
        fi
        
        sleep $interval
        elapsed=$((elapsed + interval))
        echo "  Still waiting... (${elapsed}s elapsed)"
    done
    
    echo "✗ Timeout waiting for job $job_name"
    return 1
}

# Function to delete job if it exists
delete_job_if_exists() {
    local job_name=$1
    if kubectl get job "$job_name" -n "$NAMESPACE" &>/dev/null; then
        echo "Deleting existing job $job_name..."
        kubectl delete job "$job_name" -n "$NAMESPACE"
    fi
}

# Function to run parquet generation
run_parquet() {
    echo "=== Step 1: Generate Parquet from GPKG ==="
    delete_job_if_exists "redlining-convert"
    kubectl apply -f redlining/convert-job.yaml -n "$NAMESPACE"
    wait_for_job "redlining-convert" 3600  # 1 hour timeout
}

# Function to run pmtiles generation
run_pmtiles() {
    echo "=== Step 2: Generate PMTiles from GPKG ==="
    delete_job_if_exists "redlining-pmtiles"
    kubectl apply -f redlining/pmtiles-job.yaml -n "$NAMESPACE"
    wait_for_job "redlining-pmtiles" 3600  # 1 hour timeout
}

# Function to run hex generation
run_hex() {
    echo "=== Step 3: Generate H3 Hexes at resolution 10 ==="
    delete_job_if_exists "redlining-hex"
    kubectl apply -f redlining/hex-job.yaml -n "$NAMESPACE"
    wait_for_job "redlining-hex" 7200  # 2 hour timeout for hex processing
}

# Function to run repartitioning
run_repartition() {
    echo "=== Step 4: Repartition Data by H0 ==="
    delete_job_if_exists "redlining-repartition"
    kubectl apply -f redlining/repartition-job.yaml -n "$NAMESPACE"
    
    if wait_for_job "redlining-repartition" 3600; then
        echo "Repartitioning successful, chunks cleaned up automatically"
    else
        echo "Repartitioning failed, keeping chunks directory for debugging"
        return 1
    fi
}

# Function to cleanup chunks directory
cleanup_chunks() {
    echo "=== Cleanup: Removing chunks directory from S3 ==="
    
    # Use mc (minio client) or aws cli to remove chunks
    if command -v mc &> /dev/null; then
        mc rm --recursive --force s3/public-redlining/chunks/ 2>/dev/null || true
        echo "✓ Chunks directory removed (mc)"
    elif command -v aws &> /dev/null; then
        aws s3 rm s3://public-redlining/chunks/ --recursive --endpoint-url https://s3-west.nrp-nautilus.io 2>/dev/null || true
        echo "✓ Chunks directory removed (aws)"
    else
        echo "⚠ Neither mc nor aws cli found, skipping cleanup"
    fi
}

# Main orchestration logic
case "${1:-all}" in
    all)
        echo "Running complete workflow..."
        
        # Run parquet and pmtiles in parallel
        echo "Starting parquet and pmtiles generation in parallel..."
        run_parquet &
        parquet_pid=$!
        run_pmtiles &
        pmtiles_pid=$!
        
        # Wait for both to complete
        wait $parquet_pid
        parquet_result=$?
        wait $pmtiles_pid
        pmtiles_result=$?
        
        if [ $parquet_result -ne 0 ]; then
            echo "✗ Parquet generation failed"
            exit 1
        fi
        
        if [ $pmtiles_result -ne 0 ]; then
            echo "⚠ PMTiles generation failed (continuing anyway)"
        fi
        
        # Run hex processing (depends on parquet)
        run_hex || exit 1
        
        # Run repartitioning (depends on hex)
        run_repartition || exit 1
        
        echo "✓ Complete workflow finished successfully!"
        ;;
    parquet)
        run_parquet
        ;;
    pmtiles)
        run_pmtiles
        ;;
    hex)
        run_hex
        ;;
    repartition)
        run_repartition
        ;;
    cleanup)
        cleanup_chunks
        ;;
    *)
        echo "Usage: $0 [all|parquet|pmtiles|hex|repartition|cleanup]"
        exit 1
        ;;
esac
