# Kubernetes Workflow Guide

## Overview

The `cng-datasets` package generates complete Kubernetes workflows for processing geospatial datasets into cloud-native formats. The workflow uses a **PVC-based orchestration** approach that allows stateless execution entirely within Kubernetes.

## Architecture

### PVC-Based Orchestration

The workflow uses a Persistent Volume Claim (PVC) to store job YAML files, enabling:
- **Stateless execution**: No git repository dependencies
- **Laptop disconnect**: Workflows run entirely in cluster
- **Clean separation**: YAML files remain readable and separate (not embedded as ConfigMap strings)
- **Reusability**: Same RBAC across all datasets in a namespace

### Workflow Components

1. **Processing Jobs** (apply YAMLs individually or via orchestrator):
   - `convert-job.yaml` - Convert source format → GeoParquet
   - `pmtiles-job.yaml` - Generate PMTiles vector tiles
   - `hex-job.yaml` - H3 hexagonal tiling with automatic chunking
   - `repartition-job.yaml` - Consolidate chunks by h0 partition

2. **Orchestration Infrastructure**:
   - `workflow-rbac.yaml` - Generic ServiceAccount/Role/RoleBinding (one per namespace)
   - `workflow-pvc.yaml` - PVC for storing YAML files in cluster
   - `workflow-upload.yaml` - Helper job for uploading YAMLs to PVC
   - `workflow.yaml` - Orchestrator job that applies jobs from PVC

## Quick Start

### 1. Generate Workflow Files

```bash
cng-datasets workflow \
  --dataset my-dataset \
  --source-url https://example.com/data.gpkg \
  --bucket public-my-dataset \
  --h3-resolution 10 \
  --parent-resolutions "9,8,0" \
  --namespace biodiversity \
  --output-dir my-dataset/
```

### 2. Run Complete Workflow (Recommended)

```bash
# One-time setup per namespace
kubectl apply -f my-dataset/workflow-rbac.yaml

# Create PVC for YAML storage
kubectl apply -f my-dataset/workflow-pvc.yaml

# Upload YAML files to PVC
kubectl apply -f my-dataset/workflow-upload.yaml
kubectl wait --for=condition=ready pod -l job-name=my-dataset-upload-yamls -n biodiversity
POD=$(kubectl get pods -l job-name=my-dataset-upload-yamls -n biodiversity -o jsonpath='{.items[0].metadata.name}')
kubectl cp my-dataset/convert-job.yaml $POD:/yamls/ -n biodiversity
kubectl cp my-dataset/pmtiles-job.yaml $POD:/yamls/ -n biodiversity
kubectl cp my-dataset/hex-job.yaml $POD:/yamls/ -n biodiversity
kubectl cp my-dataset/repartition-job.yaml $POD:/yamls/ -n biodiversity

# Start orchestrator (laptop can disconnect after this)
kubectl apply -f my-dataset/workflow.yaml

# Monitor progress
kubectl logs -f job/my-dataset-workflow -n biodiversity
```

### 3. Run Jobs Individually (Alternative)

```bash
# Apply RBAC once
kubectl apply -f my-dataset/workflow-rbac.yaml

# Run each job manually
kubectl apply -f my-dataset/convert-job.yaml
kubectl apply -f my-dataset/pmtiles-job.yaml
# Wait for convert to finish before hex
kubectl wait --for=condition=complete job/my-dataset-convert -n biodiversity
kubectl apply -f my-dataset/hex-job.yaml
kubectl wait --for=condition=complete job/my-dataset-hex -n biodiversity
kubectl apply -f my-dataset/repartition-job.yaml
```

## Configurable Options

### H3 Resolutions

The workflow generates H3 hexagons at configurable resolutions:

```bash
--h3-resolution 10        # Primary resolution (default: 10)
--parent-resolutions "9,8,0"  # Parent hexes for aggregation (default: "9,8,0")
```

**Resolution Reference:**
- h12: ~3m (building-level)
- h11: ~10m (lot-level)  
- h10: ~15m (street-level) - **default**
- h9: ~50m (block-level)
- h8: ~175m (neighborhood)
- h7: ~600m (district)
- h0: continent-scale (partitioning key)

### Chunking Behavior

The hex job automatically determines optimal chunking based on feature count:
- Uses GDAL to count features from source URL
- Targets 200 completions with 50 parallelism
- Falls back to defaults if counting fails

### Namespace

```bash
--namespace biodiversity  # Default namespace (must exist)
```

All jobs and RBAC use the specified namespace.

## Processing Details

### Two-Pass H3 Approach

1. **Chunking Phase** (`hex-job.yaml`):
   - Process source data in parallel chunks
   - Write to temporary `s3://bucket/chunks/` directory
   - Each chunk contains all H3 resolutions

2. **Repartition Phase** (`repartition-job.yaml`):
   - Read all chunks
   - Repartition by h0 hexagon (continent-scale)
   - Write to final `s3://bucket/hex/` directory
   - Delete temporary `chunks/` directory

This enables efficient parallel processing while ensuring optimal query performance.

### Output Structure

```
s3://bucket/
├── dataset.parquet         # GeoParquet with all attributes
├── dataset.pmtiles         # PMTiles vector tiles
├── hex/                    # H3-indexed parquet (partitioned by h0)
│   └── h0=*/
│       └── *.parquet
└── chunks/                 # Temporary (deleted after repartition)
```

## Monitoring & Debugging

### Check Status

```bash
# List all jobs
kubectl get jobs -n biodiversity

# Check specific job
kubectl describe job my-dataset-hex -n biodiversity

# View logs
kubectl logs job/my-dataset-workflow -n biodiversity
kubectl logs job/my-dataset-hex-0-xxxxx -n biodiversity  # specific pod

# List pods for a job
kubectl get pods -n biodiversity | grep my-dataset-hex
```

### Common Issues

**PVC Upload Fails:**
```bash
# Check uploader pod status
kubectl get pod -l job-name=my-dataset-upload-yamls -n biodiversity

# Check PVC status
kubectl get pvc my-dataset-workflow-yamls -n biodiversity
```

**Orchestrator Can't Apply Jobs:**
```bash
# Check RBAC permissions
kubectl get sa,role,rolebinding -n biodiversity | grep cng-datasets-workflow

# Check orchestrator logs
kubectl logs job/my-dataset-workflow -n biodiversity
```

**Job Stuck Pending:**
```bash
# Check resource limits and node capacity
kubectl describe pod <pod-name> -n biodiversity
```

## Cleanup

### Delete Jobs and Resources

```bash
# Delete all dataset jobs and PVC
kubectl delete job my-dataset-convert my-dataset-pmtiles my-dataset-hex my-dataset-repartition my-dataset-upload-yamls my-dataset-workflow -n biodiversity --ignore-not-found=true
kubectl delete pvc my-dataset-workflow-yamls -n biodiversity --ignore-not-found=true
```

### Delete Output Data

```bash
# Using rclone
rclone purge nrp:bucket-name

# Or specific paths
rclone delete nrp:bucket-name/dataset.parquet
rclone purge nrp:bucket-name/hex/
```

## Advanced Usage

### Custom Chunking

Override automatic chunking by editing `hex-job.yaml`:
```yaml
env:
  - name: CHUNK_SIZE
    value: "100"  # Features per chunk
  - name: TOTAL_CHUNKS  
    value: "50"   # Number of chunks
```

### Custom Resource Limits

Edit individual job YAML files to adjust CPU/memory:
```yaml
resources:
  requests:
    cpu: "2"
    memory: "8Gi"
  limits:
    cpu: "4"
    memory: "16Gi"
```

### Multiple Datasets

The generic RBAC (`cng-datasets-workflow`) can be shared across datasets:
```bash
# Apply RBAC once
kubectl apply -f workflow-rbac.yaml

# Run multiple datasets
kubectl apply -f dataset1/workflow.yaml
kubectl apply -f dataset2/workflow.yaml
# Each gets its own PVC for YAML files
```

## Design Rationale

### Why PVC Instead of Git?

- **No dev repository dependency**: Don't require push access to run workflows
- **Stateless K8s execution**: Everything runs in cluster, laptop can disconnect
- **No initContainer overhead**: No git clone delays

### Why Not ConfigMaps?

- **Readability**: YAMLs remain separate files, not embedded strings
- **Size limits**: ConfigMaps have 1MB limit, can be restrictive
- **Maintainability**: Easier to inspect and modify individual jobs

### Why Generic RBAC?

- **Simplicity**: One ServiceAccount/Role/RoleBinding per namespace
- **Consistency**: Same permissions for all datasets
- **Scalability**: No per-dataset RBAC proliferation
