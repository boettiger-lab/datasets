# Redlining / Mapping Inequality

## Source Data

Original geopackage: https://dsl.richmond.edu/panorama/redlining/static/mappinginequality.gpkg

Historical redlining maps showing discriminatory mortgage lending practices in US cities from the 1930s-1940s.

## Overview

This workflow processes historical redlining data into cloud-native formats:
1. **GeoParquet** - Optimized columnar format for analysis
2. **PMTiles** - Vector tiles for web mapping
3. **H3-indexed Parquet** - Hexagonal tiling at resolution 10 with parent hexes (h9, h8, h0)

All processing uses the `cng_datasets` Python package for standardized, reusable data processing. Jobs use the `ghcr.io/rocker-org/ml-spatial` image with all dependencies pre-installed, and clone the repository via an initContainer for fast execution.

### H3 Resolution Configuration

The workflow generates H3 hexagons at **resolution 10** (h10) and includes parent hexagons at resolutions **h9, h8, and h0**. These resolutions can be configured during workflow generation:

```bash
cng-datasets workflow \
  --dataset mappinginequality \
  --source-url https://dsl.richmond.edu/panorama/redlining/static/mappinginequality.gpkg \
  --bucket public-mappinginequality \
  --h3-resolution 10 \              # Target resolution (default: 10)
  --parent-resolutions "9,8,0"      # Parent resolutions (default: "9,8,0")
```

**Resolution Guide:**
- **h10** (~15m hexagons) - Fine-grained analysis, optimal for urban features
- **h9** (~50m hexagons) - Intermediate scale
- **h8** (~175m hexagons) - Neighborhood scale  
- **h0** (continent scale) - Used for efficient partitioning

Other datasets may use different resolutions based on their spatial scale (e.g., h8 for regional data, h12 for detailed local data).

### Processing Workflow

The H3 hexagonal tiling follows a **two-pass approach** (matching wdpa/):
1. **Chunking** - Process data in parallel chunks, writing to `s3://public-mappinginequality/chunks/`
2. **Repartitioning** - Consolidate all chunks into h0-partitioned format in `s3://public-mappinginequality/hex/`, then delete temporary chunks/

This approach enables efficient parallel processing of large datasets while ensuring optimal query performance with h0 partitioning.

## Quick Start

### 1. Generate All Workflow Files

```bash
cng-datasets workflow \
  --dataset mappinginequality \
  --source-url https://dsl.richmond.edu/panorama/redlining/static/mappinginequality.gpkg \
  --bucket public-mappinginequality \
  --h3-resolution 10 \
  --parent-resolutions "9,8,0"
  # --namespace defaults to "biodiversity"
```

This generates all required Kubernetes job configurations:
- `convert-job.yaml` - GPKG → GeoParquet conversion
- `pmtiles-job.yaml` - PMTiles vector tile generation
- `hex-job.yaml` - H3 hexagonal tiling (automatic chunking based on dataset size)
- `repartition-job.yaml` - Consolidate chunks by h0 partition
- `workflow.yaml` - K8s Job orchestrator (runs in cluster)
- `workflow-rbac.yaml` - Kubernetes RBAC permissions

### 2. Run the Workflow

```bash
# First-time setup: create RBAC permissions
kubectl apply -f workflow-rbac.yaml

# Run the complete automated workflow (K8s orchestrator)
kubectl apply -f workflow.yaml -n biodiversity

# Monitor progress
kubectl logs -f job/mappinginequality-workflow -n biodiversity

# Or run jobs individually:
kubectl apply -f convert-job.yaml -n biodiversity
kubectl apply -f pmtiles-job.yaml -n biodiversity
kubectl apply -f hex-job.yaml -n biodiversity
kubectl apply -f repartition-job.yaml -n biodiversity
```

The workflow automatically:
1. Creates bucket and sets public read access with CORS
2. Converts GPKG to GeoParquet and PMTiles (parallel)
3. Processes into H3 hexagons in chunks (50 chunks, 20 parallel workers → `chunks/`)
4. Repartitions chunks by h0 for efficient querying (`chunks/` → `hex/`)
5. Cleans up temporary chunks directory
6. Runs entirely in K8s (laptop can disconnect)

## Data Structure

The final hex data contains:
- All original redlining polygon attributes
- `h10`: H3 hexagon ID at resolution 10 (UBIGINT)
- `h9`: Parent hex at resolution 9 (UBIGINT)
- `h8`: Parent hex at resolution 8 (UBIGINT)
- `h0`: Parent hex at resolution 0 (UBIGINT)

The h0 partitioning enables efficient spatial queries by limiting reads to relevant geographic regions.

## Output Locations

- `s3://public-mappinginequality/mappinginequality.parquet` - GeoParquet
- `s3://public-mappinginequality/mappinginequality.pmtiles` - PMTiles
- `s3://public-mappinginequality/chunks/` - Temporary H3 processing chunks (deleted after repartitioning)
- `s3://public-mappinginequality/hex/` - H3-indexed parquet (partitioned by h0)

## Cleanup and Reset

### Delete All Jobs
```bash
# Delete all redlining jobs
kubectl delete job mappinginequality-convert mappinginequality-pmtiles mappinginequality-hex mappinginequality-repartition mappinginequality-workflow -n biodiversity --ignore-not-found=true
```

### Delete Data from Bucket
```bash
# Delete all generated data (using rclone)
rclone purge nrp:public-mappinginequality

# Or delete specific outputs:
rclone delete nrp:public-mappinginequality/mappinginequality.parquet
rclone delete nrp:public-mappinginequality/mappinginequality.pmtiles
rclone purge nrp:public-mappinginequality/hex/
rclone purge nrp:public-mappinginequality/chunks/  # if workflow was interrupted
```

### Complete Reset
To completely reset and rerun from scratch:
```bash
# 1. Delete all jobs
kubectl delete job mappinginequality-convert mappinginequality-pmtiles mappinginequality-hex mappinginequality-repartition mappinginequality-workflow -n biodiversity --ignore-not-found=true

# 2. Delete all data
rclone purge nrp:public-mappinginequality

# 3. Rerun workflow (bucket will be recreated automatically)
kubectl apply -f workflow.yaml -n biodiversity
```

### Check Job Status
```bash
# List all jobs
kubectl get jobs -n biodiversity | grep mappinginequality

# Check specific job status
kubectl describe job mappinginequality-hex -n biodiversity

# View logs
kubectl logs job/mappinginequality-convert -n biodiversity
kubectl logs job/mappinginequality-hex-0-xxxxx -n biodiversity  # specific pod

# List all pods for a job
kubectl get pods -n biodiversity | grep mappinginequality-hex
```