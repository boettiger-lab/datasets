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

## Quick Start

### 1. Generate All Workflow Files

```bash
cng-datasets workflow \
  --dataset mappinginequality \
  --source-url https://dsl.richmond.edu/panorama/redlining/static/mappinginequality.gpkg \
  --bucket public-mappinginequality
  # --namespace defaults to "biodiversity"
```

This generates all required Kubernetes job configurations:
- `convert-job.yaml` - GPKG → GeoParquet conversion
- `pmtiles-job.yaml` - PMTiles vector tile generation
- `hex-job.yaml` - H3 hexagonal tiling (50 chunks, 20 parallel workers)
- `repartition-job.yaml` - Consolidate chunks by h0 partition
- `workflow.yaml` - K8s Job orchestrator (runs in cluster)
- `workflow-rbac.yaml` - Kubernetes RBAC permissions

### 2. Run the Workflow

```bash
# First-time setup: create RBAC permissions
kubectl apply -f workflow-rbac.yaml

# Create ConfigMap with job specifications
kubectl create configmap mappinginequality-jobs --from-file=. -n biodiversity

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
1. Converts GPKG to GeoParquet and PMTiles (parallel)
2. Processes into H3 hexagons at resolution 10 with parent resolutions
3. Repartitions by h0 for efficient querying
4. Runs entirely in K8s (laptop can disconnect)

## Data Structure

The final hex data contains:
- All original redlining polygon attributes
- `h10`: H3 hexagon ID at resolution 10 (UBIGINT)
- `h9`: Parent hex at resolution 9 (UBIGINT)
- `h8`: Parent hex at resolution 8 (UBIGINT)
- `h0`: Parent hex at resolution 0 (UBIGINT)

The h0 partitioning enables efficient spatial queries by limiting reads to relevant geographic regions.

## Output Locations

- `s3://public-redlining/mappinginequality.parquet` - GeoParquet
- `s3://public-redlining/mappinginequality.pmtiles` - PMTiles
- `s3://public-redlining/hex/` - H3-indexed parquet (partitioned by h0)