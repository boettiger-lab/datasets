# Dataset Processing Guide

This guide explains how to process new geospatial datasets into cloud-optimized formats using the `cng-datasets` CLI tool and the NRP Kubernetes cluster. **Read this document before studying any source code.**

## Overview

The pipeline takes source geospatial data (Shapefiles, GeoPackages, GeoDatabase files, GeoTIFFs) and produces three output formats:

1. **GeoParquet** (`.parquet`) — optimized columnar format for analytical queries with DuckDB/Polars
2. **PMTiles** (`.pmtiles`) — optimized vector tiles for web map visualization
3. **H3 Hex Parquet** (`hex/h0={cell}/data_0.parquet`) — hexagonal grid indexed at H3 resolution 10, hive-partitioned by h0 cell, for fast spatial joins and aggregation in DuckDB

All outputs are stored on the NRP Ceph S3 system.

## Quick Reference: Processing a Vector Dataset

```bash
# 1. Generate k8s job YAML files
cng-datasets workflow \
  --dataset <dataset-name> \
  --source-url <public-url-to-source-data> \
  --bucket <s3-bucket-name> \
  --h3-resolution 10 \
  --hex-memory 32Gi \
  --max-completions 200 \
  --max-parallelism 50 \
  --parent-resolutions "9,8,0" \
  --layer <layer-name>              # only needed for multi-layer sources (GDB, GPKG)

# 2. Apply the generated k8s jobs
kubectl apply -f catalog/<dataset>/k8s/<dataset-name>/workflow-rbac.yaml
kubectl apply -f catalog/<dataset>/k8s/<dataset-name>/configmap.yaml
kubectl apply -f catalog/<dataset>/k8s/<dataset-name>/workflow.yaml
```

That's it. The workflow orchestrates everything: bucket setup → convert to GeoParquet → PMTiles + H3 hex (in parallel) → repartition hex by h0.

## The `cng-datasets` CLI

### Installation

```bash
pip install -e "."   # from the repo root
```

### Key Commands

| Command | Purpose |
|---------|---------|
| `cng-datasets workflow` | Generate a complete vector processing pipeline (k8s YAML) |
| `cng-datasets raster-workflow` | Generate a raster processing pipeline (k8s YAML) |
| `cng-datasets vector` | Run H3 hex tiling directly (used inside k8s pods) |
| `cng-datasets raster` | Run raster H3 tiling directly (used inside k8s pods) |
| `cng-datasets repartition` | Consolidate hex chunks into h0-partitioned layout |
| `cng-convert-to-parquet` | Convert vector data to optimized GeoParquet |
| `cng-datasets storage setup-bucket` | Create and configure an S3 bucket |

### `cng-datasets workflow` Options

```
--dataset DATASET          Dataset name — determines S3 output paths. Can include / for
                           hierarchical layout (e.g., "padus-4-1/fee" → bucket/padus-4-1/fee.parquet).
                           Slashes become hyphens in k8s job names.
--source-url SOURCE_URL    Public URL to source data (shapefile, GDB, etc.)
--bucket BUCKET            Target S3 bucket name (e.g., "public-padus")
--layer LAYER              Layer name within multi-layer sources (GDB/GPKG)
--h3-resolution N          Target H3 resolution (default: 10)
--parent-resolutions STR   Comma-separated parent resolutions (default: "9,8,0")
--hex-memory SIZE          Memory per hex pod (default: 8Gi, use 32Gi for large datasets)
--max-completions N        Max parallel chunk jobs (default: 200, cap at 200)
--max-parallelism N        Max concurrent pods (default: 50)
--id-column COL            ID column name (auto-detected if omitted)
--output-dir DIR           Where to write YAML files (default: auto)
--namespace NS             k8s namespace (default: biodiversity)
--intermediate-chunk-size  Rows per pass-2 batch (reduce if OOM on unnest step)
--row-group-size N         Rows per parquet row group in convert step (default: 100000)
```

## How the Pipeline Works

The `cng-datasets workflow` command generates a suite of k8s Job YAML files:

### Step 1: Setup Bucket
Creates the S3 bucket with public-read policy and CORS headers. Uses `rclone` + `aws` CLI inside the pod.

### Step 2: Convert to GeoParquet
Runs `cng-convert-to-parquet` in a pod. Reads the source data, detects CRS, reprojects to EPSG:4326, detects/creates an ID column, and writes optimized GeoParquet with bbox metadata. Uploads as `<dataset>.parquet` to the bucket.

### Step 3a: PMTiles (parallel with 3b)
Converts the GeoParquet to GeoJSONSeq via `ogr2ogr`, then to PMTiles via `tippecanoe`. Uploads as `<dataset>.pmtiles`.

### Step 3b: H3 Hex Tiling (parallel with 3a)
The most compute-intensive step. Runs as an **indexed completion** job with up to 200 parallel pods. Each pod processes one chunk of the source parquet:
- **Pass 1:** Reads source geometries, computes H3 cell arrays via `h3_polygon_wkt_to_cells`, writes intermediate arrays
- **Pass 2:** Unnests H3 arrays, adds parent resolution columns, writes to S3 as `<dataset>/hex/chunks/`

### Step 4: Repartition
Consolidates the chunked hex output into hive-partitioned format: `<dataset>/hex/h0={cell}/data_0.parquet`. Joins back attribute columns (without geometry) and cleans up chunks.

### Orchestration
A workflow pod (using `bitnami/kubectl`) runs steps 1→2→(3a∥3b)→4 sequentially, applying each job YAML from a ConfigMap volume.

## S3 Bucket Layout Convention

For a dataset named `foo` in bucket `public-foo`:

```
nrp:public-foo/
├── raw/                          # Original source data
├── foo.parquet                   # GeoParquet with geometry
├── foo.pmtiles                   # PMTiles for web maps
├── foo/
│   └── hex/
│       └── h0={cell}/data_0.parquet   # H3-indexed, hive-partitioned
├── README.md                     # Documentation
└── stac-collection.json          # STAC metadata
```

For datasets with multiple layers (e.g., PAD-US with Fee, Easement, etc.), use `/` in `--dataset` to create a hierarchical layout within the same bucket:

```
nrp:public-padus/
├── raw/
├── padus-4-1/
│   ├── fee.parquet
│   ├── fee.pmtiles
│   ├── fee/hex/h0={cell}/...
│   ├── easement.parquet
│   ├── easement.pmtiles
│   ├── easement/hex/h0={cell}/...
│   └── ...
└── ...
```

## Multi-Layer Sources (GDB, GPKG)

When a source file contains multiple layers:

1. **Inspect layers** using `ogrinfo`:
   ```bash
   ogrinfo /vsicurl/https://s3-west.nrp-nautilus.io/<bucket>/raw/<file>.gdb
   ```

2. **Classify layers** — spatial layers (Multi Polygon, Polygon, etc.) go through the full hex pipeline. Non-spatial layers (lookup/reference tables) are small enough to convert locally with `ogr2ogr -f Parquet`.

3. **Run one `cng-datasets workflow` per spatial layer**, using `--layer <LayerName>` to specify which layer.

4. **Convert non-spatial layers locally:**
   ```bash
   ogr2ogr -f Parquet output.parquet /vsicurl/<url>.gdb <LayerName>
   rclone copy output.parquet nrp:<bucket>/lookup/ -P
   ```

## Kubernetes Cluster Notes

- **Namespace:** `biodiversity` (the only namespace we have access to)
- **Secrets:** `aws` (S3 credentials) and `rclone-config` are pre-configured in the namespace
- **Priority:** All jobs use `priorityClassName: opportunistic` — they can be preempted
- **Node affinity:** Jobs target non-GPU nodes
- **Internal S3:** Pods access S3 via `http://rook-ceph-rgw-nautiluss3.rook` (no SSL, path-style)
- **External S3:** `https://s3-west.nrp-nautilus.io` (for source URLs and public access)
- **Max completions:** Never exceed 200 completions per job to avoid overwhelming etcd
- **Container image:** `ghcr.io/boettiger-lab/datasets:latest`

## Monitoring Jobs

```bash
# Watch workflow progress
kubectl get jobs -w

# Check pod status for a specific job
kubectl get pods -l job-name=<job-name>

# View logs
kubectl logs job/<job-name>

# Check for OOM-killed pods
kubectl get pods | grep OOMKilled

# Delete a failed job to retry
kubectl delete job <job-name>
```

## Troubleshooting

### OOM (Out of Memory) on Hex Jobs
The hex tiling step is memory-intensive. If pods get OOMKilled:
- **Increase `--hex-memory`** (e.g., 32Gi → 64Gi)
- **Increase `--max-completions`** (more chunks = smaller per-chunk memory)
- **Decrease `--intermediate-chunk-size`** (fewer rows in pass-2 unnesting)

### 503 SlowDown Errors from S3
The NRP Ceph cluster rate-limits. If you see `SlowDown` errors:
- Wait a few minutes and retry
- Reduce parallelism if many pods are hitting S3 simultaneously

### GDB Access via GDAL
FileGDB is a directory format. Access via `/vsicurl/` works for `ogrinfo` metadata inspection, but processing is done inside k8s pods using the internal S3 endpoint which is faster and more reliable.

### Convert Job Fails on CRS
The convert step auto-detects CRS and reprojects to EPSG:4326. If it fails, check if the source data has a valid CRS defined. Common US datasets use ESRI projected CRS (Albers Equal Area) which are handled automatically.

## Raster Datasets

For raster data (GeoTIFF, COG), use `cng-datasets raster-workflow` instead:

```bash
cng-datasets raster-workflow \
  --dataset <name> \
  --source-url <url-to-geotiff> \
  --bucket <bucket>
```

This creates a COG (Cloud Optimized GeoTIFF) and H3-indexed parquet, with fixed 122 completions (one per h0 cell).

## After Processing: Documentation

Follow `DATASET_DOCUMENTATION_WORKFLOW.md` to create:
1. A `stac/README.md` with data dictionary, usage examples, and citation
2. A `stac/stac-collection.json` with STAC metadata

Upload both to the bucket root:
```bash
rclone copy catalog/<dataset>/stac/README.md nrp:<bucket>/
rclone copy catalog/<dataset>/stac/stac-collection.json nrp:<bucket>/
```

## Reference Examples

- **CPAD** (`catalog/cpad/`) — Complete modern example with k8s YAML, README, STAC, and map viewer
- **PAD-US** (`catalog/pad-us/`) — Multi-layer GDB example with separate processing per layer
