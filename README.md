# cng-datasets

A CLI toolkit for processing large geospatial datasets into cloud-native formats on Kubernetes.

**What it does:** Takes source geospatial data (Shapefiles, GeoPackages, FileGDB, GeoTIFFs) and produces:

- **GeoParquet** — columnar format for analytical queries (DuckDB, Polars)
- **PMTiles** — vector tiles for web map visualization
- **H3 Hex Parquet** — hexagonal grid indexed at configurable H3 resolution, hive-partitioned for fast spatial joins
- **Cloud-Optimized GeoTIFF** — for raster data, optimized for HTTP range requests

**How it works:** You run a single CLI command locally. It generates Kubernetes Job YAML files that orchestrate the entire pipeline on your cluster. You never process data on your local machine — the CLI just generates the jobs.

## Installation

```bash
pip install cng-datasets
```

Or from source:

```bash
pip install -e "."
```

## Usage

### Generate a vector processing pipeline

```bash
cng-datasets workflow \
  --dataset cpad-2024 \
  --source-url https://example.com/cpad.gdb \
  --bucket public-cpad \
  --layer CPAD_SuperUnits \
  --h3-resolution 10 \
  --parent-resolutions "9,8,0" \
  --hex-memory 32Gi \
  --max-completions 200 \
  --max-parallelism 50
```

This generates YAML files for a 5-step pipeline:

1. **setup-bucket** — creates the S3 bucket with public-read policy
2. **convert** — reads source data, reprojects to EPSG:4326, writes GeoParquet
3. **pmtiles** — converts GeoParquet to PMTiles (parallel with step 4)
4. **hex** — computes H3 cell assignments in parallel pods
5. **repartition** — consolidates hex chunks into hive-partitioned layout

Apply them:

```bash
kubectl apply -f <output-dir>/workflow-rbac.yaml
kubectl apply -f <output-dir>/configmap.yaml
kubectl apply -f <output-dir>/workflow.yaml
```

The workflow orchestrator runs steps sequentially, launching pmtiles and hex in parallel.

### Generate a raster processing pipeline

```bash
cng-datasets raster-workflow \
  --dataset wetlands-cog \
  --source-url https://example.com/wetlands.tif \
  --bucket public-wetlands
```

### Multi-layer sources

For GeoDatabase or GeoPackage files with multiple layers, run one workflow per layer. Use `--layer` to select each layer and `--dataset` with `/` for hierarchical S3 paths:

```bash
# Inspect layers
ogrinfo /vsicurl/https://example.com/data.gdb

# Process each layer separately
cng-datasets workflow --dataset mydata/layer-a --layer LayerA ...
cng-datasets workflow --dataset mydata/layer-b --layer LayerB ...
```

The `/` in `--dataset` creates nested S3 paths (e.g., `mydata/layer-a.parquet`) while using hyphens for k8s resource names.

## CLI Reference

| Command | Purpose |
|---------|---------|
| `cng-datasets workflow` | Generate vector processing k8s pipeline |
| `cng-datasets raster-workflow` | Generate raster processing k8s pipeline |
| `cng-datasets storage setup-bucket` | Create and configure an S3 bucket |
| `cng-convert-to-parquet` | Convert vector data to GeoParquet |
| `cng-datasets vector` | Run H3 hex tiling (used inside k8s pods) |
| `cng-datasets raster` | Run raster H3 tiling (used inside k8s pods) |
| `cng-datasets repartition` | Consolidate hex chunks (used inside k8s pods) |

Commands marked "used inside k8s pods" are called by the generated jobs — you don't run them directly.

### `cng-datasets workflow` options

```
--dataset NAME             Dataset name for S3 paths. Use / for hierarchy (e.g., "padus/fee").
--source-url URL           Public URL to source data.
--bucket BUCKET            Target S3 bucket name.
--layer LAYER              Layer name for multi-layer sources (GDB, GPKG).
--h3-resolution N          H3 resolution for hex tiling (default: 10).
--parent-resolutions STR   Comma-separated parent resolutions (default: "9,8,0").
--hex-memory SIZE          Memory per hex pod (default: 8Gi).
--max-completions N        Number of parallel chunks, max 200 (default: auto).
--max-parallelism N        Max concurrent pods (default: 50).
--id-column COL            ID column name (auto-detected if omitted).
--output-dir DIR           Directory for generated YAML files.
--intermediate-chunk-size  Rows per unnest batch (decrease if OOM).
--row-group-size N         Rows per parquet row group (default: 100000).
```

## S3 Output Layout

```
bucket/
├── dataset.parquet              # GeoParquet
├── dataset.pmtiles              # PMTiles
├── dataset/
│   └── hex/
│       └── h0={cell}/data_0.parquet   # H3-indexed, hive-partitioned
├── README.md
└── stac-collection.json
```

## Docker

The CLI and all dependencies are packaged in a Docker image used by the k8s jobs:

```bash
docker pull ghcr.io/boettiger-lab/datasets:latest
```

## Troubleshooting

**OOM on hex jobs:** Increase `--hex-memory` (e.g., 32Gi → 64Gi), increase `--max-completions` for smaller chunks, or decrease `--intermediate-chunk-size`.

**Convert fails on curved geometries (MULTISURFACE):** Handled automatically — the converter linearizes curved geometry types via ogr2ogr before processing.

**Monitoring:**

```bash
kubectl get jobs              # Pipeline status
kubectl logs job/<name>       # Job logs
kubectl get pods | grep OOM   # Check for memory issues
```

## Development

```bash
pip install -e ".[dev]"
pytest tests/
```

## License

Apache 2.0
