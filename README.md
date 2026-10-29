# cng-datasets

[![DOI](https://zenodo.org/badge/921944685.svg)](https://zenodo.org/badge/latestdoi/921944685)

Tools for converting large geospatial datasets into cloud-native formats indexed on the H3 hexagonal grid.

**What it produces:** Takes source geospatial data (Shapefiles, GeoPackages, FileGDB, CSV points, GeoTIFFs, NetCDF and Zarr cubes) and produces:

- **GeoParquet** — columnar format for analytical queries (DuckDB, Polars)
- **PMTiles** — vector tiles for web map visualization
- **H3 Hex Parquet** — hexagonal grid indexed at configurable H3 resolution, hive-partitioned for fast spatial joins
- **Cloud-Optimized GeoTIFF** — for raster data, optimized for HTTP range requests

**Two ways in:**

1. **The Python API** converts vector, raster and multidimensional data to H3 hex Parquet directly. It suits a small import, or a large single machine, with no orchestration at all.
2. **The CLI** generates Kubernetes jobs that run the same Python code at scale on a cluster. You run one command locally, it writes the job YAML, and the cluster does the processing.

Full documentation, including the API reference, is at **https://boettiger-lab.github.io/datasets/**.

## Installation

```bash
pip install cng-datasets
```

Or from source:

```bash
pip install -e "."
```

## Python API

### Vector to H3

```python
from cng_datasets.vector import H3VectorProcessor, repartition_by_h0
from cng_datasets.vector.convert_to_parquet import convert_to_parquet

# Source (any GDAL-readable vector) to GeoParquet in EPSG:4326, with a _cng_fid id column
convert_to_parquet("https://example.com/parks.gpkg", "parks.parquet")

# Polyfill each feature at res 10, with parents 9, 8 and the h0 partition key
H3VectorProcessor(
    input_url="parks.parquet",
    output_url="parks/chunks",
    h3_resolution=10,
    parent_resolutions=[9, 8, 0],
).process_all_chunks()

# Consolidate into parks/hex/h0=<cell>/data_0.parquet, joining the attributes back
repartition_by_h0("parks/chunks", "parks/hex", source_parquet="parks.parquet")
```

### Raster to H3

```python
from cng_datasets.raster import RasterProcessor

processor = RasterProcessor(
    input_path="https://example.com/wetlands.tif",
    output_cog_path="wetlands-cog.tif",
    output_parquet_path="wetlands/hex",
    h3_resolution=8,
    parent_resolutions=[0],
    hex_resampling="mean",   # area-weighted; also sum, mode, fractions, max, min
)
processor.create_cog()
processor.process_all_h0_regions()   # writes wetlands/hex/h0=<cell>/data_0.parquet
```

Inputs and outputs may be local paths or `s3://` URLs. See the [API reference](https://boettiger-lab.github.io/datasets/api_reference.html) for every option, and `cng_datasets.mdim` for NetCDF and Zarr cubes.

## Kubernetes Usage


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

This generates YAML files for a 6-step pipeline:

1. **setup-bucket** — creates the S3 bucket with public-read policy
2. **convert** — reads source data, reprojects to EPSG:4326, writes GeoParquet in spatial (Hilbert) order
3. **plan** — cuts the GeoParquet into hex chunks by estimated H3 cells (`--cells-per-chunk`)
4. **pmtiles** — converts GeoParquet to PMTiles (parallel with step 5)
5. **hex** — computes H3 cell assignments in parallel pods, sized to the plan
6. **repartition** — consolidates hex chunks into hive-partitioned layout

Jobs run in the `geo-workflows` namespace unless `--namespace` or `--profile` says otherwise.

**Multiple sources:** Add `--source-url` multiple times to merge datasets:

```bash
cng-datasets workflow \
  --dataset merged-regions \
  --source-url https://example.com/region1.shp \
  --source-url https://example.com/region2.shp \
  --source-url https://example.com/region3.shp \
  --bucket my-bucket
```

Apply them:

```bash
kubectl apply -f <output-dir>/workflow-rbac.yaml   # once per namespace
kubectl apply -f <output-dir>/configmap.yaml -f <output-dir>/workflow.yaml
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
| `cng-datasets raster-workflow` | Generate raster processing k8s pipeline (`--years` for an annual series) |
| `cng-datasets mdim-workflow` | Generate a NetCDF/Zarr cube processing k8s pipeline |
| `cng-datasets sync-job` | Generate a job that syncs a bucket to another provider with rclone |
| `cng-datasets storage setup-bucket` | Create and configure an S3 bucket |
| `cng-convert-to-parquet` | Convert vector data to GeoParquet |
| `cng-datasets vector-plan` | Plan hex chunks by estimated H3 cells (used inside k8s pods) |
| `cng-datasets vector` | Run H3 hex tiling (used inside k8s pods) |
| `cng-datasets mdim` | Run cube H3 tiling (used inside k8s pods) |
| `cng-datasets raster` | Run raster H3 tiling (used inside k8s pods) |
| `cng-datasets repartition` | Consolidate hex chunks (used inside k8s pods) |
| `cng-datasets merge-chunks` | Consolidate sub-h0 raster hex chunks (used inside k8s pods) |
| `cng-datasets gapfill` | Re-run the sub-h0 chunks that never completed (Armada backend) |

Commands marked "used inside k8s pods" are called by the generated jobs — you don't run them directly.

### `cng-datasets workflow` options

```
--dataset NAME             Dataset name for S3 paths. Use / for hierarchy (e.g., "padus/fee").
--source-url URL           Public URL to source data.
--bucket BUCKET            Target S3 bucket name.
--layer LAYER              Layer name for multi-layer sources (GDB, GPKG).
--h3-resolution N          H3 resolution for hex tiling (default: 10, or 8 for lines).
--resolution-by-area SPEC  Variable resolution by polygon area, e.g. '12:8,600:6,5'.
--parent-resolutions STR   Comma-separated parent resolutions (default: "9,8,0").
--hex-memory SIZE          Memory per hex pod (default: 8Gi).
--cells-per-chunk N        Estimated H3 cells per hex chunk (default: 5,000,000).
--chunk-size N             Fixed features per chunk instead of a plan (default 1000).
--max-completions N        Maximum hex chunks, max 200 (default: 200).
--max-parallelism N        Max concurrent pods (default: 50).
--id-column COL            ID column name (auto-detected if omitted).
--output-dir DIR           Directory for generated YAML files.
--intermediate-chunk-size  Rows per unnest batch (decrease if OOM).
--row-group-size N         Rows per parquet row group (default: 100000).
--convert-memory SIZE      Memory for the convert pod (default: --hex-memory).
--simplify-tolerance T     Simplify geometry during convert (degrees for EPSG:4326).
--trim-strings             Strip whitespace from string columns during convert.
--lat-column/--lon-column  Coordinate columns for a CSV of points (auto-detected).
--expect-features N        Fail the convert step unless it writes exactly N features.
--namespace NS             Kubernetes namespace (default: geo-workflows).
--profile NAME             Cluster profile name or YAML path (default: NRP settings).
--backend BACKEND          k8s (default) | armada.
```

### `cng-datasets raster-workflow` options

```
--dataset NAME             Dataset name for S3 paths. Use / for hierarchy.
--source-url URL           Public URL to a source raster. Repeat to mosaic tiles.
--years LIST               Annual series, e.g. '1985,2015-2025'; the URL holds {year}.
--bucket BUCKET            Target S3 bucket name.
--h3-resolution N          H3 resolution for hex tiling (default: 8).
--parent-resolutions STR   Comma-separated parent resolutions (default: "0").
--hex-resampling R         mean | sum | mode | fractions | max | min (default: mean).
--nodata VALS              NoData value(s) to exclude, comma-separated.
--hex-memory SIZE          Memory per hex pod (default: 32Gi).
--hex-cpu N                CPU request/limit per hex pod (default: 4).
--hex-workers N            Worker processes per hex pod (default: one per --hex-cpu).
--hex-chunk-size N         Cells per exact_extract call (default: 100000).
--h0-subset POSITIONS      h0 grid positions the source overlaps (default: all 122).
--h0-cells BASECELLS       The same, as H3 base cell numbers (see the note below).
--chunk-resolution N       H3 resolution of one hex pod's unit of work (default: 0, one h0).
--max-hex-memory SIZE      Pick --chunk-resolution automatically to fit this budget.
--backend BACKEND          k8s (default) | armada | auto (armada past ~200 chunks).
--max-parallelism N        Max concurrent hex pods (default: 61).
--namespace NS             Kubernetes namespace (default: geo-workflows).
--output-dir DIR           Directory for generated YAML files.
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

**OOM on hex jobs (vector):** Lower `--cells-per-chunk` so each chunk holds fewer cells, increase `--hex-memory` (e.g., 32Gi → 64Gi), or decrease `--intermediate-chunk-size`. `kubectl logs job/<name>-plan` shows the chunk count and the largest chunk.

**Raster hex pod too big even at one worker:** the pod's memory tracks the *largest chunk's* H3-cell count, and the default chunk is a whole h0 base cell (~282M cells at res 10). Pass `--chunk-resolution 1` or `2` to `raster-workflow` to make the unit a descendant of an h0 instead — roughly 7x fewer cells per level — or `--max-hex-memory 8Gi` to have it chosen for you. The generated workflow gains a merge step that restores the usual `h0={cell}/data_0.parquet` layout, so the published layout is unchanged. Past ~200 chunks a single indexed Job exceeds the namespace pod guideline; `--backend auto` routes to Armada at that point. Sub-chunked pods read only their own window of the source COG rather than localizing the whole file (`--window-reads`), so total transfer stays proportional to the data instead of to the number of chunks.

**OOM on hex jobs (raster):** Lower `--hex-workers` before raising `--hex-memory`. Peak RSS is roughly `--hex-workers × --hex-chunk-size × bytes per cell`, and a memory request large enough to matter (past ~128Gi) makes the pod contend for scarce large-RAM nodes, turning a retryable OOM into an unschedulable one. Both knobs are always written into the manifest as `CNG_HEX_WORKERS` / `CNG_HEX_CHUNK_SIZE`, so a tuned pod survives regeneration.

**Convert fails on curved geometries (MULTISURFACE):** Handled automatically — the converter linearizes curved geometry types via ogr2ogr before processing.

**Monitoring:**

```bash
kubectl get jobs              # Pipeline status
kubectl logs job/<name>       # Job logs
kubectl get pods | grep OOM   # Check for memory issues
```

## Development

The tests expect the GDAL, PROJ and DuckDB stack in the project's Docker image, so run them there:

```bash
docker run --rm -v "$PWD":/app -w /app ghcr.io/boettiger-lab/datasets:latest \
  bash -c 'pip install -q -e ".[dev]" && python -m pytest tests/ -q'
```

See [CHANGELOG.md](CHANGELOG.md) for what changed in each release.

## License

Apache 2.0

## Citation

If you use cng-datasets in your work, please cite it via its Zenodo archive.
The DOI badge at the top of this README resolves to the latest release; each
release also has its own version-specific DOI. Citation metadata (authors,
title, funding) lives in [`.zenodo.json`](.zenodo.json) and is minted
automatically by Zenodo on each GitHub release.

> Boettiger, C., & Buhler, C. K. cng-datasets: A cloud-native toolkit for
> processing large geospatial datasets on Kubernetes. Zenodo.
> https://zenodo.org/badge/latestdoi/921944685

Once the first release is archived on Zenodo, replace the redirect above with
the concrete versioned DOI it mints.
