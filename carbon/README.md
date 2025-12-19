# Conservation International - Vulnerable Carbon

This workflow processes Conservation International's vulnerable carbon dataset from Cloud Optimized GeoTIFF (COG) format to a globally partitioned H3 hexagon-based dataset.

## Overview

The pipeline transforms a global raster dataset of vulnerable carbon stocks into a partitioned Parquet dataset organized by H3 hexagons, enabling efficient spatial queries and analysis at multiple resolutions.

**Input:** Cloud Optimized GeoTIFF raster
- Source: `vulnerable_c_total_2018.tif` 
- Format: Raster with carbon values (integer)
- NoData value: 65535

**Output:** Partitioned Parquet dataset
- Location: `s3://public-carbon/hex/vulnerable-carbon/`
- Partitioning: By H3 level 0 hex (`h0=<hex_id>/`)
- Schema: `carbon` (integer), `h{zoom}` (H3 cell ID at target resolution), `h0` (H3 level 0 cell ID)
- Default resolution: H3 level 8

## Workflow Steps

### 1. Spatial Partitioning Strategy

The workflow uses the global H3 grid system at resolution 0 (122 hexagons covering Earth) to partition processing:

- Load reference grid: `s3://public-grids/hex/h0-valid.parquet`
- Contains geometry and identifiers for each h0 hexagon
- Each h0 hex is processed independently in parallel

### 2. Per-Hexagon Processing (`job.py`)

For each h0 hexagon (indexed 0-121):

1. **Extract hex geometry** from h0-valid.parquet based on index `i`
2. **Crop raster** using GDAL Warp:
   - Clips input COG to hex boundary (cutlineWKT)
   - Reprojects to EPSG:4326 if needed
   - Outputs to temporary XYZ point format
3. **Convert to H3 cells**:
   - Load XYZ data (X, Y, carbon value)
   - Filter out NoData values (65535)
   - Compute H3 cell IDs at target resolution (default zoom=8)
   - Compute H3 level 0 cell for partitioning
4. **Write output**:
   - Save as Parquet partitioned by h0
   - Path: `s3://public-carbon/hex/vulnerable-carbon/h0={h0}/data_0.parquet`

### 3. Kubernetes Job Array Execution (`job.yaml`)

The workflow uses Kubernetes indexed Jobs for parallelization:

```yaml
completions: 122        # One job per h0 hex
parallelism: 16         # Process 16 hexes concurrently
completionMode: Indexed # Each job gets unique index 0-121
```

**Job configuration:**
- Image: `ghcr.io/rocker-org/ml-spatial`
- Resources: 4 CPU cores, 34 GiB RAM per task
- Priority: Opportunistic (can be pre-empted)
- Init container clones GitHub repo with processing scripts
- AWS credentials mounted via Kubernetes secrets
- Uses internal S3 endpoint for efficiency

**Command execution:**
```bash
python carbon/job.py --i $INDEX --zoom 8
```

Where `$INDEX` (0-121) is automatically injected by Kubernetes from the job completion index.

### 4. Management Script (`run-job.sh`)

Helper script to deploy and monitor the Kubernetes job:
- Deletes any existing job
- Applies job manifest
- Waits for pods to be created
- Streams logs from containers
- Provides diagnostics on failure

## Usage

### Interactive Development (`hexes.ipynb`)

Test the workflow interactively for single hexagons:
```python
# Process one h0 hex locally
i = 100
wkt = df.geom[i]  # Get hex geometry
h0 = df.h0[i]     # Get hex ID
gdal.Warp("tmp-carbon.xyz", input_url, cutlineWKT=wkt, ...)
# Convert and write to parquet...
```

### Production Run

Deploy to Kubernetes cluster:
```bash
./run-job.sh
```

Or with custom configuration:
```bash
JOB_NAME=carbon-job JOB_FILE=./job.yaml TIMEOUT=4h ./run-job.sh
```

### Single Hex Processing

Process a specific hex locally:
```bash
python job.py --i 42 --zoom 8 --input-url /vsis3/public-carbon/cogs/vulnerable_c_total_2018.tif
```

Parameters:
- `--i`: Hex index (0-121, required)
- `--zoom`: H3 resolution (default: 8)
- `--input-url`: Input raster path/URL
- `--profile`: Enable memory/runtime profiling

## Data Flow

```
COG Raster (Global)
    ↓
[GDAL Warp] → Crop to h0 hex → XYZ points
    ↓
[DuckDB + H3 extension]
    ↓
Compute H3 cells at target resolution
    ↓
Filter NoData values
    ↓
Parquet (partitioned by h0)
```

## Dependencies

- Python with GDAL bindings (`osgeo.gdal`)
- DuckDB with spatial and H3 extensions
- Ibis with DuckDB backend
- Custom utilities: `cng.utils`, `cng.h3`
- AWS S3 access (credentials via environment)

## Output Structure

```
s3://public-carbon/hex2/vulnerable-carbon/
├── h0=8009fffffffffff/
│   └── data_0.parquet
├── h0=8011fffffffffff/
│   └── data_0.parquet
├── ...
└── h0=80f3fffffffffff/
    └── data_0.parquet
```

Each partition contains:
- `carbon`: Integer carbon value from raster
- `h8`: H3 cell ID at resolution 8 (or specified zoom)
- `h0`: H3 cell ID at resolution 0 (partitioning key)

