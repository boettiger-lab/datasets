# Redlining / Mapping Inequality

## Source Data

Original geopackage: https://dsl.richmond.edu/panorama/redlining/static/mappinginequality.gpkg

Historical redlining maps showing discriminatory mortgage lending practices in US cities from the 1930s-1940s.

## Automated Workflow

### Quick Start

Run the complete processing pipeline with a single command:

```bash
# IMPORTANT: Commit and push all changes to GitHub first!
# The workflow clones from GitHub, so local changes won't be included.
git add -A
git commit -m "Update redlining processing scripts"
git push

# First, set up RBAC (one-time setup)
kubectl apply -f redlining/workflow-rbac.yaml

# Run the complete workflow
kubectl apply -f redlining/workflow.yaml -n biodiversity

# Monitor progress
kubectl logs -f job/redlining-workflow -n biodiversity
```

The workflow automatically orchestrates all processing steps:
1. **Parquet & PMTiles Generation** (parallel) - Convert GPKG to parquet and PMTiles
2. **H3 Hex Processing** - Generate H3 resolution 10 hexagons with parent hexes (h9, h8, h0)
3. **Repartitioning** - Consolidate hex data by h0 partition
4. **Cleanup** - Remove temporary chunks directory (only on success)

### Workflow Components

The workflow job (`workflow.yaml`) runs inside Kubernetes and manages the entire pipeline without requiring an active laptop connection. It:
- Submits jobs in the correct order
- Waits for each step to complete before proceeding
- Handles failures appropriately
- Cleans up temporary data on success

## Manual Processing Steps

You can also run individual steps manually:

### 1. GeoParquet Conversion

Convert the GPKG to GeoParquet format:

**Script:** `convert_gpkg_to_parquet.sh`  
**Job:** `convert-job.yaml`

```bash
kubectl apply -f redlining/convert-job.yaml -n biodiversity
```

**Output:** `s3://public-redlining/mappinginequality.parquet`

### 2. PMTiles Generation

Create vector tiles for web mapping:

**Script:** `create_pmtiles.sh`  
**Job:** `pmtiles-job.yaml`

The script converts GPKG → GeoJSONSeq → PMTiles using tippecanoe.

```bash
kubectl apply -f redlining/pmtiles-job.yaml -n biodiversity
```

**Output:** `s3://public-redlining/mappinginequality.pmtiles`

### 3. H3 Hexagon Processing

Process polygons into H3 resolution 10 hexagons with parent hexes:

**Script:** `vec.py`  
**Job:** `hex-job.yaml`

This script:
- Generates H3 hexagons at resolution 10
- Computes parent hexes at resolutions 9, 8, and 0
- Processes data in indexed chunks with parallel workers

```bash
kubectl apply -f redlining/hex-job.yaml -n biodiversity
```

**Output:** `s3://public-redlining/chunks/chunk_*.parquet` (temporary chunks)

### 4. Repartitioning by H3 Resolution 0

Reorganize hex chunks into hive-partitioned format by h0 cell:

**Script:** `repartition.py`  
**Job:** `repartition-job.yaml`

Reads all chunks and writes with h0 partitioning for efficient spatial queries. On success, automatically removes the temporary `chunks/` directory.

```bash
kubectl apply -f redlining/repartition-job.yaml -n biodiversity
```

**Output:** `s3://public-redlining/hex/` (partitioned by h0)

## Data Structure

The final hex data contains:
- All original attributes from the redlining polygons
- `h10`: H3 hexagon ID at resolution 10 (~15m edge length)
- `h9`: Parent hex at resolution 9 (~51m edge length)
- `h8`: Parent hex at resolution 8 (~174m edge length)
- `h0`: Parent hex at resolution 0 (continental scale)

The h0 partitioning enables efficient spatial queries by limiting data reads to relevant geographic regions.

## Manual Orchestration

If you prefer not to use the automated workflow, you can run the orchestration script:

```bash
# Run complete pipeline
./redlining/orchestrate.sh all

# Or run individual steps
./redlining/orchestrate.sh parquet
./redlining/orchestrate.sh pmtiles
./redlining/orchestrate.sh hex
./redlining/orchestrate.sh repartition
```