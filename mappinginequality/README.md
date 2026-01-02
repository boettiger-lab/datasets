# Redlining / Mapping Inequality

## Source Data

Original geopackage: https://dsl.richmond.edu/panorama/redlining/static/mappinginequality.gpkg

Historical redlining maps showing discriminatory mortgage lending practices in US cities from the 1930s-1940s.

## Overview

This workflow processes historical redlining data into cloud-native formats:
1. **GeoParquet** - Optimized columnar format for analysis
2. **PMTiles** - Vector tiles for web mapping
3. **H3-indexed Parquet** - Hexagonal tiling at resolution 10 with parent hexes (h9, h8, h0)

All processing uses the `cng_datasets` Python package for standardized, reusable data processing. Jobs use the `ghcr.io/boettiger-lab/datasets` image with all dependencies pre-installed, and clone the repository via an initContainer for fast execution.

### H3 Resolution Configuration

The workflow generates H3 hexagons at **resolution 10** (h10) and includes parent hexagons at resolutions **h9, h8, and h0**. These resolutions can be configured during workflow generation:

```bash
cng-datasets workflow \
  --dataset mappinginequality \
  --source-url https://dsl.richmond.edu/panorama/redlining/static/mappinginequality.gpkg \
  --bucket public-mappinginequality \
  --h3-resolution 10 \
  --parent-resolutions "9,8,0"
```

**Resolution Guide:**
- **h10** (~15m hexagons) - Fine-grained analysis, optimal for urban features
- **h9** (~50m hexagons) - Intermediate scale
- **h8** (~175m hexagons) - Neighborhood scale  
- **h0** (continent scale) - Used for efficient partitioning

Other datasets may use different resolutions based on their spatial scale (e.g., h8 for regional data, h12 for detailed local data).

### Processing Workflow

The H3 hexagonal tiling follows a **two-pass approach** (matching wdpa/):
1. **Chunking** - Process data in parallel chunks, writing to `s3://public-mappinginequality/mappinginequality/chunks/`
2. **Repartitioning** - Consolidate all chunks into h0-partitioned format in `s3://public-mappinginequality/mappinginequality/hex/`, then delete temporary chunks/

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
```
