# Raster Processing

Create Cloud-Optimized GeoTIFFs (COGs) and H3-indexed parquet from raster datasets.

## Overview

The raster processing module provides tools to:
- Create Cloud-Optimized GeoTIFFs optimized for cloud rendering (titiler)
- Convert rasters to H3-indexed parquet files
- Auto-detect optimal H3 resolution from pixel size
- Process global rasters by h0 regions for memory efficiency

## Basic Usage

### Python API

```python
from cng_datasets.raster import RasterProcessor

# Process raster to COG and H3-indexed parquet
processor = RasterProcessor(
    input_path="wetlands.tif",
    output_cog_path="s3://bucket/wetlands-cog.tif",
    output_parquet_path="s3://bucket/wetlands/hex/",
    h3_resolution=None,  # Auto-detect
    parent_resolutions=[8, 0],
    value_column="wetland_class",
    nodata_value=255,
)

# Create COG
processor.create_cog()

# Convert to H3-indexed parquet
processor.process_all_h0_regions()
```

### Command-Line Interface

```bash
# Create COG only
cng-datasets raster \
    --input wetlands.tif \
    --output-cog s3://bucket/wetlands-cog.tif \
    --compression zstd

# Raster to H3 parquet (auto-detect resolution)
cng-datasets raster \
    --input wetlands.tif \
    --output-parquet s3://bucket/wetlands/hex/ \
    --parent-resolutions "8,0" \
    --value-column wetland_class \
    --nodata 255

# COG + H3 in one command
cng-datasets raster \
    --input data.tif \
    --output-cog s3://bucket/data-cog.tif \
    --output-parquet s3://bucket/data/hex/ \
    --resolution 10 \
    --parent-resolutions "9,8,0"
```

## Auto-Detection of H3 Resolution

The processor can automatically detect the optimal H3 resolution based on the raster's pixel resolution:

```python
from cng_datasets.raster import detect_optimal_h3_resolution

# Get recommended H3 resolution
h3_res = detect_optimal_h3_resolution("high-res-raster.tif")
print(f"Recommended H3 resolution: {h3_res}")
```

### Resolution Mapping

| Pixel Size | Recommended H3 | Use Case |
|------------|---------------|----------|
| 0.5-2m | h14-h15 | High-res imagery |
| 7-25m | h12-h13 | Sentinel/aerial |
| 30-300m | h9-h10 | Landsat/regional |
| 1-12km | h7-h9 | Climate/global |

The processor provides helpful feedback when you choose a resolution different from the detected one:
- **Finer resolution**: "Using h12 instead of detected h10 - will create more cells"
- **Coarser resolution**: "Using h8 instead of detected h10 - will aggregate more pixels"

## Parameters

### RasterProcessor

- `input_path` (str): Path to input raster file (supports /vsis3/ URLs)
- `output_cog_path` (str, optional): Path to output COG
- `output_parquet_path` (str, optional): Path to output parquet directory
- `h3_resolution` (int, optional): H3 resolution (None for auto-detect)
- `parent_resolutions` (list[int]): Parent resolutions for aggregation (default: [0])
- `h0_index` (int, optional): Process specific h0 region (0-121)
- `value_column` (str): Name for raster value column (default: "value")
- `nodata_value` (float, optional): NoData value to exclude
- `compression` (str): COG compression method (default: "zstd")
- `blocksize` (int): COG tile size (default: 512)
- `resampling` (str): Resampling method (default: "nearest")

## Cloud-Optimized GeoTIFF (COG)

COGs are optimized for cloud rendering with titiler:

```python
processor = RasterProcessor(
    input_path="data.tif",
    output_cog_path="s3://bucket/data-cog.tif",
    compression="zstd",  # or "deflate", "lzw"
    blocksize=512,  # Tile size
    resampling="bilinear"  # or "nearest", "cubic"
)

cog_path = processor.create_cog()
```

COGs include:
- Internal tiling (configurable blocksize)
- Overview pyramids for zoom levels
- Optimized compression
- EPSG:4326 reprojection if needed
- Multi-threaded processing

## H3 Processing by h0 Regions

For global rasters, process by h0 regions (0-121) for memory efficiency:

```python
# Process all h0 regions
processor = RasterProcessor(
    input_path="s3://bucket/global.tif",
    output_parquet_path="s3://bucket/global/hex/",
    h3_resolution=8,
    parent_resolutions=[0],
)
output_files = processor.process_all_h0_regions()

# Or process specific h0 region (useful for K8s jobs)
processor = RasterProcessor(
    input_path="s3://bucket/global.tif",
    output_parquet_path="s3://bucket/global/hex/",
    h0_index=42,  # Process only h0 region 42
    h3_resolution=8,
)
processor.process_h0_region()
```

This enables:
- Memory-efficient processing of large rasters
- Parallel processing via Kubernetes
- Independent failure handling per region

## Kubernetes Processing

Process global rasters in parallel using Kubernetes:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: raster-processing
spec:
  completions: 122  # One per h0 region
  parallelism: 61
  completionMode: Indexed
  template:
    spec:
      containers:
      - name: processor
        image: ghcr.io/boettiger-lab/datasets:latest
        command:
        - python
        - /app/job.py
        - --i
        - $(JOB_COMPLETION_INDEX)
        - --input-url
        - /vsis3/bucket/data.tif
        - --output-url
        - s3://bucket/output/
```

Or use the Python API:

```python
from cng_datasets.k8s import K8sJobManager

manager = K8sJobManager()
job = manager.generate_chunked_job(
    job_name="wetlands-raster-h3",
    script_path="/app/wetlands/glwd/job.py",
    num_chunks=122,  # One per h0 region
    base_args=[
        "--input-url", "s3://bucket/wetlands.tif",
        "--output-url", "s3://bucket/wetlands/hex/",
        "--parent-resolutions", "8,0",
    ],
    parallelism=61,
    cpu="4",
    memory="34Gi",
)
manager.save_job_yaml(job, "wetlands-job.yaml")
```

## Output Format

Output is partitioned by h0 (continent-scale) H3 cells:

```
s3://bucket/dataset/
├── dataset-cog.tif          # Cloud-Optimized GeoTIFF
└── hex/                     # H3-indexed parquet
    └── h0=0/
        └── h0_0.parquet
    └── h0=1/
        └── h0_1.parquet
    ...
```

Each parquet file contains:
- `h3_cell`: H3 cell ID at specified resolution
- `value`: Raster value (customizable column name)
- Parent H3 cells if `parent_resolutions` specified
- Excludes nodata values if specified

## Examples

See the following directories for complete examples:
- `wetlands/glwd/` - Raster to H3 conversion with global h0 processing
- `iucn/` - Species range maps raster processing
- `ncp/` - Nature contributions to people raster data
