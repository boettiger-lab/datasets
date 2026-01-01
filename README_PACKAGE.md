# CNG Datasets Toolkit

A Python toolkit for processing large geospatial datasets into cloud-native formats with H3 hexagonal indexing.

## Features

- **Vector Processing**: Convert polygon and point datasets to H3-indexed GeoParquet
- **Raster Processing**: Create Cloud-Optimized GeoTIFFs (COGs) and H3-indexed parquet
- **Kubernetes Integration**: Generate and submit K8s jobs for large-scale processing
- **Cloud Storage**: Manage S3 buckets and sync across multiple providers with rclone
- **Scalable**: Chunk-based processing for datasets that don't fit in memory

## Installation

```bash
# From this repository
pip install -e .

# With development tools
pip install -e ".[dev]"

# With raster processing support (requires system GDAL)
pip install -e ".[raster]"
```

**Note on GDAL**: For raster processing, GDAL requires system libraries. Install GDAL first:

```bash
# Ubuntu/Debian
sudo apt-get install gdal-bin libgdal-dev python3-gdal

# macOS
brew install gdal

# Then install with raster support
pip install -e ".[raster]"
```

For containerized environments (Docker/Kubernetes), use images with GDAL pre-installed like `ghcr.io/rocker-org/ml-spatial:latest`.

## Quick Start

### Raster Processing

```python
from cng_datasets.raster import RasterProcessor

# Process raster to COG and H3-indexed parquet
processor = RasterProcessor(
    input_path="wetlands.tif",
    output_cog_path="s3://bucket/wetlands-cog.tif",
    output_parquet_path="s3://bucket/wetlands/hex/",
    h3_resolution=None,  # Auto-detect from raster resolution
    parent_resolutions=[8, 0],
)

# Create Cloud-Optimized GeoTIFF (optimized for titiler)
processor.create_cog()

# Convert to H3-indexed parquet partitioned by h0
processor.process_all_h0_regions()

# Or process a specific h0 region (useful for K8s jobs)
processor = RasterProcessor(
    input_path="wetlands.tif",
    output_parquet_path="s3://bucket/wetlands/hex/",
    h0_index=42,  # Process only h0 region 42
    h3_resolution=8,
    parent_resolutions=[0],
)
processor.process_h0_region()
```

**Auto-Detection of H3 Resolution**: The processor can automatically detect the optimal H3 resolution based on the raster's pixel resolution:

```python
from cng_datasets.raster import detect_optimal_h3_resolution

# Get recommended H3 resolution
h3_res = detect_optimal_h3_resolution("high-res-raster.tif")
print(f"Recommended H3 resolution: {h3_res}")  # e.g., 12 for 25m pixels

# You can also specify a different resolution
processor = RasterProcessor(
    input_path="data.tif",
    h3_resolution=10,  # User override
    # Informative message will show: "Using h10 instead of detected h12"
)
```

The processor provides helpful feedback when you choose a resolution different from the detected one:
- **Finer resolution**: "Using h12 instead of detected h10 - will create more cells"
- **Coarser resolution**: "Using h8 instead of detected h10 - will aggregate more pixels"
- **No error**: Your choice is always respected

**Resolution Mapping**:
- High-res imagery (0.5-2m pixels) → h14-h15
- Medium-res (25-90m pixels) → h11-h12
- Landsat/Sentinel (30-300m pixels) → h9-h10
- Regional datasets (1-12km pixels) → h7-h9

### Using the Class Interface

```python
from cng_datasets.raster import RasterProcessor

processor = RasterProcessor(
    input_path="s3://source-bucket/data.tif",
    output_cog_path="s3://bucket/data-cog.tif",
    output_parquet_path="s3://bucket/dataset/hex/",
    h3_resolution=8,
    parent_resolutions=[0],  # Include h0 for partitioning
    value_column="wetland_class",
    nodata_value=255,  # Exclude nodata pixels
    compression="zstd",  # COG compression
)

# Create COG
cog_path = processor.create_cog()

# Process by h0 regions (memory-efficient for global data)
output_files = processor.process_all_h0_regions()
```

```python
from cng_datasets.vector import process_vector_chunks

# Process entire dataset
process_vector_chunks(
    input_url="s3://my-bucket/input.parquet",
    output_url="s3://my-bucket/output/",
    h3_resolution=10,
    chunk_size=500,
    intermediate_chunk_size=10  # Reduce if hitting OOM with large polygons
)

# Process specific chunk (useful for parallel K8s jobs)
process_vector_chunks(
    input_url="s3://my-bucket/input.parquet",
    output_url="s3://my-bucket/output/",
    chunk_id=0,
    h3_resolution=10
)
```

**Two-Pass Processing**: The toolkit uses a memory-efficient two-pass approach:
- **Pass 1**: Converts geometries to H3 cell arrays (no unnesting) and writes to intermediate file
- **Pass 2**: Reads arrays in small batches, unnests them, and writes final output

This prevents OOM errors when processing large polygons at high H3 resolutions. If you still hit memory limits, reduce `intermediate_chunk_size` (default: 10).

### Using the Class Interface

```python
from cng_datasets.vector import H3VectorProcessor

processor = H3VectorProcessor(
    input_url="s3://my-bucket/polygons.parquet",
    output_url="s3://my-bucket/h3-indexed/",
    h3_resolution=10,
    parent_resolutions=[9, 8, 0],
    chunk_size=500,  # Pass 1: rows to process at once
    intermediate_chunk_size=10  # Pass 2: array rows to unnest at once
)

# Process all chunks
output_files = processor.process_all_chunks()

# Or process a specific chunk
output_file = processor.process_chunk(chunk_id=5)
```

### Kubernetes Job Generation

```python
from cng_datasets.k8s import K8sJobManager

manager = K8sJobManager(
    namespace="datasets",
    image="ghcr.io/boettiger-lab/datasets:latest"
)

# Generate a chunked processing job
job_spec = manager.generate_chunked_job(
    job_name="redlining-h3",
    script_path="/app/process.py",
    num_chunks=100,
    base_args=["--resolution", "10"],
    cpu="2",
    memory="8Gi",
    parallelism=10
)

manager.save_job_yaml(job_spec, "job.yaml")
```

### Command-Line Interface

```bash
# Vector processing
cng-datasets vector \
    --input s3://bucket/input.parquet \
    --output s3://bucket/output/ \
    --resolution 10 \
    --chunk-size 500 \
    --intermediate-chunk-size 10

# Process specific chunk (reduce intermediate-chunk-size if hitting OOM)
cng-datasets vector \
    --input s3://bucket/input.parquet \
    --output s3://bucket/output/ \
    --chunk-id 0 \
    --intermediate-chunk-size 5

# Raster processing - Create COG
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

# Raster: Process specific h0 region (for K8s jobs)
cng-datasets raster \
    --input s3://bucket/data.tif \
    --output-parquet s3://bucket/dataset/hex/ \
    --h0-index 42 \
    --resolution 8

# Raster: COG + H3 in one command
cng-datasets raster \
    --input data.tif \
    --output-cog s3://bucket/data-cog.tif \
    --output-parquet s3://bucket/data/hex/ \
    --resolution 10 \
    --parent-resolutions "9,8,0"

# Generate K8s job
cng-datasets k8s \
    --job-name my-processing-job \
    --command python /app/process.py \
    --chunks 100 \
    --output job.yaml

# Configure bucket CORS
cng-datasets storage cors \
    --bucket my-public-bucket \
    --endpoint https://s3.amazonaws.com

# Sync with rclone
cng-datasets storage sync \
    --source remote1:bucket/data \
    --destination remote2:bucket/data
```

## Architecture

The toolkit is organized into four main modules:

1. **`vector`**: H3 tiling and vector data processing
2. **`raster`**: COG creation and raster to H3 conversion
3. **`k8s`**: Kubernetes job generation and management
4. **`storage`**: S3 bucket management and rclone syncing

## Typical Workflow

### Vector Datasets (Polygons)

1. Convert to optimized GeoParquet (if needed)
2. Generate PMTiles for web visualization
3. Tile to H3 hexagons in chunks
4. Partition by h0 cells for efficient querying

```python
from cng_datasets.vector import H3VectorProcessor
from cng_datasets.k8s import K8sJobManager

# Step 1: Process locally or generate K8s job
processor = H3VectorProcessor(
    input_url="s3://public-dataset/input.parquet",
    output_url="s3://public-dataset/dataset-name/chunks/",
    h3_resolution=10,
    chunk_size=500
)

# Step 2: Generate K8s job for parallel processing
manager = K8sJobManager()
job = manager.generate_chunked_job(
    job_name="dataset-h3-tiling",
    script_path="/app/tile_vectors.py",
    num_chunks=100,
    parallelism=20
)
manager.save_job_yaml(job, "tiling-job.yaml")
```

### Raster Datasets

1. Create Cloud-Optimized GeoTIFF (COG)
2. Convert to H3-indexed parquet by h0 regions
3. Partition by h0 cells for efficient querying

```python
from cng_datasets.raster import RasterProcessor

# Process raster dataset
processor = RasterProcessor(
    input_path="wetlands.tif",
    output_cog_path="s3://bucket/wetlands-cog.tif",
    output_parquet_path="s3://bucket/wetlands/hex/",
    h3_resolution=None,  # Auto-detect optimal resolution
    parent_resolutions=[8, 0],
)

# Create COG (optimized for cloud rendering)
processor.create_cog()

# Convert to H3 parquet
processor.process_all_h0_regions()
```

**Kubernetes Workflow for Global Rasters**:

```python
from cng_datasets.k8s import K8sJobManager

# Generate indexed job to process each h0 region in parallel
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

The raster processor:
- Automatically detects optimal H3 resolution from pixel size
- Processes global data by h0 regions (memory-efficient)
- Creates COGs optimized for cloud rendering (titiler)
- Supports parent resolutions for aggregation
- Handles nodata values
- Works with /vsis3/ URLs for direct S3 access

## Configuration

### S3 Credentials

The toolkit supports multiple authentication methods:

```python
# Using cng.utils (if available)
from cng.utils import set_secrets
con = setup_duckdb_connection()
set_secrets(con)

# Manual configuration
processor = H3VectorProcessor(
    input_url="s3://bucket/input.parquet",
    output_url="s3://bucket/output/",
    read_credentials={
        "key": "ACCESS_KEY",
        "secret": "SECRET_KEY",
        "region": "us-west-2"
    },
    write_credentials={
        "key": "ACCESS_KEY",
        "secret": "SECRET_KEY",
        "region": "us-west-2"
    }
)
```

### Rclone

Configure rclone remotes in `~/.config/rclone/rclone.conf` or use the API:

```python
from cng_datasets.storage import RcloneSync

syncer = RcloneSync(config_path="/path/to/rclone.conf")
syncer.sync(
    source="aws:public-dataset/",
    destination="cloudflare:public-dataset/"
)
```

## Development

```bash
# Install in development mode
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black cng_datasets/
ruff check cng_datasets/
```

## Examples

See the individual dataset directories for complete examples:
- `redlining/` - Vector polygon processing with chunking
- `wetlands/glwd/` - Raster to H3 conversion with global h0 processing
- `wdpa/` - Large-scale protected areas processing
- `hydrobasins/` - Multi-level watershed processing
