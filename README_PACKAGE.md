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
```

## Quick Start

### Vector Processing

```python
from cng_datasets.vector import process_vector_chunks

# Process entire dataset
process_vector_chunks(
    input_url="s3://my-bucket/input.parquet",
    output_url="s3://my-bucket/output/",
    h3_resolution=10,
    chunk_size=500
)

# Process specific chunk (useful for parallel K8s jobs)
process_vector_chunks(
    input_url="s3://my-bucket/input.parquet",
    output_url="s3://my-bucket/output/",
    chunk_id=0,
    h3_resolution=10
)
```

### Using the Class Interface

```python
from cng_datasets.vector import H3VectorProcessor

processor = H3VectorProcessor(
    input_url="s3://my-bucket/polygons.parquet",
    output_url="s3://my-bucket/h3-indexed/",
    h3_resolution=10,
    parent_resolutions=[9, 8, 0],
    chunk_size=500
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
# Process vector data
cng-datasets vector \
    --input s3://bucket/input.parquet \
    --output s3://bucket/output/ \
    --resolution 10 \
    --chunk-size 500

# Process specific chunk
cng-datasets vector \
    --input s3://bucket/input.parquet \
    --output s3://bucket/output/ \
    --chunk-id 0

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
2. Convert to H3-indexed parquet
3. Partition by h0 cells

```python
from cng_datasets.raster import RasterProcessor

processor = RasterProcessor(
    input_path="wetlands.tif",
    output_cog_path="s3://bucket/wetlands-cog.tif",
    output_parquet_path="s3://bucket/wetlands-h3/",
    h3_resolution=10
)

processor.create_cog()
processor.raster_to_h3_parquet()
```

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
- `wetlands/` - Raster to H3 conversion
- `wdpa/` - Large-scale protected areas processing

## License

MIT
