# CNG Datasets Toolkit

A Python toolkit for processing large geospatial datasets into cloud-native formats with H3 hexagonal indexing.

[![Documentation](https://img.shields.io/badge/docs-github%20pages-blue)](https://boettiger-lab.github.io/datasets/)
[![PyPI](https://img.shields.io/pypi/v/cng-datasets)](https://pypi.org/project/cng-datasets/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Features

- **Vector Processing**: Convert polygon and point datasets to H3-indexed GeoParquet
- **Raster Processing**: Create Cloud-Optimized GeoTIFFs (COGs) and H3-indexed parquet
- **Kubernetes Integration**: Generate and submit K8s jobs for large-scale processing
- **Cloud Storage**: Manage S3 buckets and sync across multiple providers with rclone
- **Scalable**: Chunk-based processing for datasets that don't fit in memory

## Quick Start

### Installation

```bash
# From PyPI
pip install cng-datasets

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

### Using Docker (Recommended)

The easiest way to use this package with full GDAL support is via Docker:

```bash
# Pull the pre-built image
docker pull ghcr.io/boettiger-lab/datasets:latest

# Run interactively
docker run -it --rm -v $(pwd):/data ghcr.io/boettiger-lab/datasets:latest bash

# Or run a specific command
docker run --rm -v $(pwd):/data ghcr.io/boettiger-lab/datasets:latest \
  cng-datasets raster --input /data/input.tif --output-cog /data/output.tif
```

## Usage

### Vector Processing

Process polygon and point datasets to H3-indexed parquet:

```python
from cng_datasets.vector import H3VectorProcessor

# Process entire dataset
processor = H3VectorProcessor(
    input_url="s3://my-bucket/polygons.parquet",
    output_url="s3://my-bucket/h3-indexed/",
    h3_resolution=10,
    parent_resolutions=[9, 8, 0],
    chunk_size=500,  # Rows to process at once
    intermediate_chunk_size=10  # Reduce if hitting OOM
)

# Process all chunks
output_files = processor.process_all_chunks()

# Or process a specific chunk (useful for parallel K8s jobs)
output_file = processor.process_chunk(chunk_id=5)
```

**Command-line:**

```bash
# Process entire dataset
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
```

### Raster Processing

Create Cloud-Optimized GeoTIFFs and H3-indexed parquet:

```python
from cng_datasets.raster import RasterProcessor

# Process raster to COG and H3-indexed parquet
processor = RasterProcessor(
    input_path="wetlands.tif",
    output_cog_path="s3://bucket/wetlands-cog.tif",
    output_parquet_path="s3://bucket/wetlands/hex/",
    h3_resolution=None,  # Auto-detect from raster resolution
    parent_resolutions=[8, 0],
    value_column="wetland_class",
    nodata_value=255,  # Exclude nodata pixels
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

| Pixel Size | Recommended H3 | Use Case |
|------------|---------------|----------|
| 0.5-2m | h14-h15 | High-res imagery |
| 7-25m | h12-h13 | Sentinel/aerial |
| 30-300m | h9-h10 | Landsat/regional |
| 1-12km | h7-h9 | Climate/global |

**Command-line:**

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

# Process specific h0 region (for K8s jobs)
cng-datasets raster \
    --input s3://bucket/data.tif \
    --output-parquet s3://bucket/dataset/hex/ \
    --h0-index 42 \
    --resolution 8

# COG + H3 in one command
cng-datasets raster \
    --input data.tif \
    --output-cog s3://bucket/data-cog.tif \
    --output-parquet s3://bucket/data/hex/ \
    --resolution 10 \
    --parent-resolutions "9,8,0"
```

### Kubernetes Workflows

Generate complete K8s workflows for large-scale processing:

```bash
# Generate workflow files
cng-datasets workflow \
  --dataset my-dataset \
  --source-url https://example.com/data.gpkg \
  --bucket public-my-dataset \
  --h3-resolution 10 \
  --parent-resolutions "9,8,0" \
  --namespace biodiversity \
  --output-dir my-dataset/
```

This generates:
- `workflow-rbac.yaml` - ServiceAccount/Role/RoleBinding (one per namespace)
- `workflow-pvc.yaml` - PVC for storing YAML files
- `workflow-upload.yaml` - Job for uploading YAMLs to PVC
- `workflow.yaml` - Orchestrator job
- `convert-job.yaml` - Convert source format → GeoParquet
- `pmtiles-job.yaml` - Generate PMTiles vector tiles
- `hex-job.yaml` - H3 hexagonal tiling with automatic chunking
- `repartition-job.yaml` - Consolidate chunks by h0 partition

**Run the workflow:**

```bash
# One-time setup per namespace
kubectl apply -f my-dataset/workflow-rbac.yaml

# Create PVC and upload YAMLs
kubectl apply -f my-dataset/workflow-pvc.yaml
kubectl apply -f my-dataset/workflow-upload.yaml

# Wait for upload pod and copy files
kubectl wait --for=condition=ready pod -l job-name=my-dataset-upload-yamls -n biodiversity
POD=$(kubectl get pods -l job-name=my-dataset-upload-yamls -n biodiversity -o jsonpath='{.items[0].metadata.name}')
kubectl cp my-dataset/convert-job.yaml $POD:/yamls/ -n biodiversity
kubectl cp my-dataset/pmtiles-job.yaml $POD:/yamls/ -n biodiversity
kubectl cp my-dataset/hex-job.yaml $POD:/yamls/ -n biodiversity
kubectl cp my-dataset/repartition-job.yaml $POD:/yamls/ -n biodiversity

# Start orchestrator (laptop can disconnect after this)
kubectl apply -f my-dataset/workflow.yaml

# Monitor progress
kubectl logs -f job/my-dataset-workflow -n biodiversity
```

## Architecture

### Output Structure

```
s3://bucket/
├── dataset-name.parquet         # GeoParquet with all attributes
├── dataset-name.pmtiles         # PMTiles vector tiles
└── dataset-name/
    └── hex/                     # H3-indexed parquet (partitioned by h0)
        └── h0=*/
            └── *.parquet
```

### Processing Approach

**Vector Datasets:**
1. Convert to optimized GeoParquet (if needed)
2. Generate PMTiles for web visualization
3. Tile to H3 hexagons in chunks
4. Partition by h0 cells for efficient querying

**Raster Datasets:**
1. Create Cloud-Optimized GeoTIFF (COG)
2. Convert to H3-indexed parquet by h0 regions
3. Partition by h0 cells for efficient querying

### H3 Resolutions

```bash
--h3-resolution 10        # Primary resolution (default: 10)
--parent-resolutions "9,8,0"  # Parent hexes for aggregation (default: "9,8,0")
```

**Resolution Reference:**
- h12: ~3m (building-level)
- h11: ~10m (lot-level)  
- h10: ~15m (street-level) - **default**
- h9: ~50m (block-level)
- h8: ~175m (neighborhood)
- h7: ~600m (district)
- h0: continent-scale (partitioning key)

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

Configure rclone remotes in `~/.config/rclone/rclone.conf`:

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
- `gbif/` - Species occurrence data processing

## Documentation

Full documentation is available at: https://boettiger-lab.github.io/datasets/

## License

MIT License - see [LICENSE](LICENSE) for details

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development guidelines

