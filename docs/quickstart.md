# Quick Start

This guide will get you started with the CNG Datasets toolkit.

## Vector Processing Example

Process polygon datasets to H3-indexed parquet:

```python
from cng_datasets.vector import H3VectorProcessor

# Create processor
processor = H3VectorProcessor(
    input_url="s3://my-bucket/polygons.parquet",
    output_url="s3://my-bucket/h3-indexed/",
    h3_resolution=10,
    parent_resolutions=[9, 8, 0],
    chunk_size=500,
)

# Process all chunks
output_files = processor.process_all_chunks()
```

### Command-Line

```bash
cng-datasets vector \
    --input s3://bucket/input.parquet \
    --output s3://bucket/output/ \
    --resolution 10 \
    --chunk-size 500
```

## Raster Processing Example

Create Cloud-Optimized GeoTIFFs and H3-indexed parquet:

```python
from cng_datasets.raster import RasterProcessor

# Create processor
processor = RasterProcessor(
    input_path="wetlands.tif",
    output_cog_path="s3://bucket/wetlands-cog.tif",
    output_parquet_path="s3://bucket/wetlands/hex/",
    h3_resolution=None,  # Auto-detect
    parent_resolutions=[8, 0],
)

# Create COG
processor.create_cog()

# Convert to H3-indexed parquet
processor.process_all_h0_regions()
```

### Command-Line

```bash
# Create COG + H3 parquet
cng-datasets raster \
    --input data.tif \
    --output-cog s3://bucket/data-cog.tif \
    --output-parquet s3://bucket/data/hex/ \
    --resolution 10 \
    --parent-resolutions "9,8,0"
```

## Kubernetes Workflow Example

Generate and run a complete K8s workflow:

```bash
# Generate workflow files
cng-datasets workflow \
  --dataset my-dataset \
  --source-url https://example.com/data.gpkg \
  --bucket public-my-dataset \
  --h3-resolution 10 \
  --namespace biodiversity \
  --output-dir my-dataset/

# Apply RBAC
kubectl apply -f my-dataset/workflow-rbac.yaml

# Run workflow
kubectl apply -f my-dataset/workflow.yaml

# Monitor
kubectl logs -f job/my-dataset-workflow -n biodiversity
```

## Next Steps

- Learn more about [Vector Processing](vector_processing.md)
- Learn more about [Raster Processing](raster_processing.md)
- Set up [Kubernetes Workflows](kubernetes_workflows.md)
- Configure [S3 Credentials](configuration.md)
