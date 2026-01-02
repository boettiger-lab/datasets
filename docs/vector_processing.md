# Vector Processing

Convert polygon and point datasets to H3-indexed GeoParquet format.

## Overview

The vector processing module provides tools to convert geospatial vector data into H3-indexed parquet files. This is particularly useful for:

- Large polygon datasets (e.g., protected areas, administrative boundaries)
- Point datasets (e.g., species observations)
- Datasets that need hierarchical aggregation at multiple H3 resolutions

## Basic Usage

### Python API

```python
from cng_datasets.vector import H3VectorProcessor

processor = H3VectorProcessor(
    input_url="s3://my-bucket/polygons.parquet",
    output_url="s3://my-bucket/h3-indexed/",
    h3_resolution=10,
    parent_resolutions=[9, 8, 0],
    chunk_size=500,
    intermediate_chunk_size=10
)

# Process all chunks
output_files = processor.process_all_chunks()

# Or process a specific chunk (useful for parallel processing)
output_file = processor.process_chunk(chunk_id=5)
```

### Command-Line Interface

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
    --chunk-id 0 \
    --intermediate-chunk-size 5
```

## Two-Pass Processing

The toolkit uses a memory-efficient two-pass approach to handle large polygons:

### Pass 1: Convert to H3 Arrays
- Converts geometries to H3 cell arrays (no unnesting)
- Writes to intermediate file
- Memory-efficient for complex polygons

### Pass 2: Unnest and Write
- Reads arrays in small batches
- Unnests them into individual H3 cells
- Writes final output

This prevents OOM errors when processing large polygons at high H3 resolutions. If you still hit memory limits, reduce `intermediate_chunk_size` (default: 10).

## Parameters

### H3VectorProcessor

- `input_url` (str): Path to input GeoParquet file
- `output_url` (str): Path to output directory
- `h3_resolution` (int): Primary H3 resolution (default: 10)
- `parent_resolutions` (list[int]): Parent resolutions for aggregation (default: [9, 8, 0])
- `chunk_size` (int): Number of rows to process at once in Pass 1 (default: 500)
- `intermediate_chunk_size` (int): Number of array rows to unnest at once in Pass 2 (default: 10)
- `read_credentials` (dict, optional): S3 credentials for reading
- `write_credentials` (dict, optional): S3 credentials for writing

## Chunked Processing

For large datasets, process in chunks to avoid memory issues:

```python
# Process in parallel using Kubernetes
from cng_datasets.k8s import K8sJobManager

manager = K8sJobManager()
job = manager.generate_chunked_job(
    job_name="dataset-h3-tiling",
    script_path="/app/tile_vectors.py",
    num_chunks=100,
    parallelism=20
)
manager.save_job_yaml(job, "tiling-job.yaml")
```

## Memory Optimization

If you encounter Out-Of-Memory errors:

1. **Reduce `chunk_size`**: Processes fewer rows in Pass 1
2. **Reduce `intermediate_chunk_size`**: Unnests fewer arrays in Pass 2
3. **Use lower H3 resolution**: Fewer cells per geometry
4. **Process in chunks**: Use `chunk_id` parameter for parallel processing

Example for memory-constrained environments:

```python
processor = H3VectorProcessor(
    input_url="s3://bucket/large-polygons.parquet",
    output_url="s3://bucket/output/",
    h3_resolution=10,
    chunk_size=100,  # Reduced from default 500
    intermediate_chunk_size=5  # Reduced from default 10
)
```

## Output Format

Output is partitioned by h0 (continent-scale) H3 cells:

```
s3://bucket/output/
└── h0=0/
    └── chunk_0.parquet
└── h0=1/
    └── chunk_0.parquet
...
```

Each parquet file contains:
- `h3_cell`: H3 cell ID at specified resolution
- Original attributes from input dataset
- Parent H3 cells if `parent_resolutions` specified

## Examples

See the following directories for complete examples:
- `redlining/` - Vector polygon processing with chunking
- `wdpa/` - Large-scale protected areas processing
- `pad-us/` - Protected areas database H3 tiling
