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
- `h3_resolution` (int): Primary H3 resolution (default: 10). Ignored when `resolution_by_area` is set.
- `resolution_by_area` (list[tuple], optional): Variable resolution per feature, parsed from a `--resolution-by-area` spec (see [Variable Resolution by Area](#variable-resolution-by-area)). Mutually exclusive with `h3_resolution`.
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

## Chunk Planning

A hex pod's memory and runtime follow the H3 cells of its features, not how many
features it has, and per-feature cell counts are extremely skewed. On IUCN ranges at
res 6 the median feature is 345 cells and the largest is 2.5 M. So `cng-datasets
workflow` does not cut the hex fan-out at a fixed number of features. A **plan** step
runs after convert:

```bash
cng-datasets vector-plan --input s3://bucket/data.parquet \
    --output s3://bucket/data/_hex_plan.parquet --resolution 8 \
    --cells-per-chunk 5000000 --max-chunks 200
```

It estimates each row's cells (area / hex area for polygons, length / edge for lines,
one per point) and cuts the file into contiguous row ranges. A new chunk starts when
*either* the cells budget or 1000 features would be exceeded, so chunks are never larger
than the fixed rule would make them; heavy features are only split further. A feature
larger than the budget gets a chunk of its own. If the budget would need more than
`--max-chunks` chunks, it is raised to fit and the log says so. Hex pods run with
`--plan <url>`, and pod K processes range K.

On three published layers the largest fixed-count chunk was 2.2–2.4× the mean, while a
5 M-cell budget gave 1.0–1.2× (#124). Pass `--chunk-size N` to `workflow` for fixed
chunks instead.

Measured on a real 107-chunk build, a hex pod peaks at about **0.74 GiB + 354 bytes per
estimated cell**, so the default budget needs about 2.4 GiB. Memory binds only on a *single*
feature larger than the budget. The plan lists any chunk predicted to peak near the
`--hex-memory` limit. For those, raise `--hex-memory` or hex the largest features at a
coarser resolution with `--resolution-by-area`.

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

## Variable Resolution by Area

**When to use this.** Reach for `--resolution-by-area` when a single dataset mixes
small and very large polygons and a uniform fine resolution either OOMs in Pass 2
or trips the 2 GB parquet-page limit on the biggest features — but dropping the
*whole* dataset to a coarse resolution would throw away edge precision on the many
small features. The canonical case is a species-range or protected-areas layer
where a handful of continental/global polygons dominate cost while most features
are small. If all your features are a similar size, keep using `--resolution`.

Each feature is hexed at the native H3 resolution its **planar `ST_Area`** (deg²;
≈12,000 km² per deg² at the equator) maps to — coarser bins for bigger features:

```bash
cng-datasets vector \
    --input s3://bucket/ranges.parquet \
    --output s3://bucket/ranges/chunks \
    --resolution-by-area "12:8,600:6,5" \
    --parent-resolutions "7,6,5,4,0"
# area ≤ 12 deg²  -> res 8
# area ≤ 600 deg² -> res 6
# otherwise       -> res 5   (the trailing bare integer is the required catch-all)
```

**Output schema.** The result is a single uniform *union schema* — one `h{r}`
column for every resolution in `{native resolutions} ∪ {parent_resolutions}`
(capped at the finest native resolution) — plus a `native_res` column giving each
row's true resolution. Finer columns are `NULL` in coarser tiers; the coarsest
native resolution is non-null in every row, so flat equality joins still work
across the mixed-resolution union (no compaction / ancestor-walks needed).

**Requirements & notes.**
- Include `0` in `--parent-resolutions` — it is the `h0` hive partition key (the
  tool errors clearly if you omit it).
- Mutually exclusive with `--resolution` / `--h3-resolution`.
- The [oversized-feature guardrail](#memory-optimization) estimates cells at each
  feature's *own* native resolution, so a polygon that would exceed the per-feature
  cell-array limit at a fine resolution passes once a coarse bin is assigned.
- **Chunk-size caveat:** because larger features are hexed coarser, cells-per-feature
  still *rises* with the tier, so coarser/larger-feature tiers want a *smaller*
  `--chunk-size` (aim for a roughly constant few-million cells per chunk, not a
  constant row count). Automatic cells-per-chunk scaling is tracked in issue #124.
- For a *single* feature too large even for the coarsest bin, see issue #125
  (recursive classify-and-descend) — not yet implemented.

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
