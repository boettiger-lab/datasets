# Vector Processing

Convert polygon and point datasets to H3-indexed GeoParquet format.

## Overview

The vector processing module provides tools to convert geospatial vector data into H3-indexed parquet files. This is particularly useful for:

- Large polygon datasets (e.g., protected areas, administrative boundaries)
- Point datasets (e.g., species observations)
- Datasets that need hierarchical aggregation at multiple H3 resolutions

## Basic Usage

### Converting the source to GeoParquet

The hex step reads GeoParquet in EPSG:4326 with an id column. `convert_to_parquet` (CLI:
`cng-convert-to-parquet`) produces it from anything GDAL or DuckDB reads: Shapefile,
GeoPackage, FileGDB, GeoJSON, GeoParquet, or a CSV of points. Several sources are merged
with `UNION ALL`, so they must share their columns in order.

```python
from cng_datasets.vector.convert_to_parquet import convert_to_parquet

convert_to_parquet(
    "https://example.com/parks.gdb",
    "s3://my-bucket/parks.parquet",
    layer="Parks",
)
```

```bash
cng-convert-to-parquet https://example.com/parks.gdb s3://my-bucket/parks.parquet --layer Parks
```

| Option | Default | What it does |
|---|---|---|
| `--layer` | the only layer | Layer of a multi-layer source |
| `--id-column` | auto-detected | Id column to keep. Without one, a `_cng_fid` column is added (`--no-force-id` skips it) |
| `--target-crs` | `EPSG:4326` | Output CRS. Features that cross the antimeridian are split at 180° after reprojection |
| `--simplify-tolerance` | none | Simplify geometry, in target-CRS units (0.0001° ≈ 10 m), for very high-vertex sources |
| `--trim-strings` | off | Strip whitespace from every string column, which otherwise silently breaks equality filters |
| `--lat-column`, `--lon-column` | auto-detected | Coordinate columns of a CSV of points |
| `--csv-sample-size` | -1 (whole file) | Rows sampled for CSV type inference |
| `--expect-features` | none | Fail unless exactly this many features are written, to catch a truncated source |
| `--no-spatial-order` | off | Keep source row order instead of Hilbert order (see [Output Format](#output-format)) |

`cng-datasets workflow` exposes the same options for its convert step, except
`--no-spatial-order`, `--target-crs` and `--csv-sample-size` (#311).

### H3 tiling from Python

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
- `id_column` (str, optional): Id column to carry into the hex output (auto-detected if not given)
- `plan_url` (str, optional): A chunk plan from `cng-datasets vector-plan`. With a plan, `process_chunk(k)` processes the plan's row range k instead of a fixed `chunk_size` slice (see [Chunk Planning](#chunk-planning))
- `read_credentials` (dict, optional): S3 credentials for reading
- `write_credentials` (dict, optional): S3 credentials for writing

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
small and very large polygons and a uniform fine resolution makes the biggest
features produce more cells than you want to store or than a hex pod can handle —
but dropping the
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
- The oversized-feature guard (`CNG_MAX_CELLS_PER_FEATURE`, default 1 billion
  cells) estimates cells at each feature's *own* native resolution, so a polygon
  that would exceed it at a fine resolution passes once a coarse bin is assigned.
- **Chunk-size caveat:** because larger features are hexed coarser, cells-per-feature
  still *rises* with the tier, so coarser/larger-feature tiers want a *smaller*
  `--chunk-size` (aim for a roughly constant few-million cells per chunk, not a
  constant row count). `cng-datasets workflow` plans chunks by estimated cells by default (`--cells-per-chunk`), which does this for you.
- A single very large feature no longer needs a coarse bin just to be hexed at
  all: since issue #125 every large polygon is cut into pieces before the
  polyfill (see below).

## Large and complex polygons

Before the polyfill, a polygon part that is large or complex is cut into pieces,
and H3 fills each piece separately (issue #125). The pieces come from halving the
part's bounding box in both directions, again and again, until each piece has at
most ~1 million cells and its cells × vertices stays under 100 million. H3 tests
every candidate cell centre against every edge, so the second limit is what keeps
a long, detailed coastline from taking hours: a 200,000-vertex polygon took 565 s
to fill whole at res 7.

The output is unchanged. A cell belongs to the fill exactly when its centre is
inside the polygon, so the pieces' fills add up to the fill of the whole part,
with no gaps or overlaps. Each piece's cells are a separate list in Pass 1, so no
single list gets near the 2 GB limit on one parquet value, which used to cap a
feature at ~134 million cells.

Each polygon part is passed to H3 as WKB (`h3_polygon_wkb_to_cells`), not as WKT text.
Multipolygons are split into parts first.

Not cut: line features, invalid polygons (clipping one can fail) and parts too
small to need it. A large part that crosses the antimeridian is first cut at
180° into two ordinary parts.

## Output Format

Output is partitioned by h0 (continent-scale) H3 cells:

```
s3://bucket/<dataset>/
├── chunks/chunk_000000.parquet   # written by each hex pod, removed by repartition
└── hex/
    ├── h0=<cell>/data_0.parquet  # written by repartition
    └── ...
```

Each hex file contains:
- The feature id (`_cng_fid` unless the source has its own id column)
- `h{res}`: H3 cell ID at the target resolution, e.g. `h10`
- One column per parent resolution, e.g. `h9`, `h8`, `h0`

Join back to the GeoParquet on the id for feature attributes. Rows are in spatial
(Hilbert) order and `_cng_fid` is numbered in that order, so **`_cng_fid` values
change when a dataset is rebuilt**. Pass `--no-spatial-order` to `cng-convert-to-parquet`
(or `spatial_order=False` to `convert_to_parquet`) to keep source order.

## Examples

See the following directories for complete examples:
- `catalog/wdpa/` - Large-scale protected areas processing
- `catalog/pad-us/` - Multi-layer geodatabase, one workflow per layer
- `catalog/mappinginequality/` - Vector polygon processing
