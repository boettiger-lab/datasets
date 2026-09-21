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

Detection targets an H3 edge length of ~3x the source pixel width (about 9 source pixels per
cell):

| Pixel Size | Detected H3 | Use Case |
|------------|-------------|----------|
| 0.5-2m | h13-h14 | High-res imagery |
| 7-25m | h10-h11 | Sentinel/aerial |
| 30-300m | h7-h10 | Landsat/regional |
| 1-12km | h4-h6 | Climate/global |

The processor provides helpful feedback when you choose a resolution different from the detected one:
- **Finer resolution**: "Using h12 instead of detected h10 - will create more cells"
- **Coarser resolution**: "Using h8 instead of detected h10 - will aggregate more pixels"

### h8 is the catalog join key

The `3x edge` target agrees with the catalog convention (roughly one cell per pixel) at fine
pixels and diverges as pixels coarsen — a ~1 km global raster detects **h6**, two levels below
h8. **A dataset whose finest resolution is coarser than h8 carries no `h8` column at all**, so it
cannot be joined against the rest of the catalog on the universal join key.

A finer target only carries h8 if h8 is among the parent resolutions, and the raster default is
`--parent-resolutions 0` — so an h10 build emits h10 and h0, and nothing to join on either.

`raster` warns in both cases, naming the consequence and the flag that fixes it. Pass
`--h3-resolution 8` for a coarse global product, or add `8` to `--parent-resolutions` for a finer
one, whenever a joinable output matters (issue #182):

```bash
cng-datasets raster --input chelsa-bio1.tif ... \
    --h3-resolution 8 --parent-resolutions "7,6,0"

cng-datasets raster --input nlcd-2021.tif ... \
    --resolution 10 --parent-resolutions "9,8,0"
```

## Parameters

### RasterProcessor

- `input_path` (str): Path to input raster file (supports /vsis3/ URLs)
- `output_cog_path` (str, optional): Path to output COG
- `output_parquet_path` (str, optional): Path to output parquet directory
- `h3_resolution` (int, optional): H3 resolution (None for auto-detect)
- `parent_resolutions` (list[int]): Parent resolutions for aggregation (default: [0])
- `h0_index` (int, optional): Process specific h0 region (0-121)
- `value_column` (str): Name for raster value column (default: "value")
- `nodata_value` (float or comma-separated string, optional): NoData value(s) to exclude. A
  categorical product often carries several fill codes in one band (LANDFIRE: `-9999`
  Fill-NoData, `-1111` Fill-Not-Mapped, `32767` internal), and a GDAL band can declare only
  one — see [Several fill codes in one band](#several-fill-codes-in-one-band).
- `compression` (str): COG compression method (default: "zstd")
- `blocksize` (int): COG tile size (default: 512)
- `resampling` (str): Resampling method for COG (default: "nearest")
- `method` (str): Raster→H3 algorithm (default: `"exact-extract"`). One of `exact-extract` or `warp-centroid` — see [Aggregation methods](#aggregation-methods).
- `hex_resampling` (str): Reducer for aggregating source pixels into each H3 cell (default: "mean"). Valid values depend on `method`: with `exact-extract`, one of `sum`, `mean`, `mode`; with `warp-centroid`, any GDAL `resampleAlg` (`average`, `sum`, `mode`, `near`, `bilinear`, `cubic`, ...).

### Several fill codes in one band

exactextract honours a single declared band NoData, so extra fill codes have to be mapped
onto the primary one before aggregation. For an **integer** source this costs nothing: the
mapping is expressed as a VRT lookup table and applied on read, so no pixels are written.

```
Collapsing fill codes [-9999.0, -1111.0, 32767.0] → -9999.0 for hex aggregation...
✓ Collapsed as a VRT lookup table, no raster written: /tmp/cng_collapsed_798466992877.vrt
```

This used to stage an **uncompressed copy of the whole source** instead. That copy is
`grid pixels x bytes per pixel` — independent of the source's compression, and independent
of the chunk the pod is working, so every pod wrote the same full-grid file. For the
LANDFIRE CONUS grid that is 34 GB per pod against the `ephemeral-storage: 40Gi` the
generator emits, which evicted every pod on the largest layers of a tranche (issue #209).
An ephemeral eviction surfaces as exit 137, byte-identical to an OOM kill in
`kubectl get pods`, so it is easy to misdiagnose as memory.

A **float** source cannot use a lookup table — a table interpolates between its entries
rather than substituting values — so it falls back to a materialised copy, which is now
compressed with the predictor matching the band type. Sources that cannot be expressed
exactly are refused rather than approximated.

In a generated `raster-workflow` the preprocess-cog step collapses the codes once, and the
hex pods are handed only the primary. That step is skipped when the source is already a
single-band COG needing no clip — which is precisely when the hex pods receive the full
list and do the work themselves, so the path above is the normal one for a well-formed
source.

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

### Where the results are accumulated

Each worker writes its own chunk straight to a parquet part and returns the **path**; the
final partition is one DuckDB statement over those parts. The parent therefore holds
nothing proportional to the cell count.

It used to collect every worker's rows as a pandas frame, hold them all in a list, and
`pd.concat` them — which allocates the result while the inputs are still referenced, so the
process peaked at roughly twice the accumulated size at exactly its largest moment.

Nor does a frame have to be built in the first place. exactextract is C++ and can serialise
through GDAL, so where OGR can write Parquet the worker has it write the file directly and
DuckDB normalises that into the part. The runtime image ships that driver; most
distribution GDALs do not, so the pandas writer remains the fallback and which one runs is
**feature-detected**. `CNG_HEX_GDAL_WRITER=0` forces the fallback, and the two are asserted
to produce identical rows.

### Cost follows the raster, not the cell

Within a chunk, only the cells the source can actually reach are enumerated. The chunk's
hierarchy is descended a level at a time: a subtree whose footprint misses the source —
widened by a margin, because an H3 child can protrude past its parent — is dropped whole,
and a subtree that lies entirely inside the source is taken whole without descending. Work
is therefore proportional to the source's *perimeter* rather than its area.

This matters most for small rasters, which previously cost exactly what a continental one
did. A 196 x 169 pixel raster in California enumerated all 5,764,801 res-8 descendants of
its h0 — a ~196x overshoot that made a fan-out over many small rasters impractical
(issue #215). Each chunk now reports what it kept:

```
✓ 29,465 of 5,764,801 cells reach the source (0.5% of the chunk)
```

That figure is also the one that explains the pod's peak memory and runtime, and it is
printed before the aggregation rather than inferred afterwards.

The restriction never removes a cell the raster touches: the margin is checked against
exhaustive enumeration of every child in the test suite, including the cells that wrap the
antimeridian, whose true footprint is two longitude intervals rather than one. Set
`CNG_HEX_PRUNE_CELLS=0` to enumerate every descendant instead, which is useful only for
comparing a suspect run like for like.

### Regional rasters: restrict the fan-out with `--h0-subset` / `--h0-cells`

A generated hex job runs one completion per h0 cell, 122 in all. A regional source
overlaps only a few of them, and every other pod localizes the whole COG, finds no overlap
and exits — CONUS occupies **6** of the 122 cells, so 116 pods (95%) start only to do
nothing, each first pulling a multi-GB COG.

Pass the cells the source covers:

```bash
cng-datasets raster-workflow \
  --dataset landfire-2024-cbd \
  --source-url s3://public-landfire/landfire-2024-cbd/landfire-2024-cbd-cog.tif \
  --bucket public-landfire --namespace geo-workflows \
  --h3-resolution 10 --parent-resolutions "9,8,0" --value-column cbd \
  --h0-subset "12,14,20,50,71,78"        # CONUS
```

The hex job then carries `completions: 6`, and the completion index selects from the list:

```yaml
completions: 6
...
H0S=(12 14 20 50 71 78)
H0=${H0S[$JOB_COMPLETION_INDEX]}
cng-datasets raster ... --h0-index ${H0} ...
```

The list is sorted and de-duplicated, so a completion index maps to the same cell across
regenerations. Values outside 0-121 are rejected, and a subset naming all 122 is the default
fan-out. Omit the flag for a global source.

The restriction also applies to a **serial** run — `cng-datasets raster` with neither
`--h0-index` nor `--chunk-index`, which processes the regions in one process. It used to
apply only to the chunked fan-out, so on the serial path the flag was parsed, validated,
echoed and then ignored, and the run worked through the whole grid anyway
([issue #215](https://github.com/boettiger-lab/datasets/issues/215)).

#### Positions are not H3 base cell numbers

`--h0-index` and `--h0-subset` take **positions** — values of the `i` column in the h0 grid
(`s3://public-grids/hex/h0-valid.parquet`). That column is an arbitrary permutation of the
122 H3 base cells, so position 12 is base cell 9, and exactly one of the 122 positions
coincides with its own base cell. The CONUS set above is these six:

| `--h0-subset` position | cell id | H3 base cell |
|---:|---|---:|
| 12 | 576812596024311807 | 9 |
| 14 | 577692205326532607 | 34 |
| 20 | 577164439745200127 | 19 |
| 50 | 577199624117288959 | 20 |
| 71 | 577762574070710271 | 36 |
| 78 | 577234808489377791 | 21 |

Note that **20 appears in both columns meaning different cells**. Both numberings run
0-121, so a base-cell list passed to `--h0-subset` is always in range, never errors, and
silently builds a different part of the world — the job succeeds and writes the expected
number of partitions ([issue #213](https://github.com/boettiger-lab/datasets/issues/213)).

If your list came from the H3 library — which is the obvious way to compute which cells a
raster covers — pass it to `--h0-cells` instead and it is converted for you:

```bash
cng-datasets raster-workflow ... --h0-cells "9,19,20,21,34,36"
# ✓ H3 base cells [9, 19, 20, 21, 34, 36] → h0 grid positions [12, 14, 20, 50, 71, 78]
```

The two flags are the same restriction in two numberings, so passing both is refused.
Either way, each pod logs what its position resolved to in its first lines:

```
Processing h0 grid position 50...
  h0 cell: 577199624117288959 (h3 8029fffffffffff, H3 base cell 20)
```

To derive the mapping yourself, read the grid's `i` column — **not** its row order, which
sorts by cell id and therefore by base cell, giving position == base cell for every row and
a wrong answer for all but one:

```sql
SELECT i AS position, h0, h3_get_base_cell_number(h0::ubigint) AS base_cell
FROM read_parquet('https://s3-west.nrp-nautilus.io/public-grids/hex/h0-valid.parquet')
ORDER BY i;
```

To find the cells for a bounding box, intersect it with the same grid. Inferring the set
from the source footprint at generation time is
[issue #191](https://github.com/boettiger-lab/datasets/issues/191)'s option 2 and is not
implemented — the subset is explicit.

### Workers, and why the default is small

The hex step aggregates in parallel worker processes. Peak memory is roughly
`workers x bytes-per-cell x cells-per-chunk`, so the worker count is a **memory** setting
as much as a CPU one — measured on one LANDFIRE res-10 layer, unchanged in every other
respect:

| `CNG_HEX_WORKERS` | peak RSS | outcome |
|---:|---:|---|
| 48–64 | 190.5 GiB | no slice completed in 3 h 40 m |
| **8** | **~37 GiB** | all six slices complete, no failures |

The default is the pod's CPU quota, read from cgroup v2 `cpu.max` or the v1 equivalent.
When that cannot be read the default is **8**, not the host's core count: a container whose
`/sys/fs/cgroup` is the host root reports `max` even though the pod *is* CPU limited, and
taking the node's cores there meant 256 workers against a `cpu: 8` limit — a 32x
oversubscription of a shared node, and a peak the manifest never asked for. It is also not
stable, since two pods of the same job can land on nodes with different core counts.

The two failure modes are not symmetric: too few workers is slower, too many is an OOM kill
after hours of un-checkpointed work. Set `CNG_HEX_WORKERS` to pin it; generated manifests
already do.

### Everything in the pod is sized from the pod

Three separate things in a hex pod default to a share of the **host**, which inside a
container is neither the pod's limit nor anything the manifest asked for:

| | default | now |
|---|---|---|
| worker processes | `os.cpu_count()` — the node's cores | the cgroup CPU quota, else 8 |
| DuckDB's buffer manager | 80% of host RAM | `DUCKDB_MEMORY_LIMIT`, 85% of the pod |
| GDAL's block cache, **per worker process** | 5% of host RAM (12.6 GiB on a 251 GiB node) | `GDAL_CACHEMAX=512` MB each |

The last is the one that scales with the fan-out, because the block cache is per *process*
and a hex pod runs many. Generated manifests set all three, so a pod's memory profile
follows from the manifest rather than from whichever node it landed on.

Bounding DuckDB also changes its behaviour under pressure from *fail* to *spill*: with a
limit it writes to `temp_directory` instead of growing until the cgroup kills it.

### What built this dataset

Every published parquet — the hex partitions, the merged `data_0.parquet`, the vector
repartition output and the GeoParquet — records the version that produced it, in the
file's own key-value metadata rather than a sidecar, so it survives copies and syncs:

```sql
SELECT key, value
FROM parquet_kv_metadata('s3://bucket/dataset/hex/h0=*/data_0.parquet');
-- cng_datasets_version  0.8.0
-- built_at              2026-09-21T23:06:45Z
-- gdal_version          3.13.0
-- duckdb_version        1.5.4
-- image                 ghcr.io/boettiger-lab/datasets:0.8.0
```

`image` appears when the environment sets `CNG_IMAGE`; its absence means a local run.

This exists because generated manifests pin `ghcr.io/boettiger-lab/datasets:latest`,
which is rebuilt on **every push to `main`** — so the code that ran was not necessarily a
released version, and nothing recorded which. When a build turns out to have been wrong,
as with the multi-band mislabel in
[#214](https://github.com/boettiger-lab/datasets/issues/214), the question "which datasets
are affected?" otherwise has no answer but correlating S3 timestamps against git history.

**A dataset with no stamp predates 0.8.0** and should be checked against the correctness
fixes in 0.7.0 — the multi-band read (#214) and the h0 position/base-cell confusion
(#213, #218).

The library versions are recorded because two of this project's sharper problems were
environment-dependent rather than code-dependent: a GDAL without
`WarpOptions(cutlineWKT=)` (#197), and one without OGR's Parquet driver.

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

Each parquet file contains one row per native H3 cell:
- `h3_cell`: H3 cell ID at specified resolution
- `value`: Aggregated raster value using area-weighted aggregation (customizable column name)
- Parent H3 cells if `parent_resolutions` specified
- Excludes nodata values if specified

Aggregation uses exact-area weighting (via `exactextract`) to account for partially-covered H3 cells, ensuring mass-conserving aggregation across the raster boundary.

(aggregation-methods)=
## Aggregation methods

Two raster→H3 algorithms are available via the `method` argument (CLI: `--method`).

| | `exact-extract` (default) | `warp-centroid` |
|---|---|---|
| How | Polyfill each h0 cell to its H3 children, then `exactextract` the area-weighted overlap of source pixels per cell. | `gdal.Warp` the source to a grid at the H3 edge pitch, then assign each warped pixel to a cell by its centroid. |
| Schema | **One row per H3 cell.** | **One row per warped pixel** — consumers must `GROUP BY h<res>`. |
| Mass-conserving | Always (by construction). | Only when the hex pitch is finer than the source pixel pitch (see [#84](https://github.com/boettiger-lab/datasets/issues/84)). |
| `hex_resampling` vocabulary | `sum`, `mean`, `mode`. | Any GDAL `resampleAlg` (`average`/`mean`, `sum`, `mode`, `near`/`nearest`, `bilinear`, `cubic`, ...). |
| Cost | Higher memory and wall time at fine resolutions (exact per-cell coverage). | Fast and low-memory. |
| Antimeridian | Handled. | Not antimeridian-correct by design (planar cutline). |
| Requires | Any supported GDAL. | A GDAL whose Python bindings accept `WarpOptions(cutlineWKT=...)` — each h0 is warped clipped to its own boundary. Absent from GDAL 3.8.4 (what Ubuntu noble ships); present in the project image. |

`warp-centroid` checks for that GDAL capability when the processor is constructed, before any raster is opened or localized, and raises naming the requirement. It does **not** fall back to `exact-extract`: the two produce different output (one row per cell versus one row per warped pixel), so the substitution is a decision for the caller, not a silent downgrade.

**Use `exact-extract` (the default) for stock/quantity rasters** (population, carbon) where mass conservation matters. When the two methods are both valid (hex pitch ≤ source pitch) they agree closely, and `exact-extract` is never worse — so reach for `warp-centroid` only when its speed/memory advantage is needed at scale.

```python
# Opt into the fast, low-memory path (note: emits one row per warped pixel)
processor = RasterProcessor(
    input_path="s3://bucket/global.tif",
    output_parquet_path="s3://bucket/global/hex/",
    h3_resolution=10,
    method="warp-centroid",
    hex_resampling="average",  # "mean" is accepted as an alias
)
```

## Examples

See the following directories for complete examples:
- `wetlands/glwd/` - Raster to H3 conversion with global h0 processing
- `iucn/` - Species range maps raster processing
- `ncp/` - Nature contributions to people raster data
