Changelog
=========

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2026-09-09

### Added
- `--simplify-tolerance` on `cng-convert-to-parquet` and `workflow`: `ST_SimplifyPreserveTopology` applied *after* reprojection, so the tolerance is in target-CRS units (degrees for EPSG:4326; ~0.0001 ≈ 10 m). Large high-vertex sources such as national FEMA NFHL at full FIRM precision are far finer than the H3/tile target needs and overflowed the PMTiles ephemeral cap, which previously forced a hand-rolled DuckDB simplify job. PreserveTopology avoids collapsing polygons to invalid or empty geometry (#132)
- First-class CSV lat/lon point input on `cng-convert-to-parquet`. `.csv`/`.tsv` sources now route to a dedicated path: `read_csv_auto` + `ST_Point(lon, lat)` in EPSG:4326, wrapped in `ST_MakeValid`, carrying every source column as an attribute and adding the synthetic `_cng_fid`. Columns are resolved from `--lat-column`/`--lon-column` or auto-detected from common names (`Latitude`/`lat`/`y`, `Longitude`/`lon`/`lng`/`x`, `decimalLatitude`), so a standard CSV needs no flags at all. NULL coordinates yield NULL geometry rather than crashing, and out-of-range values warn (the usual sign of swapped columns). CSV with lat/lon columns is the common distribution format for point observations — species occurrences, sensor stations — and previously required a bespoke DuckDB job (#78)
- `--trim-strings` on `cng-convert-to-parquet` and `workflow`: strips spaces, tabs, CRs and LFs from the ends of every VARCHAR column during conversion. Upstream sources sometimes carry stray whitespace in categorical fields — WDPA stores the fully-no-take category as `'All '` in the source GDB itself — so every consumer's `WHERE NO_TAKE = 'All'` silently returns zero rows. Emitted as a DuckDB star-`REPLACE`, so column order, non-string columns, geometry handling and the `_cng_fid` wrapper are untouched, and interior whitespace is left alone since it is usually meaningful. **Off by default**: no attribute value is mutated unless asked (#180)
- `cng-datasets pmtiles-columns <source>` plus `cng_datasets/vector/pmtiles.py`: read tile-accurate `table:columns` from a PMTiles footer. tippecanoe writes only a subset of source columns into the vector tiles, so the PMTiles schema differs from the source Parquet schema, and STAC collections here are authored by hand — the only way to know which fields survived was to byte-range the footer with a throwaway script. Parses the PMTiles v3 header over a local path or an `http(s)://`/`s3://` byte range, fetching only the header and metadata blob and never the multi-GB tile body (#140)
- `--window-reads {auto,always,never}` on `cng-datasets raster`: a chunk reads only its own window of the source COG instead of localizing the whole file. Full localization is a fixed cost *per pod*, so total transfer scaled with the fan-out rather than with the data — tolerable across 122 h0 pods, ruinous across the thousands that sub-h0 chunking creates (a 2 GB COG at h2 would move ~12 TB). A COG is internally tiled, so GDAL fetches only the tiles the window touches. Measured on a 4 MiB tiled fixture: at h2 chunking, 14.5 MiB windowed against 196 MiB of whole-file copies (13.5x); at h3, 20 MiB against 1.37 GiB (68.6x). The windowed total grows slowly with depth (4 → 12 → 14.5 → 20 MiB) while the copy total grows linearly with the chunk count, which is the point — transfer becomes proportional to the data, not to how finely it was chunked. `auto` windows when `--chunk-resolution > 0` *and* the source is remote: a window over a local file transfers nothing and would only decode and re-encode the region (#173, lever C)
- `--chunk-resolution N` / `--chunk-index K` on `cng-datasets raster`, plus a `cng-datasets merge-chunks` step. The unit of work was fixed at one h0 base cell — the only chunk knob — so peak memory tracked the densest h0's native-cell count regardless of how small the raster's own footprint was: ~282M cells and ~140 GiB measured at res 10, which is why large builds needed an oversized flat memory request that then struggled to schedule. `--chunk-resolution` makes the unit an h0's res-N descendant instead, cutting the largest chunk's cell count ~7x per level (measured in #173: h1 4.6 GiB, h2 0.68 GiB, against ~32 GiB for a whole h0). H3 nests exactly, so descendants tile their parent with no seams to dedup and no gaps — unlike bbox tiling, where a native cell straddling an edge would be split or double-counted. Sub-chunks write `part-{cell}.parquet` and `merge-chunks` consolidates each h0 back to the single published `h0={cell}/data_0.parquet`, streamed per partition, so the layout does not depend on how finely the build was chunked. `--h0-subset` is accepted here too, applied at chunk granularity. Default behaviour is unchanged: at `--chunk-resolution 0` the index-to-cell mapping, the geometry source and the output path are all exactly as before (#173)
- `--hex-workers`, `--hex-chunk-size` and `--hex-cpu` on `raster-workflow`. `CNG_HEX_WORKERS` — the number of chunks a hex pod holds in flight — is the one lever that reliably brings a hex pod's peak memory down, but nothing emitted it, so a generated manifest could not set it and a hand-tuned one lost it on regeneration with nothing in the diff to explain the OOM that followed. Peak RSS is roughly workers × chunk size × bytes per cell; `--hex-memory` is not an equivalent lever, since a request large enough to matter makes the pod contend for scarce large-RAM nodes and converts a retryable OOM into an unschedulable pod. `--hex-cpu` also makes the previously hardcoded CPU request configurable. The generator now prints the resulting pod profile (`cpu 4, memory 32Gi, 4 workers × 100000 cells/chunk`) so an oversubscribed pod is visible before it is applied. Only `raster-workflow` gets these: the vector hex step runs no worker pool and never reads the variables, so emitting them there would be a knob that does nothing (#195)
- `--expect-features N` on `cng-convert-to-parquet` and `workflow`: the row count the conversion is expected to produce, known independently by the caller. The conversion exits non-zero when the written count differs, so a silently truncated source fails the job rather than flowing into the hex and PMTiles steps. Every conversion now also logs `Wrote N rows` unconditionally — previously the tool never reported a count anywhere, so a run that converted a third of its input produced a log byte-identical to a complete run and there was no signal, for a human or a pipeline, that most of the data was missing (#186)
- `--lat-column` / `--lon-column` on the `workflow` subcommand, passed through to the convert step. They existed on `cng-convert-to-parquet` but not on `workflow`, so a CSV point source could not be a `--source-url` for a generated pipeline at all — it had to be pre-converted in a separate job (#188)
- `--csv-sample-size` on `cng-convert-to-parquet`, for a CSV too large to scan in full (#188)
- `--h0-subset` on `raster-workflow`: the h0 base cells the source overlaps, e.g. `"12,14,20,50,71,78"` for CONUS. The hex job then runs one completion per listed cell instead of all 122, and the completion index selects from the list. A regional raster previously started 116 pods (95%) that each localized a multi-GB COG, found no overlap with their h0 and exited — the reason large regional recipes in the catalog were hand-edited away from generator output. The list is sorted and de-duplicated so an index maps to the same cell across regenerations; cells outside 0-121 are rejected, and a subset naming all 122 collapses to the default fan-out. Parallelism is capped at the completion count, and the generated script fails loudly if the completion index has no cell rather than silently processing the first one (#191)
- `--armada-priority-class` on `workflow` and `raster-workflow`: the Armada priority class was previously unreachable from the CLI, since `convert_workflow_to_armada` neither accepted nor forwarded one. Accepts a shorthand (`default`, `preemptible`, `high`) or a literal class name, and the generated-workflow summary now reports the class in effect (#183)

### Changed
- **Behaviour change (regenerated vector workflows):** hex completions are now right-sized to the chunk count. `_calculate_chunking` derived `chunk_size = ceil(total_rows / max_completions)`, which drove chunk size *down* to spread work across up to `max_completions` chunks — shredding a small dataset into ~200 tiny pods, each pulling the multi-GB image and claiming a namespace-quota slot for a few dozen features (5,697 features → chunk size 29 → 197 pods). Chunk size is now floored at 1000, so completions is `ceil(total_rows / 1000)` for small and medium datasets (5,697 → 6 pods) and only grows above the floor once the dataset would otherwise exceed `max_completions` (711,583 → chunk size 3558, completions 200). 1000 matches the effective chunk size the old formula already produced at the 200k-feature scale, so per-pod memory and time are unchanged for large builds. **Regenerating a small or medium workflow will emit a very different completion count** — that is the fix, but it changes the manifest (#144)
- The H3 child-protrusion margin is now a measured constant (`_H3_PROTRUSION_MARGIN = 0.25` of a chunk's latitude extent) rather than an arbitrary half-extent. Sampled across the globe at chunk resolutions 1-3 for descendants 1 to 4 levels down, the protrusion *converges* rather than compounding — 0.127 at one level, 0.150 by three, unchanged at four — so one depth-independent margin is sound and 0.25 carries a ~1.7x safety factor. Both facts are pinned by tests, so a future H3 release that widens the bound fails here rather than in a build's output. The two callers now take different sides of the trade deliberately: pruning a chunk keeps a looser 2x margin because a false positive costs one pod that exits in seconds, while a read window uses the measured bound directly because it pays for its margin in bytes on every chunk. Tightening it roughly halved windowed transfer at every chunk depth (#173)
- Generated raster hex jobs now pin `CNG_HEX_WORKERS` and `CNG_HEX_CHUNK_SIZE` explicitly, defaulting the worker count to one process per requested CPU. Previously neither was emitted and the pod sized its own pool from the cgroup CPU quota — which reads as unlimited whenever the container's `/sys/fs/cgroup` is the host root rather than its own namespace, so the count silently became the *node's* core count. One manifest therefore ran 48 workers on one node and 64 on another, on a pod requesting 4 CPUs, and its peak memory followed: measured 145-185 GiB against a 192 GiB limit, versus 115-144 GiB for the hand-written equivalent that set the variable. A hex pod's memory profile is now a property of its manifest rather than of the node it lands on. Expect slower but bounded hex pods at the default; pass `--hex-workers` to trade back. When the quota genuinely cannot be read, `cng-datasets raster` now says so and names the fallback instead of proceeding silently (#195)
- **Breaking (Armada backend):** converted jobs now default to the non-preemptible `armada-default` instead of `armada-preemptible`, and a k8s `opportunistic` priority class no longer maps onto `armada-preemptible`. An opportunistic k8s pod is preempted but recreated by its Job controller; a preempted Armada job is not rescheduled at all, and the Job-level retry settings do not survive conversion — so the old default turned multi-hour work into preemptible work with no retry and no rescheduling. Pass `--armada-priority-class preemptible` to opt back in, which is the right default once units are small enough that losing one is cheap (#183)

### Fixed
- **Behaviour change (may fail builds that previously passed):** `repartition_by_h0` now hard-fails when the count of distinct hexed features is lower than the count of source features with non-null geometry, reporting both. The k8s hex Job covers only `max-completions × chunk-size` features, so a source larger than that cap — when the generator could not count it up front — was **silently truncated**, and every presence-based downstream check still passed. A build that was quietly losing features will now stop. Set `CNG_SKIP_COMPLETENESS_CHECK=1` to bypass it for sources with known-degenerate geometries (#170)
- A chunk whose features all have null geometry no longer aborts the whole indexed Job. It produced a zero-row intermediate, so Pass 2 never created the local file and the final `COPY` failed with "No files found". Such a chunk now writes an empty partition carrying the correct output schema and returns cleanly — so expect empty partition files where a build previously died (#169)
- A non-geometry attribute column named `GEOMETRY`, `SHAPE` or `GEOM` no longer shadows the real geometry column. A `DOUBLE` carried over from a source DBF aborted the hex job with `ST_GeometryType(DOUBLE)`. `_find_geometry_column` now resolves by DuckDB type first — a `GEOMETRY`-typed column wins regardless of name — and falls back to name matching only when no typed geometry column exists (#171)
- **Behaviour change (a hang becomes a clear failure):** a polygon whose ring crosses the antimeridian now fails immediately instead of hanging for hours. Such a ring polyfills its full *planar* cartesian span — a near-global cell set — so `h3_polygon_wkt_to_cells` enumerated billions of cells and stalled Pass 1 CPU-bound at low RAM, taking the whole indexed Job with it. The #107 oversized-feature guard missed it because `ST_Area_Spheroid` measures the geodesic short-way region, so a narrow dateline strip reads as tiny (or NaN for a self-wrapping ring). The guard now estimates cells from planar area for features whose longitude span exceeds 180°, matching what the polyfill actually walks, and keeps the accurate geodesic estimate below that. The error names the antimeridian and points to splitting at ±180° upstream; a clean multipolygon of islands either side of the dateline has small planar area and still hexes normally (#167)
- Pruning a chunk against its own H3 boundary no longer drops native cells. H3's hierarchy is only *approximately* containing — a child cell can protrude beyond its parent's boundary polygon — so the overlap test that skips a chunk not covering the raster excluded chunks whose children genuinely did overlap. The result was a silently short output: exit 0, a clean log, and a cell count that looked plausible. Caught by the new sub-chunk correctness gate, which asserts sub-chunked output is row-for-row identical to the h0 baseline. The overlap test now takes a margin of roughly one cell edge, scaled by 1/cos(latitude) so it does not under-cover near the poles; the asymmetry is deliberate, since a false positive costs one pod that exits in seconds and a false negative costs data (#173)
- `--method warp-centroid` now reports its GDAL requirement instead of crashing partway through a build. The path clips every h0's warp with `WarpOptions(cutlineWKT=...)`, which GDAL 3.8.4 (Ubuntu noble) does not accept, so the method failed with a bare `TypeError: WarpOptions() got an unexpected keyword argument 'cutlineWKT'` from inside the warp — after the pod had already localized the COG, and reading like a bug in the tool rather than a missing dependency. Nothing declared the requirement. It is now detected by feature (not by version number, since bindings can lag the library) when the processor is constructed, before any raster is opened, and raises naming the requirement and the remedy. It deliberately does not fall back to `exact-extract`: that is a different aggregation, not a slower one (#173)
- The raster hex step no longer materialises a boundary WKT string for every H3 cell before starting work. `_native_cells_for_h0` fetched `(cell_id, boundary_wkt)` for all of an h0's children at once and handed both to the workers; the WKT was ~96% of that list (183 bytes per cell against 8 for the id alone), and materialising it was ~85% of the parent process's peak RSS. A cell boundary is a pure function of its id, so each worker now derives its own chunk's boundaries with the same `h3_cell_to_boundary_wkt`, and the parent carries a plain uint64 array. Measured on one h0: peak RSS for the cell enumeration falls from 18.3 GiB to 4.6 GiB at res 9 and from 4.0 GiB to 0.68 GiB at res 8 — a res-10 h0 is projected to fall from ~128 GiB to ~32 GiB, which is what makes the largest builds fit in a normal pod. Output is byte-identical: verified over a full h0 for `mean`, `mode` and `fractions` (#173, phase 1)

- CSV point input now types columns from the whole file instead of DuckDB's default 20480-row sample. A numeric column that is entirely NULL within that head sample was typed `VARCHAR` even when cleanly numeric over the whole file, so `SUM()` on it failed outright until the consumer added a cast — fatality and evacuation *counts* published as strings. This bites any CSV ordered by time with columns introduced in a later reporting era, which is the normal shape for incident and observation records. All three `read_csv_auto` calls (the DESCRIBE, the coordinate range check and the write) now take the same `sample_size`, so they agree on the schema; `--csv-sample-size` restores a bounded sample for a file too large to scan (#188)
- A source CRS with no EPSG code and no authority string now drives reprojection instead of being discarded. `detect_crs` returned `None` for such a CRS, and `build_read_reproject_query` reads `None` as "no reprojection needed", so the geometries were written unchanged while the GeoParquet presented them as lon/lat — a MODIS sinusoidal source converted to projected metres tagged EPSG:4326, with exit code 0 and a single `Warning:` line. Every MODIS-derived product ships such a CRS. Detection now returns the CRS's full WKT definition, which `ST_Transform` accepts, so the existing reprojection path handles it unchanged. The three ways a source can lack a code are also reported distinctly: a definition without a code (reprojected), a dataset that declares no CRS (assumed to be in the target CRS, as before), and detection failing outright (still assumed, but the reason and the consequence are now printed at normal verbosity rather than only under `--verbose`) (#187)
- Every generated step manifest now carries `metadata.namespace`. Only `workflow.yaml`, `configmap.yaml` and `workflow-rbac.yaml` had it, so applying a step manifest directly — which the docs sanction for step-by-step control — targeted whatever namespace `kubectl` defaulted to: an RBAC error naming `default` rather than the missing field, or, on a permissive cluster, the job silently running in the wrong namespace. Filed against `raster-workflow`, but the vector generator's `convert`, `pmtiles`, `hex`, `repartition` and `setup-bucket` manifests had the same gap and are fixed with it (#190)
- `raster-workflow` no longer flattens a hierarchical `--dataset` in the S3 paths it writes. `--dataset a/b` published its hex to `s3://<bucket>/a-b/hex/` — the Kubernetes object name, which must flatten `/` — instead of `s3://<bucket>/a/b/hex/`, so a raster ingest landed outside its own dataset prefix and the STAC hex and COG hrefs pointed into different trees. The hex job now takes the S3 dataset path separately from the k8s name, as the vector generator already did, and the intermediate COG follows the same convention (`{dataset}-cog.tif`, mirroring the vector `{dataset}.parquet`). Flat dataset names are unaffected (#189)
- Converting a k8s Job to Armada now warns when Job-level retry settings (`backoffLimit`, `backoffLimitPerIndex`, `maxFailedIndexes`) are dropped, naming what is lost; conversion reads `spec.template.spec`, so these were silently discarded. Settings that grant no retries (a `0` value) are not reported, and the warning additionally flags the preemptible case, where a preemption loses the whole unit (#183)
- `raster` now warns when a build has no path to `h8`, the catalog's universal join key, naming the consequence: the output cannot be joined against the rest of the catalog. Two cases, both previously silent — the target resolution is coarser than h8 (`detect_optimal_h3_resolution` targets ~3x the source pixel edge, which agrees with the catalog at fine pixels but lands on h6 for a ~1 km global raster, so a caller who omitted `--h3-resolution` got a non-joinable dataset with only an informational log line), or the target is finer than h8 and h8 is not among `--parent-resolutions`, whose raster default is `0` alone. Resolution detection itself is unchanged (#182)
- Stale default Armada queue in `convert_workflow_to_armada` (`biodiversity` → `geo-workflows`); the workflow generators pass the namespace explicitly, so this affects direct API callers (#183)

## [0.3.1] - 2026-07-21

### Fixed
- `.zenodo.json` dropped an unresolvable NSF grant identifier that caused Zenodo DOI minting to fail on the 0.3.0 release; the archive/DOI can now be minted (#165)

## [0.3.0] - 2026-07-20

First release published to PyPI (`pip install cng-datasets`) via trusted publishing.

### Fixed
- Vector H3 hex now splits circumpolar / transmeridian polygons (longitude-bbox span > 180°) into sub-180° longitude bands before polyfill, so they fill their full band instead of collapsing to ~1 cell. H3 `polygon_to_cells` reads a >180° ring as the minimal-area (complement) side; a full −180..180 band (e.g. CCAMLR's Southern-Ocean RFB) previously produced ~1 cell instead of millions. Narrow (<180°-span) polygons keep the original fast path unchanged (#145)
- `workflow` now `shlex.quote`s each `--source-url` when interpolating it into the generated convert command, so URLs carrying `&` query strings (common for ArcGIS Hub / REST download endpoints) no longer break the `bash -c` step apart into background jobs (#147)

### Added
- `--resolution-by-area` for variable-resolution (size-stratified) H3 hex tiling: each feature is hexed at the native resolution its planar `ST_Area` maps to, so very large polygons use a coarse resolution while small ones keep fine edges — avoiding the Pass-2 OOM / 2 GB parquet-page limit without dropping the whole dataset to a coarse resolution. Output carries a uniform union schema (one column per resolution, finer columns null in coarser tiers) plus a `native_res` column, preserving flat equality joins. The #107 oversized-feature guardrail now estimates each feature's cells at its own native resolution, so large features back off automatically (#98)

## [0.2.0] - 2026-06-13

### Added
- `--method` flag for raster→H3 aggregation: mass-conserving `exact-extract` (default) and opt-in `warp-centroid` (#86)
- `max`/`min`/`mode` `--hex-resampling` reducers for peak/richness and categorical rasters (#96, #80)
- Multi-value `--nodata` with categorical-safe COG overviews (#108)
- LineString geometry support in H3 hex tiling via buffering (#69)
- Multi-cluster configuration (`ClusterConfig`) and reusable YAML cluster profiles (`--profile`) for non-NRP deployments (#57, #58)

### Changed
- **Breaking:** default raster→H3 reducer is now `mean` (was `average`) and the default algorithm is the mass-conserving `exact-extract` path; rasters built before this should be reprocessed (#84)
- Removed the `geoparquet-io` dependency — DuckDB 1.5 writes GeoParquet natively (#56)
- CI lint (`ruff`) is now blocking (#90, #94); agent/dev instructions are Docker-based (#97)
- `cng-convert-to-parquet` always creates a row-unique `_cng_fid` on both convert paths (#43)

### Fixed
- Mass-conserving area-weighted raster→H3 aggregation, fixing ~50% mass loss (#84)
- BLOB→`GEOMETRY` cast for MULTIPOINT/WKB sources on both convert paths, fixing null geometries in PMTiles (#61)
- Post-hex-build `UBIGINT` assertion on all `h{N≥1}` columns + intra-partition `_cng_fid` ordering for row-group pruning (#102, #103)
- Sub-H3-cell polygons retained via representative-point fallback (#104); antimeridian/polar boundary cells split correctly (#92)
- Deterministic, lazy PROJ configuration (`proj.db` MINOR ≥ 7) (#72, #91, #101); M/3D geometry flattened to 2D (#50, #51, #59)
- Non-COG source auto-detection and `/vsis3/` COG write (#66, #68); rclone-config mount in raster hex job (#99); `TIPPECANOE_MAX_THREADS` in the PMTiles job (#77)
- Bucket CORS exposes range-read headers (#35, #79, #87); network-free workflow-generation tests (#112)

## [0.1.1] - 2026-01-01

### Changed
- Made GDAL an optional dependency (install with `pip install -e ".[raster]"`)
- Tests requiring GDAL array support now skip gracefully when unavailable
- Updated documentation with GDAL installation instructions

### Fixed
- Test suite now passes in virtual environments without system GDAL
- All 50 core tests pass, 9 GDAL-dependent tests skip cleanly

## [0.1.0] - 2026-01-01

### Added
- **Raster Processing Pipeline**
  - Complete `RasterProcessor` class for COG creation and H3 tiling
  - Automatic H3 resolution detection from raster pixel size
  - Support for processing by h0 regions (memory-efficient global processing)
  - COG optimization for cloud rendering (titiler-compatible)
  - Parent resolution support for hierarchical aggregation
  - Configurable value columns and nodata handling
  - CLI commands for raster processing
  
- **Vector Processing (Existing)**
  - H3 hexagonal tiling for polygon and point datasets
  - Two-pass processing to avoid OOM with large datasets
  - Chunked processing with configurable batch sizes
  - Parent resolution support
  - Repartitioning by h0 cells for efficient querying
  - ID column auto-detection and handling
  
- **Kubernetes Integration**
  - Job generation for parallel processing
  - Indexed jobs for chunk-based workflows
  - Resource configuration (CPU, memory, parallelism)
  - Support for h0-based regional processing
  
- **Storage Management**
  - S3 bucket configuration and CORS setup
  - Rclone integration for multi-cloud syncing
  - Credential management
  
- **CLI Tools**
  - `cng-datasets vector` - Vector processing
  - `cng-datasets raster` - Raster processing
  - `cng-datasets k8s` - Kubernetes job generation
  - `cng-datasets storage` - Storage management
  - `cng-datasets workflow` - Complete dataset workflows
  
- **Documentation**
  - Comprehensive package README
  - Dataset-specific READMEs with examples
  - API documentation in docstrings
  - Example scripts and notebooks
  - Contributing guidelines
  
- **Testing**
  - Unit tests for vector processing
  - Unit tests for raster processing
  - Integration tests for S3 and H3 operations
  - Mock tests for external services
  - Test fixtures and utilities

### Changed
- Updated H3 edge length values to match official h3geo.org specification
- Improved resolution detection with informative user feedback
- Enhanced error messages and logging throughout

### Fixed
- H3 edge length accuracy (using official values)
- Resolution override behavior with helpful messages
- Memory efficiency for large polygon processing

[0.4.0]: https://github.com/boettiger-lab/datasets/releases/tag/v0.4.0
[0.3.1]: https://github.com/boettiger-lab/datasets/releases/tag/v0.3.1
[0.3.0]: https://github.com/boettiger-lab/datasets/releases/tag/v0.3.0
[0.1.0]: https://github.com/boettiger-lab/datasets/releases/tag/v0.1.0
