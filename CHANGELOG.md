Changelog
=========

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.1.0]: https://github.com/boettiger-lab/datasets/releases/tag/v0.1.0
