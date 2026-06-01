# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- Raster → H3 pipeline now produces one row per native H3 cell with mass-conserving area-weighted aggregation, fixing ~50% mass loss observed on `ghs-pop-2020` and `irrecoverable-carbon-2024` (#84). Implementation uses `exactextract` for precise pixel-to-cell overlap accounting.

### Breaking
- The default raster→H3 reducer changed from `average` to `mean`, and the default algorithm is now the mass-conserving `exact-extract` path (`--method exact-extract`), where `--hex-resampling` accepts only `sum`, `mean`, or `mode`. Migration: replace `average` with `mean` in existing scripts. Categorical rasters still use `mode`; use `sum` for count/stock data. Rasters processed before this change have undercounted totals and should be reprocessed. The previous GDAL-Warp resampling vocabulary is still available via `--method warp-centroid` (see Added).

### Added
- **`--method` flag for raster→H3 aggregation** (#86): `exact-extract` (default) is the mass-conserving, one-row-per-cell area-weighted path; `warp-centroid` restores the older `gdal.Warp`→XYZ→centroid path as an opt-in fast/low-memory alternative. `warp-centroid` emits one row per warped pixel (consumers `GROUP BY h<res>`), accepts any GDAL `resampleAlg` for `--hex-resampling` (with `mean`/`nearest` accepted as aliases for GDAL's `average`/`near`), and is mass-conserving only when the hex pitch is finer than the source pixel pitch. See [Aggregation methods](#aggregation-methods).
- **Multi-cluster configuration** (`ClusterConfig`): all NRP-specific values in generated Kubernetes job specs are now overridable via CLI flags (`--s3-endpoint`, `--s3-public-endpoint`, `--s3-secret-name`, `--rclone-secret-name`, `--rclone-remote`, `--priority-class`, `--node-affinity`) and matching keyword arguments on `generate_dataset_workflow` / `generate_raster_workflow`. All defaults equal the current hardcoded NRP values — existing commands produce identical YAML without changes.
- **YAML cluster profiles** (`--profile`): save cluster settings in a YAML file and reuse them across datasets. Profile resolution: explicit file path → `~/.config/cng-datasets/profiles/<name>.yaml` → built-in package profiles. The built-in `nrp` profile captures the NRP Nautilus defaults. Explicit CLI flags override individual profile values.
- **Per-bucket credential scoping**: `--s3-secret-name` lets each workflow reference a dedicated Kubernetes secret, limiting job S3 access to a single bucket.
- **Configurable node affinity**: `--node-affinity none` omits the NRP-specific NFD GPU-avoidance rule for clusters that don't use Node Feature Discovery labels.
- **Configurable priority class**: `--priority-class ""` omits `priorityClassName` for clusters without NRP/Armada priority classes.
- `load_profile()` and `cluster_config_from_args()` exported from `cng_datasets.k8s` for programmatic use.
- Eliminated duplicated S3 env-var blocks across all six job generators via shared `_s3_env_vars()` and `_apply_scheduling()` helpers.

## [0.1.1] - 2026-01-01

### Added
- Comprehensive Sphinx documentation with GitHub Pages deployment
- Consolidated README with complete feature overview
- API reference documentation for all modules
- User guides for vector, raster, and Kubernetes workflows

### Changed
- Merged README_PACKAGE.md, RASTER_IMPLEMENTATION.md, and WORKFLOW_GUIDE.md into single README
- Updated documentation structure for better organization
- Improved installation and quickstart guides

### Removed
- README_PACKAGE.md (merged into README.md)
- RASTER_IMPLEMENTATION.md (moved to docs)
- WORKFLOW_GUIDE.md (moved to docs)

## [0.1.0] - 2025-12-01

### Added
- Initial release
- Vector processing with H3 indexing
- Raster processing with COG creation
- Kubernetes job generation and management
- S3 storage management with rclone
- Command-line interface
- Docker support
- Two-pass processing for memory efficiency
- Auto-detection of optimal H3 resolution for rasters
- Parent resolution support for hierarchical aggregation

### Features
- `H3VectorProcessor` class for vector tiling
- `RasterProcessor` class for raster to H3 conversion
- `K8sJobManager` for Kubernetes workflow generation
- `RcloneSync` for multi-provider storage sync
- PVC-based K8s orchestration
- Chunk-based processing for large datasets
- h0 partitioning for efficient queries

[0.1.1]: https://github.com/boettiger-lab/datasets/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/boettiger-lab/datasets/releases/tag/v0.1.0
