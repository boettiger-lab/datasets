# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
