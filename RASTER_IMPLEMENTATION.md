# Raster Processing Implementation Summary

## Overview

Implemented comprehensive raster processing pipeline for the cng_datasets package, following the same design patterns as the vector processing side.

## Key Features Implemented

### 1. RasterProcessor Class (`cng_datasets/raster/cog.py`)

A comprehensive class for processing raster datasets into cloud-native formats:

#### Features:
- **Auto-detection of optimal H3 resolution** based on raster pixel size
- **Cloud-Optimized GeoTIFF (COG) creation** optimized for titiler rendering
- **H3-indexed parquet conversion** with h0 partitioning
- **Memory-efficient global processing** by h0 regions (0-121)
- **Parent resolution support** for hierarchical aggregation
- **NoData handling** to exclude invalid pixels
- **Configurable compression** and resampling methods
- **S3 credential management** via environment variables

#### Key Methods:
- `detect_optimal_h3_resolution()`: Auto-detect H3 zoom from pixel size
- `create_cog()`: Create Cloud-Optimized GeoTIFF with overviews
- `process_h0_region()`: Process single h0 region to H3 parquet
- `process_all_h0_regions()`: Process all 122 h0 regions

### 2. Updated Job Script (`wetlands/glwd/job.py`)

Refactored to use the new RasterProcessor class:

#### Features:
- Clean argument parsing with sensible defaults
- Auto-detection of H3 resolution (optional override)
- Parent resolution support via comma-separated string
- Configurable value column names
- NoData value filtering
- Memory/runtime profiling option
- Clear progress reporting

#### Usage:
```bash
python job.py \
  --i 42 \
  --zoom 8 \
  --parent-resolutions "9,8,0" \
  --input-url /vsis3/bucket/raster.tif \
  --output-url s3://bucket/output/ \
  --value-column wetland_class \
  --nodata 255 \
  --profile
```

### 3. CLI Integration (`cng_datasets/cli.py`)

Extended the command-line interface with comprehensive raster support:

#### New Options:
```bash
cng-datasets raster \
  --input wetlands.tif \
  --output-cog s3://bucket/wetlands-cog.tif \
  --output-parquet s3://bucket/wetlands/hex/ \
  --resolution 8 \
  --parent-resolutions "0" \
  --h0-index 42 \
  --value-column wetland_class \
  --nodata 255 \
  --compression zstd \
  --blocksize 512 \
  --resampling nearest
```

Supports:
- COG-only generation
- H3 parquet-only generation
- Combined COG + H3 processing
- Single h0 region or all regions
- Auto-detection of H3 resolution

### 4. Documentation Updates

#### Package README (`README_PACKAGE.md`):
- Comprehensive raster processing examples
- Auto-detection documentation
- Resolution mapping guide
- Kubernetes workflow examples
- Command-line usage examples

#### Wetlands README (`wetlands/glwd/README.md`):
- Complete workflow documentation
- Configuration options
- Output structure
- Kubernetes job details
- Usage examples

## Architecture Highlights

### Design Patterns (Consistent with Vector Processing):

1. **Two-stage processing**: 
   - Stage 1: Create COG (optional)
   - Stage 2: Convert to H3 parquet by h0 regions

2. **Memory-efficient global processing**:
   - Process each h0 region independently
   - Only loads relevant geographic extent
   - Suitable for parallel Kubernetes jobs

3. **Flexible configuration**:
   - Auto-detect or specify H3 resolution
   - Optional parent resolutions
   - Configurable column names and NoData values

4. **Cloud-native outputs**:
   - COGs optimized for titiler/cloud rendering
   - H3 parquet partitioned by h0 for efficient queries
   - ZSTD compression for optimal storage

### H3 Resolution Detection

Automatic detection based on pixel resolution:

| Pixel Size | Recommended H3 | Use Case |
|------------|---------------|----------|
| 0.5-2m | h14-h15 | High-res imagery |
| 7-25m | h12-h13 | Sentinel/aerial |
| 30-300m | h9-h10 | Landsat/regional |
| 1-12km | h7-h9 | Climate/global |

### COG Optimization

COGs are created with:
- Internal tiling (configurable blocksize)
- Overview pyramids for zoom levels
- Optimized compression (deflate/zstd/lzw)
- EPSG:4326 reprojection if needed
- Multi-threaded processing

### Kubernetes Integration

Designed for parallel processing:
- 122 indexed completions (one per h0 region)
- Configurable parallelism
- Independent failure handling
- Memory-efficient per-region processing

## Example Workflows

### Local Processing

```python
from cng_datasets.raster import RasterProcessor

# Process with auto-detection
processor = RasterProcessor(
    input_path="data.tif",
    output_cog_path="s3://bucket/data-cog.tif",
    output_parquet_path="s3://bucket/data/hex/",
    parent_resolutions=[8, 0],
)

processor.create_cog()
processor.process_all_h0_regions()
```

### Kubernetes Job

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
        command:
        - python
        - /app/job.py
        - --i
        - $(INDEX)
        - --input-url
        - /vsis3/bucket/data.tif
        - --output-url
        - s3://bucket/output/
```

## Benefits

1. **Consistency**: Matches vector processing interface and patterns
2. **Scalability**: Process global rasters via parallel h0 regions
3. **Flexibility**: Auto-detection with manual override options
4. **Cloud-native**: COGs for visualization, H3 for analysis
5. **Documentation**: Comprehensive examples and guides
6. **Testability**: Clean interfaces suitable for unit testing

## Files Modified/Created

1. `cng_datasets/raster/cog.py` - Complete implementation
2. `cng_datasets/raster/__init__.py` - Updated exports
3. `wetlands/glwd/job.py` - Refactored to use RasterProcessor
4. `cng_datasets/cli.py` - Extended with raster commands
5. `README_PACKAGE.md` - Added raster documentation
6. `wetlands/glwd/README.md` - Created workflow guide

## Next Steps

Recommended enhancements for future work:

1. **Testing**: Add unit tests for RasterProcessor class
2. **Validation**: Test with various raster formats and projections
3. **Performance**: Benchmark and optimize for large rasters
4. **Examples**: Add more dataset-specific examples
5. **Integration**: Test full Kubernetes workflow end-to-end
