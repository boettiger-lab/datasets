# Multi-Source Input Support - Implementation Summary

## Overview

Added support for processing multiple source files in a single workflow. Instead of requiring users to create ZIP files or run separate workflows, users can now specify multiple `--source-url` arguments to merge datasets.

## Changes Made

### 1. CLI Modifications

**`cng_datasets/cli.py`**
- Updated `workflow` command: `--source-url` now uses `action='append'` to accept multiple values
- Changed parameter from `source_url` to `source_urls` (with backwards compatibility)

**`cng_datasets/vector/convert_to_parquet.py`**
- Updated `main()` function: source argument now accepts multiple positional arguments (`nargs='+'`)
- Updated `convert_to_parquet()` function signature: `source_url` parameter now accepts `Union[str, List[str]]`
- Added logic to handle multiple sources:
  - Validates no mixing of ZIP files with other sources
  - Processes multiple non-ZIP sources via `build_read_reproject_query`'s existing UNION ALL support
  - Uses first source for CRS detection and geometry column detection

### 2. Workflow Generation

**`cng_datasets/k8s/workflows.py`**
- Updated `generate_dataset_workflow()`:
  - New parameter: `source_urls: Union[str, List[str]]`
  - Backwards compatibility: still accepts `source_url` (singular) for existing code
  - Normalizes input to list internally
- Updated `_count_source_features()`:
  - Now accepts `Union[str, List[str]]`
  - Sums feature counts across all sources
  - Extracted single-source logic into `_count_single_source()`
- Updated `_generate_convert_job()`:
  - Handles multiple source URLs in generated command
  - Formats multiple URLs as separate `--source-url` arguments (one per line for readability)
- Updated workflow command reconstruction:
  - Generates correct `--source-url` flags for each input URL in ConfigMap documentation

### 3. Documentation

**New Files:**
- `examples/multi-source-workflow.md` - Complete guide with use cases and examples
- `CHANGELOG-multi-source.md` - This summary

**Updated Files:**
- `README.md` - Added multi-source example
- `cng_datasets/vector/convert_to_parquet.py` - Updated docstrings
- `cng_datasets/k8s/workflows.py` - Updated docstrings

### 4. Tests

**`tests/test_k8s_workflows.py`**
- Added `test_multi_source_workflow()` - Verifies multiple URLs are processed correctly
- Added `test_single_source_as_string()` - Ensures backwards compatibility
- Both tests mock `_count_source_features` to avoid network access

## Usage Examples

### Single Source (Backwards Compatible)
```bash
cng-datasets workflow \
  --dataset my-dataset \
  --source-url https://example.com/data.shp \
  --bucket my-bucket
```

### Multiple Sources (New Feature)
```bash
cng-datasets workflow \
  --dataset merged-dataset \
  --source-url https://example.com/region1.shp \
  --source-url https://example.com/region2.shp \
  --source-url https://example.com/region3.shp \
  --bucket my-bucket
```

### Direct Conversion
```bash
cng-convert-to-parquet \
  source1.shp \
  source2.shp \
  source3.shp \
  output.parquet
```

## Technical Details

### Merge Strategy
- Sources are merged using DuckDB's `UNION ALL` semantics
- All sources must have compatible schemas (matching column names and types)
- CRS detection and geometry column detection use the first source as representative
- Feature counting sums across all sources for optimal chunk size calculation

### Limitations
- Cannot mix `.zip` files with other source types (error raised)
- Multiple parquet inputs not yet supported (error raised, use vector formats)
- Layer parameter applies to all sources (useful for GDB files with same layer structure)
- All sources should use similar formats (don't mix shapefiles with GeoPackage, etc.)

### Backwards Compatibility
- Function parameter `source_url` (singular) still works
- Automatically mapped to `source_urls` internally
- All existing tests pass without modification
- CLI accepts both single and multiple `--source-url` flags

## Test Results

All 13 workflow generation tests pass:
- ✅ Existing tests using `source_url` parameter (backwards compatibility)
- ✅ New test for multiple source URLs (`test_multi_source_workflow`)
- ✅ New test for single source as string (`test_single_source_as_string`)

## Future Enhancements

Potential improvements:
1. Support multiple parquet inputs (currently only accepts vector formats for merging)
2. Allow per-source layer specification for multi-layer datasets
3. Schema validation and automatic column alignment
4. Progress reporting showing which source is being processed
