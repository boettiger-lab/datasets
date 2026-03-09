# GDAL + NumPy 2.x Compatibility Issue

**Date**: February 2026  
**Status**: Temporarily constrained to numpy<2.0

## Problem

When running tests in CI, GDAL's Python bindings fail to import with numpy 2.x:

```
ImportError: numpy.core.multiarray failed to import

A module that was compiled using NumPy 1.x cannot be run in
NumPy 2.4.2 as it may crash. To support both 1.x and 2.x
versions of NumPy, modules must be compiled with NumPy 2.0.
```

## Root Cause

The `ghcr.io/osgeo/gdal:ubuntu-full-latest` Docker image contains:
- GDAL 3.13.0dev (released Feb 16, 2026) - **supports numpy 2.x in theory**
- System Python packages in `/usr/lib/python3/dist-packages/osgeo/` compiled against numpy 1.x
- Ubuntu apt packages (`python3-numpy`, GDAL Python bindings) built with older numpy

While GDAL 3.9+ added numpy 2.x API support, the pre-built Ubuntu packages were compiled against numpy 1.x and are **binary incompatible** with numpy 2.x at runtime.

## Current Solution

`pyproject.toml`:
```toml
"numpy>=1.20.0,<2.0",
```

`Dockerfile`:
```dockerfile
RUN uv pip install "numpy<2" pip
```

Both constraints ensure numpy 1.x is installed everywhere, matching GDAL's compilation.

## What We Tried (Failed)

1. **Remove system `python3-numpy` package**: Doesn't help - GDAL bindings are still compiled against numpy 1.x in the base image
2. **Install numpy 2.x anyway**: Immediate import failure of `osgeo.gdal_array`

Test results:
```bash
$ docker run --rm cng-datasets-test:numpy2 python -c "from osgeo import gdal_array"
ImportError: numpy.core.multiarray failed to import
```

## Future Solutions

### Option A: Wait for Upstream (Recommended)
OSGeo needs to rebuild their Docker images with numpy 2.x. Track:
- https://github.com/OSGeo/gdal/issues (numpy 2.x support)
- https://github.com/osgeo/gdal-docker (Docker image builds)

GDAL 3.9+ already supports numpy 2.x API - just needs recompilation.

### Option B: Build GDAL from Source
Add to Dockerfile:
```dockerfile
RUN apt-get build-dep -y gdal
RUN pip install numpy>=2.0
RUN git clone --depth 1 --branch v3.13.0 https://github.com/OSGeo/gdal.git && \
    cd gdal && \
    cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && \
    cmake --build build -j$(nproc) && \
    cmake --install build
```

**Downside**: Adds 15-30 minutes to Docker build time.

### Option C: Use PyPI GDAL Wheels
If/when GDAL releases numpy 2.x compatible wheels to PyPI:
```toml
raster = [
    "GDAL>=3.11.0",  # Will install from PyPI wheel
    "numpy>=2.0",
]
```

Remove `--system-site-packages` from Dockerfile venv creation.

## Impact

- **Raster processing**: Requires numpy<2.0 constraint (uses GDAL via rasterio)
- **Vector processing**: Unaffected (uses DuckDB, no direct GDAL dependency)
- **Performance**: No performance impact, just version constraint

## Testing

Only 2 tests affected (both in `test_raster.py`):
- `test_complete_workflow_small_dataset`
- `test_process_h0_region_basic`

Main branch tests pass because they use the same Docker container with numpy<2.0 constraint.

## When to Revisit

Check quarterly or when:
1. OSGeo releases new ubuntu-full Docker images
2. Ubuntu updates GDAL packages with numpy 2.x support
3. GDAL releases PyPI wheels with numpy 2.x
4. We need a numpy 2.x feature (unlikely given our use case)

Test by temporarily removing the constraint and running:
```bash
docker build -t test .
docker run --rm test python -c "import numpy; print(numpy.__version__); from osgeo import gdal_array; print('OK')"
```

If it prints numpy version and "OK", the constraint can be removed.
