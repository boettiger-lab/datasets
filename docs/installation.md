# Installation

## Basic Installation

Install from PyPI (Python 3.10 or later):

```bash
pip install cng-datasets
```

Or install from source:

```bash
git clone https://github.com/boettiger-lab/datasets.git
cd datasets
pip install -e .
```

## Development Installation

To install with development tools:

```bash
pip install -e ".[dev]"
```

This includes:
- pytest, ruff, black and mypy

## Raster Processing Support

The `[raster]` extra adds exactextract and rasterio but deliberately leaves out the GDAL Python bindings, which must match the system's libgdal exactly. Install GDAL from your system package manager first:

### Ubuntu/Debian

```bash
sudo apt-get install gdal-bin libgdal-dev python3-gdal
pip install -e ".[raster]"
```

### macOS

```bash
brew install gdal
pip install -e ".[raster]"
```

### Using Docker (Recommended)

The easiest way to use this package with full GDAL support is via Docker:

```bash
# Pull the pre-built image
docker pull ghcr.io/boettiger-lab/datasets:latest

# Run interactively
docker run -it --rm -v $(pwd):/data ghcr.io/boettiger-lab/datasets:latest bash

# Or run a specific command
docker run --rm -v $(pwd):/data ghcr.io/boettiger-lab/datasets:latest \
  cng-datasets raster --input /data/input.tif --output-cog /data/output.tif
```

The Docker image includes:
- One GDAL and one GEOS: rasterio, pyogrio, pyproj, shapely and exactextract are built from source against the system libraries
- DuckDB 2.0
- All Python dependencies
- AWS CLI and rclone for cloud storage
- Pre-installed cng-datasets package

## Verifying Installation

Check that the package is installed correctly:

```bash
cng-datasets --help
```

You should see the command-line interface help message.
