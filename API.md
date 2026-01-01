# API Reference

Complete API documentation for the CNG Datasets toolkit.

## Table of Contents

- [Vector Processing](#vector-processing)
- [Raster Processing](#raster-processing)
- [Kubernetes Jobs](#kubernetes-jobs)
- [Storage Management](#storage-management)
- [Utilities](#utilities)

---

## Vector Processing

### `cng_datasets.vector.H3VectorProcessor`

Process vector datasets into H3-indexed parquet files.

```python
from cng_datasets.vector import H3VectorProcessor

processor = H3VectorProcessor(
    input_url: str,
    output_url: str,
    h3_resolution: int = 10,
    parent_resolutions: Optional[List[int]] = None,
    chunk_size: int = 500,
    intermediate_chunk_size: int = 10,
    id_column: Optional[str] = None,
    read_credentials: Optional[Dict[str, str]] = None,
    write_credentials: Optional[Dict[str, str]] = None,
)
```

**Parameters:**
- `input_url` (str): S3 URL or local path to input parquet/geoparquet file
- `output_url` (str): S3 URL or local path to output directory
- `h3_resolution` (int): Target H3 resolution for tiling (default: 10)
- `parent_resolutions` (List[int], optional): Parent resolutions to include (e.g., [9, 8, 0])
- `chunk_size` (int): Number of rows in pass 1 (geometry to H3 arrays, default: 500)
- `intermediate_chunk_size` (int): Number of rows in pass 2 (unnesting, default: 10)
- `id_column` (str, optional): ID column name (auto-detected if not specified)
- `read_credentials` (Dict, optional): AWS credentials for reading
- `write_credentials` (Dict, optional): AWS credentials for writing

**Methods:**

#### `process_chunk(chunk_id: int) -> Optional[str]`

Process a single chunk using two-pass approach.

**Returns:** Output file path if successful, None if chunk_id out of range

**Example:**
```python
processor = H3VectorProcessor(
    input_url="s3://bucket/input.parquet",
    output_url="s3://bucket/output/",
    h3_resolution=10,
    parent_resolutions=[9, 8, 0],
)

# Process specific chunk (for K8s jobs)
output_file = processor.process_chunk(chunk_id=0)
```

#### `process_all_chunks() -> List[str]`

Process all chunks in the dataset.

**Returns:** List of output file paths

---

### `cng_datasets.vector.process_vector_chunks`

Convenience function to process vector data into H3-indexed chunks.

```python
from cng_datasets.vector import process_vector_chunks

outputs = process_vector_chunks(
    input_url: str,
    output_url: str,
    chunk_id: Optional[int] = None,
    h3_resolution: int = 10,
    parent_resolutions: Optional[List[int]] = None,
    chunk_size: int = 500,
    intermediate_chunk_size: int = 10,
    **kwargs
)
```

**Example:**
```python
# Process entire dataset
outputs = process_vector_chunks(
    input_url="s3://bucket/input.parquet",
    output_url="s3://bucket/output/",
    h3_resolution=10,
)

# Process specific chunk
output = process_vector_chunks(
    input_url="s3://bucket/input.parquet",
    output_url="s3://bucket/output/",
    chunk_id=5,
)
```

---

## Raster Processing

### `cng_datasets.raster.RasterProcessor`

Process raster datasets into cloud-native formats (COG and H3-indexed parquet).

```python
from cng_datasets.raster import RasterProcessor

processor = RasterProcessor(
    input_path: str,
    output_cog_path: Optional[str] = None,
    output_parquet_path: Optional[str] = None,
    h3_resolution: Optional[int] = None,
    parent_resolutions: Optional[List[int]] = None,
    h0_index: Optional[int] = None,
    value_column: str = "value",
    compression: str = "deflate",
    blocksize: int = 512,
    resampling: str = "nearest",
    nodata_value: Optional[float] = None,
)
```

**Parameters:**
- `input_path` (str): Path to input raster (local or /vsis3/ URL)
- `output_cog_path` (str, optional): Path for output COG file
- `output_parquet_path` (str, optional): Base path for output parquet
- `h3_resolution` (int, optional): Target H3 resolution (auto-detected if None)
- `parent_resolutions` (List[int], optional): Parent resolutions to include
- `h0_index` (int, optional): Specific h0 cell index to process (0-121)
- `value_column` (str): Name for raster value column (default: "value")
- `compression` (str): COG compression method (default: "deflate")
- `blocksize` (int): COG block size (default: 512)
- `resampling` (str): Resampling method (default: "nearest")
- `nodata_value` (float, optional): NoData value to exclude

**Methods:**

#### `create_cog(output_path: Optional[str] = None) -> str`

Create a Cloud-Optimized GeoTIFF from input raster.

**Returns:** Path to created COG file

**Example:**
```python
processor = RasterProcessor(
    input_path="data.tif",
    output_cog_path="s3://bucket/data-cog.tif",
    compression="zstd",
)

cog_path = processor.create_cog()
```

#### `process_h0_region(h0_index: Optional[int] = None) -> Optional[str]`

Process a single h0 region to H3-indexed parquet.

**Returns:** Path to output parquet file, or None if region has no data

**Example:**
```python
processor = RasterProcessor(
    input_path="global-data.tif",
    output_parquet_path="s3://bucket/data/hex/",
    h0_index=42,
    h3_resolution=8,
)

output_file = processor.process_h0_region()
```

#### `process_all_h0_regions() -> List[str]`

Process all h0 regions (0-121) to H3-indexed parquet.

**Returns:** List of output parquet file paths

---

### `cng_datasets.raster.detect_optimal_h3_resolution`

Detect optimal H3 resolution based on raster pixel size.

```python
from cng_datasets.raster import detect_optimal_h3_resolution

h3_res = detect_optimal_h3_resolution(
    raster_path: str,
    verbose: bool = True
) -> int
```

**Parameters:**
- `raster_path` (str): Path to raster file
- `verbose` (bool): Whether to print detection message

**Returns:** Recommended H3 resolution (0-15)

**Example:**
```python
h3_res = detect_optimal_h3_resolution("high-res.tif")
print(f"Recommended: h{h3_res}")
# Output: Raster resolution: 30.0m → Recommended H3: 10
```

---

### `cng_datasets.raster.create_cog`

Convenience function to create a COG without full RasterProcessor initialization.

```python
from cng_datasets.raster import create_cog

cog_path = create_cog(
    input_path: str,
    output_path: str,
    compression: str = "deflate",
    blocksize: int = 512,
    overviews: bool = True,
    resampling: str = "nearest",
)
```

**Example:**
```python
cog_path = create_cog(
    input_path="imagery.tif",
    output_path="s3://bucket/imagery-cog.tif",
    compression="jpeg",
    blocksize=256,
)
```

---

## Kubernetes Jobs

### `cng_datasets.k8s.K8sJobManager`

Generate and manage Kubernetes jobs for parallel processing.

```python
from cng_datasets.k8s import K8sJobManager

manager = K8sJobManager(
    namespace: str = "biodiversity",
    image: str = "ghcr.io/rocker-org/ml-spatial:latest"
)
```

**Methods:**

#### `generate_chunked_job(...) -> Dict`

Generate an indexed Kubernetes job for parallel chunk processing.

```python
job_spec = manager.generate_chunked_job(
    job_name: str,
    script_path: str,
    num_chunks: int,
    base_args: List[str] = None,
    cpu: str = "2",
    memory: str = "8Gi",
    parallelism: int = 10,
)
```

**Example:**
```python
manager = K8sJobManager(namespace="datasets")

job_spec = manager.generate_chunked_job(
    job_name="process-data",
    script_path="/app/process.py",
    num_chunks=100,
    base_args=["--resolution", "10"],
    cpu="4",
    memory="16Gi",
    parallelism=20,
)

manager.save_job_yaml(job_spec, "job.yaml")
```

---

## Storage Management

### `cng_datasets.storage.configure_s3_credentials`

Configure S3 credentials for DuckDB using environment variables.

```python
from cng_datasets.storage import configure_s3_credentials

configure_s3_credentials(con: duckdb.DuckDBPyConnection)
```

**Environment Variables:**
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_S3_ENDPOINT`
- `AWS_REGION`
- `AWS_HTTPS`

---

### `cng_datasets.storage.RcloneSync`

Sync data between cloud providers using rclone.

```python
from cng_datasets.storage import RcloneSync

syncer = RcloneSync(
    config_path: Optional[str] = None,
    dry_run: bool = False
)
```

**Example:**
```python
syncer = RcloneSync()

syncer.sync(
    source="aws:public-dataset/",
    destination="cloudflare:public-dataset/",
)
```

---

## Utilities

### `cng_datasets.vector.setup_duckdb_connection`

Set up a DuckDB connection with required extensions.

```python
from cng_datasets.vector import setup_duckdb_connection

con = setup_duckdb_connection(
    extensions: Optional[List[str]] = None,
    http_retries: int = 20,
    http_retry_wait_ms: int = 5000,
)
```

**Returns:** Configured DuckDB connection with spatial and H3 extensions

---

### `cng_datasets.vector.identify_id_column`

Identify or validate an ID column in a table.

```python
from cng_datasets.vector import identify_id_column

id_col, is_unique = identify_id_column(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    specified_id_col: Optional[str] = None,
    check_uniqueness: bool = True,
)
```

**Returns:** Tuple of (column_name, is_unique)

---

### `cng_datasets.vector.geom_to_h3_cells`

Convert geometries to H3 cells at specified resolution.

```python
from cng_datasets.vector import geom_to_h3_cells

sql = geom_to_h3_cells(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    zoom: int = 10,
    keep_cols: Optional[List[str]] = None,
    geom_col: str = "geom",
)
```

**Returns:** SQL query string that generates H3 cells

---

## CLI Commands

### Vector Processing

```bash
cng-datasets vector \
  --input <url> \
  --output <url> \
  --resolution <int> \
  [--chunk-id <int>] \
  [--parent-resolutions <str>] \
  [--chunk-size <int>] \
  [--intermediate-chunk-size <int>]
```

### Raster Processing

```bash
cng-datasets raster \
  --input <path> \
  [--output-cog <path>] \
  [--output-parquet <path>] \
  [--resolution <int>] \
  [--parent-resolutions <str>] \
  [--h0-index <int>] \
  [--value-column <str>] \
  [--nodata <float>]
```

### Kubernetes Job Generation

```bash
cng-datasets k8s \
  --job-name <name> \
  --cmd <command> \
  [--chunks <int>] \
  [--output <path>]
```

### Storage Management

```bash
# Configure CORS
cng-datasets storage cors \
  --bucket <name> \
  --endpoint <url>

# Sync with rclone
cng-datasets storage sync \
  --source <path> \
  --destination <path>

# Setup public bucket
cng-datasets storage setup-bucket \
  --bucket <name> \
  [--remote <name>]
```

---

## Type Hints

All functions include comprehensive type hints for better IDE support:

```python
from typing import Optional, List, Dict, Tuple
import duckdb

def process_data(
    input_url: str,
    resolution: int = 10,
    options: Optional[Dict[str, str]] = None
) -> List[str]:
    """Process data and return output paths."""
    ...
```

---

## Error Handling

The package uses standard Python exceptions:

- `ValueError`: Invalid parameters or data
- `FileNotFoundError`: Missing input files
- `RuntimeError`: Processing failures
- `ConnectionError`: Network or S3 issues

**Example:**
```python
try:
    processor = RasterProcessor(input_path="missing.tif")
except ValueError as e:
    print(f"Error: {e}")
```

---

## Environment Variables

### AWS/S3 Configuration

- `AWS_ACCESS_KEY_ID`: AWS access key
- `AWS_SECRET_ACCESS_KEY`: AWS secret key
- `AWS_S3_ENDPOINT`: S3 endpoint URL
- `AWS_REGION`: AWS region (default: us-east-1)
- `AWS_HTTPS`: Use HTTPS (TRUE/FALSE)

### DuckDB Configuration

Set via code:
```python
con.execute("SET http_retries=20")
con.execute("SET http_retry_wait_ms=5000")
con.execute("SET temp_directory='/tmp'")
```

---

## Further Reading

- [Package README](README_PACKAGE.md) - User guide and examples
- [Contributing Guide](CONTRIBUTING.md) - Development guidelines
- [Changelog](CHANGELOG.md) - Version history
- [H3 Documentation](https://h3geo.org/) - H3 geospatial indexing
- [DuckDB Spatial](https://duckdb.org/docs/extensions/spatial) - Spatial extension
