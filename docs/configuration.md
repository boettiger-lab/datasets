# Configuration

Configure credentials and settings for cloud storage and processing.

## S3 Credentials

The toolkit supports multiple authentication methods for S3 access.

### Environment Variables

Set AWS credentials as environment variables:

```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-west-2"
```

### Using cng.utils

If you have the `cng` package installed:

```python
from cng.utils import set_secrets, setup_duckdb_connection

con = setup_duckdb_connection()
set_secrets(con)
```

### Manual Configuration

Pass credentials directly to processors:

```python
from cng_datasets.vector import H3VectorProcessor

processor = H3VectorProcessor(
    input_url="s3://bucket/input.parquet",
    output_url="s3://bucket/output/",
    read_credentials={
        "key": "ACCESS_KEY",
        "secret": "SECRET_KEY",
        "region": "us-west-2"
    },
    write_credentials={
        "key": "ACCESS_KEY",
        "secret": "SECRET_KEY",
        "region": "us-west-2"
    }
)
```

### Kubernetes Secrets

For Kubernetes workflows, use secrets:

```bash
# Create secret
kubectl create secret generic aws-credentials \
  --from-literal=AWS_ACCESS_KEY_ID=your-key \
  --from-literal=AWS_SECRET_ACCESS_KEY=your-secret \
  -n biodiversity

# Reference in job
env:
  - name: AWS_ACCESS_KEY_ID
    valueFrom:
      secretKeyRef:
        name: aws-credentials
        key: AWS_ACCESS_KEY_ID
  - name: AWS_SECRET_ACCESS_KEY
    valueFrom:
      secretKeyRef:
        name: aws-credentials
        key: AWS_SECRET_ACCESS_KEY
```

## Rclone Configuration

Configure rclone for syncing between cloud providers.

### Configuration File

Create `~/.config/rclone/rclone.conf`:

```ini
[aws]
type = s3
provider = AWS
access_key_id = your-access-key
secret_access_key = your-secret-key
region = us-west-2

[cloudflare]
type = s3
provider = Cloudflare
access_key_id = your-r2-access-key
secret_access_key = your-r2-secret-key
endpoint = https://your-account-id.r2.cloudflarestorage.com
```

### Python API

```python
from cng_datasets.storage import RcloneSync

# Use default config
syncer = RcloneSync()

# Or specify custom config
syncer = RcloneSync(config_path="/path/to/rclone.conf")

# Sync between remotes
syncer.sync(
    source="aws:public-dataset/",
    destination="cloudflare:public-dataset/"
)
```

### Command-Line

```bash
cng-datasets storage sync \
    --source aws:bucket/data \
    --destination cloudflare:bucket/data
```

## Bucket CORS Configuration

Configure CORS for public bucket access:

```python
from cng_datasets.storage import configure_bucket_cors

configure_bucket_cors(
    bucket="my-public-bucket",
    endpoint="https://s3.amazonaws.com"
)
```

Or use command-line:

```bash
cng-datasets storage cors \
    --bucket my-public-bucket \
    --endpoint https://s3.amazonaws.com
```

## Docker Configuration

### Build Custom Image

```dockerfile
FROM ghcr.io/boettiger-lab/datasets:latest

# Add custom dependencies
RUN pip install my-package

# Copy custom scripts
COPY scripts/ /app/
```

### Mount Credentials

```bash
# Mount AWS credentials
docker run --rm \
  -v ~/.aws:/root/.aws:ro \
  -v $(pwd):/data \
  ghcr.io/boettiger-lab/datasets:latest \
  cng-datasets raster --input /data/input.tif

# Use environment variables
docker run --rm \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -v $(pwd):/data \
  ghcr.io/boettiger-lab/datasets:latest \
  cng-datasets raster --input /data/input.tif
```

## GDAL Configuration

For raster processing with GDAL:

### Virtual File Systems

Use `/vsis3/` for direct S3 access:

```python
from cng_datasets.raster import RasterProcessor

processor = RasterProcessor(
    input_path="/vsis3/bucket/data.tif",  # Direct S3 access
    output_cog_path="s3://bucket/output.tif",
)
```

### GDAL Options

Configure GDAL behavior:

```python
import os

# Set GDAL options
os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "EMPTY_DIR"
os.environ["CPL_VSIL_CURL_ALLOWED_EXTENSIONS"] = ".tif,.tiff"
os.environ["GDAL_HTTP_MAX_RETRY"] = "3"
os.environ["GDAL_HTTP_RETRY_DELAY"] = "5"
```

## Performance Tuning

### Memory Settings

```python
# Vector processing
processor = H3VectorProcessor(
    input_url="s3://bucket/data.parquet",
    output_url="s3://bucket/output/",
    chunk_size=100,  # Reduce for memory-constrained environments
    intermediate_chunk_size=5,
)

# Raster processing
from cng_datasets.raster import RasterProcessor

processor = RasterProcessor(
    input_path="data.tif",
    blocksize=256,  # Smaller tiles for less memory
)
```

### Parallelism

```bash
# Kubernetes job parallelism
cng-datasets workflow \
  --dataset my-dataset \
  --parallelism 50  # Adjust based on cluster capacity
```

### Compression

```python
# Vector output
processor = H3VectorProcessor(
    input_url="s3://bucket/data.parquet",
    output_url="s3://bucket/output/",
    compression="zstd",  # or "snappy", "gzip"
)

# COG compression
processor = RasterProcessor(
    input_path="data.tif",
    output_cog_path="s3://bucket/output.tif",
    compression="zstd",  # or "deflate", "lzw"
)
```
