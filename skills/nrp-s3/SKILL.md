---
name: nrp-s3
description: >
  Manage S3 object storage on the NRP (National Research Platform) Nautilus cluster,
  which uses Ceph S3 (not AWS). Covers public vs internal endpoints, rclone usage,
  bucket creation, public-read policies, CORS configuration, DuckDB S3 access, and
  credential handling. Use when working with S3 buckets on NRP Nautilus, uploading or
  downloading data, configuring bucket access, or reading data from Ceph S3 storage.
license: Apache-2.0
compatibility: >
  Requires kubectl configured for the NRP Nautilus cluster, rclone with an "nrp" remote,
  and optionally the AWS CLI. Works with any agent that can run shell commands.
metadata:
  author: boettiger-lab
  version: "1.0"
---

# NRP S3 Storage

The NRP Nautilus cluster provides S3-compatible object storage powered by Ceph (Rook). This is **not** AWS S3 — it has its own endpoints, credential system, and quirks.

## Two Endpoints

There are two S3 endpoints. Getting these right is critical.

| Endpoint | URL | Use |
|----------|-----|-----|
| **Public** | `s3-west.nrp-nautilus.io` | External access (browsers, local machines, HTTPS) |
| **Internal** | `rook-ceph-rgw-nautiluss3.rook` | Inside k8s pods (faster, no TLS overhead) |

### When to use which

- **Local machine / CI / external tools**: Always use the public endpoint
- **Inside k8s pods**: Use the internal endpoint for S3 read/write operations
- **Public data URLs for end users**: Always use the public endpoint

### Public URL format

Public data is accessible via HTTPS using **path-style** URLs (not virtual-hosted):

```
https://s3-west.nrp-nautilus.io/<bucket>/<path>
```

Example:
```
https://s3-west.nrp-nautilus.io/public-padus/padus-4-1/fee.parquet
```

**Never** use virtual-hosted-style URLs like `<bucket>.s3-west.nrp-nautilus.io` — they won't work.

## Credentials

### Kubernetes secrets

Two secrets are pre-configured in the `biodiversity` namespace:

1. **`aws`** — Contains `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
2. **`rclone-config`** — Contains a complete rclone configuration file

In k8s job YAML, reference them like this:

```yaml
env:
  - name: AWS_ACCESS_KEY_ID
    valueFrom:
      secretKeyRef:
        name: aws
        key: AWS_ACCESS_KEY_ID
  - name: AWS_SECRET_ACCESS_KEY
    valueFrom:
      secretKeyRef:
        name: aws
        key: AWS_SECRET_ACCESS_KEY
volumes:
  - name: rclone-config
    secret:
      secretName: rclone-config
volumeMounts:
  - name: rclone-config
    mountPath: /root/.config/rclone
    readOnly: true
```

### Local credentials

Locally, rclone is already configured with a remote named `nrp`. AWS credentials should be set as environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) when using the AWS CLI directly.

## Environment Variables for K8s Pods

When running inside the cluster, pods need these environment variables:

```yaml
env:
  - name: AWS_ACCESS_KEY_ID
    valueFrom:
      secretKeyRef: { name: aws, key: AWS_ACCESS_KEY_ID }
  - name: AWS_SECRET_ACCESS_KEY
    valueFrom:
      secretKeyRef: { name: aws, key: AWS_SECRET_ACCESS_KEY }
  - name: AWS_S3_ENDPOINT
    value: "rook-ceph-rgw-nautiluss3.rook"      # Internal endpoint
  - name: AWS_PUBLIC_ENDPOINT
    value: "s3-west.nrp-nautilus.io"             # For public URL generation
  - name: AWS_HTTPS
    value: "false"                                # Internal endpoint is HTTP
  - name: AWS_VIRTUAL_HOSTING
    value: "FALSE"                                # Path-style URLs required
```

Note: `AWS_HTTPS` is `"false"` for the internal endpoint (plain HTTP inside the cluster). The public endpoint always uses HTTPS.

## Rclone

Rclone is the primary tool for file transfers. The remote is named `nrp`.

### Common operations

```bash
# List bucket contents
rclone ls nrp:<bucket>/

# Upload a file
rclone copy local-file.parquet nrp:<bucket>/path/ -P

# Upload a directory
rclone copy ./local-dir/ nrp:<bucket>/remote-dir/ -P

# Download a file
rclone copy nrp:<bucket>/path/file.parquet ./local/ -P

# Sync (mirror source to destination, deletes extras)
rclone sync nrp:<bucket>/source/ nrp:<bucket>/dest/ -P
```

### Inside k8s pods

The rclone config is mounted from the `rclone-config` secret. No additional configuration is needed — just use `rclone copy ... nrp:<bucket>/...`.

## Bucket Management

### Creating a bucket

```bash
rclone mkdir nrp:<bucket-name>
```

### Setting public read access

Use the AWS CLI with the public endpoint:

```bash
aws s3api put-bucket-policy \
  --bucket <bucket-name> \
  --endpoint-url https://s3-west.nrp-nautilus.io \
  --policy '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {"AWS": ["*"]},
        "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
        "Resource": ["arn:aws:s3:::<bucket-name>"]
      },
      {
        "Effect": "Allow",
        "Principal": {"AWS": ["*"]},
        "Action": ["s3:GetObject"],
        "Resource": ["arn:aws:s3:::<bucket-name>/*"]
      }
    ]
  }'
```

### Setting CORS (required for browser access to PMTiles, etc.)

```bash
aws s3api put-bucket-cors \
  --bucket <bucket-name> \
  --endpoint-url https://s3-west.nrp-nautilus.io \
  --cors-configuration '{
    "CORSRules": [{
      "AllowedOrigins": ["*"],
      "AllowedMethods": ["GET", "HEAD"],
      "AllowedHeaders": ["*"],
      "ExposeHeaders": ["ETag", "Content-Length", "Content-Type", "Accept-Ranges", "Content-Range"],
      "MaxAgeSeconds": 3600
    }]
  }'
```

### Verifying configuration

```bash
# Check bucket policy
aws s3api get-bucket-policy \
  --bucket <bucket-name> \
  --endpoint-url https://s3-west.nrp-nautilus.io

# Check CORS
aws s3api get-bucket-cors \
  --bucket <bucket-name> \
  --endpoint-url https://s3-west.nrp-nautilus.io
```

## DuckDB S3 Access

To read from NRP S3 in DuckDB (locally or in pods):

```sql
INSTALL httpfs;
LOAD httpfs;

CREATE OR REPLACE SECRET s3_secret (
    TYPE S3,
    KEY_ID '',
    SECRET '',
    USE_SSL 'TRUE',
    ENDPOINT 's3-west.nrp-nautilus.io',
    URL_STYLE 'path',
    REGION 'us-east-1'
);

-- Now you can query directly
SELECT * FROM 's3://public-padus/padus-4-1/fee.parquet' LIMIT 10;
```

For **public** (anonymous) access, empty `KEY_ID` and `SECRET` work. For write access, provide real credentials.

Inside k8s pods, use the internal endpoint and set `USE_SSL` to `'FALSE'`:

```sql
CREATE OR REPLACE SECRET s3_secret (
    TYPE S3,
    KEY_ID '${AWS_ACCESS_KEY_ID}',
    SECRET '${AWS_SECRET_ACCESS_KEY}',
    USE_SSL 'FALSE',
    ENDPOINT 'rook-ceph-rgw-nautiluss3.rook',
    URL_STYLE 'path',
    REGION 'us-east-1'
);
```

## GDAL / OGR with Remote Files

Use `/vsicurl/` prefix to read remote files without downloading:

```bash
# Inspect a remote geodatabase
ogrinfo /vsicurl/https://s3-west.nrp-nautilus.io/<bucket>/raw/data.gdb

# Convert a remote file to local parquet
ogr2ogr -f Parquet output.parquet /vsicurl/https://s3-west.nrp-nautilus.io/<bucket>/raw/data.gdb "LayerName"
```

## Standard Bucket Layout

```
<bucket>/
├── raw/                              # Source/original data (uploaded once)
├── <dataset>.parquet                 # Cloud-optimized GeoParquet
├── <dataset>.pmtiles                 # Vector tiles for web maps
├── <dataset>/
│   └── hex/
│       └── h0={cell}/data_0.parquet  # H3 hex-indexed, hive-partitioned
├── README.md                         # Data dictionary and usage docs
└── stac-collection.json              # STAC metadata
```

## Common Pitfalls

1. **Using virtual-hosted URLs** — Always use path-style: `https://s3-west.nrp-nautilus.io/<bucket>/...`
2. **Using the public endpoint inside pods** — Use the internal endpoint `rook-ceph-rgw-nautiluss3.rook` for all S3 operations inside the cluster
3. **Forgetting CORS** — PMTiles and other browser-based tools require CORS to be set on the bucket
4. **HTTPS inside pods** — The internal endpoint is HTTP, not HTTPS. Set `AWS_HTTPS=false`
5. **Hardcoding credentials** — Always use k8s secrets, never hardcode in YAML files
6. **Forgetting to set public policy** — New buckets are private by default; you must explicitly set the public read policy
