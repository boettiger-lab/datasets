# Public Output Bucket Configuration

## Bucket: `public-output`
**S3 Endpoint:** NRP (Nautilus) - `https://s3-west.nrp-nautilus.io`

## Configuration Summary

### Anonymous Read+Write Access
The bucket is configured for public anonymous access:
- **GetObject** - Anyone can read files
- **PutObject** - Anyone can upload files
- **ListBucket** - Anyone can list bucket contents

### CORS Configuration
- **Allowed Origins:** `*` (all)
- **Allowed Methods:** GET, PUT, POST, DELETE, HEAD
- **Allowed Headers:** `*` (all)
- **Exposed Headers:** ETag, Content-Length, Content-Type
- **Max Age:** 3600 seconds

### Object Size Limit
- **Maximum object size:** 1 GB (1,073,741,824 bytes)
- Uploads larger than 1 GB will be denied

### Lifecycle Policy
- **Auto-deletion:** Objects are automatically deleted after 30 days
- **Purpose:** Prevents unlimited growth and ensures temporary storage

## Use Cases
This bucket serves as a temporary public workspace where:
- Users can upload and download files without authentication
- Files are automatically cleaned up after 30 days
- Large files (>1GB) are rejected to prevent abuse

## Applied Configurations

```bash
# Apply bucket policy
aws s3api put-bucket-policy --profile nrp --bucket public-output \
  --policy file://public-output-policy.json

# Apply CORS configuration
aws s3api put-bucket-cors --profile nrp --bucket public-output \
  --cors-configuration file://public-output-cors.json

# Apply lifecycle rule
aws s3api put-bucket-lifecycle-configuration --profile nrp --bucket public-output \
  --lifecycle-configuration file://public-output-lifecycle.json
```

**Date Configured:** January 3, 2026
