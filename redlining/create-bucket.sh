#!/bin/bash
# Create public-redlining bucket on NRP endpoint
# and set policy to allow anonymous downloads

set -e

BUCKET_NAME="public-redlining"
ENDPOINT="s3-west.nrp-nautilus.io"

# Extract credentials from rclone config for nrp alias
export AWS_ACCESS_KEY_ID=$(rclone config dump | jq -r '.nrp.access_key_id')
export AWS_SECRET_ACCESS_KEY=$(rclone config dump | jq -r '.nrp.secret_access_key')

if [ -z "$AWS_ACCESS_KEY_ID" ] || [ "$AWS_ACCESS_KEY_ID" == "null" ]; then
    echo "Error: Could not extract AWS credentials from rclone config for 'nrp' alias"
    exit 1
fi

echo "======================================"
echo "Creating ${BUCKET_NAME} bucket"
echo "======================================"
echo ""

# Create bucket on NRP
echo "Creating bucket on NRP..."
if rclone lsf nrp:${BUCKET_NAME} &>/dev/null; then
    echo "✓ Bucket ${BUCKET_NAME} already exists on NRP"
else
    rclone mkdir nrp:${BUCKET_NAME}
    echo "✓ Bucket ${BUCKET_NAME} created on NRP"
fi

echo ""
echo "======================================"
echo "Setting anonymous download policy"
echo "======================================"
echo ""

# Set anonymous read policy on NRP using AWS CLI
# Match the policy used in public-wdpa bucket
echo "Setting policy on NRP..."

aws s3api --endpoint-url https://${ENDPOINT} put-bucket-policy --bucket ${BUCKET_NAME} --policy '{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": ["*"]
            },
            "Action": [
                "s3:GetBucketLocation",
                "s3:ListBucket"
            ],
            "Resource": ["arn:aws:s3:::'${BUCKET_NAME}'"]
        },
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": ["*"]
            },
            "Action": ["s3:GetObject"],
            "Resource": ["arn:aws:s3:::'${BUCKET_NAME}'/*"]
        }
    ]
}'

echo "✓ Policy set on NRP"
echo "Verifying policy..."
aws s3api --endpoint-url https://${ENDPOINT} get-bucket-policy --bucket ${BUCKET_NAME}

echo ""
echo "======================================"
echo "Setting CORS configuration"
echo "======================================"
echo ""

# Set CORS on NRP using AWS CLI
echo "Setting CORS on NRP..."

aws s3api --endpoint-url https://${ENDPOINT} put-bucket-cors --bucket ${BUCKET_NAME} --cors-configuration '{
  "CORSRules": [
    {
      "AllowedOrigins": ["*"],
      "AllowedMethods": ["GET"],
      "ExposeHeaders": ["ETag", "Content-Type", "Content-Disposition"],
      "AllowedHeaders": ["*"]
    }
  ]
}'

echo "✓ CORS configuration set"
echo "Verifying CORS configuration..."
aws s3api --endpoint-url https://${ENDPOINT} get-bucket-cors --bucket ${BUCKET_NAME}

echo ""
echo "======================================"
echo "✓ Bucket setup complete!"
echo "======================================"
echo ""
echo "Bucket: ${BUCKET_NAME}"
echo "NRP URL: https://s3-west.nrp-nautilus.io/${BUCKET_NAME}/"
echo ""
echo "You can now upload files using:"
echo "  rclone copy <source> nrp:${BUCKET_NAME}/"
