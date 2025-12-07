#! /bin/bash

# S3 Bucket requires CORS rules before we can easily stream from it on HTTPS 
# Usage: ./set-bucket-cors.sh <bucket-name> [endpoint] [aws-key] [aws-secret]

BUCKET_NAME=${1}
ENDPOINT=${2:-${AWS_S3_ENDPOINT}}
AWS_ACCESS_KEY_ID=${3:-${AWS_ACCESS_KEY_ID}}
AWS_SECRET_ACCESS_KEY=${4:-${AWS_SECRET_ACCESS_KEY}}

if [ -z "$BUCKET_NAME" ]; then
  echo "Error: Bucket name is required"
  echo "Usage: $0 <bucket-name> [endpoint] [aws-key] [aws-secret]"
  exit 1
fi

if [ -z "$ENDPOINT" ]; then
  echo "Error: Endpoint must be provided as argument or set in AWS_S3_ENDPOINT"
  exit 1
fi

# Export the credentials for aws cli to use
export AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY

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

echo "CORS configuration set for bucket: ${BUCKET_NAME}"
echo "Verifying CORS configuration..."

aws s3api --endpoint-url https://${ENDPOINT} get-bucket-cors --bucket ${BUCKET_NAME}

