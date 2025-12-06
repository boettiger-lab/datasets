#!/bin/bash
# Convert Ramsar shapefile to GeoParquet

set -e

echo "Converting Ramsar shapefile to GeoParquet..."

# Use geopandas with encoding handling to clean and convert
python3 << 'EOPYTHON'
import geopandas as gpd
import warnings
import boto3
import os

warnings.filterwarnings('ignore')

# Read shapefile with Latin-1 encoding (common for shapefiles with special chars)
gdf = gpd.read_file(
    'https://minio.carlboettiger.info/public-wetlands/ramsar/features_publishedPolygon.shp',
    encoding='latin1'
)

# Clean string columns - replace any invalid UTF-8
for col in gdf.select_dtypes(include=['object']):
    if col != 'geometry':
        gdf[col] = gdf[col].apply(lambda x: x.encode('utf-8', errors='replace').decode('utf-8') if isinstance(x, str) else x)

# Write to local temp file
local_path = '/tmp/ramsar_wetlands.parquet'
gdf.to_parquet(local_path, compression='snappy')

print("Local parquet created, uploading to S3...")

# Upload to S3
s3_client = boto3.client(
    's3',
    endpoint_url=f"http://{os.environ['AWS_S3_ENDPOINT']}",
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
)

s3_client.upload_file(
    local_path,
    'public-wetlands',
    'ramsar/ramsar_wetlands.parquet'
)

print("GeoParquet created and uploaded successfully!")
EOPYTHON
