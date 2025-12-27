#!/bin/bash
# Convert Redlining GPKG to GeoParquet

set -e

echo "Downloading Redlining GPKG..."

# Download the GPKG file
curl -L -o /tmp/mappinginequality.gpkg \
  "https://dsl.richmond.edu/panorama/redlining/static/mappinginequality.gpkg"

echo "Cleaning data and converting to GeoParquet..."

# Clean the data and convert to GeoParquet
python redlining/clean_data.py \
  --input /tmp/mappinginequality.gpkg \
  --output /tmp/mappinginequality_clean.parquet

echo "Uploading to S3..."

# Upload to S3 using ogr2ogr for proper S3 handling
ogr2ogr \
  -f Parquet \
  /vsis3/public-redlining/mappinginequality.parquet \
  /tmp/mappinginequality_clean.parquet \
  -progress

echo "Cleaning up local files..."
rm /tmp/mappinginequality.gpkg
rm /tmp/mappinginequality_clean.parquet

echo "GeoParquet created successfully!"
