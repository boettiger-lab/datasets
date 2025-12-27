#!/bin/bash
# Create PMTiles from Redlining GPKG

set -e

echo "Downloading Redlining GPKG..."

# Download the GPKG file
curl -L -o /tmp/mappinginequality.gpkg \
  "https://dsl.richmond.edu/panorama/redlining/static/mappinginequality.gpkg"

echo "Cleaning data..."

# Clean the data first
python redlining/clean_data.py \
  --input /tmp/mappinginequality.gpkg \
  --output /tmp/mappinginequality_clean.parquet

echo "Converting cleaned data to GeoJSONSeq for tippecanoe..."

# Convert cleaned parquet to GeoJSONSeq
ogr2ogr \
  -f GeoJSONSeq \
  /tmp/mappinginequality.geojsonl \
  /tmp/mappinginequality_clean.parquet \
  -progress

echo "GeoJSONSeq created successfully!"

# Generate PMTiles from the GeoJSONSeq
echo "Generating PMTiles from GeoJSONSeq..."

tippecanoe \
  -o /tmp/mappinginequality.pmtiles \
  -l redlining \
  --drop-densest-as-needed \
  --extend-zooms-if-still-dropping \
  --force \
  /tmp/mappinginequality.geojsonl

echo "PMTiles created successfully!"

# Upload PMTiles to S3
echo "Uploading PMTiles to S3..."

# Configure mc alias if not already configured
mc alias set s3 https://${AWS_PUBLIC_ENDPOINT} ${AWS_ACCESS_KEY_ID} ${AWS_SECRET_ACCESS_KEY}

mc cp /tmp/mappinginequality.pmtiles s3/public-redlining/mappinginequality.pmtiles

echo "Cleaning up local files..."
rm /tmp/mappinginequality.gpkg
rm /tmp/mappinginequality_clean.parquet
rm /tmp/mappinginequality.geojsonl
rm /tmp/mappinginequality.pmtiles

echo "PMTiles uploaded successfully!"
