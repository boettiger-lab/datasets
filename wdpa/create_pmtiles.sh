#!/bin/bash
# Create PMTiles from WDPA GeoParquet

set -e

echo "Converting GeoParquet to GeoJSONSeq for tippecanoe..."

# Convert parquet to GeoJSONSeq (which tippecanoe can read)
# Use GeoJSONSeq for streaming large files
ogr2ogr \
  -f GeoJSONSeq \
  /tmp/WDPA_Dec2025.geojsonl \
  /vsis3/public-wdpa/WDPA_Dec2025.parquet \
  -progress

echo "GeoJSONSeq created successfully!"

# Generate PMTiles from the GeoJSONSeq
echo "Generating PMTiles from GeoJSONSeq..."

tippecanoe \
  -o /tmp/WDPA_Dec2025.pmtiles \
  -l wdpa \
  --drop-densest-as-needed \
  --extend-zooms-if-still-dropping \
  --force \
  /tmp/WDPA_Dec2025.geojsonl

echo "PMTiles created successfully!"

# Upload PMTiles to S3
echo "Uploading PMTiles to S3..."

# Configure mc alias if not already configured
mc alias set s3 https://${AWS_PUBLIC_ENDPOINT} ${AWS_ACCESS_KEY_ID} ${AWS_SECRET_ACCESS_KEY}

mc cp /tmp/WDPA_Dec2025.pmtiles s3/public-wdpa/WDPA_Dec2025.pmtiles

echo "PMTiles uploaded successfully!"
