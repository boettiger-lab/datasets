#!/bin/bash
# Create PMTiles from WDPA GeoParquet

set -e

echo "Converting GeoParquet to FlatGeobuf for tippecanoe..."

# Convert parquet to FlatGeobuf (which tippecanoe can read)
ogr2ogr \
  -f FlatGeobuf \
  /tmp/WDPA_Dec2025.fgb \
  /vsis3/public-wdpa/WDPA_Dec2025.parquet \
  -progress

echo "FlatGeobuf created successfully!"

# Generate PMTiles from the FlatGeobuf
echo "Generating PMTiles from FlatGeobuf..."

tippecanoe \
  -o /tmp/WDPA_Dec2025.pmtiles \
  -l wdpa \
  --drop-densest-as-needed \
  --extend-zooms-if-still-dropping \
  --force \
  /tmp/WDPA_Dec2025.fgb

echo "PMTiles created successfully!"

# Upload PMTiles to S3
echo "Uploading PMTiles to S3..."

aws s3 cp /tmp/WDPA_Dec2025.pmtiles s3://public-wdpa/WDPA_Dec2025.pmtiles \
  --endpoint-url https://s3-west.nrp-nautilus.io

echo "PMTiles uploaded successfully!"
