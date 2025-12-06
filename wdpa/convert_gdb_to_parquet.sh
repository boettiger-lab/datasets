#!/bin/bash
# Convert WDPA GDB to GeoParquet and generate PMTiles

set -e

echo "Converting WDPA GDB to GeoParquet..."

# Convert GDB layer to GeoParquet
ogr2ogr \
  -f Parquet \
  /vsis3/public-wdpa/WDPA_Dec2025.parquet \
  /vsis3/public-wdpa/WDPA_Dec2025_Public.gdb \
  WDPA_poly_Dec2025 \
  -progress

echo "GeoParquet created successfully!"

# Download the GeoParquet locally for tippecanoe
echo "Downloading GeoParquet for PMTiles generation..."
aws s3 cp s3://public-wdpa/WDPA_Dec2025.parquet /tmp/WDPA_Dec2025.parquet \
  --endpoint-url https://s3-west.nrp-nautilus.io

# Generate PMTiles from the GeoParquet
echo "Generating PMTiles from GeoParquet..."

tippecanoe \
  -o /tmp/WDPA_Dec2025.pmtiles \
  -l wdpa \
  --drop-densest-as-needed \
  --extend-zooms-if-still-dropping \
  --force \
  /tmp/WDPA_Dec2025.parquet

echo "PMTiles created successfully!"

# Upload PMTiles to S3
echo "Uploading PMTiles to S3..."

aws s3 cp /tmp/WDPA_Dec2025.pmtiles s3://public-wdpa/WDPA_Dec2025.pmtiles \
  --endpoint-url https://s3-west.nrp-nautilus.io

echo "All conversions complete!"
