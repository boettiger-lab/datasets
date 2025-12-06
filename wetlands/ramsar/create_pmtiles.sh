#!/bin/bash
# Create PMTiles from Ramsar GeoParquet

set -e

echo "Converting GeoParquet to GeoJSONSeq for tippecanoe..."

# Convert parquet to GeoJSONSeq (which tippecanoe can read)
# Set encoding options to handle UTF-8 properly
ogr2ogr \
  -f GeoJSONSeq \
  /tmp/ramsar_wetlands.geojsonl \
  /vsis3/public-wetlands/ramsar/ramsar_wetlands.parquet \
  -lco ENCODING=UTF-8 \
  -progress

echo "GeoJSONSeq created successfully!"

# Generate PMTiles from the GeoJSONSeq
echo "Generating PMTiles from GeoJSONSeq..."

# Use --read-parallel to handle encoding issues more gracefully
tippecanoe \
  -o /tmp/ramsar_wetlands.pmtiles \
  -l ramsar \
  --drop-densest-as-needed \
  --extend-zooms-if-still-dropping \
  --force \
  --attribute-type=officialna:string \
  /tmp/ramsar_wetlands.geojsonl

echo "PMTiles created successfully!"

# Upload PMTiles to S3
echo "Uploading PMTiles to S3..."

aws s3 cp /tmp/ramsar_wetlands.pmtiles s3://public-wetlands/ramsar/ramsar_wetlands.pmtiles \
  --endpoint-url https://s3-west.nrp-nautilus.io

echo "PMTiles uploaded successfully!"
