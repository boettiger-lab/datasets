#!/bin/bash
# Convert Redlining GPKG to GeoParquet

set -e

echo "Downloading Redlining GPKG..."

# Download the GPKG file
curl -L -o /tmp/mappinginequality.gpkg \
  "https://dsl.richmond.edu/panorama/redlining/static/mappinginequality.gpkg"

echo "Converting GPKG to GeoParquet..."

# Convert GPKG to GeoParquet
ogr2ogr \
  -f Parquet \
  /vsis3/public-redlining/mappinginequality.parquet \
  /tmp/mappinginequality.gpkg \
  -progress

echo "Cleaning up local file..."
rm /tmp/mappinginequality.gpkg

echo "GeoParquet created successfully!"
