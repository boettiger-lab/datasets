#!/bin/bash
# Convert Redlining GPKG to GeoParquet

set -e

echo "Converting GPKG to GeoParquet (reading directly from source)..."

# Convert GPKG to GeoParquet - read directly via vsicurl, no download needed
ogr2ogr \
  -f Parquet \
  /vsis3/${BUCKET}/mappinginequality.parquet \
  /vsicurl/https://dsl.richmond.edu/panorama/redlining/static/mappinginequality.gpkg \
  -progress

echo "GeoParquet created successfully!"
