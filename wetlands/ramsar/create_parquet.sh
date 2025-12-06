#!/bin/bash
# Convert Ramsar shapefile to GeoParquet

set -e

echo "Converting Ramsar shapefile to GeoParquet..."

# Convert shapefile to GeoParquet
# Use RECODING to force proper UTF-8 encoding from shapefile
export SHAPE_ENCODING="UTF-8"
export CPL_DEBUG=ON

ogr2ogr \
  -f Parquet \
  /vsis3/public-wetlands/ramsar/ramsar_wetlands.parquet \
  /vsicurl/https://minio.carlboettiger.info/public-wetlands/ramsar/features_publishedPolygon.shp \
  -lco ENCODING=UTF-8 \
  --config SHAPE_ENCODING UTF-8 \
  --config OGR_FORCE_ASCII NO \
  -progress

echo "GeoParquet created successfully!"
