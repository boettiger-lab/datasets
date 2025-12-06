#!/bin/bash
# Convert Ramsar shapefile to GeoParquet

set -e

echo "Converting Ramsar shapefile to GeoParquet..."

# Convert shapefile to GeoParquet
# Force recoding to UTF-8 to handle special characters
ogr2ogr \
  -f Parquet \
  /vsis3/public-wetlands/ramsar/ramsar_wetlands.parquet \
  /vsicurl/https://minio.carlboettiger.info/public-wetlands/ramsar/features_publishedPolygon.shp \
  -lco ENCODING=UTF-8 \
  -oo ENCODING=UTF-8 \
  -progress

echo "GeoParquet created successfully!"
