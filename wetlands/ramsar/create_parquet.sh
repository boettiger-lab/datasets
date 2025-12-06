#!/bin/bash
# Convert Ramsar shapefile to GeoParquet

set -e

echo "Converting Ramsar shapefile to GeoParquet..."

# Use geopandas with encoding handling to clean and convert
python3 << 'EOPYTHON'
import geopandas as gpd
import warnings
warnings.filterwarnings('ignore')

# Read shapefile with Latin-1 encoding (common for shapefiles with special chars)
# Then write to parquet which will be UTF-8
gdf = gpd.read_file(
    'https://minio.carlboettiger.info/public-wetlands/ramsar/features_publishedPolygon.shp',
    encoding='latin1'
)

# Clean string columns - replace any invalid UTF-8
for col in gdf.select_dtypes(include=['object']):
    if col != 'geometry':
        gdf[col] = gdf[col].apply(lambda x: x.encode('utf-8', errors='replace').decode('utf-8') if isinstance(x, str) else x)

# Write to S3 as parquet
gdf.to_parquet(
    's3://public-wetlands/ramsar/ramsar_wetlands.parquet',
    compression='snappy'
)

print("GeoParquet created successfully!")
EOPYTHON
