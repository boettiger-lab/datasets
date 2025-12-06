#!/bin/bash
# Convert Ramsar shapefile to GeoParquet

set -e

echo "Converting Ramsar shapefile to GeoParquet with UTF-8 cleanup..."

# Use a Python script to read and clean the data
python3 << 'EOPYTHON'
import duckdb

con = duckdb.connect()
con.install_extension("spatial")
con.load_extension("spatial")

# Read shapefile with invalid UTF-8 replacement
con.execute("SET invalid_utf8='REPLACE'")

# Read shapefile
con.execute("""
    COPY (
        SELECT * 
        FROM ST_Read('/vsicurl/https://minio.carlboettiger.info/public-wetlands/ramsar/features_publishedPolygon.shp')
    ) TO '/vsis3/public-wetlands/ramsar/ramsar_wetlands.parquet' 
    (FORMAT PARQUET)
""")

print("GeoParquet created successfully with UTF-8 cleanup!")
EOPYTHON
