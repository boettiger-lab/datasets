#!/bin/bash
# Script to generate PMTiles from HydroBasins GeoPackage
# Requires: tippecanoe (https://github.com/felt/tippecanoe)
# Requires: gdal/ogr2ogr for GeoPackage conversion

set -euo pipefail

GPKG="combined_hydrobasins.gpkg"

if [ ! -f "$GPKG" ]; then
    wget https://minio.carlboettiger.info/public-hydrobasins/combined_hydrobasins.gpkg
fi

OUTPUT_DIR="pmtiles"

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo "Starting PMTiles generation from $GPKG"
echo "Output directory: $OUTPUT_DIR"
echo ""

# Check if GPKG exists
if [ ! -f "$GPKG" ]; then
    echo "Error: $GPKG not found!"
    exit 1
fi

OUTPUT="$OUTPUT_DIR/hydrobasins.pmtiles"

echo "Generating PMTiles with level-appropriate zoom ranges..."
echo "  Zoom 0:     level_01 (largest basins - global view)"
echo "  Zoom 1:     level_01 + level_02 combined"
echo "  Zoom 2:     level_03"
echo "  Zoom 3:     level_04"
echo "  Zoom 4:     level_05"
echo "  Zoom 5:     level_06"
echo "  Zoom 6:     level_07"
echo "  Zoom 7+:    level_08 (finest detail for this pass)"
echo ""

for var_name in AWS_S3_ENDPOINT MINIO_KEY MINIO_SECRET; do
    if [ -z "${!var_name:-}" ]; then
        echo "Error: $var_name is not set."
        exit 1
    fi
done

if ! command -v mc >/dev/null 2>&1; then
    echo "Error: MinIO client (mc) is not installed or not on PATH."
    exit 1
fi

echo "Configuring MinIO client..."
mc alias set s3 "http://${AWS_S3_ENDPOINT}" "${AWS_ACCESS_KEY_ID}" "${AWS_SECRET_ACCESS_KEY}" >/dev/null

upload_pmtiles() {
    local file_path="$1"
    local base_name
    base_name=$(basename "$file_path")
    echo "  ↥ Uploading ${base_name} to s3/public-hydrobasins/"
    mc cp "$file_path" s3/public-hydrobasins/ >/dev/null
}

# Create temporary directory for GeoJSON files
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

# Convert each level to GeoJSON with zoom range metadata
for level in 01 02 03 04 05 06 07 08; do
    LAYER="level_$level"
    echo "Extracting $LAYER..."
    ogr2ogr -f GeoJSON "$TEMP_DIR/${LAYER}.geojson" "$GPKG" "$LAYER"
done

# Combine all levels into single PMTiles with appropriate zoom ranges
# Show finer detail earlier - users should see sub-basins sooner

# Combine all levels into single PMTiles with appropriate zoom ranges
# Show progressively finer detail at each zoom level

echo "Creating zoom 0 tiles from level_01..."
tippecanoe -o "$OUTPUT_DIR/z0.pmtiles" -Z0 -z0 -l hydrobasins --force \
    --name="HydroBasins" --attribution="HydroSHEDS/HydroBasins" \
    --no-tile-size-limit --drop-densest-as-needed \
    --quiet \
    "$TEMP_DIR/level_01.geojson"
upload_pmtiles "$OUTPUT_DIR/z0.pmtiles"

echo "Creating zoom 1 tiles from level_01 + level_02 combined..."
tippecanoe -o "$OUTPUT_DIR/z1.pmtiles" -Z1 -z1 -l hydrobasins --force \
    --no-tile-size-limit --drop-densest-as-needed \
    --quiet \
    "$TEMP_DIR/level_01.geojson" "$TEMP_DIR/level_02.geojson"
upload_pmtiles "$OUTPUT_DIR/z1.pmtiles"

echo "Creating zoom 2 tiles from level_03..."
tippecanoe -o "$OUTPUT_DIR/z2.pmtiles" -Z2 -z2 -l hydrobasins --force \
    --no-tile-size-limit --drop-densest-as-needed \
    --quiet \
    "$TEMP_DIR/level_03.geojson"
upload_pmtiles "$OUTPUT_DIR/z2.pmtiles"

echo "Creating zoom 3 tiles from level_04..."
tippecanoe -o "$OUTPUT_DIR/z3.pmtiles" -Z3 -z3 -l hydrobasins --force \
    --no-tile-size-limit --drop-densest-as-needed \
    --quiet \
    "$TEMP_DIR/level_04.geojson"
upload_pmtiles "$OUTPUT_DIR/z3.pmtiles"

echo "Creating zoom 4 tiles from level_05..."
tippecanoe -o "$OUTPUT_DIR/z4.pmtiles" -Z4 -z4 -l hydrobasins --force \
    --no-tile-size-limit --drop-densest-as-needed \
    --quiet \
    "$TEMP_DIR/level_05.geojson"
upload_pmtiles "$OUTPUT_DIR/z4.pmtiles"

echo "Creating zoom 5 tiles from level_06..."
tippecanoe -o "$OUTPUT_DIR/z5.pmtiles" -Z5 -z5 -l hydrobasins --force \
    --no-tile-size-limit --drop-densest-as-needed \
    --quiet \
    "$TEMP_DIR/level_06.geojson"
upload_pmtiles "$OUTPUT_DIR/z5.pmtiles"

echo "Creating zoom 6 tiles from level_07..."
tippecanoe -o "$OUTPUT_DIR/z6.pmtiles" -Z6 -z6 -l hydrobasins --force \
    --no-tile-size-limit --drop-densest-as-needed \
    --quiet \
    "$TEMP_DIR/level_07.geojson"
upload_pmtiles "$OUTPUT_DIR/z6.pmtiles"

echo "Creating zoom 7+ tiles from level_08..."
tippecanoe -o "$OUTPUT_DIR/z7.pmtiles" -Z7 -z14 -l hydrobasins --force \
    --no-tile-size-limit --drop-densest-as-needed \
    --quiet \
    "$TEMP_DIR/level_08.geojson"
upload_pmtiles "$OUTPUT_DIR/z7.pmtiles"

echo ""
echo "✓ All individual zoom level PMTiles created and uploaded"
echo "  Note: Using levels 1-8 with 1:1 zoom mapping for detailed progression"
echo ""
