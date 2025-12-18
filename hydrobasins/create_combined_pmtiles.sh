#!/bin/bash
# Create unified PMTiles from HydroBasins Levels 3-6 with zoom-based detail
# Zoom 0-1: Level 3 (major sub-continental watersheds)
# Zoom 2: Level 4
# Zoom 3-4: Level 5
# Zoom 5+: Level 6 (finest detail)

set -e

echo "=========================================="
echo "Creating Unified HydroBasins PMTiles"
echo "=========================================="

# Convert each level to GeoJSONSeq
echo ""
echo "Step 1/5: Converting Level 3 to GeoJSONSeq..."
ogr2ogr \
  -f GeoJSONSeq \
  /tmp/level_03.geojsonl \
  /vsis3/public-hydrobasins/level_03.parquet \
  -progress

echo ""
echo "Step 2/5: Converting Level 4 to GeoJSONSeq..."
ogr2ogr \
  -f GeoJSONSeq \
  /tmp/level_04.geojsonl \
  /vsis3/public-hydrobasins/level_04.parquet \
  -progress

echo ""
echo "Step 3/5: Converting Level 5 to GeoJSONSeq..."
ogr2ogr \
  -f GeoJSONSeq \
  /tmp/level_05.geojsonl \
  /vsis3/public-hydrobasins/level_05.parquet \
  -progress

echo ""
echo "Step 4/5: Converting Level 6 to GeoJSONSeq..."
ogr2ogr \
  -f GeoJSONSeq \
  /tmp/level_06.geojsonl \
  /vsis3/public-hydrobasins/level_06.parquet \
  -progress

echo ""
echo "✓ All GeoJSONSeq files created successfully!"
echo ""

# Generate unified PMTiles with zoom-based layers
# tippecanoe will combine these and automatically handle the zoom levels
echo "Step 5/5: Generating unified PMTiles with zoom-based detail..."
echo ""

tippecanoe \
  -o /tmp/hydrobasins_unified.pmtiles \
  -l hydrobasins \
  --drop-densest-as-needed \
  --extend-zooms-if-still-dropping \
  --force \
  -Z 0 -z 12 \
  -L level_03:/tmp/level_03.geojsonl \
  -L level_04:/tmp/level_04.geojsonl \
  -L level_05:/tmp/level_05.geojsonl \
  -L level_06:/tmp/level_06.geojsonl \
  --maximum-zoom-at-zero-density=1:3 \
  --maximum-zoom-at-zero-density=2:4 \
  --maximum-zoom-at-zero-density=3:5 \
  --maximum-zoom-at-zero-density=4:5 \
  --minimum-zoom=1:0 \
  --minimum-zoom=2:2 \
  --minimum-zoom=3:3 \
  --minimum-zoom=4:5

echo ""
echo "✓ Unified PMTiles created successfully!"
echo ""

# Upload to S3
echo "Uploading unified PMTiles to S3..."

# Configure mc alias if not already configured
mc alias set s3 https://${AWS_PUBLIC_ENDPOINT} ${AWS_ACCESS_KEY_ID} ${AWS_SECRET_ACCESS_KEY}

mc cp /tmp/hydrobasins_unified.pmtiles s3/public-hydrobasins/hydrobasins_unified.pmtiles

echo ""
echo "✓ PMTiles uploaded successfully to s3://public-hydrobasins/hydrobasins_unified.pmtiles"
echo ""

# Clean up temporary files
echo "Cleaning up temporary files..."
rm -f /tmp/level_03.geojsonl \
      /tmp/level_04.geojsonl \
      /tmp/level_05.geojsonl \
      /tmp/level_06.geojsonl \
      /tmp/hydrobasins_unified.pmtiles

echo ""
echo "=========================================="
echo "✓ Unified PMTiles generation complete!"
echo "=========================================="
echo ""
echo "Zoom levels:"
echo "  0-1: Level 3 (major sub-continental watersheds)"
echo "  2:   Level 4"
echo "  3-4: Level 5"
echo "  5+:  Level 6 (finest detail)"
echo ""
