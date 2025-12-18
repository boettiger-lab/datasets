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
# We'll create separate tilesets and then merge them
echo "Step 5/5: Generating PMTiles with zoom-based detail..."
echo ""

# Level 3: Zoom 0-1
echo "  - Creating Level 3 tiles (zoom 0-1)..."
tippecanoe \
  -o /tmp/level_03.pmtiles \
  -l hydrobasins \
  --drop-densest-as-needed \
  --force \
  -Z 0 -z 1 \
  /tmp/level_03.geojsonl

# Level 4: Zoom 2
echo "  - Creating Level 4 tiles (zoom 2)..."
tippecanoe \
  -o /tmp/level_04.pmtiles \
  -l hydrobasins \
  --drop-densest-as-needed \
  --force \
  -Z 2 -z 2 \
  /tmp/level_04.geojsonl

# Level 5: Zoom 3-4
echo "  - Creating Level 5 tiles (zoom 3-4)..."
tippecanoe \
  -o /tmp/level_05.pmtiles \
  -l hydrobasins \
  --drop-densest-as-needed \
  --force \
  -Z 3 -z 4 \
  /tmp/level_05.geojsonl

# Level 6: Zoom 5+
echo "  - Creating Level 6 tiles (zoom 5+)..."
tippecanoe \
  -o /tmp/level_06.pmtiles \
  -l hydrobasins \
  --drop-densest-as-needed \
  --extend-zooms-if-still-dropping \
  --force \
  -Z 5 -z 12 \
  /tmp/level_06.geojsonl

# Merge all PMTiles into a unified tileset
echo "  - Merging all levels into unified tileset..."
tile-join \
  -o /tmp/hydrobasins_unified.pmtiles \
  --force \
  /tmp/level_03.pmtiles \
  /tmp/level_04.pmtiles \
  /tmp/level_05.pmtiles \
  /tmp/level_06.pmtiles

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
      /tmp/level_03.pmtiles \
      /tmp/level_04.pmtiles \
      /tmp/level_05.pmtiles \
      /tmp/level_06.pmtiles \
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
