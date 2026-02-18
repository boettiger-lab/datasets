# Multiple Source Files Workflow

The `cng-datasets` workflow now supports processing multiple source files that will be merged together into a single dataset.

## Use Cases

- Merging multiple shapefiles covering different regions
- Combining data from different time periods
- Consolidating multiple related datasets into one

## Usage

### Using CLI

#### Multiple Source Files with Workflow

```bash
cng-datasets workflow \
  --dataset my-merged-dataset \
  --source-url https://example.com/region1.shp \
  --source-url https://example.com/region2.shp \
  --source-url https://example.com/region3.shp \
  --bucket my-bucket \
  --h3-resolution 10 \
  --hex-memory 32Gi \
  --max-completions 200 \
  --max-parallelism 50 \
  --parent-resolutions "9,8,0"
```

#### Direct Conversion

```bash
cng-convert-to-parquet \
  https://example.com/source1.shp \
  https://example.com/source2.shp \
  https://example.com/source3.shp \
  s3://my-bucket/merged-output.parquet
```

## How It Works

1. **Multiple URLs**: Each `--source-url` adds another source to be processed
2. **UNION ALL**: Sources are merged using SQL `UNION ALL` semantics
3. **Schema Alignment**: All sources must have compatible schemas (matching column names and types)
4. **Feature Counting**: Total features are counted across all sources to calculate optimal chunking
5. **Single Output**: All sources are merged into one GeoParquet/PMTiles/H3 output

## Requirements

- All source files must have **compatible schemas** (column names and types must match)
- All sources should be in **similar formats** (don't mix parquet with shapefiles)
- For multi-layer sources (GDB, GPKG), use `--layer` to specify which layer to extract

## Limitations

- Cannot mix `.zip` files with other source types (extract first or process separately)
- Multiple parquet inputs not yet supported (use vector formats for merging)
- Layer parameter applies to all sources (all must have the same layer name if specified)

## Example: HydroBasins Continental Merge

HydroBasins provides data by continent. To create a global dataset:

```bash
cng-datasets workflow \
  --dataset hydrobasins-global-level04 \
  --source-url https://data.hydrosheds.org/africa/level04.shp \
  --source-url https://data.hydrosheds.org/asia/level04.shp \
  --source-url https://data.hydrosheds.org/europe/level04.shp \
  --source-url https://data.hydrosheds.org/north_america/level04.shp \
  --source-url https://data.hydrosheds.org/south_america/level04.shp \
  --bucket public-hydrobasins \
  --h3-resolution 8 \
  --hex-memory 16Gi
```

## Example: Multi-Region Protected Areas

Combining protected areas from different US states:

```bash
cng-datasets workflow \
  --dataset western-states-protected-areas \
  --source-url s3://my-data/california-pas.shp \
  --source-url s3://my-data/oregon-pas.shp \
  --source-url s3://my-data/washington-pas.shp \
  --bucket my-bucket \
  --h3-resolution 10
```

## Technical Details

- Sources are processed with DuckDB's `ST_Read` function
- The SQL query structure: `SELECT * FROM source1 UNION ALL SELECT * FROM source2 ...`
- CRS detection uses the first source file
- ID columns are auto-detected from the first source
- All reprojection and optimization steps apply to the merged result
