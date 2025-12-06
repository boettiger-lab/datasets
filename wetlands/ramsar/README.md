

# Ramsar Wetlands

## Source Data

Original shapefile: `https://minio.carlboettiger.info/public-wetlands/ramsar/features_publishedPolygon.shp`

Ramsar wetlands polygons (7,401 features) representing Wetlands of International Importance.

## Processing Pipeline

### 1. GeoParquet Conversion

Convert the shapefile to GeoParquet format:

**Script:** `create_parquet.sh`  
**Job:** `parquet-job.yaml`

```bash
kubectl apply -f wetlands/ramsar/parquet-job.yaml
```

**Output:** `s3://public-wetlands/ramsar/ramsar_wetlands.parquet`

### 2. PMTiles Generation

Create vector tiles for web mapping:

**Script:** `create_pmtiles.sh`  
**Job:** `pmtiles-job.yaml`

The script converts GeoParquet → GeoJSONSeq → PMTiles using tippecanoe.

```bash
kubectl apply -f wetlands/ramsar/pmtiles-job.yaml
```

**Output:** `s3://public-wetlands/ramsar/ramsar_wetlands.pmtiles`

### 3. H3 Hexagon Processing

Process polygons into H3 resolution 8 hexagons:

**Script:** `vec.py`  
**Job:** `hex-job.yaml`

Processes the data in 150 indexed chunks (50 features per chunk) with 30 parallel workers.

```bash
kubectl apply -f wetlands/ramsar/hex-job.yaml
```

**Output:** `s3://public-wetlands/ramsar/chunks/chunk_*.parquet` (~148 files)

### 4. Repartitioning by H3 Resolution 0

Reorganize hex chunks into hive-partitioned format by h0 cell:

**Script:** `repartition.py`  
**Job:** `repartition-job.yaml`

Reads all chunks and writes with h0 partitioning for efficient spatial queries.

```bash
kubectl apply -f wetlands/ramsar/repartition-job.yaml
```

**Output:** `s3://public-wetlands/ramsar/hex/h0=*/` (hive-partitioned by h0)

## Final Outputs

- **GeoParquet:** `s3://public-wetlands/ramsar/ramsar_wetlands.parquet` - Full dataset with all attributes
- **PMTiles:** `s3://public-wetlands/ramsar/ramsar_wetlands.pmtiles` - Vector tiles for web mapping
- **H3 Hexagons:** `s3://public-wetlands/ramsar/hex/` - Hive-partitioned by h0, resolution 8 hexagons

## Attributes

Key fields in the dataset:
- `ramsarid`: Ramsar site ID
- `officialna`: Official name of the wetland
- `iso3`: Country code
- `country_en`: Country name
- `area_off`: Official area
 