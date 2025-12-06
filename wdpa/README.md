

# World Database of Protected Areas (WDPA)

## Source Data

Original geodatabase: `s3://public-wdpa/WDPA_Dec2025_Public.gdb`

We process the polygon data only, layer `WDPA_poly_Dec2025` (296,046 features).

## Processing Pipeline

### 1. GeoParquet Conversion

Convert the GDB layer to GeoParquet format:

**Script:** `create_parquet.sh`  
**Job:** `parquet-job.yaml`

```bash
kubectl apply -f wdpa/parquet-job.yaml
```

**Output:** `s3://public-wdpa/WDPA_Dec2025.parquet` (5.1 GB)

### 2. PMTiles Generation

Create vector tiles for web mapping:

**Script:** `create_pmtiles.sh`  
**Job:** `pmtiles-job.yaml`

The script converts GeoParquet → GeoJSONSeq → PMTiles using tippecanoe.

```bash
kubectl apply -f wdpa/pmtiles-job.yaml
```

**Output:** `s3://public-wdpa/WDPA_Dec2025.pmtiles`

### 3. H3 Hexagon Processing

Process polygons into H3 resolution 8 hexagons:

**Script:** `vec.py`  
**Job:** `hex-job.yaml`

Processes the data in 200 indexed chunks (1,500 features per chunk) with 50 parallel workers.

```bash
kubectl apply -f wdpa/hex-job.yaml
```

**Output:** `s3://public-wdpa/chunks/chunk_*.parquet` (200 files)

### 4. Repartitioning by H3 Resolution 0

Reorganize hex chunks into hive-partitioned format by h0 cell:

**Script:** `repartition.py`  
**Job:** `repartition-job.yaml`

Reads all chunks and writes with h0 partitioning for efficient spatial queries.

```bash
kubectl apply -f wdpa/repartition-job.yaml
```

**Output:** `s3://public-wdpa/hex/h0=*/` (hive-partitioned by h0)

## Final Outputs

- **GeoParquet:** `s3://public-wdpa/WDPA_Dec2025.parquet` - Full dataset with all attributes
- **PMTiles:** `s3://public-wdpa/WDPA_Dec2025.pmtiles` - Vector tiles for web mapping
- **H3 Hexagons:** `s3://public-wdpa/hex/` - Hive-partitioned by h0, resolution 8 hexagons

## Bucket Setup

```bash
mc anonymous set download nvme/public-wdpa
mc mb nrp/public-wdpa
mc anonymous set download nrp/public-wdpa
mc cp -r nvme/public-wdpa/ nrp/public-wdpa
```

