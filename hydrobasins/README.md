# HydroBasins Global Watershed Boundaries

## Overview

This directory contains processed global watershed boundary data from HydroBasins, organized by hierarchical basin levels. The data has been compiled from multiple continental datasets into unified global layers.

## Data Source

**Original Data:** HydroSHEDS HydroBasins v1c  
**Source URL:** https://www.hydrosheds.org/products/hydrobasins  
**Download Date:** December 2, 2025  
**Data Version:** v1c  
**Spatial Reference:** WGS 84 (EPSG:4326)  

## Description

HydroBasins provides a series of polygon layers that depict watershed boundaries at different hierarchical levels (also known as Pfafstetter levels). Level 1 represents the largest continental-scale basins, while Level 12 represents the finest sub-basin delineations.

The dataset uses the Pfafstetter coding system, which provides a systematic method for assigning IDs to hydrographic units based on the topology of the drainage network.

## Geographic Coverage

The compiled dataset includes HydroBasins data from all available continents:
- Africa (af)
- Arctic (ar)
- Asia (as)
- Australia/Oceania (au)
- Europe (eu)
- Greenland (gr)
- North America (na)
- South America (sa)
- Siberia (si)

Global extent: Longitude -180° to 180°, Latitude -55.99° to 83.63°

## File Structure

### GeoPackage (combined_hydrobasins.gpkg)

A single multi-layer GeoPackage containing all 12 hierarchical levels:

| Layer Name | Feature Count | Description |
|------------|--------------|-------------|
| level_01   | 10           | Coarsest resolution - major global basins |
| level_02   | 62           | Continental-scale basins |
| level_03   | 292          | Large regional basins |
| level_04   | 1,342        | Regional basins |
| level_05   | 4,734        | Sub-regional basins |
| level_06   | 16,397       | Major watersheds |
| level_07   | 57,646       | Watersheds |
| level_08   | 190,675      | Sub-watersheds |
| level_09   | 508,190      | Fine sub-watersheds |
| level_10   | 941,012      | Very fine sub-watersheds |
| level_11   | 1,031,785    | Near-finest resolution |
| level_12   | 1,034,083    | Finest resolution - detailed sub-basins |

**File Size:** 7.7 GB

### GeoParquet Files (level_*.parquet)

Individual GeoParquet files for each hierarchical level, optimized for cloud-native spatial data access:

| File Name           | Size  | Feature Count | Use Case |
|---------------------|-------|---------------|----------|
| level_01.parquet    | 19M   | 10            | Global/continental analysis |
| level_02.parquet    | 25M   | 62            | Continental basins |
| level_03.parquet    | 32M   | 292           | Large regional basins |
| level_04.parquet    | 52M   | 1,342         | Regional analysis |
| level_05.parquet    | 81M   | 4,734         | Sub-regional watersheds |
| level_06.parquet    | 130M  | 16,397        | Major watershed analysis |
| level_07.parquet    | 214M  | 57,646        | Watershed-scale studies |
| level_08.parquet    | 353M  | 190,675       | Sub-watershed analysis |
| level_09.parquet    | 550M  | 508,190       | Fine-scale watershed studies |
| level_10.parquet    | 754M  | 941,012       | Very detailed basin analysis |
| level_11.parquet    | 793M  | 1,031,785     | High-resolution studies |
| level_12.parquet    | 794M  | 1,034,083     | Maximum detail analysis |

**Total GeoParquet Size:** ~3.8 GB (compressed)

### PMTiles Files (level_*.pmtiles)

Individual PMTiles files for each hierarchical level, optimized for web mapping and tile serving:

| File Name           | Zoom Range | Feature Count | Use Case |
|---------------------|------------|---------------|----------|
| level_01.pmtiles    | 0-4        | 10            | Global/continental web maps |
| level_02.pmtiles    | 0-5        | 62            | Continental basin visualization |
| level_03.pmtiles    | 0-6        | 292           | Large regional web maps |
| level_04.pmtiles    | 0-7        | 1,342         | Regional web mapping |
| level_05.pmtiles    | 0-8        | 4,734         | Sub-regional web maps |
| level_06.pmtiles    | 0-9        | 16,397        | Watershed web visualization |
| level_07.pmtiles    | 0-10       | 57,646        | Detailed web maps |
| level_08.pmtiles    | 0-11       | 190,675       | High-detail web mapping |
| level_09.pmtiles    | 0-12       | 508,190       | Very detailed web maps |
| level_10.pmtiles    | 0-13       | 941,012       | Maximum detail web maps |
| level_11.pmtiles    | 0-14       | 1,031,785     | Ultra-high resolution web maps |
| level_12.pmtiles    | 0-14       | 1,034,083     | Finest resolution web maps |

PMTiles is a cloud-native archive format for tiled map data, enabling efficient serverless map tile hosting.

### Source Shapefiles

The original downloaded shapefiles are organized in continent-specific subdirectories:
```
africa/          - African basins (levels 01-12)
arctic/          - Arctic region basins (levels 01-12)
asia/            - Asian basins (levels 01-12)
australia/       - Australian/Oceania basins (levels 01-12)
europe/          - European basins (levels 01-12)
greenland/       - Greenland basins (levels 01-12)
north_america/   - North American basins (levels 01-12)
south_america/   - South American basins (levels 01-12)
siberia/         - Siberian basins (levels 01-12)
```

Each continent directory contains 12 shapefiles (one per level) with associated .shx, .dbf, and .prj files.

## Attributes

Each basin polygon contains the following attributes:

| Attribute  | Type    | Description |
|------------|---------|-------------|
| HYBAS_ID   | Integer | Unique basin identifier |
| NEXT_DOWN  | Integer | ID of the next downstream basin |
| NEXT_SINK  | Integer | ID of the most downstream basin (sink) |
| MAIN_BAS   | Integer | ID of the main basin |
| DIST_SINK  | Float   | Distance to sink (outlet) in km |
| DIST_MAIN  | Float   | Distance to main basin outlet in km |
| SUB_AREA   | Float   | Sub-basin area in km² |
| UP_AREA    | Float   | Upstream area in km² |
| PFAF_ID    | Integer | Pfafstetter code |
| ENDO       | Integer | Endorheic basin flag (1=yes, 0=no) |
| COAST      | Integer | Coastal basin flag (1=yes, 0=no) |
| ORDER      | Integer | Strahler stream order |
| SORT       | Integer | Topological sort order |

## Data Processing

### Processing Date
December 2, 2025

### Processing Steps

1. **Download:** Retrieved HydroBasins v1c shapefiles for all 9 continents from hydrosheds.org
2. **Extraction:** Unzipped all continental datasets (108 shapefiles total: 9 continents × 12 levels)
3. **Consolidation:** Merged continental datasets by level into unified global layers
4. **GeoPackage Creation:** Combined all levels into a single multi-layer GeoPackage
5. **GeoParquet Export:** Generated individual GeoParquet files for each level

### Tools Used
- `wget` - Data download
- `unzip` - Archive extraction  
- `ogr2ogr` (GDAL 3.x) - Shapefile to GeoPackage conversion
- `geopandas` (Python) - GeoParquet generation
- `pyarrow` - Parquet file writing

## Usage Examples

### Load GeoPackage in Python
```python
import geopandas as gpd

# Read a specific level
basins = gpd.read_file('combined_hydrobasins.gpkg', layer='level_06')

# List all layers
import fiona
with fiona.open('combined_hydrobasins.gpkg') as src:
    print(src.listlayers())
```

### Load GeoParquet in Python
```python
import geopandas as gpd

# Read a specific level (faster than GeoPackage)
basins = gpd.read_parquet('level_06.parquet')
```

### Load in R
```r
library(sf)

# Read from GeoPackage
basins <- st_read("combined_hydrobasins.gpkg", layer = "level_06")

# Read from GeoParquet
library(arrow)
basins <- read_parquet("level_06.parquet") %>% st_as_sf()
```

### Display PMTiles in Web Map
```html
<!-- Using MapLibre GL JS -->
<script src="https://unpkg.com/maplibre-gl@3/dist/maplibre-gl.js"></script>
<script src="https://unpkg.com/pmtiles@3/dist/pmtiles.js"></script>

<script>
let protocol = new pmtiles.Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);

const map = new maplibregl.Map({
  container: 'map',
  style: 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json',
  center: [0, 20],
  zoom: 2
});

map.on('load', () => {
  map.addSource('hydrobasins', {
    type: 'vector',
    url: 'pmtiles://https://your-domain.com/level_06.pmtiles'
  });
  
  map.addLayer({
    id: 'basins',
    type: 'line',
    source: 'hydrobasins',
    'source-layer': 'level_06',
    paint: {
      'line-color': '#0080ff',
      'line-width': 1
    }
  });
});
</script>
```

### Query with DuckDB
```sql
-- Query GeoParquet directly
SELECT HYBAS_ID, SUB_AREA, UP_AREA, PFAF_ID 
FROM read_parquet('level_08.parquet')
WHERE UP_AREA > 10000
ORDER BY UP_AREA DESC;
```

## Choosing the Right Level

- **Levels 1-3:** Global to continental-scale analysis, biogeographic studies
- **Levels 4-6:** Regional watershed analysis, large-scale hydrological modeling
- **Levels 7-9:** Watershed-scale ecosystem analysis, medium-resolution modeling
- **Levels 10-12:** Detailed sub-basin analysis, high-resolution hydrological studies

## Citations

When using this data, please cite:

**HydroSHEDS Database:**
> Lehner, B., Grill G. (2013). Global river hydrography and network routing: baseline data and new approaches to study the world's large river systems. Hydrological Processes, 27(15): 2171-2186. https://doi.org/10.1002/hyp.9740

**HydroBasins:**
> Lehner, B., Grill G. (2013). Global river hydrography and network routing: baseline data and new approaches to study the world's large river systems. Hydrological Processes, 27(15): 2171-2186.

## License

This dataset is derived from HydroSHEDS, which is available for non-commercial use. Please refer to the [HydroSHEDS website](https://www.hydrosheds.org) for the most current licensing information.

## Notes

- The GeoPackage format includes some MULTIPOLYGON geometries in POLYGON layers, which is handled by the GDAL driver but may produce warnings.
- GeoParquet files are more efficient for cloud-based analysis and selective spatial queries.
- All data uses WGS 84 geographic coordinates (EPSG:4326).

## Contact

For questions about this processed dataset, please contact the repository maintainers.  
For questions about the original HydroBasins data, visit https://www.hydrosheds.org
