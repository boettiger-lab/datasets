# Global Wetlands Datasets

This collection includes two major global wetland datasets:
1. **Ramsar Sites Information Service:** Polygons of wetlands of international importance.
2. **Global Lakes and Wetlands Database (GLWD) v2.0:** Raster maps of wetland extent and types.

## Assets

### 1. Ramsar Sites (Vector)
**File:** `s3://public-wetlands/ramsar/ramsar_wetlands.parquet`
Partitioned Hexagons: `s3://public-wetlands/ramsar/hex/` (h0)
Web Map Tiles: `s3://public-wetlands/ramsar/ramsar_wetlands.pmtiles`

**Schema:**
- `ramsarid`: Unique ID
- `Site name`: Official name
- `Country`: Country code
- `Designation date`: Date of designation
- `Area (ha)`: Official area
- `Wetland Type`: Classification code
- `Ecosystem services`: List of services provided
- `Threats`: List of threats
- `geometry`: Polygon boundary

### 2. GLWD v2.0 (Raster)
**Files:** `s3://public-wetlands/GLWD_v2_0/`

A collection of Cloud-Optimized GeoTIFFs (COGs) representing wetland area (migrations of v1.0 polygons to 30-arc-second raster).
- `GLWD_v2_0_area_ha_x10_cog.tif`: Total wetland area (hectares * 10).
- `GLWD_v2_0_area_by_class_ha/*.tif`: Wetland area by specific class.
- `GLWD_v2_0_combined_classes/*.tif`: aggregated layers.

## Source

- **Ramsar:** [Ramsar Sites Information Service](https://rsis.ramsar.org/)
- **GLWD:** Lehner, B. and Döll, P. (2004). Development and validation of a global database of lakes, reservoirs and wetlands. *Journal of Hydrology*, 296/1-4: 1-22. [WWF/GWSP](https://www.worldwildlife.org/pages/global-lakes-and-wetlands-database)

## License

- **Ramsar:** [Ramsar Copyright](https://www.ramsar.org/terms-use)
- **GLWD:** [WWF License](https://www.worldwildlife.org/pages/global-lakes-and-wetlands-database)
