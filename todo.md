# Dataset Completion TODO

This document tracks the completion status of datasets in the boettiger-lab/datasets repository. Datasets are considered complete when they have:

**For Vector Datasets:**
- ✅ PMTiles format
- ✅ GeoParquet format
- ✅ H3 hexagonal tiling (partitioned by h0)
- ✅ Comprehensive data-description.md

**For Raster Datasets:**
- ✅ Cloud-Optimized GeoTIFF (COG)
- ✅ H3 hexagonal tiling (partitioned by h0)
- ✅ Comprehensive data-description.md

## ✅ Complete Datasets

### CPAD (California Protected Areas Database)
- **Status:** ✅ Complete
- **Type:** Vector
- **Formats:** PMTiles ✅, Parquet ✅, H3 Hex ✅
- **Documentation:** data-description.md ✅
- **Bucket:** `public-cpad`
- **Notes:** Excellent example with comprehensive documentation

### IUCN Species Richness
- **Status:** ✅ Complete (missing data-description.md)
- **Type:** Raster
- **Formats:** COG ✅, H3 Hex ✅
- **Documentation:** README.md ✅, data-description.md ❌
- **Bucket:** `public-iucn`
- **Action needed:** Create data-description.md following CPAD example

### WDPA (World Database of Protected Areas)
- **Status:** ✅ Complete (missing data-description.md)
- **Type:** Vector
- **Formats:** PMTiles ✅, Parquet ✅, H3 Hex ✅
- **Documentation:** README.md ✅, data-description.md ❌
- **Bucket:** `public-wdpa`
- **Action needed:** Create data-description.md following CPAD example

### Mapping Inequality (Redlining)
- **Status:** ✅ Complete (missing data-description.md)
- **Type:** Vector
- **Formats:** PMTiles ✅, Parquet ✅, H3 Hex ✅
- **Documentation:** README.md ✅, data-description.md ❌
- **Bucket:** `public-mappinginequality`
- **Action needed:** Create data-description.md following CPAD example

### NCP (Nature's Contributions to People)
- **Status:** ✅ Complete (missing data-description.md)
- **Type:** Raster
- **Formats:** COG ✅, H3 Hex ✅
- **Documentation:** README.md ✅, data-description.md ❌
- **Bucket:** `public-ncp`
- **Action needed:** Create data-description.md following CPAD example

## ⚠️ Partially Complete Datasets

### HydroBasins
- **Status:** ✅ Complete (missing data-description.md)
- **Type:** Vector
- **Formats:** PMTiles ✅, Parquet ✅, H3 Hex ✅
- **Documentation:** README.md ✅, data-description.md ❌
- **Bucket:** `public-hydrobasins`
- **Notes:** Levels 3-6 have H3 hexagonal tiling; also has zoom-based PMTiles (z0-z7)
- **Action needed:** Create data-description.md

### GBIF (Occurrence Data)
- **Status:** ✅ Complete (missing data-description.md + PMTiles)
- **Type:** Vector
- **Formats:** PMTiles ❌, Parquet ✅, H3 Hex ✅
- **Documentation:** README.md ✅, data-description.md ❌
- **Bucket:** `public-gbif`
- **Missing:**
  - PMTiles format for visualization
  - data-description.md

### US Census
- **Status:** ⚠️ Partially complete
- **Type:** Vector
- **Formats:** PMTiles ❌, Parquet ❓, H3 Hex ✅
- **Documentation:** README.md ❌, data-description.md ❌
- **Bucket:** `public-census`
- **Missing:**
  - PMTiles format
  - Standard parquet (only hex parquet exists)
  - All documentation

## ❌ Incomplete or Unpublished Datasets

### CalEnviroScreen
- **Status:** ❌ No published data
- **Type:** Vector
- **Bucket:** None found
- **Directory:** `calenviroscreen/`
- **Action needed:** Process and publish dataset

### Carbon
- **Status:** ✅ Complete (missing data-description.md)
- **Type:** Raster
- **Formats:** COG ✅, H3 Hex ✅
- **Documentation:** README.md ✅, data-description.md ❌
- **Bucket:** `public-carbon`
- **Action needed:** Create data-description.md

### Fire
- **Status:** ❌ No published data
- **Type:** Raster
- **Bucket:** `public-fire` (exists but empty)
- **Directory:** `fire/`
- **Action needed:** Process and publish dataset

### iNaturalist
- **Status:** ⚠️ Partially complete
- **Type:** Vector
- **Formats:** PMTiles ❌, Parquet ✅, H3 Hex ✅
- **Documentation:** README.md ❌, data-description.md ❌
- **Bucket:** `public-inat`
- **Notes:** Has hex data (taxonomic parquet files)
- **Missing:**
  - PMTiles format
  - All documentation

### Overture Maps
- **Status:** ⚠️ Partially complete
- **Type:** Vector
- **Formats:** PMTiles ❌, Parquet ✅, H3 Hex ✅
- **Documentation:** README.md ✅, data-description.md ❌
- **Bucket:** `public-overturemaps`
- **Notes:** Has hex data organized by regions
- **Missing:**
  - PMTiles format
  - data-description.md

### PAD-US
- **Status:** ❌ No published data
- **Type:** Vector
- **Bucket:** None found
- **Directory:** `pad-us/`
- **Action needed:** Process and publish dataset

### Social Vulnerability Index
- **Status:** ✅ Complete (missing data-description.md + hex)
- **Type:** Vector
- **Formats:** PMTiles ✅, Parquet ✅, H3 Hex ❌
- **Documentation:** README.md ❌, data-description.md ❌
- **Bucket:** `public-social-vulnerability`
- **Notes:** Has data for years 2000, 2010, 2020, 2022 at county and tract levels
- **Missing:**
  - H3 hexagonal tiling
  - All documentation

### Wetlands
- **Status:** ✅ Complete (missing data-description.md + hex for Ramsar)
- **Type:** Mixed (Vector + Raster)
- **Formats:** PMTiles ✅, Parquet ✅, COG ✅, H3 Hex ❌
- **Documentation:** README.md ✅, data-description.md ❌
- **Bucket:** `public-wetlands`
- **Notes:** Has Ramsar wetlands (parquet/pmtiles) and GLWD v2.0 (COG)
- **Missing:**
  - H3 hexagonal tiling for Ramsar sites
  - data-description.md

## Summary Statistics

- **Complete:** 1 dataset (CPAD)
- **Complete (missing data-description.md):** 7 datasets (IUCN, WDPA, Mapping Inequality, NCP, HydroBasins, Carbon, Wetlands)
- **Partially complete:** 4 datasets (GBIF, Census, iNaturalist, Overture Maps)
- **Complete but needs hex:** 1 dataset (Social Vulnerability Index)
- **Incomplete/Unpublished:** 3 datasets (CalEnviroScreen, Fire, PAD-US)
- **Total:** 16 datasets

## Priority Actions

1. **High Priority:** Create data-description.md for 7 datasets (IUCN, WDPA, Mapping Inequality, NCP, HydroBasins, Carbon, Wetlands)
2. **Medium Priority:** Add PMTiles to GBIF; Add hex tiling to Social Vulnerability Index; Add documentation to iNaturalist and Overture Maps
3. **Low Priority:** Process and publish the 3 incomplete datasets (CalEnviroScreen, Fire, PAD-US)
4. **Documentation:** Ensure all datasets follow the CPAD data-description.md template

## Notes

- CPAD serves as the gold standard for dataset completion
- Most datasets have good processing documentation in README.md
- Main gap is user-facing data-description.md files
- Several datasets have bucket placeholders but no published data
