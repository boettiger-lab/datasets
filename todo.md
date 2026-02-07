# Dataset Completion Status

This document tracks the completion status of datasets in the boettiger-lab/datasets repository. Datasets are considered complete when they have:

**File Formats:**
- ✅ **PMTiles** (for visualization)
- ✅ **GeoParquet** (for analysis)
- ✅ **H3 Hexagons** (partitioned by h0)
- ✅ **Cloud-Optimized GeoTIFF (COG)** (for rasters)

**Documentation & Metadata:**
- ✅ **README.md** (Comprehensive description & source info)
- ✅ **STAC Collection** (`stac-collection.json` with correct links)
- ✅ **Column Definitions** (`table:columns` schema in STAC/README)

**Standard Workflow:** See [DATASET_DOCUMENTATION_WORKFLOW.md](DATASET_DOCUMENTATION_WORKFLOW.md).

---

## ✅ Complete Datasets

### CPAD (California Protected Areas Database)
- **Status:** 🌟 Gold Standard
- **Type:** Vector
- **Bucket:** `public-cpad`
- **Formats:**
  - ✅ PMTiles
  - ✅ GeoParquet
  - ✅ H3 Hexagons
- **Documentation:**
  - ✅ README.md (Comprehensive)
  - ✅ STAC Collection
  - ✅ Column Definitions

### IUCN Species Richness
- **Status:** ✅ Complete
- **Type:** Raster
- **Bucket:** `public-iucn`
- **Formats:**
  - ✅ COG (14 layers)
  - ✅ H3 Hexagons
- **Documentation:**
  - ✅ README.md (Comprehensive)
  - ✅ STAC Collection
  - ✅ Column Definitions (Richness, Threatened, Range-Weighted)

### WDPA (World Database on Protected Areas)
- **Status:** ✅ Complete
- **Type:** Vector
- **Bucket:** `public-wdpa`
- **Formats:**
  - ✅ PMTiles
  - ✅ GeoParquet
  - ✅ H3 Hexagons
- **Documentation:**
  - ✅ README.md (Comprehensive)
  - ✅ STAC Collection
  - ✅ Column Definitions (Full WDPA Dictionary)

### Carbon (Irrecoverable/Manageable)
- **Status:** ✅ Complete
- **Type:** Raster
- **Bucket:** `public-carbon`
- **Formats:**
  - ✅ COG (18 files)
  - ✅ H3 Hexagons
- **Documentation:**
  - ✅ README.md (Detailed Methodology)
  - ✅ STAC Collection
  - ✅ Layer Descriptions

### Social Vulnerability Index (SVI)
- **Status:** ✅ Complete
- **Type:** Vector
- **Bucket:** `public-social-vulnerability`
- **Formats:**
  - ✅ PMTiles
  - ✅ GeoParquet
  - ❌ H3 Hexagons (Pending processing)
- **Documentation:**
  - ✅ README.md (Linked to CDC docs)
  - ✅ STAC Collection
  - ✅ Column Definitions (RPL_THEME variables)

---

## ⚠️ Partially Complete Datasets

### Mapping Inequality (Redlining)
- **Status:** ⚠️ Documentation Needed
- **Type:** Vector
- **Bucket:** `public-mappinginequality`
- **Formats:**
  - ✅ PMTiles
  - ✅ GeoParquet
  - ✅ H3 Hexagons
- **Documentation:**
  - ❌ README.md (Missing details)
  - ✅ STAC Collection
  - ❌ Column Definitions (Need DSL Richmond dictionary)

### NCP (Nature's Contributions to People)
- **Status:** ⚠️ Documentation Needed
- **Type:** Raster
- **Bucket:** `public-ncp`
- **Formats:**
  - ✅ COG
  - ✅ H3 Hexagons
- **Documentation:**
  - ❌ README.md (Missing details)
  - ✅ STAC Collection
  - ❌ Layer Definitions (Need source publication info)

### HydroBasins
- **Status:** ⚠️ Documentation Review
- **Type:** Vector
- **Bucket:** `public-hydrobasins`
- **Formats:**
  - ✅ PMTiles (Zoom-based)
  - ✅ GeoParquet
  - ✅ H3 Hexagons (Levels 3-6)
- **Documentation:**
  - ✅ README.md (Basic)
  - ❓ STAC Collection (Verify schema extensions)
  - ❓ Column Definitions

### GBIF (Occurrence Data)
- **Status:** ⚠️ Missing PMTiles & Metadata
- **Type:** Vector
- **Bucket:** `public-gbif`
- **Formats:**
  - ❌ PMTiles
  - ✅ GeoParquet
  - ✅ H3 Hexagons
- **Documentation:**
  - ❌ README.md (Missing details)
  - ✅ STAC Collection
  - ❌ Column Definitions (Map to DarwinCore)

### Wetlands (Ramsar & GLWD)
- **Status:** ⚠️ Mixed Completion
- **Type:** Mixed
- **Bucket:** `public-wetlands`
- **Formats:**
  - ✅ PMTiles
  - ✅ GeoParquet
  - ✅ COG (GLWD)
  - ❌ H3 Hexagons (Missing for Ramsar)
- **Documentation:**
  - ❌ README.md (Missing details)
  - ✅ STAC Collection
  - ❌ Column Definitions

### US Census
- **Status:** ⚠️ Major Work Needed
- **Type:** Vector
- **Bucket:** `public-census`
- **Formats:**
  - ❌ PMTiles
  - ❓ GeoParquet (Standard parquet missing?)
  - ✅ H3 Hexagons
- **Documentation:**
  - ❌ README.md
  - ✅ STAC Collection
  - ❌ Column Definitions (Need Census/ACS variable map)

### iNaturalist
- **Status:** ⚠️ Major Work Needed
- **Type:** Vector
- **Bucket:** `public-inat`
- **Formats:**
  - ❌ PMTiles
  - ✅ GeoParquet
  - ✅ H3 Hexagons
- **Documentation:**
  - ❌ README.md
  - ❌ STAC Collection
  - ❌ Column Definitions

### Overture Maps
- **Status:** ⚠️ Major Work Needed
- **Type:** Vector
- **Bucket:** `public-overturemaps`
- **Formats:**
  - ❌ PMTiles
  - ✅ GeoParquet
  - ✅ H3 Hexagons
- **Documentation:**
  - ❌ README.md
  - ❌ STAC Collection
  - ❌ Column Definitions

---

## ❌ Incomplete / Unpublished

- **CalEnviroScreen**: No published data
- **Fire**: Bucket exists but empty
- **PAD-US**: No published data
