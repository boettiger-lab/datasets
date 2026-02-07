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
- **Status:** ✅ Complete
- **Type:** Vector
- **Bucket:** `public-mappinginequality`
- **Formats:**
  - ✅ PMTiles
  - ✅ GeoParquet
  - ✅ H3 Hexagons
- **Documentation:**
  - ✅ README.md (Comprehensive)
  - ✅ STAC Collection
  - ✅ Column Definitions (DSL Richmond dictionary)

### NCP (Nature's Contributions to People)
- **Status:** ✅ Complete
- **Type:** Raster
- **Bucket:** `public-ncp`
- **Formats:**
  - ✅ COG
  - ✅ H3 Hexagons
- **Documentation:**
  - ✅ README.md (Chaplin-Kramer et al. 2019)
  - ✅ STAC Collection
  - ✅ Layer Definitions

### HydroBasins
- **Status:** ✅ Complete
- **Type:** Vector
- **Bucket:** `public-hydrobasins`
- **Formats:**
  - ✅ PMTiles (Zoom-based)
  - ✅ GeoParquet
  - ✅ H3 Hexagons (Levels 3-6)
- **Documentation:**
  - ✅ README.md (Basic)
  - ✅ STAC Collection
  - ✅ Column Definitions

### GBIF (Occurrence Data)
- **Status:** ✅ Complete (Derived Products)
- **Type:** Vector
- **Bucket:** `public-gbif`
- **Formats:**
  - ❌ PMTiles (Not for this subset)
  - ✅ GeoParquet (Redlined Cities Subset)
  - ✅ H3 Hexagons (Taxonomic Counts)
- **Documentation:**
  - ✅ README.md (Describes derived assets)
  - ✅ STAC Collection
  - ✅ Column Definitions (Mapping Inequality + Taxa)

### Ramsar (Wetlands of International Importance)
- **Status:** ✅ Complete
- **Type:** Vector
- **Bucket:** `public-wetlands`
- **Formats:**
  - ✅ PMTiles
  - ✅ GeoParquet
  - ✅ H3 Hexagons
- **Documentation:**
  - ✅ README.md
  - ✅ STAC Collection (part of wetlands-global)
  - ✅ Column Definitions

### GLWD (Global Lakes and Wetlands Database)
- **Status:** ✅ Complete
- **Type:** Raster
- **Bucket:** `public-wetlands`
- **Formats:**
  - ✅ COG
  - ❓ H3 Hexagons (Found in glwd/hex?)
- **Documentation:**
  - ✅ README.md (part of wetlands-global)
  - ✅ STAC Collection (part of wetlands-global)
  - ❌ Column Definitions (Need category codes)

### NWI (National Wetlands Inventory - USA)
- **Status:** ⚠️ Partial
- **Type:** Vector
- **Bucket:** `public-wetlands`
- **Formats:**
  - ❌ PMTiles (Not found?)
  - ❌ GeoParquet (Raw polygons?)
  - ✅ H3 Hexagons (Found in nwi/hex)
- **Documentation:**
  - ❌ README.md
  - ❌ STAC Collection
  - ❌ Column Definitions

### US Census
- **Status:** ✅ Complete (Spatial Crosswalk)
- **Type:** Vector (Index Only)
- **Bucket:** `public-census`
- **Formats:**
  - ❌ PMTiles (Not needed for index)
  - ❌ GeoParquet (Uses hive-partitioned parquet)
  - ✅ H3 Hexagons (Tracts -> H3)
- **Documentation:**
  - ✅ README.md (Clarifies crosswalk nature)
  - ✅ STAC Collection
  - ✅ Column Definitions (FIPS only)

### iNaturalist
- **Status:** ✅ Complete (Species Ranges)
- **Type:** Vector
- **Bucket:** `public-inat`
- **Formats:**
  - ✅ GeoParquet (Modeled Ranges & Taxonomy)
  - ✅ H3 Hexagons (Ranges indexed to H4)
- **Documentation:**
  - ✅ README.md (Describes ranges, not observations)
  - ✅ STAC Collection
  - ✅ Column Definitions (Taxon ID, Geomodel Version)

### Overture Maps
- **Status:** ✅ Complete (Divisions)
- **Type:** Vector
- **Bucket:** `public-overturemaps`
- **Formats:**
  - ✅ GeoParquet (Regions)
  - ✅ PMTiles (Regions)
  - ✅ H3 Hexagons (Partitioned)
- **Documentation:**
  - ✅ README.md
  - ✅ STAC Collection
  - ✅ Column Definitions

## Major Updates

- [ ] **Carbon:** Update to 2025 release (https://zenodo.org/records/17645053)

---

## ❌ Incomplete / Unpublished

- **CalEnviroScreen**: No published data
- **Fire**: Bucket exists but empty
- **PAD-US**: No published data
