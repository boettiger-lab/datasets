# Dataset Completion Status

This document tracks the completion status of datasets in the boettiger-lab/datasets repository.

**Standard Workflow:** See [DATASET_DOCUMENTATION_WORKFLOW.md](DATASET_DOCUMENTATION_WORKFLOW.md) for the process of creating READMEs and STAC collections.

## ✅ Complete Datasets

The following datasets have comprehensive `README.md` and `stac-collection.json` files uploaded to S3, with full column definitions and proper parent/root links.

### **CPAD (California Protected Areas)**
- **Bucket:** `public-cpad`
- **Documentation:** ✅ Comprehensive README + STAC with `table:columns`
- **Source:** GreenInfo Network

### **IUCN Species Richness**
- **Bucket:** `public-iucn`
- **Documentation:** ✅ Comprehensive README + STAC with `table:columns`
- **Source:** IUCN Red List

### **Carbon (Irrecoverable/Manageable)**
- **Bucket:** `public-carbon`
- **Documentation:** ✅ Comprehensive README + STAC (18 COG files)
- **Source:** Conservation International (Noon et al. 2022)

### **Social Vulnerability Index (SVI)**
- **Bucket:** `public-social-vulnerability`
- **Documentation:** ✅ Comprehensive README + STAC with `table:columns`
- **Source:** CDC / ATSDR

### **WDPA (World Database on Protected Areas)**
- **Bucket:** `public-wdpa`
- **Documentation:** ✅ Comprehensive README + STAC with `table:columns`
- **Source:** UNEP-WCMC & IUCN (Protected Planet)

## ⚠️ Partially Complete / Pending Documentation

These datasets exist but need the "Standard Workflow" applied (Schema inspection -> Research -> README/STAC creation).

### **Mapping Inequality (Redlining)**
- **Bucket:** `public-mappinginequality`
- **Status:** Has STAC, needs detailed README/Schema
- **Action:** Inspect parquet schema, research DSL Richmond data dictionary.

### **NCP (Nature's Contributions to People)**
- **Bucket:** `public-ncp`
- **Status:** Has STAC, needs detailed README/Schema
- **Action:** Research Chaplin-Kramer et al. source for layer definitions.

### **HydroBasins**
- **Bucket:** `public-hydrobasins`
- **Status:** Has basic README, needs STAC update?
- **Action:** Review current documentation against new standard.

### **GBIF (Occurrence Data)**
- **Bucket:** `public-gbif`
- **Status:** Has STAC, needs DarwinCore field definitions
- **Action:** Map parquet columns to DarwinCore terms in README/STAC.

### **US Census**
- **Bucket:** `public-census`
- **Status:** Needs documentation
- **Action:** Inspect schema, identifying ACS/Decennial variables.

### **Wetlands**
- **Bucket:** `public-wetlands`
- **Status:** Needs documentation for Ramsar & GLWD
- **Action:** Research Ramsar and GLWD attributes.

### **iNaturalist**
- **Bucket:** `public-inat`
- **Status:** Needs documentation
- **Action:** Document parquet schema (likely similar to GBIF/DwC).

### **Overture Maps**
- **Bucket:** `public-overturemaps`
- **Status:** Needs documentation
- **Action:** Document Overture schema (places, buildings, etc.).

## ❌ Incomplete / Unpublished

- **CalEnviroScreen**: No published data
- **Fire**: Bucket exists but empty
- **PAD-US**: No published data
