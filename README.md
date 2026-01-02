# Boettiger Lab Geospatial Datasets

A collection of cloud-native geospatial datasets optimized for analysis and visualization, available as H3-indexed GeoParquet, PMTiles, and Cloud-Optimized GeoTIFFs (COGs). All datasets are accessible via a [STAC catalog](https://radiantearth.github.io/stac-browser/#/external/s3-west.nrp-nautilus.io/public-data/stac/catalog.json) hosted on the National Research Platform.

## Available Datasets

Our collection currently includes 11 published datasets covering biodiversity, conservation, environmental justice, and infrastructure:

- **CPAD** (California Protected Areas Database) - Protected lands in California
- **IUCN** - Global species range maps and Red List assessments  
- **WDPA** (World Database on Protected Areas) - Global protected areas
- **Mapping Inequality** - Historical redlining maps of US cities
- **HydroBasins** - Global watershed boundaries at multiple hierarchical levels
- **Natural Capital Project** - Ecosystem services and nature's contributions to people
- **GBIF** - Global biodiversity occurrence records
- **US Census** - Demographic and geographic data
- **Carbon** - Carbon storage and emissions datasets
- **Social Vulnerability Index** - CDC's social vulnerability indicators
- **Wetlands** - National Wetlands Inventory data

All datasets are H3-indexed at resolution 0 (coarsest partitioning) for efficient spatial queries and parallel processing. Browse the [STAC catalog](https://radiantearth.github.io/stac-browser/#/external/s3-west.nrp-nautilus.io/public-data/stac/catalog.json) for complete metadata, spatial/temporal extents, and direct HTTPS access to files.

---

# CNG Datasets Toolkit

This Python toolkit was used to process and generate the datasets above, converting large geospatial datasets into cloud-native formats with H3 hexagonal indexing.

[![Documentation](https://img.shields.io/badge/docs-github%20pages-blue)](https://boettiger-lab.github.io/datasets/)
[![PyPI](https://img.shields.io/pypi/v/cng-datasets)](https://pypi.org/project/cng-datasets/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Features

- **Vector Processing**: Convert polygon and point datasets to H3-indexed GeoParquet
- **Raster Processing**: Create Cloud-Optimized GeoTIFFs (COGs) and H3-indexed parquet
- **Kubernetes Integration**: Generate and submit K8s jobs for large-scale processing
- **Cloud Storage**: Manage S3 buckets and sync across multiple providers with rclone
- **Scalable**: Chunk-based processing for datasets that don't fit in memory


## Usage

While package functions can all run locally with the python API, the intended use of this package is to auto-generate kubernetes jobs that can handle all the processing, e.g. on the [NRP Nautilus](https://nrp.ai/documentation) cluster. Follow the NRP documentation on how to get an account, set up `kubectl`, and run basic jobs on the cluster first.


### Vector Processing

For example, the following process will create PMTiles, GeoParquet, and partitioned, H3-indexed parquet running on the cluster.  It will also create the bucket and configure public-read access and CORS headers appropriately.  The helper utility generates the k8s jobs:

```bash
cng-datasets workflow \
  --dataset my-dataset \
  --source-url https://dsl.richmond.edu/panorama/redlining/static/mappinginequality.gpkg \
  --bucket public-test \
  --h3-resolution 10 \
  --parent-resolutions "9,8,0" \
  --hex-memory 8Gi \
  --max-completions 200
```

Then it will instruct you to run the job as follows: 

```bash
# One-time RBAC setup
kubectl apply -f k8s/workflow-rbac.yaml

# Apply all workflow files (safe to re-run)
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/workflow.yaml
```

And the jobs will run on the cluster in order.  (Because the workflow also runs on the cluster you don't have to keep the latop open). 

The configmap is just a list of the underlying jobs that the workflow will run.  You can modify any of the k8s `*-job.yaml` files for other kubernetes clusters or to tweak various settings and then just `kubectl apply -f` them individually as well.  Most use a single pod, except for the hex job where the most computationally intensive steps happen.  


## Architecture

Kubernetes jobs run in remote pods that are cleaned up on completion.  All data outputs are written directly to an S3 bucket, ready for use.


### Output Structure

```
s3://bucket/
├── dataset-name.parquet         # GeoParquet with all attributes
├── dataset-name.pmtiles         # PMTiles vector tiles
└── dataset-name/
    └── hex/                     # H3-indexed parquet (partitioned by h0)
        └── h0=*/
            └── *.parquet
```

### Processing Approach

**Vector Datasets:**
1. Convert to optimized GeoParquet (if needed)
2. Generate PMTiles for web visualization
3. Tile to H3 hexagons in chunks
4. Partition by h0 cells for efficient querying

**Raster Datasets:**
1. Create Cloud-Optimized GeoTIFF (COG)
2. Convert to H3-indexed parquet by h0 regions
3. Partition by h0 cells for efficient querying

### H3 Resolutions

```bash
--h3-resolution 10        # Primary resolution (default: 10)
--parent-resolutions "9,8,0"  # Parent hexes for aggregation (default: "9,8,0")
```

**Resolution Reference:**
- h12: ~3m (building-level)
- h11: ~10m (lot-level)  
- h10: ~15m (street-level) - **default**
- h9: ~50m (block-level)
- h8: ~175m (neighborhood)
- h7: ~600m (district)
- h0: continent-scale (partitioning key)

## Configuration

These routines rely on several software tools that can all read and write to S3 buckets: GDAL, duckdb, and rclone.  GDAL and duckdb can both 'stream' data directly to a bucket without writing to a local file, and the package relies on environmental variables to configure them.  rclone provides file-based operations when streaming is not an option or slower.  The initial bucket creation and setting access permissions and CORS uses aws cli. 


### Sync

Sync bucket to other S3 system, e.g. source.coop.  (Be sure to create the bucket first, e.g. with source.coop create the repo in the web interface.)


```
cng-datasets sync-job \
    --job-name sync-to-source-coop \
    --source nrp:public-mappinginequality \
    --destination source:us-west-2.opendata.source.coop/cboettig/mappinginequality \
    --output sync-job.yaml
```    

## Examples

See the individual dataset directories for complete examples:
- `redlining/` - Vector polygon processing with chunking
- `wetlands/glwd/` - Raster to H3 conversion with global h0 processing
- `wdpa/` - Large-scale protected areas processing
- `hydrobasins/` - Multi-level watershed processing
- `gbif/` - Species occurrence data processing

## License

MIT License - see [LICENSE](LICENSE) for details

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development guidelines

