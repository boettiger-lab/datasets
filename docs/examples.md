# Examples

Real-world examples of using the CNG Datasets toolkit.

## Vector Processing Examples

### Protected Areas (WDPA)

Large-scale protected areas processing:

```python
from cng_datasets.vector import H3VectorProcessor

processor = H3VectorProcessor(
    input_url="s3://public-wdpa/wdpa.parquet",
    output_url="s3://public-wdpa/hex/",
    h3_resolution=10,
    parent_resolutions=[9, 8, 0],
    chunk_size=500,
)

output_files = processor.process_all_chunks()
```

See: `wdpa/` directory

### Redlining Data

Historical redlining polygon processing:

```python
from cng_datasets.vector import H3VectorProcessor

processor = H3VectorProcessor(
    input_url="s3://public-redlining/redlining.parquet",
    output_url="s3://public-redlining/hex/",
    h3_resolution=10,
    parent_resolutions=[9, 8, 7, 0],
    chunk_size=100,
)

output_files = processor.process_all_chunks()
```

See: `redlining/` directory

### PAD-US

Protected Areas Database of the United States:

```python
from cng_datasets.vector import H3VectorProcessor

processor = H3VectorProcessor(
    input_url="s3://public-padus/padus.parquet",
    output_url="s3://public-padus/hex/",
    h3_resolution=10,
    parent_resolutions=[9, 8, 0],
)

output_files = processor.process_all_chunks()
```

See: `pad-us/` directory

## Raster Processing Examples

### Global Wetlands (GLWD)

Global wetlands raster to H3:

```python
from cng_datasets.raster import RasterProcessor

# Create COG
processor = RasterProcessor(
    input_path="/vsis3/public-wetlands/glwd.tif",
    output_cog_path="s3://public-wetlands/glwd-cog.tif",
    compression="zstd",
)
processor.create_cog()

# Convert to H3 by h0 regions
for h0_index in range(122):
    processor = RasterProcessor(
        input_path="/vsis3/public-wetlands/glwd.tif",
        output_parquet_path="s3://public-wetlands/hex/",
        h0_index=h0_index,
        h3_resolution=8,
        parent_resolutions=[0],
        value_column="wetland_class",
        nodata_value=255,
    )
    processor.process_h0_region()
```

See: `wetlands/glwd/` directory

### IUCN Range Maps

Species range maps processing:

```python
from cng_datasets.raster import RasterProcessor
import glob

# Process each species raster
for raster_file in glob.glob("species/*.tif"):
    processor = RasterProcessor(
        input_path=raster_file,
        output_cog_path=f"s3://public-iucn/{species}-cog.tif",
        output_parquet_path=f"s3://public-iucn/{species}/hex/",
        h3_resolution=9,
        parent_resolutions=[8, 0],
        value_column="presence",
    )
    
    processor.create_cog()
    processor.process_all_h0_regions()
```

See: `iucn/` directory

## Kubernetes Examples

### Vector Dataset Workflow

```bash
# Generate workflow
cng-datasets workflow \
  --dataset wdpa \
  --source-url https://d1gam3xoknrgr2.cloudfront.net/current/WDPA_WDOECM_Nov2024_Public_all_gdb.zip \
  --bucket public-wdpa \
  --h3-resolution 10 \
  --parent-resolutions "9,8,0" \
  --namespace biodiversity \
  --output-dir wdpa/k8s/

# Run workflow
kubectl apply -f wdpa/k8s/workflow-rbac.yaml
kubectl apply -f wdpa/k8s/workflow-pvc.yaml
kubectl apply -f wdpa/k8s/workflow.yaml
```

### Raster Dataset Workflow

```python
from cng_datasets.k8s import K8sJobManager

# Generate indexed job for h0 regions
manager = K8sJobManager(namespace="biodiversity")

job = manager.generate_chunked_job(
    job_name="wetlands-h3",
    script_path="/app/wetlands/glwd/job.py",
    num_chunks=122,  # One per h0 region
    base_args=[
        "--input-url", "/vsis3/public-wetlands/glwd.tif",
        "--output-url", "s3://public-wetlands/hex/",
        "--parent-resolutions", "8,0",
    ],
    parallelism=61,
    cpu="4",
    memory="34Gi",
)

manager.save_job_yaml(job, "wetlands/k8s/hex-job.yaml")
```

## Multi-Provider Sync

Sync datasets across cloud providers:

```python
from cng_datasets.storage import RcloneSync

syncer = RcloneSync()

# Sync from AWS to Cloudflare R2
syncer.sync(
    source="aws:public-wdpa/",
    destination="cloudflare:public-wdpa/",
    args=["--progress"]
)

# Sync from AWS to Google Cloud Storage
syncer.sync(
    source="aws:public-wdpa/",
    destination="gcs:public-wdpa/",
)
```

See: Individual dataset directories for complete examples and job scripts.
