# Agent Instructions: Dataset Processing

You are working in a repository that uses `cng-datasets` to process geospatial data into cloud-native formats on a Kubernetes cluster. This document tells you everything you need to know.

Alongside it, `.claude/skills/` carries three skills that load automatically for
agents working in this repo: `nrp-k8s-batch` (cluster batch jobs), `nrp-s3`
(Ceph S3 endpoints, credentials, buckets) and `gdal-remote` (reading remote
rasters/vectors without downloading). They live here rather than in a global
skills directory so they are version-matched to this code and reviewed in the
same PR as the changes they describe.

## What You Are Doing

You are taking source geospatial data and producing three outputs per dataset:

| Format | File | Use |
|--------|------|-----|
| GeoParquet | `dataset.parquet` | Analytical queries with DuckDB/Polars |
| PMTiles | `dataset.pmtiles` | Web map visualization |
| H3 Hex Parquet | `dataset/hex/h0={cell}/data_0.parquet` | Spatial joins and aggregation |

You do **not** process data locally. You generate Kubernetes jobs that do the processing on the cluster.

The `cng-datasets` CLI and all its dependencies (GDAL, PROJ, exactextract, ...) are provided by the project's Docker image — there is no local virtualenv. Run the CLI from the image, e.g. `docker run --rm -v "$PWD":/app -w /app ghcr.io/boettiger-lab/datasets:latest cng-datasets ...`. The `cng-datasets ...` commands below are what you run inside that container.

## How To Process a Dataset

### Step 1: Identify the source data

Find the public URL to the source data. If it's already uploaded to S3, it will be at:
```
https://s3-west.nrp-nautilus.io/<bucket>/raw/<filename>
```

For multi-layer files (GDB, GPKG), inspect the layers:
```bash
ogrinfo /vsicurl/<source-url>
```

### Step 2: Generate the pipeline

Run `cng-datasets workflow` locally — this only generates YAML files, it does not process data:

```bash
cng-datasets workflow \
  --dataset <name> \
  --source-url <url> \
  --bucket <bucket> \
  --h3-resolution 10 \
  --parent-resolutions "9,8,0" \
  --hex-memory 32Gi \
  --max-completions 200 \
  --max-parallelism 50 \
  --output-dir catalog/<dataset>/k8s/<name>
```

Add `--layer <LayerName>` for multi-layer sources.

**For multi-layer sources**, run one workflow command per spatial layer:
```bash
cng-datasets workflow --dataset mydata/fee --layer FeeLayer ...
cng-datasets workflow --dataset mydata/easement --layer EasementLayer ...
```

The `/` in `--dataset` creates hierarchical S3 paths while using `-` in k8s job names.

### Step 3: Apply to the cluster

```bash
kubectl apply -f catalog/<dataset>/k8s/<name>/<name>-setup-bucket.yaml \
              -f catalog/<dataset>/k8s/<name>/<name>-convert.yaml \
              -f catalog/<dataset>/k8s/<name>/workflow.yaml
```

The workflow orchestrator handles the rest: setup-bucket → convert → pmtiles + hex (parallel) → repartition.

### Step 4: Monitor

```bash
kubectl get jobs | grep <name>       # Job status
kubectl logs job/<name>-convert      # Check conversion
kubectl logs job/<name>-workflow     # Orchestrator log
```

A complete run for a ~300K feature dataset typically takes 1-2 hours.

### Step 5: Document

After processing completes, create:
- `catalog/<dataset>/stac/README.md` — data dictionary, usage examples, citation
- `catalog/<dataset>/stac/stac-collection.json` — STAC metadata

Upload to the bucket:
```bash
rclone copy catalog/<dataset>/stac/README.md nrp:<bucket>/
rclone copy catalog/<dataset>/stac/stac-collection.json nrp:<bucket>/
```

## Common Parameters

| Parameter | Default | When to change |
|-----------|---------|----------------|
| `--h3-resolution` | 10 | Lower (8, 6) for coarser data or very large features |
| `--hex-memory` | 8Gi | Increase to 32Gi or 64Gi for large/complex geometries |
| `--max-completions` | 200 | Keep at 200 for datasets > 50K features |
| `--max-parallelism` | 50 | Reduce if cluster is already busy |
| `--parent-resolutions` | "9,8,0" | Almost never change this |
| `--intermediate-chunk-size` | auto | Decrease if hex pods OOM during unnest step |

`raster-workflow` additionally takes:

| Parameter | Default | When to change |
|-----------|---------|----------------|
| `--hex-cpu` | 4 | Raise for faster hex pods; also raises the default `--hex-workers` |
| `--hex-workers` | one per `--hex-cpu` | **Lower it first when a raster hex pod OOMs** — see Troubleshooting |
| `--hex-chunk-size` | 100000 | Lower when fewer workers alone is too coarse a step |
| `--h0-subset` | all 122 cells | List the h0 cells a regional source overlaps |

## S3 Bucket Layout

```
bucket/
├── raw/                         # Source data
├── dataset.parquet              # GeoParquet
├── dataset.pmtiles              # PMTiles
├── dataset/
│   └── hex/
│       └── h0={cell}/data_0.parquet
├── README.md
└── stac-collection.json
```

## Troubleshooting

**Convert fails → check logs:**
```bash
kubectl logs job/<name>-convert
```

**Hex pods OOM → increase memory or chunks:**
Regenerate with `--hex-memory 64Gi` or `--max-completions 200`, delete failed job, reapply.

**Raster hex pods OOM → fewer chunks in flight, not more memory:**
A raster hex pod's peak RSS is roughly `--hex-workers × --hex-chunk-size × bytes
per cell`, and `--hex-workers` is the lever to reach for first. Raising
`--hex-memory` past ~128Gi makes the pod contend for scarce large-RAM nodes,
which converts a retryable OOM into an unschedulable pod — strictly worse.

```bash
cng-datasets raster-workflow ... --hex-cpu 8 --hex-workers 8
```

Both knobs are written into the manifest as `CNG_HEX_WORKERS` /
`CNG_HEX_CHUNK_SIZE` whether or not you pass them, so a tuned pod survives
regeneration and its memory profile is readable from the YAML alone. The
generator prints the resulting profile when it runs.

**Hex pods preempted or crawling:** generated pods use default priority (no
`priorityClassName`). Do not add `opportunistic` to a long fan-out — on NRP it is
priority -2000000000 and preemption exposure scales with runtime; a measured
LANDFIRE build spread 5x in runtime because of it. It is still the right choice
for genuinely interruptible work, or when a build must exceed its namespace
quota, via `--priority-class opportunistic`.

**S3 throttling (503 SlowDown):** Transient. Wait a few minutes and retry.

**Workflow stuck → check what step it's on:**
```bash
kubectl logs job/<name>-workflow
kubectl get jobs | grep <name>
```

## What NOT To Do

- **Do not process data locally.** The CLI generates k8s jobs. You apply them. The cluster does the work.
- **Do not modify `cng_datasets/` source code** unless fixing a bug in the tool itself. User workflows only touch `catalog/` and generated YAML.
- **Do not hardcode S3 endpoints or credentials.** The generated jobs handle S3 configuration (internal endpoints, secrets) automatically.
- **Do not exceed 200 completions per job.** This is a hard limit to avoid overwhelming the cluster's etcd.

## Reference: Complete PAD-US Example

PAD-US is a multi-layer GDB with 5 spatial layers. Each was processed with a separate workflow:

```bash
# Upload raw data first (one-time)
rclone copy PADUS4_1Geodatabase.gdb nrp:public-padus/raw/PADUS4_1Geodatabase.gdb -P

# Generate and apply each layer
for args in \
  "padus-4-1/fee PADUS4_1Fee" \
  "padus-4-1/easement PADUS4_1Easement" \
  "padus-4-1/proclamation PADUS4_1Proclamation" \
  "padus-4-1/marine PADUS4_1Marine" \
  "padus-4-1/combined PADUS4_1Combined_Proclamation_Marine_Fee_Designation_Easement"; do
  set -- $args
  cng-datasets workflow \
    --dataset "$1" \
    --source-url https://s3-west.nrp-nautilus.io/public-padus/raw/PADUS4_1Geodatabase.gdb \
    --bucket public-padus \
    --layer "$2" \
    --h3-resolution 10 --hex-memory 32Gi --max-completions 200 --max-parallelism 50 \
    --parent-resolutions "9,8,0" \
    --output-dir "catalog/pad-us/k8s/$(echo $1 | cut -d/ -f2)"
done

# Apply all workflows
for layer in fee easement proclamation marine combined; do
  kubectl apply \
    -f catalog/pad-us/k8s/$layer/*-setup-bucket.yaml \
    -f catalog/pad-us/k8s/$layer/*-convert.yaml \
    -f catalog/pad-us/k8s/$layer/workflow.yaml
done
```

Non-spatial lookup tables in the GDB were converted locally:
```bash
for table in Public_Access Category DesignationType ManagerType; do
  ogr2ogr -f Parquet "$table.parquet" /vsicurl/<source-url>.gdb "$table"
done
rclone copy *.parquet nrp:public-padus/padus-4-1/lookup/ -P
```
