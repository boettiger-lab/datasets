# Kubernetes Workflows

Generate and run complete Kubernetes workflows for large-scale geospatial processing.

## Overview

`cng-datasets workflow`, `raster-workflow` and `mdim-workflow` generate every manifest a build needs and an orchestrator Job that runs them in order inside the cluster. Nothing runs on the machine that generates the YAML, and once the orchestrator is applied you can disconnect.

The steps all call the same Python functions documented in the [API reference](api_reference.md), so a small dataset can be converted directly in Python without any of this.

## Architecture

### Vector pipeline

`cng-datasets workflow` writes these files to `--output-dir`:

| File | Purpose |
|------|---------|
| `<name>-setup-bucket.yaml` | Create the S3 bucket with a public-read policy and CORS |
| `<name>-convert.yaml` | Read the source, reproject to EPSG:4326 and write GeoParquet |
| `<name>-plan.yaml` | Cut the GeoParquet into hex chunks by estimated H3 cells |
| `<name>-pmtiles.yaml` | Build PMTiles vector tiles |
| `<name>-hex.yaml` | H3 tiling as an indexed Job, one completion per chunk |
| `<name>-repartition.yaml` | Consolidate the chunks into `hex/h0=<cell>/data_0.parquet` |
| `configmap.yaml` | The step manifests above, packaged for the orchestrator |
| `workflow-rbac.yaml` | ServiceAccount, Role and RoleBinding, shared by every workflow in a namespace |
| `workflow.yaml` | Orchestrator Job: setup-bucket → convert → plan → (pmtiles ∥ hex) → repartition |

The plan step runs only when chunks are planned by cells (the default). With `--chunk-size` the hex Job is cut into a fixed number of features per chunk and there is no plan step. When there is a plan, the hex manifest holds only an upper bound on completions and the orchestrator resizes it to the plan, so always apply the hex step through `workflow.yaml` rather than on its own.

### Raster and mdim pipelines

`raster-workflow` writes setup-bucket, preprocess (when needed), cog, hex and repartition steps, plus a merge step when `--chunk-resolution` splits each h0 cell. `mdim-workflow` has the same shape for NetCDF and Zarr cubes. Both also write `configmap.yaml`, `workflow-rbac.yaml` and `workflow.yaml`.

## Quick Start

### 1. Generate workflow files

```bash
cng-datasets workflow \
  --dataset my-dataset \
  --source-url https://example.com/data.gpkg \
  --bucket public-my-dataset \
  --h3-resolution 10 \
  --parent-resolutions "9,8,0" \
  --output-dir my-dataset/
```

Jobs run in the `geo-workflows` namespace unless `--namespace` or a profile says otherwise. Every manifest carries `metadata.namespace`, so `kubectl apply` targets that namespace whatever your current context is.

### 2. Run the workflow

```bash
# One-time setup per namespace
kubectl apply -f my-dataset/workflow-rbac.yaml

# Apply the step manifests and start the orchestrator (safe to re-run)
kubectl apply -f my-dataset/configmap.yaml -f my-dataset/workflow.yaml

# Monitor progress
kubectl -n geo-workflows logs -f job/my-dataset-workflow
```

## Configuration

### H3 Resolutions

```bash
--h3-resolution 10              # Primary resolution (default: 10)
--parent-resolutions "9,8,0"    # Parent hexes for aggregation
```

**Resolution Reference:**
- h12: ~3m (building-level)
- h11: ~10m (lot-level)
- h10: ~15m (street-level) - **default**
- h9: ~50m (block-level)
- h8: ~175m (neighborhood)
- h7: ~600m (district)
- h0: continent-scale (partitioning key)

### Namespace

```bash
--namespace geo-workflows  # Kubernetes namespace (default: geo-workflows, or the profile's)
```

All jobs and RBAC use the specified namespace. It is also the Armada queue unless `--armada-queue` overrides it. Every generated manifest carries
`metadata.namespace`, including the per-step Job manifests, so applying one on its own
(`kubectl apply -f <dataset>-hex.yaml`) targets the workflow's namespace rather than
whatever `kubectl` currently defaults to.

### Cluster and Storage Configuration

All cluster-specific values default to NRP Nautilus. Override any of them to target a different cluster or S3 backend — existing commands produce identical YAML without any changes.

| Flag | Default | Description |
|------|---------|-------------|
| `--s3-endpoint` | `rook-ceph-rgw-nautiluss3.rook` | Internal S3 endpoint used by job pods |
| `--s3-public-endpoint` | `s3-west.nrp-nautilus.io` | Public endpoint (used in PMTiles URL construction) |
| `--s3-secret-name` | `aws` | Kubernetes secret holding `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` |
| `--rclone-secret-name` | `rclone-config` | Kubernetes secret holding the rclone config file |
| `--rclone-remote` | `nrp` | Rclone remote name used in setup-bucket and PMTiles upload |
| `--priority-class` | omitted | Kubernetes `priorityClassName`. Omitted by default, which means default priority. Pass `opportunistic` only for short, interruptible work: on NRP it is the lowest priority and a long fan-out at that priority is preempted repeatedly |
| `--namespace` | `geo-workflows` | Namespace the jobs run in |
| `--node-affinity` | `gpu-avoid` | `gpu-avoid` adds an NRP NFD rule to avoid GPU nodes; `none` omits affinity |

#### Example: Using a profile file

Save cluster settings once, reuse across datasets:

```yaml
# ~/.config/cng-datasets/profiles/my-cluster.yaml
s3_endpoint: minio.my-cluster.svc.cluster.local
s3_public_endpoint: minio.my-cluster.io
s3_secret_name: minio-credentials
rclone_secret_name: minio-rclone-config
rclone_remote: minio
priority_class: ""
node_affinity: none
```

```bash
cng-datasets workflow \
  --dataset my-dataset \
  --source-url https://example.com/data.gpkg \
  --bucket my-bucket \
  --profile my-cluster \
  --output-dir my-dataset/
```

Individual flags override profile values when both are given.

#### Example: MinIO without a profile file

```bash
cng-datasets workflow \
  --dataset my-dataset \
  --source-url https://example.com/data.gpkg \
  --bucket my-bucket \
  --s3-endpoint minio.my-cluster.svc.cluster.local \
  --s3-public-endpoint minio.my-cluster.io \
  --s3-secret-name minio-credentials \
  --rclone-secret-name minio-rclone-config \
  --rclone-remote minio \
  --priority-class "" \
  --node-affinity none \
  --output-dir my-dataset/
```

#### Example: Per-bucket credential scoping

Use separate S3 credentials for each dataset bucket to limit the blast radius of any single job:

```bash
# Create a scoped secret for this specific bucket
kubectl create secret generic s3-creds-my-dataset \
  --from-literal=AWS_ACCESS_KEY_ID=<bucket-specific-key> \
  --from-literal=AWS_SECRET_ACCESS_KEY=<bucket-specific-secret> \
  -n geo-workflows

# Generate workflow using the scoped secret
cng-datasets workflow \
  --dataset my-dataset \
  --source-url https://example.com/data.gpkg \
  --bucket public-my-dataset \
  --s3-secret-name s3-creds-my-dataset \
  --output-dir my-dataset/
```

Jobs will use only the credentials for `public-my-dataset`, with no access to other buckets.

#### Example: Using from Python

The same options are available when calling `generate_dataset_workflow` or `generate_raster_workflow` directly:

```python
from cng_datasets.k8s import generate_dataset_workflow

generate_dataset_workflow(
    dataset_name="my-dataset",
    source_urls="https://example.com/data.gpkg",
    bucket="my-bucket",
    output_dir="my-dataset/",
    # Override cluster defaults
    s3_endpoint="minio.my-cluster.svc.cluster.local",
    s3_public_endpoint="minio.my-cluster.io",
    s3_secret_name="minio-credentials",
    rclone_secret_name="minio-rclone-config",
    rclone_remote="minio",
    priority_class="",        # omit priorityClassName
    node_affinity="none",     # omit affinity
)
```

### Chunking

The hex step is an indexed Job whose completions are chunks of the GeoParquet.

- **Planned by cells (default).** The plan step estimates each feature's H3 cell count and cuts chunks at `--cells-per-chunk` (default 5,000,000), never more than 1000 features per chunk. Memory follows cells, not features, so a few continent-sized polygons get their own chunks. Lower `--cells-per-chunk` if hex pods run out of memory or straggle.
- **Fixed size.** `--chunk-size N` cuts N features per chunk with no plan step. It is mutually exclusive with `--cells-per-chunk`.
- `--max-completions` (default 200) caps the number of chunks and `--max-parallelism` (default 50) caps the pods alive at once.

Raster workflows chunk by H3 cell instead: one h0 base cell per pod by default, or each h0 split into its descendants at `--chunk-resolution N`. `--max-hex-memory 8Gi` picks the coarsest chunk resolution that fits the budget.

## Processing Details

### Two-pass H3 approach

1. **Hex** (`<name>-hex.yaml`): each pod tiles its chunk at the target resolution and all parent resolutions, and writes `s3://bucket/<dataset>/chunks/chunk_<id>.parquet`.
2. **Repartition** (`<name>-repartition.yaml`): reads all chunks, partitions them by h0 cell, writes `s3://bucket/<dataset>/hex/h0=<cell>/data_0.parquet` and deletes `chunks/`.

### Output structure

```
s3://bucket/
├── <dataset>.parquet         # GeoParquet with all attributes
├── <dataset>.pmtiles         # PMTiles vector tiles
└── <dataset>/
    └── hex/                  # H3-indexed parquet, partitioned by h0
        ├── h0=<cell>/data_0.parquet   # <cell> is the h0 cell id as an integer
        └── ...
```

Hex files carry one column per resolution (`h10`, `h9`, `h8`, `h0`) alongside the feature id. The GeoParquet and hex files record the cng-datasets and DuckDB versions that built them in their Parquet key-value metadata.

## Monitoring & Debugging

### Check status

```bash
kubectl -n geo-workflows get jobs | grep my-dataset
kubectl -n geo-workflows logs job/my-dataset-workflow   # which step the orchestrator is on
kubectl -n geo-workflows logs job/my-dataset-plan       # chunk count and largest chunk
kubectl -n geo-workflows get pods | grep my-dataset-hex
```

### Common issues

**Orchestrator can't apply jobs:**
```bash
kubectl -n geo-workflows get sa,role,rolebinding | grep cng-datasets-workflow
```

**Hex pods run out of memory:** lower `--cells-per-chunk` (vector) or raise `--chunk-resolution` / lower `--hex-workers` (raster), regenerate, delete the failed Job and re-apply.

**Pod stuck pending:**
```bash
kubectl -n geo-workflows describe pod <pod-name>
```

## Cleanup

```bash
kubectl delete -f my-dataset/workflow.yaml -f my-dataset/configmap.yaml
kubectl -n geo-workflows delete job my-dataset-setup-bucket my-dataset-convert my-dataset-plan \
  my-dataset-pmtiles my-dataset-hex my-dataset-repartition --ignore-not-found
```

## Armada Backend (Experimental)

[Armada](https://nrp.ai/documentation/userdocs/running/scheduling/) is the NRP's multi-cluster batch job scheduler. It provides fair-use queuing across namespaces and can schedule more jobs than the namespace pod limit allows by drip-feeding pods to the cluster as resources free up.

### When to Use Armada

- You need to **queue multiple dataset workflows** and have them scheduled fairly
- You want to **exceed the 200-pod cap** per workflow (Armada queues excess jobs)
- You want jobs visible in the [Armada Lookout dashboard](https://armada-lookout.nrp-nautilus.io)

### Prerequisites

Install `armadactl` from the [Armada releases page](https://github.com/armadaproject/armada/releases) and place the [`.armadactl.yaml`](https://nrp.ai/documentation/userdocs/running/scheduling/) config in your home directory.

### Generate Armada Workflow

Add `--backend armada` to any workflow command. `raster-workflow` and `mdim-workflow` also take `--backend auto`, which uses Armada only when the chunk count passes the ~200-pod guideline for one indexed Job. `--armada-queue` sets the queue, which defaults to the namespace.

```bash
# Vector workflow
cng-datasets workflow \
  --dataset my-dataset \
  --source-url https://example.com/data.gpkg \
  --bucket public-my-dataset \
  --backend armada \
  --output-dir my-dataset/

# Raster workflow
cng-datasets raster-workflow \
  --dataset my-raster \
  --source-url https://example.com/data.tif \
  --bucket public-my-raster \
  --backend armada \
  --output-dir my-raster/
```

This generates the standard k8s Job YAMLs **plus** `armada-*.yaml` files in Armada submission format.

### Submit to Armada

Submit each step in order, waiting for completion between steps:

```bash
armadactl submit my-dataset/armada-my-dataset-setup-bucket.yaml
# wait for completion in Lookout UI
armadactl submit my-dataset/armada-my-dataset-convert.yaml
# wait for completion
armadactl submit my-dataset/armada-my-dataset-plan.yaml
# wait for completion
armadactl submit my-dataset/armada-my-dataset-hex.yaml
# wait for completion (this is the long parallel step)
armadactl submit my-dataset/armada-my-dataset-repartition.yaml
```

Monitor progress at https://armada-lookout.nrp-nautilus.io

### How It Works

- **Single-pod steps** (setup-bucket, convert, pmtiles, repartition) become one Armada job each
- **Indexed parallel steps** (hex) are expanded into N individual Armada jobs (one per chunk), each with the chunk index baked into the command. For vector workflows this is up to 200+ jobs; for raster workflows, one per H3 h0 region the build covers — 122 by default, or the length of `--h0-subset`
- Converted jobs run at the **non-preemptible `armada-default`** priority class (see below)
- All existing k8s Job YAMLs are still generated alongside the Armada files, so you can switch between backends without regenerating

### Priority Class and Retries

Conversion reads the k8s podSpec (`spec.template.spec`), so **Job-level retry settings do not
survive it** — `backoffLimit`, `backoffLimitPerIndex` and `maxFailedIndexes` are dropped, and
Armada has no per-job equivalent (retries are a server-side setting). Generation warns when a
real retry budget is discarded.

That is why converted jobs default to the non-preemptible `armada-default` rather than
`armada-preemptible`: a preempted Armada job is
[not automatically rescheduled](https://nrp.ai/documentation/userdocs/running/scheduling/), so a
preemptible long job with no retries loses its whole unit of work. The k8s `opportunistic` class
is deliberately *not* mapped onto `armada-preemptible` for the same reason — an opportunistic pod
is preempted but recreated by its Job controller, which the Armada equivalent would not be.

Choose the class explicitly with `--armada-priority-class`, which accepts a shorthand or a literal
class name:

```bash
cng-datasets workflow ... --backend armada --armada-priority-class preemptible
```

| Shorthand | Armada class | Preemptible? | Priority |
|---|---|---|---|
| `default` (default) | `armada-default` | no | 100 |
| `preemptible` | `armada-preemptible` | yes | 50 |
| `high` | `armada-high-priority` | no | 1000 |

Preemptible is the right choice once the unit of work is small enough that losing one to a
preemption is cheap. The converter currently reproduces the *shape* of the k8s job it reads — one
Armada job per completion, at the same cpu/memory/runtime — so units are as large as the k8s ones.
For rasters, `--chunk-resolution` makes them small: each h0 cell splits into its descendants.

Generation refuses an Armada pod larger than 16 cores or 32Gi, the size NRP's Armada nodes can schedule. Lower the pod's memory, for example with `--chunk-resolution` or `--max-hex-memory`, rather than raising the cap.

### Differences from Standard k8s Backend

| Aspect | `--backend k8s` (default) | `--backend armada` |
|--------|--------------------------|-------------------|
| Submission | `kubectl apply` | `armadactl submit` |
| Orchestration | Automated (workflow pod) | Manual (submit steps in order) |
| Parallel hex jobs | Single k8s Indexed Job | N individual Armada jobs |
| Pod limit | Bounded by namespace quota | Armada queues excess jobs |
| Monitoring | `kubectl logs` | Lookout UI |
| Priority class | omitted (default priority) | `armada-default` (set with `--armada-priority-class`) |
| Retry on failure | Job-level `backoffLimit` | none (dropped in conversion) |

## Advanced Usage

### Custom Resource Limits

Edit individual job YAML files:

```yaml
resources:
  requests:
    cpu: "2"
    memory: "8Gi"
  limits:
    cpu: "4"
    memory: "16Gi"
```

### Multiple Datasets

The generic RBAC can be shared:

```bash
# Apply RBAC once
kubectl apply -f workflow-rbac.yaml

# Run multiple datasets
kubectl apply -f dataset1/workflow.yaml
kubectl apply -f dataset2/workflow.yaml
```

Each dataset gets its own ConfigMap holding its step manifests.

## Design Rationale

### Why a ConfigMap?
- The orchestrator reads the step manifests from inside the cluster, with no git checkout or volume to populate
- `kubectl apply` of two files starts a build, and re-applying is safe

### Why generic RBAC?
- One ServiceAccount per namespace
- Consistent permissions
- No per-dataset RBAC proliferation
