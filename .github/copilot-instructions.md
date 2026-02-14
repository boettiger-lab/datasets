## Start Here

Read `AGENTS.md` before doing anything. It explains the workflow for processing datasets.

Do NOT read or modify `cng_datasets/` source code unless you are fixing a bug in the package itself. For processing datasets, you only need the CLI documented in `AGENTS.md` and `README.md`.

## Repository Structure

- `cng_datasets/` — Python package source code (do not modify for dataset processing)
- `catalog/` — per-dataset configs: k8s YAML, STAC metadata, processing notes
- `AGENTS.md` — complete instructions for processing datasets
- `README.md` — package documentation and CLI reference

## Local Environment

Activate the virtualenv: `source .venv/bin/activate`

The `cng-datasets` CLI is installed in the venv. Use it to generate k8s job YAML files. Do not run processing commands (vector, raster, repartition) locally — those run inside k8s pods.

## Kubernetes

- `kubectl` is pre-configured for the NRP Nautilus cluster, namespace `biodiversity`
- Secrets `aws` and `rclone-config` are already set up in the namespace
- All jobs use `priorityClassName: opportunistic` (preemptible)
- The generated YAML handles all k8s configuration — you just apply it

## S3 Storage

This cluster uses Ceph S3 (not AWS). The `cng-datasets` tool handles all S3 configuration automatically in the generated k8s jobs.

For **local read-only access** to public data:
```
https://s3-west.nrp-nautilus.io/<bucket>/<path>
```
Use path-style URLs, not virtual-hosted-style. No credentials needed for public buckets.

For `rclone`, configure with remote name `nrp`:
```
rclone ls nrp:<bucket>/
```

You do not need to know about internal S3 endpoints — the generated jobs handle this.

## GDAL

For inspecting remote files locally, use VSI-style paths:
```bash
ogrinfo /vsicurl/https://s3-west.nrp-nautilus.io/<bucket>/raw/<file>.gdb
```
