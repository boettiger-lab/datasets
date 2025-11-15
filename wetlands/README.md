# Wetlands batch tooling

This directory contains helpers for the wetlands raster workflow. The new
`orchestrate_indices.py` script creates **one Kubernetes Job per index** so that
high-memory outliers cannot stall the rest of the dataset.

## Quick start

1. Install the Python dependency (PyYAML is only required when you point the
  script at a local template file) using the repository’s `uv` setup:

  ```bash
  uv pip install -r wetlands/requirements.txt
  ```

2. Preview the generated manifests locally without touching the cluster:

   ```bash
   python wetlands/orchestrate_indices.py \
     --template-path wetlands/job.yaml \
     --indexes 0 1 2 \
     --dry-run print
   ```

3. Apply jobs to the `biodiversity` namespace, cloning the live `wetlands` Job,
   throttling to the first 100 indexes, and pinning the RAM tier (see below):

   ```bash
   python wetlands/orchestrate_indices.py \
     --source-job wetlands \
     --namespace biodiversity \
     --count 18587 \
     --limit 100 \
     --tier 0
   ```

## Handy flags

- `--tier` picks from the RAM schedule (default base: 10 GiB, doubling each tier);
  combine with `--schedule-start-gi` / `--schedule-factor` to customize
  (defaults: `10 GiB` start, `x2` growth).
- `--memory` / `--cpu` still override resources directly when you want absolute
  control rather than the tiered schedule.
- `--backoff-limit` defaults to `0`, so each per-index Job fails fast unless you
  raise it for built-in retries.
- `--batch-label` tags every generated Job/pod with `wetlands/batch=<value>` so
  you can `kubectl wait` or clean up per chunk (disabled by default).
- `--chunk-size` lets the script iterate automatically over batches (e.g., 400
  indexes at a time) while `--batch-label-template` controls per-chunk labels
  (template placeholders: `{chunk}`, `{start}`, `{end}`, `{tier}`; default:
  `tier{tier}-chunk-{start:05d}`).
- `--wait-for-batch` tells the script to block after each chunk until the Jobs
  with that batch label finish; combine with `--wait-timeout` (default `2h`).
- `--dry-run none|server|print` defaults to `none`; use `server` for API
  validation or `print` to dump JSON locally without contacting the cluster.
- `--replace` deletes any previously created Jobs with the same name so you can
  requeue specific indexes cleanly (defaults to disabled).

Combine this orchestrator with the existing `restart_failed_indices.py` helper
for later high-memory retries: run the orchestrator at the lowest RAM tier, then
feed any failed indexes into the restart script with a higher `--memory` value.

## RAM schedule example (10 GiB → doubling)

The orchestrator now understands tiers so you can standardize RAM bumps without
hand-typing new values each time. By default, tier 0 requests **10 GiB**, and
each higher tier doubles the memory (tier 1 → 20 GiB, tier 2 → 40 GiB, etc.).

```bash
# Tier 0 sweep (10 GiB per index)
python wetlands/orchestrate_indices.py \
  --source-job wetlands \
  --namespace biodiversity \
  --tier 0 \
  --job-name-prefix wetlands-tier0 \
  --backoff-limit 0

# Tier 1 reruns only failing indexes (20 GiB) from a saved list
python wetlands/orchestrate_indices.py \
  --source-job wetlands \
  --namespace biodiversity \
  --tier 1 \
  --index-file tier0-failures.txt \
  --job-name-prefix wetlands-tier1 \
  --backoff-limit 0

# Tier 2 (40 GiB) for the stubborn remainder
python wetlands/orchestrate_indices.py \
  --source-job wetlands \
  --namespace biodiversity \
  --tier 2 \
  --index-file tier1-failures.txt \
  --job-name-prefix wetlands-tier2 \
  --backoff-limit 0
```

Need a different growth curve? Set `--schedule-start-gi 8 --schedule-factor 1.5`
and tiers will follow that progression instead. Use `--memory` for one-off
manual overrides.

## Running all 18k indexes without flooding the cluster

Submitting 18k Jobs simultaneously is usually unnecessary overhead for the API
server and etcd. Use `--chunk-size` so the orchestrator loops over manageable
chunks, tagging each batch and optionally waiting for completion:

```bash
python wetlands/orchestrate_indices.py \
  --source-job wetlands \
  --namespace biodiversity \
  --tier 0 \
  --chunk-size 400 \
  --job-name-prefix wetlands-tier0 \
  --backoff-limit 0 \
  --wait-for-batch \
  --wait-timeout 2h
```

With this command the script:

1. Builds the full index list (0–18586 by default).
2. Splits it into sequential chunks of 400 indexes each.
3. Generates a batch label such as `tier0-chunk-00000` (you can customize via
   `--batch-label-template` or force a constant label with `--batch-label`).
4. Submits all Jobs for that chunk, then runs `kubectl wait` on the label before
   moving to the next chunk when `--wait-for-batch` is enabled.

You can change chunk size, tier, or timeout at any time. If a chunk stalls, grab
its label (`wetlands/batch=...`), inspect the failed pods, and rerun the chunk
with `--replace` or roll the failed indexes into the next RAM tier.

## Configuration reference

| Option | Purpose | Default |
| --- | --- | --- |
| `--template-path` | Clone manifests from a local YAML file | `wetlands/job.yaml` (when neither source flag is provided) |
| `--source-job` | Clone an existing cluster Job | unset |
| `--namespace` | Namespace for kubectl operations | `biodiversity` |
| `--start` | First index (inclusive) when auto-generating ranges | `0` |
| `--count` | Number of consecutive indexes from `--start` | `18587` |
| `--indexes` | Explicit indexes to run | unset |
| `--index-file` | Read newline-delimited indexes from a file | unset |
| `--limit` | Cap how many resolved indexes get submitted | unset |
| `--job-name-prefix` | Prefix for generated Job names | `wetlands-index` |
| `--batch-label` | Add `wetlands/batch=<value>` to Jobs/pods | unset |
| `--batch-label-template` | Template for per-chunk labels (`{chunk}`, `{start}`, `{end}`, `{tier}`) | `tier{tier}-chunk-{start:05d}` |
| `--chunk-size` | Number of indexes per sequential chunk | unset (no chunking) |
| `--wait-for-batch` | Block until each chunk’s Jobs complete | disabled |
| `--wait-timeout` | Timeout passed to `kubectl wait` | `2h` |
| `--tier` | Use the RAM schedule to set memory | unset (disabled) |
| `--schedule-start-gi` | Tier 0 GiB value | `10` |
| `--schedule-factor` | Multiplier applied per tier | `2` |
| `--memory` | Hard override for memory request/limit | inherit template (or tier) |
| `--cpu` | Hard override for CPU request/limit | inherit template |
| `--backoff-limit` | Retries per per-index Job | `0` |
| `--ttl` | `ttlSecondsAfterFinished` | `3600` seconds (use `-1` to disable) |
| `--dry-run` | Apply mode (`none`, `server`, or `print`) | `none` |
| `--replace` | Delete conflicting Jobs before applying | disabled |
| `--quiet` | Suppress per-job status output | disabled |

Defaults mirror the CLI help text in `orchestrate_indices.py`, so the README
stays in sync with the script.
