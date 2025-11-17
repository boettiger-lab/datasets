# Wetlands Processing - Kubernetes Batch Jobs

Process 18,587 wetlands indices using Kubernetes indexed jobs with automatic resource tier escalation.

## Quick Start

```bash
# Check status of current Tier 1 job
kubectl get job wetlands-tier1 -n biodiversity -w

# After completion, escalate to Tier 2 if needed
python3 tier_escalate.py wetlands-tier1 1 wetlands_config.yaml
kubectl apply -f wetlands-tier2.yaml
```

## Files

- **`nwi.py`** - Main processing script
- **`tier_escalate.py`** - Check job results & create next tier
- **`wetlands_config.yaml`** - Job configuration
- **`check_status.sh`** - Quick status checker
- **`failed_indices_tier*.json`** - Failed indices per tier

## Resource Tiers

- Tier 0: 10 GiB - Initial run
- Tier 1: 20 GiB - Memory-intensive regions
- Tier 2: 40 GiB - Large datasets
- Tier 3: 80 GiB - Extreme cases
- Tier 4: 160 GiB - Maximum

## Workflow

1. **Tier completes** → job marked as "Failed" (expected with `podFailurePolicy`)
2. **Check failures**: `python3 tier_escalate.py <job-name> <tier> wetlands_config.yaml`
3. **Apply next tier**: `kubectl apply -f wetlands-tier<N>.yaml`
4. **Repeat** until all indices succeed

## Why This Approach?

✅ Stateless - no long-running controller  
✅ Resumable - run escalation script anytime  
✅ Permanent records - JSON files survive cleanup  
✅ Manual control - you decide when to escalate  
✅ Per-index failures tracked in `.status.failedIndexes`

## Monitoring

```bash
# Quick status
./check_status.sh

# Detailed
kubectl get job <name> -n biodiversity -o json | jq '.status'

# Failed indices
kubectl get job <name> -n biodiversity -o json | jq -r '.status.failedIndexes'
```

See README.md.old for full documentation.
