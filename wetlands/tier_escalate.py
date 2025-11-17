#!/usr/bin/env python3
"""
Robust tier escalation - checks completed job and creates next tier job.
Run this manually after each tier completes. Stateless and resumable.
"""
import subprocess
import json
import yaml
import sys
from typing import Set, List

def get_failed_indices(job_name: str, namespace: str) -> Set[int]:
    """Get failed indices from a completed job."""
    result = subprocess.run(
        ["kubectl", "get", "job", job_name, "-n", namespace, "-o", "json"],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print(f"Error: Job {job_name} not found in namespace {namespace}")
        return set()
    
    job_status = json.loads(result.stdout)
    status = job_status.get("status", {})
    
    succeeded = status.get("succeeded", 0)
    failed = status.get("failed", 0)
    completions = job_status["spec"]["completions"]
    failed_indexes_str = status.get("failedIndexes", "")
    
    print(f"\nJob: {job_name}")
    print(f"Status: {succeeded}/{completions} succeeded, {failed} failed")
    
    if not failed_indexes_str:
        print("✅ All indices succeeded!")
        return set()
    
    # Parse failed indexes
    failed_completion_indices = set()
    for part in failed_indexes_str.split(","):
        part = part.strip()
        if "-" in part:
            start, end = part.split("-")
            failed_completion_indices.update(range(int(start), int(end) + 1))
        elif part:
            failed_completion_indices.add(int(part))
    
    # Get the INDEX_MAPPING from job env to map back to original indices
    pods_result = subprocess.run(
        ["kubectl", "get", "pods", "-n", namespace, "-l", f"job-name={job_name}",
         "-o", "json", "--limit=1"],
        capture_output=True,
        text=True
    )
    
    if pods_result.returncode == 0:
        pods = json.loads(pods_result.stdout)
        for pod in pods.get("items", []):
            for container in pod["spec"]["containers"]:
                for env in container.get("env", []):
                    if env["name"] == "INDEX_MAPPING":
                        index_mapping = json.loads(env["value"])
                        failed_original = {index_mapping[i] for i in failed_completion_indices 
                                         if i < len(index_mapping)}
                        print(f"Failed indices: {sorted(list(failed_original))}")
                        return failed_original
    
    # Fallback: assume identity mapping
    print(f"⚠️  Warning: Could not get INDEX_MAPPING, assuming identity mapping")
    print(f"Failed completion indices: {sorted(list(failed_completion_indices))}")
    return failed_completion_indices


def create_next_tier_job(failed_indices: Set[int], current_tier: int, 
                        config_file: str, namespace: str) -> str:
    """Create job manifest for next tier with failed indices."""
    
    with open(config_file) as f:
        config = yaml.safe_load(f)
    
    next_tier = current_tier + 1
    if next_tier >= len(config["resource_tiers"]):
        print(f"❌ No more tiers available (max tier: {len(config['resource_tiers']) - 1})")
        return None
    
    tier = config["resource_tiers"][next_tier]
    job_name = f"{config['job_name_prefix']}-tier{next_tier}-manual"
    
    print(f"\n🚀 Creating Tier {next_tier} job: {job_name}")
    print(f"   Memory: {tier['memory']}, CPU: {tier['cpu']}")
    print(f"   Indices to retry: {len(failed_indices)}")
    
    # Build job manifest
    manifest = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "namespace": namespace,
            "labels": {
                "app": config["job_name_prefix"],
                "tier": str(next_tier)
            }
        },
        "spec": {
            "completions": len(failed_indices),
            "parallelism": min(len(failed_indices), config.get("parallelism", 50)),
            "completionMode": "Indexed",
            "backoffLimit": len(failed_indices),
            "backoffLimitPerIndex": 0,
            "podFailurePolicy": {
                "rules": [{
                    "action": "FailIndex",
                    "onExitCodes": {
                        "operator": "In",
                        "values": list(range(1, 256))
                    }
                }]
            },
            "ttlSecondsAfterFinished": config.get("ttl_seconds_after_finished", 3600),
            "template": {
                "metadata": {
                    "labels": {
                        "app": config["job_name_prefix"],
                        "tier": str(next_tier)
                    }
                },
                "spec": {
                    "restartPolicy": "Never",
                    "volumes": [{"name": "repo", "emptyDir": {}}] if config.get("init_container", {}).get("enabled") else [],
                    "containers": [{
                        "name": "worker",
                        "image": config["container_image"],
                        "imagePullPolicy": config.get("image_pull_policy", "Always"),
                        "workingDir": config.get("working_directory", "/workspace/datasets"),
                        "command": config["command"],
                        "volumeMounts": [{"name": "repo", "mountPath": "/workspace"}] if config.get("init_container", {}).get("enabled") else [],
                        "env": _build_env_vars(failed_indices, next_tier, config),
                        "resources": {
                            "requests": {"memory": tier["memory"], "cpu": tier["cpu"]},
                            "limits": {"memory": tier["memory"], "cpu": tier["cpu"]}
                        }
                    }]
                }
            }
        }
    }
    
    # Add init container if needed
    if config.get("init_container", {}).get("enabled"):
        init_config = config["init_container"]
        manifest["spec"]["template"]["spec"]["initContainers"] = [{
            "name": "git-clone",
            "image": "alpine/git:2.45.2",
            "imagePullPolicy": "IfNotPresent",
            "command": ["sh", "-lc", 
                f"git clone --depth {init_config.get('clone_depth', 1)} \"{init_config.get('repo_url')}\" /workspace/datasets"],
            "volumeMounts": [{"name": "repo", "mountPath": "/workspace"}],
            "resources": {"requests": {"cpu": "1", "memory": "1Gi"}, "limits": {"cpu": "1", "memory": "1Gi"}}
        }]
    
    # Add node affinity if needed
    if config.get("avoid_gpu_nodes"):
        manifest["spec"]["template"]["spec"]["affinity"] = {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [{
                        "matchExpressions": [{
                            "key": "feature.node.kubernetes.io/pci-10de.present",
                            "operator": "NotIn",
                            "values": ["true"]
                        }]
                    }]
                }
            }
        }
    
    # Save manifest
    manifest_file = f"{job_name}.yaml"
    with open(manifest_file, "w") as f:
        yaml.dump(manifest, f, default_flow_style=False)
    
    print(f"   Saved manifest to: {manifest_file}")
    
    return manifest_file


def _build_env_vars(indices: Set[int], tier_level: int, config: dict) -> List[dict]:
    """Build environment variables."""
    env_vars = [
        {
            "name": "JOB_COMPLETION_INDEX",
            "valueFrom": {
                "fieldRef": {
                    "fieldPath": "metadata.annotations['batch.kubernetes.io/job-completion-index']"
                }
            }
        },
        {
            "name": "INDEX_MAPPING",
            "value": json.dumps(sorted(list(indices)))
        },
        {
            "name": "MEMORY_TIER",
            "value": str(tier_level)
        }
    ]
    
    for env in config.get("environment", []):
        if "value_from" in env:
            env_var = {"name": env["name"], "valueFrom": {}}
            if "secret" in env["value_from"]:
                env_var["valueFrom"]["secretKeyRef"] = {
                    "name": env["value_from"]["secret"]["name"],
                    "key": env["value_from"]["secret"]["key"]
                }
            env_vars.append(env_var)
        else:
            env_vars.append({"name": env["name"], "value": env["value"]})
    
    return env_vars


def main():
    if len(sys.argv) < 4:
        print("Usage: python3 tier_escalate.py <completed_job_name> <current_tier> <config.yaml>")
        print("Example: python3 tier_escalate.py wetlands-tier0-batch0 0 wetlands_config.yaml")
        sys.exit(1)
    
    job_name = sys.argv[1]
    current_tier = int(sys.argv[2])
    config_file = sys.argv[3]
    namespace = "biodiversity"
    
    print("=" * 60)
    print("Tier Escalation Tool - Robust & Stateless")
    print("=" * 60)
    
    # Get failed indices from completed job
    failed_indices = get_failed_indices(job_name, namespace)
    
    if not failed_indices:
        print("\n✅ All done! No more indices to retry.")
        return 0
    
    # Save failed indices for reference
    indices_file = f"failed_indices_tier{current_tier}.json"
    with open(indices_file, "w") as f:
        json.dump(sorted(list(failed_indices)), f, indent=2)
    print(f"\n💾 Saved failed indices to: {indices_file}")
    
    # Create next tier job
    manifest_file = create_next_tier_job(failed_indices, current_tier, config_file, namespace)
    
    if manifest_file:
        print(f"\n📋 To apply the job, run:")
        print(f"   kubectl apply -f {manifest_file}")
        print(f"\n📊 To check progress:")
        print(f"   kubectl get job -n {namespace} -w")
        print(f"\n🔄 After completion, run this script again with tier {current_tier + 1}")
    
    return 0


if __name__ == "__main__":
    exit(main())
