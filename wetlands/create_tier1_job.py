#!/usr/bin/env python3
"""
Create Tier 1 job directly from the known failed indices from Tier 0
"""
import json
import yaml
import subprocess
from typing import Set, List

def build_env_vars(indices: Set[int], tier_level: int, config: dict) -> List[dict]:
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

# Load failed indices
with open("failed_indices_tier0.json") as f:
    failed_indices = set(json.load(f))

print(f"Creating Tier 1 job for {len(failed_indices)} failed indices from Tier 0")

# Load config
with open("wetlands_config.yaml") as f:
    config = yaml.safe_load(f)

tier = 1  # Tier 1 = 20Gi
tier_config = config["resource_tiers"][tier]
job_name = "wetlands-tier1"
namespace = "biodiversity"

# Build job manifest
manifest = {
    "apiVersion": "batch/v1",
    "kind": "Job",
    "metadata": {
        "name": job_name,
        "namespace": namespace,
        "labels": {
            "app": "wetlands",
            "tier": str(tier)
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
                    "app": "wetlands",
                    "tier": str(tier)
                }
            },
            "spec": {
                "restartPolicy": "Never",
                "volumes": [{"name": "repo", "emptyDir": {}}],
                "containers": [{
                    "name": "worker",
                    "image": config["container_image"],
                    "imagePullPolicy": config.get("image_pull_policy", "Always"),
                    "workingDir": config.get("working_directory", "/workspace/datasets"),
                    "command": config["command"],
                    "volumeMounts": [{"name": "repo", "mountPath": "/workspace"}],
                    "env": build_env_vars(failed_indices, tier, config),
                    "resources": {
                        "requests": {"memory": tier_config["memory"], "cpu": tier_config["cpu"]},
                        "limits": {"memory": tier_config["memory"], "cpu": tier_config["cpu"]}
                    }
                }],
                "initContainers": [{
                    "name": "git-clone",
                    "image": "alpine/git:2.45.2",
                    "imagePullPolicy": "IfNotPresent",
                    "command": ["sh", "-lc", 
                        f"git clone --depth 1 \"{config['init_container']['repo_url']}\" /workspace/datasets"],
                    "volumeMounts": [{"name": "repo", "mountPath": "/workspace"}],
                    "resources": {"requests": {"cpu": "1", "memory": "1Gi"}, "limits": {"cpu": "1", "memory": "1Gi"}}
                }]
            }
        }
    }
}

# Add node affinity
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

# Save and apply
manifest_file = "wetlands-tier1.yaml"
with open(manifest_file, "w") as f:
    yaml.dump(manifest, f, default_flow_style=False)

print(f"Created {manifest_file}")
print(f"Memory: {tier_config['memory']}, CPU: {tier_config['cpu']}")
print(f"\nApplying to cluster...")

result = subprocess.run(["kubectl", "apply", "-f", manifest_file], capture_output=True, text=True)
if result.returncode == 0:
    print(f"✅ Job {job_name} created successfully!")
    print(f"\nMonitor with:")
    print(f"  kubectl get job {job_name} -n {namespace} -w")
    print(f"\nAfter completion, check for failures:")
    print(f"  python3 tier_escalate.py {job_name} 1 wetlands_config.yaml")
else:
    print(f"❌ Error: {result.stderr}")
