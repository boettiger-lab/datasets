"""
Workflow generation utilities.

Functions for creating complete dataset processing workflows with
multiple coordinated Kubernetes jobs.
"""

from typing import Optional, Dict, Any
from pathlib import Path
from .jobs import K8sJobManager


def generate_dataset_workflow(
    dataset_name: str,
    source_url: str,
    bucket: str,
    output_dir: str = ".",
    namespace: str = "biodiversity",
    image: str = "ghcr.io/rocker-org/ml-spatial",
    git_repo: str = "https://github.com/boettiger-lab/datasets.git",
):
    """
    Generate complete workflow for a dataset.
    
    Creates all necessary Kubernetes job configurations for processing
    a geospatial dataset through the standard pipeline:
    1. Convert to GeoParquet
    2. Generate PMTiles
    3. H3 hexagonal tiling
    4. Repartition by h0
    
    Args:
        dataset_name: Name of the dataset (e.g., "redlining")
        source_url: URL to source data file
        bucket: S3 bucket for outputs
        output_dir: Directory to write YAML files
        namespace: Kubernetes namespace
        image: Container image to use
    """
    manager = K8sJobManager(namespace=namespace, image=image)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Generate conversion job
    _generate_convert_job(manager, dataset_name, source_url, bucket, output_path, git_repo)
    
    # Generate pmtiles job
    _generate_pmtiles_job(manager, dataset_name, source_url, bucket, output_path, git_repo)
    
    # Generate hex tiling job
    _generate_hex_job(manager, dataset_name, bucket, output_path, git_repo)
    
    # Generate repartition job
    _generate_repartition_job(manager, dataset_name, bucket, output_path, git_repo)
    
    # Generate workflow RBAC
    _generate_workflow_rbac(dataset_name, namespace, output_path)
    
    # Generate Argo workflow
    _generate_argo_workflow(dataset_name, namespace, output_path)
    
    print(f"\n✓ Generated complete workflow for {dataset_name}")
    print(f"\nFiles created in {output_dir}:")
    print(f"  - convert-job.yaml")
    print(f"  - pmtiles-job.yaml")
    print(f"  - hex-job.yaml")
    print(f"  - repartition-job.yaml")
    print(f"  - workflow-rbac.yaml")
    print(f"  - workflow.yaml")
    print(f"\nTo run:")
    print(f"  kubectl apply -f {output_dir}/workflow-rbac.yaml")
    print(f"  kubectl create configmap {dataset_name}-jobs --from-file={output_dir} -n {namespace}")
    print(f"  kubectl apply -f {output_dir}/workflow.yaml")


def _generate_convert_job(manager, dataset_name, source_url, bucket, output_path, git_repo):
    """Generate GeoParquet conversion job."""
    job_spec = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": f"{dataset_name}-convert",
            "labels": {"k8s-app": f"{dataset_name}-convert"}
        },
        "spec": {
            "completions": 1,
            "parallelism": 1,
            "backoffLimit": 1,
            "ttlSecondsAfterFinished": 10800,
            "template": {
                "metadata": {"labels": {"k8s-app": f"{dataset_name}-convert"}},
                "spec": {
                    "priorityClassName": "opportunistic",
                    "affinity": {
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
                    },
                    "restartPolicy": "Never",
                    "initContainers": [{
                        "name": "git-clone",
                        "image": "alpine/git:2.45.2",
                        "imagePullPolicy": "IfNotPresent",
                        "resources": {
                            "requests": {"cpu": "1", "memory": "1Gi"},
                            "limits": {"cpu": "1", "memory": "1Gi"}
                        },
                        "command": ["sh", "-lc", f"git clone --depth 1 \"{git_repo}\" /workspace/datasets"],
                        "volumeMounts": [{"name": "repo", "mountPath": "/workspace"}]
                    }],
                    "containers": [{
                        "name": "convert-task",
                        "image": "ghcr.io/rocker-org/ml-spatial",
                        "imagePullPolicy": "Always",
                        "workingDir": "/workspace/datasets",
                        "volumeMounts": [{"name": "repo", "mountPath": "/workspace"}],
                        "env": [
                            {"name": "AWS_ACCESS_KEY_ID", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_ACCESS_KEY_ID"}}},
                            {"name": "AWS_SECRET_ACCESS_KEY", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_SECRET_ACCESS_KEY"}}},
                            {"name": "AWS_S3_ENDPOINT", "value": "rook-ceph-rgw-nautiluss3.rook"},
                            {"name": "AWS_PUBLIC_ENDPOINT", "value": "s3-west.nrp-nautilus.io"},
                            {"name": "AWS_HTTPS", "value": "false"},
                            {"name": "AWS_VIRTUAL_HOSTING", "value": "FALSE"},
                            {"name": "GDAL_DATA", "value": "/opt/conda/share/gdal"},
                            {"name": "PROJ_LIB", "value": "/opt/conda/share/proj"}
                        ],
                        "command": ["bash", "-c", f"set -e\n./{output_path.name}/convert_gpkg_to_parquet.sh"],
                        "resources": {
                            "requests": {"cpu": "4", "memory": "8Gi"},
                            "limits": {"cpu": "8", "memory": "16Gi"}
                        }
                    }],
                    "volumes": [{"name": "repo", "emptyDir": {}}]
                }
            }
        }
    }
    manager.save_job_yaml(job_spec, str(output_path / "convert-job.yaml"))


def _generate_pmtiles_job(manager, dataset_name, source_url, bucket, output_path, git_repo):
    """Generate PMTiles job."""
    job_spec = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": f"{dataset_name}-pmtiles",
            "labels": {"k8s-app": f"{dataset_name}-pmtiles"}
        },
        "spec": {
            "completions": 1,
            "parallelism": 1,
            "backoffLimit": 1,
            "ttlSecondsAfterFinished": 10800,
            "template": {
                "metadata": {"labels": {"k8s-app": f"{dataset_name}-pmtiles"}},
                "spec": {
                    "priorityClassName": "opportunistic",
                    "affinity": {
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
                    },
                    "restartPolicy": "Never",
                    "initContainers": [{
                        "name": "git-clone",
                        "image": "alpine/git:2.45.2",
                        "imagePullPolicy": "IfNotPresent",
                        "resources": {
                            "requests": {"cpu": "1", "memory": "1Gi"},
                            "limits": {"cpu": "1", "memory": "1Gi"}
                        },
                        "command": ["sh", "-lc", f"git clone --depth 1 \"{git_repo}\" /workspace/datasets"],
                        "volumeMounts": [{"name": "repo", "mountPath": "/workspace"}]
                    }],
                    "containers": [{
                        "name": "pmtiles-task",
                        "image": "ghcr.io/rocker-org/ml-spatial",
                        "imagePullPolicy": "Always",
                        "workingDir": "/workspace/datasets",
                        "volumeMounts": [{"name": "repo", "mountPath": "/workspace"}],
                        "env": [
                            {"name": "AWS_ACCESS_KEY_ID", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_ACCESS_KEY_ID"}}},
                            {"name": "AWS_SECRET_ACCESS_KEY", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_SECRET_ACCESS_KEY"}}},
                            {"name": "AWS_S3_ENDPOINT", "value": "rook-ceph-rgw-nautiluss3.rook"},
                            {"name": "AWS_PUBLIC_ENDPOINT", "value": "s3-west.nrp-nautilus.io"},
                            {"name": "AWS_HTTPS", "value": "false"},
                            {"name": "AWS_VIRTUAL_HOSTING", "value": "FALSE"},
                            {"name": "GDAL_DATA", "value": "/opt/conda/share/gdal"},
                            {"name": "PROJ_LIB", "value": "/opt/conda/share/proj"}
                        ],
                        "command": ["bash", "-c", f"set -e\n./{output_path.name}/create_pmtiles.sh"],
                        "resources": {
                            "requests": {"cpu": "4", "memory": "8Gi"},
                            "limits": {"cpu": "8", "memory": "16Gi"}
                        }
                    }],
                    "volumes": [{"name": "repo", "emptyDir": {}}]
                }
            }
        }
    }
    manager.save_job_yaml(job_spec, str(output_path / "pmtiles-job.yaml"))


def _generate_hex_job(manager, dataset_name, bucket, output_path, git_repo):
    """Generate H3 hex tiling job."""
    job_spec = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": f"{dataset_name}-hex",
            "labels": {"k8s-app": f"{dataset_name}-hex"}
        },
        "spec": {
            "completions": 50,
            "parallelism": 20,
            "completionMode": "Indexed",
            "backoffLimitPerIndex": 3,
            "podFailurePolicy": {
                "rules": [{
                    "action": "Ignore",
                    "onPodConditions": [{"type": "DisruptionTarget"}]
                }]
            },
            "ttlSecondsAfterFinished": 10800,
            "template": {
                "metadata": {"labels": {"k8s-app": f"{dataset_name}-hex"}},
                "spec": {
                    "priorityClassName": "opportunistic",
                    "affinity": {
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
                    },
                    "restartPolicy": "Never",
                    "initContainers": [{
                        "name": "git-clone",
                        "image": "alpine/git:2.45.2",
                        "imagePullPolicy": "IfNotPresent",
                        "resources": {
                            "requests": {"cpu": "1", "memory": "1Gi"},
                            "limits": {"cpu": "1", "memory": "1Gi"}
                        },
                        "command": ["sh", "-lc", f"git clone --depth 1 \"{git_repo}\" /workspace/datasets"],
                        "volumeMounts": [{"name": "repo", "mountPath": "/workspace"}]
                    }],
                    "containers": [{
                        "name": "hex-task",
                        "image": "ghcr.io/rocker-org/ml-spatial",
                        "imagePullPolicy": "Always",
                        "workingDir": "/workspace/datasets",
                        "volumeMounts": [{"name": "repo", "mountPath": "/workspace"}],
                        "env": [
                            {"name": "AWS_ACCESS_KEY_ID", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_ACCESS_KEY_ID"}}},
                            {"name": "AWS_SECRET_ACCESS_KEY", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_SECRET_ACCESS_KEY"}}},
                            {"name": "AWS_S3_ENDPOINT", "value": "rook-ceph-rgw-nautiluss3.rook"},
                            {"name": "AWS_PUBLIC_ENDPOINT", "value": "s3-west.nrp-nautilus.io"},
                            {"name": "AWS_HTTPS", "value": "false"},
                            {"name": "AWS_VIRTUAL_HOSTING", "value": "FALSE"},
                            {"name": "GDAL_DATA", "value": "/opt/conda/share/gdal"},
                            {"name": "PROJ_LIB", "value": "/opt/conda/share/proj"},
                            {"name": "TMPDIR", "value": "/tmp"}
                        ],
                        "command": ["bash", "-c", f"set -e\npython {output_path.name}/vec.py --i ${{JOB_COMPLETION_INDEX}} --zoom 10"],
                        "resources": {
                            "requests": {"cpu": "4", "memory": "8Gi"},
                            "limits": {"cpu": "8", "memory": "16Gi"}
                        }
                    }],
                    "volumes": [{"name": "repo", "emptyDir": {}}]
                }
            }
        }
    }
    manager.save_job_yaml(job_spec, str(output_path / "hex-job.yaml"))


def _generate_repartition_job(manager, dataset_name, bucket, output_path, git_repo):
    """Generate repartition job."""
    job_spec = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": f"{dataset_name}-repartition",
            "labels": {"k8s-app": f"{dataset_name}-repartition"}
        },
        "spec": {
            "completions": 1,
            "parallelism": 1,
            "backoffLimit": 1,
            "ttlSecondsAfterFinished": 10800,
            "template": {
                "metadata": {"labels": {"k8s-app": f"{dataset_name}-repartition"}},
                "spec": {
                    "priorityClassName": "opportunistic",
                    "affinity": {
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
                    },
                    "restartPolicy": "Never",
                    "initContainers": [{
                        "name": "git-clone",
                        "image": "alpine/git:2.45.2",
                        "imagePullPolicy": "IfNotPresent",
                        "resources": {
                            "requests": {"cpu": "1", "memory": "1Gi"},
                            "limits": {"cpu": "1", "memory": "1Gi"}
                        },
                        "command": ["sh", "-lc", f"git clone --depth 1 \"{git_repo}\" /workspace/datasets"],
                        "volumeMounts": [{"name": "repo", "mountPath": "/workspace"}]
                    }],
                    "containers": [{
                        "name": "repartition-task",
                        "image": "ghcr.io/rocker-org/ml-spatial",
                        "imagePullPolicy": "Always",
                        "workingDir": "/workspace/datasets",
                        "volumeMounts": [{"name": "repo", "mountPath": "/workspace"}],
                        "env": [
                            {"name": "AWS_ACCESS_KEY_ID", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_ACCESS_KEY_ID"}}},
                            {"name": "AWS_SECRET_ACCESS_KEY", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_SECRET_ACCESS_KEY"}}},
                            {"name": "AWS_S3_ENDPOINT", "value": "rook-ceph-rgw-nautiluss3.rook"},
                            {"name": "AWS_PUBLIC_ENDPOINT", "value": "s3-west.nrp-nautilus.io"},
                            {"name": "AWS_HTTPS", "value": "false"},
                            {"name": "AWS_VIRTUAL_HOSTING", "value": "FALSE"},
                            {"name": "GDAL_DATA", "value": "/opt/conda/share/gdal"},
                            {"name": "PROJ_LIB", "value": "/opt/conda/share/proj"}
                        ],
                        "command": ["bash", "-c", f"set -e\npython {dataset_name}/repartition.py"],
                        "resources": {
                            "requests": {"cpu": "4", "memory": "8Gi"},
                            "limits": {"cpu": "8", "memory": "16Gi"}
                        }
                    }],
                    "volumes": [{"name": "repo", "emptyDir": {}}]
                }
            }
        }
    }
    manager.save_job_yaml(job_spec, str(output_path / "repartition-job.yaml"))


def _generate_workflow_rbac(dataset_name, namespace, output_path):
    """Generate workflow RBAC configuration."""
    import yaml
    
    rbac = {
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {
            "name": f"{dataset_name}-workflow",
            "namespace": namespace
        }
    }
    
    with open(output_path / "workflow-rbac.yaml", "w") as f:
        yaml.dump_all([
            rbac,
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "Role",
                "metadata": {"name": f"{dataset_name}-workflow", "namespace": namespace},
                "rules": [
                    {"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["get", "list", "watch", "create", "delete"]},
                    {"apiGroups": [""], "resources": ["pods", "pods/log"], "verbs": ["get", "list"]}
                ]
            },
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": {"name": f"{dataset_name}-workflow", "namespace": namespace},
                "subjects": [{"kind": "ServiceAccount", "name": f"{dataset_name}-workflow"}],
                "roleRef": {"kind": "Role", "name": f"{dataset_name}-workflow", "apiGroup": "rbac.authorization.k8s.io"}
            }
        ], f, default_flow_style=False)


def _generate_argo_workflow(dataset_name, namespace, output_path):
    """Generate K8s Job that orchestrates the workflow."""
    import yaml
    
    workflow_job = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": f"{dataset_name}-workflow",
            "namespace": namespace
        },
        "spec": {
            "template": {
                "spec": {
                    "serviceAccountName": f"{dataset_name}-workflow",
                    "restartPolicy": "OnFailure",
                    "containers": [{
                        "name": "workflow",
                        "image": "bitnami/kubectl:latest",
                        "command": ["bash", "-c"],
                        "args": [f"""
set -e
cd /workspace

echo "Starting {dataset_name} workflow..."

# Run convert and pmtiles in parallel
echo "Step 1: Converting to GeoParquet and PMTiles (parallel)..."
kubectl apply -f convert-job.yaml -n {namespace}
kubectl apply -f pmtiles-job.yaml -n {namespace}

echo "Waiting for conversion jobs to complete..."
kubectl wait --for=condition=complete --timeout=3600s job/{dataset_name}-convert -n {namespace}
kubectl wait --for=condition=complete --timeout=3600s job/{dataset_name}-pmtiles -n {namespace}

echo "Step 2: H3 hexagonal tiling (50 chunks, 20 parallel)..."
kubectl apply -f hex-job.yaml -n {namespace}

echo "Waiting for hex tiling to complete..."
kubectl wait --for=condition=complete --timeout=7200s job/{dataset_name}-hex -n {namespace}

echo "Step 3: Repartitioning by h0..."
kubectl apply -f repartition-job.yaml -n {namespace}

echo "Waiting for repartition to complete..."
kubectl wait --for=condition=complete --timeout=3600s job/{dataset_name}-repartition -n {namespace}

echo "✓ Workflow complete!"
echo ""
echo "Clean up jobs with:"
echo "  kubectl delete jobs {dataset_name}-convert {dataset_name}-pmtiles {dataset_name}-hex {dataset_name}-repartition -n {namespace}"
"""],
                        "resources": {
                            "requests": {"cpu": "500m", "memory": "512Mi"},
                            "limits": {"cpu": "500m", "memory": "512Mi"}
                        },
                        "volumeMounts": [{
                            "name": "jobs",
                            "mountPath": "/workspace"
                        }]
                    }],
                    "volumes": [{
                        "name": "jobs",
                        "configMap": {"name": f"{dataset_name}-jobs"}
                    }]
                }
            },
            "ttlSecondsAfterFinished": 10800
        }
    }
    
    with open(output_path / "workflow.yaml", "w") as f:
        yaml.dump(workflow_job, f, default_flow_style=False)
