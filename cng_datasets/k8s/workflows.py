"""
Workflow generation utilities.

Functions for creating complete dataset processing workflows with
multiple coordinated Kubernetes jobs.
"""

from typing import Optional, Dict, Any, List
from pathlib import Path
import math
from .jobs import K8sJobManager


def _count_source_features(source_url: str) -> int:
    """
    Count features in a source file (shapefile, geopackage, etc.) using GDAL with vsicurl.
    
    Args:
        source_url: HTTP(S) or S3 URL to source vector file
        
    Returns:
        Number of features in the source file
    """
    import subprocess
    
    # Convert s3:// URLs to https:// for vsicurl
    if source_url.startswith('s3://'):
        path = source_url.replace('s3://', '')
        source_url = f"https://s3-west.nrp-nautilus.io/{path}"
    
    # Use vsicurl prefix for remote files
    if source_url.startswith('http://') or source_url.startswith('https://'):
        gdal_path = f"/vsicurl/{source_url}"
    else:
        gdal_path = source_url
    
    # Use ogrinfo to count features
    # -so: summary only (faster), -al: all layers
    result = subprocess.run(
        ['ogrinfo', '-so', '-al', gdal_path],
        capture_output=True,
        text=True,
        check=True
    )
    
    # Parse output to find "Feature Count: XXXXX"
    for line in result.stdout.split('\n'):
        if 'Feature Count:' in line:
            count_str = line.split(':')[1].strip()
            return int(count_str)
    
    raise ValueError("Could not find feature count in ogrinfo output")


def _calculate_chunking(total_rows: int, max_completions: int = 200, max_parallelism: int = 50) -> tuple[int, int, int]:
    """
    Calculate optimal chunk size, completions, and parallelism.
    
    Args:
        total_rows: Total number of rows/features in dataset
        max_completions: Maximum number of job completions (default: 200)
        max_parallelism: Maximum parallelism (default: 50)
        
    Returns:
        Tuple of (chunk_size, completions, parallelism)
    """
    # Calculate chunk size to stay under max_completions
    chunk_size = math.ceil(total_rows / max_completions)
    
    # Calculate actual number of completions needed
    completions = math.ceil(total_rows / chunk_size)
    
    # Set parallelism to min of max_parallelism or completions
    parallelism = min(max_parallelism, completions)
    
    return chunk_size, completions, parallelism


def generate_dataset_workflow(
    dataset_name: str,
    source_url: str,
    bucket: str,
    output_dir: str = ".",
    namespace: str = "biodiversity",
    image: str = "ghcr.io/boettiger-lab/datasets:latest",
    git_repo: str = "https://github.com/boettiger-lab/datasets.git",
    h3_resolution: int = 10,
    parent_resolutions: Optional[List[int]] = None,
    id_column: Optional[str] = None,
    hex_memory: str = "8Gi",
    max_parallelism: int = 50,
    max_completions: int = 200,
    intermediate_chunk_size: int = 10
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
        git_repo: Git repository URL for package source
        h3_resolution: Target H3 resolution for tiling (default: 10)
        parent_resolutions: List of parent H3 resolutions to include (default: [9, 8, 0])
        id_column: ID column name (auto-detected if not specified)
        hex_memory: Memory request/limit for hex job pods (default: "8Gi")
        max_parallelism: Maximum parallelism for hex jobs (default: 50)
        max_completions: Maximum job completions - increase to reduce chunk size (default: 200)
        intermediate_chunk_size: Number of rows to process in pass 2 (unnesting arrays) - reduce if hitting OOM (default: 10)
    """
    manager = K8sJobManager(namespace=namespace, image=image)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Sanitize dataset name for Kubernetes (replace underscores with hyphens)
    k8s_name = dataset_name.replace('_', '-').lower()
    
    # Set defaults for parent resolutions if not provided
    if parent_resolutions is None:
        parent_resolutions = [9, 8, 0]
    
    # Generate bucket setup job (must run first)
    _generate_setup_bucket_job(manager, k8s_name, bucket, output_path, git_repo)
    
    # Generate conversion job
    _generate_convert_job(manager, k8s_name, source_url, bucket, output_path, git_repo)
    
    # Generate pmtiles job
    _generate_pmtiles_job(manager, k8s_name, source_url, bucket, output_path, git_repo)
    
    # Count features in source file and calculate chunking parameters
    print(f"Counting features in {source_url}...")
    try:
        total_rows = _count_source_features(source_url)
        chunk_size, completions, parallelism = _calculate_chunking(total_rows, max_completions=max_completions, max_parallelism=max_parallelism)
        print(f"  Total features: {total_rows:,}")
        print(f"  Chunk size: {chunk_size:,}")
        print(f"  Completions: {completions}")
        print(f"  Parallelism: {parallelism}")
    except Exception as e:
        # Fall back to default values if counting fails (e.g., in tests or if file doesn't exist yet)
        print(f"  Warning: Could not count features ({e}). Using default chunking parameters.")
        total_rows = 10000  # Default assumption
        chunk_size, completions, parallelism = _calculate_chunking(total_rows, max_completions=max_completions, max_parallelism=max_parallelism)
        print(f"  Using defaults: chunk_size={chunk_size}, completions={completions}, parallelism={parallelism}")
    
    print(f"  H3 resolution: {h3_resolution}")
    print(f"  Parent resolutions: {parent_resolutions}")
    
    # Generate hex tiling job
    _generate_hex_job(manager, k8s_name, bucket, output_path, git_repo, chunk_size, completions, parallelism, h3_resolution, parent_resolutions, id_column, hex_memory, intermediate_chunk_size)
    
    # Generate repartition job
    _generate_repartition_job(manager, k8s_name, bucket, output_path, git_repo)
    
    # Generate workflow RBAC (generic for all cng-datasets workflows)
    _generate_workflow_rbac(namespace, output_path)
    
    # Build generation command for documentation
    parent_res_str = ','.join(map(str, parent_resolutions))
    gen_command = (f"cng-datasets workflow --dataset {dataset_name} "
                   f"--source-url {source_url} --bucket {bucket} "
                   f"--h3-resolution {h3_resolution} --parent-resolutions \"{parent_res_str}\"")
    
    # Generate ConfigMap YAML from job files
    _generate_configmap(k8s_name, namespace, output_path, gen_command)
    
    # Generate Argo workflow with ConfigMap-based approach
    _generate_argo_workflow(k8s_name, namespace, output_path, output_dir)
    
    print(f"\n✓ Generated complete workflow for {dataset_name}")
    print(f"\nFiles created in {output_dir}:")
    print(f"  - setup-bucket-job.yaml")
    print(f"  - convert-job.yaml")
    print(f"  - pmtiles-job.yaml")
    print(f"  - hex-job.yaml")
    print(f"  - repartition-job.yaml")
    print(f"  - workflow-rbac.yaml (generic, reusable)")
    print(f"  - configmap.yaml (job configs)")
    print(f"  - workflow.yaml (orchestrator)")
    print(f"\nTo run:")
    print(f"  # One-time RBAC setup")
    print(f"  kubectl apply -f {output_dir}/workflow-rbac.yaml")
    print(f"")
    print(f"  # Apply all workflow files (safe to re-run)")
    print(f"  kubectl apply -f {output_dir}/configmap.yaml")
    print(f"  kubectl apply -f {output_dir}/workflow.yaml")
    print(f"")
    print(f"  # Monitor progress")
    print(f"  kubectl logs -f job/{k8s_name}-workflow")
    print(f"")
    print(f"  # Clean up")
    print(f"  kubectl delete -f {output_dir}/workflow.yaml")
    print(f"  kubectl delete -f {output_dir}/configmap.yaml")


def _generate_setup_bucket_job(manager, dataset_name, bucket, output_path, git_repo):
    """Generate S3 bucket setup job with public access and CORS."""
    job_spec = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": f"{dataset_name}-setup-bucket",
            "labels": {"k8s-app": f"{dataset_name}-setup-bucket"}
        },
        "spec": {
            "completions": 1,
            "parallelism": 1,
            "backoffLimit": 2,
            "ttlSecondsAfterFinished": 10800,
            "template": {
                "metadata": {"labels": {"k8s-app": f"{dataset_name}-setup-bucket"}},
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
                    "containers": [{
                        "name": "setup-bucket-task",
                        "image": "ghcr.io/boettiger-lab/datasets:latest",
                        "imagePullPolicy": "Always",
                        "env": [
                            {"name": "AWS_ACCESS_KEY_ID", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_ACCESS_KEY_ID"}}},
                            {"name": "AWS_SECRET_ACCESS_KEY", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_SECRET_ACCESS_KEY"}}},
                            {"name": "AWS_S3_ENDPOINT", "value": "rook-ceph-rgw-nautiluss3.rook"},
                            {"name": "AWS_PUBLIC_ENDPOINT", "value": "s3-west.nrp-nautilus.io"},
                            {"name": "AWS_HTTPS", "value": "false"},
                            {"name": "AWS_VIRTUAL_HOSTING", "value": "FALSE"},
                            {"name": "BUCKET", "value": bucket}
                        ],
                        "volumeMounts": [
                            {"name": "rclone-config", "mountPath": "/root/.config/rclone", "readOnly": True}
                        ],
                        "command": ["bash", "-c", f"""set -e
echo "Installing cng-datasets..."
pip install -q git+{git_repo}

echo "Setting up bucket with public access and CORS..."
cng-datasets storage setup-bucket \\
  --bucket "${{BUCKET}}" \\
  --remote nrp \\
  --verify

echo "Bucket setup complete!"
"""],
                        "resources": {
                            "requests": {"cpu": "1", "memory": "2Gi"},
                            "limits": {"cpu": "1", "memory": "2Gi"}
                        }
                    }],
                    "volumes": [
                        {"name": "rclone-config", "secret": {"secretName": "rclone-config"}}
                    ]
                }
            }
        }
    }
    manager.save_job_yaml(job_spec, str(output_path / "setup-bucket-job.yaml"))


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
                    "containers": [{
                        "name": "convert-task",
                        "image": "ghcr.io/boettiger-lab/datasets:latest",
                        "imagePullPolicy": "Always",
                        "env": [
                            {"name": "AWS_ACCESS_KEY_ID", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_ACCESS_KEY_ID"}}},
                            {"name": "AWS_SECRET_ACCESS_KEY", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_SECRET_ACCESS_KEY"}}},
                            {"name": "AWS_S3_ENDPOINT", "value": "rook-ceph-rgw-nautiluss3.rook"},
                            {"name": "AWS_PUBLIC_ENDPOINT", "value": "s3-west.nrp-nautilus.io"},
                            {"name": "AWS_HTTPS", "value": "false"},
                            {"name": "AWS_VIRTUAL_HOSTING", "value": "FALSE"},
                            {"name": "BUCKET", "value": bucket}
                        ],
                        "volumeMounts": [
                            {"name": "rclone-config", "mountPath": "/root/.config/rclone", "readOnly": True}
                        ],
                        "command": ["bash", "-c", f"""set -e
pip install -q git+{git_repo}
cng-convert-to-parquet \\
  {source_url} \\
  /vsis3/{bucket}/{dataset_name}.parquet
"""],
                        "resources": {
                            "requests": {"cpu": "4", "memory": "8Gi"},
                            "limits": {"cpu": "4", "memory": "8Gi"}
                        }
                    }],
                    "volumes": [
                        {"name": "rclone-config", "secret": {"secretName": "rclone-config"}}
                    ]
                }
            }
        }
    }
    manager.save_job_yaml(job_spec, str(output_path / "convert-job.yaml"))


def _generate_pmtiles_job(manager, dataset_name, source_url, bucket, output_path, git_repo):
    """Generate PMTiles job.
    
    Uses the optimized GeoParquet from convert job as input (includes ID column).
    """
    # Use the converted geoparquet instead of original source
    geoparquet_url = f"https://s3-west.nrp-nautilus.io/{bucket}/{dataset_name}.parquet"
    
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
                    "containers": [{
                        "name": "pmtiles-task",
                        "image": "ghcr.io/boettiger-lab/datasets:latest",
                        "imagePullPolicy": "Always",
                        "env": [
                            {"name": "AWS_ACCESS_KEY_ID", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_ACCESS_KEY_ID"}}},
                            {"name": "AWS_SECRET_ACCESS_KEY", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_SECRET_ACCESS_KEY"}}},
                            {"name": "AWS_S3_ENDPOINT", "value": "rook-ceph-rgw-nautiluss3.rook"},
                            {"name": "AWS_PUBLIC_ENDPOINT", "value": "s3-west.nrp-nautilus.io"},
                            {"name": "AWS_HTTPS", "value": "false"},
                            {"name": "AWS_VIRTUAL_HOSTING", "value": "FALSE"},
                            {"name": "TMPDIR", "value": "/tmp"},
                            {"name": "BUCKET", "value": bucket},
                            {"name": "DATASET", "value": dataset_name}
                        ],
                        "volumeMounts": [
                            {"name": "rclone-config", "mountPath": "/root/.config/rclone", "readOnly": True}
                        ],
                        "command": ["bash", "-c", f"""set -e
# Use optimized GeoParquet (has ID column) from convert job
# GDAL can read parquet via vsicurl
ogr2ogr -f GeoJSONSeq /tmp/$DATASET.geojsonl /vsicurl/{geoparquet_url} -progress

# Generate PMTiles from GeoJSONSeq
tippecanoe -o /tmp/$DATASET.pmtiles -l $DATASET --drop-densest-as-needed --extend-zooms-if-still-dropping --force /tmp/$DATASET.geojsonl

# Upload to S3 using rclone
rclone copy /tmp/$DATASET.pmtiles nrp:{bucket}/
rm /tmp/$DATASET.geojsonl /tmp/$DATASET.pmtiles
"""],
                        "resources": {
                            "requests": {"cpu": "4", "memory": "8Gi"},
                            "limits": {"cpu": "4", "memory": "8Gi"}
                        }
                    }],
                    "volumes": [
                        {"name": "rclone-config", "secret": {"secretName": "rclone-config"}}
                    ]
                }
            }
        }
    }
    manager.save_job_yaml(job_spec, str(output_path / "pmtiles-job.yaml"))


def _generate_hex_job(manager, dataset_name, bucket, output_path, git_repo, chunk_size, completions, parallelism, h3_resolution, parent_resolutions, id_column=None, hex_memory="8Gi", intermediate_chunk_size=10):
    """Generate H3 hex tiling job.
    
    Args:
        manager: K8sJobManager instance
        dataset_name: Name of the dataset
        bucket: S3 bucket name
        output_path: Path to write YAML files
        git_repo: Git repository URL
        chunk_size: Number of rows per chunk
        completions: Number of job completions
        parallelism: Number of parallel jobs
        h3_resolution: Target H3 resolution (e.g., 10)
        parent_resolutions: List of parent resolutions (e.g., [9, 8, 0])
        id_column: ID column name (auto-detected if None)
        hex_memory: Memory request/limit (e.g., "8Gi", "16Gi")
        intermediate_chunk_size: Number of rows to process in pass 2 (unnesting arrays)
    """
    # Format parent resolutions as comma-separated string
    parent_res_str = ','.join(map(str, parent_resolutions))
    
    # Build command with optional id-column parameter
    cmd_parts = [
        "set -e",
        f"pip install -q git+{git_repo}",
        f"cng-datasets vector --input s3://{bucket}/{dataset_name}.parquet --output s3://{bucket}/{dataset_name}/chunks --chunk-id ${{JOB_COMPLETION_INDEX}} --chunk-size {chunk_size} --intermediate-chunk-size {intermediate_chunk_size} --resolution {h3_resolution} --parent-resolutions {parent_res_str}"
    ]
    if id_column:
        cmd_parts[-1] += f" --id-column {id_column}"
    
    command_str = "\n".join(cmd_parts)
    
    job_spec = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": f"{dataset_name}-hex",
            "labels": {"k8s-app": f"{dataset_name}-hex"}
        },
        "spec": {
            "completions": completions,
            "parallelism": parallelism,
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
                    "containers": [{
                        "name": "hex-task",
                        "image": "ghcr.io/boettiger-lab/datasets:latest",
                        "imagePullPolicy": "Always",

                        "env": [
                            {"name": "AWS_ACCESS_KEY_ID", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_ACCESS_KEY_ID"}}},
                            {"name": "AWS_SECRET_ACCESS_KEY", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_SECRET_ACCESS_KEY"}}},
                            {"name": "AWS_S3_ENDPOINT", "value": "rook-ceph-rgw-nautiluss3.rook"},
                            {"name": "AWS_PUBLIC_ENDPOINT", "value": "s3-west.nrp-nautilus.io"},
                            {"name": "AWS_HTTPS", "value": "false"},
                            {"name": "AWS_VIRTUAL_HOSTING", "value": "FALSE"},
                            {"name": "TMPDIR", "value": "/tmp"},
                            {"name": "BUCKET", "value": bucket},
                            {"name": "DATASET", "value": dataset_name}
                        ],
                        "command": ["bash", "-c", command_str],
                        "resources": {
                            "requests": {"cpu": "4", "memory": hex_memory},
                            "limits": {"cpu": "4", "memory": hex_memory}
                        }
                    }]
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
                    "containers": [{
                        "name": "repartition-task",
                        "image": "ghcr.io/boettiger-lab/datasets:latest",
                        "imagePullPolicy": "Always",
                        "env": [
                            {"name": "AWS_ACCESS_KEY_ID", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_ACCESS_KEY_ID"}}},
                            {"name": "AWS_SECRET_ACCESS_KEY", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_SECRET_ACCESS_KEY"}}},
                            {"name": "AWS_S3_ENDPOINT", "value": "rook-ceph-rgw-nautiluss3.rook"},
                            {"name": "AWS_PUBLIC_ENDPOINT", "value": "s3-west.nrp-nautilus.io"},
                            {"name": "AWS_HTTPS", "value": "false"},
                            {"name": "AWS_VIRTUAL_HOSTING", "value": "FALSE"},
                            {"name": "TMPDIR", "value": "/tmp"},
                            {"name": "BUCKET", "value": bucket}
                        ],
                        "volumeMounts": [
                            {"name": "rclone-config", "mountPath": "/root/.config/rclone", "readOnly": True}
                        ],
                        "command": ["bash", "-c", f"""set -e
pip install -q git+{git_repo}
cng-datasets repartition --chunks-dir s3://{bucket}/{dataset_name}/chunks --output-dir s3://{bucket}/{dataset_name}/hex --source-parquet s3://{bucket}/{dataset_name}.parquet --cleanup
"""],
                        "resources": {
                            "requests": {"cpu": "4", "memory": "8Gi"},
                            "limits": {"cpu": "4", "memory": "8Gi"}
                        }
                    }],
                    "volumes": [
                        {"name": "rclone-config", "secret": {"secretName": "rclone-config"}}
                    ]
                }
            }
        }
    }
    manager.save_job_yaml(job_spec, str(output_path / "repartition-job.yaml"))


def _generate_workflow_rbac(namespace, output_path):
    """Generate generic workflow RBAC configuration for cng-datasets package."""
    import yaml
    
    rbac = {
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {
            "name": "cng-datasets-workflow",
            "namespace": namespace
        }
    }
    
    with open(output_path / "workflow-rbac.yaml", "w") as f:
        yaml.dump_all([
            rbac,
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "Role",
                "metadata": {"name": "cng-datasets-workflow", "namespace": namespace},
                "rules": [
                    {"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["get", "list", "watch", "create", "delete"]},
                    {"apiGroups": [""], "resources": ["pods", "pods/log"], "verbs": ["get", "list"]}
                ]
            },
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": {"name": "cng-datasets-workflow", "namespace": namespace},
                "subjects": [{"kind": "ServiceAccount", "name": "cng-datasets-workflow"}],
                "roleRef": {"kind": "Role", "name": "cng-datasets-workflow", "apiGroup": "rbac.authorization.k8s.io"}
            }
        ], f, default_flow_style=False)


def _generate_configmap(dataset_name, namespace, output_path, gen_command):
    """Generate ConfigMap YAML containing all job definitions.
    
    Args:
        dataset_name: Sanitized k8s-compatible dataset name
        namespace: Kubernetes namespace  
        output_path: Path object where YAML files are written
        gen_command: Command used to generate this workflow
    """
    import yaml
    
    # Read all job YAML files
    job_files = ["setup-bucket-job.yaml", "convert-job.yaml", "pmtiles-job.yaml", "hex-job.yaml", "repartition-job.yaml"]
    data = {}
    
    for job_file in job_files:
        file_path = output_path / job_file
        if file_path.exists():
            with open(file_path, 'r') as f:
                data[job_file] = f.read()
    
    configmap = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": f"{dataset_name}-yamls",
            "namespace": namespace
        },
        "data": data
    }
    
    with open(output_path / "configmap.yaml", "w") as f:
        f.write("# Auto-generated ConfigMap containing job definitions\n")
        f.write("# Generated from: setup-bucket-job.yaml, convert-job.yaml, pmtiles-job.yaml, hex-job.yaml, repartition-job.yaml\n")
        f.write(f"# Generation command: {gen_command}\n")
        f.write("#\n")
        f.write("# This ConfigMap is equivalent to running:\n")
        f.write(f"#   kubectl create configmap {dataset_name}-yamls \\\n")
        f.write("#     --from-file=setup-bucket-job.yaml \\\n")
        f.write("#     --from-file=convert-job.yaml \\\n")
        f.write("#     --from-file=pmtiles-job.yaml \\\n")
        f.write("#     --from-file=hex-job.yaml \\\n")
        f.write("#     --from-file=repartition-job.yaml\n")
        f.write("#\n")
        f.write("# To update: regenerate workflow with cng-datasets and re-apply with kubectl apply -f configmap.yaml\n")
        yaml.dump(configmap, f, default_flow_style=False)


def _generate_argo_workflow(dataset_name, namespace, output_path, output_dir):
    """Generate K8s Job that orchestrates the workflow using a ConfigMap.
    
    Args:
        dataset_name: Sanitized k8s-compatible dataset name (with hyphens)
        namespace: Kubernetes namespace
        output_path: Path object where YAML files are written
        output_dir: String path to YAML directory for kubectl create configmap command
    """
    import yaml
    
    configmap_name = f"{dataset_name}-yamls"
    
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
                    "serviceAccountName": "cng-datasets-workflow",
                    "restartPolicy": "OnFailure",
                    "containers": [{
                        "name": "workflow",
                        "image": "bitnami/kubectl:latest",
                        "command": ["bash", "-c"],
                        "args": [f"""
set -e

echo "Starting {dataset_name} workflow..."

# Step 0: Setup bucket with public access and CORS
echo "Step 0: Setting up bucket..."
kubectl apply -f /yamls/setup-bucket-job.yaml -n {namespace}

echo "Waiting for bucket setup to complete..."
kubectl wait --for=condition=complete --timeout=600s job/{dataset_name}-setup-bucket -n {namespace}

# Step 1: Convert to optimized GeoParquet (needed by both pmtiles and hex jobs)
echo "Step 1: Converting to optimized GeoParquet..."
kubectl apply -f /yamls/convert-job.yaml -n {namespace}

echo "Waiting for GeoParquet conversion to complete..."
kubectl wait --for=condition=complete --timeout=3600s job/{dataset_name}-convert -n {namespace}

# Step 2: Run pmtiles and hex tiling in parallel (both use the converted geoparquet)
echo "Step 2: H3 hexagonal tiling and PMTiles generation (parallel)..."
kubectl apply -f /yamls/pmtiles-job.yaml -n {namespace}
kubectl apply -f /yamls/hex-job.yaml -n {namespace}

echo "Waiting for hex tiling to complete (PMTiles continues in background)..."
kubectl wait --for=condition=complete --timeout=7200s job/{dataset_name}-hex -n {namespace}

echo "Step 3: Repartitioning by h0..."
kubectl apply -f /yamls/repartition-job.yaml -n {namespace}

echo "Waiting for repartition to complete..."
kubectl wait --for=condition=complete --timeout=3600s job/{dataset_name}-repartition -n {namespace}

echo "✓ Workflow complete!"
echo "Note: PMTiles job may still be running in the background"
echo ""
echo "Clean up with:"
echo "  kubectl delete jobs {dataset_name}-setup-bucket {dataset_name}-convert {dataset_name}-pmtiles {dataset_name}-hex {dataset_name}-repartition {dataset_name}-workflow -n {namespace}"
echo "  kubectl delete configmap {configmap_name} -n {namespace}"
"""],
                        "resources": {
                            "requests": {"cpu": "500m", "memory": "512Mi"},
                            "limits": {"cpu": "500m", "memory": "512Mi"}
                        },
                        "volumeMounts": [{"name": "yamls", "mountPath": "/yamls"}]
                    }],
                    "volumes": [{
                        "name": "yamls",
                        "configMap": {"name": configmap_name}
                    }]
                }
            },
            "ttlSecondsAfterFinished": 10800
        }
    }
    
    with open(output_path / "workflow.yaml", "w") as f:
        yaml.dump(workflow_job, f, default_flow_style=False)
