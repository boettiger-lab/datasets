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
    image: str = "ghcr.io/rocker-org/ml-spatial",
    git_repo: str = "https://github.com/boettiger-lab/datasets.git",
    h3_resolution: int = 10,
    parent_resolutions: Optional[List[int]] = None
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
    """
    manager = K8sJobManager(namespace=namespace, image=image)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Sanitize dataset name for Kubernetes (replace underscores with hyphens)
    k8s_name = dataset_name.replace('_', '-').lower()
    
    # Set defaults for parent resolutions if not provided
    if parent_resolutions is None:
        parent_resolutions = [9, 8, 0]
    
    # Generate conversion job
    _generate_convert_job(manager, k8s_name, source_url, bucket, output_path, git_repo)
    
    # Generate pmtiles job
    _generate_pmtiles_job(manager, k8s_name, source_url, bucket, output_path, git_repo)
    
    # Count features in source file and calculate chunking parameters
    print(f"Counting features in {source_url}...")
    try:
        total_rows = _count_source_features(source_url)
        chunk_size, completions, parallelism = _calculate_chunking(total_rows)
        print(f"  Total features: {total_rows:,}")
        print(f"  Chunk size: {chunk_size:,}")
        print(f"  Completions: {completions}")
        print(f"  Parallelism: {parallelism}")
    except Exception as e:
        # Fall back to default values if counting fails (e.g., in tests or if file doesn't exist yet)
        print(f"  Warning: Could not count features ({e}). Using default chunking parameters.")
        total_rows = 10000  # Default assumption
        chunk_size, completions, parallelism = _calculate_chunking(total_rows)
        print(f"  Using defaults: chunk_size={chunk_size}, completions={completions}, parallelism={parallelism}")
    
    print(f"  H3 resolution: {h3_resolution}")
    print(f"  Parent resolutions: {parent_resolutions}")
    
    # Generate hex tiling job
    _generate_hex_job(manager, k8s_name, bucket, output_path, git_repo, chunk_size, completions, parallelism, h3_resolution, parent_resolutions)
    
    # Generate repartition job
    _generate_repartition_job(manager, k8s_name, bucket, output_path, git_repo)
    
    # Generate workflow RBAC (generic for all cng-datasets workflows)
    _generate_workflow_rbac(namespace, output_path)
    
    # Generate Argo workflow with ConfigMap-based approach
    _generate_argo_workflow(k8s_name, namespace, output_path, output_dir)
    
    print(f"\n✓ Generated complete workflow for {dataset_name}")
    print(f"\nFiles created in {output_dir}:")
    print(f"  - convert-job.yaml")
    print(f"  - pmtiles-job.yaml")
    print(f"  - hex-job.yaml")
    print(f"  - repartition-job.yaml")
    print(f"  - workflow-rbac.yaml (generic, reusable)")
    print(f"  - workflow.yaml (orchestrator)")
    print(f"\nTo run:")
    print(f"  # One-time RBAC setup")
    print(f"  kubectl apply -f {output_dir}/workflow-rbac.yaml")
    print(f"")
    print(f"  # Create ConfigMap from job YAMLs and run workflow")
    print(f"  kubectl create configmap {k8s_name}-yamls \\")
    print(f"    --from-file={output_dir}/convert-job.yaml \\")
    print(f"    --from-file={output_dir}/pmtiles-job.yaml \\")
    print(f"    --from-file={output_dir}/hex-job.yaml \\")
    print(f"    --from-file={output_dir}/repartition-job.yaml")
    print(f"  kubectl apply -f {output_dir}/workflow.yaml")
    print(f"")
    print(f"  # Monitor progress")
    print(f"  kubectl logs -f job/{k8s_name}-workflow")


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
                        "image": "ghcr.io/rocker-org/ml-spatial",
                        "imagePullPolicy": "Always",
                        "env": [
                            {"name": "AWS_ACCESS_KEY_ID", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_ACCESS_KEY_ID"}}},
                            {"name": "AWS_SECRET_ACCESS_KEY", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_SECRET_ACCESS_KEY"}}},
                            {"name": "AWS_S3_ENDPOINT", "value": "rook-ceph-rgw-nautiluss3.rook"},
                            {"name": "AWS_PUBLIC_ENDPOINT", "value": "s3-west.nrp-nautilus.io"},
                            {"name": "AWS_HTTPS", "value": "false"},
                            {"name": "AWS_VIRTUAL_HOSTING", "value": "FALSE"},
                            {"name": "GDAL_DATA", "value": "/opt/conda/share/gdal"},
                            {"name": "PROJ_LIB", "value": "/opt/conda/share/proj"},
                            {"name": "BUCKET", "value": bucket}
                        ],
                        "command": ["bash", "-c", f"""set -e
# Install package
pip install -q git+{git_repo}

# Create bucket and set public access
python -c "
from cng_datasets.storage.rclone import create_public_bucket
create_public_bucket('{bucket}', remote='nrp', set_cors=True)
"

# Convert to GeoParquet
ogr2ogr -f Parquet /vsis3/{bucket}/{dataset_name}.parquet /vsicurl/{source_url} -progress
"""],
                        "resources": {
                            "requests": {"cpu": "4", "memory": "8Gi"},
                            "limits": {"cpu": "4", "memory": "8Gi"}
                        }
                    }]
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
                    "containers": [{
                        "name": "pmtiles-task",
                        "image": "ghcr.io/rocker-org/ml-spatial",
                        "imagePullPolicy": "Always",
                        "env": [
                            {"name": "AWS_ACCESS_KEY_ID", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_ACCESS_KEY_ID"}}},
                            {"name": "AWS_SECRET_ACCESS_KEY", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_SECRET_ACCESS_KEY"}}},
                            {"name": "AWS_S3_ENDPOINT", "value": "rook-ceph-rgw-nautiluss3.rook"},
                            {"name": "AWS_PUBLIC_ENDPOINT", "value": "s3-west.nrp-nautilus.io"},
                            {"name": "AWS_HTTPS", "value": "false"},
                            {"name": "AWS_VIRTUAL_HOSTING", "value": "FALSE"},
                            {"name": "GDAL_DATA", "value": "/opt/conda/share/gdal"},
                            {"name": "PROJ_LIB", "value": "/opt/conda/share/proj"},
                            {"name": "TMPDIR", "value": "/tmp"},
                            {"name": "BUCKET", "value": bucket},
                            {"name": "SOURCE_URL", "value": source_url}
                        ],
                        "command": ["bash", "-c", f"""set -e
# Convert GPKG to GeoJSONSeq for tippecanoe (read directly via vsicurl)
ogr2ogr -f GeoJSONSeq /tmp/mappinginequality.geojsonl /vsicurl/{source_url} -progress

# Generate PMTiles from GeoJSONSeq
tippecanoe -o /tmp/mappinginequality.pmtiles -l redlining --drop-densest-as-needed --extend-zooms-if-still-dropping --force /tmp/mappinginequality.geojsonl

# Upload to S3
mc alias set s3 https://$AWS_PUBLIC_ENDPOINT $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY
mc cp /tmp/mappinginequality.pmtiles s3/{bucket}/mappinginequality.pmtiles
rm /tmp/mappinginequality.geojsonl /tmp/mappinginequality.pmtiles
"""],
                        "resources": {
                            "requests": {"cpu": "4", "memory": "8Gi"},
                            "limits": {"cpu": "4", "memory": "8Gi"}
                        }
                    }]
                }
            }
        }
    }
    manager.save_job_yaml(job_spec, str(output_path / "pmtiles-job.yaml"))


def _generate_hex_job(manager, dataset_name, bucket, output_path, git_repo, chunk_size, completions, parallelism, h3_resolution, parent_resolutions):
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
    """
    # Format parent resolutions as comma-separated string
    parent_res_str = ','.join(map(str, parent_resolutions))
    
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
                        "image": "ghcr.io/rocker-org/ml-spatial",
                        "imagePullPolicy": "Always",

                        "env": [
                            {"name": "AWS_ACCESS_KEY_ID", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_ACCESS_KEY_ID"}}},
                            {"name": "AWS_SECRET_ACCESS_KEY", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_SECRET_ACCESS_KEY"}}},
                            {"name": "AWS_S3_ENDPOINT", "value": "rook-ceph-rgw-nautiluss3.rook"},
                            {"name": "AWS_PUBLIC_ENDPOINT", "value": "s3-west.nrp-nautilus.io"},
                            {"name": "AWS_HTTPS", "value": "false"},
                            {"name": "AWS_VIRTUAL_HOSTING", "value": "FALSE"},
                            {"name": "GDAL_DATA", "value": "/opt/conda/share/gdal"},
                            {"name": "PROJ_LIB", "value": "/opt/conda/share/proj"},
                            {"name": "TMPDIR", "value": "/tmp"},
                            {"name": "BUCKET", "value": bucket},
                            {"name": "DATASET", "value": dataset_name}
                        ],
                        "command": ["bash", "-c", f"set -e\npip install -q git+{git_repo}\ncng-datasets vector --input s3://{bucket}/{dataset_name}.parquet --output s3://{bucket}/chunks --chunk-id ${{JOB_COMPLETION_INDEX}} --chunk-size {chunk_size} --resolution {h3_resolution} --parent-resolutions {parent_res_str}"],
                        "resources": {
                            "requests": {"cpu": "4", "memory": "8Gi"},
                            "limits": {"cpu": "4", "memory": "8Gi"}
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
                        "image": "ghcr.io/rocker-org/ml-spatial",
                        "imagePullPolicy": "Always",
                        "env": [
                            {"name": "AWS_ACCESS_KEY_ID", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_ACCESS_KEY_ID"}}},
                            {"name": "AWS_SECRET_ACCESS_KEY", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_SECRET_ACCESS_KEY"}}},
                            {"name": "AWS_S3_ENDPOINT", "value": "rook-ceph-rgw-nautiluss3.rook"},
                            {"name": "AWS_PUBLIC_ENDPOINT", "value": "s3-west.nrp-nautilus.io"},
                            {"name": "AWS_HTTPS", "value": "false"},
                            {"name": "AWS_VIRTUAL_HOSTING", "value": "FALSE"},
                            {"name": "GDAL_DATA", "value": "/opt/conda/share/gdal"},
                            {"name": "PROJ_LIB", "value": "/opt/conda/share/proj"},
                            {"name": "TMPDIR", "value": "/tmp"},
                            {"name": "BUCKET", "value": bucket}
                        ],
                        "command": ["bash", "-c", f"""set -e
pip install -q git+{git_repo}
cng-datasets repartition --chunks-dir s3://{bucket}/chunks --output-dir s3://{bucket}/hex --cleanup
"""],
                        "resources": {
                            "requests": {"cpu": "4", "memory": "8Gi"},
                            "limits": {"cpu": "4", "memory": "8Gi"}
                        }
                    }]
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

# Run convert and pmtiles in parallel
echo "Step 1: Converting to GeoParquet and PMTiles (parallel)..."
kubectl apply -f /yamls/convert-job.yaml -n {namespace}
kubectl apply -f /yamls/pmtiles-job.yaml -n {namespace}

echo "Waiting for conversion jobs to complete..."
kubectl wait --for=condition=complete --timeout=3600s job/{dataset_name}-convert -n {namespace}
kubectl wait --for=condition=complete --timeout=3600s job/{dataset_name}-pmtiles -n {namespace}

echo "Step 2: H3 hexagonal tiling..."
kubectl apply -f /yamls/hex-job.yaml -n {namespace}

echo "Waiting for hex tiling to complete..."
kubectl wait --for=condition=complete --timeout=7200s job/{dataset_name}-hex -n {namespace}

echo "Step 3: Repartitioning by h0..."
kubectl apply -f /yamls/repartition-job.yaml -n {namespace}

echo "Waiting for repartition to complete..."
kubectl wait --for=condition=complete --timeout=3600s job/{dataset_name}-repartition -n {namespace}

echo "✓ Workflow complete!"
echo ""
echo "Clean up with:"
echo "  kubectl delete jobs {dataset_name}-convert {dataset_name}-pmtiles {dataset_name}-hex {dataset_name}-repartition {dataset_name}-workflow -n {namespace}"
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
