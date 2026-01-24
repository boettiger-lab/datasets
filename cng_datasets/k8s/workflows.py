"""
Workflow generation utilities.

Functions for creating complete dataset processing workflows with
multiple coordinated Kubernetes jobs.
"""

from typing import Optional, Dict, Any, List
from pathlib import Path
import math
import yaml
from .jobs import K8sJobManager


def _count_source_features(source_url: str) -> int:
    """
    Count features in a source file (shapefile, geopackage, etc.) using GDAL with vsicurl.
    For .zip files, it downloads, extracts, and counts features in all shapefiles.
    
    Args:
        source_url: HTTP(S) or S3 URL to source vector file
        
    Returns:
        Number of features in the source file(s)
    """
    import subprocess
    import tempfile
    import os
    import shutil
    import zipfile
    from urllib.request import urlretrieve
    
    # Check for zip file
    if source_url.lower().endswith('.zip'):
        try:
            print(f"  Detected zip file, downloading to temporary directory...")
            temp_dir = tempfile.mkdtemp()
            local_zip = os.path.join(temp_dir, "download.zip")
            
            # Handle S3 vs HTTP
            download_url = source_url
            if source_url.startswith('s3://'):
                 if "nrp-nautilus.io" in source_url or "public-iucn" in source_url:
                     path = source_url.replace("s3://", "")
                     download_url = f"https://s3-west.nrp-nautilus.io/{path}"
            
            # Strip vsicurl if present (legacy compat)
            if download_url.startswith('/vsicurl/'):
                download_url = download_url.replace('/vsicurl/', '')
            
            if download_url.startswith('http://') or download_url.startswith('https://') or download_url.startswith('ftp://'):
                urlretrieve(download_url, local_zip)
            elif os.path.exists(download_url):
                shutil.copy(download_url, local_zip)
            else:
                 # Try adding file:// scheme if it looks like an absolute path but failed above checks? 
                 # Or just let urlretrieve try if it has a scheme.
                 urlretrieve(download_url, local_zip)

            
            with zipfile.ZipFile(local_zip, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
                
            # Find all shapefiles
            total_count = 0
            shapefiles = []
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    if file.lower().endswith(".shp"):
                        shapefiles.append(os.path.join(root, file))
            
            if not shapefiles:
                raise ValueError("No shapefiles found in zip archive")
                
            print(f"  Found {len(shapefiles)} shapefiles, counting features...")
            
            for shp in shapefiles:
                result = subprocess.run(
                    ['ogrinfo', '-so', '-al', shp],
                    capture_output=True,
                    text=True,
                    check=True
                )
                
                for line in result.stdout.split('\n'):
                    if 'Feature Count:' in line:
                        count = int(line.split(':')[1].strip())
                        total_count += count
                        break
            
            return total_count

        finally:
            if 'temp_dir' in locals() and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

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
    layer: Optional[str] = None,
    hex_memory: str = "8Gi",
    max_parallelism: int = 50,
    max_completions: int = 200,
    intermediate_chunk_size: int = 10,
    row_group_size: int = 100000
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
    _generate_convert_job(manager, k8s_name, source_url, bucket, output_path, git_repo, layer, memory=hex_memory, row_group_size=row_group_size)
    
    # Generate pmtiles job
    _generate_pmtiles_job(manager, k8s_name, source_url, bucket, output_path, git_repo, memory=hex_memory)
    
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
    print(f"  - {k8s_name}-setup-bucket.yaml")
    print(f"  - {k8s_name}-convert.yaml")
    print(f"  - {k8s_name}-pmtiles.yaml")
    print(f"  - {k8s_name}-hex.yaml")
    print(f"  - {k8s_name}-repartition.yaml")
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


def generate_raster_workflow(
    dataset_name: str,
    source_url: str,
    bucket: str,
    output_dir: str = ".",
    namespace: str = "biodiversity",
    image: str = "ghcr.io/boettiger-lab/datasets:latest",
    git_repo: str = "https://github.com/boettiger-lab/datasets.git",
    h3_resolution: int = 8,
    parent_resolutions: Optional[List[int]] = None,
    value_column: str = "value",
    nodata_value: Optional[float] = None,
    hex_memory: str = "32Gi",
    max_parallelism: int = 61,
):
    """
    Generate complete workflow for a raster dataset.
    
    Creates all necessary Kubernetes job configurations for processing
    a raster dataset:
    1. Setup bucket
    2. H3 hexagonal tiling (partitioned by h0)
    
    Args:
        dataset_name: Name of the dataset
        source_url: URL to source COG file
        bucket: S3 bucket for outputs
        output_dir: Directory to write YAML files
        namespace: Kubernetes namespace
        image: Container image to use
        git_repo: Git repository URL for package source
        h3_resolution: Target H3 resolution for tiling (default: 8)
        parent_resolutions: List of parent H3 resolutions to include (default: [0])
        value_column: Name for raster value column (default: "value")
        nodata_value: NoData value to exclude (optional)
        hex_memory: Memory request/limit for hex job pods (default: "32Gi")
        max_parallelism: Maximum parallelism for hex jobs (default: 61)
    """
    manager = K8sJobManager(namespace=namespace, image=image)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Sanitize dataset name for Kubernetes
    k8s_name = dataset_name.replace('_', '-').lower()
    
    # Set defaults for parent resolutions if not provided
    if parent_resolutions is None:
        parent_resolutions = [0]
    
    # Generate bucket setup job (must run first)
    _generate_setup_bucket_job(manager, k8s_name, bucket, output_path, git_repo)
    
    # Generate raster hex tiling job
    _generate_raster_hex_job(
        manager, k8s_name, source_url, bucket, output_path, git_repo,
        h3_resolution, parent_resolutions, value_column, nodata_value,
        hex_memory, max_parallelism
    )
    
    # Generate workflow RBAC
    _generate_workflow_rbac(namespace, output_path)
    
    # Build generation command for documentation
    parent_res_str = ','.join(map(str, parent_resolutions))
    gen_command = (f"cng-datasets raster-workflow --dataset {dataset_name} "
                   f"--source-url {source_url} --bucket {bucket} "
                   f"--h3-resolution {h3_resolution} --parent-resolutions \"{parent_res_str}\"")
    
    # Generate ConfigMap YAML
    _generate_raster_configmap(k8s_name, namespace, output_path, gen_command)
    
    # Generate Argo workflow
    _generate_raster_argo_workflow(k8s_name, namespace, output_path, output_dir)
    
    print(f"\n✓ Generated raster workflow for {dataset_name}")
    print(f"\nFiles created in {output_dir}:")
    print(f"  - {k8s_name}-setup-bucket.yaml")
    print(f"  - {k8s_name}-hex.yaml")
    print(f"  - workflow-rbac.yaml")
    print(f"  - configmap.yaml")
    print(f"  - workflow.yaml")
    print(f"\nTo run:")
    print(f"  # One-time RBAC setup")
    print(f"  kubectl apply -f {output_dir}/workflow-rbac.yaml")
    print(f"")
    print(f"  # Apply all workflow files")
    print(f"  kubectl apply -f {output_dir}/configmap.yaml")
    print(f"  kubectl apply -f {output_dir}/workflow.yaml")


def _generate_raster_hex_job(
    manager, dataset_name, source_url, bucket, output_path, git_repo,
    h3_resolution, parent_resolutions, value_column, nodata_value,
    hex_memory, max_parallelism
):
    """Generate raster H3 hex tiling job."""
    parent_res_str = ','.join(map(str, parent_resolutions))
    
    # Build command
    cmd_parts = [
        "set -e",
        f"cng-datasets raster --input \"{source_url}\" --output-parquet s3://{bucket}/{dataset_name}/hex/ --h0-index ${{JOB_COMPLETION_INDEX}} --resolution {h3_resolution} --parent-resolutions {parent_res_str} --value-column {value_column}"
    ]
    
    if nodata_value is not None:
        cmd_parts[-1] += f" --nodata {nodata_value}"
        
    command_str = "\n".join(cmd_parts)
    
    job_spec = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": f"{dataset_name}-hex",
            "labels": {"k8s-app": f"{dataset_name}-hex"}
        },
        "spec": {
            "completions": 122,  # Always 122 h0 regions
            "parallelism": max_parallelism,
            "completionMode": "Indexed",
            "backoffLimit": 0,
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
                            {"name": "GDAL_DATA", "value": "/usr/share/gdal"},
                            {"name": "PROJ_LIB", "value": "/usr/local/share/proj"},
                            {"name": "PYTHONPATH", "value": "/usr/lib/python3/dist-packages"},
                            {"name": "BUCKET", "value": bucket}
                        ],
                        "command": ["bash", "-c", command_str],
                        "resources": {
                            "requests": {"cpu": "4", "memory": hex_memory, "ephemeral-storage": "250Gi"},
                            "limits": {"cpu": "4", "memory": hex_memory}
                        }
                    }]
                }
            }
        }
    }
    manager.save_job_yaml(job_spec, str(output_path / f"{dataset_name}-hex.yaml"))


def _generate_raster_configmap(dataset_name, namespace, output_path, gen_command):
    """Generate ConfigMap for raster workflow."""
    import yaml
    
    job_files = [f"{dataset_name}-setup-bucket.yaml", f"{dataset_name}-hex.yaml"]
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
        f.write("# Auto-generated ConfigMap for raster workflow\n")
        f.write(f"# Generation command: {gen_command}\n")
        yaml.dump(configmap, f, default_flow_style=False)


def _generate_raster_argo_workflow(dataset_name, namespace, output_path, output_dir):
    """Generate orchestrator job for raster workflow."""
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

echo "Starting {dataset_name} raster workflow..."

# Step 0: Setup bucket
echo "Step 0: Setting up bucket..."
kubectl apply -f /yamls/{dataset_name}-setup-bucket.yaml -n {namespace}

echo "Waiting for bucket setup..."
kubectl wait --for=condition=complete --timeout=600s job/{dataset_name}-setup-bucket -n {namespace}

# Step 1: Hex tiling
echo "Step 1: H3 hexagonal tiling..."
kubectl apply -f /yamls/{dataset_name}-hex.yaml -n {namespace}

echo "Waiting for hex tiling..."
kubectl wait --for=condition=complete --timeout=7200s job/{dataset_name}-hex -n {namespace}

echo "✓ Workflow complete!"
echo ""
echo "Clean up with:"
echo "  kubectl delete jobs {dataset_name}-setup-bucket {dataset_name}-hex {dataset_name}-workflow -n {namespace}"
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
    manager.save_job_yaml(job_spec, str(output_path / f"{dataset_name}-setup-bucket.yaml"))


def _generate_convert_job(manager, dataset_name, source_url, bucket, output_path, git_repo, layer=None, memory="8Gi", row_group_size=100000):
    """Generate GeoParquet conversion job."""
    # Build the conversion command with optional layer parameter
    layer_flag = f" \\\n  --layer {layer}" if layer else ""
    convert_cmd = f"""set -e
cng-convert-to-parquet \\
  {source_url} \\
  s3://{bucket}/{dataset_name}.parquet \\
  --row-group-size {row_group_size}{layer_flag}
"""
    
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
                        "command": ["bash", "-c", convert_cmd],
                        "resources": {
                            "requests": {"cpu": "4", "memory": memory},
                            "limits": {"cpu": "4", "memory": memory}
                        }
                    }],
                    "volumes": [
                        {"name": "rclone-config", "secret": {"secretName": "rclone-config"}}
                    ]
                }
            }
        }
    }
    manager.save_job_yaml(job_spec, str(output_path / f"{dataset_name}-convert.yaml"))


def _generate_pmtiles_job(manager, dataset_name, source_url, bucket, output_path, git_repo, memory="8Gi"):
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
                            "requests": {"cpu": "4", "memory": memory},
                            "limits": {"cpu": "4", "memory": memory}
                        }
                    }],
                    "volumes": [
                        {"name": "rclone-config", "secret": {"secretName": "rclone-config"}}
                    ]
                }
            }
        }
    }
    manager.save_job_yaml(job_spec, str(output_path / f"{dataset_name}-pmtiles.yaml"))


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
    manager.save_job_yaml(job_spec, str(output_path / f"{dataset_name}-hex.yaml"))


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
cng-datasets repartition --chunks-dir s3://{bucket}/{dataset_name}/chunks --output-dir s3://{bucket}/{dataset_name}/hex --source-parquet s3://{bucket}/{dataset_name}.parquet --cleanup
"""],
                        "resources": {
                            "requests": {"cpu": "4", "memory": "32Gi"},
                            "limits": {"cpu": "4", "memory": "32Gi"}
                        }
                    }],
                    "volumes": [
                        {"name": "rclone-config", "secret": {"secretName": "rclone-config"}}
                    ]
                }
            }
        }
    }
    manager.save_job_yaml(job_spec, str(output_path / f"{dataset_name}-repartition.yaml"))


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
    job_files = [f"{dataset_name}-setup-bucket.yaml", f"{dataset_name}-convert.yaml", f"{dataset_name}-pmtiles.yaml", f"{dataset_name}-hex.yaml", f"{dataset_name}-repartition.yaml"]
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
        f.write(f"# Generated from: {dataset_name}-setup-bucket.yaml, {dataset_name}-convert.yaml, {dataset_name}-pmtiles.yaml, {dataset_name}-hex.yaml, {dataset_name}-repartition.yaml\n")
        f.write(f"# Generation command: {gen_command}\n")
        f.write("#\n")
        f.write("# This ConfigMap is equivalent to running:\n")
        f.write(f"#   kubectl create configmap {dataset_name}-yamls \\\n")
        f.write(f"#     --from-file={dataset_name}-setup-bucket.yaml \\\n")
        f.write(f"#     --from-file={dataset_name}-convert.yaml \\\n")
        f.write(f"#     --from-file={dataset_name}-pmtiles.yaml \\\n")
        f.write(f"#     --from-file={dataset_name}-hex.yaml \\\n")
        f.write(f"#     --from-file={dataset_name}-repartition.yaml\n")
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
kubectl apply -f /yamls/{dataset_name}-setup-bucket.yaml -n {namespace}

echo "Waiting for bucket setup to complete..."
kubectl wait --for=condition=complete --timeout=600s job/{dataset_name}-setup-bucket -n {namespace}

# Step 1: Convert to optimized GeoParquet (needed by both pmtiles and hex jobs)
echo "Step 1: Converting to optimized GeoParquet..."
kubectl apply -f /yamls/{dataset_name}-convert.yaml -n {namespace}

echo "Waiting for GeoParquet conversion to complete..."
kubectl wait --for=condition=complete --timeout=3600s job/{dataset_name}-convert -n {namespace}

# Step 2: Run pmtiles and hex tiling in parallel (both use the converted geoparquet)
echo "Step 2: H3 hexagonal tiling and PMTiles generation (parallel)..."
kubectl apply -f /yamls/{dataset_name}-pmtiles.yaml -n {namespace}
kubectl apply -f /yamls/{dataset_name}-hex.yaml -n {namespace}

echo "Waiting for hex tiling to complete (PMTiles continues in background)..."
echo "Timeout set to 48 hours (172800s)..."
kubectl wait --for=condition=complete --timeout=172800s job/{dataset_name}-hex -n {namespace}

echo "Step 3: Repartitioning by h0..."
kubectl apply -f /yamls/{dataset_name}-repartition.yaml -n {namespace}

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


def generate_sync_job(
    job_name: str,
    source: str,
    destination: str,
    output_file: str = "sync-job.yaml",
    namespace: str = "biodiversity",
    image: str = "ghcr.io/boettiger-lab/datasets:latest",
    cpu: str = "2",
    memory: str = "4Gi",
    dry_run: bool = False,
) -> str:
    """
    Generate Kubernetes job for syncing between two S3 storage locations using rclone.
    
    Args:
        job_name: Name for the Kubernetes job
        source: Source path in rclone remote format (e.g., 'remote1:bucket/path')
        destination: Destination path in rclone remote format (e.g., 'remote2:bucket/path')
        output_file: Path to save the generated YAML file
        namespace: Kubernetes namespace (default: biodiversity)
        image: Container image with rclone (default: ghcr.io/boettiger-lab/datasets:latest)
        cpu: CPU request/limit (default: 2)
        memory: Memory request/limit (default: 4Gi)
        dry_run: If True, only show what would be synced without actually syncing
        
    Returns:
        Path to the generated YAML file
        
    Example:
        >>> generate_sync_job(
        ...     job_name="sync-data",
        ...     source="nrp:public-dataset/data",
        ...     destination="aws:backup-bucket/data",
        ...     output_file="k8s/sync-job.yaml"
        ... )
    """
    dry_run_flag = "--dry-run" if dry_run else ""
    
    job_spec = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "namespace": namespace,
            "labels": {"k8s-app": job_name}
        },
        "spec": {
            "completions": 1,
            "parallelism": 1,
            "backoffLimit": 2,
            "ttlSecondsAfterFinished": 10800,  # Clean up after 3 hours
            "template": {
                "metadata": {"labels": {"k8s-app": job_name}},
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
                        "name": "sync-task",
                        "image": image,
                        "imagePullPolicy": "Always",
                        "volumeMounts": [
                            {"name": "rclone-config", "mountPath": "/root/.config/rclone", "readOnly": True}
                        ],
                        "command": ["bash", "-c"],
                        "args": [f"""set -e
echo "Starting rclone sync..."
echo "Source: {source}"
echo "Destination: {destination}"
echo ""

# Run rclone sync
rclone sync {dry_run_flag} -v --progress \\
    "{source}" \\
    "{destination}"

echo ""
echo "Sync complete!"

# Show summary
echo ""
echo "Summary:"
rclone size "{destination}"
"""],
                        "resources": {
                            "requests": {"cpu": cpu, "memory": memory},
                            "limits": {"cpu": cpu, "memory": memory}
                        }
                    }],
                    "volumes": [
                        {"name": "rclone-config", "secret": {"secretName": "rclone-config"}}
                    ]
                }
            }
        }
    }
    
    # Save to file with custom representer for multiline strings
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Custom representer for multi-line strings to use literal style (|)
    def str_representer(dumper, data):
        if '\n' in data:
            return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
        return dumper.represent_scalar('tag:yaml.org,2002:str', data)
    
    yaml.add_representer(str, str_representer)
    
    with open(output_path, "w") as f:
        yaml.dump(job_spec, f, default_flow_style=False, sort_keys=False)
    
    print(f"✓ Generated sync job: {output_file}")
    print(f"")
    print(f"Submit with:")
    print(f"  kubectl apply -f {output_file} -n {namespace}")
    print(f"")
    print(f"Monitor progress:")
    print(f"  kubectl logs -f job/{job_name} -n {namespace}")
    print(f"")
    print(f"Clean up:")
    print(f"  kubectl delete -f {output_file} -n {namespace}")
    
    return str(output_path)

