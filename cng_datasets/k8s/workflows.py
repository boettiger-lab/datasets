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
    image: str = "ghcr.io/boettiger-lab/cng-datasets:latest",
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
    _generate_convert_job(manager, dataset_name, source_url, bucket, output_path)
    
    # Generate pmtiles job
    _generate_pmtiles_job(manager, dataset_name, source_url, bucket, output_path)
    
    # Generate hex tiling job
    _generate_hex_job(manager, dataset_name, bucket, output_path)
    
    # Generate repartition job
    _generate_repartition_job(manager, dataset_name, bucket, output_path)
    
    # Generate workflow RBAC
    _generate_workflow_rbac(dataset_name, namespace, output_path)
    
    # Generate Argo workflow
    _generate_argo_workflow(manager, dataset_name, namespace, output_path)
    
    print(f"\n✓ Generated complete workflow for {dataset_name}")
    print(f"\nFiles created in {output_dir}:")
    print(f"  - convert-job.yaml")
    print(f"  - pmtiles-job.yaml")
    print(f"  - hex-job.yaml")
    print(f"  - repartition-job.yaml")
    print(f"  - workflow-rbac.yaml")
    print(f"  - run-workflow.sh")
    print(f"\nTo run:")
    print(f"  kubectl apply -f {output_dir}/workflow-rbac.yaml")
    print(f"  cd {output_dir} && ./run-workflow.sh")


def _generate_convert_job(manager, dataset_name, source_url, bucket, output_path):
    """Generate GeoParquet conversion job."""
    job_spec = manager.generate_job_yaml(
        job_name=f"{dataset_name}-convert",
        command=["python", "-c"],
        args=[f"""
import geopandas as gpd
import urllib.request

print('Downloading source data...')
urllib.request.urlretrieve('{source_url}', '/tmp/input.gpkg')

print('Reading and cleaning data...')
gdf = gpd.read_file('/tmp/input.gpkg')

# Basic cleaning
for col in gdf.columns:
    if col.strip().lower() == 'grade' and col != 'grade':
        gdf = gdf.rename(columns={{col: 'grade'}})
        gdf['grade'] = gdf['grade'].astype(str).str.strip().replace(['', 'nan'], None)

if gdf.geometry.isna().any():
    gdf = gdf[~gdf.geometry.isna()]
if not gdf.geometry.is_valid.all():
    gdf.geometry = gdf.geometry.buffer(0)

print('Writing to S3...')
gdf.to_parquet('s3://{bucket}/{dataset_name}.parquet')
print('✓ Conversion complete!')
"""],
        cpu="2",
        memory="8Gi",
    )
    _add_common_config(job_spec)
    manager.save_job_yaml(job_spec, str(output_path / "convert-job.yaml"))


def _generate_pmtiles_job(manager, dataset_name, source_url, bucket, output_path):
    """Generate PMTiles job."""
    job_spec = manager.generate_job_yaml(
        job_name=f"{dataset_name}-pmtiles",
        command=["bash", "-c"],
        args=[f"""
set -e
pip install geopandas pyarrow
curl -L -o /tmp/input.gpkg {source_url}
ogr2ogr -f GeoJSONSeq /tmp/input.geojsonl /tmp/input.gpkg -progress
tippecanoe -o /tmp/output.pmtiles -l {dataset_name} --drop-densest-as-needed --extend-zooms-if-still-dropping --force /tmp/input.geojsonl
mc alias set s3 https://${{AWS_PUBLIC_ENDPOINT}} ${{AWS_ACCESS_KEY_ID}} ${{AWS_SECRET_ACCESS_KEY}}
mc cp /tmp/output.pmtiles s3/{bucket}/{dataset_name}.pmtiles
echo "✓ PMTiles complete!"
"""],
        cpu="2",
        memory="8Gi",
    )
    _add_common_config(job_spec)
    manager.save_job_yaml(job_spec, str(output_path / "pmtiles-job.yaml"))


def _generate_hex_job(manager, dataset_name, bucket, output_path):
    """Generate H3 hex tiling job."""
    job_spec = manager.generate_chunked_job(
        job_name=f"{dataset_name}-hex",
        script_path="/app/process-hex.py",
        num_chunks=50,
        base_args=[
            "--input", f"s3://{bucket}/{dataset_name}.parquet",
            "--output", f"s3://{bucket}/chunks",
            "--resolution", "10",
            "--parent-resolutions", "9", "8", "0",
            "--chunk-size", "500",
        ],
        cpu="1",
        memory="4Gi",
        parallelism=20,
    )
    _add_common_config(job_spec)
    manager.save_job_yaml(job_spec, str(output_path / "hex-job.yaml"))


def _generate_repartition_job(manager, dataset_name, bucket, output_path):
    """Generate repartition job."""
    job_spec = manager.generate_job_yaml(
        job_name=f"{dataset_name}-repartition",
        command=["python", "-c"],
        args=[f"""
import duckdb
from cng.utils import set_secrets
import shutil

con = duckdb.connect()
set_secrets(con)
con.execute("SET preserve_insertion_order=false")
con.execute("SET http_timeout=1200")
con.execute("SET http_retries=30")

print('Repartitioning by h0...')
con.execute(\"\"\"
    COPY (SELECT * FROM read_parquet('s3://{bucket}/chunks/*.parquet'))
    TO '/tmp/hex' (FORMAT PARQUET, PARTITION_BY h0)
\"\"\")

con.execute(\"\"\"
    COPY (SELECT * FROM read_parquet('/tmp/hex/**/*.parquet'))
    TO 's3://{bucket}/hex/' (FORMAT PARQUET, PARTITION_BY h0)
\"\"\")

shutil.rmtree('/tmp/hex')
print('✓ Repartitioning complete!')
"""],
        cpu="2",
        memory="8Gi",
    )
    _add_common_config(job_spec)
    manager.save_job_yaml(job_spec, str(output_path / "repartition-job.yaml"))


def _add_common_config(job_spec):
    """Add common configuration to all jobs."""
    container = job_spec["spec"]["template"]["spec"]["containers"][0]
    container["env"] = [
        {"name": "AWS_ACCESS_KEY_ID", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_ACCESS_KEY_ID"}}},
        {"name": "AWS_SECRET_ACCESS_KEY", "valueFrom": {"secretKeyRef": {"name": "aws", "key": "AWS_SECRET_ACCESS_KEY"}}},
        {"name": "AWS_S3_ENDPOINT", "value": "rook-ceph-rgw-nautiluss3.rook"},
        {"name": "AWS_PUBLIC_ENDPOINT", "value": "s3-west.nrp-nautilus.io"},
    ]
    
    job_spec["spec"]["template"]["spec"]["affinity"] = {
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
    
    job_spec["spec"]["template"]["spec"]["priorityClassName"] = "opportunistic"
    job_spec["spec"]["backoffLimitPerIndex"] = 3
    job_spec["spec"]["ttlSecondsAfterFinished"] = 10800


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
                    {"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["get", "list", "create", "delete"]},
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


def _generate_argo_workflow(manager, dataset_name, namespace, output_path):
    """Generate simple workflow script."""
    import yaml
    
    # Instead of a complex orchestrator, just create a simple shell script
    workflow_script = f"""#!/bin/bash
# Workflow for {dataset_name}
set -e

echo "Starting {dataset_name} workflow..."
echo "Make sure you've already applied workflow-rbac.yaml!"
echo ""

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
"""
    
    with open(output_path / "run-workflow.sh", "w") as f:
        f.write(workflow_script)
    
    # Make it executable
    import os
    os.chmod(output_path / "run-workflow.sh", 0o755)
