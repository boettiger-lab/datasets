"""
Workflow generation utilities.

Functions for creating complete dataset processing workflows with
multiple coordinated Kubernetes jobs.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Union
from pathlib import Path
import math
import re
import yaml
from .jobs import K8sJobManager
from .armada import convert_workflow_to_armada

# Keys that ClusterConfig accepts (used for profile validation)
_CONFIG_KEYS = {
    "s3_endpoint", "s3_public_endpoint", "s3_secret_name",
    "rclone_secret_name", "rclone_remote", "priority_class", "node_affinity",
}


@dataclass
class ClusterConfig:
    """Configuration for a target Kubernetes cluster and S3 backend.

    All values default to the NRP Nautilus cluster, preserving backwards
    compatibility. Override individual fields to target a different cluster.

    Can be constructed from a profile file via :func:`load_profile`.
    """
    # S3 endpoints
    s3_endpoint: str = "rook-ceph-rgw-nautiluss3.rook"
    s3_public_endpoint: str = "s3-west.nrp-nautilus.io"
    # K8s secret names
    s3_secret_name: str = "aws"
    rclone_secret_name: str = "rclone-config"
    # Rclone remote name used in setup-bucket and pmtiles jobs
    rclone_remote: str = "nrp"
    # Priority class (empty string = omit from spec)
    priority_class: str = "opportunistic"
    # Node affinity: "gpu-avoid" (default NRP rule) or "none" (disable)
    node_affinity: str = "gpu-avoid"


# Built-in profiles directory (ships with the package)
_BUILTIN_PROFILES_DIR = Path(__file__).parent / "profiles"

# User profile directory
_USER_PROFILES_DIR = Path.home() / ".config" / "cng-datasets" / "profiles"


def load_profile(name_or_path: str) -> Dict[str, Any]:
    """Load a cluster profile and return its values as a dict.

    Resolution order:
    1. Treat as a file path if it ends in ``.yaml`` / ``.yml`` or contains
       a path separator — resolved relative to the current working directory.
    2. ``~/.config/cng-datasets/profiles/<name>.yaml``
    3. Built-in profiles shipped with the package (currently: ``nrp``).

    Args:
        name_or_path: Built-in profile name (e.g. ``"nrp"``) or path to a
            YAML file (e.g. ``"./my-cluster.yaml"``).

    Returns:
        Dict of config values from the profile.  Unknown keys are ignored.

    Raises:
        FileNotFoundError: If no profile matching the name or path is found.
        ValueError: If the profile YAML is invalid.
    """
    p = Path(name_or_path)

    # Explicit path: ends with .yaml/.yml or contains a separator
    candidates: List[Path] = []
    if name_or_path.endswith((".yaml", ".yml")) or "/" in name_or_path or "\\" in name_or_path:
        candidates.append(p if p.is_absolute() else Path.cwd() / p)
    else:
        # Named profile: user dir first, then built-in
        candidates.append(_USER_PROFILES_DIR / f"{name_or_path}.yaml")
        candidates.append(_BUILTIN_PROFILES_DIR / f"{name_or_path}.yaml")

    for candidate in candidates:
        if candidate.exists():
            with open(candidate) as f:
                data = yaml.safe_load(f) or {}
            # Strip the display-only 'name' key, keep only ClusterConfig fields
            return {k: v for k, v in data.items() if k in _CONFIG_KEYS}

    raise FileNotFoundError(
        f"Cluster profile {name_or_path!r} not found.\n"
        f"Searched:\n" + "\n".join(f"  {c}" for c in candidates) + "\n"
        f"Built-in profiles: {[p.stem for p in _BUILTIN_PROFILES_DIR.glob('*.yaml')]}\n"
        f"To create a custom profile, write a YAML file to "
        f"{_USER_PROFILES_DIR}/<name>.yaml"
    )


def cluster_config_from_args(
    profile: Optional[str] = None,
    **cli_overrides,
) -> "ClusterConfig":
    """Build a ClusterConfig by merging a profile with explicit CLI overrides.

    Precedence (highest to lowest):
    1. Values in ``cli_overrides`` that are not ``None``
    2. Values from the profile file (if ``profile`` is given)
    3. ``ClusterConfig`` field defaults (NRP Nautilus values)

    Args:
        profile: Profile name or path, or ``None`` to use bare defaults.
        **cli_overrides: Keyword arguments matching ``ClusterConfig`` field
            names.  Pass ``None`` for any flag the user did not explicitly set.

    Returns:
        A fully-populated :class:`ClusterConfig`.
    """
    base: Dict[str, Any] = {}
    if profile is not None:
        base = load_profile(profile)

    # Apply non-None CLI overrides on top of profile values
    for key, value in cli_overrides.items():
        if key in _CONFIG_KEYS and value is not None:
            base[key] = value

    return ClusterConfig(**base)


def _s3_env_vars(config: ClusterConfig) -> List[Dict[str, Any]]:
    """Build S3 environment variables for job specs from cluster config."""
    return [
        {"name": "AWS_ACCESS_KEY_ID", "valueFrom": {"secretKeyRef": {"name": config.s3_secret_name, "key": "AWS_ACCESS_KEY_ID"}}},
        {"name": "AWS_SECRET_ACCESS_KEY", "valueFrom": {"secretKeyRef": {"name": config.s3_secret_name, "key": "AWS_SECRET_ACCESS_KEY"}}},
        {"name": "AWS_S3_ENDPOINT", "value": config.s3_endpoint},
        {"name": "AWS_PUBLIC_ENDPOINT", "value": config.s3_public_endpoint},
        {"name": "AWS_HTTPS", "value": "false"},
        {"name": "AWS_VIRTUAL_HOSTING", "value": "FALSE"},
    ]


def _node_affinity_spec(config: ClusterConfig) -> Optional[Dict[str, Any]]:
    """Return the affinity dict for pod specs, or None to omit affinity."""
    if config.node_affinity == "none":
        return None
    # Default: avoid NVIDIA GPU nodes (NRP NFD label)
    return {
        "nodeAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": {
                "nodeSelectorTerms": [{
                    "matchExpressions": [{
                        "key": "feature.node.kubernetes.io/pci-10de.present",
                        "operator": "NotIn",
                        "values": ["true"],
                    }]
                }]
            }
        }
    }


def _apply_scheduling(pod_spec: Dict[str, Any], config: ClusterConfig) -> None:
    """Mutate pod_spec in place: add priorityClassName and affinity from config."""
    if config.priority_class:
        pod_spec["priorityClassName"] = config.priority_class
    affinity = _node_affinity_spec(config)
    if affinity is not None:
        pod_spec["affinity"] = affinity


def _validate_k8s_name(k8s_name: str, original: str) -> None:
    """Raise ValueError if k8s_name is not a valid Kubernetes resource name."""
    pattern = re.compile(r'^[a-z0-9][a-z0-9\-]*[a-z0-9]$')
    if not pattern.match(k8s_name):
        raise ValueError(
            f"Dataset name {original!r} produces invalid Kubernetes name {k8s_name!r}.\n"
            "Kubernetes names must consist of lowercase alphanumeric characters and '-', "
            "and must start and end with an alphanumeric character.\n"
            "Common causes: dots ('.'), underscores left after substitution, or starting/ending with '-'.\n"
            f"Suggested fix: rename using only lowercase letters, digits, hyphens, and '/' as path separator."
        )


def _count_source_features(source_urls: Union[str, List[str]], layer: str = None) -> int:
    """
    Count features in source file(s) (shapefile, geopackage, etc.) using GDAL with vsicurl.
    For .zip files, it downloads, extracts, and counts features in all shapefiles.
    
    Args:
        source_urls: HTTP(S) or S3 URL(s) to source vector file(s). Can be a single string or list.
        layer: Optional layer name for multi-layer sources (GDB, GPKG)
        
    Returns:
        Total number of features across all source file(s)
    """
    # Normalize to list
    if isinstance(source_urls, str):
        source_urls = [source_urls]
    
    total_count = 0
    for source_url in source_urls:
        total_count += _count_single_source(source_url, layer)
    
    return total_count


def _count_single_source(source_url: str, layer: str = None) -> int:
    """
    Count features in a single source file.
    
    Args:
        source_url: HTTP(S) or S3 URL to source vector file
        layer: Optional layer name for multi-layer sources (GDB, GPKG)
        
    Returns:
        Number of features in the source file
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
    if layer:
        # Query specific layer for multi-layer sources (GDB, GPKG)
        result = subprocess.run(
            ['ogrinfo', '-so', gdal_path, layer],
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )
    else:
        # -al: all layers (for single-layer sources)
        result = subprocess.run(
            ['ogrinfo', '-so', '-al', gdal_path],
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )
    
    # Parse output to find "Feature Count: XXXXX"
    for line in result.stdout.split('\n'):
        if 'Feature Count:' in line:
            count_str = line.split(':')[1].strip()
            return int(count_str)
    
    raise ValueError("Could not find feature count in ogrinfo output")


def _detect_geometry_type(source_urls: Union[str, List[str]], layer: str = None) -> str:
    """Detect the predominant geometry type from source file(s).

    Returns one of 'point', 'line', 'polygon', or 'unknown'.  Uses the
    first source URL only (multi-source datasets are expected to share
    geometry type).
    """
    import subprocess

    url = source_urls[0] if isinstance(source_urls, list) else source_urls

    # Convert s3:// to https:// for vsicurl
    if url.startswith('s3://'):
        path = url.replace('s3://', '')
        url = f"https://s3-west.nrp-nautilus.io/{path}"

    if url.startswith('http://') or url.startswith('https://'):
        gdal_path = f"/vsicurl/{url}"
    else:
        gdal_path = url

    try:
        cmd = ['ogrinfo', '-so', gdal_path]
        if layer:
            cmd.append(layer)
        else:
            cmd.append('-al')
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        output = result.stdout.lower()

        if 'line string' in output or 'linestring' in output or 'multi line string' in output:
            return 'line'
        if 'point' in output or 'multi point' in output:
            return 'point'
        if 'polygon' in output or 'multi polygon' in output:
            return 'polygon'
    except Exception:
        pass

    return 'unknown'


# Default H3 resolutions per geometry type
_DEFAULT_H3_RESOLUTION = {
    'point': 10,
    'polygon': 10,
    'line': 8,
    'unknown': 10,
}


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
    source_urls: Union[str, List[str]] = None,
    bucket: str = None,
    output_dir: str = ".",
    namespace: str = "biodiversity",
    image: str = "ghcr.io/boettiger-lab/datasets:latest",
    git_repo: str = "https://github.com/boettiger-lab/datasets.git",
    h3_resolution: Optional[int] = None,
    parent_resolutions: Optional[List[int]] = None,
    id_column: Optional[str] = None,
    layer: Optional[str] = None,
    hex_memory: str = "8Gi",
    max_parallelism: int = 50,
    max_completions: int = 200,
    intermediate_chunk_size: int = 10,
    row_group_size: int = 100000,
    backend: str = "k8s",
    hex_storage: str = "10Gi",
    repartition_storage: str = "50Gi",
    repartition_memory: str = "32Gi",
    # Cluster/storage configuration — all default to None so explicit values
    # can be distinguished from "not set" when merging with a profile.
    profile: Optional[str] = None,
    s3_endpoint: Optional[str] = None,
    s3_public_endpoint: Optional[str] = None,
    s3_secret_name: Optional[str] = None,
    rclone_secret_name: Optional[str] = None,
    rclone_remote: Optional[str] = None,
    priority_class: Optional[str] = None,
    node_affinity: Optional[str] = None,
    # Backwards compatibility: accept source_url (singular)
    source_url: Union[str, List[str]] = None,
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
        source_urls: URL(s) to source data file(s). Can be a single string or list. Multiple sources will be merged.
        bucket: S3 bucket for outputs
        output_dir: Directory to write YAML files
        namespace: Kubernetes namespace
        image: Container image to use
        git_repo: Git repository URL for package source
        h3_resolution: Target H3 resolution for tiling. Auto-detected if None
            (10 for polygons/points, 8 for lines).
        parent_resolutions: List of parent H3 resolutions to include (default: [9, 8, 0])
        id_column: ID column name (auto-detected if not specified)
        hex_memory: Memory request/limit for hex job pods (default: "8Gi")
        max_parallelism: Maximum parallelism for hex jobs (default: 50)
        max_completions: Maximum job completions - increase to reduce chunk size (default: 200)
        intermediate_chunk_size: Number of rows to process in pass 2 (unnesting arrays) - reduce if hitting OOM (default: 10)
        source_url: (Deprecated) Use source_urls instead. Kept for backwards compatibility.
    """
    # Handle backwards compatibility for source_url parameter
    if source_url is not None and source_urls is None:
        source_urls = source_url
    elif source_urls is None:
        raise ValueError("Either source_urls or source_url must be provided")

    if bucket is None:
        raise ValueError("bucket parameter is required")

    config = cluster_config_from_args(
        profile=profile,
        s3_endpoint=s3_endpoint,
        s3_public_endpoint=s3_public_endpoint,
        s3_secret_name=s3_secret_name,
        rclone_secret_name=rclone_secret_name,
        rclone_remote=rclone_remote,
        priority_class=priority_class,
        node_affinity=node_affinity,
    )

    manager = K8sJobManager(namespace=namespace, image=image)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Sanitize dataset name for Kubernetes (replace underscores, slashes with hyphens)
    k8s_name = dataset_name.replace('_', '-').replace('/', '-').lower()
    _validate_k8s_name(k8s_name, dataset_name)

    # Normalize source_urls to list
    if isinstance(source_urls, str):
        source_urls = [source_urls]

    # Auto-detect geometry type and set H3 resolution default if not specified
    if h3_resolution is None:
        geom_type = _detect_geometry_type(source_urls, layer=layer)
        h3_resolution = _DEFAULT_H3_RESOLUTION.get(geom_type, 10)
        print(f"  Detected geometry type: {geom_type} — using H3 resolution {h3_resolution}")
        if geom_type == 'line':
            print(f"  Line geometries will be buffered before H3 polyfill for continuous cell coverage.")

    # Set defaults for parent resolutions if not provided
    if parent_resolutions is None:
        parent_resolutions = [9, 8, 0]

    # Generate bucket setup job (must run first)
    _generate_setup_bucket_job(manager, k8s_name, bucket, output_path, git_repo, config)

    # Generate conversion job
    _generate_convert_job(manager, k8s_name, source_urls, bucket, output_path, git_repo, layer, memory=hex_memory, row_group_size=row_group_size, s3_dataset=dataset_name, config=config)

    # Generate pmtiles job (uses converted parquet, not source)
    _generate_pmtiles_job(manager, k8s_name, None, bucket, output_path, git_repo, memory=hex_memory, s3_dataset=dataset_name, config=config)
    
    # Count features in source file(s) and calculate chunking parameters
    if len(source_urls) > 1:
        print(f"Counting features in {len(source_urls)} sources...")
    else:
        print(f"Counting features in {source_urls[0]}...")
    try:
        total_rows = _count_source_features(source_urls, layer=layer)
        chunk_size, completions, parallelism = _calculate_chunking(total_rows, max_completions=max_completions, max_parallelism=max_parallelism)
        print(f"  Total features: {total_rows:,}")
        print(f"  Chunk size: {chunk_size:,}")
        print(f"  Completions: {completions}")
        print(f"  Parallelism: {parallelism}")
    except Exception as e:
        # Fall back to default values if counting fails (e.g., in tests or if file doesn't exist yet)
        print(f"  Warning: Could not count features ({e}). Using conservative chunking parameters.")
        total_rows = max_completions * 1000  # Conservative: covers up to max_completions*1000 features
        chunk_size, completions, parallelism = _calculate_chunking(total_rows, max_completions=max_completions, max_parallelism=max_parallelism)
        print(f"  Warning: feature count unknown — defaults cover at most {total_rows:,} features (chunk_size={chunk_size}).")
        print(f"  If your dataset is larger, set --chunk-size manually.")
        print(f"  Using defaults: chunk_size={chunk_size}, completions={completions}, parallelism={parallelism}")
    
    print(f"  H3 resolution: {h3_resolution}")
    print(f"  Parent resolutions: {parent_resolutions}")
    
    # Generate hex tiling job
    _generate_hex_job(manager, k8s_name, bucket, output_path, git_repo, chunk_size, completions, parallelism, h3_resolution, parent_resolutions, id_column, hex_memory, intermediate_chunk_size, s3_dataset=dataset_name, hex_storage=hex_storage, config=config)

    # Generate repartition job
    _generate_repartition_job(manager, k8s_name, bucket, output_path, git_repo, s3_dataset=dataset_name, repartition_storage=repartition_storage, repartition_memory=repartition_memory, config=config)
    
    # Generate workflow RBAC (generic for all cng-datasets workflows)
    _generate_workflow_rbac(namespace, output_path)
    
    # Build generation command for documentation
    parent_res_str = ','.join(map(str, parent_resolutions))
    # Format source URLs for command recreation
    source_urls_str = ' '.join([f'--source-url {url}' for url in source_urls])
    gen_command = (f"cng-datasets workflow --dataset {dataset_name} "
                   f"{source_urls_str} --bucket {bucket} "
                   f"--h3-resolution {h3_resolution} --parent-resolutions \"{parent_res_str}\"")
    
    # Generate ConfigMap YAML from job files
    _generate_configmap(k8s_name, namespace, output_path, gen_command)
    
    # Generate Argo workflow with ConfigMap-based approach
    _generate_argo_workflow(k8s_name, namespace, output_path, output_dir)
    
    if backend == "armada":
        armada_files = convert_workflow_to_armada(
            k8s_yaml_dir=str(output_path),
            dataset_name=k8s_name,
            queue=namespace,
        )
        print(f"\n✓ Generated Armada workflow for {dataset_name}")
        print(f"\nArmada files created in {output_dir}:")
        for f in armada_files:
            print(f"  - {Path(f).name}")
        print(f"\nTo run (submit each step in order):")
        steps = ["setup-bucket", "convert", "pmtiles", "hex", "repartition"]
        for step in steps:
            armada_file = output_path / f"armada-{k8s_name}-{step}.yaml"
            if armada_file.exists():
                print(f"  armadactl submit {output_dir}/armada-{k8s_name}-{step}.yaml")
        print(f"\nMonitor at: https://armada-lookout.nrp-nautilus.io")
    else:
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
    source_urls: Union[str, List[str]],
    bucket: str,
    output_dir: str = ".",
    namespace: str = "biodiversity",
    image: str = "ghcr.io/boettiger-lab/datasets:latest",
    git_repo: str = "https://github.com/boettiger-lab/datasets.git",
    h3_resolution: int = 8,
    parent_resolutions: Optional[List[int]] = None,
    value_column: str = "value",
    nodata_value: Optional[float] = None,
    hex_resampling: str = "average",
    hex_memory: str = "32Gi",
    max_parallelism: int = 61,
    hex_storage: str = "20Gi",
    cog_storage: str = "50Gi",
    target_extent: Optional[tuple] = None,
    target_resolution: Optional[float] = None,
    band: Optional[int] = None,
    output_cog_name: Optional[str] = None,
    backend: str = "k8s",
    # Cluster/storage configuration — all default to None so explicit values
    # can be distinguished from "not set" when merging with a profile.
    profile: Optional[str] = None,
    s3_endpoint: Optional[str] = None,
    s3_public_endpoint: Optional[str] = None,
    s3_secret_name: Optional[str] = None,
    rclone_secret_name: Optional[str] = None,
    rclone_remote: Optional[str] = None,
    priority_class: Optional[str] = None,
    node_affinity: Optional[str] = None,
    # backwards compat
    source_url: Optional[str] = None,
):
    """
    Generate complete workflow for a raster dataset.

    Creates all necessary Kubernetes job configurations:
    - Single COG source URL: setup-bucket → hex
    - Single non-COG source URL: setup-bucket → preprocess-cog → hex  (auto-detected)
    - Multiple source URLs (or target_extent/band): setup-bucket → preprocess-cog → hex

    The preprocess-cog step mosaics/converts tiles (handling mixed CRS such as multiple
    UTM zones), optionally clips to target_extent, and writes a single WGS84 COG to S3
    that the hex job then reads.  Non-COG sources are automatically detected and converted
    because GDAL must scan the entire file per pod without COG block/overview structure,
    causing catastrophic slowdowns on large rasters.

    Args:
        dataset_name: Name of the dataset (e.g., "wyoming/rap-arte")
        source_urls: URL(s) to source raster tile(s). Repeat for multi-tile mosaics.
        bucket: S3 bucket for outputs
        output_dir: Directory to write YAML files
        namespace: Kubernetes namespace
        image: Container image to use
        h3_resolution: Target H3 resolution for tiling (default: 8)
        parent_resolutions: List of parent H3 resolutions (default: [0])
        value_column: Name for raster value column (default: "value")
        nodata_value: NoData value to exclude (optional)
        hex_resampling: Resampling method for H3 hex downsampling (default: "average").
            Use "mode" for categorical rasters (land cover, classifications) to
            preserve class codes — averaging yields meaningless non-canonical values.
        hex_memory: Memory per hex pod (default: "32Gi")
        max_parallelism: Max parallel hex pods (default: 61)
        target_extent: Clip bbox (xmin, ymin, xmax, ymax) in EPSG:4326 for mosaic step
        target_resolution: Output pixel size in degrees for mosaic step
        band: Extract single band from multi-band sources (1-indexed) for mosaic step
        output_cog_name: S3 key for intermediate COG (default: "{k8s_name}-cog.tif")
        source_url: Deprecated alias for source_urls (single URL).
    """
    # backwards compat
    if source_url is not None and source_urls is None:
        source_urls = source_url

    if isinstance(source_urls, str):
        source_urls = [source_urls]

    config = cluster_config_from_args(
        profile=profile,
        s3_endpoint=s3_endpoint,
        s3_public_endpoint=s3_public_endpoint,
        s3_secret_name=s3_secret_name,
        rclone_secret_name=rclone_secret_name,
        rclone_remote=rclone_remote,
        priority_class=priority_class,
        node_affinity=node_affinity,
    )

    manager = K8sJobManager(namespace=namespace, image=image)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    k8s_name = dataset_name.replace('_', '-').replace('/', '-').lower()
    _validate_k8s_name(k8s_name, dataset_name)

    if parent_resolutions is None:
        parent_resolutions = [0]

    # Decide whether a preprocess-cog step is needed
    needs_preprocess = len(source_urls) > 1 or target_extent is not None or band is not None

    # For single-source case, check if the source is already a COG.  Non-COG rasters
    # force GDAL to scan the entire file for every hex pod, causing catastrophic slowdowns
    # (e.g. GHS-POP 10.9 GB stalled 32+ hours per pod vs 25 min after COG conversion).
    if not needs_preprocess:
        from cng_datasets.raster.cog import is_cog
        if not is_cog(source_urls[0]):
            print(f"  ⚠ Source raster is not a COG — adding preprocess-cog step to convert before hex tiling")
            needs_preprocess = True

    if needs_preprocess:
        cog_key = output_cog_name or f"{k8s_name}-cog.tif"
        cog_s3_url = f"s3://{bucket}/{cog_key}"
        hex_input_url = cog_s3_url
    else:
        cog_key = None
        hex_input_url = source_urls[0]

    # Generate bucket setup job
    _generate_setup_bucket_job(manager, k8s_name, bucket, output_path, git_repo, config)

    # Generate preprocess-cog job when needed
    if needs_preprocess:
        _generate_cog_preprocess_job(
            manager, k8s_name, source_urls, cog_s3_url, output_path,
            target_extent=target_extent,
            target_resolution=target_resolution,
            band=band,
            nodata_value=nodata_value,
            cog_storage=cog_storage,
            config=config,
        )

    # Generate raster hex tiling job
    _generate_raster_hex_job(
        manager, k8s_name, hex_input_url, bucket, output_path, git_repo,
        h3_resolution, parent_resolutions, value_column, nodata_value,
        hex_memory, max_parallelism, hex_storage=hex_storage,
        hex_resampling=hex_resampling, config=config,
    )

    # Generate workflow RBAC
    _generate_workflow_rbac(namespace, output_path)

    # Build generation command for documentation
    parent_res_str = ','.join(map(str, parent_resolutions))
    source_flags = ' '.join(f'--source-url "{u}"' for u in source_urls)
    gen_command = (f"cng-datasets raster-workflow --dataset {dataset_name} "
                   f"{source_flags} --bucket {bucket} "
                   f"--h3-resolution {h3_resolution} --parent-resolutions \"{parent_res_str}\"")

    # Generate ConfigMap and orchestrator workflow
    _generate_raster_configmap(k8s_name, namespace, output_path, gen_command, needs_preprocess)
    _generate_raster_argo_workflow(k8s_name, namespace, output_path, output_dir, needs_preprocess)

    if backend == "armada":
        armada_files = convert_workflow_to_armada(
            k8s_yaml_dir=str(output_path),
            dataset_name=k8s_name,
            queue=namespace,
        )
        print(f"\n✓ Generated Armada raster workflow for {dataset_name}")
        print(f"\nArmada files created in {output_dir}:")
        for f in armada_files:
            print(f"  - {Path(f).name}")
        print(f"\nTo run (submit each step in order):")
        steps = ["setup-bucket"]
        if needs_preprocess:
            steps.append("preprocess-cog")
        steps.append("hex")
        for step in steps:
            print(f"  armadactl submit {output_dir}/armada-{k8s_name}-{step}.yaml")
        print(f"\nMonitor at: https://armada-lookout.nrp-nautilus.io")
    else:
        print(f"\n✓ Generated raster workflow for {dataset_name}")
        print(f"\nFiles created in {output_dir}:")
        print(f"  - {k8s_name}-setup-bucket.yaml")
        if needs_preprocess:
            print(f"  - {k8s_name}-preprocess-cog.yaml  (mosaic {len(source_urls)} tiles → {cog_key})")
        print(f"  - {k8s_name}-hex.yaml")
        print(f"  - workflow-rbac.yaml")
        print(f"  - configmap.yaml")
        print(f"  - workflow.yaml")
        print(f"\nTo run:")
        print(f"  kubectl apply -f {output_dir}/workflow-rbac.yaml  # one-time")
        print(f"  kubectl apply -f {output_dir}/configmap.yaml -f {output_dir}/workflow.yaml")


def _generate_cog_preprocess_job(
    manager, dataset_name, source_urls, output_cog_url, output_path,
    target_extent=None, target_resolution=None, band=None, nodata_value=None,
    cog_storage="50Gi", config: ClusterConfig = None,
):
    """Generate a k8s job that mosaics multiple raster tiles into a single COG on S3."""
    if config is None:
        config = ClusterConfig()
    # Build --input flags (one per tile)
    input_flags = "\n  ".join(f'--input "{u}" \\' for u in source_urls)

    optional_flags = ""
    if target_extent is not None:
        xmin, ymin, xmax, ymax = target_extent
        optional_flags += f'\n  --target-extent "{xmin},{ymin},{xmax},{ymax}" \\'
    if target_resolution is not None:
        optional_flags += f"\n  --target-resolution {target_resolution} \\"
    if band is not None:
        optional_flags += f"\n  --band {band} \\"
    if nodata_value is not None:
        optional_flags += f"\n  --nodata {nodata_value} \\"

    command_str = f"""set -e

# Locate PROJ database
PROJ_DB=$(find /usr /opt -name "proj.db" 2>/dev/null | head -1)
if [ -n "$PROJ_DB" ]; then
  export PROJ_DATA="$(dirname $PROJ_DB)"
  export PROJ_LIB="$PROJ_DATA"
fi

echo "Mosaicking {len(source_urls)} tiles → {output_cog_url}"

cng-datasets raster \\
  {input_flags}
  --output-cog "{output_cog_url}" \\
  --target-crs EPSG:4326 \\
  --resampling bilinear{optional_flags}

echo "✓ Preprocess COG complete: {output_cog_url}"
"""

    pod_spec = {
        "restartPolicy": "Never",
        "volumes": [
            {"name": "rclone-config", "secret": {"secretName": config.rclone_secret_name}}
        ],
        "containers": [{
            "name": "preprocess-cog",
            "image": "ghcr.io/boettiger-lab/datasets:latest",
            "imagePullPolicy": "Always",
            "env": _s3_env_vars(config) + [
                {"name": "GDAL_CACHEMAX", "value": "4096"},
                {"name": "GDAL_HTTP_TIMEOUT", "value": "60"},
                {"name": "GDAL_HTTP_MAX_RETRY", "value": "5"},
            ],
            "volumeMounts": [
                {"name": "rclone-config", "mountPath": "/root/.config/rclone", "readOnly": True}
            ],
            "command": ["bash", "-c", command_str],
            "resources": {
                "requests": {"cpu": "8", "memory": "32Gi", "ephemeral-storage": cog_storage},
                "limits": {"cpu": "8", "memory": "32Gi", "ephemeral-storage": cog_storage},
            },
        }],
    }
    _apply_scheduling(pod_spec, config)
    job_spec = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": f"{dataset_name}-preprocess-cog",
            "labels": {"k8s-app": f"{dataset_name}-preprocess-cog"},
        },
        "spec": {
            "completions": 1,
            "parallelism": 1,
            "backoffLimit": 1,
            "ttlSecondsAfterFinished": 86400,
            "template": {
                "metadata": {"labels": {"k8s-app": f"{dataset_name}-preprocess-cog"}},
                "spec": pod_spec,
            },
        },
    }
    manager.save_job_yaml(job_spec, str(output_path / f"{dataset_name}-preprocess-cog.yaml"))


def _generate_raster_hex_job(
    manager, dataset_name, source_url, bucket, output_path, git_repo,
    h3_resolution, parent_resolutions, value_column, nodata_value,
    hex_memory, max_parallelism, hex_storage="20Gi",
    hex_resampling: str = "average", config: ClusterConfig = None,
):
    """Generate raster H3 hex tiling job."""
    if config is None:
        config = ClusterConfig()
    parent_res_str = ','.join(map(str, parent_resolutions))

    # Build command
    cng_cmd = f"cng-datasets raster --input \"{source_url}\" --output-parquet s3://{bucket}/{dataset_name}/hex/ --h0-index ${{JOB_COMPLETION_INDEX}} --resolution {h3_resolution} --parent-resolutions {parent_res_str} --value-column {value_column} --hex-resampling {hex_resampling}"
    if nodata_value is not None:
        cng_cmd += f" --nodata {nodata_value}"

    command_str = f"""set -e

# Locate PROJ database dynamically to avoid version mismatches
PROJ_DB=$(find /usr /opt -name "proj.db" 2>/dev/null | head -1)
if [ -n "$PROJ_DB" ]; then
  export PROJ_DATA="$(dirname $PROJ_DB)"
  export PROJ_LIB="$PROJ_DATA"
fi

{cng_cmd}"""
    
    pod_spec = {
        "restartPolicy": "Never",
        "containers": [{
            "name": "hex-task",
            "image": "ghcr.io/boettiger-lab/datasets:latest",
            "imagePullPolicy": "Always",
            "env": _s3_env_vars(config) + [
                {"name": "GDAL_DATA", "value": "/usr/share/gdal"},
                {"name": "PYTHONPATH", "value": "/usr/lib/python3/dist-packages"},
                {"name": "BUCKET", "value": bucket}
            ],
            "command": ["bash", "-c", command_str],
            "resources": {
                "requests": {"cpu": "4", "memory": hex_memory, "ephemeral-storage": hex_storage},
                "limits": {"cpu": "4", "memory": hex_memory, "ephemeral-storage": hex_storage}
            }
        }]
    }
    _apply_scheduling(pod_spec, config)
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
            "podFailurePolicy": {
                "rules": [{
                    "action": "Ignore",
                    "onPodConditions": [{"type": "DisruptionTarget"}]
                }]
            },
            "ttlSecondsAfterFinished": 10800,
            "template": {
                "metadata": {"labels": {"k8s-app": f"{dataset_name}-hex"}},
                "spec": pod_spec,
            }
        }
    }
    manager.save_job_yaml(job_spec, str(output_path / f"{dataset_name}-hex.yaml"))


def _generate_raster_configmap(dataset_name, namespace, output_path, gen_command, needs_preprocess=False):
    """Generate ConfigMap for raster workflow."""
    import yaml

    job_files = [f"{dataset_name}-setup-bucket.yaml"]
    if needs_preprocess:
        job_files.append(f"{dataset_name}-preprocess-cog.yaml")
    job_files.append(f"{dataset_name}-hex.yaml")

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
            "namespace": namespace,
        },
        "data": data,
    }

    with open(output_path / "configmap.yaml", "w") as f:
        f.write("# Auto-generated ConfigMap for raster workflow\n")
        f.write(f"# Generation command: {gen_command}\n")
        yaml.dump(configmap, f, default_flow_style=False)


def _generate_raster_argo_workflow(dataset_name, namespace, output_path, output_dir, needs_preprocess=False):
    """Generate orchestrator job for raster workflow."""
    import yaml

    configmap_name = f"{dataset_name}-yamls"

    preprocess_steps = ""
    cleanup_jobs = f"{dataset_name}-setup-bucket {dataset_name}-hex {dataset_name}-workflow"
    if needs_preprocess:
        preprocess_steps = f"""
# Step 1: Preprocess — mosaic tiles into a single COG on S3
echo "Step 1: Preprocessing COG mosaic..."
kubectl apply -f /yamls/{dataset_name}-preprocess-cog.yaml -n {namespace}

echo "Waiting for preprocess-cog (may take 30-60 min for large tile sets)..."
kubectl wait --for=condition=complete --timeout=7200s job/{dataset_name}-preprocess-cog -n {namespace}
"""
        hex_step_num = "2"
        cleanup_jobs = f"{dataset_name}-setup-bucket {dataset_name}-preprocess-cog {dataset_name}-hex {dataset_name}-workflow"
    else:
        hex_step_num = "1"

    workflow_job = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": f"{dataset_name}-workflow",
            "namespace": namespace,
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
                        "args": [f"""set -e

echo "Starting {dataset_name} raster workflow..."

# Step 0: Setup bucket
echo "Step 0: Setting up bucket..."
kubectl apply -f /yamls/{dataset_name}-setup-bucket.yaml -n {namespace}
kubectl wait --for=condition=complete --timeout=600s job/{dataset_name}-setup-bucket -n {namespace}
{preprocess_steps}
# Step {hex_step_num}: H3 hexagonal tiling
echo "Step {hex_step_num}: H3 hexagonal tiling..."
kubectl apply -f /yamls/{dataset_name}-hex.yaml -n {namespace}
kubectl wait --for=condition=complete --timeout=7200s job/{dataset_name}-hex -n {namespace}

echo "✓ Workflow complete!"
echo "Clean up with:"
echo "  kubectl delete jobs {cleanup_jobs} -n {namespace}"
echo "  kubectl delete configmap {configmap_name} -n {namespace}"
"""],
                        "resources": {
                            "requests": {"cpu": "500m", "memory": "512Mi"},
                            "limits": {"cpu": "500m", "memory": "512Mi"},
                        },
                        "volumeMounts": [{"name": "yamls", "mountPath": "/yamls"}],
                    }],
                    "volumes": [{
                        "name": "yamls",
                        "configMap": {"name": configmap_name},
                    }],
                }
            },
            "ttlSecondsAfterFinished": 10800,
        },
    }

    with open(output_path / "workflow.yaml", "w") as f:
        yaml.dump(workflow_job, f, default_flow_style=False)


def _generate_setup_bucket_job(manager, dataset_name, bucket, output_path, git_repo, config: ClusterConfig = None):
    """Generate S3 bucket setup job with public access and CORS."""
    if config is None:
        config = ClusterConfig()
    pod_spec = {
        "restartPolicy": "Never",
        "containers": [{
            "name": "setup-bucket-task",
            "image": "ghcr.io/boettiger-lab/datasets:latest",
            "imagePullPolicy": "Always",
            "env": _s3_env_vars(config) + [
                {"name": "BUCKET", "value": bucket}
            ],
            "volumeMounts": [
                {"name": "rclone-config", "mountPath": "/root/.config/rclone", "readOnly": True}
            ],
            "command": ["bash", "-c", f"""set -e
echo "Setting up bucket with public access and CORS..."
cng-datasets storage setup-bucket \\
  --bucket "${{BUCKET}}" \\
  --remote {config.rclone_remote} \\
  --verify

echo "Bucket setup complete!"
"""],
            "resources": {
                "requests": {"cpu": "1", "memory": "2Gi"},
                "limits": {"cpu": "1", "memory": "2Gi"}
            }
        }],
        "volumes": [
            {"name": "rclone-config", "secret": {"secretName": config.rclone_secret_name}}
        ]
    }
    _apply_scheduling(pod_spec, config)
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
                "spec": pod_spec,
            }
        }
    }
    manager.save_job_yaml(job_spec, str(output_path / f"{dataset_name}-setup-bucket.yaml"))


def _generate_convert_job(manager, dataset_name, source_urls, bucket, output_path, git_repo, layer=None, memory="8Gi", row_group_size=100000, s3_dataset=None, config: ClusterConfig = None):
    """Generate GeoParquet conversion job."""
    if config is None:
        config = ClusterConfig()
    s3_dataset = s3_dataset or dataset_name

    # Normalize to list
    if isinstance(source_urls, str):
        source_urls = [source_urls]
    
    # Build source URLs string (one per line for readability)
    sources_str = " \\\n  ".join(source_urls)
    
    # Build the conversion command with optional layer parameter
    layer_flag = f" \\\n  --layer {layer}" if layer else ""
    convert_cmd = f"""set -e
cng-convert-to-parquet \\
  {sources_str} \\
  s3://{bucket}/{s3_dataset}.parquet \\
  --row-group-size {row_group_size}{layer_flag}
"""
    
    pod_spec = {
        "restartPolicy": "Never",
        "containers": [{
            "name": "convert-task",
            "image": "ghcr.io/boettiger-lab/datasets:latest",
            "imagePullPolicy": "Always",
            "env": _s3_env_vars(config) + [
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
            {"name": "rclone-config", "secret": {"secretName": config.rclone_secret_name}}
        ]
    }
    _apply_scheduling(pod_spec, config)
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
                "spec": pod_spec,
            }
        }
    }
    manager.save_job_yaml(job_spec, str(output_path / f"{dataset_name}-convert.yaml"))


def _generate_pmtiles_job(manager, dataset_name, source_url, bucket, output_path, git_repo, memory="8Gi", s3_dataset=None, config: ClusterConfig = None):
    """Generate PMTiles job.

    Uses the optimized GeoParquet from convert job as input (includes ID column).
    """
    if config is None:
        config = ClusterConfig()
    s3_dataset = s3_dataset or dataset_name
    # Use the converted geoparquet instead of original source
    geoparquet_url = f"https://{config.s3_public_endpoint}/{bucket}/{s3_dataset}.parquet"
    
    # For rclone upload, compute the S3 directory that contains the output file
    if '/' in s3_dataset:
        s3_parent_dir = '/'.join(s3_dataset.split('/')[:-1]) + '/'
    else:
        s3_parent_dir = ''
    # basename for temp files and tippecanoe layer name
    s3_basename = s3_dataset.split('/')[-1]
    
    pod_spec = {
        "restartPolicy": "Never",
        "containers": [{
            "name": "pmtiles-task",
            "image": "ghcr.io/boettiger-lab/datasets:latest",
            "imagePullPolicy": "Always",
            "env": _s3_env_vars(config) + [
                {"name": "TMPDIR", "value": "/tmp"},
                {"name": "BUCKET", "value": bucket},
                {"name": "DATASET", "value": s3_basename}
            ],
            "volumeMounts": [
                {"name": "rclone-config", "mountPath": "/root/.config/rclone", "readOnly": True}
            ],
            "command": ["bash", "-c", f"""set -e
# Use optimized GeoParquet (has ID column) from convert job
# GDAL can read parquet via vsicurl
ogr2ogr -wrapdateline -datelineoffset 15 -f GeoJSONSeq /tmp/$DATASET.geojsonl /vsicurl/{geoparquet_url} -progress

# Generate PMTiles from GeoJSONSeq
# TIPPECANOE_MAX_THREADS must be a power of 2; Kubernetes nodes can expose
# non-power-of-2 logical CPU counts which triggers an internal assertion
# failure "N shards not a power of 2" (felt/tippecanoe#216).
# Round detected CPU count down to nearest power of 2 to preserve I/O parallelism.
TIPPECANOE_MAX_THREADS=$(python3 -c "import os; n=os.cpu_count() or 1; print(1<<(n.bit_length()-1))")
tippecanoe -o /tmp/$DATASET.pmtiles -l $DATASET --drop-densest-as-needed --extend-zooms-if-still-dropping --force /tmp/$DATASET.geojsonl

# Upload to S3 using rclone
rclone copy /tmp/$DATASET.pmtiles {config.rclone_remote}:{bucket}/{s3_parent_dir}
rm /tmp/$DATASET.geojsonl /tmp/$DATASET.pmtiles
"""],
            "resources": {
                "requests": {"cpu": "4", "memory": memory},
                "limits": {"cpu": "4", "memory": memory}
            }
        }],
        "volumes": [
            {"name": "rclone-config", "secret": {"secretName": config.rclone_secret_name}}
        ]
    }
    _apply_scheduling(pod_spec, config)
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
                "spec": pod_spec,
            }
        }
    }
    manager.save_job_yaml(job_spec, str(output_path / f"{dataset_name}-pmtiles.yaml"))


def _generate_hex_job(manager, dataset_name, bucket, output_path, git_repo, chunk_size, completions, parallelism, h3_resolution, parent_resolutions, id_column=None, hex_memory="8Gi", intermediate_chunk_size=10, s3_dataset=None, hex_storage="10Gi", config: ClusterConfig = None):
    """Generate H3 hex tiling job.
    
    Args:
        manager: K8sJobManager instance
        dataset_name: Name for k8s resources
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
        s3_dataset: S3 path prefix (defaults to dataset_name)
    """
    if config is None:
        config = ClusterConfig()
    s3_dataset = s3_dataset or dataset_name
    # Format parent resolutions as comma-separated string
    parent_res_str = ','.join(map(str, parent_resolutions))
    
    # Build command with optional id-column parameter
    cmd_parts = [
        "set -e",
        f"cng-datasets vector --input s3://{bucket}/{s3_dataset}.parquet --output s3://{bucket}/{s3_dataset}/chunks --chunk-id ${{JOB_COMPLETION_INDEX}} --chunk-size {chunk_size} --intermediate-chunk-size {intermediate_chunk_size} --resolution {h3_resolution} --parent-resolutions {parent_res_str}"
    ]
    if id_column:
        cmd_parts[-1] += f" --id-column {id_column}"
    
    command_str = "\n".join(cmd_parts)
    
    pod_spec = {
        "restartPolicy": "Never",
        "containers": [{
            "name": "hex-task",
            "image": "ghcr.io/boettiger-lab/datasets:latest",
            "imagePullPolicy": "Always",
            "env": _s3_env_vars(config) + [
                {"name": "TMPDIR", "value": "/tmp"},
                {"name": "BUCKET", "value": bucket},
                {"name": "DATASET", "value": dataset_name}
            ],
            "command": ["bash", "-c", command_str],
            "resources": {
                "requests": {"cpu": "4", "memory": hex_memory, "ephemeral-storage": hex_storage},
                "limits": {"cpu": "4", "memory": hex_memory, "ephemeral-storage": hex_storage}
            }
        }]
    }
    _apply_scheduling(pod_spec, config)
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
            "backoffLimit": 0,
            "podFailurePolicy": {
                "rules": [{
                    "action": "Ignore",
                    "onPodConditions": [{"type": "DisruptionTarget"}]
                }]
            },
            "ttlSecondsAfterFinished": 10800,
            "template": {
                "metadata": {"labels": {"k8s-app": f"{dataset_name}-hex"}},
                "spec": pod_spec,
            }
        }
    }
    manager.save_job_yaml(job_spec, str(output_path / f"{dataset_name}-hex.yaml"))


def _duckdb_memory_limit(memory_str: str, fraction: float = 0.85) -> str:
    """Return a DuckDB memory_limit string that is *fraction* of *memory_str*.

    Parses strings like "32Gi", "8Gi", "512Mi" and returns e.g. "27GiB",
    keeping DuckDB's preferred "GiB"/"MiB" suffix so the value is unambiguous.
    Falls back to returning *memory_str* unchanged if it cannot be parsed.
    """
    m = re.match(r'^(\d+(?:\.\d+)?)(Gi|Mi|G|M)$', memory_str)
    if not m:
        return memory_str
    value, unit = float(m.group(1)), m.group(2)
    result = int(value * fraction)
    duckdb_unit = {"Gi": "GiB", "Mi": "MiB", "G": "GB", "M": "MB"}[unit]
    return f"{result}{duckdb_unit}"


def _generate_repartition_job(manager, dataset_name, bucket, output_path, git_repo, s3_dataset=None, repartition_storage: str = "50Gi", repartition_memory: str = "32Gi", config: ClusterConfig = None):
    """Generate repartition job."""
    if config is None:
        config = ClusterConfig()
    s3_dataset = s3_dataset or dataset_name
    pod_spec = {
        "restartPolicy": "Never",
        "containers": [{
            "name": "repartition-task",
            "image": "ghcr.io/boettiger-lab/datasets:latest",
            "imagePullPolicy": "Always",
            "env": _s3_env_vars(config) + [
                {"name": "TMPDIR", "value": "/tmp"},
                {"name": "BUCKET", "value": bucket}
            ],
            "volumeMounts": [
                {"name": "rclone-config", "mountPath": "/root/.config/rclone", "readOnly": True}
            ],
            "command": ["bash", "-c", f"""set -e
cng-datasets repartition --chunks-dir s3://{bucket}/{s3_dataset}/chunks --output-dir s3://{bucket}/{s3_dataset}/hex --source-parquet s3://{bucket}/{s3_dataset}.parquet --cleanup --memory-limit {_duckdb_memory_limit(repartition_memory)}
"""],
            "resources": {
                "requests": {"cpu": "4", "memory": repartition_memory, "ephemeral-storage": repartition_storage},
                "limits": {"cpu": "4", "memory": repartition_memory, "ephemeral-storage": repartition_storage}
            }
        }],
        "volumes": [
            {"name": "rclone-config", "secret": {"secretName": config.rclone_secret_name}}
        ]
    }
    _apply_scheduling(pod_spec, config)
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
                "spec": pod_spec,
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

