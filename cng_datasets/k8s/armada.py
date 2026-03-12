"""
Armada job generation utilities.

Converts standard Kubernetes Job specs to Armada submission format.
Armada is a multi-cluster batch job scheduler used on the NRP Nautilus cluster.
See: https://nrp.ai/documentation/userdocs/running/scheduling/
"""

import copy
import re
import yaml
from typing import Dict, Any, List, Optional
from pathlib import Path


# Armada priority classes available on NRP
ARMADA_PRIORITY_CLASSES = {
    "default": "armada-default",        # non-preemptible, priority 100
    "preemptible": "armada-preemptible", # preemptible, priority 50
    "high": "armada-high-priority",      # non-preemptible, priority 1000
}

# Map from k8s priority classes to Armada equivalents
K8S_TO_ARMADA_PRIORITY = {
    "opportunistic": "armada-preemptible",
}


def _extract_pod_spec(job_spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract the podSpec from a k8s Job spec.

    Strips fields that Armada doesn't use (priorityClassName is handled
    at the Armada job level, not in the podSpec).
    """
    pod_spec = copy.deepcopy(job_spec["spec"]["template"]["spec"])

    # Remove k8s-level priorityClassName (Armada uses its own)
    pod_spec.pop("priorityClassName", None)

    return pod_spec


def _map_priority_class(job_spec: Dict[str, Any], default: str = "armada-preemptible") -> str:
    """Map a k8s Job's priorityClassName to the Armada equivalent."""
    k8s_priority = job_spec.get("spec", {}).get("template", {}).get(
        "spec", {}
    ).get("priorityClassName", "")
    return K8S_TO_ARMADA_PRIORITY.get(k8s_priority, default)


def k8s_job_to_armada(
    job_spec: Dict[str, Any],
    queue: str,
    job_set_id: str,
    priority_class: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Convert a single-pod k8s Job spec to Armada submission format.

    Args:
        job_spec: Standard k8s batch/v1 Job spec dict
        queue: Armada queue (typically matches k8s namespace)
        job_set_id: Unique identifier for this job set
        priority_class: Armada priority class (auto-mapped from k8s if None)

    Returns:
        Armada submission dict ready for YAML serialization
    """
    if priority_class is None:
        priority_class = _map_priority_class(job_spec)

    namespace = job_spec["metadata"].get("namespace", queue)
    pod_spec = _extract_pod_spec(job_spec)

    return {
        "queue": queue,
        "jobSetId": job_set_id,
        "jobs": [{
            "namespace": namespace,
            "priorityClassName": priority_class,
            "podSpec": pod_spec,
        }],
    }


def k8s_indexed_job_to_armada(
    job_spec: Dict[str, Any],
    queue: str,
    job_set_id: str,
    priority_class: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Convert a k8s Indexed Job to multiple Armada jobs.

    Each indexed completion becomes a separate Armada job with
    ${JOB_COMPLETION_INDEX} replaced by the literal index value
    in all command/args strings.

    Args:
        job_spec: k8s Job spec with completionMode: Indexed
        queue: Armada queue
        job_set_id: Unique identifier for this job set
        priority_class: Armada priority class (auto-mapped from k8s if None)

    Returns:
        Armada submission dict with N jobs (one per completion index)
    """
    if priority_class is None:
        priority_class = _map_priority_class(job_spec)

    completions = job_spec["spec"]["completions"]
    namespace = job_spec["metadata"].get("namespace", queue)
    base_pod_spec = _extract_pod_spec(job_spec)

    jobs = []
    for i in range(completions):
        pod = copy.deepcopy(base_pod_spec)
        _replace_completion_index(pod, i)
        jobs.append({
            "namespace": namespace,
            "priorityClassName": priority_class,
            "podSpec": pod,
        })

    return {
        "queue": queue,
        "jobSetId": job_set_id,
        "jobs": jobs,
    }


def _replace_completion_index(pod_spec: Dict[str, Any], index: int):
    """
    Replace all occurrences of ${JOB_COMPLETION_INDEX} in container
    command and args strings with the literal index value.
    """
    pattern = re.compile(r'\$\{?JOB_COMPLETION_INDEX\}?')
    replacement = str(index)

    for container in pod_spec.get("containers", []):
        if "command" in container:
            container["command"] = [
                pattern.sub(replacement, s) for s in container["command"]
            ]
        if "args" in container:
            container["args"] = [
                pattern.sub(replacement, s) for s in container["args"]
            ]


def save_armada_yaml(armada_spec: Dict[str, Any], output_path: str):
    """Save Armada submission spec to YAML file."""
    def str_representer(dumper, data):
        if '\n' in data:
            return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
        return dumper.represent_scalar('tag:yaml.org,2002:str', data)

    yaml.add_representer(str, str_representer)

    with open(output_path, 'w') as f:
        yaml.dump(armada_spec, f, default_flow_style=False, sort_keys=False)
    print(f"Armada YAML saved to {output_path}")


def convert_workflow_to_armada(
    k8s_yaml_dir: str,
    dataset_name: str,
    queue: str = "biodiversity",
    output_dir: Optional[str] = None,
) -> List[str]:
    """
    Convert a directory of k8s Job YAMLs to Armada submission YAMLs.

    Reads each job YAML from k8s_yaml_dir, converts to Armada format,
    and writes to output_dir (defaults to k8s_yaml_dir).

    Args:
        k8s_yaml_dir: Directory containing k8s Job YAML files
        dataset_name: Dataset name (used for job set IDs)
        queue: Armada queue name
        output_dir: Output directory (defaults to k8s_yaml_dir)

    Returns:
        List of generated Armada YAML file paths
    """
    k8s_path = Path(k8s_yaml_dir)
    out_path = Path(output_dir) if output_dir else k8s_path
    out_path.mkdir(parents=True, exist_ok=True)

    generated = []

    # Process each job YAML (skip workflow/configmap/rbac files)
    skip_files = {"workflow.yaml", "configmap.yaml", "workflow-rbac.yaml"}

    for yaml_file in sorted(k8s_path.glob(f"{dataset_name}-*.yaml")):
        if yaml_file.name in skip_files:
            continue

        with open(yaml_file) as f:
            job_spec = yaml.safe_load(f)

        if not job_spec or job_spec.get("kind") != "Job":
            continue

        # Derive step name from job file (e.g., "dataset-hex.yaml" -> "hex")
        step = yaml_file.stem.replace(f"{dataset_name}-", "")
        job_set_id = f"{dataset_name}-{step}"

        # Check if this is an indexed job
        is_indexed = job_spec.get("spec", {}).get("completionMode") == "Indexed"

        if is_indexed:
            armada_spec = k8s_indexed_job_to_armada(
                job_spec, queue=queue, job_set_id=job_set_id
            )
        else:
            armada_spec = k8s_job_to_armada(
                job_spec, queue=queue, job_set_id=job_set_id
            )

        out_file = out_path / f"armada-{yaml_file.name}"
        save_armada_yaml(armada_spec, str(out_file))
        generated.append(str(out_file))

    return generated
