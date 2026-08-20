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


# Armada priority classes available on NRP, keyed by the shorthand accepted
# by --armada-priority-class
ARMADA_PRIORITY_CLASSES = {
    "default": "armada-default",        # non-preemptible, priority 100
    "preemptible": "armada-preemptible", # preemptible, priority 50
    "high": "armada-high-priority",      # non-preemptible, priority 1000
}

# Non-preemptible. The converter reproduces the *shape* of the k8s job it reads
# — currently one Armada job per completion, same cpu/memory/runtime — so the
# units it emits are long-running. A preempted Armada job is not rescheduled
# (nrp.ai/documentation/userdocs/running/scheduling/) and the Job-level retry
# settings do not survive conversion, so preemptible is the wrong default until
# units are small enough that losing one is cheap (issue #183).
DEFAULT_ARMADA_PRIORITY_CLASS = "armada-default"

# Map from k8s priority classes to Armada equivalents.
#
# "opportunistic" is deliberately absent. It is preemptible on the k8s side, but
# an opportunistic pod is preempted and then *recreated by its Job controller*,
# whereas a preempted Armada job simply stops. Mapping it onto
# "armada-preemptible" preserves the preemption and drops the recovery, which is
# how a converted long job ends up with neither retries nor rescheduling. The
# closest behavioural equivalent — work that eventually finishes — is the
# non-preemptible default; pass priority_class explicitly to override (#183).
K8S_TO_ARMADA_PRIORITY: Dict[str, str] = {}

# Job-level retry settings that live outside the podSpec and so cannot survive
# conversion: Armada exposes no per-job equivalent (retries are a server-side
# setting), so these are dropped rather than remapped (issue #183).
K8S_JOB_LEVEL_RETRY_FIELDS = (
    "backoffLimit",
    "backoffLimitPerIndex",
    "maxFailedIndexes",
)


def resolve_armada_priority_class(value: Optional[str]) -> Optional[str]:
    """
    Resolve a user-supplied priority class to a literal Armada class name.

    Accepts the shorthands in ARMADA_PRIORITY_CLASSES ("default",
    "preemptible", "high") and passes any other non-empty string through
    unchanged, so cluster-specific classes stay usable.
    """
    if value is None or value == "":
        return None
    return ARMADA_PRIORITY_CLASSES.get(value, value)


def _warn_dropped_retry_settings(job_spec: Dict[str, Any], priority_class: str):
    """
    Warn about k8s Job-level retry settings that conversion discards.

    `_extract_pod_spec` takes `spec.template.spec`, so everything set at Job
    level is left behind. Silently dropping the retry budget is worst when the
    converted job also lands at preemptible priority: multi-hour work, no
    retries, no rescheduling (issue #183).
    """
    spec = job_spec.get("spec", {})
    # A field set to 0 grants no retries, so dropping it loses nothing — only
    # warn when a real retry budget disappears.
    dropped = {
        field: spec[field]
        for field in K8S_JOB_LEVEL_RETRY_FIELDS
        if spec.get(field)
    }
    if not dropped:
        return

    settings = ", ".join(f"{k}={v}" for k, v in dropped.items())
    name = job_spec.get("metadata", {}).get("name", "job")
    message = (
        f"⚠ Dropping k8s Job-level retry settings when converting {name} to "
        f"Armada: {settings}.\n"
        f"  Armada has no per-job equivalent (retries are a server-side "
        f"setting), so a failed job stays failed."
    )
    if priority_class == ARMADA_PRIORITY_CLASSES["preemptible"]:
        message += (
            "\n  This job is also preemptible, and preempted Armada jobs are "
            "not rescheduled — a preemption\n"
            "  loses the whole unit. Consider "
            "--armada-priority-class default for long-running units."
        )
    print(message)


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


def _map_priority_class(job_spec: Dict[str, Any], default: str = DEFAULT_ARMADA_PRIORITY_CLASS) -> str:
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
        priority_class: Armada priority class, either a literal name
            ("armada-default") or a shorthand ("default", "preemptible",
            "high"). Defaults to DEFAULT_ARMADA_PRIORITY_CLASS when None.

    Returns:
        Armada submission dict ready for YAML serialization
    """
    priority_class = resolve_armada_priority_class(priority_class)
    if priority_class is None:
        priority_class = _map_priority_class(job_spec)
    _warn_dropped_retry_settings(job_spec, priority_class)

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
        priority_class: Armada priority class, either a literal name
            ("armada-default") or a shorthand ("default", "preemptible",
            "high"). Defaults to DEFAULT_ARMADA_PRIORITY_CLASS when None.

    Returns:
        Armada submission dict with N jobs (one per completion index)
    """
    priority_class = resolve_armada_priority_class(priority_class)
    if priority_class is None:
        priority_class = _map_priority_class(job_spec)
    _warn_dropped_retry_settings(job_spec, priority_class)

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
    queue: str = "geo-workflows",
    output_dir: Optional[str] = None,
    priority_class: Optional[str] = None,
) -> List[str]:
    """
    Convert a directory of k8s Job YAMLs to Armada submission YAMLs.

    Reads each job YAML from k8s_yaml_dir, converts to Armada format,
    and writes to output_dir (defaults to k8s_yaml_dir).

    Args:
        k8s_yaml_dir: Directory containing k8s Job YAML files
        dataset_name: Dataset name (used for job set IDs)
        queue: Armada queue name. On NRP, queues map one-to-one onto cluster
            namespaces, so callers normally pass the workflow's namespace.
        output_dir: Output directory (defaults to k8s_yaml_dir)
        priority_class: Armada priority class applied to every converted job,
            as a literal name or a shorthand ("default", "preemptible",
            "high"). Defaults to DEFAULT_ARMADA_PRIORITY_CLASS when None
            (issue #183).

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
                job_spec,
                queue=queue,
                job_set_id=job_set_id,
                priority_class=priority_class,
            )
        else:
            armada_spec = k8s_job_to_armada(
                job_spec,
                queue=queue,
                job_set_id=job_set_id,
                priority_class=priority_class,
            )

        out_file = out_path / f"armada-{yaml_file.name}"
        save_armada_yaml(armada_spec, str(out_file))
        generated.append(str(out_file))

    return generated
