#!/usr/bin/env python3
"""Restart failed wetlands Job indices with higher-memory pods.

This script:
1. Finds failed pods for a given Job.
2. Extracts their completion indexes.
3. Creates one high-memory retry Job per failed index and applies it via kubectl.
"""
from __future__ import annotations

import argparse
import copy
import json
import subprocess
import sys
from datetime import UTC, datetime
from typing import Dict, List, Sequence


def run_kubectl(args: Sequence[str], namespace: str | None = None) -> subprocess.CompletedProcess:
    cmd = ["kubectl"]
    if namespace:
        cmd.extend(["-n", namespace])
    cmd.extend(args)
    try:
        return subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(exc.stderr or "")
        raise


def kubectl_json(args: Sequence[str], namespace: str | None = None) -> Dict:
    result = run_kubectl([*args, "-o", "json"], namespace=namespace)
    return json.loads(result.stdout)


def collect_failed_indexes(job_name: str, namespace: str) -> List[int]:
    pods = kubectl_json(
        [
            "get",
            "pods",
            "-l",
            f"job-name={job_name}",
            "--field-selector",
            "status.phase=Failed",
        ],
        namespace=namespace,
    )
    indexes = set()
    for item in pods.get("items", []):
        annotations = item.get("metadata", {}).get("annotations", {})
        raw_index = annotations.get("batch.kubernetes.io/job-completion-index")
        if raw_index is None:
            continue
        try:
            indexes.add(int(raw_index))
        except ValueError:
            continue
    return sorted(indexes)


RESERVED_LABELS = {
    "batch.kubernetes.io/controller-uid",
    "batch.kubernetes.io/job-name",
    "controller-uid",
    "job-name",
}


def scrub_labels(labels: Dict[str, str]) -> Dict[str, str]:
    return {k: v for k, v in labels.items() if k not in RESERVED_LABELS}


def build_retry_job(
    base_job: Dict,
    index: int,
    memory: str,
    namespace: str,
    backoff_limit: int,
    ttl_seconds: int | None,
) -> Dict:
    base_labels = scrub_labels(base_job.get("metadata", {}).get("labels", {}))
    template = copy.deepcopy(base_job["spec"]["template"])
    pod_labels = template.setdefault("metadata", {}).setdefault("labels", {})
    pod_labels = scrub_labels(pod_labels)
    template["metadata"]["labels"] = pod_labels
    pod_labels["wetlands/retry"] = str(index)

    containers = template["spec"].get("containers", [])
    if not containers:
        raise RuntimeError("Base job template has no containers to modify.")
    main_container = containers[0]

    # Force higher memory
    resources = main_container.setdefault("resources", {})
    for key in ("requests", "limits"):
        section = resources.setdefault(key, {})
        section["memory"] = memory

    # Overwrite INDEX env entry with the explicit value
    env_list = main_container.setdefault("env", [])
    replaced = False
    for entry in env_list:
        if entry.get("name") == "INDEX":
            entry.clear()
            entry["name"] = "INDEX"
            entry["value"] = str(index)
            replaced = True
            break
    if not replaced:
        env_list.append({"name": "INDEX", "value": str(index)})

    # Build Job manifest
    metadata = {
        "name": f"{base_job['metadata']['name']}-retry-{index}",
        "namespace": namespace,
        "labels": {
            **base_labels,
            "wetlands/retry": str(index),
        },
        "annotations": {
            "wetlands/retry-index": str(index),
            "wetlands/retry-timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
        },
    }

    spec = {
        "parallelism": 1,
        "completions": 1,
        "backoffLimit": backoff_limit,
        "template": template,
    }
    if ttl_seconds is not None:
        spec["ttlSecondsAfterFinished"] = ttl_seconds

    retry_job = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": metadata,
        "spec": spec,
    }
    return retry_job


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job-name", default="wetlands", help="Source Job to inspect")
    parser.add_argument("--namespace", default="biodiversity", help="Kubernetes namespace")
    parser.add_argument("--memory", default="120Gi", help="Memory request/limit for retry pods")
    parser.add_argument(
        "--backoff-limit", type=int, default=1, help="Backoff limit to use for retry Jobs"
    )
    parser.add_argument(
        "--ttl", type=int, default=3600, help="ttlSecondsAfterFinished for retry Jobs"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print what would be applied without creating Jobs",
    )
    parser.add_argument(
        "--indexes",
        nargs="+",
        type=int,
        help="Explicit completion indexes to re-run (skip auto-discovery)",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Delete existing retry Jobs with the same name before creating new ones",
    )
    args = parser.parse_args()

    if args.indexes:
        failed_indexes = sorted(set(args.indexes))
    else:
        failed_indexes = collect_failed_indexes(args.job_name, args.namespace)
    if not failed_indexes:
        print("No failed pods detected; nothing to restart.")
        return

    base_job = kubectl_json(["get", "job", args.job_name], namespace=args.namespace)

    created = []
    for index in failed_indexes:
        manifest = build_retry_job(
            base_job=base_job,
            index=index,
            memory=args.memory,
            namespace=args.namespace,
            backoff_limit=args.backoff_limit,
            ttl_seconds=args.ttl,
        )
        job_name = manifest["metadata"]["name"]

        if args.replace and not args.dry_run:
            run_kubectl(
                [
                    "delete",
                    "job",
                    job_name,
                    "--ignore-not-found",
                    "--wait=true",
                ],
                namespace=args.namespace,
            )
        else:
            # Check if the job exists already when not replacing
            if not args.dry_run:
                try:
                    run_kubectl(["get", "job", job_name], namespace=args.namespace)
                    print(
                        f"Skipping {job_name} (already exists). Use --replace to recreate.")
                    continue
                except subprocess.CalledProcessError:
                    pass

        payload = json.dumps(manifest)
        cmd = ["kubectl", "-n", args.namespace, "apply", "-f", "-"]
        if args.dry_run:
            cmd.append("--dry-run=server")
        subprocess.run(cmd, input=payload.encode(), check=True)
        created.append(job_name)

    print(f"Applied {len(created)} retry Jobs: {', '.join(created)}")


if __name__ == "__main__":
    main()
