#!/usr/bin/env python3
"""Create one Kubernetes Job per wetlands index.

This orchestrator reads a base Job manifest (either from the cluster or a local
YAML file) and spawns an individual Job per index so that no single high-memory
index can block progress. Each generated Job runs exactly once with an
explicit `INDEX` environment variable, keeping retries isolated per index.
"""
from __future__ import annotations

import argparse
import copy
import json
import pathlib
import subprocess
import sys
from datetime import UTC, datetime
from typing import Dict, Iterable, List, Sequence, Tuple

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - optional dependency when using --template-path
    yaml = None


def run_kubectl(args: Sequence[str], namespace: str | None = None) -> subprocess.CompletedProcess:
    cmd = ["kubectl"]
    if namespace:
        cmd.extend(["-n", namespace])
    cmd.extend(args)
    try:
        return subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as exc:  # pragma: no cover - passthrough to caller
        sys.stderr.write(exc.stderr or "")
        raise


def kubectl_json(args: Sequence[str], namespace: str | None = None) -> Dict:
    result = run_kubectl([*args, "-o", "json"], namespace=namespace)
    return json.loads(result.stdout)


RESERVED_LABELS = {
    "batch.kubernetes.io/controller-uid",
    "batch.kubernetes.io/job-name",
    "controller-uid",
    "job-name",
}


def scrub_labels(labels: Dict[str, str]) -> Dict[str, str]:
    return {k: v for k, v in labels.items() if k not in RESERVED_LABELS}


def load_base_job(template_path: pathlib.Path | None, source_job: str | None, namespace: str) -> Dict:
    if template_path is not None:
        if yaml is None:
            raise RuntimeError(
                "PyYAML is required to read template files. Install it or use --source-job"
            )
        data = template_path.read_text()
        return yaml.safe_load(data)
    if source_job is None:
        raise ValueError("Either --template-path or --source-job must be provided")
    return kubectl_json(["get", "job", source_job], namespace=namespace)


def set_index_env(main_container: Dict, index: int) -> None:
    env_list = main_container.setdefault("env", [])
    for entry in env_list:
        if entry.get("name") == "INDEX":
            entry.clear()
            entry["name"] = "INDEX"
            entry["value"] = str(index)
            return
    env_list.append({"name": "INDEX", "value": str(index)})


def override_resources(main_container: Dict, memory: str | None, cpu: str | None) -> None:
    if memory is None and cpu is None:
        return
    resources = main_container.setdefault("resources", {})
    for section_name in ("requests", "limits"):
        section = resources.setdefault(section_name, {})
        if memory is not None:
            section["memory"] = memory
        if cpu is not None:
            section["cpu"] = cpu


def build_single_index_job(
    base_job: Dict,
    index: int,
    namespace: str,
    job_name_prefix: str,
    batch_label: str | None,
    backoff_limit: int,
    ttl_seconds: int | None,
    memory: str | None,
    cpu: str | None,
) -> Dict:
    template = copy.deepcopy(base_job["spec"]["template"])
    template.setdefault("metadata", {})
    template["metadata"].setdefault("labels", {})
    template["metadata"]["labels"] = scrub_labels(template["metadata"]["labels"])
    template["metadata"]["labels"]["wetlands/index"] = str(index)
    if batch_label:
        template["metadata"]["labels"]["wetlands/batch"] = batch_label

    containers = template["spec"].get("containers", [])
    if not containers:
        raise RuntimeError("Base job template must define at least one container")
    main_container = containers[0]
    set_index_env(main_container, index)
    override_resources(main_container, memory=memory, cpu=cpu)

    job_labels = scrub_labels(base_job.get("metadata", {}).get("labels", {}))
    job_labels["wetlands/index"] = str(index)
    if batch_label:
        job_labels["wetlands/batch"] = batch_label

    metadata = {
        "name": f"{job_name_prefix}-{index}",
        "namespace": namespace,
        "labels": job_labels,
        "annotations": {
            "wetlands/index": str(index),
            "wetlands/orchestrator-timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
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

    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": metadata,
        "spec": spec,
    }


def iter_indexes(args: argparse.Namespace) -> Iterable[int]:
    if args.indexes:
        return sorted(set(args.indexes))
    if args.index_file:
        values: List[int] = []
        for line in pathlib.Path(args.index_file).read_text().splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            values.append(int(stripped))
        return sorted(set(values))
    stop = args.start + args.count
    return range(args.start, stop)


def apply_manifest(manifest: Dict, namespace: str, dry_run_mode: str) -> None:
    payload = json.dumps(manifest)
    cmd = ["kubectl", "-n", namespace, "apply", "-f", "-"]
    if dry_run_mode == "server":
        cmd.append("--dry-run=server")
    subprocess.run(cmd, input=payload.encode(), check=True)


def ensure_job_absent(job_name: str, namespace: str) -> None:
    run_kubectl(["delete", "job", job_name, "--ignore-not-found", "--wait=true"], namespace)


def job_exists(job_name: str, namespace: str) -> bool:
    try:
        run_kubectl(["get", "job", job_name], namespace)
        return True
    except subprocess.CalledProcessError:
        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=False)
    source.add_argument(
        "--template-path",
        type=pathlib.Path,
        help="Path to a Job YAML manifest to clone",
    )
    source.add_argument(
        "--source-job",
        help="Name of an existing Job in the cluster to clone",
    )
    parser.add_argument("--namespace", default="biodiversity", help="Kubernetes namespace")
    parser.add_argument("--start", type=int, default=0, help="First index (inclusive)")
    parser.add_argument(
        "--count",
        type=int,
        default=18587,
        help="Number of indexes to generate starting from --start",
    )
    parser.add_argument(
        "--indexes",
        nargs="+",
        type=int,
        help="Explicit list of indexes (overrides --start/--count)",
    )
    parser.add_argument(
        "--index-file",
        help="File containing newline-separated indexes (overrides --start/--count)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Only process the first N indexes from the resolved list",
    )
    parser.add_argument(
        "--job-name-prefix",
        default="wetlands-index",
        help="Prefix for generated Job names",
    )
    parser.add_argument(
        "--batch-label",
        help="Label applied to every Job/pod (wetlands/batch=<value>) for waiting/cleanup",
    )
    parser.add_argument(
        "--batch-label-template",
        default="tier{tier}-chunk-{start:05d}",
        help="Template for per-chunk batch labels when --chunk-size is used",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        help="Number of indexes per submission chunk (processes sequentially when set)",
    )
    parser.add_argument(
        "--wait-for-batch",
        action="store_true",
        help="Wait for each chunk (selector wetlands/batch=<label>) to complete before continuing",
    )
    parser.add_argument(
        "--wait-timeout",
        default="2h",
        help="Timeout passed to kubectl wait when --wait-for-batch is enabled (default: 2h)",
    )
    parser.add_argument(
        "--memory",
        help="Override memory request/limit (e.g., 60Gi); use template value when omitted",
    )
    parser.add_argument(
        "--tier",
        type=int,
        help="Tier index for the memory schedule (0 -> base GiB, doubles by default)",
    )
    parser.add_argument(
        "--schedule-start-gi",
        type=float,
        default=10.0,
        help="Base GiB value for tier 0 when --tier is supplied (default: 10 GiB)",
    )
    parser.add_argument(
        "--schedule-factor",
        type=float,
        default=2.0,
        help="Multiplier applied per tier when --tier is supplied (default: doubling)",
    )
    parser.add_argument(
        "--cpu",
        help="Override CPU request/limit (e.g., 2);")
    parser.add_argument(
        "--backoff-limit",
        type=int,
        default=0,
        help="Backoff limit for generated Jobs",
    )
    parser.add_argument(
        "--ttl",
        type=int,
        default=3600,
        help="ttlSecondsAfterFinished for generated Jobs (omit via --ttl -1)",
    )
    parser.add_argument(
        "--dry-run",
        choices=["none", "server", "print"],
        default="none",
        help="Control how manifests are applied: real apply, server dry-run, or just print",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Delete any existing Jobs with the same name before applying",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-job status lines",
    )
    args = parser.parse_args()
    if args.ttl is not None and args.ttl < 0:
        args.ttl = None
    if not args.template_path and not args.source_job:
        args.template_path = pathlib.Path(__file__).with_name("job.yaml")
    return args


def format_gib(value: float) -> str:
    if value.is_integer():
        return f"{int(value)}Gi"
    return f"{value:g}Gi"


def resolve_memory_override(args: argparse.Namespace) -> str | None:
    if args.memory:
        return args.memory
    if args.tier is not None:
        if args.schedule_start_gi <= 0:
            raise ValueError("--schedule-start-gi must be positive when --tier is used")
        if args.schedule_factor <= 0:
            raise ValueError("--schedule-factor must be positive when --tier is used")
        memory_value = args.schedule_start_gi * (args.schedule_factor ** args.tier)
        return format_gib(memory_value)
    return None


def main() -> None:
    args = parse_args()
    base_job = load_base_job(args.template_path, args.source_job, args.namespace)
    ttl_seconds = args.ttl
    memory_override = resolve_memory_override(args)

    resolved_indexes = list(iter_indexes(args))
    if args.limit is not None:
        resolved_indexes = resolved_indexes[: args.limit]
    if not resolved_indexes:
        print("No indexes resolved; nothing to do.")
        return

    applied_total = 0

    def process_chunk(chunk_indexes: List[int], chunk_label: str | None) -> int:
        chunk_applied = 0
        for index in chunk_indexes:
            manifest = build_single_index_job(
                base_job=base_job,
                index=index,
                namespace=args.namespace,
                job_name_prefix=args.job_name_prefix,
                batch_label=chunk_label,
                backoff_limit=args.backoff_limit,
                ttl_seconds=ttl_seconds,
                memory=memory_override,
                cpu=args.cpu,
            )
            job_name = manifest["metadata"]["name"]
            if args.dry_run == "print":
                print(json.dumps(manifest, indent=2))
                continue
            if args.replace:
                ensure_job_absent(job_name, args.namespace)
            elif job_exists(job_name, args.namespace):
                if not args.quiet:
                    print(f"Skipping {job_name} (already exists)")
                continue
            apply_manifest(manifest, args.namespace, args.dry_run)
            chunk_applied += 1
            if not args.quiet:
                mode = "dry-run" if args.dry_run != "none" else "applied"
                print(f"{job_name} {mode}")
        return chunk_applied

    def chunk_iterator(indexes: List[int]) -> Iterable[Tuple[int, List[int]]]:
        if not args.chunk_size:
            yield 0, indexes
            return
        size = args.chunk_size
        if size <= 0:
            raise ValueError("--chunk-size must be positive")
        for chunk_num, start in enumerate(range(0, len(indexes), size)):
            yield chunk_num, indexes[start : start + size]

    def compute_batch_label(chunk_num: int, chunk_indexes: List[int]) -> str | None:
        if args.batch_label:
            return args.batch_label
        if not args.chunk_size:
            return None
        template = args.batch_label_template
        tier_value = args.tier if args.tier is not None else "na"
        context = {
            "chunk": chunk_num,
            "start": chunk_indexes[0],
            "end": chunk_indexes[-1],
            "tier": tier_value,
        }
        try:
            return template.format(**context)
        except KeyError as exc:  # pragma: no cover - surface formatting issues early
            raise ValueError(f"Unknown placeholder {exc} in --batch-label-template") from exc

    for chunk_num, chunk_indexes in chunk_iterator(resolved_indexes):
        batch_label = compute_batch_label(chunk_num, chunk_indexes)
        if batch_label and not args.quiet:
            first, last = chunk_indexes[0], chunk_indexes[-1]
            print(f"Submitting chunk {chunk_num} ({first}-{last}) label={batch_label}")
        applied_total += process_chunk(chunk_indexes, batch_label)
        if args.chunk_size and args.wait_for_batch and args.dry_run == "none" and batch_label:
            selector = f"wetlands/batch={batch_label}"
            if not args.quiet:
                print(f"Waiting for {selector} (timeout {args.wait_timeout})")
            run_kubectl(
                [
                    "wait",
                    "--for=condition=complete",
                    "job",
                    "-l",
                    selector,
                    f"--timeout={args.wait_timeout}",
                ],
                namespace=args.namespace,
            )

    if args.dry_run == "print":
        print(f"Generated {len(resolved_indexes)} manifests")
    else:
        print(f"Processed {len(resolved_indexes)} indexes; {applied_total} submissions")


if __name__ == "__main__":
    main()
