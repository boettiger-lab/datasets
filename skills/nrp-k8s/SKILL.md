---
name: nrp-k8s-batch
description: >
  Run batch processing jobs on the NRP (National Research Platform) Nautilus Kubernetes
  cluster. Covers job creation, indexed completions for parallel processing, pod
  scheduling with opportunistic priority, GPU node avoidance, secret mounting, RBAC for
  workflow orchestration, and job monitoring. Use when creating or managing Kubernetes
  jobs on the NRP Nautilus cluster, or when the user mentions NRP, Nautilus, or needs to
  run batch workloads on a shared academic cluster.
license: Apache-2.0
compatibility: >
  Requires kubectl configured for the NRP Nautilus cluster (namespace: biodiversity).
  Works with any agent that can run shell commands.
metadata:
  author: boettiger-lab
  version: "1.0"
---

# NRP Kubernetes Batch Jobs

The NRP (National Research Platform) Nautilus cluster is a shared academic Kubernetes cluster. It is primarily designed for GPU workloads, so CPU-only batch jobs require specific configuration to coexist properly.

## Namespace

All jobs run in the `biodiversity` namespace:

```bash
kubectl -n biodiversity get jobs
```

Most commands below omit `-n biodiversity` — configure your default context or always include it.

## Job Template

Every batch job on NRP should include these elements:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: my-job
  labels:
    k8s-app: my-job
spec:
  completions: 1
  parallelism: 1
  backoffLimit: 2
  ttlSecondsAfterFinished: 10800    # Auto-cleanup after 3 hours
  template:
    metadata:
      labels:
        k8s-app: my-job
    spec:
      priorityClassName: opportunistic   # Required for CPU jobs
      affinity:                          # Avoid GPU nodes
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - key: feature.node.kubernetes.io/pci-10de.present
                    operator: NotIn
                    values: ["true"]
      restartPolicy: Never
      containers:
        - name: worker
          image: ghcr.io/boettiger-lab/datasets:latest
          imagePullPolicy: Always
          command: ["bash", "-c", "echo hello"]
          resources:
            requests:
              cpu: "4"
              memory: "8Gi"
            limits:
              cpu: "4"
              memory: "8Gi"
```

### Key fields explained

| Field | Purpose |
|-------|---------|
| `priorityClassName: opportunistic` | **Required for CPU jobs.** Makes pods preemptible so they don't block GPU users. |
| Node anti-affinity for `pci-10de.present` | Avoids scheduling on GPU nodes (NVIDIA device label). |
| `ttlSecondsAfterFinished: 10800` | Auto-deletes completed jobs after 3 hours to avoid resource leaks. |
| `restartPolicy: Never` | For indexed jobs; use `OnFailure` for simple retry semantics. |
| `backoffLimit` | Number of retries before marking job as failed. |

## Indexed (Parallel) Completions

For embarrassingly parallel workloads, use indexed completions. Each pod gets a unique index via `JOB_COMPLETION_INDEX`:

```yaml
spec:
  completions: 200          # Total number of chunks
  parallelism: 50           # Max concurrent pods
  completionMode: Indexed   # Each pod gets a unique index
  backoffLimitPerIndex: 3   # Retries per index (not global)
  podFailurePolicy:         # Don't count preemptions as failures
    rules:
      - action: Ignore
        onPodConditions:
          - type: DisruptionTarget
```

Inside the container, read the index:

```bash
echo "Processing chunk ${JOB_COMPLETION_INDEX}"
my-tool --chunk-id ${JOB_COMPLETION_INDEX} --chunk-size 500
```

### Limits

- **Max 200 completions per job** — Hard limit to avoid overwhelming the cluster's etcd.
- **Max ~50 parallelism** — Reduce if the cluster is busy.

## Secrets

Two secrets are available in the `biodiversity` namespace:

### `aws` — S3 credentials (environment variables)

```yaml
env:
  - name: AWS_ACCESS_KEY_ID
    valueFrom:
      secretKeyRef:
        name: aws
        key: AWS_ACCESS_KEY_ID
  - name: AWS_SECRET_ACCESS_KEY
    valueFrom:
      secretKeyRef:
        name: aws
        key: AWS_SECRET_ACCESS_KEY
```

### `rclone-config` — Rclone configuration (volume mount)

```yaml
volumeMounts:
  - name: rclone-config
    mountPath: /root/.config/rclone
    readOnly: true
volumes:
  - name: rclone-config
    secret:
      secretName: rclone-config
```

## Workflow Orchestration

For multi-step pipelines (e.g., convert → pmtiles → hex → repartition), use a workflow orchestrator job that applies and waits for sub-jobs:

### RBAC setup (one-time)

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: cng-datasets-workflow
  namespace: biodiversity
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: cng-datasets-workflow
  namespace: biodiversity
rules:
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["get", "list", "watch", "create", "delete"]
  - apiGroups: [""]
    resources: ["pods", "pods/log"]
    verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: cng-datasets-workflow
  namespace: biodiversity
subjects:
  - kind: ServiceAccount
    name: cng-datasets-workflow
roleRef:
  kind: Role
  name: cng-datasets-workflow
  apiGroup: rbac.authorization.k8s.io
```

### Orchestrator pattern

Store sub-job YAMLs in a ConfigMap, then run an orchestrator job that applies them sequentially:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: my-workflow
spec:
  template:
    spec:
      serviceAccountName: cng-datasets-workflow
      restartPolicy: OnFailure
      containers:
        - name: workflow
          image: bitnami/kubectl:latest
          command: ["bash", "-c"]
          args:
            - |
              set -e
              # Step 1
              kubectl apply -f /yamls/step1.yaml -n biodiversity
              kubectl wait --for=condition=complete --timeout=600s job/step1 -n biodiversity

              # Step 2 (parallel)
              kubectl apply -f /yamls/step2a.yaml -n biodiversity
              kubectl apply -f /yamls/step2b.yaml -n biodiversity
              kubectl wait --for=condition=complete --timeout=7200s job/step2a job/step2b -n biodiversity

              # Step 3
              kubectl apply -f /yamls/step3.yaml -n biodiversity
              kubectl wait --for=condition=complete --timeout=7200s job/step3 -n biodiversity
          volumeMounts:
            - name: yamls
              mountPath: /yamls
      volumes:
        - name: yamls
          configMap:
            name: my-workflow-yamls
```

## Monitoring

```bash
# List all jobs
kubectl get jobs | grep <name>

# Watch job progress
kubectl get jobs -w | grep <name>

# Check pod status (look for OOM, errors)
kubectl get pods | grep <name>

# View logs
kubectl logs job/<name>

# Follow logs in real-time
kubectl logs -f job/<name>

# Check for OOM kills
kubectl get pods | grep OOM

# Describe a pod for detailed status
kubectl describe pod <pod-name>
```

## Cleanup

```bash
# Delete a specific job
kubectl delete job <name>

# Delete all jobs matching a pattern
kubectl delete jobs -l k8s-app=<label>

# Jobs with ttlSecondsAfterFinished auto-delete after completion
```

## Common Patterns

### Ephemeral storage for large temporary files

Some jobs (e.g., PMTiles generation) need substantial scratch space:

```yaml
resources:
  requests:
    cpu: "4"
    memory: "32Gi"
    ephemeral-storage: "250Gi"
  limits:
    cpu: "4"
    memory: "32Gi"
```

### Container image

The standard image for geospatial processing:

```
ghcr.io/boettiger-lab/datasets:latest
```

This image includes GDAL, Python, tippecanoe, rclone, and the `cng-datasets` CLI.

### Environment variables for S3

See the [nrp-s3 skill](../nrp-s3/SKILL.md) for the complete set of S3-related environment variables needed in pod specs.

## Troubleshooting

| Problem | Diagnosis | Fix |
|---------|-----------|-----|
| Pod stuck in Pending | `kubectl describe pod <name>` — check events | Reduce resource requests or wait for capacity |
| OOMKilled | `kubectl get pods \| grep OOM` | Increase memory in resource limits |
| Job never completes | `kubectl logs job/<name>` | Check for errors in logs; may need more retries |
| Preempted frequently | Normal for `opportunistic` priority | Jobs will restart automatically (use `backoffLimitPerIndex` for indexed) |
| S3 SlowDown (503) | Transient Ceph throttling | Wait a few minutes and retry |
