# GitStratum User Manual

This guide walks you through deploying and operating a GitStratum cluster.

## Prerequisites

- Kubernetes cluster (v1.28+)
- `kubectl` configured
- Storage class with ReadWriteOnce support (preferably NVMe-backed)
- Helm 3.x (optional, for chart-based deployment)

## Quick Start

### 1. Install the Operator

```bash
kubectl apply -f https://github.com/gitstratum/gitstratum/releases/latest/download/operator.yaml
```

Verify the operator is running:

```bash
kubectl get pods -n gitstratum-system
```

### 2. Create a GitStratum Cluster

Create a file named `cluster.yaml`:

```yaml
apiVersion: gitstratum.io/v1
kind: GitStratumCluster
metadata:
  name: my-cluster
  namespace: default
spec:
  replication_factor: 3

  control_plane:
    replicas: 3
    resources:
      cpu: "2"
      memory: "4Gi"
      storage: "50Gi"
    storage_class: "standard"

  metadata:
    replicas: 3
    resources:
      cpu: "2"
      memory: "16Gi"
      storage: "200Gi"
    storage_class: "standard"
    replication_mode: Sync

  object_cluster:
    replicas: 6
    resources:
      cpu: "2"
      memory: "8Gi"
      storage: "500Gi"
    storage_class: "standard"

  frontend:
    replicas: 2
    resources:
      cpu: "1"
      memory: "4Gi"
    service:
      service_type: LoadBalancer
```

Apply it:

```bash
kubectl apply -f cluster.yaml
```

### 3. Monitor Cluster Status

```bash
kubectl get gitStratumcluster my-cluster -w
```

Wait for the `PHASE` to show `Running`:

```
NAME         PHASE        CP-READY   META-READY   OBJ-READY   FE-READY
my-cluster   Running      3/3        3/3          6/6         2/2
```

### 4. Get the Frontend Endpoint

```bash
kubectl get svc my-cluster-frontend
```

## Configuration Reference

### Control Plane

The control plane runs Raft consensus for cluster coordination.

| Field | Type | Description |
|-------|------|-------------|
| `replicas` | int | Number of nodes (3, 5, or 7 recommended) |
| `resources.cpu` | string | CPU request/limit |
| `resources.memory` | string | Memory request/limit |
| `resources.storage` | string | Storage for Raft logs |
| `storage_class` | string | Kubernetes StorageClass |

### Metadata Cluster

Stores commits, trees, and refs with partition-based routing.

| Field | Type | Description |
|-------|------|-------------|
| `replicas` | int | Number of nodes (1-50) |
| `replication_mode` | enum | `Sync` or `Async` |
| `resources.*` | - | Same as control plane |

### Object Cluster

Blob storage with consistent hashing.

| Field | Type | Description |
|-------|------|-------------|
| `replicas` | int | Number of nodes (1-100) |
| `auto_scaling.enabled` | bool | Enable HPA |
| `auto_scaling.min_replicas` | int | Minimum nodes |
| `auto_scaling.max_replicas` | int | Maximum nodes |
| `auto_scaling.target_disk_utilization` | int | Scale trigger (%) |

### Frontend

Stateless Git protocol servers.

| Field | Type | Description |
|-------|------|-------------|
| `replicas` | int | Number of pods (1-100) |
| `service.service_type` | enum | `ClusterIP`, `LoadBalancer`, `NodePort` |
| `auto_scaling.enabled` | bool | Enable HPA |
| `auto_scaling.target_cpu_utilization` | int | CPU target (%) |

## Creating Repositories

```yaml
apiVersion: gitstratum.io/v1
kind: GitRepository
metadata:
  name: my-repo
spec:
  owner: "myorg"
  name: "my-repo"
  visibility: Private
  size_limit_bytes: 10737418240  # 10GB
```

```bash
kubectl apply -f repo.yaml
```

## Creating Users

```yaml
apiVersion: gitstratum.io/v1
kind: GitUser
metadata:
  name: alice
spec:
  username: "alice"
  email: "alice@example.com"
  ssh_keys:
    - name: "laptop"
      public_key: "ssh-ed25519 AAAA..."
  rate_limits:
    requests_per_minute: 100
    concurrent_connections: 10
  repository_access:
    - repository: "myorg/my-repo"
      permissions: [read, write]
```

## Operations

### Scaling

Scale the object cluster:

```bash
kubectl patch gitStratumcluster my-cluster \
  --type merge \
  -p '{"spec":{"object_cluster":{"replicas":12}}}'
```

### Viewing Metrics

Each component exposes Prometheus metrics on port 9090:

```bash
kubectl port-forward svc/my-cluster-control-plane 9090:9090
curl http://localhost:9090/metrics
```

### Checking Logs

```bash
# Control plane logs
kubectl logs -l app=my-cluster-control-plane -f

# Frontend logs
kubectl logs -l app=my-cluster-frontend -f
```

### Backup

The control plane stores cluster state in Raft logs. Back up the PVCs:

```bash
kubectl get pvc -l app=my-cluster-control-plane
```

## Troubleshooting

### Cluster stuck in Provisioning

Check pod status:

```bash
kubectl get pods -l gitstratum.io/cluster=my-cluster
kubectl describe pod <pod-name>
```

### Frontend not accessible

Verify the service:

```bash
kubectl get svc my-cluster-frontend
kubectl get endpoints my-cluster-frontend
```

### Storage issues

Ensure your StorageClass supports the required access mode:

```bash
kubectl get storageclass
kubectl describe pvc <pvc-name>
```

## Production Recommendations

| Component | Min Replicas | CPU | Memory | Storage |
|-----------|--------------|-----|--------|---------|
| Control Plane | 3 | 4 | 8Gi | 100Gi |
| Metadata | 3 | 4 | 32Gi | 500Gi |
| Object | 6 | 4 | 16Gi | 1Ti |
| Frontend | 3 | 2 | 8Gi | - |

- Use NVMe-backed storage classes for best performance
- Enable auto-scaling for object and frontend tiers
- Set up monitoring with Prometheus/Grafana
- Configure pod anti-affinity for high availability
