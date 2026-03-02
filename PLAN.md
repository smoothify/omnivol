# omnivol — Implementation Plan

> **Status**: In progress. This document is the authoritative design reference.
> Update it as decisions change; do not let it drift from the code.

---

## Identity

| | |
|---|---|
| **Module** | `github.com/smoothify/omnivol` |
| **API group** | `omnivol.smoothify.com/v1alpha1` |
| **Provisioner name** | `omnivol.smoothify.com/provisioner` |
| **Controller namespace** | `omnivol-system` |
| **Image** | `ghcr.io/smoothify/omnivol` |
| **Go version** | `1.25` |
| **License** | Apache 2.0 |

---

## What omnivol replaces

| Old component | Replacement |
|---|---|
| `pvc-plumber` HTTP sidecar | S3 check in `Provision()` directly |
| Kyverno `volsync-pvc-backup-restore` ClusterPolicy | Provisioner lifecycle |
| Kyverno `volsync-orphan-cleanup` ClusterCleanupPolicy | Owner references + orphan reconciler |
| `backup.*` section in `defaults.yaml` | `BackupStore` + `BackupPolicy` CRs |
| `backup: hourly/daily` PVC labels | `storageClassName: openebs-lvm-hourly` etc. |

What stays unchanged: the volsync operator, openebs-lvm, ExternalSecrets/Doppler for
credentials, and the three unrelated Kyverno policies (registry, pull secret, credential sync).

---

## CRD Design

### `BackupStore` (cluster-scoped)

Defines WHERE backups go and how to authenticate. Referenced by `BackupPolicy`.

```yaml
apiVersion: omnivol.smoothify.com/v1alpha1
kind: BackupStore
metadata:
  name: idrive-s3
spec:
  # Exactly one backend block must be set (CEL validation enforces this).
  restic:
    s3:
      endpoint: h7w5.fra.idrivee2-13.com
      bucket: volsync
      secure: true
    credentialsSecretRef:
      accessKeyID:
        name: omnivol-s3-credentials
        namespace: omnivol-system
        key: AWS_ACCESS_KEY_ID
      secretAccessKey:
        name: omnivol-s3-credentials
        namespace: omnivol-system
        key: AWS_SECRET_ACCESS_KEY
      resticPassword:
        name: omnivol-s3-credentials
        namespace: omnivol-system
        key: RESTIC_PASSWORD
  # Go template: available vars are .Namespace and .PVC
  # Default: "{{ .Namespace }}/{{ .PVC }}"
  repositoryPathTemplate: "{{ .Namespace }}/{{ .PVC }}"
status:
  conditions:
    - type: Ready
      status: "True"
```

**Future backend fields** (same struct, one must be set):
```yaml
spec:
  kopia: ...      # VolSync kopia mover
  rsyncTLS: ...   # VolSync rsync-tls mover
  syncthing: ...  # VolSync syncthing (continuous, no ReplicationDestination)
```

### `BackupPolicy` (cluster-scoped)

Defines HOW and WHEN to back up. Referenced by `StorageClass.parameters.backupPolicy`.

```yaml
apiVersion: omnivol.smoothify.com/v1alpha1
kind: BackupPolicy
metadata:
  name: hourly
spec:
  storeRef:
    name: idrive-s3

  # Cron schedule for ReplicationSource trigger.
  # Per-PVC override: annotation omnivol.smoothify.com/schedule
  # UID-based minute stagger is applied automatically (see Schedule Stagger below).
  schedule: "0 * * * *"

  # REQUIRED — no default. CEL validation error if absent.
  copyMethod: Snapshot

  # StorageClass for underlying (real) PVC and VolSync mover temp volumes.
  storageClassName: openebs-lvm
  volumeSnapshotClassName: openebs-lvm   # required when copyMethod=Snapshot

  cacheCapacity: 1Gi
  cacheStorageClassName: openebs-lvm

  pruneIntervalDays: 7

  retain:
    hourly: 24
    daily: 7
    weekly: 4
    monthly: 2

  moverSecurityContext:
    runAsUser: 568
    runAsGroup: 568
    fsGroup: 568

  # If true (default), Provision() blocks PV binding until restore completes.
  # Set false to skip restore entirely on new PVCs.
  restoreOnCreate: true

  # How long to wait for a final sync before proceeding with PVC deletion.
  # If the sync does not complete within this duration, deletion proceeds anyway.
  deleteTimeout: 5m

status:
  managedPVCCount: 0
  conditions: []
```

### `StorageClass` (user-created in flux-bootstrap)

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: openebs-lvm-hourly
provisioner: omnivol.smoothify.com/provisioner
parameters:
  backupPolicy: hourly
reclaimPolicy: Delete
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
```

### User PVC

No labels required. StorageClass selection is the only required change.
Optional annotation overrides:

```yaml
metadata:
  annotations:
    omnivol.smoothify.com/schedule: "30 2 * * *"         # override cron
    omnivol.smoothify.com/repository-path: "custom/path" # override S3 path
spec:
  storageClassName: openebs-lvm-hourly
```

---

## Naming Conventions

| Resource | Pattern | Example |
|---|---|---|
| Underlying PVC | `<pvcname>-omnivol` | `grafana-pvc-omnivol` |
| Restic Secret | `omnivol-<pvcname>` | `omnivol-grafana-pvc` |
| ReplicationSource | `<pvcname>-omnivol` | `grafana-pvc-omnivol` |
| ReplicationDestination | `<pvcname>-omnivol` | `grafana-pvc-omnivol` |
| Managed-by label | `omnivol.smoothify.com/managed-by: omnivol` | |
| PVC label (tracks policy) | `omnivol.smoothify.com/policy: hourly` | |

---

## Schedule Stagger

Identical to the current Kyverno policy approach. The schedule field in
`BackupPolicy.spec.schedule` uses a `*` wildcard in the minutes position (e.g. `0 * * * *`).
During `Provision()`, the minute field is replaced with a deterministic offset derived
from the PVC UID:

```
minuteOffset = parseInt(numericCharsOf(pvc.UID)[0:4]) % 60
finalCron    = replace(schedule, minuteField, minuteOffset)
```

If `omnivol.smoothify.com/schedule` annotation is present on the PVC, the annotation
value is used as-is with no stagger applied.

---

## Provisioner Lifecycle

### `Provision(ctx, opts)` — full step-by-step

1. Parse `opts.StorageClass.Parameters["backupPolicy"]` → look up `BackupPolicy`
2. Look up `BackupStore` via `policy.Spec.StoreRef.Name`
3. **Idempotency check**: if underlying PVC `<name>-omnivol` already exists, skip to step 6
4. **Create underlying PVC** `<name>-omnivol` in same namespace with the policy's
   `storageClassName` and `WaitForFirstConsumer` binding mode
5. **Wait for underlying PVC to bind** — return `ProvisioningInBackground` until bound;
   `kube-controller-manager` will re-call `Provision()` periodically
6. Read the underlying PV's `nodeAffinity` and `volumeHandle` from the bound PVC
7. **Build restic Secret** `omnivol-<pvcname>` in same namespace:
   - Read `accessKeyID`, `secretAccessKey`, `resticPassword` from cross-namespace
     secret references in `BackupStore.spec.restic.credentialsSecretRef`
   - Compute `RESTIC_REPOSITORY` from `repositoryPathTemplate` (or per-PVC annotation)
   - Create Secret if not exists; do not update if exists (credentials are stable)
8. **Check S3** for existing backup at the computed repository path (ported from pvc-plumber
   S3 client — list objects with `MaxKeys=1` under the prefix)
9. **Create `ReplicationSource`** `<pvcname>-omnivol` with:
   - `sourcePVC: <pvcname>-omnivol`
   - `trigger.schedule`: staggered cron (or annotation override, unstaggered)
   - Restic spec from `BackupPolicy`
   - Label `omnivol.smoothify.com/managed-by: omnivol`
   - Label `omnivol.smoothify.com/policy: <policyName>`
   - Owner reference → underlying PVC (so deletion cascades)
10. **If backup exists AND `policy.Spec.RestoreOnCreate == true`**:
    - Create `ReplicationDestination` `<pvcname>-omnivol` with:
      - `trigger.manual: restore-once`
      - `spec.restic.destinationPVC: <pvcname>-omnivol` (restore into the underlying PVC)
      - Owner reference → underlying PVC
    - Watch `ReplicationDestination.status.latestImage` — return `ProvisioningInBackground`
      until it is populated
    - Once populated, delete the `ReplicationDestination` (restore is complete)
11. **Return PV** with:
    - `spec.nodeAffinity` copied from underlying PV
    - `spec.csi` with `driver` and `volumeHandle` copied from underlying PV
    - `spec.storageClassName: <user StorageClass name>` (e.g. `openebs-lvm-hourly`)
    - `spec.claimRef` pointing to user's PVC
    - Annotations: `omnivol.smoothify.com/underlying-pvc: <name>-omnivol`
    - `persistentVolumeReclaimPolicy: Delete`

### `Delete(ctx, pv)`

1. Read `pv.Annotations["omnivol.smoothify.com/underlying-pvc"]` → underlying PVC name
2. Find `ReplicationSource` `<pvcname>-omnivol` in same namespace
3. **Trigger final sync**: patch `ReplicationSource.spec.trigger.manual = pre-delete-<timestamp>`
4. **Wait for sync**: watch `ReplicationSource.status.lastManualSyncTime` until it equals
   the manual trigger value, or until `BackupPolicy.spec.deleteTimeout` elapses
5. Delete `ReplicationSource` (if still exists)
6. Delete `ReplicationDestination` (if still exists — shouldn't be, but guard)
7. Delete Secret `omnivol-<pvcname>`
8. Delete underlying PVC `<pvcname>-omnivol`

---

## Controller Components

### External Provisioner (`internal/provisioner/`)

- Implements `sigs.k8s.io/sig-storage-lib-external-provisioner/v13` `Provisioner` interface
- Also implements `Qualifier` (`ShouldProvision`) — only act on PVCs whose StorageClass
  has `provisioner: omnivol.smoothify.com/provisioner`
- Leader election enabled (default in v13)
- Metrics on `:8080/metrics`

### BackupStore Controller (`internal/controller/backupstore_controller.go`)

- Reconciles `BackupStore` objects
- On create/update: validates S3 connectivity using the referenced secrets
- Writes `status.conditions` — `Ready: True` or `Ready: False` with reason
- No ongoing polling — purely event-driven (secrets are watched for changes)

### BackupPolicy Status Reconciler (`internal/controller/backuppolicy_controller.go`)

- Reconciles `BackupPolicy` objects
- Scans for PVCs whose StorageClass references this policy
- Updates `BackupPolicy.status.managedPVCCount`
- Runs on a 5-minute resync period and on BackupPolicy spec changes

### Orphan Reconciler (`internal/controller/orphan_controller.go`)

- Watches `ReplicationSource`, `ReplicationDestination`, `Secret` with label
  `omnivol.smoothify.com/managed-by: omnivol`
- On PVC delete event (or on resync): if the owner PVC no longer exists → delete the orphan
- Supplements owner references (which handle most cascades) for edge cases where
  owner reference cascade is blocked by namespace or policy

### Node Drain Watcher (`internal/drain/watcher.go`)

- Watches `Node` objects for `spec.unschedulable=true` (cordon)
- On cordon: finds all PVs managed by `omnivol.smoothify.com/provisioner` with
  `nodeAffinity` matching that node
- For each found PV: patches its `ReplicationSource.spec.trigger.manual = pre-drain-<timestamp>`
- Emits a Kubernetes `Event` on the Node with sync status
- Does NOT block the drain — it fires-and-forgets

---

## Backend Abstraction

Defined in `internal/backend/interface.go`. Allows non-VolSync backends in future.

```go
type BackupBackend interface {
    // BackupExists checks whether a backup exists for the given PVC.
    BackupExists(ctx context.Context, namespace, pvc string) (bool, error)

    // Provision creates all backup infrastructure for a newly provisioned PVC.
    Provision(ctx context.Context, params ProvisionParams) error

    // Delete triggers a final sync and removes all backup infrastructure.
    Delete(ctx context.Context, params DeleteParams) error

    // Name returns the backend identifier used in labels and log fields.
    Name() string
}
```

---

## Repository Layout

```
omnivol/
├── api/
│   └── v1alpha1/
│       ├── backupstore_types.go
│       ├── backuppolicy_types.go
│       ├── groupversion_info.go
│       └── zz_generated.deepcopy.go     # generated
├── internal/
│   ├── backend/
│   │   ├── interface.go
│   │   ├── volsync/
│   │   │   ├── backend.go
│   │   │   ├── replicationsource.go
│   │   │   ├── replicationdestination.go
│   │   │   └── secret.go
│   │   └── s3check/
│   │       ├── client.go
│   │       └── client_test.go
│   ├── controller/
│   │   ├── backupstore_controller.go
│   │   ├── backuppolicy_controller.go
│   │   ├── orphan_controller.go
│   │   └── suite_test.go
│   ├── provisioner/
│   │   ├── provisioner.go
│   │   ├── stagger.go
│   │   └── provisioner_test.go
│   └── drain/
│       └── watcher.go
├── cmd/
│   └── omnivol/
│       └── main.go
├── config/
│   ├── crd/bases/                       # generated
│   ├── rbac/                            # generated
│   ├── manager/
│   │   └── deployment.yaml
│   └── samples/
│       ├── backupstore.yaml
│       ├── backuppolicy.yaml
│       └── storageclass.yaml
├── hack/
│   ├── boilerplate.go.txt
│   └── setup-kind-cluster.sh
├── Dockerfile
├── Makefile
├── go.mod
├── go.sum
└── PROJECT
```

---

## Key Dependencies

| Package | Version | Purpose |
|---|---|---|
| `sigs.k8s.io/sig-storage-lib-external-provisioner/v13` | v13.0.0 | External provisioner framework |
| `sigs.k8s.io/controller-runtime` | v0.23.1 | Manager, reconcilers, fake client |
| `k8s.io/client-go` | v0.35.2 | Kubernetes API client |
| `k8s.io/api` | v0.35.2 | Core API types |
| `k8s.io/apimachinery` | v0.35.2 | Meta types, runtime |
| `github.com/minio/minio-go/v7` | latest | S3 prefix check |
| `sigs.k8s.io/controller-tools` | latest | controller-gen |

---

## Implementation Phases

### Phase 1 — Scaffold + plan doc
- [x] Write PLAN.md
- [ ] `go mod init` + kubebuilder scaffold
- [ ] Define Go types with controller-gen markers
- [ ] `make manifests generate`

### Phase 2 — Full provisioner skeleton
- [ ] `internal/backend/interface.go`
- [ ] `internal/backend/s3check/` (port from pvc-plumber)
- [ ] `internal/backend/volsync/` (resource builders)
- [ ] `internal/provisioner/stagger.go`
- [ ] `internal/provisioner/provisioner.go`
- [ ] Controllers (BackupStore, BackupPolicy, orphan, drain)
- [ ] `cmd/omnivol/main.go`
- [ ] Dockerfile + Makefile
- [ ] Unit tests

### Phase 3 — kind integration test setup
- [ ] `hack/setup-kind-cluster.sh`
- [ ] End-to-end smoke tests

### Phase 4 — flux-bootstrap cutover
- [ ] Add `kluctl/deployment/omnivol/`
- [ ] Remove pvc-plumber + Kyverno volsync policies
- [ ] Migrate 4 PVC files to new StorageClass
- [ ] Deploy to oke-ldn-2 test namespace → flip real workloads
