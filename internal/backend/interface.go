/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package backend defines the interface that backup backend implementations must satisfy,
// along with shared types used across backends.
package backend

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	omniv1alpha1 "github.com/smoothify/omnivol/api/v1alpha1"
)

// BackupExists reports whether at least one restic snapshot already exists in
// the backend store at the computed repository path.
type BackupExistsFunc func(ctx context.Context) (bool, error)

// Interface is the abstraction that omnivol uses to interact with a backup
// backend.  The VolSync backend is the first (and currently only) implementation.
type Interface interface {
	// Name returns a human-readable identifier for this backend (e.g. "volsync").
	Name() string

	// BackupExists checks whether a prior backup exists for the given PVC at the
	// repository path that would be computed from the policy + PVC annotations.
	BackupExists(ctx context.Context, repoPath string) (bool, error)

	// EnsureReplicationSource creates or updates the VolSync ReplicationSource
	// for the given underlying PVC according to the policy.
	EnsureReplicationSource(ctx context.Context, params EnsureParams) error

	// EnsureReplicationDestination creates or updates the VolSync
	// ReplicationDestination for a restore operation, waits until latestImage is
	// populated, and then deletes the destination.
	// Returns the VolumeSnapshot name that should be used as the data source for
	// the underlying PVC.
	EnsureReplicationDestination(ctx context.Context, params EnsureParams) (latestImage *corev1.TypedLocalObjectReference, err error)

	// TriggerFinalSync patches the ReplicationSource to fire a manual sync and
	// waits until that sync completes (or ctx is cancelled / deadline exceeded).
	TriggerFinalSync(ctx context.Context, rsName, namespace string) error

	// Cleanup removes all backend resources (ReplicationSource, ReplicationDestination,
	// restic Secret) for the given PVC.  It does NOT delete the underlying PVC.
	Cleanup(ctx context.Context, pvcName, namespace string) error
}

// EnsureParams bundles all information needed to reconcile backend resources
// for a single PVC.
type EnsureParams struct {
	// Client is a controller-runtime client scoped to the cluster.
	Client client.Client

	// Policy is the BackupPolicy that governs this PVC.
	Policy *omniv1alpha1.BackupPolicy

	// Store is the BackupStore referenced by the policy.
	Store *omniv1alpha1.BackupStore

	// UnderlyingPVC is the real openebs-lvm PVC (name: <pvcname>-omnivol).
	UnderlyingPVC *corev1.PersistentVolumeClaim

	// RepoPath is the computed restic repository path (may be overridden by annotation).
	RepoPath string

	// Schedule is the effective cron expression (policy default, staggered minute).
	Schedule string

	// ResticSecretName is the name of the restic credentials Secret to create/use.
	ResticSecretName string

	// UserPVCName is the name of the user-facing PVC (the virtual PVC backed by
	// this provisioner).  Used to set orphan-tracking annotations on managed resources.
	UserPVCName string

	// UserPVCNamespace is the namespace of the user-facing PVC.
	UserPVCNamespace string

	// ControllerNamespace is the namespace where the omnivol controller runs.
	// Credential secrets referenced by BackupStore are resolved in this namespace.
	ControllerNamespace string
}
