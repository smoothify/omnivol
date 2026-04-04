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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BackupPolicySpec defines HOW and WHEN backups are taken for PVCs that
// reference a StorageClass backed by this policy.
type BackupPolicySpec struct {
	// backupStore is the name of the BackupStore (cluster-scoped) that provides
	// the S3 endpoint and credentials.
	// +required
	// +kubebuilder:validation:MinLength=1
	BackupStore string `json:"backupStore"`

	// schedule is the default cron expression for backups.
	// Individual PVCs may override this via the omnivol.smoothify.com/schedule annotation.
	// The minute field will be staggered deterministically based on the PVC UID —
	// any minute value provided here is replaced at reconcile time.
	// Example: "0 * * * *" (hourly), "0 2 * * *" (daily at 2am).
	// +required
	// +kubebuilder:validation:MinLength=1
	Schedule string `json:"schedule"`

	// retain defines how many snapshots to keep.
	// +required
	Retain RetainPolicy `json:"retain"`

	// copyMethod defines the VolSync copy method to use when creating snapshots.
	// Must be one of: None, Clone, Snapshot, Direct.
	// There is no default — this field is required.
	// +required
	// +kubebuilder:validation:Enum=None;Clone;Snapshot;Direct
	CopyMethod string `json:"copyMethod"`

	// restoreOnCreate controls whether the provisioner blocks PV binding until
	// a restore from the latest S3 backup completes (when a prior backup exists).
	// Defaults to true.
	// +optional
	// +kubebuilder:default=true
	RestoreOnCreate bool `json:"restoreOnCreate,omitempty"`

	// deleteTimeout is the maximum duration to wait for a final pre-delete backup
	// to complete before forcibly removing the PVC.  Defaults to 5 minutes if not set.
	// +optional
	DeleteTimeout *metav1.Duration `json:"deleteTimeout,omitempty"`

	// repositoryPath is the default restic repository path prefix inside the bucket.
	// Individual PVCs may override via the omnivol.smoothify.com/repository-path annotation.
	// If empty the path defaults to "/<pvcNamespace>/<pvcName>".
	// +optional
	RepositoryPath string `json:"repositoryPath,omitempty"`

	// moverSecurityContext overrides the PodSecurityContext applied to the
	// VolSync restic mover pod.  When omitted, VolSync uses its own defaults.
	//
	// The primary use-case is a shared NFS-backed volume whose subdirectories
	// are owned by a variety of UIDs (e.g. WordPress www-data/UID 33).  Running
	// the mover as root allows it to read and restore all files regardless of
	// owning UID:
	//
	//   moverSecurityContext:
	//     runAsUser: 0
	//
	// Note: container-level capabilities like DAC_READ_SEARCH require the
	// VolSync privileged-movers namespace annotation instead
	// (volsync.backube/privileged-movers: "true").
	// +optional
	MoverSecurityContext *corev1.PodSecurityContext `json:"moverSecurityContext,omitempty"`
}

// RetainPolicy defines how many periodic backup snapshots to keep.
type RetainPolicy struct {
	// hourly is the number of hourly snapshots to retain.
	// +optional
	Hourly *int32 `json:"hourly,omitempty"`

	// daily is the number of daily snapshots to retain.
	// +optional
	Daily *int32 `json:"daily,omitempty"`

	// weekly is the number of weekly snapshots to retain.
	// +optional
	Weekly *int32 `json:"weekly,omitempty"`

	// monthly is the number of monthly snapshots to retain.
	// +optional
	Monthly *int32 `json:"monthly,omitempty"`

	// yearly is the number of yearly snapshots to retain.
	// +optional
	Yearly *int32 `json:"yearly,omitempty"`
}

// BackupPolicyStatus defines the observed state of BackupPolicy.
type BackupPolicyStatus struct {
	// conditions represent the current state of the BackupPolicy resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// managedPVCCount is the number of user PVCs currently provisioned by this policy.
	// +optional
	ManagedPVCCount int32 `json:"managedPVCCount,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=bp
// +kubebuilder:printcolumn:name="Store",type=string,JSONPath=`.spec.backupStore`
// +kubebuilder:printcolumn:name="Schedule",type=string,JSONPath=`.spec.schedule`
// +kubebuilder:printcolumn:name="CopyMethod",type=string,JSONPath=`.spec.copyMethod`
// +kubebuilder:printcolumn:name="PVCs",type=integer,JSONPath=`.status.managedPVCCount`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// BackupPolicy is the Schema for the backuppolicies API.
// A BackupPolicy describes HOW and WHEN backups are taken, and which underlying
// StorageClass provides the real PV.  A StorageClass references a BackupPolicy
// via the "backupPolicy" parameter, tying PVC creation to this policy.
type BackupPolicy struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of BackupPolicy.
	// +required
	Spec BackupPolicySpec `json:"spec"`

	// status defines the observed state of BackupPolicy.
	// +optional
	Status BackupPolicyStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// BackupPolicyList contains a list of BackupPolicy.
type BackupPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []BackupPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&BackupPolicy{}, &BackupPolicyList{})
}
