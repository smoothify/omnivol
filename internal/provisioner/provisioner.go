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

// Package provisioner implements the sigs.k8s.io/sig-storage-lib-external-provisioner
// Provisioner interface for omnivol.
package provisioner

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	provisioner "sigs.k8s.io/sig-storage-lib-external-provisioner/v13/controller"

	omniv1alpha1 "github.com/smoothify/omnivol/api/v1alpha1"
	"github.com/smoothify/omnivol/internal/backend"
	volsyncbackend "github.com/smoothify/omnivol/internal/backend/volsync"
	"github.com/smoothify/omnivol/internal/stagger"
)

const (
	// ProvisionerName is the name registered in the StorageClass provisioner field.
	ProvisionerName = "omnivol.smoothify.com/provisioner"

	// Annotations applied to PVs and underlying PVCs.
	annUnderlyingPVC = "omnivol.smoothify.com/underlying-pvc"
	annUnderlyingNS  = "omnivol.smoothify.com/underlying-namespace"

	// StorageClass parameter key.
	paramBackupPolicy = "backupPolicy"

	// Annotation keys that users may set on PVCs.
	annScheduleOverride = "omnivol.smoothify.com/schedule"
	annRepoPathOverride = "omnivol.smoothify.com/repository-path"

	// Label applied to all omnivol-managed resources.
	labelManagedBy = "omnivol.smoothify.com/managed-by"

	// Suffix appended to the underlying PVC name.
	underlyingSuffix = "-omnivol"

	pollInterval = 5 * time.Second
	pollTimeout  = 10 * time.Minute
)

// OmnivolProvisioner implements the external provisioner interface.
type OmnivolProvisioner struct {
	client       client.Client
	backend      backend.Interface
	controllerNS string
}

// New creates a new OmnivolProvisioner.
func New(c client.Client, controllerNS string) *OmnivolProvisioner {
	return &OmnivolProvisioner{
		client:       c,
		backend:      volsyncbackend.New(c),
		controllerNS: controllerNS,
	}
}

// Provision is called by the external-provisioner controller when a new PVC
// needs a PV.  It returns ProvisioningInBackground while waiting for the
// underlying PVC to bind, and ProvisioningFinished once the PV is ready.
func (p *OmnivolProvisioner) Provision(ctx context.Context, options provisioner.ProvisionOptions) (*corev1.PersistentVolume, provisioner.ProvisioningState, error) {
	pvc := options.PVC
	sc := options.StorageClass

	// 1. Resolve BackupPolicy + BackupStore.
	policy, store, err := p.resolvePolicy(ctx, sc)
	if err != nil {
		return nil, provisioner.ProvisioningFinished, fmt.Errorf("resolve policy: %w", err)
	}

	underlyingName := pvc.Name + underlyingSuffix
	underlyingNS := pvc.Namespace

	// 2. Idempotency: check if underlying PVC already exists.
	underlyingPVC := &corev1.PersistentVolumeClaim{}
	err = p.client.Get(ctx, types.NamespacedName{Name: underlyingName, Namespace: underlyingNS}, underlyingPVC)
	if apierrors.IsNotFound(err) {
		// 3. Create underlying PVC.
		underlyingPVC, err = p.createUnderlyingPVC(ctx, pvc, policy, underlyingName)
		if err != nil {
			return nil, provisioner.ProvisioningFinished, fmt.Errorf("create underlying PVC: %w", err)
		}
	} else if err != nil {
		return nil, provisioner.ProvisioningFinished, fmt.Errorf("get underlying PVC: %w", err)
	}

	// 4. Wait for underlying PVC to bind.
	if underlyingPVC.Status.Phase != corev1.ClaimBound {
		return nil, provisioner.ProvisioningInBackground, nil
	}

	// 5. Read underlying PV to copy nodeAffinity + volumeHandle.
	underlyingPV := &corev1.PersistentVolume{}
	if err := p.client.Get(ctx, types.NamespacedName{Name: underlyingPVC.Spec.VolumeName}, underlyingPV); err != nil {
		return nil, provisioner.ProvisioningInBackground, nil // PV might not be visible yet
	}

	// 6. Build the restic Secret name and effective repo path.
	resticSecretName := "omnivol-" + pvc.Name
	repoPath := computeRepoPath(pvc, policy)

	// Compute staggered schedule.
	schedule, err := stagger.ApplyStagger(effectiveSchedule(pvc, policy), pvc.UID)
	if err != nil {
		return nil, provisioner.ProvisioningFinished, fmt.Errorf("compute schedule: %w", err)
	}

	params := backend.EnsureParams{
		Client:              p.client,
		Policy:              policy,
		Store:               store,
		UnderlyingPVC:       underlyingPVC,
		RepoPath:            repoPath,
		Schedule:            schedule,
		ResticSecretName:    resticSecretName,
		UserPVCName:         pvc.Name,
		UserPVCNamespace:    pvc.Namespace,
		ControllerNamespace: p.controllerNS,
	}

	// 7. Check S3 for existing backup.
	backupExists, err := volsyncbackend.S3CheckBackupExists(ctx, params, repoPath)
	if err != nil {
		// Non-fatal: treat as no backup so we proceed without restore.
		backupExists = false
	}

	// 8. Create ReplicationSource.
	if err := p.backend.EnsureReplicationSource(ctx, params); err != nil {
		return nil, provisioner.ProvisioningFinished, fmt.Errorf("ensure ReplicationSource: %w", err)
	}

	// 9. Restore if backup exists and restoreOnCreate is true.
	if backupExists && policy.Spec.RestoreOnCreate {
		_, err := p.backend.EnsureReplicationDestination(ctx, params)
		if err != nil {
			return nil, provisioner.ProvisioningFinished, fmt.Errorf("restore from backup: %w", err)
		}
	}

	// 10. Build and return the PV, copying nodeAffinity + volumeHandle from the
	//     underlying PV so that the user's pod can mount it directly.
	pv := p.buildPV(options, underlyingPVC, underlyingPV, underlyingName, underlyingNS)
	return pv, provisioner.ProvisioningFinished, nil
}

// Delete is called when a PV with our provisioner is released.
func (p *OmnivolProvisioner) Delete(ctx context.Context, pv *corev1.PersistentVolume) error {
	underlyingName := pv.Annotations[annUnderlyingPVC]
	underlyingNS := pv.Annotations[annUnderlyingNS]
	if underlyingName == "" {
		return nil // nothing to clean up
	}

	// 1. Trigger a final pre-delete backup and wait.
	deleteTimeout := p.resolveDeleteTimeout(ctx, pv)
	timeoutCtx, cancel := context.WithTimeout(ctx, deleteTimeout)
	defer cancel()

	if err := p.backend.TriggerFinalSync(timeoutCtx, underlyingName, underlyingNS); err != nil {
		// Log but do not block deletion.
		_ = err
	}

	// 2. Delete RS, RD, Secret.
	if err := p.backend.Cleanup(ctx, underlyingName, underlyingNS); err != nil {
		return fmt.Errorf("cleanup backend resources: %w", err)
	}

	// 3. Delete underlying PVC.
	underlyingPVC := &corev1.PersistentVolumeClaim{}
	if err := p.client.Get(ctx, types.NamespacedName{Name: underlyingName, Namespace: underlyingNS}, underlyingPVC); err == nil {
		if err := p.client.Delete(ctx, underlyingPVC); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete underlying PVC: %w", err)
		}
	}

	return nil
}

// resolvePolicy loads the BackupPolicy and BackupStore referenced by the StorageClass.
func (p *OmnivolProvisioner) resolvePolicy(ctx context.Context, sc *storagev1.StorageClass) (*omniv1alpha1.BackupPolicy, *omniv1alpha1.BackupStore, error) {
	policyName, ok := sc.Parameters[paramBackupPolicy]
	if !ok || policyName == "" {
		return nil, nil, fmt.Errorf("StorageClass %q missing required parameter %q", sc.Name, paramBackupPolicy)
	}

	policy := &omniv1alpha1.BackupPolicy{}
	if err := p.client.Get(ctx, types.NamespacedName{Name: policyName}, policy); err != nil {
		return nil, nil, fmt.Errorf("get BackupPolicy %q: %w", policyName, err)
	}

	store := &omniv1alpha1.BackupStore{}
	if err := p.client.Get(ctx, types.NamespacedName{Name: policy.Spec.BackupStore}, store); err != nil {
		return nil, nil, fmt.Errorf("get BackupStore %q: %w", policy.Spec.BackupStore, err)
	}

	return policy, store, nil
}

// createUnderlyingPVC creates the real openebs-lvm PVC named <pvcname>-omnivol.
func (p *OmnivolProvisioner) createUnderlyingPVC(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
	policy *omniv1alpha1.BackupPolicy,
	underlyingName string,
) (*corev1.PersistentVolumeClaim, error) {
	sc := policy.Spec.StorageClassName
	underlying := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      underlyingName,
			Namespace: pvc.Namespace,
			Labels: map[string]string{
				labelManagedBy:              "omnivol",
				"omnivol.smoothify.com/pvc": pvc.Name,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      pvc.Spec.AccessModes,
			StorageClassName: &sc,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: pvc.Spec.Resources.Requests[corev1.ResourceStorage],
				},
			},
		},
	}

	if pvc.Spec.VolumeMode != nil {
		underlying.Spec.VolumeMode = pvc.Spec.VolumeMode
	}

	if err := p.client.Create(ctx, underlying); err != nil && !apierrors.IsAlreadyExists(err) {
		return nil, err
	}

	// Re-fetch to get the server-populated fields.
	created := &corev1.PersistentVolumeClaim{}
	if err := p.client.Get(ctx, types.NamespacedName{Name: underlyingName, Namespace: pvc.Namespace}, created); err != nil {
		return nil, err
	}
	return created, nil
}

// buildPV constructs the PersistentVolume to return to the user's PVC.
// nodeAffinity and volumeHandle are copied from the underlying openebs-lvm PV
// so that the pod can mount the volume directly (no extra indirection).
func (p *OmnivolProvisioner) buildPV(
	options provisioner.ProvisionOptions,
	underlyingPVC *corev1.PersistentVolumeClaim,
	underlyingPV *corev1.PersistentVolume,
	underlyingName, underlyingNS string,
) *corev1.PersistentVolume {
	pvc := options.PVC
	capacity := underlyingPV.Spec.Capacity[corev1.ResourceStorage]
	if capacity.IsZero() {
		capacity = pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	}

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: options.PVName,
			Annotations: map[string]string{
				annUnderlyingPVC: underlyingName,
				annUnderlyingNS:  underlyingNS,
			},
			Labels: map[string]string{
				labelManagedBy: "omnivol",
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: pvc.Spec.AccessModes,
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: capacity,
			},
			PersistentVolumeReclaimPolicy: reclaimPolicyOrDefault(options.StorageClass.ReclaimPolicy),
			StorageClassName:              options.StorageClass.Name,
			MountOptions:                  options.StorageClass.MountOptions,
		},
	}

	// Copy volume mode.
	if pvc.Spec.VolumeMode != nil {
		pv.Spec.VolumeMode = pvc.Spec.VolumeMode
	}

	// Copy CSI source from underlying PV so pods mount the real volume.
	if underlyingPV.Spec.CSI != nil {
		pv.Spec.PersistentVolumeSource = corev1.PersistentVolumeSource{
			CSI: &corev1.CSIPersistentVolumeSource{
				Driver:           underlyingPV.Spec.CSI.Driver,
				VolumeHandle:     underlyingPV.Spec.CSI.VolumeHandle,
				FSType:           underlyingPV.Spec.CSI.FSType,
				VolumeAttributes: underlyingPV.Spec.CSI.VolumeAttributes,
			},
		}
	}

	// Copy node affinity from underlying PV.
	if underlyingPV.Spec.NodeAffinity != nil {
		pv.Spec.NodeAffinity = underlyingPV.Spec.NodeAffinity
	}

	return pv
}

// computeRepoPath returns the effective restic repository path for a PVC.
// If the PVC has the annotation omnivol.smoothify.com/repository-path set, that
// value is used; otherwise the path defaults to "<namespace>/<pvcname>".
func computeRepoPath(pvc *corev1.PersistentVolumeClaim, policy *omniv1alpha1.BackupPolicy) string {
	if v, ok := pvc.Annotations[annRepoPathOverride]; ok && v != "" {
		return v
	}
	if policy.Spec.RepositoryPath != "" {
		return fmt.Sprintf("%s/%s/%s", policy.Spec.RepositoryPath, pvc.Namespace, pvc.Name)
	}
	return fmt.Sprintf("%s/%s", pvc.Namespace, pvc.Name)
}

// effectiveSchedule returns the schedule to use for a PVC, respecting the
// per-PVC annotation override.
func effectiveSchedule(pvc *corev1.PersistentVolumeClaim, policy *omniv1alpha1.BackupPolicy) string {
	if v, ok := pvc.Annotations[annScheduleOverride]; ok && v != "" {
		return v
	}
	return policy.Spec.Schedule
}

// waitForBound polls until the PVC is bound.  Used in tests.
func waitForBound(ctx context.Context, c client.Client, name, namespace string) (*corev1.PersistentVolumeClaim, error) {
	var result *corev1.PersistentVolumeClaim
	err := wait.PollUntilContextTimeout(ctx, pollInterval, pollTimeout, true, func(ctx context.Context) (bool, error) {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, pvc); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		if pvc.Status.Phase == corev1.ClaimBound {
			result = pvc
			return true, nil
		}
		return false, nil
	})
	return result, err
}

// resourceFromStr is a small helper for tests.
func resourceFromStr(s string) resource.Quantity {
	return resource.MustParse(s)
}

// defaultDeleteTimeout is used when the BackupPolicy cannot be resolved or its
// deleteTimeout cannot be parsed.
const defaultDeleteTimeout = 5 * time.Minute

// resolveDeleteTimeout looks up the BackupPolicy via the PV's StorageClass to
// read the configured deleteTimeout.  Falls back to defaultDeleteTimeout on any
// error or when the PV/StorageClass/policy chain is broken.
func (p *OmnivolProvisioner) resolveDeleteTimeout(ctx context.Context, pv *corev1.PersistentVolume) time.Duration {
	scName := pv.Spec.StorageClassName
	if scName == "" {
		return defaultDeleteTimeout
	}

	sc := &storagev1.StorageClass{}
	if err := p.client.Get(ctx, types.NamespacedName{Name: scName}, sc); err != nil {
		return defaultDeleteTimeout
	}

	policyName, ok := sc.Parameters[paramBackupPolicy]
	if !ok || policyName == "" {
		return defaultDeleteTimeout
	}

	policy := &omniv1alpha1.BackupPolicy{}
	if err := p.client.Get(ctx, types.NamespacedName{Name: policyName}, policy); err != nil {
		return defaultDeleteTimeout
	}

	if policy.Spec.DeleteTimeout == nil {
		return defaultDeleteTimeout
	}

	return policy.Spec.DeleteTimeout.Duration
}

// reclaimPolicyOrDefault safely dereferences a *PersistentVolumeReclaimPolicy,
// defaulting to Delete if nil.
func reclaimPolicyOrDefault(p *corev1.PersistentVolumeReclaimPolicy) corev1.PersistentVolumeReclaimPolicy {
	if p == nil {
		return corev1.PersistentVolumeReclaimDelete
	}
	return *p
}
