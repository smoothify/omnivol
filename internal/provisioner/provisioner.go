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
	"k8s.io/klog/v2"
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

	// StorageClass parameter keys.
	paramBackupPolicy           = "backupPolicy"
	paramUnderlyingStorageClass = "underlyingStorageClass"

	// Annotation keys that users may set on PVCs.
	annScheduleOverride = "omnivol.smoothify.com/schedule"
	annRepoPathOverride = "omnivol.smoothify.com/repository-path"

	// Annotations for seeding from an existing backup
	annSeedPath  = "omnivol.smoothify.com/seed-path"
	annSeedStore = "omnivol.smoothify.com/seed-store"

	// Label applied to all omnivol-managed resources.
	labelManagedBy = "omnivol.smoothify.com/managed-by"

	// Suffix appended to the underlying PVC name.
	underlyingSuffix = "-omnivol"
)

// OmnivolProvisioner implements the external provisioner interface.
type OmnivolProvisioner struct {
	client       client.Client
	apiReader    client.Reader
	backend      backend.Interface
	controllerNS string
}

// New creates a new OmnivolProvisioner.  The apiReader should be an uncached
// reader (e.g. mgr.GetAPIReader()) so we can read the latest PVC annotations
// that may not yet be reflected in the informer cache.
func New(c client.Client, apiReader client.Reader, controllerNS string) *OmnivolProvisioner {
	return &OmnivolProvisioner{
		client:       c,
		apiReader:    apiReader,
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

	// Read the underlying StorageClass from the omnivol SC parameters.
	underlyingSC := sc.Parameters[paramUnderlyingStorageClass]
	if underlyingSC == "" {
		return nil, provisioner.ProvisioningFinished, fmt.Errorf("StorageClass %q missing required parameter %q", sc.Name, paramUnderlyingStorageClass)
	}

	// 2. Ensure underlying PVC exists, is pinned to the correct node, and is bound.
	underlyingNS := pvc.Namespace
	underlyingName := pvc.Name + underlyingSuffix
	underlyingPVC, err := p.ensureUnderlyingPVCBound(ctx, pvc, underlyingSC, underlyingName)
	if err != nil {
		return nil, provisioner.ProvisioningFinished, fmt.Errorf("ensure underlying PVC: %w", err)
	}

	// 3. Read underlying PV to copy nodeAffinity + volumeHandle.
	underlyingPV := &corev1.PersistentVolume{}
	err = wait.PollUntilContextTimeout(ctx, 2*time.Second, 1*time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := p.apiReader.Get(ctx, types.NamespacedName{Name: underlyingPVC.Spec.VolumeName}, underlyingPV); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		return true, nil
	})
	if err != nil {
		return nil, provisioner.ProvisioningFinished, fmt.Errorf("wait for underlying PV %s: %w", underlyingPVC.Spec.VolumeName, err)
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
		Client:                     p.client,
		Policy:                     policy,
		Store:                      store,
		UnderlyingPVC:              underlyingPVC,
		RepoPath:                   repoPath,
		Schedule:                   schedule,
		ResticSecretName:           resticSecretName,
		UserPVCName:                pvc.Name,
		UserPVCNamespace:           pvc.Namespace,
		ControllerNamespace:        p.controllerNS,
		UnderlyingStorageClassName: underlyingSC,
		NodeName:                   nodeNameFromPV(underlyingPV),
	}

	// Ensure the namespace is annotated for privileged movers if needed.
	if policy.Spec.MoverSecurityContext != nil {
		if err := p.ensurePrivilegedMoversAnnotation(ctx, pvc.Namespace); err != nil {
			return nil, provisioner.ProvisioningFinished, fmt.Errorf("ensure privileged-movers annotation: %w", err)
		}
	}

	// 7. Check S3 for existing backup.
	backupExists, err := volsyncbackend.S3CheckBackupExists(ctx, params, repoPath)
	if err != nil {
		// Non-fatal: treat as no backup so we proceed without restore.
		backupExists = false
	}

	// 8. Restore if backup exists or a seed is provided, before creating ReplicationSource.
	if backupExists && policy.Spec.RestoreOnCreate {
		_, err := p.backend.EnsureReplicationDestination(ctx, params)
		if err != nil {
			return nil, provisioner.ProvisioningFinished, fmt.Errorf("restore from backup: %w", err)
		}
	} else if !backupExists {
		// No own backup. Check if a seed source is requested.
		if seedPath := pvc.Annotations[annSeedPath]; seedPath != "" {
			klog.InfoS("Restoring from seed source", "pvc", pvc.Name, "namespace", pvc.Namespace, "seedPath", seedPath)

			// Resolve seed store: either from annotation or fallback to the policy's store.
			seedStore := store
			if storeName := pvc.Annotations[annSeedStore]; storeName != "" {
				seedStore = &omniv1alpha1.BackupStore{}
				if err := p.client.Get(ctx, types.NamespacedName{Name: storeName}, seedStore); err != nil {
					return nil, provisioner.ProvisioningFinished, fmt.Errorf("get seed BackupStore %q: %w", storeName, err)
				}
			}

			// Construct temporary parameters for the seed restore.
			seedParams := params
			seedParams.Store = seedStore
			seedParams.RepoPath = seedPath
			seedParams.ResticSecretName = params.ResticSecretName + "-seed"

			// Ensure the cleanup of the temporary seed credential secret even if restore fails
			defer func() {
				seedSecret := &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      seedParams.ResticSecretName,
						Namespace: seedParams.UnderlyingPVC.Namespace,
					},
				}
				if err := p.client.Delete(ctx, seedSecret); err != nil && !apierrors.IsNotFound(err) {
					klog.ErrorS(err, "failed to cleanup temporary seed restic secret", "secret", seedSecret.Name)
				}
			}()

			seedExists, err := volsyncbackend.S3CheckBackupExists(ctx, seedParams, seedPath)
			if err != nil {
				klog.ErrorS(err, "failed to check S3 for seed backup", "seedPath", seedPath)
			} else if seedExists {
				_, err := p.backend.EnsureReplicationDestination(ctx, seedParams)
				if err != nil {
					return nil, provisioner.ProvisioningFinished, fmt.Errorf("restore from seed source: %w", err)
				}
			} else {
				klog.InfoS("Seed backup not found in S3, skipping restore", "seedPath", seedPath)
			}
		}
	}

	// 9. Create ReplicationSource.
	if err := p.backend.EnsureReplicationSource(ctx, params); err != nil {
		return nil, provisioner.ProvisioningFinished, fmt.Errorf("ensure ReplicationSource: %w", err)
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

// ensurePrivilegedMoversAnnotation ensures the namespace has the
// volsync.backube/privileged-movers annotation set to "true".  This is required
// when the BackupPolicy specifies a custom MoverSecurityContext (e.g. runAsUser: 0)
// so that VolSync allows the mover pod to run with the requested context.
func (p *OmnivolProvisioner) ensurePrivilegedMoversAnnotation(ctx context.Context, namespace string) error {
	const annPrivilegedMovers = "volsync.backube/privileged-movers"

	ns := &corev1.Namespace{}
	if err := p.client.Get(ctx, types.NamespacedName{Name: namespace}, ns); err != nil {
		return fmt.Errorf("get namespace %q: %w", namespace, err)
	}

	if ns.Annotations != nil && ns.Annotations[annPrivilegedMovers] == "true" {
		return nil // already set
	}

	patch := client.MergeFrom(ns.DeepCopy())
	if ns.Annotations == nil {
		ns.Annotations = map[string]string{}
	}
	ns.Annotations[annPrivilegedMovers] = "true"
	if err := p.client.Patch(ctx, ns, patch); err != nil {
		return fmt.Errorf("patch namespace %q: %w", namespace, err)
	}

	klog.InfoS("Annotated namespace for privileged movers", "namespace", namespace)
	return nil
}

// ensureUnderlyingPVCBound gets or creates the underlying PVC, patches the selected-node annotation, and waits for it to bind.
func (p *OmnivolProvisioner) ensureUnderlyingPVCBound(ctx context.Context, pvc *corev1.PersistentVolumeClaim, underlyingSC string, underlyingName string) (*corev1.PersistentVolumeClaim, error) {
	underlyingNS := pvc.Namespace
	underlyingPVC := &corev1.PersistentVolumeClaim{}

	err := p.client.Get(ctx, types.NamespacedName{Name: underlyingName, Namespace: underlyingNS}, underlyingPVC)
	if apierrors.IsNotFound(err) {
		underlyingPVC, err = p.createUnderlyingPVC(ctx, pvc, underlyingSC, underlyingName)
		if err != nil {
			return nil, fmt.Errorf("create underlying PVC: %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("get underlying PVC: %w", err)
	}

	// Propagate selected-node annotation if the scheduler has set it on the user PVC but it's missing from the underlying PVC.
	const annSelectedNode = "volume.kubernetes.io/selected-node"
	freshPVC := &corev1.PersistentVolumeClaim{}
	if err := p.apiReader.Get(ctx, types.NamespacedName{Name: pvc.Name, Namespace: pvc.Namespace}, freshPVC); err == nil {
		selectedNode := freshPVC.Annotations[annSelectedNode]
		klog.InfoS("selected-node check",
			"userPVC", pvc.Name,
			"selectedNode", selectedNode,
			"underlyingAnnotations", underlyingPVC.Annotations)
		if selectedNode != "" {
			if underlyingPVC.Annotations == nil || underlyingPVC.Annotations[annSelectedNode] != selectedNode {
				patch := client.MergeFrom(underlyingPVC.DeepCopy())
				if underlyingPVC.Annotations == nil {
					underlyingPVC.Annotations = map[string]string{}
				}
				underlyingPVC.Annotations[annSelectedNode] = selectedNode
				if err := p.client.Patch(ctx, underlyingPVC, patch); err != nil {
					klog.ErrorS(err, "failed to patch selected-node on underlying PVC")
					return nil, fmt.Errorf("patch selected-node on underlying PVC: %w", err)
				}
				klog.InfoS("patched selected-node on underlying PVC",
					"selectedNode", selectedNode,
					"underlyingPVC", underlyingPVC.Name)
			}
		}
	} else {
		klog.ErrorS(err, "failed to read user PVC from API server", "namespace", pvc.Namespace, "name", pvc.Name)
	}

	// Wait for underlying PVC to bind.
	err = wait.PollUntilContextTimeout(ctx, 2*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
		current := &corev1.PersistentVolumeClaim{}
		if err := p.apiReader.Get(ctx, types.NamespacedName{Name: underlyingName, Namespace: underlyingNS}, current); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		if current.Status.Phase == corev1.ClaimBound {
			underlyingPVC = current
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return nil, fmt.Errorf("wait for underlying PVC %s/%s to bind: %w", underlyingNS, underlyingName, err)
	}

	return underlyingPVC, nil
}

// createUnderlyingPVC creates the real underlying PVC named <pvcname>-omnivol.
func (p *OmnivolProvisioner) createUnderlyingPVC(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
	underlyingSC string,
	underlyingName string,
) (*corev1.PersistentVolumeClaim, error) {
	sc := underlyingSC
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

	// Propagate the selected-node annotation so that WaitForFirstConsumer
	// StorageClasses bind the underlying PVC to the correct node.
	if selectedNode := pvc.Annotations["volume.kubernetes.io/selected-node"]; selectedNode != "" {
		underlying.Annotations = map[string]string{
			"volume.kubernetes.io/selected-node": selectedNode,
		}
	}

	if pvc.Spec.VolumeMode != nil {
		underlying.Spec.VolumeMode = pvc.Spec.VolumeMode
	}

	if err := p.client.Create(ctx, underlying); err != nil && !apierrors.IsAlreadyExists(err) {
		return nil, err
	}

	// Re-fetch to get the server-populated fields directly from apiserver
	// to avoid a cache race where client.Get returns NotFound.
	created := &corev1.PersistentVolumeClaim{}
	if err := p.apiReader.Get(ctx, types.NamespacedName{Name: underlyingName, Namespace: pvc.Namespace}, created); err != nil {
		return nil, err
	}
	return created, nil
}

// buildPV constructs the PersistentVolume to return to the user's PVC.
// nodeAffinity and volumeHandle are copied from the underlying PV
// so that the pod can mount the volume directly (no extra indirection).
func (p *OmnivolProvisioner) buildPV(
	options provisioner.ProvisionOptions,
	_ *corev1.PersistentVolumeClaim,
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

// nodeNameFromPV extracts the node name from a PV's nodeAffinity.
// It returns the first value from the first In-type match expression,
// regardless of the topology key (works with openebs.io/nodename,
// topology.topolvm.io/node, kubernetes.io/hostname, etc.).
func nodeNameFromPV(pv *corev1.PersistentVolume) string {
	if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
		return ""
	}
	for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
		for _, expr := range term.MatchExpressions {
			if expr.Operator == corev1.NodeSelectorOpIn && len(expr.Values) > 0 {
				return expr.Values[0]
			}
		}
	}
	return ""
}
