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

package controller

import (
	"context"
	"fmt"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	omniv1alpha1 "github.com/smoothify/omnivol/api/v1alpha1"
	"github.com/smoothify/omnivol/internal/backend"
	volsyncbackend "github.com/smoothify/omnivol/internal/backend/volsync"
	"github.com/smoothify/omnivol/internal/stagger"
)

const (
	// policyResyncPeriod is how often the BackupPolicy reconciler rescans managed
	// PVCs even without an explicit spec-change event.
	policyResyncPeriod = 5 * time.Minute

	paramBackupPolicy           = "backupPolicy"
	paramUnderlyingStorageClass = "underlyingStorageClass"
	annScheduleOverride         = "omnivol.smoothify.com/schedule"
	annRepoPathOverride         = "omnivol.smoothify.com/repository-path"
	annUnderlyingPVC            = "omnivol.smoothify.com/underlying-pvc"
	annUnderlyingNS             = "omnivol.smoothify.com/underlying-namespace"
	annSelectedNode             = "volume.kubernetes.io/selected-node"
	annPrivilegedMovers         = "volsync.backube/privileged-movers"
	underlyingSuffix            = "-omnivol"
)

// BackupPolicyReconciler reconciles BackupPolicy objects and maintains
// BackupPolicy.status.managedPVCCount.
//
// +kubebuilder:rbac:groups=omnivol.smoothify.com,resources=backuppolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=omnivol.smoothify.com,resources=backuppolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=omnivol.smoothify.com,resources=backuppolicies/finalizers,verbs=update
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=volsync.backube,resources=replicationsources,verbs=get;list;watch;create;update;patch;delete
type BackupPolicyReconciler struct {
	client.Client
	Backend             backend.Interface
	ControllerNamespace string
}

// SetupWithManager registers the BackupPolicyReconciler with the controller manager.
func (r *BackupPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&omniv1alpha1.BackupPolicy{}).
		Complete(r)
}

// Reconcile counts the PVCs managed by this BackupPolicy and updates status.
func (r *BackupPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	policy := &omniv1alpha1.BackupPolicy{}
	if err := r.Get(ctx, req.NamespacedName, policy); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	logger.Info("Reconciling BackupPolicy", "name", policy.Name)

	// Validate the referenced BackupStore exists and is Ready.
	store := &omniv1alpha1.BackupStore{}
	if err := r.Get(ctx, client.ObjectKey{Name: policy.Spec.BackupStore}, store); err != nil {
		current := &omniv1alpha1.BackupPolicy{}
		if getErr := r.Get(ctx, req.NamespacedName, current); getErr != nil {
			return ctrl.Result{}, client.IgnoreNotFound(getErr)
		}
		meta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
			Type:               conditionReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: current.Generation,
			Reason:             "BackupStoreNotFound",
			Message:            fmt.Sprintf("Referenced BackupStore %q not found", policy.Spec.BackupStore),
		})
		if updateErr := r.Status().Update(ctx, current); updateErr != nil {
			return ctrl.Result{}, updateErr
		}
		return ctrl.Result{RequeueAfter: policyResyncPeriod}, nil
	}

	// Check if the BackupStore is Ready.
	storeReady := false
	for _, c := range store.Status.Conditions {
		if c.Type == conditionReady && c.Status == metav1.ConditionTrue {
			storeReady = true
			break
		}
	}

	managedPVCs, err := r.listManagedPVCs(ctx, policy.Name)
	if err != nil {
		return ctrl.Result{}, err
	}
	count := len(managedPVCs)

	if storeReady {
		if err := r.reconcileManagedPVCBackups(ctx, policy, store, managedPVCs); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Re-fetch before status update to avoid conflicts.
	current := &omniv1alpha1.BackupPolicy{}
	if err := r.Get(ctx, req.NamespacedName, current); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if storeReady {
		meta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
			Type:               conditionReady,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: current.Generation,
			Reason:             "Reconciled",
			Message:            "BackupPolicy is active",
		})
	} else {
		meta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
			Type:               conditionReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: current.Generation,
			Reason:             "BackupStoreNotReady",
			Message:            fmt.Sprintf("Referenced BackupStore %q is not Ready", policy.Spec.BackupStore),
		})
	}
	current.Status.ManagedPVCCount = int32(count)

	if err := r.Status().Update(ctx, current); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("BackupPolicy reconciled", "name", policy.Name, "managedPVCCount", count, "storeReady", storeReady)

	// Resync periodically to keep the count up to date even without events.
	return ctrl.Result{RequeueAfter: policyResyncPeriod}, nil
}

// managedPVCRef ties a user PVC to the StorageClass that references its BackupPolicy.
type managedPVCRef struct {
	PVC *corev1.PersistentVolumeClaim
	SC  *storagev1.StorageClass
}

// listManagedPVCs returns PVCs whose StorageClass references policyName.
func (r *BackupPolicyReconciler) listManagedPVCs(ctx context.Context, policyName string) ([]managedPVCRef, error) {
	// Find all StorageClasses backed by our provisioner that reference this policy.
	scList := &storagev1.StorageClassList{}
	if err := r.List(ctx, scList); err != nil {
		return nil, err
	}

	scByName := map[string]*storagev1.StorageClass{}
	for i := range scList.Items {
		sc := &scList.Items[i]
		if sc.Provisioner == "omnivol.smoothify.com/provisioner" {
			if sc.Parameters[paramBackupPolicy] == policyName {
				scByName[sc.Name] = sc
			}
		}
	}
	if len(scByName) == 0 {
		return nil, nil
	}

	// Count user PVCs using those StorageClasses (excludes the -omnivol underlying PVCs,
	// which are on the real underlying StorageClass).
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcList); err != nil {
		return nil, err
	}

	managed := make([]managedPVCRef, 0)
	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]
		if pvc.Spec.StorageClassName == nil {
			continue
		}
		sc, ok := scByName[*pvc.Spec.StorageClassName]
		if !ok {
			continue
		}
		managed = append(managed, managedPVCRef{PVC: pvc, SC: sc})
	}
	return managed, nil
}

func (r *BackupPolicyReconciler) reconcileManagedPVCBackups(
	ctx context.Context,
	policy *omniv1alpha1.BackupPolicy,
	store *omniv1alpha1.BackupStore,
	managedPVCs []managedPVCRef,
) error {
	if r.Backend == nil {
		r.Backend = volsyncbackend.New(r.Client)
	}

	controllerNS := r.ControllerNamespace
	if controllerNS == "" {
		controllerNS = os.Getenv("POD_NAMESPACE")
		if controllerNS == "" {
			controllerNS = "omnivol-system"
		}
	}

	for _, ref := range managedPVCs {
		pvc := ref.PVC
		if pvc.DeletionTimestamp != nil || pvc.Status.Phase != corev1.ClaimBound {
			continue
		}

		if err := r.ensurePrivilegedMoversAnnotation(ctx, pvc.Namespace, policy); err != nil {
			return fmt.Errorf("ensure privileged movers annotation for namespace %q: %w", pvc.Namespace, err)
		}

		sourcePVC, underlyingSC, err := r.resolveSourcePVC(ctx, pvc, ref.SC)
		if err != nil {
			return fmt.Errorf("resolve source PVC for %s/%s: %w", pvc.Namespace, pvc.Name, err)
		}

		repoPath := computeRepoPath(pvc, policy)
		schedule, err := stagger.ApplyStagger(effectiveSchedule(pvc, policy), pvc.UID)
		if err != nil {
			return fmt.Errorf("compute staggered schedule for %s/%s: %w", pvc.Namespace, pvc.Name, err)
		}

		params := backend.EnsureParams{
			Client:                     r.Client,
			Policy:                     policy,
			Store:                      store,
			UnderlyingPVC:              sourcePVC,
			RepoPath:                   repoPath,
			Schedule:                   schedule,
			ResticSecretName:           "omnivol-" + pvc.Name,
			UserPVCName:                pvc.Name,
			UserPVCNamespace:           pvc.Namespace,
			ControllerNamespace:        controllerNS,
			UnderlyingStorageClassName: underlyingSC,
			NodeName:                   sourcePVC.Annotations[annSelectedNode],
		}

		if err := r.Backend.EnsureReplicationSource(ctx, params); err != nil {
			return fmt.Errorf("ensure ReplicationSource for %s/%s: %w", pvc.Namespace, pvc.Name, err)
		}
	}

	return nil
}

func (r *BackupPolicyReconciler) resolveSourcePVC(
	ctx context.Context,
	userPVC *corev1.PersistentVolumeClaim,
	sc *storagev1.StorageClass,
) (*corev1.PersistentVolumeClaim, string, error) {
	underlyingName := userPVC.Name + underlyingSuffix
	underlying := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Name: underlyingName, Namespace: userPVC.Namespace}, underlying)
	if err == nil {
		underlyingSC := sc.Parameters[paramUnderlyingStorageClass]
		if underlyingSC == "" && underlying.Spec.StorageClassName != nil {
			underlyingSC = *underlying.Spec.StorageClassName
		}
		if underlyingSC == "" {
			return nil, "", fmt.Errorf("no underlying StorageClass found for %s/%s", userPVC.Namespace, userPVC.Name)
		}
		return underlying, underlyingSC, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, "", err
	}

	if err := r.patchPVUnderlyingRefIfStale(ctx, userPVC); err != nil {
		return nil, "", err
	}

	underlyingSC := sc.Parameters[paramUnderlyingStorageClass]
	if underlyingSC == "" {
		if userPVC.Spec.StorageClassName != nil {
			underlyingSC = *userPVC.Spec.StorageClassName
		}
	}
	if underlyingSC == "" {
		return nil, "", fmt.Errorf("no underlying StorageClass found for %s/%s", userPVC.Namespace, userPVC.Name)
	}

	// Backward compatibility: clusters provisioned before the dedicated underlying
	// PVC naming change may not have <name>-omnivol objects. In that case, use the
	// user PVC itself as VolSync source.
	return userPVC.DeepCopy(), underlyingSC, nil
}

func (r *BackupPolicyReconciler) patchPVUnderlyingRefIfStale(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	if pvc.Spec.VolumeName == "" {
		return nil
	}

	pv := &corev1.PersistentVolume{}
	if err := r.Get(ctx, types.NamespacedName{Name: pvc.Spec.VolumeName}, pv); err != nil {
		return client.IgnoreNotFound(err)
	}

	ann := pv.GetAnnotations()
	if ann == nil {
		return nil
	}

	underlyingName := ann[annUnderlyingPVC]
	underlyingNS := ann[annUnderlyingNS]
	if underlyingName == "" {
		return nil
	}
	if underlyingNS == "" {
		underlyingNS = pvc.Namespace
	}

	underlyingPVC := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Name: underlyingName, Namespace: underlyingNS}, underlyingPVC)
	if err == nil || !apierrors.IsNotFound(err) {
		return client.IgnoreNotFound(err)
	}

	patch := client.MergeFrom(pv.DeepCopy())
	pv.Annotations[annUnderlyingPVC] = pvc.Name
	pv.Annotations[annUnderlyingNS] = pvc.Namespace
	if err := r.Patch(ctx, pv, patch); err != nil {
		return fmt.Errorf("patch PV %q underlying annotations: %w", pv.Name, err)
	}

	return nil
}

func (r *BackupPolicyReconciler) ensurePrivilegedMoversAnnotation(ctx context.Context, namespace string, policy *omniv1alpha1.BackupPolicy) error {
	if policy.Spec.MoverSecurityContext == nil {
		return nil
	}

	ns := &corev1.Namespace{}
	if err := r.Get(ctx, types.NamespacedName{Name: namespace}, ns); err != nil {
		return err
	}

	if ns.Annotations != nil && ns.Annotations[annPrivilegedMovers] == "true" {
		return nil
	}

	patch := client.MergeFrom(ns.DeepCopy())
	if ns.Annotations == nil {
		ns.Annotations = map[string]string{}
	}
	ns.Annotations[annPrivilegedMovers] = "true"
	return r.Patch(ctx, ns, patch)
}

func computeRepoPath(pvc *corev1.PersistentVolumeClaim, policy *omniv1alpha1.BackupPolicy) string {
	if v, ok := pvc.Annotations[annRepoPathOverride]; ok && v != "" {
		return v
	}
	if policy.Spec.RepositoryPath != "" {
		return fmt.Sprintf("%s/%s/%s", policy.Spec.RepositoryPath, pvc.Namespace, pvc.Name)
	}
	return fmt.Sprintf("%s/%s", pvc.Namespace, pvc.Name)
}

func effectiveSchedule(pvc *corev1.PersistentVolumeClaim, policy *omniv1alpha1.BackupPolicy) string {
	if v, ok := pvc.Annotations[annScheduleOverride]; ok && v != "" {
		return v
	}
	return policy.Spec.Schedule
}
