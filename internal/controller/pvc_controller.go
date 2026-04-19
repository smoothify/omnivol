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

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	omniv1alpha1 "github.com/smoothify/omnivol/api/v1alpha1"
	"github.com/smoothify/omnivol/internal/backend"
	volsyncbackend "github.com/smoothify/omnivol/internal/backend/volsync"
	"github.com/smoothify/omnivol/internal/stagger"
)

const (
	// labelBackupPolicy is the label users apply to PVCs to opt into backup.
	labelBackupPolicy = "omnivol.smoothify.com/backup-policy"

	// annScheduleOverride allows per-PVC cron schedule overrides.
	annScheduleOverride = "omnivol.smoothify.com/schedule"

	// annRepoPathOverride allows per-PVC restic repo path overrides.
	annRepoPathOverride = "omnivol.smoothify.com/repository-path"

	// annAutoMigrate allows per-PVC override of the BackupPolicy autoMigrate field.
	annAutoMigrate = "omnivol.smoothify.com/auto-migrate"

	// annRestoreInProgress is set on PVCs while a restore is being performed.
	annRestoreInProgress = "omnivol.smoothify.com/restore-in-progress"

	// annPrivilegedMovers is the VolSync namespace annotation.
	pvcAnnPrivilegedMovers = "volsync.backube/privileged-movers"

	// pvcResyncPeriod is how often the PVC reconciler rescans managed PVCs.
	pvcResyncPeriod = 5 * time.Minute
)

// PVCReconciler watches PVCs with the omnivol.smoothify.com/backup-policy label
// and manages VolSync ReplicationSource resources for backup, and
// ReplicationDestination for restore.
//
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=omnivol.smoothify.com,resources=backuppolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=omnivol.smoothify.com,resources=backuppolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=omnivol.smoothify.com,resources=backupstores,verbs=get;list;watch
// +kubebuilder:rbac:groups=volsync.backube,resources=replicationsources,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=volsync.backube,resources=replicationdestinations,verbs=get;list;watch;create;update;patch;delete
type PVCReconciler struct {
	client.Client
	Backend             backend.Interface
	ControllerNamespace string
}

// SetupWithManager registers the PVCReconciler with the controller manager.
func (r *PVCReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("pvc-backup").
		// Watch all PVCs — filter in Reconcile based on label.
		For(&corev1.PersistentVolumeClaim{}).
		// Re-reconcile when BackupPolicy changes.
		Watches(
			&omniv1alpha1.BackupPolicy{},
			handler.EnqueueRequestsFromMapFunc(r.mapBackupPolicyToPVCs),
		).
		Complete(r)
}

// Reconcile handles PVC create/update/delete events.
func (r *PVCReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

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

	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, req.NamespacedName, pvc); err != nil {
		if apierrors.IsNotFound(err) {
			// PVC deleted — cleanup handled by orphan controller.
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Only process PVCs with the backup-policy label.
	policyName := pvc.Labels[labelBackupPolicy]
	if policyName == "" {
		return ctrl.Result{}, nil
	}

	// Skip PVCs being deleted.
	if pvc.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}

	// Wait for the PVC to be bound.
	if pvc.Status.Phase != corev1.ClaimBound {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Resolve BackupPolicy and BackupStore.
	policy := &omniv1alpha1.BackupPolicy{}
	if err := r.Get(ctx, types.NamespacedName{Name: policyName}, policy); err != nil {
		logger.Error(err, "Could not resolve BackupPolicy", "policy", policyName, "pvc", pvc.Name)
		return ctrl.Result{RequeueAfter: pvcResyncPeriod}, nil
	}

	store := &omniv1alpha1.BackupStore{}
	if err := r.Get(ctx, types.NamespacedName{Name: policy.Spec.BackupStore}, store); err != nil {
		logger.Error(err, "Could not resolve BackupStore", "store", policy.Spec.BackupStore, "pvc", pvc.Name)
		return ctrl.Result{RequeueAfter: pvcResyncPeriod}, nil
	}

	// Check if the BackupStore is Ready.
	storeReady := false
	for _, c := range store.Status.Conditions {
		if c.Type == conditionReady && c.Status == metav1.ConditionTrue {
			storeReady = true
			break
		}
	}
	if !storeReady {
		logger.Info("BackupStore not ready, skipping", "store", store.Name, "pvc", pvc.Name)
		return ctrl.Result{RequeueAfter: pvcResyncPeriod}, nil
	}

	// Compute parameters.
	repoPath := computeRepoPath(pvc, policy)
	schedule, err := stagger.ApplyStagger(effectiveSchedule(pvc, policy), pvc.UID)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("compute staggered schedule: %w", err)
	}

	// Resolve node name from PV.
	nodeName := r.nodeNameFromPVC(ctx, pvc)

	params := backend.EnsureParams{
		Client:              r.Client,
		Policy:              policy,
		Store:               store,
		PVC:                 pvc,
		RepoPath:            repoPath,
		Schedule:            schedule,
		ResticSecretName:    "omnivol-" + pvc.Name,
		ControllerNamespace: controllerNS,
		NodeName:            nodeName,
	}

	// Ensure the namespace is annotated for privileged movers if needed.
	if policy.Spec.MoverSecurityContext != nil {
		if err := r.ensurePrivilegedMoversAnnotation(ctx, pvc.Namespace); err != nil {
			return ctrl.Result{}, fmt.Errorf("ensure privileged-movers annotation: %w", err)
		}
	}

	// Check if this is a new PVC that might need restoration.
	// A PVC needs restore if:
	// 1. No ReplicationSource exists yet (first reconcile after PVC creation)
	// 2. A backup exists in S3
	rs := &volsyncv1alpha1.ReplicationSource{}
	rsExists := true
	if err := r.Get(ctx, types.NamespacedName{Name: pvc.Name, Namespace: pvc.Namespace}, rs); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		rsExists = false
	}

	if !rsExists {
		// Check S3 for existing backup.
		backupExists, err := volsyncbackend.S3CheckBackupExists(ctx, params, repoPath)
		if err != nil {
			// Non-fatal: treat as no backup so we proceed without restore.
			logger.Error(err, "Could not check S3 for existing backup", "pvc", pvc.Name)
			backupExists = false
		}

		if backupExists {
			logger.Info("Restoring from existing backup", "pvc", pvc.Name, "repoPath", repoPath)

			// Mark PVC as restore-in-progress.
			if pvc.Annotations == nil || pvc.Annotations[annRestoreInProgress] != "true" {
				patch := client.MergeFrom(pvc.DeepCopy())
				if pvc.Annotations == nil {
					pvc.Annotations = map[string]string{}
				}
				pvc.Annotations[annRestoreInProgress] = "true"
				if err := r.Patch(ctx, pvc, patch); err != nil {
					return ctrl.Result{}, fmt.Errorf("annotate PVC restore-in-progress: %w", err)
				}
			}

			_, err := r.Backend.EnsureReplicationDestination(ctx, params)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("restore from backup: %w", err)
			}

			// Clear restore-in-progress annotation.
			patch := client.MergeFrom(pvc.DeepCopy())
			delete(pvc.Annotations, annRestoreInProgress)
			if err := r.Patch(ctx, pvc, patch); err != nil {
				return ctrl.Result{}, fmt.Errorf("remove restore-in-progress annotation: %w", err)
			}

			logger.Info("Restore complete", "pvc", pvc.Name)
		}
	}

	// Ensure ReplicationSource exists for ongoing backups.
	if err := r.Backend.EnsureReplicationSource(ctx, params); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure ReplicationSource: %w", err)
	}

	// Update BackupPolicy status.
	if err := r.updatePolicyStatus(ctx, policyName); err != nil {
		logger.Error(err, "Could not update BackupPolicy status", "policy", policyName)
	}

	logger.Info("Reconciled PVC backup", "pvc", pvc.Name, "policy", policyName, "schedule", schedule)
	return ctrl.Result{RequeueAfter: pvcResyncPeriod}, nil
}

// nodeNameFromPVC resolves the node that the PVC's PV is bound to by reading
// the PV's node affinity.
func (r *PVCReconciler) nodeNameFromPVC(ctx context.Context, pvc *corev1.PersistentVolumeClaim) string {
	if pvc.Spec.VolumeName == "" {
		return ""
	}
	pv := &corev1.PersistentVolume{}
	if err := r.Get(ctx, types.NamespacedName{Name: pvc.Spec.VolumeName}, pv); err != nil {
		return ""
	}
	return nodeNameFromPV(pv)
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

// ensurePrivilegedMoversAnnotation ensures the namespace has the
// volsync.backube/privileged-movers annotation set to "true".
func (r *PVCReconciler) ensurePrivilegedMoversAnnotation(ctx context.Context, namespace string) error {
	ns := &corev1.Namespace{}
	if err := r.Get(ctx, types.NamespacedName{Name: namespace}, ns); err != nil {
		return fmt.Errorf("get namespace %q: %w", namespace, err)
	}

	if ns.Annotations != nil && ns.Annotations[pvcAnnPrivilegedMovers] == "true" {
		return nil // already set
	}

	patch := client.MergeFrom(ns.DeepCopy())
	if ns.Annotations == nil {
		ns.Annotations = map[string]string{}
	}
	ns.Annotations[pvcAnnPrivilegedMovers] = "true"
	if err := r.Patch(ctx, ns, patch); err != nil {
		return fmt.Errorf("patch namespace %q: %w", namespace, err)
	}

	return nil
}

// updatePolicyStatus counts managed PVCs and updates the BackupPolicy status.
func (r *PVCReconciler) updatePolicyStatus(ctx context.Context, policyName string) error {
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcList, client.MatchingLabels{labelBackupPolicy: policyName}); err != nil {
		return err
	}

	policy := &omniv1alpha1.BackupPolicy{}
	if err := r.Get(ctx, types.NamespacedName{Name: policyName}, policy); err != nil {
		return client.IgnoreNotFound(err)
	}

	count := int32(len(pvcList.Items))
	if policy.Status.ManagedPVCCount == count {
		return nil // no change
	}

	policy.Status.ManagedPVCCount = count
	meta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: policy.Generation,
		Reason:             "Reconciled",
		Message:            "BackupPolicy is active",
	})
	return r.Status().Update(ctx, policy)
}

// mapBackupPolicyToPVCs returns reconcile requests for all PVCs that reference
// the changed BackupPolicy via label.
func (r *PVCReconciler) mapBackupPolicyToPVCs(ctx context.Context, obj client.Object) []reconcile.Request {
	policy, ok := obj.(*omniv1alpha1.BackupPolicy)
	if !ok {
		return nil
	}

	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcList, client.MatchingLabels{labelBackupPolicy: policy.Name}); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0, len(pvcList.Items))
	for _, pvc := range pvcList.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      pvc.Name,
				Namespace: pvc.Namespace,
			},
		})
	}
	return requests
}

// computeRepoPath returns the effective restic repository path for a PVC.
func computeRepoPath(pvc *corev1.PersistentVolumeClaim, policy *omniv1alpha1.BackupPolicy) string {
	if v, ok := pvc.Annotations[annRepoPathOverride]; ok && v != "" {
		return v
	}
	if policy.Spec.RepositoryPath != "" {
		return fmt.Sprintf("%s/%s/%s", policy.Spec.RepositoryPath, pvc.Namespace, pvc.Name)
	}
	return fmt.Sprintf("%s/%s", pvc.Namespace, pvc.Name)
}

// effectiveSchedule returns the schedule to use for a PVC, respecting
// per-PVC annotation overrides.
func effectiveSchedule(pvc *corev1.PersistentVolumeClaim, policy *omniv1alpha1.BackupPolicy) string {
	if v, ok := pvc.Annotations[annScheduleOverride]; ok && v != "" {
		return v
	}
	return policy.Spec.Schedule
}
