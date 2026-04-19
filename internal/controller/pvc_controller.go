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
	"maps"
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

	// annMigrating is set on PVCs while a migration is in progress.
	annMigrating = "omnivol.smoothify.com/migrating"

	// annPrivilegedMovers is the VolSync namespace annotation.
	pvcAnnPrivilegedMovers = "volsync.backube/privileged-movers"

	// schedulingGateName is the scheduling gate added to pods referencing
	// PVCs that are being restored.  The gate prevents the pod from being
	// scheduled until the restore is complete.
	schedulingGateName = "omnivol.smoothify.com/restore-pending"

	// pvcResyncPeriod is how often the PVC reconciler rescans managed PVCs.
	pvcResyncPeriod = 5 * time.Minute

	// freshSyncThreshold is the maximum age of the last sync for it to be
	// considered "fresh enough" to proceed with migration.
	freshSyncThreshold = 5 * time.Minute
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
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch
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
		// Re-reconcile managed PVCs when a Node becomes cordoned.
		Watches(
			&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(r.mapNodeToPVCs),
		).
		Complete(r)
}

// Reconcile handles PVC create/update/delete events.
func (r *PVCReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if r.Backend == nil {
		r.Backend = volsyncbackend.New(r.Client)
	}

	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, req.NamespacedName, pvc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Only process PVCs with the backup-policy label.
	policyName := pvc.Labels[labelBackupPolicy]
	if policyName == "" || pvc.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}

	// Wait for the PVC to be bound.
	if pvc.Status.Phase != corev1.ClaimBound {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Resolve backup context (policy, store, params).
	bctx, result, err := r.resolveBackupContext(ctx, pvc, policyName)
	if bctx == nil {
		return result, err
	}

	// Handle restore-on-create for new PVCs with existing S3 backups.
	if err := r.handleRestore(ctx, pvc, bctx.params); err != nil {
		return ctrl.Result{}, err
	}

	// Auto-migration: move PVC off cordoned nodes.
	if r.isAutoMigrateEnabled(pvc, bctx.policy) && bctx.nodeName != "" {
		if result, err, handled := r.checkMigration(ctx, pvc, bctx.params, bctx.nodeName); handled {
			return result, err
		}
	}

	// Ensure ReplicationSource exists for ongoing backups.
	if err := r.Backend.EnsureReplicationSource(ctx, bctx.params); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure ReplicationSource: %w", err)
	}

	// Manage scheduling gates for pods referencing this PVC.
	r.syncSchedulingGates(ctx, pvc)

	// Update BackupPolicy status.
	if err := r.updatePolicyStatus(ctx, policyName); err != nil {
		logger.Error(err, "Could not update BackupPolicy status", "policy", policyName)
	}

	logger.Info("Reconciled PVC backup", "pvc", pvc.Name, "policy", policyName, "schedule", bctx.params.Schedule)
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

// backupContext holds resolved policy, store, params, and node name.
type backupContext struct {
	policy   *omniv1alpha1.BackupPolicy
	params   backend.EnsureParams
	nodeName string
}

// resolveBackupContext resolves the BackupPolicy, BackupStore, and builds
// EnsureParams.  Returns nil bctx if the PVC cannot be processed yet.
func (r *PVCReconciler) resolveBackupContext(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
	policyName string,
) (*backupContext, ctrl.Result, error) {
	logger := log.FromContext(ctx)

	controllerNS := r.ControllerNamespace
	if controllerNS == "" {
		controllerNS = os.Getenv("POD_NAMESPACE")
		if controllerNS == "" {
			controllerNS = "omnivol-system"
		}
	}

	policy := &omniv1alpha1.BackupPolicy{}
	if err := r.Get(ctx, types.NamespacedName{Name: policyName}, policy); err != nil {
		logger.Error(err, "Could not resolve BackupPolicy", "policy", policyName, "pvc", pvc.Name)
		return nil, ctrl.Result{RequeueAfter: pvcResyncPeriod}, nil
	}

	store := &omniv1alpha1.BackupStore{}
	if err := r.Get(ctx, types.NamespacedName{Name: policy.Spec.BackupStore}, store); err != nil {
		logger.Error(err, "Could not resolve BackupStore", "store", policy.Spec.BackupStore, "pvc", pvc.Name)
		return nil, ctrl.Result{RequeueAfter: pvcResyncPeriod}, nil
	}

	if !meta.IsStatusConditionTrue(store.Status.Conditions, conditionReady) {
		logger.Info("BackupStore not ready, skipping", "store", store.Name, "pvc", pvc.Name)
		return nil, ctrl.Result{RequeueAfter: pvcResyncPeriod}, nil
	}

	repoPath := computeRepoPath(pvc, policy)
	schedule, err := stagger.ApplyStagger(effectiveSchedule(pvc, policy), pvc.UID)
	if err != nil {
		return nil, ctrl.Result{}, fmt.Errorf("compute staggered schedule: %w", err)
	}

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
			return nil, ctrl.Result{}, fmt.Errorf("ensure privileged-movers annotation: %w", err)
		}
	}

	return &backupContext{policy: policy, params: params, nodeName: nodeName}, ctrl.Result{}, nil
}

// handleRestore checks if a new PVC needs restoration from an existing S3
// backup and performs the restore if needed.
func (r *PVCReconciler) handleRestore(ctx context.Context, pvc *corev1.PersistentVolumeClaim, params backend.EnsureParams) error {
	logger := log.FromContext(ctx)

	// Only restore if no ReplicationSource exists yet (first reconcile).
	rs := &volsyncv1alpha1.ReplicationSource{}
	if err := r.Get(ctx, types.NamespacedName{Name: pvc.Name, Namespace: pvc.Namespace}, rs); err == nil {
		return nil // RS exists, not a new PVC
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	backupExists, err := volsyncbackend.S3CheckBackupExists(ctx, params, params.RepoPath)
	if err != nil {
		logger.Error(err, "Could not check S3 for existing backup", "pvc", pvc.Name)
		return nil // non-fatal
	}
	if !backupExists {
		return nil
	}

	logger.Info("Restoring from existing backup", "pvc", pvc.Name, "repoPath", params.RepoPath)

	// Mark PVC as restore-in-progress.
	if pvc.Annotations == nil || pvc.Annotations[annRestoreInProgress] != annValueTrue {
		patch := client.MergeFrom(pvc.DeepCopy())
		if pvc.Annotations == nil {
			pvc.Annotations = map[string]string{}
		}
		pvc.Annotations[annRestoreInProgress] = annValueTrue
		if err := r.Patch(ctx, pvc, patch); err != nil {
			return fmt.Errorf("annotate PVC restore-in-progress: %w", err)
		}
	}

	if _, err := r.Backend.EnsureReplicationDestination(ctx, params); err != nil {
		return fmt.Errorf("restore from backup: %w", err)
	}

	// Clear restore-in-progress annotation.
	patch := client.MergeFrom(pvc.DeepCopy())
	delete(pvc.Annotations, annRestoreInProgress)
	if err := r.Patch(ctx, pvc, patch); err != nil {
		return fmt.Errorf("remove restore-in-progress annotation: %w", err)
	}

	logger.Info("Restore complete", "pvc", pvc.Name)
	return nil
}

// checkMigration checks if the PVC's node is cordoned and initiates migration
// if so.  Returns (result, err, handled) — if handled is true the caller should
// return immediately.
func (r *PVCReconciler) checkMigration(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
	params backend.EnsureParams,
	nodeName string,
) (ctrl.Result, error, bool) {
	logger := log.FromContext(ctx)

	nodeObj := &corev1.Node{}
	if err := r.Get(ctx, types.NamespacedName{Name: nodeName}, nodeObj); err != nil {
		return ctrl.Result{}, nil, false
	}
	if !nodeObj.Spec.Unschedulable || pvc.Annotations[annMigrating] == annValueTrue {
		return ctrl.Result{}, nil, false
	}

	logger.Info("PVC on cordoned node — initiating migration", "pvc", pvc.Name, "node", nodeName)
	result, err := r.handleMigration(ctx, pvc, params, nodeName)
	if err != nil {
		logger.Error(err, "Migration failed", "pvc", pvc.Name)
	}
	return result, err, true
}

// syncSchedulingGates adds or removes scheduling gates from pods
// referencing this PVC based on restore state.
func (r *PVCReconciler) syncSchedulingGates(ctx context.Context, pvc *corev1.PersistentVolumeClaim) {
	logger := log.FromContext(ctx)
	if pvc.Annotations[annRestoreInProgress] == annValueTrue {
		if err := r.addSchedulingGates(ctx, pvc); err != nil {
			logger.Error(err, "Could not add scheduling gates", "pvc", pvc.Name)
		}
	} else {
		if err := r.removeSchedulingGates(ctx, pvc); err != nil {
			logger.Error(err, "Could not remove scheduling gates", "pvc", pvc.Name)
		}
	}
}

// ensurePrivilegedMoversAnnotation ensures the namespace has the
// volsync.backube/privileged-movers annotation set.
func (r *PVCReconciler) ensurePrivilegedMoversAnnotation(ctx context.Context, namespace string) error {
	ns := &corev1.Namespace{}
	if err := r.Get(ctx, types.NamespacedName{Name: namespace}, ns); err != nil {
		return fmt.Errorf("get namespace %q: %w", namespace, err)
	}

	if ns.Annotations != nil && ns.Annotations[pvcAnnPrivilegedMovers] == annValueTrue {
		return nil // already set
	}

	patch := client.MergeFrom(ns.DeepCopy())
	if ns.Annotations == nil {
		ns.Annotations = map[string]string{}
	}
	ns.Annotations[pvcAnnPrivilegedMovers] = annValueTrue
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

// mapNodeToPVCs returns reconcile requests for all managed PVCs whose PV
// is pinned to the changed Node.  This ensures PVCs are re-reconciled
// when a node is cordoned (for auto-migration).
func (r *PVCReconciler) mapNodeToPVCs(ctx context.Context, obj client.Object) []reconcile.Request {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return nil
	}

	// Only trigger for cordoned nodes.
	if !node.Spec.Unschedulable {
		return nil
	}

	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcList, client.HasLabels{labelBackupPolicy}); err != nil {
		return nil
	}

	var requests []reconcile.Request
	for _, pvc := range pvcList.Items {
		if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
			continue
		}
		// Check if the PV is pinned to this node.
		pv := &corev1.PersistentVolume{}
		if err := r.Get(ctx, types.NamespacedName{Name: pvc.Spec.VolumeName}, pv); err != nil {
			continue
		}
		if nodeNameFromPV(pv) == node.Name {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      pvc.Name,
					Namespace: pvc.Namespace,
				},
			})
		}
	}
	return requests
}

// isAutoMigrateEnabled checks whether auto-migration is enabled for the PVC.
// Per-PVC annotation overrides the BackupPolicy spec.
func (r *PVCReconciler) isAutoMigrateEnabled(pvc *corev1.PersistentVolumeClaim, policy *omniv1alpha1.BackupPolicy) bool {
	if v, ok := pvc.Annotations[annAutoMigrate]; ok {
		return v == annValueTrue
	}
	return policy.Spec.AutoMigrate
}

// handleMigration orchestrates the full migration flow:
// 1. Ensure last sync is fresh
// 2. Save PVC spec
// 3. Clean up backend resources (RS, RD, Secret)
// 4. Delete PVC (PV cascades via reclaim policy)
// 5. Recreate PVC with saved spec
// The next reconcile will detect the backup in S3 and restore.
func (r *PVCReconciler) handleMigration(ctx context.Context, pvc *corev1.PersistentVolumeClaim, params backend.EnsureParams, nodeName string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Step 1: Check if we have a fresh backup.
	rs := &volsyncv1alpha1.ReplicationSource{}
	if err := r.Get(ctx, types.NamespacedName{Name: pvc.Name, Namespace: pvc.Namespace}, rs); err != nil {
		if apierrors.IsNotFound(err) {
			// No RS yet — trigger a sync first, then requeue.
			logger.Info("No ReplicationSource found, creating before migration", "pvc", pvc.Name)
			if err := r.Backend.EnsureReplicationSource(ctx, params); err != nil {
				return ctrl.Result{}, fmt.Errorf("ensure RS before migration: %w", err)
			}
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		return ctrl.Result{}, err
	}

	// Check if the last sync is recent enough.
	if rs.Status.LastSyncTime == nil || time.Since(rs.Status.LastSyncTime.Time) > freshSyncThreshold {
		logger.Info("Triggering final sync before migration", "pvc", pvc.Name)
		if err := r.Backend.TriggerFinalSync(ctx, pvc.Name, pvc.Namespace); err != nil {
			logger.Error(err, "Could not trigger final sync for migration", "pvc", pvc.Name)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		// Sync triggered (and waited for) — continue with migration.
	}

	logger.Info("Backup is fresh — proceeding with migration", "pvc", pvc.Name, "node", nodeName)

	// Step 2: Save the PVC spec for recreation.
	savedPVC := savePVCSpec(pvc)

	// Step 3: Clean up backend resources before deleting the PVC.
	// This prevents the orphan controller from racing.
	if err := r.Backend.Cleanup(ctx, pvc.Name, pvc.Namespace); err != nil {
		logger.Error(err, "Could not cleanup backend resources before migration", "pvc", pvc.Name)
		// Continue anyway — the orphan controller will clean up.
	}

	// Step 4: Delete the old PVC.
	logger.Info("Deleting PVC for migration", "pvc", pvc.Name, "node", nodeName)
	if err := r.Delete(ctx, pvc); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("delete PVC for migration: %w", err)
	}

	// Step 5: Recreate PVC with saved spec (minus volumeName binding).
	logger.Info("Recreating PVC for migration", "pvc", savedPVC.Name)
	if err := r.Create(ctx, savedPVC); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// PVC was recreated by something else (e.g. StatefulSet) — that's fine.
			logger.Info("PVC already recreated", "pvc", savedPVC.Name)
		} else {
			return ctrl.Result{}, fmt.Errorf("recreate PVC: %w", err)
		}
	}

	// Next reconcile will detect the unbound PVC, find the backup in S3,
	// and restore automatically.
	logger.Info("Migration PVC recreated — restore will happen on next reconcile", "pvc", savedPVC.Name)
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

// savePVCSpec creates a new PVC object from the saved spec, stripping
// binding-specific fields so it can be recreated on a new node.
func savePVCSpec(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
	// Deep copy labels and annotations.
	labels := make(map[string]string, len(pvc.Labels))
	maps.Copy(labels, pvc.Labels)

	annotations := make(map[string]string, len(pvc.Annotations))
	maps.Copy(annotations, pvc.Annotations)
	// Remove transient annotations.
	delete(annotations, annRestoreInProgress)
	delete(annotations, annMigrating)
	delete(annotations, "volume.kubernetes.io/selected-node")

	newPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        pvc.Name,
			Namespace:   pvc.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: pvc.Spec.StorageClassName,
			AccessModes:      pvc.Spec.AccessModes,
			Resources:        pvc.Spec.Resources,
			// VolumeName is intentionally omitted — let the provisioner
			// create a new PV on whatever node the scheduler picks.
		},
	}

	return newPVC
}

// addSchedulingGates adds the omnivol scheduling gate to all pods that
// reference the given PVC and don't already have the gate.
func (r *PVCReconciler) addSchedulingGates(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	pods, err := r.podsReferencingPVC(ctx, pvc)
	if err != nil {
		return err
	}

	for i := range pods {
		pod := &pods[i]
		if hasSchedulingGate(pod) {
			continue
		}
		// Only gate pods that haven't been scheduled yet.
		if pod.Spec.NodeName != "" {
			continue
		}

		patch := client.MergeFrom(pod.DeepCopy())
		pod.Spec.SchedulingGates = append(pod.Spec.SchedulingGates, corev1.PodSchedulingGate{
			Name: schedulingGateName,
		})
		if err := r.Patch(ctx, pod, patch); err != nil {
			if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
				continue
			}
			return fmt.Errorf("add scheduling gate to pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	return nil
}

// removeSchedulingGates removes the omnivol scheduling gate from all pods
// that reference the given PVC.
func (r *PVCReconciler) removeSchedulingGates(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	pods, err := r.podsReferencingPVC(ctx, pvc)
	if err != nil {
		return err
	}

	for i := range pods {
		pod := &pods[i]
		if !hasSchedulingGate(pod) {
			continue
		}

		patch := client.MergeFrom(pod.DeepCopy())
		newGates := make([]corev1.PodSchedulingGate, 0, len(pod.Spec.SchedulingGates))
		for _, g := range pod.Spec.SchedulingGates {
			if g.Name != schedulingGateName {
				newGates = append(newGates, g)
			}
		}
		pod.Spec.SchedulingGates = newGates
		if err := r.Patch(ctx, pod, patch); err != nil {
			if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
				continue
			}
			return fmt.Errorf("remove scheduling gate from pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	return nil
}

// podsReferencingPVC returns all pods in the same namespace that mount the given PVC.
func (r *PVCReconciler) podsReferencingPVC(ctx context.Context, pvc *corev1.PersistentVolumeClaim) ([]corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(pvc.Namespace)); err != nil {
		return nil, err
	}

	var result []corev1.Pod
	for _, pod := range podList.Items {
		for _, vol := range pod.Spec.Volumes {
			if vol.PersistentVolumeClaim != nil && vol.PersistentVolumeClaim.ClaimName == pvc.Name {
				result = append(result, pod)
				break
			}
		}
	}
	return result, nil
}

// hasSchedulingGate returns true if the pod has the omnivol restore-pending gate.
func hasSchedulingGate(pod *corev1.Pod) bool {
	for _, g := range pod.Spec.SchedulingGates {
		if g.Name == schedulingGateName {
			return true
		}
	}
	return false
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
