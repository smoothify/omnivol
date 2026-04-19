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
	"sync"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	omniv1alpha1 "github.com/smoothify/omnivol/api/v1alpha1"
	volsyncbackend "github.com/smoothify/omnivol/internal/backend/volsync"
	omnimetrics "github.com/smoothify/omnivol/internal/metrics"
)

const (
	// metricsRequeueInterval is the fallback requeue period to refresh metrics
	// even when no ReplicationSource event fires.
	metricsRequeueInterval = 1 * time.Hour

	// sizeDebounceDuration prevents redundant S3 size checks if they were
	// already performed recently (e.g. rapid status updates on the same RS).
	sizeDebounceDuration = 5 * time.Minute
)

// sizeEntry caches the last time we queried S3 for a particular repo.
type sizeEntry struct {
	lastChecked time.Time
	bytes       int64
}

// MetricsReconciler watches Omnivol-managed ReplicationSources and updates
// Prometheus gauges for backup status, size and schedule adherence.
//
// +kubebuilder:rbac:groups=volsync.backube,resources=replicationsources,verbs=get;list;watch
// +kubebuilder:rbac:groups=omnivol.smoothify.com,resources=backuppolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=omnivol.smoothify.com,resources=backupstores,verbs=get;list;watch
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
type MetricsReconciler struct {
	client.Client

	// Namespace is the controller namespace where credential secrets live.
	Namespace string

	// mu protects lastSyncTimes and sizeCache.
	mu sync.Mutex
	// lastSyncTimes tracks the last observed LastSyncTime per RS so we can
	// detect newly completed backups.
	lastSyncTimes map[types.NamespacedName]metav1.Time
	// sizeCache stores the last S3 size check per RS to debounce calls.
	sizeCache map[types.NamespacedName]sizeEntry
}

// SetupWithManager registers the MetricsReconciler.
func (r *MetricsReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.lastSyncTimes = make(map[types.NamespacedName]metav1.Time)
	r.sizeCache = make(map[types.NamespacedName]sizeEntry)

	return ctrl.NewControllerManagedBy(mgr).
		Named("metrics").
		For(&volsyncv1alpha1.ReplicationSource{}).
		Complete(r)
}

// Reconcile inspects a single ReplicationSource and updates the Omnivol Prometheus gauges.
func (r *MetricsReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	rs := &volsyncv1alpha1.ReplicationSource{}
	if err := r.Get(ctx, req.NamespacedName, rs); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Only process Omnivol-managed ReplicationSources.
	if rs.Labels == nil || rs.Labels["omnivol.smoothify.com/managed-by"] != "omnivol" {
		return ctrl.Result{}, nil
	}

	// Extract the user PVC identity from annotations.
	userPVC := rs.Annotations[annOwnerPVCName]
	userNS := rs.Annotations[annOwnerPVCNamespace]
	if userPVC == "" || userNS == "" {
		return ctrl.Result{}, nil
	}

	// Resolve the BackupPolicy for this RS by walking from the underlying PVC
	// name back to the StorageClass parameters.
	policyName := r.resolvePolicyName(ctx, rs)

	// --- Backup status ---
	r.updateBackupStatus(rs, userPVC, userNS, policyName)

	// --- Duration ---
	r.updateBackupDuration(rs, userPVC, userNS, policyName)

	// --- Missed schedule ---
	r.updateMissedSchedule(rs, userPVC, userNS, policyName)

	// --- Last success timestamp ---
	r.updateLastSuccess(rs, userPVC, userNS, policyName)

	// --- Backup size (S3 check) ---
	r.maybeUpdateSize(ctx, logger, rs, req.NamespacedName, userPVC, userNS, policyName)

	// --- Volume capacity ---
	r.updateVolumeCapacity(ctx, userPVC, userNS, policyName)

	return ctrl.Result{RequeueAfter: metricsRequeueInterval}, nil
}

// updateBackupStatus sets omnivol_backup_status based on the VolSync conditions.
func (r *MetricsReconciler) updateBackupStatus(rs *volsyncv1alpha1.ReplicationSource, pvc, ns, policy string) {
	val := float64(0)
	if rs.Status.LatestMoverStatus != nil && rs.Status.LatestMoverStatus.Result == "Successful" {
		val = 1
	}
	omnimetrics.BackupStatus.WithLabelValues(pvc, ns, policy).Set(val)
}

// updateBackupDuration sets omnivol_backup_duration_seconds from LastSyncDuration.
func (r *MetricsReconciler) updateBackupDuration(rs *volsyncv1alpha1.ReplicationSource, pvc, ns, policy string) {
	if rs.Status.LastSyncDuration != nil {
		omnimetrics.BackupDurationSeconds.WithLabelValues(pvc, ns, policy).Set(rs.Status.LastSyncDuration.Seconds())
	}
}

// updateMissedSchedule sets omnivol_backup_missed_schedule.
// A schedule is considered missed when NextSyncTime has passed but LastSyncTime
// is still before the NextSyncTime.
func (r *MetricsReconciler) updateMissedSchedule(rs *volsyncv1alpha1.ReplicationSource, pvc, ns, policy string) {
	val := float64(0)

	// VolSync also exposes a "Synchronizing" condition — if it is False with
	// reason "MissedSchedule" we flag it directly.
	cond := meta.FindStatusCondition(rs.Status.Conditions, "Synchronizing")
	if cond != nil && cond.Status == metav1.ConditionFalse && cond.Reason == "MissedSchedule" {
		val = 1
	} else if rs.Status.NextSyncTime != nil {
		// Fallback heuristic: if next sync is in the past and the last sync is
		// older than it, the schedule was missed.
		now := time.Now()
		if rs.Status.NextSyncTime.Time.Before(now) {
			if rs.Status.LastSyncTime == nil || rs.Status.LastSyncTime.Time.Before(rs.Status.NextSyncTime.Time) {
				val = 1
			}
		}
	}

	omnimetrics.BackupMissedSchedule.WithLabelValues(pvc, ns, policy).Set(val)
}

// updateLastSuccess sets omnivol_backup_last_success_timestamp from LastSyncTime.
func (r *MetricsReconciler) updateLastSuccess(rs *volsyncv1alpha1.ReplicationSource, pvc, ns, policy string) {
	if rs.Status.LastSyncTime != nil {
		omnimetrics.BackupLastSuccessTimestamp.WithLabelValues(pvc, ns, policy).Set(float64(rs.Status.LastSyncTime.Unix()))
	}
}

// updateVolumeCapacity reads the user PVC's bound capacity and sets
// omnivol_volume_capacity_bytes.
func (r *MetricsReconciler) updateVolumeCapacity(ctx context.Context, pvc, ns, policy string) {
	uPVC := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Name: pvc, Namespace: ns}, uPVC); err != nil {
		return
	}
	if cap, ok := uPVC.Status.Capacity[corev1.ResourceStorage]; ok {
		omnimetrics.VolumeCapacityBytes.WithLabelValues(pvc, ns, policy).Set(float64(cap.Value()))
	}
}

// maybeUpdateSize queries S3 for the repository size when a new backup has
// completed (lastSyncTime changed) or when the cache has expired.
func (r *MetricsReconciler) maybeUpdateSize(
	ctx context.Context,
	logger interface{ Info(string, ...any) },
	rs *volsyncv1alpha1.ReplicationSource,
	nn types.NamespacedName,
	pvc, ns, policy string,
) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	cached, hasCached := r.sizeCache[nn]

	// Determine if a new backup just completed.
	newBackup := false
	if rs.Status.LastSyncTime != nil {
		prev, ok := r.lastSyncTimes[nn]
		if !ok || !prev.Equal(rs.Status.LastSyncTime) {
			newBackup = true
			r.lastSyncTimes[nn] = *rs.Status.LastSyncTime
		}
	}

	// Skip if debounce window hasn't elapsed and no new backup detected.
	if hasCached && !newBackup && now.Sub(cached.lastChecked) < sizeDebounceDuration {
		// Use cached value.
		omnimetrics.BackupSizeBytes.WithLabelValues(pvc, ns, policy).Set(float64(cached.bytes))
		return
	}

	// Resolve the BackupStore + repo path from the restic secret.
	store, repoPath, err := r.resolveStoreAndRepo(ctx, rs)
	if err != nil {
		logger.Info("Could not resolve BackupStore for size check", "error", err, "replicationSource", nn.String())
		return
	}

	size, err := volsyncbackend.S3CheckBackupSize(ctx, r.Client, store, r.Namespace, repoPath)
	if err != nil {
		logger.Info("S3 backup size check failed", "error", err, "replicationSource", nn.String())
		return
	}

	r.sizeCache[nn] = sizeEntry{lastChecked: now, bytes: size}
	omnimetrics.BackupSizeBytes.WithLabelValues(pvc, ns, policy).Set(float64(size))
}

// resolveStoreAndRepo extracts the BackupStore name and the restic repository
// path for a ReplicationSource by looking at its associated BackupPolicy and
// the owner-pvc annotations.
func (r *MetricsReconciler) resolveStoreAndRepo(ctx context.Context, rs *volsyncv1alpha1.ReplicationSource) (*omniv1alpha1.BackupStore, string, error) {
	policyName := r.resolvePolicyName(ctx, rs)
	if policyName == "" {
		return nil, "", nil
	}

	policy := &omniv1alpha1.BackupPolicy{}
	if err := r.Get(ctx, types.NamespacedName{Name: policyName}, policy); err != nil {
		return nil, "", err
	}

	store := &omniv1alpha1.BackupStore{}
	if err := r.Get(ctx, types.NamespacedName{Name: policy.Spec.BackupStore}, store); err != nil {
		return nil, "", err
	}

	// Compute the repo path: policy default or annotation override.
	userPVC := rs.Annotations[annOwnerPVCName]
	userNS := rs.Annotations[annOwnerPVCNamespace]
	repoPath := policy.Spec.RepositoryPath
	if repoPath == "" {
		repoPath = userNS + "/" + userPVC
	}

	return store, repoPath, nil
}

// resolvePolicyName looks up the BackupPolicy name for a ReplicationSource by
// reading the backup-policy label from the owner PVC.
func (r *MetricsReconciler) resolvePolicyName(ctx context.Context, rs *volsyncv1alpha1.ReplicationSource) string {
	// The user PVC name is stored in an annotation.
	userPVC := rs.Annotations[annOwnerPVCName]
	userNS := rs.Annotations[annOwnerPVCNamespace]
	if userPVC == "" || userNS == "" {
		return ""
	}

	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Name: userPVC, Namespace: userNS}, pvc); err != nil {
		return ""
	}
	return pvc.Labels[labelBackupPolicy]
}
