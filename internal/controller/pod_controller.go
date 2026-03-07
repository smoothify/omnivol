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
	"strings"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// finalizerBackupProtection is added to Pods using Omnivol PVCs that have
	// backup-on-delete enabled.  It prevents the Pod from being fully deleted
	// until a final VolSync backup has completed.
	finalizerBackupProtection = "omnivol.smoothify.com/backup-protection"

	// annBackupOnDelete can be set on a Pod, PVC, or StorageClass to control
	// whether the backup-protection finalizer should be applied.
	// Values: "true" (opt-in), "false" (opt-out).
	// The most specific annotation wins: Pod > PVC > StorageClass.
	annBackupOnDelete = "omnivol.smoothify.com/backup-on-delete"

	// annDeletePVCOnBackup can be set on a Pod, PVC, or StorageClass to control
	// whether the PVC should be deleted after a successful final backup.
	// Values: "true" / "false". Hierarchy: Pod > PVC > StorageClass > controller default.
	annDeletePVCOnBackup = "omnivol.smoothify.com/delete-pvc-after-backup"

	// annValueTrue is the canonical "true" value for annotations.
	annValueTrue = "true"

	// underlyingSuffix is the suffix appended to PVC names for the underlying PVC.
	podUnderlyingSuffix = "-omnivol"

	// podRequeueInterval is how often we recheck whether the manual sync finished.
	podRequeueInterval = 5 * time.Second
)

// PodReconciler watches Pods that use Omnivol-managed PVCs.
// On creation it adds the backup-protection finalizer (if opt-in).
// On deletion it triggers a final VolSync manual sync and holds the Pod
// in Terminating until that sync completes.
//
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=pods/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=volsync.backube,resources=replicationsources,verbs=get;list;watch;update;patch
type PodReconciler struct {
	client.Client
	// DefaultDeletePVCAfterBackup is the controller-wide default for deleting
	// PVCs after a successful final backup. Can be overridden per StorageClass,
	// PVC, or Pod via the omnivol.smoothify.com/delete-pvc-after-backup annotation.
	DefaultDeletePVCAfterBackup bool
}

// SetupWithManager registers the PodReconciler with the manager.
func (r *PodReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("pod-backup-protection").
		For(&corev1.Pod{}).
		Complete(r)
}

// Reconcile handles Pod create/update/delete events.
func (r *PodReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	pod := &corev1.Pod{}
	if err := r.Get(ctx, req.NamespacedName, pod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Find all Omnivol PVC names used by this Pod.
	omnivolPVCNames := r.omnivolPVCsForPod(ctx, pod)
	if len(omnivolPVCNames) == 0 {
		// Not an Omnivol Pod — nothing to do.
		return ctrl.Result{}, nil
	}

	// Check if backup-on-delete is enabled for this Pod.
	enabled, err := r.isBackupOnDeleteEnabled(ctx, pod, omnivolPVCNames)
	if err != nil {
		return ctrl.Result{}, err
	}

	// --- Pod is NOT being deleted ---
	if pod.DeletionTimestamp.IsZero() {
		if enabled {
			// Add the finalizer if it's not already present.
			if controllerutil.AddFinalizer(pod, finalizerBackupProtection) {
				logger.Info("Adding backup-protection finalizer", "pod", pod.Name)
				if err := r.Update(ctx, pod); err != nil {
					return ctrl.Result{}, err
				}
			}
		} else {
			// Remove the finalizer if opt-out and it's present.
			if controllerutil.RemoveFinalizer(pod, finalizerBackupProtection) {
				logger.Info("Removing backup-protection finalizer (opt-out)", "pod", pod.Name)
				if err := r.Update(ctx, pod); err != nil {
					return ctrl.Result{}, err
				}
			}
		}
		return ctrl.Result{}, nil
	}

	// --- Pod IS being deleted ---

	// If we don't have the finalizer, let it go.
	if !controllerutil.ContainsFinalizer(pod, finalizerBackupProtection) {
		return ctrl.Result{}, nil
	}

	logger.Info("Pod terminating with backup-protection finalizer — triggering final sync",
		"pod", pod.Name, "namespace", pod.Namespace)

	// For each Omnivol PVC, trigger a manual sync and check completion.
	allComplete := true
	for _, pvcName := range omnivolPVCNames {
		rsName := pvcName + podUnderlyingSuffix
		complete, syncErr := r.ensureFinalSync(ctx, rsName, pod.Namespace)
		if syncErr != nil {
			logger.Error(syncErr, "Failed to trigger/check final sync", "replicationSource", rsName)
			return ctrl.Result{RequeueAfter: podRequeueInterval}, nil
		}
		if !complete {
			allComplete = false
		}
	}

	if !allComplete {
		logger.Info("Waiting for final sync to complete", "pod", pod.Name)
		return ctrl.Result{RequeueAfter: podRequeueInterval}, nil
	}

	// All syncs complete — now check if we should delete the user PVCs.
	for _, pvcName := range omnivolPVCNames {
		deleteEnabled, err := r.isDeletePVCOnBackupEnabled(ctx, pod, pvcName)
		if err != nil {
			logger.Error(err, "Failed to check if PVC should be deleted", "pvc", pvcName)
			continue
		}
		if deleteEnabled {
			logger.Info("Deleting PVC after final backup (opt-in)", "pvc", pvcName)
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: ctrl.ObjectMeta{
					Name:      pvcName,
					Namespace: pod.Namespace,
				},
			}
			if err := r.Delete(ctx, pvc); err != nil && !apierrors.IsNotFound(err) {
				logger.Error(err, "Failed to delete PVC", "pvc", pvcName)
			}
		}
	}

	// All syncs complete and PVCs handled — remove the finalizer.
	logger.Info("Final sync complete — removing backup-protection finalizer", "pod", pod.Name)
	controllerutil.RemoveFinalizer(pod, finalizerBackupProtection)
	if err := r.Update(ctx, pod); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// isDeletePVCOnBackupEnabled checks the annotation hierarchy: Pod > PVC > StorageClass > controller default.
func (r *PodReconciler) isDeletePVCOnBackupEnabled(ctx context.Context, pod *corev1.Pod, pvcName string) (bool, error) {
	// Check Pod annotation first (highest priority).
	if v, ok := pod.Annotations[annDeletePVCOnBackup]; ok {
		return v == annValueTrue, nil
	}

	// Check PVC annotation.
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: pod.Namespace}, pvc); err != nil {
		if apierrors.IsNotFound(err) {
			return r.DefaultDeletePVCAfterBackup, nil
		}
		return false, err
	}
	if v, ok := pvc.Annotations[annDeletePVCOnBackup]; ok {
		return v == annValueTrue, nil
	}

	// Check StorageClass annotation.
	if pvc.Spec.StorageClassName != nil {
		sc := &storagev1.StorageClass{}
		if err := r.Get(ctx, types.NamespacedName{Name: *pvc.Spec.StorageClassName}, sc); err == nil {
			if v, ok := sc.Annotations[annDeletePVCOnBackup]; ok {
				return v == annValueTrue, nil
			}
		}
	}

	// Fall back to controller-wide default.
	return r.DefaultDeletePVCAfterBackup, nil
}

// omnivolPVCsForPod returns the names of PVCs used by the Pod whose StorageClass
// is backed by the Omnivol provisioner.
func (r *PodReconciler) omnivolPVCsForPod(ctx context.Context, pod *corev1.Pod) []string {
	var names []string
	for _, vol := range pod.Spec.Volumes {
		if vol.PersistentVolumeClaim == nil {
			continue
		}
		pvcName := vol.PersistentVolumeClaim.ClaimName
		pvc := &corev1.PersistentVolumeClaim{}
		if err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: pod.Namespace}, pvc); err != nil {
			continue
		}
		if pvc.Spec.StorageClassName == nil {
			continue
		}
		sc := &storagev1.StorageClass{}
		if err := r.Get(ctx, types.NamespacedName{Name: *pvc.Spec.StorageClassName}, sc); err != nil {
			continue
		}
		if sc.Provisioner == "omnivol.smoothify.com/provisioner" {
			names = append(names, pvcName)
		}
	}
	return names
}

// isBackupOnDeleteEnabled checks the annotation hierarchy: Pod > PVC > StorageClass.
// Default is true (opt-in by default for Omnivol PVCs).
func (r *PodReconciler) isBackupOnDeleteEnabled(ctx context.Context, pod *corev1.Pod, pvcNames []string) (bool, error) {
	// Check Pod annotation first (highest priority).
	if v, ok := pod.Annotations[annBackupOnDelete]; ok {
		return v == annValueTrue, nil
	}

	// Check PVC annotations.
	for _, pvcName := range pvcNames {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: pod.Namespace}, pvc); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return false, err
		}
		if v, ok := pvc.Annotations[annBackupOnDelete]; ok {
			return v == annValueTrue, nil
		}
	}

	// Check StorageClass annotations.
	for _, pvcName := range pvcNames {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: pod.Namespace}, pvc); err != nil {
			continue
		}
		if pvc.Spec.StorageClassName == nil {
			continue
		}
		sc := &storagev1.StorageClass{}
		if err := r.Get(ctx, types.NamespacedName{Name: *pvc.Spec.StorageClassName}, sc); err != nil {
			continue
		}
		if v, ok := sc.Annotations[annBackupOnDelete]; ok {
			return v == annValueTrue, nil
		}
	}

	// Default: enabled for all Omnivol PVCs.
	return true, nil
}

// ensureFinalSync triggers a manual sync on the ReplicationSource if one hasn't
// been triggered yet, and returns (complete, error).
func (r *PodReconciler) ensureFinalSync(ctx context.Context, rsName, namespace string) (bool, error) {
	rs := &volsyncv1alpha1.ReplicationSource{}
	if err := r.Get(ctx, types.NamespacedName{Name: rsName, Namespace: namespace}, rs); err != nil {
		if apierrors.IsNotFound(err) {
			// No ReplicationSource means no backup was ever set up — skip.
			return true, nil
		}
		return false, err
	}

	triggerPrefix := "final-sync-"

	// Check if we already triggered a final sync.
	if rs.Spec.Trigger != nil && strings.HasPrefix(rs.Spec.Trigger.Manual, triggerPrefix) {
		// Check if the sync has completed.
		if rs.Status.LastManualSync == rs.Spec.Trigger.Manual {
			return true, nil
		}
		// Still in progress.
		return false, nil
	}

	// Trigger a new manual sync.
	triggerValue := fmt.Sprintf("%s%d", triggerPrefix, time.Now().Unix())
	patch := client.MergeFrom(rs.DeepCopy())
	if rs.Spec.Trigger == nil {
		rs.Spec.Trigger = &volsyncv1alpha1.ReplicationSourceTriggerSpec{}
	}
	rs.Spec.Trigger.Manual = triggerValue
	if err := r.Patch(ctx, rs, patch); err != nil {
		return false, fmt.Errorf("patch manual trigger on %s/%s: %w", namespace, rsName, err)
	}

	return false, nil
}
