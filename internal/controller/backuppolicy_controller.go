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
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	omniv1alpha1 "github.com/smoothify/omnivol/api/v1alpha1"
)

const (
	// policyResyncPeriod is how often the BackupPolicy reconciler rescans managed
	// PVCs even without an explicit spec-change event.
	policyResyncPeriod = 5 * time.Minute
)

// BackupPolicyReconciler reconciles BackupPolicy objects and maintains
// BackupPolicy.status.managedPVCCount.
//
// +kubebuilder:rbac:groups=omnivol.smoothify.com,resources=backuppolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=omnivol.smoothify.com,resources=backuppolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=omnivol.smoothify.com,resources=backuppolicies/finalizers,verbs=update
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch
type BackupPolicyReconciler struct {
	client.Client
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

	count, err := r.countManagedPVCs(ctx, policy.Name)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Re-fetch before status update to avoid conflicts.
	current := &omniv1alpha1.BackupPolicy{}
	if err := r.Get(ctx, req.NamespacedName, current); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	meta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: current.Generation,
		Reason:             "Reconciled",
		Message:            "BackupPolicy is active",
	})
	current.Status.ManagedPVCCount = int32(count)

	if err := r.Status().Update(ctx, current); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("BackupPolicy reconciled", "name", policy.Name, "managedPVCCount", count)

	// Resync periodically to keep the count up to date even without events.
	return ctrl.Result{RequeueAfter: policyResyncPeriod}, nil
}

// countManagedPVCs returns the number of PVCs whose StorageClass references policyName.
func (r *BackupPolicyReconciler) countManagedPVCs(ctx context.Context, policyName string) (int, error) {
	// Find all StorageClasses backed by our provisioner that reference this policy.
	scList := &storagev1.StorageClassList{}
	if err := r.List(ctx, scList); err != nil {
		return 0, err
	}

	scNames := map[string]struct{}{}
	for _, sc := range scList.Items {
		if sc.Provisioner == "omnivol.smoothify.com/provisioner" {
			if sc.Parameters["backupPolicy"] == policyName {
				scNames[sc.Name] = struct{}{}
			}
		}
	}
	if len(scNames) == 0 {
		return 0, nil
	}

	// Count user PVCs using those StorageClasses (excludes the -omnivol underlying PVCs,
	// which are on the real openebs-lvm StorageClass).
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcList); err != nil {
		return 0, err
	}

	count := 0
	for _, pvc := range pvcList.Items {
		if pvc.Spec.StorageClassName == nil {
			continue
		}
		if _, ok := scNames[*pvc.Spec.StorageClassName]; ok {
			count++
		}
	}
	return count, nil
}
