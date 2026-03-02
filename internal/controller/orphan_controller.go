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

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	labelManagedBy      = "omnivol.smoothify.com/managed-by"
	labelManagedByValue = "omnivol"

	// annOwnerPVC is the annotation we set on omnivol-managed resources to
	// record the underlying PVC (name/namespace) that owns them.
	// We use this instead of (or in addition to) owner references because
	// cross-namespace owner references are not supported by the Kubernetes GC.
	annOwnerPVCName      = "omnivol.smoothify.com/owner-pvc"
	annOwnerPVCNamespace = "omnivol.smoothify.com/owner-pvc-namespace"
)

// OrphanReconciler watches omnivol-managed ReplicationSource, ReplicationDestination,
// and Secret resources.  When the owning PVC no longer exists it deletes the orphan.
//
// +kubebuilder:rbac:groups=volsync.backube,resources=replicationsources,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=volsync.backube,resources=replicationdestinations,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch
type OrphanReconciler struct {
	client.Client
}

// SetupWithManager registers the OrphanReconciler to watch RS, RD, and Secrets
// that carry the omnivol managed-by label.
func (r *OrphanReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// We build three separate controllers — one per resource type — all sharing
	// the same reconcile logic via the generic helper below.
	if err := ctrl.NewControllerManagedBy(mgr).
		Named("orphan-replicationsource").
		Watches(
			&volsyncv1alpha1.ReplicationSource{},
			handler.EnqueueRequestsFromMapFunc(enqueueIfManaged),
		).
		Complete(reconcile.Func(r.reconcileReplicationSource)); err != nil {
		return err
	}

	if err := ctrl.NewControllerManagedBy(mgr).
		Named("orphan-replicationdestination").
		Watches(
			&volsyncv1alpha1.ReplicationDestination{},
			handler.EnqueueRequestsFromMapFunc(enqueueIfManaged),
		).
		Complete(reconcile.Func(r.reconcileReplicationDestination)); err != nil {
		return err
	}

	if err := ctrl.NewControllerManagedBy(mgr).
		Named("orphan-secret").
		Watches(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(enqueueIfManagedNamespaced),
		).
		Complete(reconcile.Func(r.reconcileSecret)); err != nil {
		return err
	}

	return nil
}

// enqueueIfManaged returns a reconcile request for an object only when it
// carries the omnivol managed-by label.
func enqueueIfManaged(_ context.Context, obj client.Object) []reconcile.Request {
	if obj.GetLabels()[labelManagedBy] != labelManagedByValue {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{
			Name:      obj.GetName(),
			Namespace: obj.GetNamespace(),
		},
	}}
}

// enqueueIfManagedNamespaced is the same as enqueueIfManaged — kept as a separate
// symbol so callers can distinguish the intent for namespaced resources.
var enqueueIfManagedNamespaced = enqueueIfManaged

// reconcileReplicationSource deletes a RS whose owner PVC no longer exists.
func (r *OrphanReconciler) reconcileReplicationSource(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	rs := &volsyncv1alpha1.ReplicationSource{}
	if err := r.Get(ctx, req.NamespacedName, rs); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if rs.GetLabels()[labelManagedBy] != labelManagedByValue {
		return ctrl.Result{}, nil
	}

	ownerName, ownerNS := ownerPVCRef(rs)
	if ownerName == "" {
		return ctrl.Result{}, nil
	}

	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Name: ownerName, Namespace: ownerNS}, pvc)
	if err == nil {
		return ctrl.Result{}, nil // owner still exists
	}
	if !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	logger.Info("Deleting orphaned ReplicationSource", "name", rs.Name, "namespace", rs.Namespace, "missingPVC", ownerName)
	if err := r.Delete(ctx, rs); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// reconcileReplicationDestination deletes an RD whose owner PVC no longer exists.
func (r *OrphanReconciler) reconcileReplicationDestination(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	rd := &volsyncv1alpha1.ReplicationDestination{}
	if err := r.Get(ctx, req.NamespacedName, rd); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if rd.GetLabels()[labelManagedBy] != labelManagedByValue {
		return ctrl.Result{}, nil
	}

	ownerName, ownerNS := ownerPVCRef(rd)
	if ownerName == "" {
		return ctrl.Result{}, nil
	}

	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Name: ownerName, Namespace: ownerNS}, pvc)
	if err == nil {
		return ctrl.Result{}, nil
	}
	if !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	logger.Info("Deleting orphaned ReplicationDestination", "name", rd.Name, "namespace", rd.Namespace, "missingPVC", ownerName)
	if err := r.Delete(ctx, rd); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// reconcileSecret deletes a restic Secret whose owner PVC no longer exists.
func (r *OrphanReconciler) reconcileSecret(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	secret := &corev1.Secret{}
	if err := r.Get(ctx, req.NamespacedName, secret); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if secret.GetLabels()[labelManagedBy] != labelManagedByValue {
		return ctrl.Result{}, nil
	}

	ownerName, ownerNS := ownerPVCRef(secret)
	if ownerName == "" {
		return ctrl.Result{}, nil
	}

	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Name: ownerName, Namespace: ownerNS}, pvc)
	if err == nil {
		return ctrl.Result{}, nil
	}
	if !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	logger.Info("Deleting orphaned Secret", "name", secret.Name, "namespace", secret.Namespace, "missingPVC", ownerName)
	if err := r.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// ownerPVCRef reads the omnivol owner-pvc annotations from an object and returns
// (pvcName, pvcNamespace).  Returns empty strings when annotations are absent.
func ownerPVCRef(obj client.Object) (name, namespace string) {
	ann := obj.GetAnnotations()
	if ann == nil {
		return "", ""
	}
	return ann[annOwnerPVCName], ann[annOwnerPVCNamespace]
}
