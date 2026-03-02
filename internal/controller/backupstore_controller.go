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

// Package controller contains the Kubernetes controllers for omnivol CRDs.
package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	omniv1alpha1 "github.com/smoothify/omnivol/api/v1alpha1"
	"github.com/smoothify/omnivol/internal/s3check"
)

const (
	conditionReady = "Ready"
)

// BackupStoreReconciler reconciles BackupStore objects.
//
// +kubebuilder:rbac:groups=omnivol.smoothify.com,resources=backupstores,verbs=get;list;watch
// +kubebuilder:rbac:groups=omnivol.smoothify.com,resources=backupstores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=omnivol.smoothify.com,resources=backupstores/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
type BackupStoreReconciler struct {
	client.Client
	// Namespace is the controller namespace where credential secrets are resolved.
	Namespace string
}

// SetupWithManager registers the BackupStoreReconciler with the controller manager.
// It also watches Secrets so that a credential rotation triggers re-validation.
func (r *BackupStoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Map a Secret change to all BackupStores that reference it.
	mapSecretToStores := handler.EnqueueRequestsFromMapFunc(
		func(ctx context.Context, obj client.Object) []reconcile.Request {
			storeList := &omniv1alpha1.BackupStoreList{}
			if err := r.List(ctx, storeList); err != nil {
				return nil
			}
			var requests []reconcile.Request
			for _, store := range storeList.Items {
				ref := store.Spec.S3.CredentialsSecret
				// Only re-reconcile when the changed Secret is in the controller
				// namespace and its name matches the BackupStore's credentialsSecret.
				if ref == obj.GetName() && obj.GetNamespace() == r.Namespace {
					requests = append(requests, reconcile.Request{
						NamespacedName: types.NamespacedName{Name: store.Name},
					})
				}
			}
			return requests
		},
	)

	return ctrl.NewControllerManagedBy(mgr).
		For(&omniv1alpha1.BackupStore{}).
		Watches(
			&corev1.Secret{},
			mapSecretToStores,
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}),
		).
		Complete(r)
}

// Reconcile validates S3 connectivity for the BackupStore and writes status conditions.
func (r *BackupStoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	store := &omniv1alpha1.BackupStore{}
	if err := r.Get(ctx, req.NamespacedName, store); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	logger.Info("Reconciling BackupStore", "name", store.Name)

	// Load the credentials secret from the controller namespace.
	credSecret := &corev1.Secret{}
	credName := store.Spec.S3.CredentialsSecret
	if err := r.Get(ctx, types.NamespacedName{
		Name:      credName,
		Namespace: r.Namespace,
	}, credSecret); err != nil {
		reason := "CredentialsSecretNotFound"
		msg := fmt.Sprintf("Could not read credentials secret %s/%s: %v", r.Namespace, credName, err)
		if apierrors.IsNotFound(err) {
			msg = fmt.Sprintf("Credentials secret %s/%s not found", r.Namespace, credName)
		}
		return ctrl.Result{}, r.setReadyCondition(ctx, store, metav1.ConditionFalse, reason, msg)
	}

	accessKey := string(credSecret.Data["access-key-id"])
	secretKey := string(credSecret.Data["secret-access-key"])
	if accessKey == "" || secretKey == "" {
		return ctrl.Result{}, r.setReadyCondition(ctx, store, metav1.ConditionFalse,
			"InvalidCredentials",
			"Credentials secret is missing access-key-id or secret-access-key",
		)
	}

	// Validate S3 connectivity by checking bucket accessibility.
	s3 := store.Spec.S3
	sc, err := s3check.NewClient(s3.Endpoint, s3.Bucket, accessKey, secretKey, s3.TLS)
	if err != nil {
		return ctrl.Result{}, r.setReadyCondition(ctx, store, metav1.ConditionFalse,
			"S3ClientError",
			fmt.Sprintf("Could not create S3 client: %v", err),
		)
	}

	// Use a lightweight probe: attempt to list with MaxKeys=1 under an empty prefix.
	// A successful list (even returning zero objects) proves bucket access.
	if _, err := sc.CheckBackupExists(ctx, "omnivol-probe"); err != nil {
		return ctrl.Result{}, r.setReadyCondition(ctx, store, metav1.ConditionFalse,
			"S3Unreachable",
			fmt.Sprintf("S3 connectivity check failed: %v", err),
		)
	}

	logger.Info("BackupStore S3 connectivity verified", "name", store.Name, "endpoint", s3.Endpoint, "bucket", s3.Bucket)
	return ctrl.Result{}, r.setReadyCondition(ctx, store, metav1.ConditionTrue,
		"Available",
		fmt.Sprintf("S3 endpoint %s bucket %s is reachable", s3.Endpoint, s3.Bucket),
	)
}

// setReadyCondition patches the BackupStore status with the given Ready condition.
func (r *BackupStoreReconciler) setReadyCondition(
	ctx context.Context,
	store *omniv1alpha1.BackupStore,
	status metav1.ConditionStatus,
	reason, message string,
) error {
	// Re-fetch to avoid update conflicts.
	current := &omniv1alpha1.BackupStore{}
	if err := r.Get(ctx, types.NamespacedName{Name: store.Name}, current); err != nil {
		return client.IgnoreNotFound(err)
	}

	meta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             status,
		ObservedGeneration: current.Generation,
		Reason:             reason,
		Message:            message,
	})

	return r.Status().Update(ctx, current)
}
