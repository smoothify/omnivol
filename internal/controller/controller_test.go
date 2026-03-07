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
	"testing"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	omniv1alpha1 "github.com/smoothify/omnivol/api/v1alpha1"
)

func newScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = storagev1.AddToScheme(s)
	_ = omniv1alpha1.AddToScheme(s)
	_ = volsyncv1alpha1.AddToScheme(s)
	return s
}

// --- BackupStore controller ---

func TestBackupStoreReconciler_CredentialsNotFound(t *testing.T) {
	ctx := context.Background()
	store := &omniv1alpha1.BackupStore{
		ObjectMeta: metav1.ObjectMeta{Name: "test-store"},
		Spec: omniv1alpha1.BackupStoreSpec{
			S3: omniv1alpha1.S3Config{
				Endpoint:          "s3.example.com",
				Bucket:            "bkt",
				CredentialsSecret: "missing-creds",
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(store).
		WithStatusSubresource(store).
		Build()

	r := &BackupStoreReconciler{Client: c, Namespace: "omnivol-system"}
	_, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "test-store"},
	})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	// Verify the status reports not-ready.
	updated := &omniv1alpha1.BackupStore{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test-store"}, updated); err != nil {
		t.Fatalf("get store: %v", err)
	}

	if len(updated.Status.Conditions) == 0 {
		t.Fatal("expected conditions to be set")
	}
	cond := updated.Status.Conditions[0]
	if cond.Status != metav1.ConditionFalse {
		t.Errorf("condition status = %q, want False", cond.Status)
	}
	if cond.Reason != "CredentialsSecretNotFound" {
		t.Errorf("condition reason = %q, want CredentialsSecretNotFound", cond.Reason)
	}
}

func TestBackupStoreReconciler_MissingStore(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(newScheme()).Build()

	r := &BackupStoreReconciler{Client: c, Namespace: "omnivol-system"}
	_, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "nonexistent"},
	})
	// Should not error — just returns (not-found is ignored in reconcile).
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
}

// --- BackupPolicy controller ---

func TestBackupPolicyReconciler_StoreNotFound(t *testing.T) {
	ctx := context.Background()
	policy := &omniv1alpha1.BackupPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "hourly"},
		Spec: omniv1alpha1.BackupPolicySpec{
			BackupStore:      "missing-store",
			StorageClassName: "openebs-lvm",
			Schedule:         "0 * * * *",
			CopyMethod:       "Direct",
		},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(policy).
		WithStatusSubresource(policy).
		Build()

	r := &BackupPolicyReconciler{Client: c}
	_, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "hourly"},
	})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	updated := &omniv1alpha1.BackupPolicy{}
	if err := c.Get(ctx, types.NamespacedName{Name: "hourly"}, updated); err != nil {
		t.Fatalf("get policy: %v", err)
	}

	if len(updated.Status.Conditions) == 0 {
		t.Fatal("expected conditions to be set")
	}
	cond := updated.Status.Conditions[0]
	if cond.Status != metav1.ConditionFalse {
		t.Errorf("condition status = %q, want False", cond.Status)
	}
	if cond.Reason != "BackupStoreNotFound" {
		t.Errorf("condition reason = %q, want BackupStoreNotFound", cond.Reason)
	}
}

func TestBackupPolicyReconciler_StoreNotReady(t *testing.T) {
	ctx := context.Background()
	store := &omniv1alpha1.BackupStore{
		ObjectMeta: metav1.ObjectMeta{Name: "default-store"},
		Spec: omniv1alpha1.BackupStoreSpec{
			S3: omniv1alpha1.S3Config{Endpoint: "s3", Bucket: "b", CredentialsSecret: "c"},
		},
		Status: omniv1alpha1.BackupStoreStatus{
			Conditions: []metav1.Condition{{
				Type:   conditionReady,
				Status: metav1.ConditionFalse,
				Reason: "NotReady",
			}},
		},
	}
	policy := &omniv1alpha1.BackupPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "hourly"},
		Spec: omniv1alpha1.BackupPolicySpec{
			BackupStore:      "default-store",
			StorageClassName: "openebs-lvm",
			Schedule:         "0 * * * *",
			CopyMethod:       "Direct",
		},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(store, policy).
		WithStatusSubresource(policy).
		Build()

	r := &BackupPolicyReconciler{Client: c}
	_, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "hourly"},
	})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	updated := &omniv1alpha1.BackupPolicy{}
	if err := c.Get(ctx, types.NamespacedName{Name: "hourly"}, updated); err != nil {
		t.Fatalf("get policy: %v", err)
	}

	if len(updated.Status.Conditions) == 0 {
		t.Fatal("expected conditions to be set")
	}
	cond := updated.Status.Conditions[0]
	if cond.Status != metav1.ConditionFalse {
		t.Errorf("condition status = %q, want False", cond.Status)
	}
	if cond.Reason != "BackupStoreNotReady" {
		t.Errorf("condition reason = %q, want BackupStoreNotReady", cond.Reason)
	}
}

func TestBackupPolicyReconciler_ReadyWithPVCCount(t *testing.T) {
	ctx := context.Background()
	store := &omniv1alpha1.BackupStore{
		ObjectMeta: metav1.ObjectMeta{Name: "default-store"},
		Spec: omniv1alpha1.BackupStoreSpec{
			S3: omniv1alpha1.S3Config{Endpoint: "s3", Bucket: "b", CredentialsSecret: "c"},
		},
		Status: omniv1alpha1.BackupStoreStatus{
			Conditions: []metav1.Condition{{
				Type:   conditionReady,
				Status: metav1.ConditionTrue,
				Reason: "Ready",
			}},
		},
	}
	policy := &omniv1alpha1.BackupPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "hourly"},
		Spec: omniv1alpha1.BackupPolicySpec{
			BackupStore:      "default-store",
			StorageClassName: "openebs-lvm",
			Schedule:         "0 * * * *",
			CopyMethod:       "Direct",
		},
	}
	// StorageClass using the omnivol provisioner with this policy.
	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "omnivol-hourly"},
		Provisioner: "omnivol.smoothify.com/provisioner",
		Parameters:  map[string]string{"backupPolicy": "hourly"},
	}
	// Two PVCs using this StorageClass.
	scName := "omnivol-hourly"
	pvc1 := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-a", Namespace: "prod"},
		Spec:       corev1.PersistentVolumeClaimSpec{StorageClassName: &scName},
	}
	pvc2 := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-b", Namespace: "staging"},
		Spec:       corev1.PersistentVolumeClaimSpec{StorageClassName: &scName},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(store, policy, sc, pvc1, pvc2).
		WithStatusSubresource(policy).
		Build()

	r := &BackupPolicyReconciler{Client: c}
	_, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "hourly"},
	})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	updated := &omniv1alpha1.BackupPolicy{}
	if err := c.Get(ctx, types.NamespacedName{Name: "hourly"}, updated); err != nil {
		t.Fatalf("get policy: %v", err)
	}

	if len(updated.Status.Conditions) == 0 {
		t.Fatal("expected conditions to be set")
	}
	cond := updated.Status.Conditions[0]
	if cond.Status != metav1.ConditionTrue {
		t.Errorf("condition status = %q, want True", cond.Status)
	}
	if updated.Status.ManagedPVCCount != 2 {
		t.Errorf("managedPVCCount = %d, want 2", updated.Status.ManagedPVCCount)
	}
}

// --- Orphan controller ---

func TestOrphanReconciler_RS_DeletedWhenPVCMissing(t *testing.T) {
	ctx := context.Background()

	rs := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-omnivol",
			Namespace: "prod",
			Labels:    map[string]string{labelManagedBy: labelManagedByValue},
			Annotations: map[string]string{
				annOwnerPVCName:      "data",
				annOwnerPVCNamespace: "prod",
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(rs).Build()

	r := &OrphanReconciler{Client: c}
	_, err := r.reconcileReplicationSource(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "data-omnivol", Namespace: "prod"},
	})
	if err != nil {
		t.Fatalf("reconcileReplicationSource() error = %v", err)
	}

	// RS should be deleted.
	if err := c.Get(ctx, types.NamespacedName{Name: "data-omnivol", Namespace: "prod"}, &volsyncv1alpha1.ReplicationSource{}); err == nil {
		t.Error("expected RS to be deleted")
	}
}

func TestOrphanReconciler_RS_KeptWhenPVCExists(t *testing.T) {
	ctx := context.Background()

	rs := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-omnivol",
			Namespace: "prod",
			Labels:    map[string]string{labelManagedBy: labelManagedByValue},
			Annotations: map[string]string{
				annOwnerPVCName:      "data",
				annOwnerPVCNamespace: "prod",
			},
		},
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "prod"},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(rs, pvc).Build()

	r := &OrphanReconciler{Client: c}
	_, err := r.reconcileReplicationSource(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "data-omnivol", Namespace: "prod"},
	})
	if err != nil {
		t.Fatalf("reconcileReplicationSource() error = %v", err)
	}

	// RS should still exist.
	if err := c.Get(ctx, types.NamespacedName{Name: "data-omnivol", Namespace: "prod"}, &volsyncv1alpha1.ReplicationSource{}); err != nil {
		t.Errorf("expected RS to still exist, got err: %v", err)
	}
}

func TestOrphanReconciler_Secret_DeletedWhenOrphaned(t *testing.T) {
	ctx := context.Background()

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "omnivol-data",
			Namespace: "prod",
			Labels:    map[string]string{labelManagedBy: labelManagedByValue},
			Annotations: map[string]string{
				annOwnerPVCName:      "data",
				annOwnerPVCNamespace: "prod",
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(secret).Build()

	r := &OrphanReconciler{Client: c}
	_, err := r.reconcileSecret(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "omnivol-data", Namespace: "prod"},
	})
	if err != nil {
		t.Fatalf("reconcileSecret() error = %v", err)
	}

	// Secret should be deleted.
	if err := c.Get(ctx, types.NamespacedName{Name: "omnivol-data", Namespace: "prod"}, &corev1.Secret{}); err == nil {
		t.Error("expected Secret to be deleted")
	}
}

func TestOrphanReconciler_NotFoundHandledGracefully(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(newScheme()).Build()

	r := &OrphanReconciler{Client: c}
	_, err := r.reconcileReplicationSource(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "gone", Namespace: "prod"},
	})
	if err != nil {
		t.Fatalf("expected no error for not-found, got: %v", err)
	}
}

// --- DeletePVCOnBackup tests ---

const testSCName = "omnivol-sc"

func TestDeletePVC_DefaultTrue_NoAnnotations(t *testing.T) {
	ctx := context.Background()
	scName := testSCName
	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: scName},
		Provisioner: "omnivol.smoothify.com/provisioner",
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "default"},
		Spec:       corev1.PersistentVolumeClaimSpec{StorageClassName: &scName},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "app-0", Namespace: "default"},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(sc, pvc, pod).Build()

	r := &PodReconciler{Client: c, DefaultDeletePVCAfterBackup: true}
	got, err := r.isDeletePVCOnBackupEnabled(ctx, pod, "data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got {
		t.Error("expected true (controller default), got false")
	}
}

func TestDeletePVC_DefaultFalse_NoAnnotations(t *testing.T) {
	ctx := context.Background()
	scName := testSCName
	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: scName},
		Provisioner: "omnivol.smoothify.com/provisioner",
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "default"},
		Spec:       corev1.PersistentVolumeClaimSpec{StorageClassName: &scName},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "app-0", Namespace: "default"},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(sc, pvc, pod).Build()

	r := &PodReconciler{Client: c, DefaultDeletePVCAfterBackup: false}
	got, err := r.isDeletePVCOnBackupEnabled(ctx, pod, "data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got {
		t.Error("expected false (controller default), got true")
	}
}

func TestDeletePVC_StorageClassOverridesDefault(t *testing.T) {
	ctx := context.Background()
	scName := testSCName
	// StorageClass sets annotation to "false", overriding default true
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:        scName,
			Annotations: map[string]string{annDeletePVCOnBackup: "false"},
		},
		Provisioner: "omnivol.smoothify.com/provisioner",
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "default"},
		Spec:       corev1.PersistentVolumeClaimSpec{StorageClassName: &scName},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "app-0", Namespace: "default"},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(sc, pvc, pod).Build()

	r := &PodReconciler{Client: c, DefaultDeletePVCAfterBackup: true}
	got, err := r.isDeletePVCOnBackupEnabled(ctx, pod, "data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got {
		t.Error("expected false (StorageClass override), got true")
	}
}

func TestDeletePVC_StorageClassEnablesWhenDefaultFalse(t *testing.T) {
	ctx := context.Background()
	scName := testSCName
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:        scName,
			Annotations: map[string]string{annDeletePVCOnBackup: "true"},
		},
		Provisioner: "omnivol.smoothify.com/provisioner",
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "default"},
		Spec:       corev1.PersistentVolumeClaimSpec{StorageClassName: &scName},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "app-0", Namespace: "default"},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(sc, pvc, pod).Build()

	r := &PodReconciler{Client: c, DefaultDeletePVCAfterBackup: false}
	got, err := r.isDeletePVCOnBackupEnabled(ctx, pod, "data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got {
		t.Error("expected true (StorageClass annotation), got false")
	}
}

func TestDeletePVC_PodAnnotationTakesPriority(t *testing.T) {
	ctx := context.Background()
	scName := testSCName
	// StorageClass says true, but Pod says false — Pod wins.
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:        scName,
			Annotations: map[string]string{annDeletePVCOnBackup: "true"},
		},
		Provisioner: "omnivol.smoothify.com/provisioner",
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "default"},
		Spec:       corev1.PersistentVolumeClaimSpec{StorageClassName: &scName},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "app-0",
			Namespace:   "default",
			Annotations: map[string]string{annDeletePVCOnBackup: "false"},
		},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(sc, pvc, pod).Build()

	r := &PodReconciler{Client: c, DefaultDeletePVCAfterBackup: true}
	got, err := r.isDeletePVCOnBackupEnabled(ctx, pod, "data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got {
		t.Error("expected false (Pod annotation overrides all), got true")
	}
}
