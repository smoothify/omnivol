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
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	omniv1alpha1 "github.com/smoothify/omnivol/api/v1alpha1"
	"github.com/smoothify/omnivol/internal/backend"
)

func newScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
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

// --- PVC controller ---

// fakeBackend is a mock backend.Interface for testing.
type fakeBackend struct {
	ensureRSCalled bool
	ensureRDCalled bool
	cleanupCalled  bool
}

func (f *fakeBackend) Name() string { return "fake" }
func (f *fakeBackend) EnsureReplicationSource(_ context.Context, _ backend.EnsureParams) error {
	f.ensureRSCalled = true
	return nil
}
func (f *fakeBackend) EnsureReplicationDestination(_ context.Context, _ backend.EnsureParams) (*corev1.TypedLocalObjectReference, error) {
	f.ensureRDCalled = true
	return nil, nil
}
func (f *fakeBackend) TriggerFinalSync(_ context.Context, _, _ string) error { return nil }
func (f *fakeBackend) Cleanup(_ context.Context, _, _ string) error {
	f.cleanupCalled = true
	return nil
}

func TestPVCReconciler_SkipUnlabelledPVC(t *testing.T) {
	ctx := context.Background()

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "prod"},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("5Gi"),
				},
			},
		},
	}

	fb := &fakeBackend{}
	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(pvc).Build()

	r := &PVCReconciler{Client: c, Backend: fb, ControllerNamespace: "omnivol-system"}
	_, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "data", Namespace: "prod"},
	})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if fb.ensureRSCalled {
		t.Error("expected EnsureReplicationSource NOT to be called for unlabelled PVC")
	}
}

func TestPVCReconciler_SkipUnboundPVC(t *testing.T) {
	ctx := context.Background()

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data",
			Namespace: "prod",
			Labels:    map[string]string{labelBackupPolicy: "hourly"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("5Gi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
	}
	policy := &omniv1alpha1.BackupPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "hourly"},
		Spec: omniv1alpha1.BackupPolicySpec{
			BackupStore: "default-store",
			Schedule:    "0 * * * *",
			CopyMethod:  "Direct",
		},
	}

	fb := &fakeBackend{}
	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(pvc, policy).Build()

	r := &PVCReconciler{Client: c, Backend: fb, ControllerNamespace: "omnivol-system"}
	res, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "data", Namespace: "prod"},
	})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if fb.ensureRSCalled {
		t.Error("expected EnsureReplicationSource NOT to be called for unbound PVC")
	}
	if res.RequeueAfter == 0 {
		t.Error("expected RequeueAfter > 0 for unbound PVC")
	}
}

func TestPVCReconciler_EnsuresRS(t *testing.T) {
	ctx := context.Background()
	scName := "topolvm-thin"

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data",
			Namespace: "prod",
			Labels:    map[string]string{labelBackupPolicy: "hourly"},
			UID:       "test-uid-123",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			VolumeName:       "pv-data",
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("5Gi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	policy := &omniv1alpha1.BackupPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "hourly"},
		Spec: omniv1alpha1.BackupPolicySpec{
			BackupStore: "default-store",
			Schedule:    "0 * * * *",
			CopyMethod:  "Direct",
			Retain:      omniv1alpha1.RetainPolicy{Hourly: ptr(int32(24))},
		},
	}
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
	// Create a fake RS so we skip the S3 backup-exists check.
	rs := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data",
			Namespace: "prod",
		},
	}

	fb := &fakeBackend{}
	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(pvc, policy, store, rs).
		WithStatusSubresource(policy).
		Build()

	r := &PVCReconciler{Client: c, Backend: fb, ControllerNamespace: "omnivol-system"}
	_, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "data", Namespace: "prod"},
	})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !fb.ensureRSCalled {
		t.Error("expected EnsureReplicationSource to be called")
	}
}

// --- Orphan controller ---

func TestOrphanReconciler_RS_DeletedWhenPVCMissing(t *testing.T) {
	ctx := context.Background()

	rs := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data",
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
		NamespacedName: types.NamespacedName{Name: "data", Namespace: "prod"},
	})
	if err != nil {
		t.Fatalf("reconcileReplicationSource() error = %v", err)
	}

	// RS should be deleted.
	if err := c.Get(ctx, types.NamespacedName{Name: "data", Namespace: "prod"}, &volsyncv1alpha1.ReplicationSource{}); err == nil {
		t.Error("expected RS to be deleted")
	}
}

func TestOrphanReconciler_RS_KeptWhenPVCExists(t *testing.T) {
	ctx := context.Background()

	rs := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data",
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
		NamespacedName: types.NamespacedName{Name: "data", Namespace: "prod"},
	})
	if err != nil {
		t.Fatalf("reconcileReplicationSource() error = %v", err)
	}

	// RS should still exist.
	if err := c.Get(ctx, types.NamespacedName{Name: "data", Namespace: "prod"}, &volsyncv1alpha1.ReplicationSource{}); err != nil {
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

// --- Pod backup-on-delete tests ---

func TestBackupOnDelete_DefaultEnabled(t *testing.T) {
	ctx := context.Background()
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data",
			Namespace: "default",
			Labels:    map[string]string{labelBackupPolicy: "hourly"},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "app-0", Namespace: "default"},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(pvc, pod).Build()

	r := &PodReconciler{Client: c, FinalSyncTimeout: 15 * time.Minute}
	got, err := r.isBackupOnDeleteEnabled(ctx, pod, []string{"data"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got {
		t.Error("expected true (default enabled for omnivol PVCs), got false")
	}
}

func TestBackupOnDelete_PodAnnotationOptOut(t *testing.T) {
	ctx := context.Background()
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data",
			Namespace: "default",
			Labels:    map[string]string{labelBackupPolicy: "hourly"},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "app-0",
			Namespace:   "default",
			Annotations: map[string]string{annBackupOnDelete: "false"},
		},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(pvc, pod).Build()

	r := &PodReconciler{Client: c, FinalSyncTimeout: 15 * time.Minute}
	got, err := r.isBackupOnDeleteEnabled(ctx, pod, []string{"data"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got {
		t.Error("expected false (pod annotation opt-out), got true")
	}
}

func TestBackupOnDelete_PVCAnnotationOptOut(t *testing.T) {
	ctx := context.Background()
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "data",
			Namespace:   "default",
			Labels:      map[string]string{labelBackupPolicy: "hourly"},
			Annotations: map[string]string{annBackupOnDelete: "false"},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "app-0", Namespace: "default"},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(pvc, pod).Build()

	r := &PodReconciler{Client: c, FinalSyncTimeout: 15 * time.Minute}
	got, err := r.isBackupOnDeleteEnabled(ctx, pod, []string{"data"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got {
		t.Error("expected false (PVC annotation opt-out), got true")
	}
}

// --- Auto-migrate tests ---

func TestIsAutoMigrateEnabled_PolicyDefault(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "default"},
	}
	policy := &omniv1alpha1.BackupPolicy{
		Spec: omniv1alpha1.BackupPolicySpec{AutoMigrate: true},
	}

	r := &PVCReconciler{}
	if !r.isAutoMigrateEnabled(pvc, policy) {
		t.Error("expected true (policy default)")
	}
}

func TestIsAutoMigrateEnabled_PVCOverrideFalse(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "data",
			Namespace:   "default",
			Annotations: map[string]string{annAutoMigrate: "false"},
		},
	}
	policy := &omniv1alpha1.BackupPolicy{
		Spec: omniv1alpha1.BackupPolicySpec{AutoMigrate: true},
	}

	r := &PVCReconciler{}
	if r.isAutoMigrateEnabled(pvc, policy) {
		t.Error("expected false (PVC annotation overrides policy)")
	}
}

func TestIsAutoMigrateEnabled_PVCOverrideTrue(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "data",
			Namespace:   "default",
			Annotations: map[string]string{annAutoMigrate: "true"},
		},
	}
	policy := &omniv1alpha1.BackupPolicy{
		Spec: omniv1alpha1.BackupPolicySpec{AutoMigrate: false},
	}

	r := &PVCReconciler{}
	if !r.isAutoMigrateEnabled(pvc, policy) {
		t.Error("expected true (PVC annotation overrides policy)")
	}
}

// --- savePVCSpec tests ---

func TestSavePVCSpec_StripsBindingFields(t *testing.T) {
	scName := "topolvm-thin"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data",
			Namespace: "prod",
			Labels:    map[string]string{labelBackupPolicy: "hourly", "app": "myapp"},
			Annotations: map[string]string{
				annScheduleOverride:                      "30 2 * * *",
				annRestoreInProgress:                     "true",
				annMigrating:                             "true",
				"volume.kubernetes.io/selected-node":     "worker-1",
				"pv.kubernetes.io/bind-completed":        "yes",
			},
			UID:             "old-uid",
			ResourceVersion: "12345",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeName:       "pv-data",
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("10Gi"),
				},
			},
		},
	}

	saved := savePVCSpec(pvc)

	// Should preserve name, namespace, labels.
	if saved.Name != "data" {
		t.Errorf("Name = %q, want %q", saved.Name, "data")
	}
	if saved.Namespace != "prod" {
		t.Errorf("Namespace = %q, want %q", saved.Namespace, "prod")
	}
	if saved.Labels[labelBackupPolicy] != "hourly" {
		t.Error("backup-policy label missing")
	}
	if saved.Labels["app"] != "myapp" {
		t.Error("app label missing")
	}

	// Should strip transient annotations.
	if _, ok := saved.Annotations[annRestoreInProgress]; ok {
		t.Error("restore-in-progress annotation should be stripped")
	}
	if _, ok := saved.Annotations[annMigrating]; ok {
		t.Error("migrating annotation should be stripped")
	}
	if _, ok := saved.Annotations["volume.kubernetes.io/selected-node"]; ok {
		t.Error("selected-node annotation should be stripped")
	}

	// Should keep user-set annotations.
	if saved.Annotations[annScheduleOverride] != "30 2 * * *" {
		t.Error("schedule override annotation should be preserved")
	}

	// Should clear VolumeName.
	if saved.Spec.VolumeName != "" {
		t.Errorf("VolumeName = %q, want empty", saved.Spec.VolumeName)
	}

	// Should preserve storage class and resources.
	if saved.Spec.StorageClassName == nil || *saved.Spec.StorageClassName != "topolvm-thin" {
		t.Error("StorageClassName should be preserved")
	}

	// UID and ResourceVersion should be empty (new object).
	if saved.UID != "" {
		t.Error("UID should be empty")
	}
	if saved.ResourceVersion != "" {
		t.Error("ResourceVersion should be empty")
	}
}

// --- Scheduling gate tests ---

func TestSchedulingGate_AddAndRemove(t *testing.T) {
	ctx := context.Background()

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data",
			Namespace: "default",
			Labels:    map[string]string{labelBackupPolicy: "hourly"},
		},
	}

	// Unscheduled pod referencing the PVC.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "app-0", Namespace: "default"},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "data",
					},
				},
			}},
			Containers: []corev1.Container{{
				Name:  "app",
				Image: "nginx",
			}},
		},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(pvc, pod).Build()

	r := &PVCReconciler{Client: c}

	// Add gate.
	if err := r.addSchedulingGates(ctx, pvc); err != nil {
		t.Fatalf("addSchedulingGates() error = %v", err)
	}

	updated := &corev1.Pod{}
	if err := c.Get(ctx, types.NamespacedName{Name: "app-0", Namespace: "default"}, updated); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if !hasSchedulingGate(updated) {
		t.Error("expected scheduling gate to be added")
	}

	// Remove gate.
	if err := r.removeSchedulingGates(ctx, pvc); err != nil {
		t.Fatalf("removeSchedulingGates() error = %v", err)
	}

	if err := c.Get(ctx, types.NamespacedName{Name: "app-0", Namespace: "default"}, updated); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if hasSchedulingGate(updated) {
		t.Error("expected scheduling gate to be removed")
	}
}

func TestSchedulingGate_SkipsScheduledPods(t *testing.T) {
	ctx := context.Background()

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data",
			Namespace: "default",
			Labels:    map[string]string{labelBackupPolicy: "hourly"},
		},
	}

	// Already-scheduled pod.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "app-0", Namespace: "default"},
		Spec: corev1.PodSpec{
			NodeName: "worker-1",
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "data",
					},
				},
			}},
			Containers: []corev1.Container{{
				Name:  "app",
				Image: "nginx",
			}},
		},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(pvc, pod).Build()

	r := &PVCReconciler{Client: c}

	if err := r.addSchedulingGates(ctx, pvc); err != nil {
		t.Fatalf("addSchedulingGates() error = %v", err)
	}

	updated := &corev1.Pod{}
	if err := c.Get(ctx, types.NamespacedName{Name: "app-0", Namespace: "default"}, updated); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if hasSchedulingGate(updated) {
		t.Error("scheduled pods should NOT get scheduling gates")
	}
}

// --- Helpers ---

func ptr[T any](v T) *T { return &v }
