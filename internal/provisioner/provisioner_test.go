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

package provisioner

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	sigprovisioner "sigs.k8s.io/sig-storage-lib-external-provisioner/v13/controller"

	omniv1alpha1 "github.com/smoothify/omnivol/api/v1alpha1"
)

func newScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = storagev1.AddToScheme(s)
	_ = omniv1alpha1.AddToScheme(s)
	return s
}

// --- computeRepoPath ---

func TestComputeRepoPath_DefaultPath(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "prod"},
	}
	policy := &omniv1alpha1.BackupPolicy{}
	got := computeRepoPath(pvc, policy)
	if got != "prod/data" {
		t.Errorf("computeRepoPath() = %q, want %q", got, "prod/data")
	}
}

func TestComputeRepoPath_PolicyPrefix(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "prod"},
	}
	policy := &omniv1alpha1.BackupPolicy{
		Spec: omniv1alpha1.BackupPolicySpec{RepositoryPath: "backups"},
	}
	got := computeRepoPath(pvc, policy)
	if got != "backups/prod/data" {
		t.Errorf("computeRepoPath() = %q, want %q", got, "backups/prod/data")
	}
}

func TestComputeRepoPath_AnnotationOverride(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "data",
			Namespace:   "prod",
			Annotations: map[string]string{annRepoPathOverride: "custom/path"},
		},
	}
	policy := &omniv1alpha1.BackupPolicy{
		Spec: omniv1alpha1.BackupPolicySpec{RepositoryPath: "backups"},
	}
	got := computeRepoPath(pvc, policy)
	if got != "custom/path" {
		t.Errorf("computeRepoPath() = %q, want %q", got, "custom/path")
	}
}

// --- effectiveSchedule ---

func TestEffectiveSchedule_Default(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "x"}}
	policy := &omniv1alpha1.BackupPolicy{
		Spec: omniv1alpha1.BackupPolicySpec{Schedule: "0 * * * *"},
	}
	got := effectiveSchedule(pvc, policy)
	if got != "0 * * * *" {
		t.Errorf("effectiveSchedule() = %q, want %q", got, "0 * * * *")
	}
}

func TestEffectiveSchedule_AnnotationOverride(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "x",
			Annotations: map[string]string{annScheduleOverride: "30 2 * * *"},
		},
	}
	policy := &omniv1alpha1.BackupPolicy{
		Spec: omniv1alpha1.BackupPolicySpec{Schedule: "0 * * * *"},
	}
	got := effectiveSchedule(pvc, policy)
	if got != "30 2 * * *" {
		t.Errorf("effectiveSchedule() = %q, want %q", got, "30 2 * * *")
	}
}

// --- reclaimPolicyOrDefault ---

func TestReclaimPolicyOrDefault_Nil(t *testing.T) {
	got := reclaimPolicyOrDefault(nil)
	if got != corev1.PersistentVolumeReclaimDelete {
		t.Errorf("reclaimPolicyOrDefault(nil) = %q, want Delete", got)
	}
}

func TestReclaimPolicyOrDefault_Retain(t *testing.T) {
	retain := corev1.PersistentVolumeReclaimRetain
	got := reclaimPolicyOrDefault(&retain)
	if got != corev1.PersistentVolumeReclaimRetain {
		t.Errorf("reclaimPolicyOrDefault(Retain) = %q, want Retain", got)
	}
}

// --- resolveDeleteTimeout ---

func TestResolveDeleteTimeout_DefaultWhenNoStorageClass(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme()).Build()
	p := &OmnivolProvisioner{client: c}

	pv := &corev1.PersistentVolume{Spec: corev1.PersistentVolumeSpec{StorageClassName: ""}}
	got := p.resolveDeleteTimeout(context.Background(), pv)
	if got != defaultDeleteTimeout {
		t.Errorf("got %v, want %v", got, defaultDeleteTimeout)
	}
}

func TestResolveDeleteTimeout_FromPolicy(t *testing.T) {
	timeout := metav1.Duration{Duration: 10 * time.Minute}
	policy := &omniv1alpha1.BackupPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "hourly"},
		Spec: omniv1alpha1.BackupPolicySpec{
			DeleteTimeout: &timeout,
			BackupStore:   "s",
			Schedule:      "0 * * * *",
			CopyMethod:    "Direct",
		},
	}
	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "omnivol-hourly"},
		Provisioner: ProvisionerName,
		Parameters:  map[string]string{paramBackupPolicy: "hourly"},
	}
	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(policy, sc).Build()
	p := &OmnivolProvisioner{client: c}

	pv := &corev1.PersistentVolume{
		Spec: corev1.PersistentVolumeSpec{StorageClassName: "omnivol-hourly"},
	}
	got := p.resolveDeleteTimeout(context.Background(), pv)
	if got != 10*time.Minute {
		t.Errorf("got %v, want 10m", got)
	}
}

func TestResolveDeleteTimeout_DefaultWhenPolicyHasNoTimeout(t *testing.T) {
	policy := &omniv1alpha1.BackupPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "hourly"},
		Spec: omniv1alpha1.BackupPolicySpec{
			BackupStore: "s",
			Schedule:    "0 * * * *",
			CopyMethod:  "Direct",
		},
	}
	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "omnivol-hourly"},
		Provisioner: ProvisionerName,
		Parameters:  map[string]string{paramBackupPolicy: "hourly"},
	}
	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(policy, sc).Build()
	p := &OmnivolProvisioner{client: c}

	pv := &corev1.PersistentVolume{
		Spec: corev1.PersistentVolumeSpec{StorageClassName: "omnivol-hourly"},
	}
	got := p.resolveDeleteTimeout(context.Background(), pv)
	if got != defaultDeleteTimeout {
		t.Errorf("got %v, want %v", got, defaultDeleteTimeout)
	}
}

// --- buildPV ---

func TestBuildPV_CSIAndNodeAffinity(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "prod"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resourceFromStr("10Gi"),
				},
			},
		},
	}

	underlyingPV := &corev1.PersistentVolume{
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resourceFromStr("10Gi"),
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       "local.csi.openebs.io",
					VolumeHandle: "pvc-abc123",
					FSType:       "ext4",
				},
			},
			NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      "kubernetes.io/hostname",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"node-1"},
						}},
					}},
				},
			},
		},
	}

	underlyingPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data-omnivol", Namespace: "prod"},
	}

	reclaimDelete := corev1.PersistentVolumeReclaimDelete
	p := &OmnivolProvisioner{}
	pv := p.buildPV(
		sigprovisioner.ProvisionOptions{
			PVC:    pvc,
			PVName: "pv-xyz",
			StorageClass: &storagev1.StorageClass{
				ObjectMeta:    metav1.ObjectMeta{Name: "omnivol-hourly"},
				ReclaimPolicy: &reclaimDelete,
			},
		},
		underlyingPVC, underlyingPV, "data-omnivol", "prod",
	)

	if pv.Spec.CSI == nil {
		t.Fatal("expected CSI source to be copied")
	}
	if pv.Spec.CSI.VolumeHandle != "pvc-abc123" {
		t.Errorf("CSI volumeHandle = %q, want %q", pv.Spec.CSI.VolumeHandle, "pvc-abc123")
	}
	if pv.Spec.NodeAffinity == nil {
		t.Fatal("expected node affinity to be copied")
	}
	if pv.Annotations[annUnderlyingPVC] != "data-omnivol" {
		t.Errorf("annotation %q = %q, want %q", annUnderlyingPVC, pv.Annotations[annUnderlyingPVC], "data-omnivol")
	}
	if pv.Annotations[annUnderlyingNS] != "prod" {
		t.Errorf("annotation %q = %q, want %q", annUnderlyingNS, pv.Annotations[annUnderlyingNS], "prod")
	}
}

// --- resolvePolicy ---

func TestResolvePolicy_Success(t *testing.T) {
	store := &omniv1alpha1.BackupStore{
		ObjectMeta: metav1.ObjectMeta{Name: "default-store"},
		Spec: omniv1alpha1.BackupStoreSpec{
			S3: omniv1alpha1.S3Config{
				Endpoint:          "s3.example.com",
				Bucket:            "bkt",
				CredentialsSecret: "creds",
			},
		},
	}
	policy := &omniv1alpha1.BackupPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "hourly"},
		Spec: omniv1alpha1.BackupPolicySpec{
			BackupStore: "default-store",
			Schedule:    "0 * * * *",
			CopyMethod:  "Direct",
		},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(store, policy).Build()
	p := &OmnivolProvisioner{client: c}

	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: "test-sc"},
		Parameters: map[string]string{paramBackupPolicy: "hourly"},
	}

	gotPolicy, gotStore, err := p.resolvePolicy(context.Background(), sc)
	if err != nil {
		t.Fatalf("resolvePolicy() error = %v", err)
	}
	if gotPolicy.Name != "hourly" {
		t.Errorf("policy name = %q, want %q", gotPolicy.Name, "hourly")
	}
	if gotStore.Name != "default-store" {
		t.Errorf("store name = %q, want %q", gotStore.Name, "default-store")
	}
}

func TestResolvePolicy_MissingParameter(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme()).Build()
	p := &OmnivolProvisioner{client: c}
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: "bad-sc"},
		Parameters: map[string]string{},
	}

	_, _, err := p.resolvePolicy(context.Background(), sc)
	if err == nil {
		t.Fatal("expected error for missing backupPolicy parameter")
	}
}
