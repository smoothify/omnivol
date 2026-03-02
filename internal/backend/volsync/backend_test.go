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

package volsync

import (
	"context"
	"testing"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	omniv1alpha1 "github.com/smoothify/omnivol/api/v1alpha1"
	"github.com/smoothify/omnivol/internal/backend"
)

func newScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = volsyncv1alpha1.AddToScheme(s)
	_ = omniv1alpha1.AddToScheme(s)
	return s
}

const testControllerNS = "omnivol-system"

func newTestParams(controllerNS string) backend.EnsureParams {
	return backend.EnsureParams{
		Policy: &omniv1alpha1.BackupPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "hourly"},
			Spec: omniv1alpha1.BackupPolicySpec{
				Schedule:         "0 * * * *",
				CopyMethod:       "Direct",
				BackupStore:      "default-store",
				StorageClassName: "openebs-lvm",
				Retain: omniv1alpha1.RetainPolicy{
					Hourly:  ptr(int32(6)),
					Daily:   ptr(int32(5)),
					Weekly:  ptr(int32(4)),
					Monthly: ptr(int32(3)),
				},
			},
		},
		Store: &omniv1alpha1.BackupStore{
			ObjectMeta: metav1.ObjectMeta{Name: "default-store"},
			Spec: omniv1alpha1.BackupStoreSpec{
				S3: omniv1alpha1.S3Config{
					Endpoint:          "s3.example.com",
					Bucket:            "my-bucket",
					Region:            "us-east-1",
					TLS:               true,
					CredentialsSecret: "s3-creds",
				},
			},
		},
		UnderlyingPVC: &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "data-omnivol", Namespace: "prod"},
		},
		RepoPath:            "prod/data",
		Schedule:            "42 * * * *",
		ResticSecretName:    "omnivol-data",
		UserPVCName:         "data",
		UserPVCNamespace:    "prod",
		ControllerNamespace: controllerNS,
	}
}

// --- ensureResticSecret ---

func TestEnsureResticSecret_CreatesSecret(t *testing.T) {
	ctx := context.Background()
	controllerNS := testControllerNS

	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "s3-creds", Namespace: controllerNS},
		Data: map[string][]byte{
			"access-key-id":     []byte("AKID"),
			"secret-access-key": []byte("SECRET"),
			"restic-password":   []byte("hunter2"),
		},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(credSecret).Build()
	b := New(c)

	params := newTestParams(controllerNS)
	params.Client = c
	if err := b.ensureResticSecret(ctx, params); err != nil {
		t.Fatalf("ensureResticSecret() error = %v", err)
	}

	// Verify the secret was created.
	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: "omnivol-data", Namespace: "prod"}, secret); err != nil {
		t.Fatalf("get created secret: %v", err)
	}

	if secret.StringData["AWS_ACCESS_KEY_ID"] != "AKID" {
		t.Errorf("AWS_ACCESS_KEY_ID = %q, want %q", secret.StringData["AWS_ACCESS_KEY_ID"], "AKID")
	}
	if secret.StringData["RESTIC_PASSWORD"] != "hunter2" {
		t.Errorf("RESTIC_PASSWORD = %q, want %q", secret.StringData["RESTIC_PASSWORD"], "hunter2")
	}
	if secret.Annotations["omnivol.smoothify.com/owner-pvc"] != "data" {
		t.Errorf("owner-pvc annotation = %q, want %q", secret.Annotations["omnivol.smoothify.com/owner-pvc"], "data")
	}
}

func TestEnsureResticSecret_MissingCredentials(t *testing.T) {
	ctx := context.Background()
	controllerNS := testControllerNS

	c := fake.NewClientBuilder().WithScheme(newScheme()).Build()
	b := New(c)

	params := newTestParams(controllerNS)
	params.Client = c
	err := b.ensureResticSecret(ctx, params)
	if err == nil {
		t.Fatal("expected error when credentials secret is missing")
	}
}

// --- resolveResticPassword ---

func TestResolveResticPassword_FallbackKey(t *testing.T) {
	ctx := context.Background()

	credSecret := &corev1.Secret{
		Data: map[string][]byte{
			"restic-password": []byte("fallback-pw"),
		},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).Build()
	b := New(c)

	params := newTestParams("omnivol-system")
	params.Client = c

	pw, err := b.resolveResticPassword(ctx, params, credSecret)
	if err != nil {
		t.Fatalf("resolveResticPassword() error = %v", err)
	}
	if pw != "fallback-pw" {
		t.Errorf("got %q, want %q", pw, "fallback-pw")
	}
}

func TestResolveResticPassword_ExplicitRef(t *testing.T) {
	ctx := context.Background()
	controllerNS := testControllerNS

	resticSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "custom-restic", Namespace: controllerNS},
		Data: map[string][]byte{
			"my-key": []byte("explicit-pw"),
		},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(resticSecret).Build()
	b := New(c)

	params := newTestParams(controllerNS)
	params.Client = c
	params.Store.Spec.S3.ResticPasswordSecretRef = &omniv1alpha1.SecretKeyRef{
		Name: "custom-restic",
		Key:  "my-key",
	}

	pw, err := b.resolveResticPassword(ctx, params, &corev1.Secret{})
	if err != nil {
		t.Fatalf("resolveResticPassword() error = %v", err)
	}
	if pw != "explicit-pw" {
		t.Errorf("got %q, want %q", pw, "explicit-pw")
	}
}

func TestResolveResticPassword_MissingKeyInCredentials(t *testing.T) {
	ctx := context.Background()

	credSecret := &corev1.Secret{
		Data: map[string][]byte{
			"some-other-key": []byte("value"),
		},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).Build()
	b := New(c)

	params := newTestParams("omnivol-system")
	params.Client = c

	_, err := b.resolveResticPassword(ctx, params, credSecret)
	if err == nil {
		t.Fatal("expected error when restic-password key is missing")
	}
}

// --- buildResticRetainPolicy ---

func TestBuildResticRetainPolicy(t *testing.T) {
	params := newTestParams("ns")
	rp := buildResticRetainPolicy(params)

	if rp.Hourly == nil || *rp.Hourly != 6 {
		t.Errorf("Hourly = %v, want 6", rp.Hourly)
	}
	if rp.Daily == nil || *rp.Daily != 5 {
		t.Errorf("Daily = %v, want 5", rp.Daily)
	}
	if rp.Weekly == nil || *rp.Weekly != 4 {
		t.Errorf("Weekly = %v, want 4", rp.Weekly)
	}
	if rp.Monthly == nil || *rp.Monthly != 3 {
		t.Errorf("Monthly = %v, want 3", rp.Monthly)
	}
	if rp.Yearly != nil {
		t.Errorf("Yearly = %v, want nil", rp.Yearly)
	}
}

// --- Cleanup ---

func TestCleanup_DeletesAllResources(t *testing.T) {
	ctx := context.Background()

	rs := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{Name: "data-omnivol", Namespace: "prod"},
	}
	rd := &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{Name: "data-omnivol", Namespace: "prod"},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "omnivol-data", Namespace: "prod"},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(rs, rd, secret).Build()
	b := New(c)

	if err := b.Cleanup(ctx, "data-omnivol", "prod"); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}

	// Verify all resources are gone.
	if err := c.Get(ctx, types.NamespacedName{Name: "data-omnivol", Namespace: "prod"}, &volsyncv1alpha1.ReplicationSource{}); err == nil {
		t.Error("expected ReplicationSource to be deleted")
	}
	if err := c.Get(ctx, types.NamespacedName{Name: "data-omnivol", Namespace: "prod"}, &volsyncv1alpha1.ReplicationDestination{}); err == nil {
		t.Error("expected ReplicationDestination to be deleted")
	}
	if err := c.Get(ctx, types.NamespacedName{Name: "omnivol-data", Namespace: "prod"}, &corev1.Secret{}); err == nil {
		t.Error("expected Secret to be deleted")
	}
}

func TestCleanup_NoErrorWhenResourcesMissing(t *testing.T) {
	ctx := context.Background()

	c := fake.NewClientBuilder().WithScheme(newScheme()).Build()
	b := New(c)

	if err := b.Cleanup(ctx, "nonexistent-omnivol", "prod"); err != nil {
		t.Fatalf("Cleanup() should not error for missing resources, got: %v", err)
	}
}
