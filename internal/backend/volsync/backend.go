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

// Package volsync implements the backend.Interface using VolSync ReplicationSource
// and ReplicationDestination resources.
package volsync

import (
	"context"
	"fmt"
	"strings"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/smoothify/omnivol/internal/backend"
	"github.com/smoothify/omnivol/internal/s3check"
)

const (
	// resticPrefix is prepended to restic repository paths to form the full
	// repository URL passed to VolSync: "s3:<endpoint>/<bucket>/<path>".
	resticPrefix = "s3"

	// underlyingSuffix is appended to the user PVC name to form the underlying PVC name.
	underlyingSuffix = "-omnivol"

	pollInterval = 5 * time.Second
	pollTimeout  = 10 * time.Minute
)

// Backend implements backend.Interface using VolSync.
type Backend struct {
	client client.Client
}

// New creates a new VolSync backend.
func New(c client.Client) *Backend {
	return &Backend{client: c}
}

// Name implements backend.Interface.
func (b *Backend) Name() string { return "volsync" }

// BackupExists checks whether the restic repository at repoPath already
// contains at least one object, indicating that a prior backup exists.
// It requires that the BackupStore S3 credentials are already readable from the
// cluster (the caller is responsible for loading them).
func (b *Backend) BackupExists(ctx context.Context, repoPath string) (bool, error) {
	// BackupExists is called before we have constructed the full EnsureParams,
	// so the caller must use the s3check package directly.  This method is a
	// convenience shim — real callers use s3check.Client directly.
	_ = repoPath
	return false, fmt.Errorf("BackupExists must be called via s3check.Client directly; use EnsureParams flow")
}

// EnsureReplicationSource creates or updates the VolSync ReplicationSource.
func (b *Backend) EnsureReplicationSource(ctx context.Context, params backend.EnsureParams) error {
	if err := b.ensureResticSecret(ctx, params); err != nil {
		return fmt.Errorf("ensure restic secret: %w", err)
	}

	copyMethod := volsyncv1alpha1.CopyMethodType(params.Policy.Spec.CopyMethod)
	pvcName := params.UnderlyingPVC.Name
	ns := params.UnderlyingPVC.Namespace

	rs := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: ns,
			Labels: map[string]string{
				"omnivol.smoothify.com/managed-by": "omnivol",
			},
			Annotations: map[string]string{
				"omnivol.smoothify.com/owner-pvc":           params.UserPVCName,
				"omnivol.smoothify.com/owner-pvc-namespace": params.UserPVCNamespace,
			},
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, b.client, rs, func() error {
		// Preserve annotations set at creation — CreateOrUpdate may clear them on
		// update unless we re-apply them in the mutate func.
		if rs.Annotations == nil {
			rs.Annotations = map[string]string{}
		}
		rs.Annotations["omnivol.smoothify.com/owner-pvc"] = params.UserPVCName
		rs.Annotations["omnivol.smoothify.com/owner-pvc-namespace"] = params.UserPVCNamespace
		rs.Spec = volsyncv1alpha1.ReplicationSourceSpec{
			SourcePVC: pvcName,
			Trigger: &volsyncv1alpha1.ReplicationSourceTriggerSpec{
				Schedule: &params.Schedule,
			},
			Restic: &volsyncv1alpha1.ReplicationSourceResticSpec{
				ReplicationSourceVolumeOptions: volsyncv1alpha1.ReplicationSourceVolumeOptions{
					CopyMethod: copyMethod,
				},
				Repository:        params.ResticSecretName,
				Retain:            buildResticRetainPolicy(params),
				PruneIntervalDays: ptr(int32(7)),
			},
		}
		return nil
	})
	return err
}

// EnsureReplicationDestination creates a VolSync ReplicationDestination,
// waits for latestImage to be populated, then returns the image reference.
// The caller is responsible for deleting the destination after use.
func (b *Backend) EnsureReplicationDestination(ctx context.Context, params backend.EnsureParams) (*corev1.TypedLocalObjectReference, error) {
	if err := b.ensureResticSecret(ctx, params); err != nil {
		return nil, fmt.Errorf("ensure restic secret: %w", err)
	}

	copyMethod := volsyncv1alpha1.CopyMethodType(params.Policy.Spec.CopyMethod)
	pvcName := params.UnderlyingPVC.Name
	ns := params.UnderlyingPVC.Namespace

	// Determine capacity from the underlying PVC request.
	capacity := params.UnderlyingPVC.Spec.Resources.Requests[corev1.ResourceStorage]

	rd := &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: ns,
			Labels: map[string]string{
				"omnivol.smoothify.com/managed-by": "omnivol",
			},
			Annotations: map[string]string{
				"omnivol.smoothify.com/owner-pvc":           params.UserPVCName,
				"omnivol.smoothify.com/owner-pvc-namespace": params.UserPVCNamespace,
			},
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, b.client, rd, func() error {
		if rd.Annotations == nil {
			rd.Annotations = map[string]string{}
		}
		rd.Annotations["omnivol.smoothify.com/owner-pvc"] = params.UserPVCName
		rd.Annotations["omnivol.smoothify.com/owner-pvc-namespace"] = params.UserPVCNamespace
		rd.Spec = volsyncv1alpha1.ReplicationDestinationSpec{
			Trigger: &volsyncv1alpha1.ReplicationDestinationTriggerSpec{
				Manual: "restore-once",
			},
			Restic: &volsyncv1alpha1.ReplicationDestinationResticSpec{
				ReplicationDestinationVolumeOptions: volsyncv1alpha1.ReplicationDestinationVolumeOptions{
					CopyMethod:       copyMethod,
					AccessModes:      params.UnderlyingPVC.Spec.AccessModes,
					Capacity:         &capacity,
					StorageClassName: &params.Policy.Spec.StorageClassName,
					DestinationPVC:   &pvcName,
				},
				Repository: params.ResticSecretName,
			},
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("create/update ReplicationDestination: %w", err)
	}

	// Poll until latestImage is set.
	var latestImage *corev1.TypedLocalObjectReference
	err = wait.PollUntilContextTimeout(ctx, pollInterval, pollTimeout, true, func(ctx context.Context) (bool, error) {
		current := &volsyncv1alpha1.ReplicationDestination{}
		if err := b.client.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: ns}, current); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		if current.Status.LatestImage != nil {
			latestImage = current.Status.LatestImage
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return nil, fmt.Errorf("waiting for ReplicationDestination latestImage: %w", err)
	}

	// Delete the destination now that we have the image reference.
	if err := b.client.Delete(ctx, rd); client.IgnoreNotFound(err) != nil {
		return nil, fmt.Errorf("delete ReplicationDestination: %w", err)
	}

	return latestImage, nil
}

// TriggerFinalSync patches a ReplicationSource with a manual trigger and waits
// for the sync to complete (lastManualSyncTime matches trigger).
func (b *Backend) TriggerFinalSync(ctx context.Context, rsName, namespace string) error {
	triggerValue := fmt.Sprintf("pre-delete-%d", time.Now().Unix())

	rs := &volsyncv1alpha1.ReplicationSource{}
	if err := b.client.Get(ctx, types.NamespacedName{Name: rsName, Namespace: namespace}, rs); err != nil {
		return client.IgnoreNotFound(err)
	}

	patch := client.MergeFrom(rs.DeepCopy())
	if rs.Spec.Trigger == nil {
		rs.Spec.Trigger = &volsyncv1alpha1.ReplicationSourceTriggerSpec{}
	}
	rs.Spec.Trigger.Manual = triggerValue
	if err := b.client.Patch(ctx, rs, patch); err != nil {
		return fmt.Errorf("patch ReplicationSource trigger: %w", err)
	}

	return wait.PollUntilContextTimeout(ctx, pollInterval, pollTimeout, false, func(ctx context.Context) (bool, error) {
		current := &volsyncv1alpha1.ReplicationSource{}
		if err := b.client.Get(ctx, types.NamespacedName{Name: rsName, Namespace: namespace}, current); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		return current.Status.LastManualSync == triggerValue, nil
	})
}

// Cleanup removes all backend resources for a PVC.
func (b *Backend) Cleanup(ctx context.Context, pvcName, namespace string) error {
	nn := types.NamespacedName{Name: pvcName, Namespace: namespace}

	rs := &volsyncv1alpha1.ReplicationSource{}
	if err := b.client.Get(ctx, nn, rs); err == nil {
		if err := b.client.Delete(ctx, rs); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete ReplicationSource: %w", err)
		}
	}

	rd := &volsyncv1alpha1.ReplicationDestination{}
	if err := b.client.Get(ctx, nn, rd); err == nil {
		if err := b.client.Delete(ctx, rd); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete ReplicationDestination: %w", err)
		}
	}

	// The secret is named "omnivol-<userPVCName>", but pvcName here is the
	// underlying PVC name (<userPVCName>-omnivol).  Strip the suffix to recover
	// the user PVC name.
	userPVCName := strings.TrimSuffix(pvcName, underlyingSuffix)
	secret := &corev1.Secret{}
	secretNN := types.NamespacedName{Name: "omnivol-" + userPVCName, Namespace: namespace}
	if err := b.client.Get(ctx, secretNN, secret); err == nil {
		if err := b.client.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete restic secret: %w", err)
		}
	}

	return nil
}

// ensureResticSecret creates or updates the restic credentials Secret for a PVC.
// The Secret is named "omnivol-<pvcname>" in the same namespace as the underlying PVC.
func (b *Backend) ensureResticSecret(ctx context.Context, params backend.EnsureParams) error {
	store := params.Store.Spec.S3
	scheme := "s3"
	if !store.TLS {
		scheme = "s3-insecure"
	}
	repoURL := fmt.Sprintf("%s:%s/%s/%s", scheme, store.Endpoint, store.Bucket, params.RepoPath)

	// Read credentials from the BackupStore's referenced Secret.
	credSecret := &corev1.Secret{}
	if err := b.client.Get(ctx, types.NamespacedName{
		Name:      store.CredentialsSecret.Name,
		Namespace: store.CredentialsSecret.Namespace,
	}, credSecret); err != nil {
		return fmt.Errorf("get BackupStore credentials secret: %w", err)
	}

	// Resolve the restic password.
	resticPassword, err := b.resolveResticPassword(ctx, params, credSecret)
	if err != nil {
		return fmt.Errorf("resolve restic password: %w", err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      params.ResticSecretName,
			Namespace: params.UnderlyingPVC.Namespace,
			Labels: map[string]string{
				"omnivol.smoothify.com/managed-by": "omnivol",
			},
			Annotations: map[string]string{
				"omnivol.smoothify.com/owner-pvc":           params.UserPVCName,
				"omnivol.smoothify.com/owner-pvc-namespace": params.UserPVCNamespace,
			},
		},
	}

	_, err = controllerutil.CreateOrUpdate(ctx, b.client, secret, func() error {
		if secret.Annotations == nil {
			secret.Annotations = map[string]string{}
		}
		secret.Annotations["omnivol.smoothify.com/owner-pvc"] = params.UserPVCName
		secret.Annotations["omnivol.smoothify.com/owner-pvc-namespace"] = params.UserPVCNamespace
		secret.StringData = map[string]string{
			"RESTIC_REPOSITORY":     repoURL,
			"RESTIC_PASSWORD":       resticPassword,
			"AWS_ACCESS_KEY_ID":     string(credSecret.Data["access-key-id"]),
			"AWS_SECRET_ACCESS_KEY": string(credSecret.Data["secret-access-key"]),
		}
		return nil
	})
	return err
}

// resolveResticPassword returns the restic encryption password for a PVC backup.
// Resolution order:
//  1. If BackupStore.spec.s3.resticPasswordSecretRef is set, read that secret+key.
//  2. Otherwise fall back to the key "restic-password" inside credentialsSecret.
func (b *Backend) resolveResticPassword(ctx context.Context, params backend.EnsureParams, credSecret *corev1.Secret) (string, error) {
	ref := params.Store.Spec.S3.ResticPasswordSecretRef
	if ref != nil {
		// Explicit secret reference — use the credentials secret's namespace as the
		// lookup namespace (SecretKeySelector has no Namespace field).
		ns := params.Store.Spec.S3.CredentialsSecret.Namespace
		s := &corev1.Secret{}
		if err := b.client.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: ns}, s); err != nil {
			return "", fmt.Errorf("get resticPasswordSecretRef %q/%q: %w", ns, ref.Name, err)
		}
		pw, ok := s.Data[ref.Key]
		if !ok {
			return "", fmt.Errorf("resticPasswordSecretRef secret %q/%q has no key %q", ns, ref.Name, ref.Key)
		}
		return string(pw), nil
	}

	// Fallback: "restic-password" key in the credentials secret.
	pw, ok := credSecret.Data["restic-password"]
	if !ok {
		return "", fmt.Errorf("credentialsSecret %q/%q has no key %q and resticPasswordSecretRef is not set",
			params.Store.Spec.S3.CredentialsSecret.Namespace,
			params.Store.Spec.S3.CredentialsSecret.Name,
			"restic-password")
	}
	return string(pw), nil
}

// buildResticRetainPolicy converts omnivol RetainPolicy to VolSync ResticRetainPolicy.
func buildResticRetainPolicy(params backend.EnsureParams) *volsyncv1alpha1.ResticRetainPolicy {
	r := params.Policy.Spec.Retain
	rp := &volsyncv1alpha1.ResticRetainPolicy{}
	if r.Hourly != nil {
		rp.Hourly = r.Hourly
	}
	if r.Daily != nil {
		rp.Daily = r.Daily
	}
	if r.Weekly != nil {
		rp.Weekly = r.Weekly
	}
	if r.Monthly != nil {
		rp.Monthly = r.Monthly
	}
	if r.Yearly != nil {
		rp.Yearly = r.Yearly
	}
	return rp
}

// ptr returns a pointer to v.
func ptr[T any](v T) *T { return &v }

// Ensure Backend implements the interface at compile time.
var _ backend.Interface = (*Backend)(nil)

// S3CheckBackupExists is a helper that wires the s3check client for use by the
// provisioner without going through the full backend interface.
func S3CheckBackupExists(ctx context.Context, params backend.EnsureParams, repoPath string) (bool, error) {
	store := params.Store.Spec.S3

	credSecret := &corev1.Secret{}
	if err := params.Client.Get(ctx, types.NamespacedName{
		Name:      store.CredentialsSecret.Name,
		Namespace: store.CredentialsSecret.Namespace,
	}, credSecret); err != nil {
		return false, fmt.Errorf("get credentials secret: %w", err)
	}

	c, err := s3check.NewClient(
		store.Endpoint,
		store.Bucket,
		string(credSecret.Data["access-key-id"]),
		string(credSecret.Data["secret-access-key"]),
		store.TLS,
	)
	if err != nil {
		return false, fmt.Errorf("create s3 client: %w", err)
	}

	return c.CheckBackupExists(ctx, repoPath)
}
