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

package drain

import (
	"context"
	"strings"
	"testing"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = volsyncv1alpha1.AddToScheme(s)
	return s
}

// --- pvPinnedToNode ---

func TestPvPinnedToNode_MatchingHostname(t *testing.T) {
	pv := &corev1.PersistentVolume{
		Spec: corev1.PersistentVolumeSpec{
			NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      "kubernetes.io/hostname",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"worker-1"},
						}},
					}},
				},
			},
		},
	}
	if !pvPinnedToNode(pv, "worker-1") {
		t.Error("expected PV to be pinned to worker-1")
	}
}

func TestPvPinnedToNode_NoMatch(t *testing.T) {
	pv := &corev1.PersistentVolume{
		Spec: corev1.PersistentVolumeSpec{
			NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      "kubernetes.io/hostname",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"worker-2"},
						}},
					}},
				},
			},
		},
	}
	if pvPinnedToNode(pv, "worker-1") {
		t.Error("expected PV NOT to be pinned to worker-1")
	}
}

func TestPvPinnedToNode_NilAffinity(t *testing.T) {
	pv := &corev1.PersistentVolume{Spec: corev1.PersistentVolumeSpec{}}
	if pvPinnedToNode(pv, "worker-1") {
		t.Error("expected PV with nil affinity NOT to be pinned")
	}
}

func TestPvPinnedToNode_MatchFields(t *testing.T) {
	pv := &corev1.PersistentVolume{
		Spec: corev1.PersistentVolumeSpec{
			NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchFields: []corev1.NodeSelectorRequirement{{
							Key:      "metadata.name",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"worker-1"},
						}},
					}},
				},
			},
		},
	}
	if !pvPinnedToNode(pv, "worker-1") {
		t.Error("expected PV to be pinned via MatchFields")
	}
}

// --- Reconcile ---

func TestReconcile_CordonedNodeTriggersSyncForPinnedPVC(t *testing.T) {
	ctx := context.Background()

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-1"},
		Spec:       corev1.NodeSpec{Unschedulable: true},
	}

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-data"},
		Spec: corev1.PersistentVolumeSpec{
			NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      "kubernetes.io/hostname",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"worker-1"},
						}},
					}},
				},
			},
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data",
			Namespace: "prod",
			Labels:    map[string]string{labelBackupPolicy: "hourly"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-data",
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}

	rs := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "prod"},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(node, pv, pvc, rs).Build()
	w := &Watcher{Client: c}

	_, err := w.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "worker-1"},
	})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	// Verify the RS was patched with a manual trigger.
	updated := &volsyncv1alpha1.ReplicationSource{}
	if err := c.Get(ctx, types.NamespacedName{Name: "data", Namespace: "prod"}, updated); err != nil {
		t.Fatalf("get RS: %v", err)
	}
	if updated.Spec.Trigger == nil || updated.Spec.Trigger.Manual == "" {
		t.Fatal("expected manual trigger to be set on RS")
	}
	if !strings.HasPrefix(updated.Spec.Trigger.Manual, "pre-drain-worker-1-") {
		t.Errorf("manual trigger = %q, want prefix 'pre-drain-worker-1-'", updated.Spec.Trigger.Manual)
	}
}

func TestReconcile_SkipsNonCordonedNode(t *testing.T) {
	ctx := context.Background()

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-1"},
		Spec:       corev1.NodeSpec{Unschedulable: false},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(node).Build()
	w := &Watcher{Client: c}

	_, err := w.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "worker-1"},
	})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
}

func TestReconcile_SkipsPVCOnDifferentNode(t *testing.T) {
	ctx := context.Background()

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-1"},
		Spec:       corev1.NodeSpec{Unschedulable: true},
	}

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-other"},
		Spec: corev1.PersistentVolumeSpec{
			NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      "kubernetes.io/hostname",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"worker-2"},
						}},
					}},
				},
			},
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "other",
			Namespace: "prod",
			Labels:    map[string]string{labelBackupPolicy: "hourly"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-other",
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}

	rs := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "prod"},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(node, pv, pvc, rs).Build()
	w := &Watcher{Client: c}

	_, err := w.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "worker-1"},
	})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	// RS should NOT have been patched.
	updated := &volsyncv1alpha1.ReplicationSource{}
	if err := c.Get(ctx, types.NamespacedName{Name: "other", Namespace: "prod"}, updated); err != nil {
		t.Fatalf("get RS: %v", err)
	}
	if updated.Spec.Trigger != nil && updated.Spec.Trigger.Manual != "" {
		t.Errorf("expected no manual trigger on RS for different node, got %q", updated.Spec.Trigger.Manual)
	}
}

func TestReconcile_MissingNode(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(newScheme()).Build()
	w := &Watcher{Client: c}

	_, err := w.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "nonexistent"},
	})
	if err != nil {
		t.Fatalf("expected no error for missing node, got: %v", err)
	}
}

// --- triggerManualSync ---

func TestTriggerManualSync_PatchesRS(t *testing.T) {
	ctx := context.Background()

	rs := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "prod"},
	}

	c := fake.NewClientBuilder().WithScheme(newScheme()).
		WithObjects(rs).Build()
	w := &Watcher{Client: c}

	if err := w.triggerManualSync(ctx, "data", "prod", "worker-1"); err != nil {
		t.Fatalf("triggerManualSync() error = %v", err)
	}

	updated := &volsyncv1alpha1.ReplicationSource{}
	if err := c.Get(ctx, types.NamespacedName{Name: "data", Namespace: "prod"}, updated); err != nil {
		t.Fatalf("get RS: %v", err)
	}
	if updated.Spec.Trigger == nil || !strings.HasPrefix(updated.Spec.Trigger.Manual, "pre-drain-worker-1-") {
		t.Errorf("expected manual trigger with prefix, got %v", updated.Spec.Trigger)
	}
}

func TestTriggerManualSync_MissingRS(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(newScheme()).Build()
	w := &Watcher{Client: c}

	// Should not error — IgnoreNotFound.
	if err := w.triggerManualSync(ctx, "missing", "prod", "worker-1"); err != nil {
		t.Fatalf("expected no error for missing RS, got: %v", err)
	}
}
