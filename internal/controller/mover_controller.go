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

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// labelMoverPod is applied to VolSync mover pods by the VolSync operator.
	// The MoverReconciler watches for pods carrying this label and injects
	// the cordon toleration.
	labelMoverPod = "app.kubernetes.io/created-by"

	// labelMoverPodValue is the expected value for the labelMoverPod.
	labelMoverPodValue = "volsync"

	// annTolerationPatched marks a pod as already patched so we don't
	// re-patch on every reconcile.
	annTolerationPatched = "omnivol.smoothify.com/toleration-patched"
)

// MoverReconciler watches VolSync mover pods (identified by the
// omnivol.smoothify.com/mover label) and patches in the
// node.kubernetes.io/unschedulable:NoSchedule toleration.
//
// This is necessary because VolSync v0.14 MoverConfig does not expose
// a Tolerations field.  Without this toleration the mover pod cannot
// be scheduled on a cordoned (draining) node, which is exactly when
// the "final backup before drain" needs to run.
//
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch
type MoverReconciler struct {
	client.Client
}

// SetupWithManager registers the MoverReconciler.
func (r *MoverReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("mover-toleration").
		Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(enqueueIfMover),
		).
		Complete(r)
}

// enqueueIfMover only enqueues pods that carry the volsync created-by label.
func enqueueIfMover(_ context.Context, obj client.Object) []reconcile.Request {
	if obj.GetLabels()[labelMoverPod] != labelMoverPodValue {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: client.ObjectKeyFromObject(obj),
	}}
}

// Reconcile patches the cordon toleration onto a mover pod if it is
// missing and the pod has not already been patched.
func (r *MoverReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	pod := &corev1.Pod{}
	if err := r.Get(ctx, req.NamespacedName, pod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Skip if already patched or not a mover pod.
	if pod.Labels[labelMoverPod] != labelMoverPodValue {
		return ctrl.Result{}, nil
	}
	if pod.Annotations[annTolerationPatched] == annValueTrue {
		return ctrl.Result{}, nil
	}

	// Check if the toleration is already present.
	for _, t := range pod.Spec.Tolerations {
		if t.Key == "node.kubernetes.io/unschedulable" {
			return ctrl.Result{}, nil
		}
	}

	// Patch the toleration onto the pod.
	logger.Info("Patching cordon toleration onto mover pod",
		"pod", pod.Name, "namespace", pod.Namespace)

	patch := client.MergeFrom(pod.DeepCopy())
	pod.Spec.Tolerations = append(pod.Spec.Tolerations, corev1.Toleration{
		Key:      "node.kubernetes.io/unschedulable",
		Operator: corev1.TolerationOpExists,
		Effect:   corev1.TaintEffectNoSchedule,
	})
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[annTolerationPatched] = annValueTrue
	if err := r.Patch(ctx, pod, patch); err != nil {
		logger.Error(err, "Failed to patch toleration onto mover pod")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}
