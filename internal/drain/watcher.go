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

// Package drain watches Node cordon events and triggers a manual VolSync sync
// for any PV managed by omnivol that is pinned to the cordoned node.
package drain

import (
	"context"
	"fmt"
	"slices"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const (
	labelManagedBy      = "omnivol.smoothify.com/managed-by"
	labelManagedByValue = "omnivol"
)

// Watcher reconciles Node objects.  When a Node becomes unschedulable (cordoned)
// it locates all PVs managed by omnivol with node affinity matching that node and
// fires a manual pre-drain sync for each.
//
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch
// +kubebuilder:rbac:groups=volsync.backube,resources=replicationsources,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
type Watcher struct {
	client.Client
}

// SetupWithManager registers the drain Watcher with the controller manager.
// It only fires on transitions to Unschedulable=true to avoid redundant syncs.
func (w *Watcher) SetupWithManager(mgr ctrl.Manager) error {
	// Only reconcile when a Node transitions to unschedulable.
	cordonPredicate := predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldNode, ok := e.ObjectOld.(*corev1.Node)
			if !ok {
				return false
			}
			newNode, ok := e.ObjectNew.(*corev1.Node)
			if !ok {
				return false
			}
			// Trigger only on the false→true transition.
			return !oldNode.Spec.Unschedulable && newNode.Spec.Unschedulable
		},
		CreateFunc:  func(_ event.CreateEvent) bool { return false },
		DeleteFunc:  func(_ event.DeleteEvent) bool { return false },
		GenericFunc: func(_ event.GenericEvent) bool { return false },
	}

	return ctrl.NewControllerManagedBy(mgr).
		Named("drain-watcher").
		For(&corev1.Node{}).
		WithEventFilter(cordonPredicate).
		Complete(w)
}

// Reconcile is called when a Node is cordoned.  It finds all omnivol-managed PVs
// pinned to that node and triggers a pre-drain manual sync on each.
func (w *Watcher) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	node := &corev1.Node{}
	if err := w.Get(ctx, req.NamespacedName, node); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !node.Spec.Unschedulable {
		return ctrl.Result{}, nil
	}

	logger.Info("Node cordoned — scanning for omnivol PVs", "node", node.Name)

	pvList := &corev1.PersistentVolumeList{}
	if err := w.List(ctx, pvList, client.MatchingLabels{labelManagedBy: labelManagedByValue}); err != nil {
		return ctrl.Result{}, err
	}

	var syncErrors []error
	for _, pv := range pvList.Items {
		if !pvPinnedToNode(&pv, node.Name) {
			continue
		}

		underlyingName := pv.Annotations["omnivol.smoothify.com/underlying-pvc"]
		underlyingNS := pv.Annotations["omnivol.smoothify.com/underlying-namespace"]
		if underlyingName == "" || underlyingNS == "" {
			continue
		}

		logger.Info("Triggering pre-drain sync", "pv", pv.Name, "underlyingPVC", underlyingName, "node", node.Name)
		if err := w.triggerManualSync(ctx, underlyingName, underlyingNS, node.Name); err != nil {
			logger.Error(err, "Could not trigger pre-drain sync", "pv", pv.Name)
			syncErrors = append(syncErrors, err)
		}
	}

	if len(syncErrors) > 0 {
		return ctrl.Result{}, fmt.Errorf("one or more pre-drain syncs failed: %v", syncErrors)
	}
	return ctrl.Result{}, nil
}

// pvPinnedToNode returns true when the PV's node affinity matches nodeName.
// It checks all topology keys (works with kubernetes.io/hostname,
// topology.topolvm.io/node, openebs.io/nodename, etc.).
func pvPinnedToNode(pv *corev1.PersistentVolume, nodeName string) bool {
	if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
		return false
	}
	for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
		for _, expr := range term.MatchExpressions {
			if expr.Operator == corev1.NodeSelectorOpIn && slices.Contains(expr.Values, nodeName) {
				return true
			}
		}
		for _, field := range term.MatchFields {
			if field.Key == "metadata.name" && slices.Contains(field.Values, nodeName) {
				return true
			}
		}
	}
	return false
}

// triggerManualSync patches the ReplicationSource with a unique manual trigger value
// and returns without waiting (fire-and-forget per PLAN.md).
func (w *Watcher) triggerManualSync(ctx context.Context, rsName, namespace, nodeName string) error {
	rs := &volsyncv1alpha1.ReplicationSource{}
	if err := w.Get(ctx, types.NamespacedName{Name: rsName, Namespace: namespace}, rs); err != nil {
		return client.IgnoreNotFound(err)
	}

	triggerValue := fmt.Sprintf("pre-drain-%s-%d", nodeName, time.Now().Unix())
	patch := client.MergeFrom(rs.DeepCopy())
	if rs.Spec.Trigger == nil {
		rs.Spec.Trigger = &volsyncv1alpha1.ReplicationSourceTriggerSpec{}
	}
	rs.Spec.Trigger.Manual = triggerValue
	return w.Patch(ctx, rs, patch)
}
