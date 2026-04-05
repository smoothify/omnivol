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

// Package metrics defines custom Prometheus metrics for Omnivol backup
// observability.  Gauges are registered with the controller-runtime metrics
// registry so they are automatically served on the manager's /metrics endpoint.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// BackupStatus reports the latest backup outcome for each PVC.
	// 1 = last backup succeeded, 0 = last backup failed / unknown.
	BackupStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "omnivol",
			Subsystem: "backup",
			Name:      "status",
			Help:      "Latest backup outcome per PVC (1 = success, 0 = failure).",
		},
		[]string{"pvc", "namespace", "backup_policy"},
	)

	// BackupSizeBytes reports the total size (in bytes) of the restic
	// repository on S3 for each PVC.
	BackupSizeBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "omnivol",
			Subsystem: "backup",
			Name:      "size_bytes",
			Help:      "Total S3 repository size in bytes per PVC.",
		},
		[]string{"pvc", "namespace", "backup_policy"},
	)

	// BackupMissedSchedule is 1 when the most recent scheduled backup was
	// not completed before the next scheduled time, 0 otherwise.
	BackupMissedSchedule = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "omnivol",
			Subsystem: "backup",
			Name:      "missed_schedule",
			Help:      "Whether the last scheduled backup was missed (1 = missed, 0 = on time).",
		},
		[]string{"pvc", "namespace", "backup_policy"},
	)

	// BackupLastSuccessTimestamp records the unix timestamp of the last
	// successful backup for each PVC.
	BackupLastSuccessTimestamp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "omnivol",
			Subsystem: "backup",
			Name:      "last_success_timestamp",
			Help:      "Unix timestamp of the last successful backup per PVC.",
		},
		[]string{"pvc", "namespace", "backup_policy"},
	)

	// BackupDurationSeconds records the duration of the last backup sync.
	BackupDurationSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "omnivol",
			Subsystem: "backup",
			Name:      "duration_seconds",
			Help:      "Duration of the last backup sync in seconds per PVC.",
		},
		[]string{"pvc", "namespace", "backup_policy"},
	)

	// ManagedPVCCount reports the total number of PVCs managed per BackupPolicy.
	ManagedPVCCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "omnivol",
			Name:      "managed_pvc_count",
			Help:      "Number of PVCs managed by each BackupPolicy.",
		},
		[]string{"backup_policy"},
	)

	// VolumeCapacityBytes reports the provisioned capacity of each Omnivol-managed
	// PVC in bytes.  For actual filesystem usage, join with the kubelet metric
	// kubelet_volume_stats_used_bytes{persistentvolumeclaim="<name>"}.
	VolumeCapacityBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "omnivol",
			Subsystem: "volume",
			Name:      "capacity_bytes",
			Help:      "Provisioned capacity of the PVC in bytes.",
		},
		[]string{"pvc", "namespace", "backup_policy"},
	)
)

func init() {
	metrics.Registry.MustRegister(
		BackupStatus,
		BackupSizeBytes,
		BackupMissedSchedule,
		BackupLastSuccessTimestamp,
		BackupDurationSeconds,
		ManagedPVCCount,
		VolumeCapacityBytes,
	)
}
