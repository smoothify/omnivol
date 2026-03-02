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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BackupStoreSpec defines the desired state of BackupStore.
// A BackupStore describes WHERE backups are stored — an S3-compatible endpoint
// and the credentials needed to access it.
type BackupStoreSpec struct {
	// s3 contains the S3-compatible storage configuration.
	// +required
	S3 S3Config `json:"s3"`
}

// S3Config holds the connection details for an S3-compatible backup store.
type S3Config struct {
	// endpoint is the S3-compatible endpoint URL (e.g. "s3.us-east-1.amazonaws.com" or a MinIO address).
	// Do not include a scheme — TLS is controlled by the tls field.
	// +required
	// +kubebuilder:validation:MinLength=1
	Endpoint string `json:"endpoint"`

	// bucket is the name of the S3 bucket where backups are stored.
	// +required
	// +kubebuilder:validation:MinLength=1
	Bucket string `json:"bucket"`

	// region is the AWS region (or equivalent) for the bucket.
	// +optional
	Region string `json:"region,omitempty"`

	// tls controls whether TLS is used when connecting to the endpoint.
	// Defaults to true.
	// +optional
	// +kubebuilder:default=true
	TLS bool `json:"tls,omitempty"`

	// credentialsSecret is a reference to a Secret containing the access key and
	// secret key for the S3 bucket.  The Secret must contain the keys
	// "access-key-id" and "secret-access-key".
	// +required
	CredentialsSecret corev1.SecretReference `json:"credentialsSecret"`

	// resticPasswordSecretRef references the secret key that holds the restic
	// repository encryption password.  If omitted the controller will look for
	// a key named "restic-password" inside credentialsSecret.
	// +optional
	ResticPasswordSecretRef *corev1.SecretKeySelector `json:"resticPasswordSecretRef,omitempty"`
}

// BackupStoreStatus defines the observed state of BackupStore.
type BackupStoreStatus struct {
	// conditions represent the current state of the BackupStore resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=bs
// +kubebuilder:printcolumn:name="Endpoint",type=string,JSONPath=`.spec.s3.endpoint`
// +kubebuilder:printcolumn:name="Bucket",type=string,JSONPath=`.spec.s3.bucket`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// BackupStore is the Schema for the backupstores API.
// A BackupStore describes WHERE backups are stored (S3 endpoint + credentials).
type BackupStore struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of BackupStore.
	// +required
	Spec BackupStoreSpec `json:"spec"`

	// status defines the observed state of BackupStore.
	// +optional
	Status BackupStoreStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// BackupStoreList contains a list of BackupStore.
type BackupStoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []BackupStore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&BackupStore{}, &BackupStoreList{})
}
