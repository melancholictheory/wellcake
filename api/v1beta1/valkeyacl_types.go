/*
Copyright 2026 The Wellcake Authors.
*/

package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ValkeyACLSpec defines the desired state of ValkeyACL.
type ValkeyACLSpec struct {
	// ClusterRef points at the target ValkeyCluster in the same namespace.
	// The operator applies ACL changes through that cluster's primary.
	// +required
	ClusterRef corev1.LocalObjectReference `json:"clusterRef"`

	// Users to ensure on the target cluster. Removing a user from this list
	// will delete it on the next reconcile (ACL DELUSER).
	// +optional
	Users []ValkeyACLUser `json:"users,omitempty"`
}

// ValkeyACLUser describes a single Valkey ACL user.
type ValkeyACLUser struct {
	// Name is the user name (must be unique within the cluster).
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// PasswordSecret optionally references a Secret containing the user's
	// password. If unset the user is created with `nopass` (only do this
	// for read-only or restricted command sets).
	// +optional
	PasswordSecret *SecretKeyReference `json:"passwordSecret,omitempty"`

	// Rules is a free-form Valkey ACL rule string, e.g.
	//   "on ~* &* +@read -@dangerous"
	// See https://valkey.io/topics/acl for syntax.
	// +kubebuilder:default="off"
	// +optional
	Rules string `json:"rules,omitempty"`
}

// SecretKeyReference picks a single key from a Secret.
type SecretKeyReference struct {
	Name string `json:"name"`
	// +kubebuilder:default=password
	Key string `json:"key,omitempty"`
}

// ValkeyACLStatus defines the observed state of ValkeyACL.
type ValkeyACLStatus struct {
	// AppliedUsers lists the user names successfully applied on the cluster
	// at the last reconcile.
	// +optional
	AppliedUsers []string `json:"appliedUsers,omitempty"`

	// ObservedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the current state of the ValkeyACL resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:resource:shortName=vkacl
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef.name`
// +kubebuilder:printcolumn:name="Users",type=integer,JSONPath=`.status.appliedUsers`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ValkeyACL is the Schema for the valkeyacls API.
type ValkeyACL struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec ValkeyACLSpec `json:"spec"`
	// +optional
	Status ValkeyACLStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ValkeyACLList contains a list of ValkeyACL.
type ValkeyACLList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ValkeyACL `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ValkeyACL{}, &ValkeyACLList{})
}
