/*
Copyright 2026 The Wellcake Authors.
*/

package v1beta1

// Hub marks v1beta1 as the conversion hub (and storage version). Spoke versions
// (v1alpha1) convert to/from this type; see api/v1alpha1/conversion.go.
func (*ValkeyCluster) Hub() {}

// Hub marks v1beta1 ValkeyACL as the conversion hub.
func (*ValkeyACL) Hub() {}
