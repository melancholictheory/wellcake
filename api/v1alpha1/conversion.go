/*
Copyright 2026 The Wellcake Authors.
*/

package v1alpha1

import (
	"encoding/json"

	"sigs.k8s.io/controller-runtime/pkg/conversion"

	v1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// v1alpha1 is a conversion spoke; v1beta1 is the hub. The two API versions are
// structurally identical today, so conversion is a field-wise copy of
// ObjectMeta plus a JSON round-trip of Spec/Status (TypeMeta is left to the
// conversion framework). When the schemas diverge, replace the round-trip with
// explicit per-field mapping.

func jsonRoundTrip(src, dst any) error {
	b, err := json.Marshal(src)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dst)
}

// ConvertTo converts this v1alpha1 ValkeyCluster to the v1beta1 hub.
func (src *ValkeyCluster) ConvertTo(dstRaw conversion.Hub) error {
	dst := dstRaw.(*v1beta1.ValkeyCluster)
	dst.ObjectMeta = src.ObjectMeta
	if err := jsonRoundTrip(&src.Spec, &dst.Spec); err != nil {
		return err
	}
	return jsonRoundTrip(&src.Status, &dst.Status)
}

// ConvertFrom converts the v1beta1 hub into this v1alpha1 ValkeyCluster.
func (dst *ValkeyCluster) ConvertFrom(srcRaw conversion.Hub) error {
	src := srcRaw.(*v1beta1.ValkeyCluster)
	dst.ObjectMeta = src.ObjectMeta
	if err := jsonRoundTrip(&src.Spec, &dst.Spec); err != nil {
		return err
	}
	return jsonRoundTrip(&src.Status, &dst.Status)
}

// ConvertTo converts this v1alpha1 ValkeyACL to the v1beta1 hub.
func (src *ValkeyACL) ConvertTo(dstRaw conversion.Hub) error {
	dst := dstRaw.(*v1beta1.ValkeyACL)
	dst.ObjectMeta = src.ObjectMeta
	if err := jsonRoundTrip(&src.Spec, &dst.Spec); err != nil {
		return err
	}
	return jsonRoundTrip(&src.Status, &dst.Status)
}

// ConvertFrom converts the v1beta1 hub into this v1alpha1 ValkeyACL.
func (dst *ValkeyACL) ConvertFrom(srcRaw conversion.Hub) error {
	src := srcRaw.(*v1beta1.ValkeyACL)
	dst.ObjectMeta = src.ObjectMeta
	if err := jsonRoundTrip(&src.Spec, &dst.Spec); err != nil {
		return err
	}
	return jsonRoundTrip(&src.Status, &dst.Status)
}
