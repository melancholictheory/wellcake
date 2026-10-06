/*
Copyright 2026 The Wellcake Authors.
*/

package v1beta1

import (
	"testing"

	"k8s.io/utils/ptr"
)

// The accessors must resolve an unset field to the same value the CRD default
// gives it, and honour an explicit false/0.
func TestOptionalFieldAccessors(t *testing.T) {
	for _, tc := range []struct {
		name string
		got  any
		want any
	}{
		{"auth block absent", (&ValkeyClusterSpec{}).AuthEnabled(), false},
		{"auth.enabled unset", (&ValkeyClusterSpec{Auth: &AuthSpec{}}).AuthEnabled(), true},
		{"auth.enabled false", (&ValkeyClusterSpec{Auth: &AuthSpec{Enabled: ptr.To(false)}}).AuthEnabled(), false},
		{"auth.enabled true", (&ValkeyClusterSpec{Auth: &AuthSpec{Enabled: ptr.To(true)}}).AuthEnabled(), true},
		{"autoReshard unset", (&ValkeyClusterSpec{}).AutoReshardEnabled(), true},
		{"autoReshard false", (&ValkeyClusterSpec{AutoReshard: ptr.To(false)}).AutoReshardEnabled(), false},
		{"retention unset", (&BackupSpec{}).RetentionCount(), int32(7)},
		{"retention 0", (&BackupSpec{Retention: ptr.To[int32](0)}).RetentionCount(), int32(0)},
		{"retention 3", (&BackupSpec{Retention: ptr.To[int32](3)}).RetentionCount(), int32(3)},
	} {
		if tc.got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, tc.got, tc.want)
		}
	}
}
