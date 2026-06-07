/*
Copyright 2026 The Wellcake Authors.
*/

package main

import (
	"sort"
	"testing"
)

func TestWatchNamespaces(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want []string // nil means cluster-scoped (all namespaces)
	}{
		{"empty → all namespaces", "", nil},
		{"whitespace → all namespaces", "   ", nil},
		{"single", "valkey-system", []string{"valkey-system"}},
		{"multi", "ns-a,ns-b,ns-c", []string{"ns-a", "ns-b", "ns-c"}},
		{"trims spaces and drops empties", " ns-a , , ns-b ,", []string{"ns-a", "ns-b"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := watchNamespaces(tc.raw)
			if tc.want == nil {
				if got != nil {
					t.Fatalf("watchNamespaces(%q) = %v, want nil (cluster-scoped)", tc.raw, got)
				}
				return
			}
			if len(got) != len(tc.want) {
				t.Fatalf("watchNamespaces(%q) has %d entries, want %d: %v", tc.raw, len(got), len(tc.want), got)
			}
			gotKeys := make([]string, 0, len(got))
			for k := range got {
				gotKeys = append(gotKeys, k)
			}
			sort.Strings(gotKeys)
			for i, w := range tc.want {
				if gotKeys[i] != w {
					t.Errorf("watchNamespaces(%q) key[%d] = %q, want %q", tc.raw, i, gotKeys[i], w)
				}
			}
		})
	}
}
