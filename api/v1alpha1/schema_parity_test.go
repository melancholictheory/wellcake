/*
Copyright 2026 The Wellcake Authors.
*/

package v1alpha1

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// There is no conversion webhook: the API server stores the object once and,
// on every write through a version, prunes the fields that version's schema
// does not know and enforces only that version's rules. A field that exists in
// v1beta1 only is wiped by any v1alpha1 write (the valkey-cluster chart renders
// v1alpha1), as status.lastPasswordRotationToken was in #99, which replayed a
// password rotation. Both versions of each generated CRD must have the same
// schema: fields, defaults, enums and CEL rules (descriptions aside).
func TestCRDSchemaParity(t *testing.T) {
	for _, file := range []string{"cache.wellcake.io_valkeyclusters.yaml", "cache.wellcake.io_valkeyacls.yaml"} {
		raw, err := os.ReadFile(filepath.Join("..", "..", "config", "crd", "bases", file))
		if err != nil {
			t.Fatal(err)
		}
		var crd struct {
			Spec struct {
				Versions []struct {
					Name   string `json:"name"`
					Schema struct {
						OpenAPIV3Schema any `json:"openAPIV3Schema"`
					} `json:"schema"`
				} `json:"versions"`
			} `json:"spec"`
		}
		if err := yaml.Unmarshal(raw, &crd); err != nil {
			t.Fatal(err)
		}
		schemas := map[string]any{}
		for _, v := range crd.Spec.Versions {
			schemas[v.Name] = normalize(v.Schema.OpenAPIV3Schema)
		}
		if len(schemas) != 2 {
			t.Fatalf("%s: want v1alpha1 and v1beta1, got %d versions", file, len(schemas))
		}
		var diffs []string
		compare(schemas["v1alpha1"], schemas["v1beta1"], "", &diffs)
		if len(diffs) > 0 {
			t.Errorf("%s: v1alpha1 and v1beta1 schemas differ:\n  %s", file, strings.Join(diffs, "\n  "))
		}
	}
}

// normalize drops descriptions and sorts CEL rule lists, whose order does not
// change what the API server enforces.
func normalize(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, val := range x {
			if k == "description" {
				continue
			}
			out[k] = normalize(val)
		}
		if rules, ok := out["x-kubernetes-validations"].([]any); ok {
			sort.Slice(rules, func(i, j int) bool { return jsonString(rules[i]) < jsonString(rules[j]) })
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i := range x {
			out[i] = normalize(x[i])
		}
		return out
	}
	return v
}

func compare(alpha, beta any, path string, diffs *[]string) {
	a, aok := alpha.(map[string]any)
	b, bok := beta.(map[string]any)
	if aok && bok {
		keys := map[string]bool{}
		for k := range a {
			keys[k] = true
		}
		for k := range b {
			keys[k] = true
		}
		sorted := make([]string, 0, len(keys))
		for k := range keys {
			sorted = append(sorted, k)
		}
		sort.Strings(sorted)
		for _, k := range sorted {
			av, ain := a[k]
			bv, bin := b[k]
			switch {
			case !ain:
				*diffs = append(*diffs, fmt.Sprintf("%s.%s: only in v1beta1", path, k))
			case !bin:
				*diffs = append(*diffs, fmt.Sprintf("%s.%s: only in v1alpha1", path, k))
			default:
				compare(av, bv, path+"."+k, diffs)
			}
		}
		return
	}
	if jsonString(alpha) != jsonString(beta) {
		*diffs = append(*diffs, fmt.Sprintf("%s: v1alpha1=%.120s v1beta1=%.120s", path, jsonString(alpha), jsonString(beta)))
	}
}

func jsonString(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
