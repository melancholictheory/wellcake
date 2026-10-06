/*
Copyright 2026 The Wellcake Authors.

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

package v1beta1

import (
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
	// TODO (user): Add any additional imports if needed
)

var _ = Describe("ValkeyCluster Webhook", func() {
	var (
		obj       *cachev1beta1.ValkeyCluster
		oldObj    *cachev1beta1.ValkeyCluster
		validator ValkeyClusterCustomValidator
	)

	BeforeEach(func() {
		obj = &cachev1beta1.ValkeyCluster{}
		oldObj = &cachev1beta1.ValkeyCluster{}
		validator = ValkeyClusterCustomValidator{}
		Expect(validator).NotTo(BeNil(), "Expected validator to be initialized")
		Expect(oldObj).NotTo(BeNil(), "Expected oldObj to be initialized")
		Expect(obj).NotTo(BeNil(), "Expected obj to be initialized")
	})

	AfterEach(func() {
		// TODO (user): Add any teardown logic common to all tests
	})

	Context("When creating or updating ValkeyCluster under Validating Webhook", func() {
		// TODO (user): Add logic for validating webhooks
		// Example:
		// It("Should deny creation if a required field is missing", func() {
		//     By("simulating an invalid creation scenario")
		//     obj.SomeRequiredField = ""
		//     Expect(validator.ValidateCreate(ctx, obj)).Error().To(HaveOccurred())
		// })
		//
		// It("Should admit creation if all required fields are present", func() {
		//     By("simulating an invalid creation scenario")
		//     obj.SomeRequiredField = "valid_value"
		//     Expect(validator.ValidateCreate(ctx, obj)).To(BeNil())
		// })
		//
		// It("Should validate updates correctly", func() {
		//     By("simulating a valid update scenario")
		//     oldObj.SomeRequiredField = "updated_value"
		//     obj.SomeRequiredField = "updated_value"
		//     Expect(validator.ValidateUpdate(ctx, oldObj, obj)).To(BeNil())
		// })
	})

})

// The mutating webhook decodes the request into the typed object, runs
// Default() and answers with a JSON patch against the original request. A field
// the typed object drops on re-encoding would be removed by that patch and then
// re-defaulted by the API server; controller-runtime skips those removals unless
// DefaulterRemoveUnknownOrOmitableFields is set. Guard that explicit false/0
// survive admission whatever the encoding of these fields.
var _ = Describe("ValkeyCluster defaulting webhook keeps zero values", func() {
	It("does not turn an explicit false/0 into the CRD default", func() {
		gvk := cachev1beta1.GroupVersion.WithKind("ValkeyCluster")
		u := &unstructured.Unstructured{Object: map[string]any{
			"metadata": map[string]any{"name": "webhook-zero-values", "namespace": "default"},
			"spec": map[string]any{
				"topology":            "Replication",
				"replicas":            int64(3),
				"autoReshard":         false,
				"auth":                map[string]any{"enabled": false},
				"podDisruptionBudget": map[string]any{"enabled": false},
				"backup": map[string]any{
					"enabled":   false,
					"retention": int64(0),
					"s3":        map[string]any{"bucket": "b", "credentialsSecret": "s3creds"},
				},
			},
		}}
		u.SetGroupVersionKind(gvk)
		Expect(k8sClient.Create(ctx, u)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, u) })

		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(gvk)
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(u), got)).To(Succeed())
		var lost []string
		for _, path := range [][]string{
			{"spec", "autoReshard"},
			{"spec", "auth", "enabled"},
			{"spec", "podDisruptionBudget", "enabled"},
		} {
			if v, found, err := unstructured.NestedBool(got.Object, path...); err != nil || !found || v {
				lost = append(lost, fmt.Sprintf("%s=%v (found=%v)", strings.Join(path, "."), v, found))
			}
		}
		if v, found, err := unstructured.NestedInt64(got.Object, "spec", "backup", "retention"); err != nil || !found || v != 0 {
			lost = append(lost, fmt.Sprintf("spec.backup.retention=%d (found=%v)", v, found))
		}
		Expect(lost).To(BeEmpty(), "zero values lost through the defaulting webhook")

		// Default() still runs: an unset field gets its value.
		Expect(got.Object["spec"].(map[string]any)["imagePullPolicy"]).To(Equal("IfNotPresent"))
	})
})
