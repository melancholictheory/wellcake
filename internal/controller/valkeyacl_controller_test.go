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

package controller

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

var _ = Describe("ValkeyACL reconcile (envtest)", func() {
	const ns = "default"

	It("marks ClusterMissing when the referenced cluster does not exist", func() {
		acl := &cachev1beta1.ValkeyACL{
			ObjectMeta: metav1.ObjectMeta{Name: "acl-missing", Namespace: ns},
			Spec: cachev1beta1.ValkeyACLSpec{
				ClusterRef: corev1.LocalObjectReference{Name: "does-not-exist"},
				Users:      []cachev1beta1.ValkeyACLUser{{Name: "app", Rules: "on ~* +@read"}},
			},
		}
		Expect(k8sClient.Create(ctx, acl)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, acl) })

		r := &ValkeyACLReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		key := client.ObjectKeyFromObject(acl)
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		var got cachev1beta1.ValkeyACL
		Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
		cond := meta.FindStatusCondition(got.Status.Conditions, cachev1beta1.ConditionAvailable)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("ClusterMissing"))
	})

	It("requeues with ClusterNotReady when the target cluster has no primary yet", func() {
		vc := &cachev1beta1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "acl-target", Namespace: ns},
			Spec: cachev1beta1.ValkeyClusterSpec{
				Topology: cachev1beta1.TopologyReplication,
				Replicas: 3,
			},
		}
		Expect(k8sClient.Create(ctx, vc)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, vc) })

		acl := &cachev1beta1.ValkeyACL{
			ObjectMeta: metav1.ObjectMeta{Name: "acl-waiting", Namespace: ns},
			Spec: cachev1beta1.ValkeyACLSpec{
				ClusterRef: corev1.LocalObjectReference{Name: vc.Name},
				Users:      []cachev1beta1.ValkeyACLUser{{Name: "app", Rules: "on ~* +@read"}},
			},
		}
		Expect(k8sClient.Create(ctx, acl)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, acl) })

		r := &ValkeyACLReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		key := client.ObjectKeyFromObject(acl)
		res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(Equal(15 * time.Second))

		var got cachev1beta1.ValkeyACL
		Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
		cond := meta.FindStatusCondition(got.Status.Conditions, cachev1beta1.ConditionAvailable)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal("ClusterNotReady"))
	})
})
