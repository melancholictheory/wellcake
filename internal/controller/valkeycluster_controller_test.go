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
	"fmt"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// reconcileToStable drives the reconciler against the real API server until it
// stops requeueing — the reconcile returns a zero Result once the work pass is
// done (no pods become Ready under envtest: there is no kubelet, so
// failover/requeue-after never engages).
func reconcileToStable(r *ValkeyClusterReconciler, key types.NamespacedName) {
	GinkgoHelper()
	for range 8 {
		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		if res.RequeueAfter == 0 {
			return
		}
	}
	Fail("reconcile did not stabilize within 8 passes")
}

// deleteAndFinalize deletes a ValkeyCluster and runs one reconcile pass so the
// finalizer is removed and the API server can garbage-collect it. Safe to defer.
func deleteAndFinalize(r *ValkeyClusterReconciler, vc *cachev1beta1.ValkeyCluster) {
	GinkgoHelper()
	_ = k8sClient.Delete(ctx, vc)
	key := client.ObjectKeyFromObject(vc)
	_, _ = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
}

var _ = Describe("ValkeyCluster CRD schema (envtest)", func() {
	const ns = "default"

	Context("CRD defaulting applied by the API server", func() {
		It("fills topology, profile, image and replicas on a bare spec", func() {
			vc := &cachev1beta1.ValkeyCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "defaults-min", Namespace: ns},
			}
			Expect(k8sClient.Create(ctx, vc)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, vc) })

			var got cachev1beta1.ValkeyCluster
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vc), &got)).To(Succeed())
			Expect(got.Spec.Topology).To(Equal(cachev1beta1.TopologyReplication))
			Expect(got.Spec.Profile).To(Equal(cachev1beta1.ProfileCache))
			Expect(got.Spec.Image).To(Equal("valkey/valkey:8.0"))
			Expect(got.Spec.Replicas).To(Equal(int32(3)))
		})
	})

	Context("CEL conditional-field rules", func() {
		It("rejects Cluster topology without shards", func() {
			vc := &cachev1beta1.ValkeyCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-cluster-noshards", Namespace: ns},
				Spec:       cachev1beta1.ValkeyClusterSpec{Topology: cachev1beta1.TopologyCluster},
			}
			err := k8sClient.Create(ctx, vc)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("shards is required when topology is Cluster"))
		})

		It("rejects shards on a non-Cluster topology", func() {
			vc := &cachev1beta1.ValkeyCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-repl-shards", Namespace: ns},
				Spec: cachev1beta1.ValkeyClusterSpec{
					Topology: cachev1beta1.TopologyReplication,
					Shards:   ptr.To[int32](3),
				},
			}
			err := k8sClient.Create(ctx, vc)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("shards is only valid when topology is Cluster"))
		})

		It("rejects replicasPerShard on a non-Cluster topology (SC5)", func() {
			vc := &cachev1beta1.ValkeyCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-repl-rps", Namespace: ns},
				Spec: cachev1beta1.ValkeyClusterSpec{
					Topology:         cachev1beta1.TopologyReplication,
					ReplicasPerShard: ptr.To[int32](1),
				},
			}
			err := k8sClient.Create(ctx, vc)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("replicasPerShard is only valid when topology is Cluster"))
		})

		It("rejects Sentinel topology without a sentinel block", func() {
			vc := &cachev1beta1.ValkeyCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-sentinel-nospec", Namespace: ns},
				Spec:       cachev1beta1.ValkeyClusterSpec{Topology: cachev1beta1.TopologySentinel},
			}
			err := k8sClient.Create(ctx, vc)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("sentinel is required when topology is Sentinel"))
		})

		It("rejects replicateFrom on Cluster topology", func() {
			vc := &cachev1beta1.ValkeyCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-cluster-replfrom", Namespace: ns},
				Spec: cachev1beta1.ValkeyClusterSpec{
					Topology:      cachev1beta1.TopologyCluster,
					Shards:        ptr.To[int32](3),
					ReplicateFrom: &cachev1beta1.ReplicateFromSpec{Host: "upstream", Port: 6379},
				},
			}
			err := k8sClient.Create(ctx, vc)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("replicateFrom only supported"))
		})
	})

	Context("CEL immutability rules", func() {
		It("rejects changing topology after creation", func() {
			vc := &cachev1beta1.ValkeyCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "immut-topology", Namespace: ns},
				Spec:       cachev1beta1.ValkeyClusterSpec{Topology: cachev1beta1.TopologyReplication},
			}
			Expect(k8sClient.Create(ctx, vc)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, vc) })

			var got cachev1beta1.ValkeyCluster
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vc), &got)).To(Succeed())
			got.Spec.Topology = cachev1beta1.TopologyStandalone
			err := k8sClient.Update(ctx, &got)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("topology is immutable"))
		})

		It("rejects shrinking storage size", func() {
			vc := &cachev1beta1.ValkeyCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "immut-storage", Namespace: ns},
				Spec: cachev1beta1.ValkeyClusterSpec{
					Topology: cachev1beta1.TopologyReplication,
					Storage:  &cachev1beta1.StorageSpec{Size: resource.MustParse("10Gi")},
				},
			}
			Expect(k8sClient.Create(ctx, vc)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, vc) })

			var got cachev1beta1.ValkeyCluster
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vc), &got)).To(Succeed())
			got.Spec.Storage.Size = resource.MustParse("5Gi")
			err := k8sClient.Update(ctx, &got)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("storage size cannot shrink"))
		})

		It("rejects changing storage persistence mode (SC4)", func() {
			vc := &cachev1beta1.ValkeyCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "immut-storage-mode", Namespace: ns},
				Spec: cachev1beta1.ValkeyClusterSpec{
					Topology: cachev1beta1.TopologyReplication,
					Storage:  &cachev1beta1.StorageSpec{Size: resource.MustParse("10Gi"), Mode: "rdb"},
				},
			}
			Expect(k8sClient.Create(ctx, vc)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, vc) })

			var got cachev1beta1.ValkeyCluster
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vc), &got)).To(Succeed())
			got.Spec.Storage.Mode = "aof"
			err := k8sClient.Update(ctx, &got)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("storage mode is immutable"))
		})
	})
})

var _ = Describe("ValkeyCluster reconcile (envtest)", func() {
	const ns = "default"

	It("creates owned objects with controller references and sets status", func() {
		vc := &cachev1beta1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "rcache", Namespace: ns},
			Spec: cachev1beta1.ValkeyClusterSpec{
				Topology: cachev1beta1.TopologyReplication,
				Profile:  cachev1beta1.ProfileCache,
				Replicas: 3,
				Auth:     &cachev1beta1.AuthSpec{Enabled: true},
			},
		}
		Expect(k8sClient.Create(ctx, vc)).To(Succeed())

		r := &ValkeyClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		key := client.ObjectKeyFromObject(vc)
		DeferCleanup(func() { deleteAndFinalize(r, vc) })

		reconcileToStable(r, key)

		By("attaching the finalizer")
		var got cachev1beta1.ValkeyCluster
		Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
		Expect(controllerutil.ContainsFinalizer(&got, finalizerName)).To(BeTrue())

		By("owning the StatefulSet, Services, ConfigMap, Secret and PDB")
		var sts appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, key, &sts)).To(Succeed())
		Expect(*sts.Spec.Replicas).To(Equal(int32(3)))
		Expect(sts.OwnerReferences).To(HaveLen(1))
		Expect(sts.OwnerReferences[0].Controller).NotTo(BeNil())
		Expect(*sts.OwnerReferences[0].Controller).To(BeTrue())
		Expect(sts.OwnerReferences[0].Name).To(Equal(vc.Name))
		Expect(sts.Spec.Template.Annotations).To(HaveKey(configHashAnnotation))

		for _, name := range []string{"rcache", "rcache-headless"} {
			var svc corev1.Service
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &svc)).
				To(Succeed(), "service %s", name)
		}

		var cm corev1.ConfigMap
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rcache-config", Namespace: ns}, &cm)).To(Succeed())
		Expect(cm.Data).To(HaveKey("valkey.conf"))

		var sec corev1.Secret
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rcache-auth", Namespace: ns}, &sec)).To(Succeed())
		Expect(sec.Data["password"]).NotTo(BeEmpty())

		var pdb policyv1.PodDisruptionBudget
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rcache-pdb", Namespace: ns}, &pdb)).To(Succeed())

		By("recording status (Creating — no kubelet under envtest, so no pod becomes Ready)")
		Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
		Expect(got.Status.Phase).To(Equal(cachev1beta1.PhaseCreating))
		Expect(got.Status.ObservedGeneration).To(Equal(got.Generation))
		Expect(got.Status.InternalEndpoint).To(Equal("rcache.default.svc:6379"))

		By("emitting a kstatus-style Ready condition (False while Creating)")
		ready := meta.FindStatusCondition(got.Status.Conditions, cachev1beta1.ConditionReady)
		Expect(ready).NotTo(BeNil())
		Expect(ready.Status).To(Equal(metav1.ConditionFalse))
		avail := meta.FindStatusCondition(got.Status.Conditions, cachev1beta1.ConditionAvailable)
		Expect(avail).NotTo(BeNil())
		Expect(ready.Status).To(Equal(avail.Status), "Ready must mirror Available")
	})

	It("removes the finalizer on deletion so the resource is collected", func() {
		vc := &cachev1beta1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "del-me", Namespace: ns},
			Spec: cachev1beta1.ValkeyClusterSpec{
				Topology: cachev1beta1.TopologyStandalone,
				Replicas: 1,
			},
		}
		Expect(k8sClient.Create(ctx, vc)).To(Succeed())
		r := &ValkeyClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		key := client.ObjectKeyFromObject(vc)
		reconcileToStable(r, key)

		Expect(k8sClient.Delete(ctx, vc)).To(Succeed())
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		var got cachev1beta1.ValkeyCluster
		err = k8sClient.Get(ctx, key, &got)
		Expect(err).To(HaveOccurred(), fmt.Sprintf("expected NotFound, got %+v", got))
	})

	It("wires TLS through config, the StatefulSet volume/mount and Services", func() {
		const tlsSecret = "tcache-tls-ext"
		sec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: tlsSecret, Namespace: ns},
			Data:       map[string][]byte{"tls.crt": []byte("x"), "tls.key": []byte("y"), "ca.crt": []byte("z")},
		}
		Expect(k8sClient.Create(ctx, sec)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, sec) })

		vc := &cachev1beta1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "tcache", Namespace: ns},
			Spec: cachev1beta1.ValkeyClusterSpec{
				Topology: cachev1beta1.TopologyReplication,
				Replicas: 3,
				Auth:     &cachev1beta1.AuthSpec{Enabled: true},
				TLS:      &cachev1beta1.TLSSpec{Enabled: true, ExistingSecret: tlsSecret},
			},
		}
		Expect(k8sClient.Create(ctx, vc)).To(Succeed())
		r := &ValkeyClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		key := client.ObjectKeyFromObject(vc)
		DeferCleanup(func() { deleteAndFinalize(r, vc) })

		reconcileToStable(r, key)

		By("rendering TLS directives into valkey.conf")
		var cm corev1.ConfigMap
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "tcache-config", Namespace: ns}, &cm)).To(Succeed())
		conf := cm.Data["valkey.conf"]
		for _, want := range []string{
			"port 0",
			fmt.Sprintf("tls-port %d", valkeyTLSPort),
			"tls-cert-file " + tlsMountPath + "/tls.crt",
			"tls-key-file " + tlsMountPath + "/tls.key",
			"tls-ca-cert-file " + tlsMountPath + "/ca.crt",
			"tls-replication yes",
		} {
			Expect(conf).To(ContainSubstring(want), "valkey.conf missing %q", want)
		}

		By("mounting the referenced TLS secret into the valkey container")
		var sts appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, key, &sts)).To(Succeed())

		var tlsVol *corev1.Volume
		for i := range sts.Spec.Template.Spec.Volumes {
			if sts.Spec.Template.Spec.Volumes[i].Name == tlsVolumeName {
				tlsVol = &sts.Spec.Template.Spec.Volumes[i]
			}
		}
		Expect(tlsVol).NotTo(BeNil(), "TLS volume missing from pod spec")
		Expect(tlsVol.Secret).NotTo(BeNil())
		Expect(tlsVol.Secret.SecretName).To(Equal(tlsSecret))

		var valkeyC *corev1.Container
		for i := range sts.Spec.Template.Spec.Containers {
			if sts.Spec.Template.Spec.Containers[i].Name == "valkey" {
				valkeyC = &sts.Spec.Template.Spec.Containers[i]
			}
		}
		Expect(valkeyC).NotTo(BeNil())
		mounted := false
		for _, m := range valkeyC.VolumeMounts {
			if m.Name == tlsVolumeName && m.MountPath == tlsMountPath {
				mounted = true
			}
		}
		Expect(mounted).To(BeTrue(), "valkey container should mount the TLS secret at %s", tlsMountPath)

		By("exposing the TLS port on client and headless Services")
		for _, name := range []string{"tcache", "tcache-headless"} {
			var svc corev1.Service
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &svc)).To(Succeed())
			Expect(svc.Spec.Ports[0].Port).To(Equal(valkeyTLSPort), "service %s should expose the TLS port", name)
		}
	})
})

var _ = Describe("Finalizer lifecycle (envtest)", func() {
	const ns = "default"

	It("adds the cleanup finalizer on create and removes it on delete", func() {
		vc := &cachev1beta1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "fin-1", Namespace: ns},
			Spec: cachev1beta1.ValkeyClusterSpec{
				Topology: cachev1beta1.TopologyReplication,
				Replicas: 3,
			},
		}
		Expect(k8sClient.Create(ctx, vc)).To(Succeed())
		key := client.ObjectKeyFromObject(vc)
		r := &ValkeyClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}

		By("adding the finalizer on the first reconcile")
		reconcileToStable(r, key)
		var got cachev1beta1.ValkeyCluster
		Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
		Expect(controllerutil.ContainsFinalizer(&got, finalizerName)).To(BeTrue(),
			"reconcile must add the cleanup finalizer")

		By("keeping the object alive after Delete until cleanup runs")
		Expect(k8sClient.Delete(ctx, &got)).To(Succeed())
		// Still present (finalizer blocks GC), now with a deletionTimestamp.
		Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
		Expect(got.DeletionTimestamp).NotTo(BeNil())

		By("removing the finalizer on the deletion reconcile so the API server GCs it")
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(ctx, key, &cachev1beta1.ValkeyCluster{}))
		}).Should(BeTrue(), "ValkeyCluster should be gone once the finalizer is removed")
	})
})

// Chaos C-7 (racing reconciles / optimistic concurrency): rapid concurrent
// spec updates racing the operator's status writes must not be lost, and the
// status must converge to the final generation. The operator patches status
// via a MergeFrom (status subresource), so a concurrent spec churn can never be
// reverted by a reconcile — and external writers retry on conflict. This guards
// the "no lost update / no stuck status" contract under update storms.
var _ = Describe("ValkeyCluster racing reconciles (chaos C-7)", func() {
	const ns = "default"

	It("loses no concurrent spec update and converges the status", func() {
		vc := &cachev1beta1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "race", Namespace: ns},
			Spec: cachev1beta1.ValkeyClusterSpec{
				Topology: cachev1beta1.TopologyStandalone,
				Profile:  cachev1beta1.ProfileCache,
				Replicas: 1,
			},
		}
		Expect(k8sClient.Create(ctx, vc)).To(Succeed())
		r := &ValkeyClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		key := client.ObjectKeyFromObject(vc)
		DeferCleanup(func() { deleteAndFinalize(r, vc) })
		reconcileToStable(r, key)

		const writers = 8
		// A generous flat backoff: status patches bump the shared resourceVersion,
		// so under an 8-way write storm + concurrent reconciles the conflict rate
		// is high. A persistent client retries until it lands — exhausting a tiny
		// retry budget would be a test artifact, not a lost update. The contract
		// under test is that every write that the client keeps retrying eventually
		// commits (no permanent lost update) and the status converges.
		backoff := wait.Backoff{Steps: 200, Duration: 5 * time.Millisecond, Factor: 1.0}
		// Each writer adds its own distinct config key, retrying on the
		// optimistic-concurrency conflicts from the other writers AND the
		// operator's concurrent status patches.
		writeKey := func(n int) error {
			return retry.RetryOnConflict(backoff, func() error {
				cur := &cachev1beta1.ValkeyCluster{}
				if err := k8sClient.Get(ctx, key, cur); err != nil {
					return err
				}
				if cur.Spec.Config == nil {
					cur.Spec.Config = map[string]string{}
				}
				cur.Spec.Config[fmt.Sprintf("io-threads-%d", n)] = "1"
				return k8sClient.Update(ctx, cur)
			})
		}

		var wg sync.WaitGroup
		errs := make([]error, writers)
		for i := range writers {
			wg.Go(func() { errs[i] = writeKey(i) })
		}
		// Reconciles racing the writers: the operator keeps patching status while
		// spec churns underneath it.
		var rg sync.WaitGroup
		rg.Go(func() {
			for range 40 {
				_, _ = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
			}
		})
		wg.Wait()
		rg.Wait()

		reconcileToStable(r, key)

		// Every writer committed (optimistic concurrency retried to success — no
		// livelock, no permanent conflict).
		for i := range writers {
			Expect(errs[i]).NotTo(HaveOccurred(), "writer %d never committed under contention", i)
		}

		final := &cachev1beta1.ValkeyCluster{}
		Expect(k8sClient.Get(ctx, key, final)).To(Succeed())
		// No lost update: every concurrent writer's key survived the storm — the
		// operator's status patches never reverted a committed spec change.
		for i := range writers {
			Expect(final.Spec.Config).To(HaveKey(fmt.Sprintf("io-threads-%d", i)),
				"a concurrent spec update was lost during racing reconciles")
		}
		// Status converged to the final spec generation — nothing stuck.
		Expect(final.Status.ObservedGeneration).To(Equal(final.Generation))
	})
})
