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
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// Op1: prove HA handover between leader-elected operator replicas. Two managers
// contend for the same lease against the envtest API server; the standby must
// not steal an actively-renewed lease, and must take over promptly once the
// active leader steps down. This exercises the real coordination.k8s.io Lease
// path the deployed operator uses (`--leader-elect`), not a mock.
var _ = Describe("Leader election handover (Op1)", func() {
	It("promotes a standby manager when the active leader steps down", func() {
		const leaseID = "op1-handover.wellcake.io"
		const leaseNS = "default"

		newMgr := func() ctrl.Manager {
			m, err := ctrl.NewManager(cfg, ctrl.Options{
				Scheme:                        scheme.Scheme,
				LeaderElection:                true,
				LeaderElectionID:              leaseID,
				LeaderElectionNamespace:       leaseNS,
				LeaderElectionReleaseOnCancel: true,
				// Short timings so the test converges in seconds; the invariant
				// (single holder, clean handover) is independent of the values.
				LeaseDuration:          ptr.To(6 * time.Second),
				RenewDeadline:          ptr.To(4 * time.Second),
				RetryPeriod:            ptr.To(1 * time.Second),
				Metrics:                metricsserver.Options{BindAddress: "0"},
				HealthProbeBindAddress: "0",
			})
			Expect(err).NotTo(HaveOccurred())
			return m
		}

		// holder returns the current lease holder identity ("" if none).
		holder := func() string {
			var lease coordinationv1.Lease
			if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: leaseNS, Name: leaseID}, &lease); err != nil {
				return ""
			}
			if lease.Spec.HolderIdentity == nil {
				return ""
			}
			return *lease.Spec.HolderIdentity
		}

		By("starting leader A")
		mgrA := newMgr()
		ctxA, cancelA := context.WithCancel(ctx)
		go func() {
			defer GinkgoRecover()
			_ = mgrA.Start(ctxA)
		}()

		Eventually(holder, 30*time.Second, 250*time.Millisecond).ShouldNot(BeEmpty())
		leaderA := holder()
		By("leader A holds the lease: " + leaderA)

		By("starting standby B — it must not steal an actively-renewed lease")
		mgrB := newMgr()
		ctxB, cancelB := context.WithCancel(ctx)
		defer cancelB()
		go func() {
			defer GinkgoRecover()
			_ = mgrB.Start(ctxB)
		}()
		Consistently(holder, 5*time.Second, 1*time.Second).Should(Equal(leaderA))

		By("leader A steps down (graceful cancel releases the lease)")
		cancelA()

		By("standby B takes over with a different, non-empty identity")
		Eventually(holder, 30*time.Second, 250*time.Millisecond).
			ShouldNot(Or(BeEmpty(), Equal(leaderA)))

		cancelB()
	})
})
