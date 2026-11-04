/*
Copyright The Kubernetes Authors.

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

package job

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	pkgconstants "sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/features"
	utilpod "sigs.k8s.io/kueue/pkg/util/pod"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingjob "sigs.k8s.io/kueue/pkg/util/testingjobs/job"
	testingpod "sigs.k8s.io/kueue/pkg/util/testingjobs/pod"
	"sigs.k8s.io/kueue/pkg/workload"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
	"sigs.k8s.io/kueue/test/integration/framework"
	"sigs.k8s.io/kueue/test/util/behavioral"
)

// This exercises the full reactive partial-preemption loop for batch/Job:
// request (scheduler sets PreemptionTargetCount) -> drain (ungater evicts the
// excess Pods) -> release (PartiallyPreempted flips to QuotaReleased) ->
// reclaim (the Job reconciler raises reclaimablePods) -> the preemptor is admitted.
var _ = ginkgo.Describe("Elastic Job partial preemption", ginkgo.Ordered, ginkgo.ContinueOnFailure, func() {
	var (
		ns *corev1.Namespace
		rf *kueue.ResourceFlavor
		cq *kueue.ClusterQueue
		lq *kueue.LocalQueue
	)

	ginkgo.BeforeAll(func() {
		features.SetFeatureGateDuringTest(ginkgo.GinkgoTB(), features.ElasticJobsViaWorkloadSlices, true)
		features.SetFeatureGateDuringTest(ginkgo.GinkgoTB(), features.ReclaimablePods, true)
		features.SetFeatureGateDuringTest(ginkgo.GinkgoTB(), features.PartialPreemption, true)
		fwk.StartManager(ctx, cfg, managerAndControllersSetup(false, true, nil))
	})

	ginkgo.AfterAll(func() {
		fwk.StopManager(ctx)
	})

	ginkgo.BeforeEach(func() {
		ns = behavioral.CreateNamespaceFromPrefixWithLog(ctx, k8sClient, "elastic-partial-preemption-")

		rf = utiltestingapi.MakeResourceFlavor("default").Obj()
		behavioral.MustCreate(ctx, k8sClient, rf)

		cq = utiltestingapi.MakeClusterQueue("elastic-partial-preemption-cq").
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas(rf.Name).Resource(corev1.ResourceCPU, "4").Obj()).
			Preemption(kueue.ClusterQueuePreemption{
				WithinClusterQueue: kueue.PreemptionPolicyLowerPriority,
			}).
			Obj()
		behavioral.MustCreate(ctx, k8sClient, cq)

		lq = utiltestingapi.MakeLocalQueue("elastic-partial-preemption-lq", ns.Name).ClusterQueue(cq.Name).Obj()
		behavioral.MustCreate(ctx, k8sClient, lq)
	})

	ginkgo.AfterEach(func() {
		gomega.Expect(behavioral.DeleteNamespace(ctx, k8sClient, ns)).To(gomega.Succeed())
		behavioral.ExpectObjectToBeDeleted(ctx, k8sClient, cq, true)
		behavioral.ExpectObjectToBeDeleted(ctx, k8sClient, lq, true)
		behavioral.ExpectObjectToBeDeleted(ctx, k8sClient, rf, true)
	})

	ginkgo.It("shrinks the victim to its floor, releases the quota and admits the preemptor", framework.SlowSpec, func() {
		const parallelism int32 = 4

		victim := testingjob.MakeJob("victim", ns.Name).
			Queue(kueue.LocalQueueName(lq.Name)).
			SetAnnotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue).
			SetAnnotation(pkgconstants.PartialPreemptionAnnotation, "true").
			SetAnnotation(pkgconstants.PartialPreemptionMinCountAnnotation, "1").
			Indexed(true).
			Request(corev1.ResourceCPU, "1").
			Parallelism(parallelism).
			Completions(parallelism).
			Obj()
		victim.Spec.PodFailurePolicy = &batchv1.PodFailurePolicy{
			Rules: []batchv1.PodFailurePolicyRule{{
				Action: batchv1.PodFailurePolicyActionIgnore,
				OnPodConditions: []batchv1.PodFailurePolicyOnPodConditionsPattern{{
					Type:   corev1.DisruptionTarget,
					Status: corev1.ConditionTrue,
				}},
			}},
		}

		var (
			victimWlKey client.ObjectKey
			podSet      kueue.PodSetReference
		)
		ginkgo.By("admitting the victim that occupies the whole ClusterQueue", func() {
			behavioral.MustCreate(ctx, k8sClient, victim)
			gomega.Eventually(func(g gomega.Gomega) {
				workloads := &kueue.WorkloadList{}
				g.Expect(k8sClient.List(ctx, workloads, client.InNamespace(ns.Name))).Should(gomega.Succeed())
				g.Expect(workloads.Items).Should(gomega.HaveLen(1))
				wl := &workloads.Items[0]
				g.Expect(workload.IsAdmitted(wl)).Should(gomega.BeTrue())
				g.Expect(wl.Status.Admission.PodSetAssignments).Should(gomega.HaveLen(1))
				g.Expect(wl.Status.Admission.PodSetAssignments[0].Count).Should(gomega.HaveValue(gomega.Equal(parallelism)))
				victimWlKey = client.ObjectKeyFromObject(wl)
				podSet = wl.Spec.PodSets[0].Name
			}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
		})

		ginkgo.By("simulating the Job's running Pods and letting the ungater ungate them", func() {
			for i := int32(0); i < parallelism; i++ {
				pod := testingpod.MakePod(fmt.Sprintf("victim-%d", i), ns.Name).
					Annotation(kueue.WorkloadAnnotation, victimWlKey.Name).
					Annotation(kueue.WorkloadSliceNameAnnotation, victimWlKey.Name).
					Label(pkgconstants.PodSetLabel, string(podSet)).
					Gate(kueue.ElasticJobSchedulingGate).
					Obj()
				behavioral.MustCreate(ctx, k8sClient, pod)
			}
			gomega.Eventually(func(g gomega.Gomega) {
				pods := &corev1.PodList{}
				g.Expect(k8sClient.List(ctx, pods, client.InNamespace(ns.Name),
					client.MatchingLabels{pkgconstants.PodSetLabel: string(podSet)})).Should(gomega.Succeed())
				g.Expect(pods.Items).Should(gomega.HaveLen(int(parallelism)))
				for i := range pods.Items {
					g.Expect(utilpod.HasGate(&pods.Items[i], kueue.ElasticJobSchedulingGate)).Should(gomega.BeFalse())
				}
			}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
		})

		preemptor := utiltestingapi.MakeWorkload("preemptor", ns.Name).
			Priority(10).
			Queue(kueue.LocalQueueName(lq.Name)).
			PodSets(*utiltestingapi.MakePodSet("main", 2).Request(corev1.ResourceCPU, "1").Obj()).
			Obj()

		var target int32
		ginkgo.By("requesting a partial preemption when the preemptor arrives", func() {
			behavioral.MustCreate(ctx, k8sClient, preemptor)
			gomega.Eventually(func(g gomega.Gomega) {
				wl := &kueue.Workload{}
				g.Expect(k8sClient.Get(ctx, victimWlKey, wl)).Should(gomega.Succeed())
				got, ok := workload.PreemptionTargetCount(wl, podSet)
				g.Expect(ok).Should(gomega.BeTrue())
				g.Expect(got).Should(gomega.BeNumerically("<", parallelism))
				g.Expect(got).Should(gomega.BeNumerically(">=", int32(1)))
				target = got
				cond := meta.FindStatusCondition(wl.Status.Conditions, kueue.WorkloadPartiallyPreempted)
				g.Expect(cond).ShouldNot(gomega.BeNil())
				g.Expect(cond.Status).Should(gomega.Equal(metav1.ConditionTrue))
			}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
		})

		ginkgo.By("draining the victim's excess Pods down to the target", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				pods := &corev1.PodList{}
				g.Expect(k8sClient.List(ctx, pods, client.InNamespace(ns.Name),
					client.MatchingLabels{pkgconstants.PodSetLabel: string(podSet)})).Should(gomega.Succeed())
				g.Expect(pods.Items).Should(gomega.HaveLen(int(target)))
			}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
		})

		ginkgo.By("releasing the quota and raising reclaimablePods once drained", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				wl := &kueue.Workload{}
				g.Expect(k8sClient.Get(ctx, victimWlKey, wl)).Should(gomega.Succeed())
				cond := meta.FindStatusCondition(wl.Status.Conditions, kueue.WorkloadPartiallyPreempted)
				g.Expect(cond).ShouldNot(gomega.BeNil())
				g.Expect(cond.Status).Should(gomega.Equal(metav1.ConditionFalse))
				g.Expect(cond.Reason).Should(gomega.Equal(kueue.WorkloadPartiallyPreemptedReasonQuotaReleased))
				var reclaimed int32
				for _, rp := range wl.Status.ReclaimablePods {
					if rp.Name == podSet {
						reclaimed = rp.Count
					}
				}
				g.Expect(reclaimed).Should(gomega.Equal(parallelism - target))
			}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
		})

		ginkgo.By("admitting the preemptor with the released quota", func() {
			behavioral.ExpectWorkloadsToBeAdmitted(ctx, k8sClient, preemptor)
		})
	})
})
