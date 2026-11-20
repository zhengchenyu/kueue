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

package preemption

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"

	config "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/features"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
)

// "b" borrows 4 with v, which can shrink to 3. "a" owns 2 and needs 3, so it
// borrows 1 while preempting the victim, which has a lower priority.
func TestPartialPreemptionWhileBorrowing(t *testing.T) {
	clusterQueue := func(name string) *kueue.ClusterQueue {
		return utiltestingapi.MakeClusterQueue(name).Cohort("all").
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "2").Obj()).
			Preemption(kueue.ClusterQueuePreemption{
				ReclaimWithinCohort: kueue.PreemptionPolicyAny,
				WithinClusterQueue:  kueue.PreemptionPolicyLowerPriority,
				BorrowWithinCohort:  &kueue.BorrowWithinCohort{Policy: kueue.BorrowWithinCohortPolicyLowerPriority},
			}).
			Obj()
	}
	cases := map[string]struct {
		fairSharing *config.FairSharing
	}{
		"classical": {},
		"fair sharing": {
			fairSharing: &config.FairSharing{
				PreemptionStrategies: []config.PreemptionStrategy{config.LessThanOrEqualToFinalShare},
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.PartialPreemption, true)
			fixture := newPlanFixture(t, tc.fairSharing,
				[]*kueue.ClusterQueue{clusterQueue("a"), clusterQueue("b"), clusterQueue("c")},
				[]*kueue.Workload{partialPreemptionVictimInCQ("b", "v", 0, 6, 3)},
				utiltestingapi.MakeWorkload("in", "").Priority(10).Request(corev1.ResourceCPU, "3").Obj())

			strategies := fixture.preemptor.getPreemptionStrategyIterator(fixture.ctx, fixture.pCtx)
			got := fixture.targets(t, strategies)

			want := []planTarget{shrink("v", 3)}
			if diff := cmp.Diff(want, got, planTargetCmpOpts); diff != "" {
				t.Errorf("Unexpected targets (-want,+got):\n%s", diff)
			}
		})
	}
}
