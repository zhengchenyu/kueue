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

package preemptioncommon

import (
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

// TestCandidatesOrderingPendingReclaimTarget checks the HasPendingReclaimTarget ordering
// criterion: a workload with a pending reclaim target sorts before one without.
func TestCandidatesOrderingPendingReclaimTarget(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	// partialWl builds a partial-preemption workload admitted at count=5.
	// reclaimTarget < 0 means no ReclaimTargetCount is set.
	partialWl := func(name string, prio int32, reserved time.Time, reclaimTarget int32) *workload.Info {
		psa := utiltesting.MakePodSetAssignment("executor").
			Assignment(corev1.ResourceCPU, "default", "5000m").
			Count(5).
			Obj()
		if reclaimTarget >= 0 {
			psa.ReclaimTargetCount = ptr.To(reclaimTarget)
		}
		wl := utiltesting.MakeWorkload(name, "ns").
			UID(types.UID(name)).
			Annotation(constants.PartialPreemptionAnnotation, "true").
			Priority(prio).
			PodSets(*utiltesting.MakePodSet("executor", 5).
				Request(corev1.ResourceCPU, "1").
				SetMinimumCount(1).
				Obj()).
			ReserveQuotaAt(
				utiltesting.MakeAdmission("cq").
					PodSets(psa).
					Obj(),
				reserved,
			).
			Obj()
		return workload.NewInfo(logr.Discard(), wl)
	}

	log := logr.Discard()
	const preemptorCQ = kueue.ClusterQueueReference("preemptor-cq")

	t.Run("gate on: pending reclaim target sorts first over recency", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.PartialPreemption, true)
		a := partialWl("a", 0, now.Add(time.Minute), -1) // no reclaim target, admitted more recently
		b := partialWl("b", 0, now, 3)                   // count=5 > reclaimTarget=3 → HasPendingReclaimTarget=true
		if got := policy.CandidatesOrdering(log, false, a, b, preemptorCQ, now); got <= 0 {
			t.Errorf("policy.CandidatesOrdering(noTarget, pendingTarget) = %d, want > 0", got)
		}
		if got := policy.CandidatesOrdering(log, false, b, a, preemptorCQ, now); got >= 0 {
			t.Errorf("policy.CandidatesOrdering(pendingTarget, noTarget) = %d, want < 0", got)
		}
	})

	t.Run("gate on: satisfied reclaim target does not sort first", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.PartialPreemption, true)
		a := partialWl("a", 0, now.Add(time.Minute), 5) // count=5 == reclaimTarget=5 → HasPendingReclaimTarget=false
		b := partialWl("b", 0, now, -1)
		// both HasPendingReclaimTarget=false, recency decides: a is newer → a sorts first
		if got := policy.CandidatesOrdering(log, false, a, b, preemptorCQ, now); got >= 0 {
			t.Errorf("policy.CandidatesOrdering(recentSatisfied, olderNoTarget) = %d, want < 0 (recency decides)", got)
		}
	})

	t.Run("gate on: recency decides when both have pending targets", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.PartialPreemption, true)
		a := partialWl("a", 0, now, 3)
		b := partialWl("b", 0, now.Add(time.Minute), 3)
		// both HasPendingReclaimTarget=true, recency decides: b is newer → b sorts first
		if got := policy.CandidatesOrdering(log, false, a, b, preemptorCQ, now); got <= 0 {
			t.Errorf("policy.CandidatesOrdering(older, newer) = %d, want > 0", got)
		}
		if got := policy.CandidatesOrdering(log, false, b, a, preemptorCQ, now); got >= 0 {
			t.Errorf("policy.CandidatesOrdering(newer, older) = %d, want < 0", got)
		}
	})

	t.Run("gate off: HasPendingReclaimTarget always false, recency decides", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.PartialPreemption, false)
		a := partialWl("a", 0, now, 3)                   // gate off → HasPendingReclaimTarget=false
		b := partialWl("b", 0, now.Add(time.Minute), -1) // newer
		// recency decides: b is newer → b sorts first → a > b → got > 0
		if got := policy.CandidatesOrdering(log, false, a, b, preemptorCQ, now); got <= 0 {
			t.Errorf("policy.CandidatesOrdering(gateOff) = %d, want > 0 (recency decides)", got)
		}
	})
}
