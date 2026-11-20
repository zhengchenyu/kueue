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
	"context"
	"fmt"
	"slices"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/metrics"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	"sigs.k8s.io/kueue/pkg/workload"
	workloadevict "sigs.k8s.io/kueue/pkg/workload/evict"
	"sigs.k8s.io/kueue/pkg/workload/patching"
)

// The rounds of iterateWithPartialPreemption.
const (
	pendingRound = iota
	shrinkRound
	wholeRound
)

// iterateWithPartialPreemption offers the candidates in three rounds, each one a
// separate pass of the iterator over the snapshot, from the least to the most
// disruptive:
//
//   - The first round whole-evicts the already-evicted prefix (the candidates the
//     iterator emits first). From the first non-evicted candidate on, it only
//     shrinks the victims with an in-flight partial-preemption request.
//   - The second round shrinks every other shrinkable victim to its floor. A victim whose
//     live used count is already at the floor is skipped.
//   - The third round whole-evicts the candidates still present.
//
// Every round reads the victim's live instance from the snapshot, so a victim
// shrunk in an earlier round is planned against its reduced instance. A
// shrinkable victim may therefore be yielded twice, as a shrink and later as a
// whole eviction, and the fill-back merges the two stages.
//
// yield already removes each target from the snapshot. It returns false if yield
// stopped the iteration.
func iterateWithPartialPreemption(
	preemptionCtx *preemptionCtx,
	iterator candidateIterator,
	allowBorrowing bool,
	yield func(*Target) bool,
) (cont bool) {
	for round := range wholeRound + 1 {
		inEvictedPrefix := round == pendingRound
		iterator.Reset()
		for candidateWl, reason := iterator.Next(allowBorrowing); candidateWl != nil; candidateWl, reason = iterator.Next(allowBorrowing) {
			cq := preemptionCtx.snapshot.ClusterQueue(candidateWl.ClusterQueue)
			if cq == nil {
				continue
			}
			current := cq.Workloads[workload.Key(candidateWl.Obj)]
			if current == nil {
				continue
			}
			var partial *policy.PartialPreemptionTarget
			switch {
			case inEvictedPrefix && workloadevict.IsEvicted(candidateWl.Obj), round == wholeRound:
				// A whole eviction.
			default:
				inEvictedPrefix = false
				if round == pendingRound && !workload.HasPendingPreemptionRetainedCount(candidateWl.Obj) {
					continue
				}
				if partial = planPreemption(candidateWl, current); partial == nil {
					continue
				}
			}
			if !yield(&Target{WorkloadInfo: candidateWl, Reason: reason, WorkloadCq: cq, PartialPreemption: partial}) {
				return false
			}
		}
	}
	return true
}

// fillBackPartialWorkloads merges the staged targets, then restores the least appealing
// victims first, dropping each one the preemptor can fit without. Shrinkable
// victims are regrown via fillbackPartialPreemption.
func fillBackPartialWorkloads(
	ctx context.Context,
	pCtx *preemptionCtx,
	targets []*Target,
	allowBorrowing bool,
	ordering func(a, b *workload.Info) int,
) []*Target {
	merged := mergeStagedTargets(targets, ordering)
	for i := len(merged) - 1; i >= 0; i-- {
		t := merged[i]
		// Shrinkable victims get how many Pods they keep searched instead of a plain
		// restore-or-keep. A victim selected by a PreemptionConfig rule is never shrunk.
		if podSet, ok := workload.PartialPreemptionPodSet(t.WorkloadInfo.Obj, t.WorkloadInfo.TotalRequests); ok && t.Reason != kueue.ConfigurablePreemptionReason {
			if fillbackPartialPreemption(ctx, pCtx, t, podSet, allowBorrowing) {
				// O(1) deletion: copy the last element into index i and reduce size.
				merged[i] = merged[len(merged)-1]
				merged = merged[:len(merged)-1]
			}
			continue
		}
		// Whole eviction: restore it and drop it if the preemptor still fits.
		pCtx.snapshot.AddWorkload(t.WorkloadInfo)
		if workloadFits(ctx, pCtx, allowBorrowing) {
			merged[i] = merged[len(merged)-1]
			merged = merged[:len(merged)-1]
			continue
		}
		pCtx.snapshot.RemoveWorkload(t.WorkloadInfo)
	}
	return merged
}

// mergeStagedTargets collapses the staged targets to one per victim, keeping the first
// stage seen, and sorts the result by ordering (decreasing preemption appeal) so
// the fill-back can restore the least appealing victims first.
func mergeStagedTargets(targets []*Target, ordering func(a, b *workload.Info) int) []*Target {
	byKey := make(map[workload.Reference]*Target, len(targets))
	for _, t := range targets {
		key := workload.Key(t.WorkloadInfo.Obj)
		if _, seen := byKey[key]; !seen {
			byKey[key] = t
		}
	}
	merged := make([]*Target, 0, len(byKey))
	for _, t := range byKey {
		merged = append(merged, t)
	}
	slices.SortStableFunc(merged, func(a, b *Target) int {
		return ordering(a.WorkloadInfo, b.WorkloadInfo)
	})
	return merged
}

// fillbackPartialPreemption raises how many Pods of its shrinkable PodSet the victim t keeps, from
// what the search left it, as far as the preemptor still fits. It tries, in order:
//
//	(a) keeping all: drop the target entirely if the preemptor still fits with the
//	    victim completely back, unless an in-flight shrink caps the keep;
//	(b) keeping the floor (minRetainedCount). If even that doesn't fit, the outcome
//	    depends on how the search left the victim, captured as wasWholeEvicted before
//	    the snapshot is mutated: a victim already whole-evicted settles at 0, which the
//	    search already proved fits; a victim that was only shrunk to the floor (never
//	    escalated) keeps that shrink, which t.PartialPreemption already reflects;
//	(c) the largest keep between the floor and min(full-1, maxKeep) that still fits,
//	    where maxKeep is the retained count of an in-flight shrink, if one is pending.
//
// All arithmetic uses the original full WorkloadInfo, so the victim's whole quota is
// what gets restored. It reports whether the target was dropped.
func fillbackPartialPreemption(ctx context.Context, pCtx *preemptionCtx, t *Target, podSet kueue.PodSetReference, allowBorrowing bool) (dropped bool) {
	floor, _ := workload.PreemptionMinRetainedCount(t.WorkloadInfo.Obj, podSet)
	full := podSetRequestCount(t.WorkloadInfo, podSet)
	maxKeep := full
	if pending, has := workload.PreemptionRetainedCount(t.WorkloadInfo.Obj, podSet); has && pending < maxKeep {
		maxKeep = pending
	}

	snapshot := pCtx.snapshot
	cq := snapshot.ClusterQueue(t.WorkloadInfo.ClusterQueue)
	key := workload.Key(t.WorkloadInfo.Obj)
	wasWholeEvicted := cq.Workloads[key] == nil

	// setKeep rewrites the victim's snapshot instance so that its shrinkable PodSet
	// keeps the given number of Pods, preserving the other PodSets. It returns the
	// reduced instance it installed, or nil if the victim is kept completely.
	setKeep := func(keep int32) *workload.Info {
		if current := cq.Workloads[key]; current != nil {
			snapshot.RemoveWorkload(current)
		}
		if keep >= full {
			snapshot.AddWorkload(t.WorkloadInfo)
			return nil
		}
		reduced := reducedInfoForPartialPreemption(t.WorkloadInfo, podSet, keep)
		snapshot.AddWorkload(reduced)
		return reduced
	}

	// (a) Keep all.
	if maxKeep >= full {
		setKeep(full)
		if workloadFits(ctx, pCtx, allowBorrowing) {
			return true
		}
	}

	// (b) Keep the floor.
	setKeep(floor)
	if !workloadFits(ctx, pCtx, allowBorrowing) {
		if wasWholeEvicted {
			// The search already proved the preemptor fits with the victim fully gone.
			if current := cq.Workloads[key]; current != nil {
				snapshot.RemoveWorkload(current)
			}
			t.PartialPreemption = nil
		}
		return false
	}

	// (c) The floor fits: search above it for the largest keep that still fits. A smaller
	// keep frees more room, so sort.Search finds the largest fitting one.
	keep := floor
	if band := min(maxKeep, full-1); band > floor {
		n := int(band - floor)
		k := sort.Search(n, func(k int) bool {
			setKeep(band - int32(k))
			return workloadFits(ctx, pCtx, allowBorrowing)
		})
		if k < n {
			keep = band - int32(k)
		}
	}
	t.PartialPreemption = &policy.PartialPreemptionTarget{
		PodSet:        podSet,
		RetainedCount: keep,
		ReducedInfo:   setKeep(keep),
	}
	return false
}

func podSetRequestCount(info *workload.Info, podSet kueue.PodSetReference) int32 {
	for i := range info.TotalRequests {
		if info.TotalRequests[i].Name == podSet {
			return info.TotalRequests[i].Count
		}
	}
	return 0
}

// planPreemption plans a shrink of the candidate's shrinkable PodSet to its floor, or
// returns nil when no shrink applies: the candidate is not partial-eligible, or the used
// count of its live instance current is already at or below the floor. It leaves the
// snapshot unchanged.
func planPreemption(candidate, current *workload.Info) *policy.PartialPreemptionTarget {
	podSet, ok := workload.PartialPreemptionPodSet(candidate.Obj, candidate.TotalRequests)
	if !ok {
		return nil
	}
	floor, _ := workload.PreemptionMinRetainedCount(candidate.Obj, podSet)
	if podSetRequestCount(current, podSet) <= floor {
		return nil
	}
	return &policy.PartialPreemptionTarget{
		PodSet:        podSet,
		RetainedCount: floor,
		ReducedInfo:   reducedInfoForPartialPreemption(candidate, podSet, floor),
	}
}

// partiallyPreempt requests a scale-down; quota remains charged until the excess Pods terminate.
func (p *Preemptor) partiallyPreempt(ctx context.Context, preemptor *workload.Info, target *Target) error {
	pp := target.PartialPreemption
	wlCopy := target.WorkloadInfo.Obj.DeepCopy()
	message := fmt.Sprintf("Requested scale-down of PodSet %q to %d to accommodate workload %s",
		pp.PodSet, pp.RetainedCount, klog.KObj(preemptor.Obj))
	if err := patching.PatchAdmissionStatus(ctx, p.client, wlCopy, p.clock, func(wl *kueue.Workload) (bool, error) {
		setRetainedCount(wl, pp.PodSet, pp.RetainedCount)
		workload.SetPartiallyPreemptedCondition(wl, p.clock.Now(), true, target.Reason, message)
		return true, nil
	}, patching.WithLooseOnApply()); err != nil {
		return err
	}
	metrics.ReportPartialPreemption(preemptor.ClusterQueue, target.Reason, p.customLabels.CQGet(preemptor.ClusterQueue), p.roleTracker)
	ctrl.LoggerFrom(ctx).V(3).Info("Partial preemption: requested scale-down",
		"targetWorkload", klog.KObj(target.WorkloadInfo.Obj), "podSet", pp.PodSet,
		"RetainedCount", pp.RetainedCount, "preemptingWorkload", klog.KObj(preemptor.Obj))
	p.recorder.Eventf(target.WorkloadInfo.Obj, nil, corev1.EventTypeNormal, "PartiallyPreempted", "PartiallyPreempted", "%s", message)
	return nil
}

// reducedInfoForPartialPreemption returns a shallow copy of info with the named PodSet's usage scaled
// down to retainedCount. The original is left untouched so the snapshot can restore it unchanged.
func reducedInfoForPartialPreemption(info *workload.Info, podSetName kueue.PodSetReference, retainedCount int32) *workload.Info {
	reduced := *info
	reduced.TotalRequests = make([]workload.PodSetResources, len(info.TotalRequests))
	for idx := range info.TotalRequests {
		psr := info.TotalRequests[idx]
		if psr.Name == podSetName {
			if retainedCount >= 0 && retainedCount < psr.Count {
				psr = *psr.ScaledTo(retainedCount)
			}
		}
		reduced.TotalRequests[idx] = psr
	}
	return &reduced
}

// setRetainedCount records count as the named PodSet's requested partial-preemption target,
// reporting whether the value changed.
func setRetainedCount(wl *kueue.Workload, podSetName kueue.PodSetReference, count int32) bool {
	if wl.Status.Admission == nil {
		return false
	}
	for i := range wl.Status.Admission.PodSetAssignments {
		psa := &wl.Status.Admission.PodSetAssignments[i]
		if psa.Name == podSetName {
			if psa.PartialPreemption != nil && psa.PartialPreemption.RetainedCount != nil && *psa.PartialPreemption.RetainedCount <= count {
				return false
			}
			psa.PartialPreemption = &kueue.PodSetPartialPreemptionStatus{RetainedCount: new(count)}
			return true
		}
	}
	return false
}
