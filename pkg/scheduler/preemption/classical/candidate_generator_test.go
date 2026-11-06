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

package classical

import (
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/cache/hierarchy"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/resources"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	"sigs.k8s.io/kueue/pkg/util/priority"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

func evictedInfo(t *testing.T, name string, evicted bool) *workload.Info {
	t.Helper()
	_, log := utiltesting.ContextWithLog(t)
	wl := utiltestingapi.MakeWorkload(name, "ns")
	if evicted {
		wl = wl.Condition(metav1.Condition{
			Type:               kueue.WorkloadEvicted,
			Status:             metav1.ConditionTrue,
			Reason:             "test",
			LastTransitionTime: metav1.NewTime(time.Now()),
		})
	}
	return workload.NewInfo(log, wl.Obj())
}

// candidate builds a flavor-stamped candidate at the given priority, optionally
// flagged as a partial-preemption job, as carrying a pending preemption target,
// and/or as evicted.
func candidate(t *testing.T, name string, prio int32, partial, pending, evicted bool) *workload.Info {
	t.Helper()
	info := candidateInfo(t, name, preemptorCQName, prio)
	info.IsPartialPreemptionJob = partial
	info.HasPendingPreemptionTarget = pending
	if evicted {
		apimeta.SetStatusCondition(&info.Obj.Status.Conditions, metav1.Condition{
			Type:   kueue.WorkloadEvicted,
			Status: metav1.ConditionTrue,
			Reason: "test",
		})
	}
	return info
}

func byName(a, b *workload.Info) int {
	return strings.Compare(a.Obj.Name, b.Obj.Name)
}

func makeWorkloadCQ(infos []*workload.Info) *schdcache.ClusterQueueSnapshot {
	cq := &schdcache.ClusterQueueSnapshot{
		Workloads: make(map[workload.Reference]*workload.Info, len(infos)),
	}
	for _, wl := range infos {
		cq.Workloads[workload.Key(wl.Obj)] = wl
	}
	return cq
}

func segmentBucket(sortedWorkloads []*workload.Info, advantage bool) *bucket {
	b := &bucket{
		cq:                       makeWorkloadCQ(sortedWorkloads),
		isSameQueue:              true,
		hasHierarchicalAdvantage: advantage,
		sortedWorkloads:          sortedWorkloads,
	}
	return b
}

func drainIter(it *cacheCandidateIterator, borrow bool) []string {
	var got []string
	for {
		wl, _ := it.Next(borrow)
		if wl == nil {
			break
		}
		got = append(got, wl.Obj.Name)
	}
	return got
}

func TestCacheIterator(t *testing.T) {
	// shared workloads
	a := candidateInfo(t, "a", preemptorCQName, 1)
	b := candidateInfo(t, "b", preemptorCQName, 1)
	c := candidateInfo(t, "c", preemptorCQName, 1)
	d := candidateInfo(t, "d", preemptorCQName, 1)
	z := candidateInfo(t, "z", preemptorCQName, 1)
	real1 := candidateInfo(t, "real1", preemptorCQName, 1)
	real2 := candidateInfo(t, "real2", preemptorCQName, 1)
	fake := candidateInfo(t, "fake", preemptorCQName, 1)
	for i := range fake.TotalRequests {
		fake.TotalRequests[i].Flavors = nil
	}
	c1 := candidateInfo(t, "c1", otherCQName, 1)
	c2 := candidateInfo(t, "c2", otherCQName, 1)

	// ctxWithin: reclaim=Never, so cross-CQ candidates are not filtered by
	// policy; used for pure iteration-order cases.
	ctxWithin := preemptorCtx(t, 10, kueue.PreemptionPolicyAny, kueue.PreemptionPolicyNever, nil)
	// ctxReclaim: reclaim=Any but borrow forbidden; other-CQ candidates
	// classify as ReclaimWithoutBorrowing and are skipped on a borrow run.
	ctxReclaim := preemptorCtx(t, 10, kueue.PreemptionPolicyNever, kueue.PreemptionPolicyAny, nil)

	cases := map[string]struct {
		ctx      *HierarchicalPreemptionCtx
		segments [numSegments][]candidateBucket
		// first pass
		borrow bool
		want   []string // nil means expect no results
		// optional Reset + second pass
		doReset        bool
		resetBorrow    bool
		wantAfterReset []string
	}{
		"merges buckets from the same segment in comparator order": {
			ctx: ctxWithin,
			segments: func() [numSegments][]candidateBucket {
				var s [numSegments][]candidateBucket
				s[0] = []candidateBucket{
					segmentBucket([]*workload.Info{a, c}, false),
					segmentBucket([]*workload.Info{b, d}, false),
				}
				return s
			}(),
			borrow: false,
			want:   []string{"a", "b", "c", "d"},
		},
		"earlier segment always precedes a later segment regardless of element order": {
			// "z" sits in segment 2, "a" in segment 3: segment index must win over cmp.
			ctx: ctxWithin,
			segments: func() [numSegments][]candidateBucket {
				var s [numSegments][]candidateBucket
				s[2] = []candidateBucket{segmentBucket([]*workload.Info{z}, false)}
				s[3] = []candidateBucket{segmentBucket([]*workload.Info{a}, false)}
				return s
			}(),
			borrow: false,
			want:   []string{"z", "a"},
		},
		"Never candidates are silently skipped": {
			// fake uses no contested flavor -> classifies as Never; real1/real2 pass.
			ctx: ctxWithin,
			segments: func() [numSegments][]candidateBucket {
				var s [numSegments][]candidateBucket
				s[0] = []candidateBucket{segmentBucket([]*workload.Info{real1, fake, real2}, false)}
				return s
			}(),
			borrow: false,
			want:   []string{"real1", "real2"},
		},
		"workloads removed from the Snapshot are silently skipped": {
			ctx: ctxWithin,
			segments: func() [numSegments][]candidateBucket {
				var s [numSegments][]candidateBucket
				bkt := segmentBucket([]*workload.Info{a, b, c}, false)
				delete(bkt.cq.Workloads, workload.Key(b.Obj))
				s[0] = []candidateBucket{bkt}
				return s
			}(),
			borrow: false,
			want:   []string{"a", "c"},
		},
		"first workload removed from the Snapshot is skipped": {
			ctx: ctxWithin,
			segments: func() [numSegments][]candidateBucket {
				var s [numSegments][]candidateBucket
				bkt := segmentBucket([]*workload.Info{a, b, c}, false)
				delete(bkt.cq.Workloads, workload.Key(a.Obj))
				s[0] = []candidateBucket{bkt}
				return s
			}(),
			borrow: false,
			want:   []string{"b", "c"},
		},
		"bucket with all workloads removed from the Snapshot is exhausted": {
			ctx: ctxWithin,
			segments: func() [numSegments][]candidateBucket {
				var s [numSegments][]candidateBucket
				bkt := segmentBucket([]*workload.Info{a, b, c}, false)
				clear(bkt.cq.Workloads)
				s[0] = []candidateBucket{bkt}
				return s
			}(),
			borrow: false,
			want:   nil,
		},
		"borrow run skips ReclaimWithoutBorrowing; non-borrow run after Reset sees them": {
			ctx: ctxReclaim,
			segments: func() [numSegments][]candidateBucket {
				var s [numSegments][]candidateBucket
				s[0] = []candidateBucket{segmentBucket([]*workload.Info{c1, c2}, false)}
				return s
			}(),
			borrow:         true,
			want:           nil,
			doReset:        true,
			resetBorrow:    false,
			wantAfterReset: []string{"c1", "c2"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			it := &cacheCandidateIterator{segments: tc.segments, cmp: byName, hierarchicalReclaimCtx: tc.ctx}
			if got := drainIter(it, tc.borrow); !slices.Equal(got, tc.want) {
				t.Errorf("first pass = %v, want %v", got, tc.want)
			}
			if tc.doReset {
				it.Reset()
				if got := drainIter(it, tc.resetBorrow); !slices.Equal(got, tc.wantAfterReset) {
					t.Errorf("after Reset = %v, want %v", got, tc.wantAfterReset)
				}
			}
		})
	}
}

func TestResetSkipsWorkloadsRemovedFromSnapshot(t *testing.T) {
	cases := map[string]struct {
		removed []string
		want    []string
	}{
		"first workload": {
			removed: []string{"a"},
			want:    []string{"b", "c"},
		},
		"middle workload": {
			removed: []string{"b"},
			want:    []string{"a", "c"},
		},
		"last workload": {
			removed: []string{"c"},
			want:    []string{"a", "b"},
		},
		"all workloads": {
			removed: []string{"a", "b", "c"},
			want:    nil,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			workloads := []*workload.Info{
				candidateInfo(t, "a", preemptorCQName, 1),
				candidateInfo(t, "b", preemptorCQName, 1),
				candidateInfo(t, "c", preemptorCQName, 1),
			}
			workloadsByName := make(map[string]*workload.Info, len(workloads))
			for _, wl := range workloads {
				workloadsByName[wl.Obj.Name] = wl
			}
			bkt := segmentBucket(workloads, false)
			it := &cacheCandidateIterator{
				segments:               [numSegments][]candidateBucket{{bkt}},
				cmp:                    byName,
				hierarchicalReclaimCtx: preemptorCtx(t, 10, kueue.PreemptionPolicyAny, kueue.PreemptionPolicyNever, nil),
			}

			if got, want := drainIter(it, false), []string{"a", "b", "c"}; !slices.Equal(got, want) {
				t.Fatalf("first pass = %v, want %v", got, want)
			}

			for _, removed := range tc.removed {
				delete(bkt.cq.Workloads, workload.Key(workloadsByName[removed].Obj))
			}
			it.Reset()
			if got := drainIter(it, false); !slices.Equal(got, tc.want) {
				t.Errorf("after Reset = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBucketCursor(t *testing.T) {
	w := []*workload.Info{
		{Obj: utiltestingapi.MakeWorkload("a", "ns").Obj()},
		{Obj: utiltestingapi.MakeWorkload("b", "ns").Obj()},
		{Obj: utiltestingapi.MakeWorkload("c", "ns").Obj()},
	}

	t.Run("forward consumption", func(t *testing.T) {
		b := segmentBucket(w, false)
		got := []string{}
		for !b.empty() {
			got = append(got, b.top().Obj.Name)
			b.advance()
		}
		if !slices.Equal(got, []string{"a", "b", "c"}) {
			t.Errorf("consumed %v, want [a b c]", got)
		}
		if !b.empty() {
			t.Error("bucket should be empty after full consumption")
		}
	})

	t.Run("empty slice", func(t *testing.T) {
		b := segmentBucket(nil, false)
		if !b.empty() {
			t.Error("nil-backed bucket should be empty")
		}
	})

	t.Run("drain exhausts immediately", func(t *testing.T) {
		b := segmentBucket(w, false)
		if b.empty() {
			t.Fatal("bucket should start non-empty")
		}
		b.drain()
		if !b.empty() {
			t.Error("bucket should be empty after drain()")
		}
	})

	t.Run("reset rewinds after cursor movement", func(t *testing.T) {
		b := segmentBucket(w, false)
		b.advance()
		b.advance()
		b.reset()
		if b.empty() || b.top().Obj.Name != "a" {
			t.Errorf("after reset top should be a, got empty=%v", b.empty())
		}
	})

	t.Run("reset rewinds after drain", func(t *testing.T) {
		b := segmentBucket(w, false)
		b.drain()
		b.reset()
		if b.empty() || b.top().Obj.Name != "a" {
			t.Error("reset must rewind a drained bucket so it participates in the next run")
		}
	})
}

func TestBuildBuckets(t *testing.T) {
	e := evictedInfo(t, "e", true)
	n1 := evictedInfo(t, "n1", false)
	n2 := evictedInfo(t, "n2", false)

	cqA := segmentBucket([]*workload.Info{e, n1}, false).cq
	cqA.Name = "cqA"
	cqB := segmentBucket([]*workload.Info{n2}, false).cq
	cqB.Name = "cqB"
	cqEmpty := segmentBucket(nil, false).cq
	cqEmpty.Name = "cqEmpty"

	cases := map[string]struct {
		classes           []classifiedClusterQueue
		wantEvicted       int
		wantNonEvicted    int
		wantEvictedCQ     string
		wantEvictedTop    string
		wantNonEvictedCQs []string
	}{
		"evicted bucket, non-evicted bucket, empty CQ skipped": {
			classes: []classifiedClusterQueue{
				// cqA has both an evicted prefix (evictedCount=1) and a non-evicted tail.
				// buildBuckets splits at the cache-computed evictedCount, not by re-probing
				// the workload conditions.
				{cq: cqA, sortedWorkloads: []*workload.Info{e, n1}, evictedCount: 1},
				// cqB has only a non-evicted workload.
				{cq: cqB, sortedWorkloads: []*workload.Info{n2}, evictedCount: 0},
				// cqEmpty contributes nothing.
				{cq: cqEmpty, sortedWorkloads: nil},
			},
			wantEvicted:       1,
			wantNonEvicted:    2,
			wantEvictedCQ:     "cqA",
			wantEvictedTop:    "e",
			wantNonEvictedCQs: []string{"cqA", "cqB"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			evicted, nonEvicted := buildBuckets(tc.classes, false)

			if len(evicted) != tc.wantEvicted {
				t.Fatalf("evicted buckets = %d, want %d", len(evicted), tc.wantEvicted)
			}
			if tc.wantEvicted > 0 {
				if evicted[0].cqSnapshot().Name != kueue.ClusterQueueReference(tc.wantEvictedCQ) || evicted[0].top().Obj.Name != tc.wantEvictedTop {
					t.Errorf("evicted bucket = (%q, %q), want (%q, %q)", evicted[0].cqSnapshot().Name, evicted[0].top().Obj.Name, tc.wantEvictedCQ, tc.wantEvictedTop)
				}
			}

			if len(nonEvicted) != tc.wantNonEvicted {
				t.Fatalf("non-evicted buckets = %d, want %d", len(nonEvicted), tc.wantNonEvicted)
			}
			gotCQs := make([]string, len(nonEvicted))
			for i, b := range nonEvicted {
				gotCQs[i] = string(b.cqSnapshot().Name)
			}
			if !slices.Equal(gotCQs, tc.wantNonEvictedCQs) {
				t.Errorf("non-evicted bucket CQs = %v, want %v", gotCQs, tc.wantNonEvictedCQs)
			}
		})
	}
}

func TestPendingReclaimConsumptionOrder(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.PartialPreemption, true)

	want := []string{"m3", "l1", "l2", "m3", "h4"}
	// Steps: whole-job victims are preempted once; the pending-reclaim partial job
	// M3 is reused in the evicted pass and then shed once more below its target.
	newSteps := func() map[string]int {
		return map[string]int{"l1": 1, "l2": 1, "m3": 2, "h4": 1}
	}

	for _, cache := range []bool{false, true} {
		name := "nocache"
		if cache {
			name = "cache"
		}
		t.Run(name, func(t *testing.T) {
			cq := &schdcache.ClusterQueueSnapshot{
				Name:      preemptorCQName,
				Workloads: map[workload.Reference]*workload.Info{},
			}
			mk := func(name string, prio int32, pending bool) *workload.Info {
				info := candidateInfo(t, name, preemptorCQName, prio)
				info.IsPartialPreemptionJob = pending
				info.HasPendingPreemptionTarget = pending
				cq.Workloads[workload.Key(info.Obj)] = info
				return info
			}
			// Insert out of priority order to prove the sort - not insertion order -
			// decides placement.
			h4 := mk("h4", 30, false)
			m3 := mk("m3", 20, true)
			l1 := mk("l1", 0, false)
			l2 := mk("l2", 10, false)

			ctx := preemptorCtx(t, 100, kueue.PreemptionPolicyAny, kueue.PreemptionPolicyNever, nil)
			frs := sets.New(resources.FlavorResource{Flavor: testFlavor, Resource: corev1.ResourceCPU})

			sorted := []*workload.Info{h4, m3, l1, l2}
			slices.SortFunc(sorted, func(a, b *workload.Info) int {
				return policy.CandidatesOrdering(ctx.Log, false, a, b, preemptorCQName, time.Now())
			})

			var it CandidateIterator
			if !cache {
				elems := make([]*candidateElem, len(sorted))
				for i, info := range sorted {
					elems[i] = &candidateElem{wl: info}
				}
				segments := buildSegments(nil, nil, elems)
				it = &candidateIterator{
					segments:          segments,
					singleRoundGroups: numEvictedGroups,
					bandStart:         -1,
					snapshot: &schdcache.Snapshot{
						Manager: hierarchy.NewManagerForTest(
							map[kueue.CohortReference]*schdcache.CohortSnapshot{},
							map[kueue.ClusterQueueReference]*schdcache.ClusterQueueSnapshot{preemptorCQName: cq},
						),
					},
					hierarchicalReclaimCtx: ctx,
					frsNeedPreemption:      frs,
				}
			} else {
				classes := []classifiedClusterQueue{{cq: cq, isSameQueue: true, sortedWorkloads: sorted, evictedCount: 0}}
				evicted, nonEvicted := buildBuckets(classes, false)
				var segs [numSegments][]candidateBucket
				segs[2] = evicted
				segs[5] = nonEvicted
				cmp := func(a, b *workload.Info) int {
					return policy.CandidatesOrdering(ctx.Log, false, a, b, preemptorCQName, time.Now())
				}
				it = &cacheCandidateIterator{
					segments:               segs,
					cmp:                    cmp,
					hierarchicalReclaimCtx: ctx,
					frsNeedPreemption:      frs,
				}
			}

			steps := newSteps()
			var got []string
			for {
				wl, _ := it.Next(false)
				if wl == nil {
					break
				}
				got = append(got, wl.Obj.Name)
				steps[wl.Obj.Name]--
				if steps[wl.Obj.Name] <= 0 {
					delete(cq.Workloads, workload.Key(wl.Obj))
				}
			}
			if !slices.Equal(got, want) {
				t.Errorf("order = %v, want %v", got, want)
			}
		})
	}
}

// consume drains the iterator, dropping each victim once its step budget is spent.
func consume(it CandidateIterator, cq *schdcache.ClusterQueueSnapshot, steps map[string]int) []string {
	var got []string
	for {
		wl, _ := it.Next(false)
		if wl == nil {
			break
		}
		got = append(got, wl.Obj.Name)
		steps[wl.Obj.Name]--
		if steps[wl.Obj.Name] <= 0 {
			delete(cq.Workloads, workload.Key(wl.Obj))
		}
	}
	return got
}

// buildIterator lays a variable-length per-slot candidate layout into either
// iterator, selected by cache, so both implementations can be asserted to yield
// the same order.
func buildIterator(cq *schdcache.ClusterQueueSnapshot, segments [][]*workload.Info, cache, partial, strictPriority bool, ctx *HierarchicalPreemptionCtx) CandidateIterator {
	frs := sets.New(resources.FlavorResource{Flavor: testFlavor, Resource: corev1.ResourceCPU})
	if !cache {
		singleRoundGroups := numEvictedGroups
		if !partial {
			singleRoundGroups = len(segments)
		}
		var segArr [numSegments][]*candidateElem
		for i, slot := range segments {
			if i >= numSegments {
				break
			}
			elems := make([]*candidateElem, len(slot))
			for j, info := range slot {
				elems[j] = &candidateElem{wl: info}
			}
			segArr[i] = elems
		}
		return &candidateIterator{
			segments:          segArr,
			singleRoundGroups: singleRoundGroups,
			strictPriority:    strictPriority,
			bandStart:         -1,
			snapshot: &schdcache.Snapshot{
				Manager: hierarchy.NewManagerForTest(
					map[kueue.CohortReference]*schdcache.CohortSnapshot{},
					map[kueue.ClusterQueueReference]*schdcache.ClusterQueueSnapshot{preemptorCQName: cq},
				),
			},
			hierarchicalReclaimCtx: ctx,
			frsNeedPreemption:      frs,
		}
	}

	cmp := func(a, b *workload.Info) int {
		if d := int(priority.Priority(a.Obj)) - int(priority.Priority(b.Obj)); d != 0 {
			return d
		}
		return strings.Compare(a.Obj.Name, b.Obj.Name)
	}
	var segs [numSegments][]candidateBucket
	for i, slot := range segments {
		if i >= numSegments || len(slot) == 0 {
			continue
		}
		var b candidateBucket
		if partial && i >= numEvictedGroups {
			b = &partialBucket{cq: cq, isSameQueue: true, sortedWorkloads: slot, bandStart: -1, strictPriority: strictPriority}
		} else {
			b = &bucket{cq: cq, isSameQueue: true, sortedWorkloads: slot}
		}
		segs[i] = []candidateBucket{b}
	}
	return &cacheCandidateIterator{
		segments:               segs,
		cmp:                    cmp,
		hierarchicalReclaimCtx: ctx,
		frsNeedPreemption:      frs,
	}
}

// TestCandidateIteratorOrder exercises the multi-round and strict-priority band
// logic of the non-cache iterator (and the cache iterator for parity) over the full
// 6-slot layout. Both implementations must yield the identical order.
func TestCandidateIteratorOrder(t *testing.T) {
	cases := map[string]struct {
		partial        bool
		strictPriority bool
		build          func(t *testing.T) (segments [][]*workload.Info, steps map[string]int)
		want           []string
	}{
		"strict priority preempts low band before high band": {
			partial:        true,
			strictPriority: true,
			build: func(t *testing.T) ([][]*workload.Info, map[string]int) {
				v1 := candidate(t, "v1", 0, true, false, false)
				v2 := candidate(t, "v2", 0, true, false, false)
				v3 := candidate(t, "v3", 1000, true, false, false)
				v4 := candidate(t, "v4", 1000, true, false, false)
				return [][]*workload.Info{nil, nil, nil, {v1, v2, v3, v4}, nil, nil},
					map[string]int{"v1": 2, "v2": 2, "v3": 2, "v4": 2}
			},
			want: []string{"v1", "v2", "v1", "v2", "v3", "v4", "v3", "v4"},
		},
		"strict priority round robins within one band": {
			partial:        true,
			strictPriority: true,
			build: func(t *testing.T) ([][]*workload.Info, map[string]int) {
				v1 := candidate(t, "v1", 0, true, false, false)
				v2 := candidate(t, "v2", 0, true, false, false)
				v3 := candidate(t, "v3", 0, true, false, false)
				return [][]*workload.Info{nil, nil, nil, {v1, v2, v3}, nil, nil},
					map[string]int{"v1": 2, "v2": 2, "v3": 2}
			},
			want: []string{"v1", "v2", "v3", "v1", "v2", "v3"},
		},
		"no strict priority interleaves across priorities": {
			partial: true,
			build: func(t *testing.T) ([][]*workload.Info, map[string]int) {
				v1 := candidate(t, "v1", 0, true, false, false)
				v2 := candidate(t, "v2", 1000, true, false, false)
				return [][]*workload.Info{nil, nil, nil, {v1, v2}, nil, nil},
					map[string]int{"v1": 2, "v2": 2}
			},
			want: []string{"v1", "v2", "v1", "v2"},
		},
		"pending reclaim target is reused in the evicted pass then keeps shedding": {
			partial: true,
			build: func(t *testing.T) ([][]*workload.Info, map[string]int) {
				v1 := candidate(t, "v1", 0, true, false, false)
				v2 := candidate(t, "v2", 0, true, true, false)
				// A pending candidate sits in both an evicted and a non-evicted slot.
				return [][]*workload.Info{nil, nil, {v2}, nil, nil, {v2, v1}},
					map[string]int{"v1": 1, "v2": 2}
			},
			want: []string{"v2", "v2", "v1"},
		},
		"trailing partial triggers another round": {
			partial: true,
			build: func(t *testing.T) ([][]*workload.Info, map[string]int) {
				v1 := candidate(t, "v1", 0, false, false, false)
				v2 := candidate(t, "v2", 0, true, false, false)
				return [][]*workload.Info{nil, nil, nil, {v1, v2}, nil, nil},
					map[string]int{"v1": 1, "v2": 2}
			},
			want: []string{"v1", "v2", "v2"},
		},
		"partial before whole job keeps the round alive": {
			// A partial returned before a whole job must still trigger a second round.
			partial: true,
			build: func(t *testing.T) ([][]*workload.Info, map[string]int) {
				v1 := candidate(t, "v1", 0, true, false, false)
				v2 := candidate(t, "v2", 0, false, false, false)
				return [][]*workload.Info{nil, nil, nil, {v1, v2}, nil, nil},
					map[string]int{"v1": 2, "v2": 1}
			},
			want: []string{"v1", "v2", "v1"},
		},
		"partial gate off walks one forward pass": {
			build: func(t *testing.T) ([][]*workload.Info, map[string]int) {
				v1 := candidate(t, "v1", 0, false, false, false)
				v2 := candidate(t, "v2", 1000, false, false, false)
				v3 := candidate(t, "v3", 0, false, false, false)
				return [][]*workload.Info{nil, nil, nil, {v1, v2, v3}, nil, nil},
					map[string]int{"v1": 1, "v2": 1, "v3": 1}
			},
			want: []string{"v1", "v2", "v3"},
		},
		"evicted slots are walked before non-evicted": {
			partial: true,
			build: func(t *testing.T) ([][]*workload.Info, map[string]int) {
				v1 := candidate(t, "v1", 0, true, false, false)
				v2 := candidate(t, "v2", 0, true, false, false)
				return [][]*workload.Info{nil, {v1}, nil, {v2}, nil, nil},
					map[string]int{"v1": 1, "v2": 2}
			},
			want: []string{"v1", "v2", "v2"},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.PartialPreemption, tc.partial)

			for _, cache := range []bool{false, true} {
				sub := "nocache"
				if cache {
					sub = "cache"
				}
				t.Run(sub, func(t *testing.T) {
					segments, steps := tc.build(t)
					var infos []*workload.Info
					for _, slot := range segments {
						infos = append(infos, slot...)
					}
					cq := makeWorkloadCQ(infos)
					cq.Name = preemptorCQName
					ctx := preemptorCtx(t, 10, kueue.PreemptionPolicyAny, kueue.PreemptionPolicyNever, nil)

					it := buildIterator(cq, segments, cache, tc.partial, tc.strictPriority, ctx)
					if got := consume(it, cq, steps); !slices.Equal(got, tc.want) {
						t.Errorf("order = %v, want %v", got, tc.want)
					}
				})
			}
		})
	}
}

// TestSplitEvicted checks that splitEvicted puts the evicted prefix plus every
// pending-target candidate in the evicted group, keeps the whole non-evicted part
// (pending candidates included) in the non-evicted group, and shares a candidate
// that lands in both groups by pointer rather than copying it.
func TestSplitEvicted(t *testing.T) {
	cases := map[string]struct {
		build          func(t *testing.T) []*candidateElem
		wantEvicted    []string
		wantNonEvicted []string
	}{
		"evicted prefix and pending sub-prefix": {
			build: func(t *testing.T) []*candidateElem {
				return []*candidateElem{
					{wl: candidate(t, "v1", 0, false, false, true)},
					{wl: candidate(t, "v2", 0, false, true, false)},
					{wl: candidate(t, "v3", 0, false, false, false)},
				}
			},
			wantEvicted:    []string{"v1", "v2"},
			wantNonEvicted: []string{"v2", "v3"},
		},
		"all evicted": {
			build: func(t *testing.T) []*candidateElem {
				return []*candidateElem{
					{wl: candidate(t, "v1", 0, false, false, true)},
					{wl: candidate(t, "v2", 0, false, false, true)},
				}
			},
			wantEvicted:    []string{"v1", "v2"},
			wantNonEvicted: nil,
		},
		"no evicted and no pending": {
			build: func(t *testing.T) []*candidateElem {
				return []*candidateElem{
					{wl: candidate(t, "v1", 0, false, false, false)},
					{wl: candidate(t, "v2", 0, false, false, false)},
				}
			},
			wantEvicted:    nil,
			wantNonEvicted: []string{"v1", "v2"},
		},
		"nil input": {
			build:          func(t *testing.T) []*candidateElem { return nil },
			wantEvicted:    nil,
			wantNonEvicted: nil,
		},
		"empty input": {
			build:          func(t *testing.T) []*candidateElem { return []*candidateElem{} },
			wantEvicted:    nil,
			wantNonEvicted: nil,
		},
	}

	names := func(cs []*candidateElem) []string {
		if len(cs) == 0 {
			return nil
		}
		out := make([]string, len(cs))
		for i, c := range cs {
			out[i] = c.wl.Obj.Name
		}
		return out
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			evicted, nonEvicted := splitEvicted(tc.build(t))

			if got := names(evicted); !slices.Equal(got, tc.wantEvicted) {
				t.Errorf("evicted = %v, want %v", got, tc.wantEvicted)
			}
			if got := names(nonEvicted); !slices.Equal(got, tc.wantNonEvicted) {
				t.Errorf("nonEvicted = %v, want %v", got, tc.wantNonEvicted)
			}
			// A candidate in both groups must be shared by pointer, not copied.
			nonEvictedByName := map[string]*candidateElem{}
			for _, e := range nonEvicted {
				nonEvictedByName[e.wl.Obj.Name] = e
			}
			for _, e := range evicted {
				if other, ok := nonEvictedByName[e.wl.Obj.Name]; ok && other != e {
					t.Errorf("candidate %q appears in both groups but is not the same pointer", e.wl.Obj.Name)
				}
			}
		})
	}
}
