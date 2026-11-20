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
	"iter"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/scheduler/flavorassigner"
	preemptexpectations "sigs.k8s.io/kueue/pkg/scheduler/preemption/expectations"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingalpha "sigs.k8s.io/kueue/pkg/util/testing/v1alpha1"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
	workloadevict "sigs.k8s.io/kueue/pkg/workload/evict"
)

// victimPodSet is the shrinkable PodSet used by partialPreemptionVictim.
const victimPodSet kueue.PodSetReference = "executor"

func byName(a, b *workload.Info) int {
	return strings.Compare(a.Obj.Name, b.Obj.Name)
}

func newPlanPreemptionCtx(t *testing.T, quota string, victims []*kueue.Workload, incomingCPU string) (context.Context, *preemptionCtx) {
	t.Helper()
	ctx, log := utiltesting.ContextWithLog(t)
	items := make([]kueue.Workload, len(victims))
	for i := range victims {
		items[i] = *victims[i]
	}
	cl := utiltesting.NewClientBuilder().
		WithLists(&kueue.WorkloadList{Items: items}).
		Build()
	cqCache := schdcache.New(cl)
	cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
	cq := partialPreemptionClusterQueue(quota)
	if err := cqCache.AddClusterQueue(ctx, cq); err != nil {
		t.Fatalf("Couldn't add ClusterQueue to cache: %v", err)
	}
	preemptor := New(cl, workload.Ordering{}, &utiltesting.EventRecorder{}, nil, false,
		clocktesting.NewFakeClock(time.Now()), nil, preemptexpectations.New(), nil)
	snapshot, err := cqCache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("unexpected error while building snapshot: %v", err)
	}
	wlInfo := workload.NewInfo(log, utiltestingapi.MakeWorkload("in", "").
		Request(corev1.ResourceCPU, incomingCPU).Obj())
	wlInfo.ClusterQueue = kueue.ClusterQueueReference(cq.Name)
	pCtx := preemptor.buildContext(ctx, *wlInfo, singlePodSetAssignment(flavorassigner.ResourceAssignment{
		corev1.ResourceCPU: &flavorassigner.FlavorAssignment{Name: "default", Mode: flavorassigner.Preempt},
	}), snapshot)
	return ctx, pCtx
}

func snapshotExecutorCount(pCtx *preemptionCtx, wl *kueue.Workload) (int32, bool) {
	info := pCtx.preemptorCQ.Workloads[workload.Key(wl)]
	if info == nil {
		return 0, false
	}
	return podSetRequestCount(info, victimPodSet), true
}

func partialPreemptionClusterQueue(cpuQuota string) *kueue.ClusterQueue {
	return utiltestingapi.MakeClusterQueue("standalone").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").
			Resource(corev1.ResourceCPU, cpuQuota).Obj()).
		Preemption(kueue.ClusterQueuePreemption{
			WithinClusterQueue: kueue.PreemptionPolicyLowerPriority,
		}).
		Obj()
}

// partialPreemptionVictim builds an elastic victim; preemptionRetainedCount seeds an in-flight shrink.
func partialPreemptionVictim(name string, priorityValue, usedCount, minCount int32, preemptionRetainedCount *int32) *kueue.Workload {
	wl := utiltestingapi.MakeWorkload(name, "").
		UID(types.UID(name)).
		Annotation(constants.PartialPreemptionAnnotation, "true").
		Priority(priorityValue).
		PodSets(*utiltestingapi.MakePodSet("executor", int(usedCount)).
			Request(corev1.ResourceCPU, "1").
			SetPartialPreemptionMinRetainedCount(minCount).
			Obj()).
		ReserveQuotaAt(utiltestingapi.MakeAdmission("standalone").
			PodSets(utiltestingapi.MakePodSetAssignment("executor").
				Assignment(corev1.ResourceCPU, "default", fmt.Sprintf("%d", usedCount)).
				Count(usedCount).
				Obj()).
			Obj(), time.Now()).
		Obj()
	if preemptionRetainedCount != nil {
		wl.Status.Admission.PodSetAssignments[0].PartialPreemption = &kueue.PodSetPartialPreemptionStatus{RetainedCount: preemptionRetainedCount}
	}
	return wl
}

// partialPreemptionVictimInCQ is partialPreemptionVictim admitted into cq.
func partialPreemptionVictimInCQ(cq kueue.ClusterQueueReference, name string, priorityValue, usedCount, minCount int32) *kueue.Workload {
	return utiltestingapi.MakeWorkload(name, "").
		UID(types.UID(name)).
		Annotation(constants.PartialPreemptionAnnotation, "true").
		Priority(priorityValue).
		PodSets(*utiltestingapi.MakePodSet("executor", int(usedCount)).
			Request(corev1.ResourceCPU, "1").
			SetPartialPreemptionMinRetainedCount(minCount).
			Obj()).
		ReserveQuotaAt(utiltestingapi.MakeAdmission(cq).
			PodSets(utiltestingapi.MakePodSetAssignment("executor").
				Assignment(corev1.ResourceCPU, "default", fmt.Sprintf("%d", usedCount)).
				Count(usedCount).
				Obj()).
			Obj(), time.Now()).
		Obj()
}

// partialPreemptionFixture is the environment the partial-preemption tests share: victims and
// optional PreemptionConfigs in a fake client, the ClusterQueue cache, and the snapshot pair
// used to check that the search leaves the cache untouched.
type partialPreemptionFixture struct {
	ctx                      context.Context
	preemptor                *Preemptor
	strategies               iter.Seq[PreemptionStrategy]
	beforeSnapshot, snapshot *schdcache.Snapshot
}

type partialPreemptionFixtureCfg struct {
	cqs        []*kueue.ClusterQueue
	configs    []*kueuealpha.PreemptionConfig
	victims    []*kueue.Workload
	incoming   *kueue.Workload
	incomingCQ kueue.ClusterQueueReference
}

func newPartialPreemptionFixture(t *testing.T, now time.Time, cfg partialPreemptionFixtureCfg) partialPreemptionFixture {
	t.Helper()
	ctx, log := utiltesting.ContextWithLog(t)
	items := make([]kueue.Workload, len(cfg.victims))
	for i := range cfg.victims {
		items[i] = *cfg.victims[i]
	}
	configItems := make([]kueuealpha.PreemptionConfig, len(cfg.configs))
	for i := range cfg.configs {
		configItems[i] = *cfg.configs[i]
	}
	cl := utiltesting.NewClientBuilder().
		WithLists(&kueue.WorkloadList{Items: items}).
		WithLists(&kueuealpha.PreemptionConfigList{Items: configItems}).
		WithStatusSubresource(&kueue.Workload{}).
		Build()
	cqCache := schdcache.New(cl)
	cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
	for _, cq := range cfg.cqs {
		if err := cqCache.AddClusterQueue(ctx, cq); err != nil {
			t.Fatalf("Couldn't add ClusterQueue %q to cache: %v", cq.Name, err)
		}
	}
	preemptor := New(cl, workload.Ordering{}, &utiltesting.EventRecorder{}, nil, false,
		clocktesting.NewFakeClock(now), nil, preemptexpectations.New(), nil)
	beforeSnapshot, err := cqCache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("unexpected error while building snapshot: %v", err)
	}
	snapshot, err := cqCache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("unexpected error while building snapshot: %v", err)
	}
	incoming := workload.NewInfo(log, cfg.incoming)
	incoming.ClusterQueue = cfg.incomingCQ
	strategies := preemptor.GetPreemptionStrategyIterator(ctx, *incoming, snapshot, singlePodSetAssignment(flavorassigner.ResourceAssignment{
		corev1.ResourceCPU: &flavorassigner.FlavorAssignment{Name: "default", Mode: flavorassigner.Preempt},
	}))
	return partialPreemptionFixture{
		ctx:            ctx,
		preemptor:      preemptor,
		strategies:     strategies,
		beforeSnapshot: beforeSnapshot,
		snapshot:       snapshot,
	}
}

// targets runs the preemption search the native assignment backend uses.
func (f partialPreemptionFixture) targets() []*Target {
	return f.preemptor.GetTargetsWithStrategy(f.ctx, f.strategies)
}

func admittedTargetCount(wl *kueue.Workload, podSet kueue.PodSetReference) (int32, bool) {
	if wl.Status.Admission == nil {
		return 0, false
	}
	for i := range wl.Status.Admission.PodSetAssignments {
		psa := &wl.Status.Admission.PodSetAssignments[i]
		if psa.Name == podSet && psa.PartialPreemption.RetainedCount != nil {
			return *psa.PartialPreemption.RetainedCount, true
		}
	}
	return 0, false
}

func admittedCount(wl *kueue.Workload, podSet kueue.PodSetReference) int32 {
	if wl.Status.Admission == nil {
		return 0
	}
	for i := range wl.Status.Admission.PodSetAssignments {
		psa := &wl.Status.Admission.PodSetAssignments[i]
		if psa.Name == podSet {
			return ptr.Deref(psa.Count, 0)
		}
	}
	return 0
}

func TestPartialPreemptionGetTargets(t *testing.T) {
	type wantTarget struct {
		partial                 bool
		preemptionRetainedCount int32
		reducedCount            *int32
		podSet                  kueue.PodSetReference
		keptCounts              map[kueue.PodSetReference]int32
	}
	headAndWorkers := func(workerFloor int32) *kueue.Workload {
		return utiltestingapi.MakeWorkload("victim", "").
			UID("victim").
			Annotation(constants.PartialPreemptionAnnotation, "true").
			PodSets(
				*utiltestingapi.MakePodSet("head", 1).Request(corev1.ResourceCPU, "2").Obj(),
				*utiltestingapi.MakePodSet("worker", 8).Request(corev1.ResourceCPU, "1").SetPartialPreemptionMinRetainedCount(workerFloor).Obj(),
			).
			ReserveQuotaAt(utiltestingapi.MakeAdmission("standalone").PodSets(
				utiltestingapi.MakePodSetAssignment("head").Assignment(corev1.ResourceCPU, "default", "2").Count(1).Obj(),
				utiltestingapi.MakePodSetAssignment("worker").Assignment(corev1.ResourceCPU, "default", "8").Count(8).Obj(),
			).Obj(), time.Now()).
			Obj()
	}
	evicting := func(wl *kueue.Workload) *kueue.Workload {
		workloadevict.SetEvictedCondition(wl, time.Now(), kueue.WorkloadEvictedByPreemption, "already evicted")
		return wl
	}
	cases := map[string]struct {
		gateDisabled bool
		quota        string
		victims      []*kueue.Workload
		incoming     *kueue.Workload
		want         map[string]wantTarget
		wantOrder    []string
	}{
		"gate on: reduces the victim to free only what the incoming workload needs": {
			quota:    "6",
			victims:  []*kueue.Workload{partialPreemptionVictim("victim", 0, 5, 1, nil)},
			incoming: utiltestingapi.MakeWorkload("in", "").UID("wl-in").Priority(1).Request(corev1.ResourceCPU, "4").Obj(),
			want:     map[string]wantTarget{"victim": {partial: true, preemptionRetainedCount: 2}},
		},
		"gate off: whole eviction": {
			gateDisabled: true,
			quota:        "6",
			victims:      []*kueue.Workload{partialPreemptionVictim("victim", 0, 5, 1, nil)},
			incoming:     utiltestingapi.MakeWorkload("in", "").UID("wl-in").Priority(1).Request(corev1.ResourceCPU, "4").Obj(),
			want:         map[string]wantTarget{"victim": {partial: false}},
		},
		"no fit: do not return an in-flight partial target": {
			quota:    "6",
			victims:  []*kueue.Workload{partialPreemptionVictim("victim", 0, 5, 1, new(int32(2)))},
			incoming: utiltestingapi.MakeWorkload("no-fit", "").UID("wl-no-fit").Priority(1).Request(corev1.ResourceCPU, "7").Obj(),
			want:     nil,
		},
		"reclaims only enough": {
			quota:    "20",
			victims:  []*kueue.Workload{partialPreemptionVictim("victim", 0, 20, 5, nil)},
			incoming: utiltestingapi.MakeWorkload("incoming", "").UID("incoming").Priority(1).Request(corev1.ResourceCPU, "10").Obj(),
			want:     map[string]wantTarget{"victim": {partial: true, preemptionRetainedCount: 10}},
		},
		"multiple candidates each partially reduced": {
			quota: "10",
			victims: []*kueue.Workload{
				partialPreemptionVictim("v1", 0, 5, 4, nil),
				partialPreemptionVictim("v2", 0, 5, 4, nil),
			},
			incoming: utiltestingapi.MakeWorkload("in", "").UID("wl-in").Priority(1).Request(corev1.ResourceCPU, "2").Obj(),
			want: map[string]wantTarget{
				"v1": {partial: true, preemptionRetainedCount: 4},
				"v2": {partial: true, preemptionRetainedCount: 4},
			},
		},
		"round-two merge preserves priority for a whole-only candidate": {
			quota: "15",
			victims: []*kueue.Workload{
				partialPreemptionVictim("H1", 30, 5, 1, nil),
				partialPreemptionVictim("M1", 20, 5, 1, nil),
				partialPreemptionVictim("L1", 10, 5, 5, nil),
			},
			incoming: utiltestingapi.MakeWorkload("incoming", "").UID("incoming").Priority(40).Request(corev1.ResourceCPU, "12").Obj(),
			want: map[string]wantTarget{
				"H1": {partial: true, preemptionRetainedCount: 2},
				"M1": {partial: true, preemptionRetainedCount: 1},
				"L1": {partial: false},
			},
			wantOrder: []string{"L1", "M1", "H1"},
		},
		"multi-round progresses to full eviction": {
			quota: "10",
			victims: []*kueue.Workload{
				partialPreemptionVictim("victim-a", 0, 5, 1, nil),
				partialPreemptionVictim("victim-b", 0, 5, 5, nil),
			},
			incoming: utiltestingapi.MakeWorkload("incoming", "").UID("incoming").Priority(1).Request(corev1.ResourceCPU, "8").Obj(),
			want: map[string]wantTarget{
				"victim-a": {partial: true, preemptionRetainedCount: 2},
				"victim-b": {partial: false},
			},
		},
		"evicted partial-preemption victim is wholly evicted": {
			quota: "6",
			victims: []*kueue.Workload{func() *kueue.Workload {
				wl := partialPreemptionVictim("victim", 0, 5, 1, nil)
				workloadevict.SetEvictedCondition(wl, time.Now(), kueue.WorkloadEvictedByPreemption, "already evicted")
				return wl
			}()},
			incoming: utiltestingapi.MakeWorkload("in", "").UID("wl-in").Priority(1).Request(corev1.ResourceCPU, "4").Obj(),
			want:     map[string]wantTarget{"victim": {partial: false}},
		},
		"TAS victim is wholly evicted": {
			quota: "6",
			victims: []*kueue.Workload{func() *kueue.Workload {
				wl := partialPreemptionVictim("victim", 0, 5, 1, nil)
				wl.Spec.PodSets[0].TopologyRequest = &kueue.PodSetTopologyRequest{Required: new("rack")}
				wl.Status.Admission.PodSetAssignments[0].TopologyAssignment = utiltestingapi.MakeTopologyAssignment([]string{"rack"}).Obj()
				return wl
			}()},
			incoming: utiltestingapi.MakeWorkload("in", "").UID("wl-in").Priority(1).Request(corev1.ResourceCPU, "4").Obj(),
			want:     map[string]wantTarget{"victim": {partial: false}},
		},
		"multiple shrinkable PodSets are wholly evicted": {
			quota: "12",
			victims: []*kueue.Workload{func() *kueue.Workload {
				wl := partialPreemptionVictim("victim", 0, 5, 1, nil)
				second := utiltestingapi.MakePodSet("second", 5).Request(corev1.ResourceCPU, "1").SetPartialPreemptionMinRetainedCount(1).Obj()
				wl.Spec.PodSets = append(wl.Spec.PodSets, *second)
				assignment := utiltestingapi.MakePodSetAssignment("second").Assignment(corev1.ResourceCPU, "default", "5").Count(5).Obj()
				wl.Status.Admission.PodSetAssignments = append(wl.Status.Admission.PodSetAssignments, assignment)
				return wl
			}()},
			incoming: utiltestingapi.MakeWorkload("in", "").UID("wl-in").Priority(1).Request(corev1.ResourceCPU, "4").Obj(),
			want:     map[string]wantTarget{"victim": {partial: false}},
		},
		"in-flight shrink is used before a victim shrinks further": {
			quota: "10",
			victims: []*kueue.Workload{
				partialPreemptionVictim("plain", 0, 5, 1, nil),
				partialPreemptionVictim("in-flight", 1, 5, 1, new(int32(3))),
			},
			incoming: utiltestingapi.MakeWorkload("in", "").UID("wl-in").Priority(5).Request(corev1.ResourceCPU, "2").Obj(),
			want:     map[string]wantTarget{"in-flight": {partial: true, preemptionRetainedCount: 3}},
		},
		"evicting victim and in-flight shrink are used before a new shrink": {
			quota: "14",
			victims: []*kueue.Workload{
				evicting(partialPreemptionVictim("evicting", 0, 4, 4, nil)),
				partialPreemptionVictim("plain", 0, 5, 1, nil),
				partialPreemptionVictim("in-flight", 1, 5, 1, new(int32(3))),
			},
			incoming: utiltestingapi.MakeWorkload("in", "").UID("wl-in").Priority(5).Request(corev1.ResourceCPU, "6").Obj(),
			want: map[string]wantTarget{
				"evicting":  {partial: false},
				"in-flight": {partial: true, preemptionRetainedCount: 3},
			},
		},
		"workers shrink and the head stays charged": {
			quota:    "10",
			victims:  []*kueue.Workload{headAndWorkers(0)},
			incoming: utiltestingapi.MakeWorkload("in", "").UID("wl-in").Priority(5).Request(corev1.ResourceCPU, "7").Obj(),
			want: map[string]wantTarget{"victim": {
				partial: true, preemptionRetainedCount: 1, podSet: "worker",
				keptCounts: map[kueue.PodSetReference]int32{"head": 1},
			}},
		},
		"workers shrink to zero and the head stays charged": {
			quota:    "10",
			victims:  []*kueue.Workload{headAndWorkers(0)},
			incoming: utiltestingapi.MakeWorkload("in", "").UID("wl-in").Priority(5).Request(corev1.ResourceCPU, "8").Obj(),
			want: map[string]wantTarget{"victim": {
				partial: true, preemptionRetainedCount: 0, podSet: "worker",
				keptCounts: map[kueue.PodSetReference]int32{"head": 1},
			}},
		},
		"victim is evicted when the head does not fit next to the incoming workload": {
			quota:    "10",
			victims:  []*kueue.Workload{headAndWorkers(0)},
			incoming: utiltestingapi.MakeWorkload("in", "").UID("wl-in").Priority(5).Request(corev1.ResourceCPU, "9").Obj(),
			want:     map[string]wantTarget{"victim": {partial: false}},
		},
		"shrinks suffice, so the whole-only victim is not evicted": {
			quota: "10",
			victims: []*kueue.Workload{
				partialPreemptionVictim("shrinkable", 0, 5, 1, nil),
				partialPreemptionVictim("whole-only", 0, 5, 5, nil),
			},
			incoming: utiltestingapi.MakeWorkload("in", "").UID("wl-in").Priority(5).Request(corev1.ResourceCPU, "2").Obj(),
			want:     map[string]wantTarget{"shrinkable": {partial: true, preemptionRetainedCount: 3}},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.PartialPreemption, !tc.gateDisabled)
			cq := partialPreemptionClusterQueue(tc.quota)
			fixture := newPartialPreemptionFixture(t, time.Now().Truncate(time.Second), partialPreemptionFixtureCfg{
				cqs:        []*kueue.ClusterQueue{cq},
				victims:    tc.victims,
				incoming:   tc.incoming,
				incomingCQ: kueue.ClusterQueueReference(cq.Name),
			})
			targets := fixture.targets()

			if tc.want == nil {
				if len(targets) != 0 {
					t.Fatalf("GetTargets() = %+v, want no targets when the preemptor cannot fit", targets)
				}
				if diff := cmp.Diff(fixture.beforeSnapshot, fixture.snapshot, snapCmpOpts); diff != "" {
					t.Errorf("Snapshot was not restored after the failed strategy attempt (-want,+got):\n%s", diff)
				}
				return
			}
			if len(targets) != len(tc.want) {
				t.Fatalf("GetTargets() returned %d targets, want %d: %+v", len(targets), len(tc.want), targets)
			}
			if tc.wantOrder != nil {
				gotOrder := make([]string, len(targets))
				for i := range targets {
					gotOrder[i] = targets[i].WorkloadInfo.Obj.Name
				}
				if diff := cmp.Diff(tc.wantOrder, gotOrder); diff != "" {
					t.Errorf("GetTargets() order mismatch (-want,+got):\n%s", diff)
				}
			}
			for _, target := range targets {
				victimName := target.WorkloadInfo.Obj.Name
				want, ok := tc.want[victimName]
				if !ok {
					t.Errorf("unexpected target %q", victimName)
					continue
				}
				if !want.partial {
					if target.PartialPreemption != nil {
						t.Errorf("target %q: PartialPreemption = %+v, want whole eviction (nil)", victimName, target.PartialPreemption)
					}
					continue
				}
				if target.PartialPreemption == nil {
					t.Errorf("target %q: PartialPreemption = nil, want partial scale-down to %d", victimName, want.preemptionRetainedCount)
					continue
				}
				wantPodSet := victimPodSet
				if want.podSet != "" {
					wantPodSet = want.podSet
				}
				if got := target.PartialPreemption.PodSet; got != wantPodSet {
					t.Errorf("target %q: PartialPreemption.PodSet = %q, want %q", victimName, got, wantPodSet)
				}
				if got := target.PartialPreemption.RetainedCount; got != want.preemptionRetainedCount {
					t.Errorf("target %q: PreemptionRetainedCount = %d, want %d", victimName, got, want.preemptionRetainedCount)
				}
				for podSet, wantCount := range want.keptCounts {
					if got := podSetRequestCount(target.PartialPreemption.ReducedInfo, podSet); got != wantCount {
						t.Errorf("target %q: ReducedInfo keeps %d Pods of %q, want %d", victimName, got, podSet, wantCount)
					}
				}
				if want.reducedCount != nil {
					if target.PartialPreemption.ReducedInfo == nil {
						t.Errorf("target %q: ReducedInfo = nil, want count %d", victimName, *want.reducedCount)
					} else if got := target.PartialPreemption.ReducedInfo.TotalRequests[0].Count; got != *want.reducedCount {
						t.Errorf("target %q: ReducedInfo count = %d, want %d", victimName, got, *want.reducedCount)
					}
				}
			}
		})
	}
}

// Candidates() is what the WAS planner hands to the scheduler simulator, which cannot
// account for a shrunk victim. It must never offer a reduce, even when the native
// search would shrink that same victim; the victim may still be offered as a whole eviction.
func TestPartialPreemptionCandidatesExcludeShrinks(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.PartialPreemption, true)

	cases := map[string]struct {
		quota          string
		victims        []*kueue.Workload
		incoming       *kueue.Workload
		wantCandidates []workload.Reference
	}{
		"a victim that ends up shrunk is still offered whole": {
			quota:          "6",
			victims:        []*kueue.Workload{partialPreemptionVictim("victim", 0, 5, 1, nil)},
			incoming:       utiltestingapi.MakeWorkload("in", "").UID("wl-in").Priority(1).Request(corev1.ResourceCPU, "4").Obj(),
			wantCandidates: []workload.Reference{"/victim"},
		},
		"only in-flight shrinks are filtered out": {
			quota: "10",
			victims: []*kueue.Workload{
				partialPreemptionVictim("plain", 0, 5, 1, nil),
				partialPreemptionVictim("in-flight", 1, 5, 1, new(int32(3))),
			},
			incoming:       utiltestingapi.MakeWorkload("in", "").UID("wl-in").Priority(5).Request(corev1.ResourceCPU, "2").Obj(),
			wantCandidates: []workload.Reference{"/plain", "/in-flight"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cq := partialPreemptionClusterQueue(tc.quota)
			fixture := newPartialPreemptionFixture(t, time.Now().Truncate(time.Second), partialPreemptionFixtureCfg{
				cqs:        []*kueue.ClusterQueue{cq},
				victims:    tc.victims,
				incoming:   tc.incoming,
				incomingCQ: kueue.ClusterQueueReference(cq.Name),
			})

			var gotCandidates []workload.Reference
			for strategy := range fixture.strategies {
				gotCandidates = slices.AppendSeq(gotCandidates, strategy.Candidates())
				targets, err := strategy.GetTargets(gotCandidates)
				if err != nil {
					t.Fatalf("GetTargets() failed: %v", err)
				}
				for _, target := range targets {
					if target.PartialPreemption != nil {
						t.Errorf("GetTargets() returned a shrink of %q, want a whole eviction", target.WorkloadInfo.Obj.Name)
					}
				}
				break
			}
			if diff := cmp.Diff(tc.wantCandidates, gotCandidates); diff != "" {
				t.Errorf("Unexpected candidates (-want,+got):\n%s", diff)
			}
			if diff := cmp.Diff(fixture.beforeSnapshot, fixture.snapshot, snapCmpOpts); diff != "" {
				t.Errorf("Snapshot was modified by the candidates (-initial,+end):\n%s", diff)
			}
		})
	}
}

func TestPartialPreemptionGetTargetsInCohort(t *testing.T) {
	cohortCQ := func(name, cohort, quota string, preemption *kueue.ClusterQueuePreemption) *kueue.ClusterQueue {
		cq := utiltestingapi.MakeClusterQueue(name).Cohort(kueue.CohortReference(cohort)).
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, quota).Obj())
		if preemption != nil {
			cq = cq.Preemption(*preemption)
		}
		return cq.Obj()
	}
	reclaimAny := &kueue.ClusterQueuePreemption{ReclaimWithinCohort: kueue.PreemptionPolicyAny}
	cases := map[string]struct {
		gateDisabled bool
		cqs          []*kueue.ClusterQueue
		victims      []*kueue.Workload
		incoming     *kueue.Workload
		incomingCQ   kueue.ClusterQueueReference
		want         map[string]*int32
	}{
		"fill-back grows a shrunk victim past its floor": {
			cqs: []*kueue.ClusterQueue{
				cohortCQ("P", "coh1", "8", reclaimAny),
				cohortCQ("A", "coh1", "2", nil),
				cohortCQ("B", "coh1", "0", nil),
			},
			victims: []*kueue.Workload{
				partialPreemptionVictimInCQ("A", "a1", 0, 4, 2),
				partialPreemptionVictimInCQ("B", "b1", 0, 4, 3),
			},
			incoming:   utiltestingapi.MakeWorkload("p1", "").UID("p1").Request(corev1.ResourceCPU, "7").Obj(),
			incomingCQ: "P",
			want:       map[string]*int32{"a1": new(int32(3)), "b1": nil},
		},
		"gate off: the borrowing victim is evicted whole": {
			gateDisabled: true,
			cqs: []*kueue.ClusterQueue{
				cohortCQ("P2", "coh2", "8", &kueue.ClusterQueuePreemption{
					ReclaimWithinCohort: kueue.PreemptionPolicyAny,
					BorrowWithinCohort:  &kueue.BorrowWithinCohort{Policy: kueue.BorrowWithinCohortPolicyLowerPriority},
				}),
				utiltestingapi.MakeClusterQueue("A2").Cohort("coh2").
					ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "5", "20").Obj()).Obj(),
			},
			victims:    []*kueue.Workload{partialPreemptionVictimInCQ("A2", "a2", 0, 10, 5)},
			incoming:   utiltestingapi.MakeWorkload("p2", "").UID("p2").Priority(10).Request(corev1.ResourceCPU, "11").Obj(),
			incomingCQ: "P2",
			want:       map[string]*int32{"a2": nil},
		},
		"floor equal to nominal: the victim stops borrowing and is not offered again": {
			cqs: []*kueue.ClusterQueue{
				cohortCQ("P2", "coh2", "8", &kueue.ClusterQueuePreemption{
					ReclaimWithinCohort: kueue.PreemptionPolicyAny,
					BorrowWithinCohort:  &kueue.BorrowWithinCohort{Policy: kueue.BorrowWithinCohortPolicyLowerPriority},
				}),
				utiltestingapi.MakeClusterQueue("A2").Cohort("coh2").
					ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "5", "20").Obj()).Obj(),
			},
			victims:    []*kueue.Workload{partialPreemptionVictimInCQ("A2", "a2", 0, 10, 5)},
			incoming:   utiltestingapi.MakeWorkload("p2", "").UID("p2").Priority(10).Request(corev1.ResourceCPU, "11").Obj(),
			incomingCQ: "P2",
			want:       map[string]*int32{},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.PartialPreemption, !tc.gateDisabled)
			fixture := newPartialPreemptionFixture(t, time.Now(), partialPreemptionFixtureCfg{
				cqs:        tc.cqs,
				victims:    tc.victims,
				incoming:   tc.incoming,
				incomingCQ: tc.incomingCQ,
			})
			targets := fixture.targets()

			got := make(map[string]*int32, len(targets))
			for _, target := range targets {
				var retained *int32
				if target.PartialPreemption != nil {
					retained = &target.PartialPreemption.RetainedCount
				}
				got[target.WorkloadInfo.Obj.Name] = retained
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("Unexpected targets, a nil value is a whole eviction (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestPartialPreemptionWithPreemptionConfig(t *testing.T) {
	const configName = "config"
	config := utiltestingalpha.MakePreemptionConfig(configName).
		Rule("within-cq", kueuealpha.Always, utiltestingalpha.MakeCandidateSelector(kueuealpha.WithinClusterQueue).Obj()).
		Obj()
	cq := utiltestingapi.MakeClusterQueue("a").Cohort("all").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).
		Preemption(kueue.ClusterQueuePreemption{WithinClusterQueue: kueue.PreemptionPolicyLowerPriority}).
		Annotation(kueuealpha.PreemptionConfigNameAnnotation, configName).
		Obj()
	cases := map[string]struct {
		victims     []*kueue.Workload
		incomingCPU string
		want        map[string]*int32
	}{
		"a victim selected by the rule is evicted, not shrunk": {
			victims: []*kueue.Workload{
				partialPreemptionVictimInCQ("a", "low", 0, 5, 1),
				partialPreemptionVictimInCQ("a", "rule", 5, 5, 1),
			},
			incomingCPU: "6",
			want:        map[string]*int32{"low": new(int32(4)), "rule": nil},
		},
		"the victim of the classical search can still shrink": {
			victims: []*kueue.Workload{
				partialPreemptionVictimInCQ("a", "low", 0, 5, 1),
				partialPreemptionVictimInCQ("a", "rule", 5, 5, 1),
			},
			incomingCPU: "8",
			want:        map[string]*int32{"low": new(int32(2)), "rule": nil},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.PartialPreemption, true)
			features.SetFeatureGateDuringTest(t, features.ConfigurablePreemptions, true)
			fixture := newPartialPreemptionFixture(t, time.Now(), partialPreemptionFixtureCfg{
				cqs:        []*kueue.ClusterQueue{cq},
				configs:    []*kueuealpha.PreemptionConfig{config},
				victims:    tc.victims,
				incoming:   utiltestingapi.MakeWorkload("in", "").Priority(5).Request(corev1.ResourceCPU, tc.incomingCPU).Obj(),
				incomingCQ: "a",
			})
			targets := fixture.targets()

			got := make(map[string]*int32, len(targets))
			for _, target := range targets {
				var retained *int32
				if target.PartialPreemption != nil {
					retained = &target.PartialPreemption.RetainedCount
				}
				got[target.WorkloadInfo.Obj.Name] = retained
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("Unexpected targets, a nil value is a whole eviction (-want,+got):\n%s", diff)
			}
			if diff := cmp.Diff(fixture.beforeSnapshot, fixture.snapshot, snapCmpOpts); diff != "" {
				t.Errorf("Snapshot was modified (-initial,+end):\n%s", diff)
			}
		})
	}
}

// A victim can be shrunk by the classical search and then whole-evicted by a
// PreemptionConfig rule within the same attempt. Restoring the snapshot must put
// the original instance back, not the shrunk one the rule selected.
func TestPartialPreemptionWithConfigurablePreemptions(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.PartialPreemption, true)
	features.SetFeatureGateDuringTest(t, features.ConfigurablePreemptions, true)
	config := utiltestingalpha.MakePreemptionConfig("config").
		Rule("all", kueuealpha.Always, utiltestingalpha.MakeCandidateSelector(kueuealpha.WithinParentCohort).Obj()).
		Obj()
	cqs := []*kueue.ClusterQueue{
		utiltestingapi.MakeClusterQueue("a").Cohort("all").
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "5").Obj()).
			Preemption(kueue.ClusterQueuePreemption{
				ReclaimWithinCohort: kueue.PreemptionPolicyAny,
				WithinClusterQueue:  kueue.PreemptionPolicyLowerPriority,
			}).
			Annotation(kueuealpha.PreemptionConfigNameAnnotation, config.Name).
			Obj(),
		utiltestingapi.MakeClusterQueue("b").Cohort("all").
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "1").Obj()).
			Obj(),
		utiltestingapi.MakeClusterQueue("c").Cohort("all").
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "0").Obj()).
			Obj(),
	}
	victims := []*kueue.Workload{
		partialPreemptionVictimInCQ("b", "w0", 0, 2, 0),
		partialPreemptionVictimInCQ("c", "w1", 0, 3, 0),
	}
	incoming := utiltestingapi.MakeWorkload("in", "").Request(corev1.ResourceCPU, "6").Obj()

	fixture := newPartialPreemptionFixture(t, time.Now(), partialPreemptionFixtureCfg{
		cqs:        cqs,
		configs:    []*kueuealpha.PreemptionConfig{config},
		victims:    victims,
		incoming:   incoming,
		incomingCQ: "a",
	})
	targets := fixture.targets()

	got := make(map[string]*int32, len(targets))
	for _, target := range targets {
		var retained *int32
		if target.PartialPreemption != nil {
			retained = &target.PartialPreemption.RetainedCount
		}
		got[target.WorkloadInfo.Obj.Name] = retained
		if target.Reason != kueue.ConfigurablePreemptionReason {
			t.Errorf("Target %q has reason %q, want %q", target.WorkloadInfo.Obj.Name, target.Reason, kueue.ConfigurablePreemptionReason)
		}
	}
	if diff := cmp.Diff(map[string]*int32{"w0": nil, "w1": nil}, got); diff != "" {
		t.Errorf("Unexpected targets, a nil value is a whole eviction (-want,+got):\n%s", diff)
	}
	if diff := cmp.Diff(fixture.beforeSnapshot, fixture.snapshot, snapCmpOpts); diff != "" {
		t.Errorf("Snapshot was modified (-initial,+end):\n%s", diff)
	}
}

func TestPlanPreemption(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.PartialPreemption, true)
	_, log := utiltesting.ContextWithLog(t)

	nonElastic := utiltestingapi.MakeWorkload("c", "").
		PodSets(*utiltestingapi.MakePodSet(victimPodSet, 5).
			Request(corev1.ResourceCPU, "1").SetPartialPreemptionMinRetainedCount(1).Obj()).
		Obj()
	evicted := partialPreemptionVictim("evicted", 0, 5, 1, nil)
	workloadevict.SetEvictedCondition(evicted, time.Now(), kueue.WorkloadEvictedByPreemption, "test")

	cases := map[string]struct {
		candidate      *kueue.Workload
		currentAtFloor bool
		wantTarget     bool
	}{
		"non-elastic candidate is not shrinkable": {
			candidate: nonElastic,
		},
		"evicted candidate is not shrinkable": {
			candidate: evicted,
		},
		"candidate at its floor is not shrinkable": {
			candidate: partialPreemptionVictim("at-floor", 0, 5, 5, nil),
		},
		"live instance already at the floor is not shrunk again": {
			candidate:      partialPreemptionVictim("shrunk", 0, 5, 1, nil),
			currentAtFloor: true,
		},
		"shrinks the candidate to its floor": {
			candidate:  partialPreemptionVictim("shrinkable", 0, 5, 1, nil),
			wantTarget: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			candidate := workload.NewInfo(log, tc.candidate)
			current := candidate
			if tc.currentAtFloor {
				floor, _ := workload.PreemptionMinRetainedCount(tc.candidate, victimPodSet)
				current = reducedInfoForPartialPreemption(candidate, victimPodSet, floor)
			}

			partial := planPreemption(candidate, current)
			if !tc.wantTarget {
				if partial != nil {
					t.Fatalf("planPreemption() = %+v, want nil", partial)
				}
				return
			}
			if partial == nil {
				t.Fatal("planPreemption() = nil, want a shrink to the floor")
			}
			wantFloor, _ := workload.PreemptionMinRetainedCount(tc.candidate, victimPodSet)
			if got := partial.PodSet; got != victimPodSet {
				t.Errorf("PodSet = %q, want %q", got, victimPodSet)
			}
			if got := partial.RetainedCount; got != wantFloor {
				t.Errorf("RetainedCount = %d, want %d", got, wantFloor)
			}
			if got := podSetRequestCount(partial.ReducedInfo, victimPodSet); got != wantFloor {
				t.Errorf("ReducedInfo executor count = %d, want %d", got, wantFloor)
			}
		})
	}
}

func TestFillbackPartialPreemption(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.PartialPreemption, true)
	const victimName = "victim"

	cases := map[string]struct {
		quota        string
		incomingCPU  string
		victim       *kueue.Workload
		wholeEvicted bool
		wantDropped  bool
		wantRetained *int32
		wantSnapshot *int32
	}{
		"fully restores and drops the target when the preemptor fits": {
			quota: "6", incomingCPU: "1",
			victim:       partialPreemptionVictim(victimName, 0, 5, 1, nil),
			wantDropped:  true,
			wantSnapshot: new(int32(5)),
		},
		"grows back to the largest count that fits": {
			quota: "6", incomingCPU: "4",
			victim:       partialPreemptionVictim(victimName, 0, 5, 1, nil),
			wantRetained: new(int32(2)),
			wantSnapshot: new(int32(2)),
		},
		"never grows above the in-flight retained count": {
			quota: "6", incomingCPU: "4",
			victim:       partialPreemptionVictim(victimName, 0, 5, 1, new(int32(1))),
			wantRetained: new(int32(1)),
			wantSnapshot: new(int32(1)),
		},
		"settles at the floor when only the floor fits": {
			quota: "6", incomingCPU: "5",
			victim:       partialPreemptionVictim(victimName, 0, 5, 1, nil),
			wantRetained: new(int32(1)),
			wantSnapshot: new(int32(1)),
		},
		"whole-evicted victim stays evicted when even the floor does not fit": {
			quota: "6", incomingCPU: "6",
			victim:       partialPreemptionVictim(victimName, 0, 5, 1, nil),
			wholeEvicted: true,
			wantRetained: nil,
			wantSnapshot: nil,
		},
		"shrunk victim settles at the floor when even the floor does not fit": {
			quota: "6", incomingCPU: "6",
			victim:       partialPreemptionVictim(victimName, 0, 5, 1, nil),
			wantRetained: new(int32(1)),
			wantSnapshot: new(int32(1)),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, pCtx := newPlanPreemptionCtx(t, tc.quota, []*kueue.Workload{tc.victim}, tc.incomingCPU)
			info := pCtx.preemptorCQ.Workloads[workload.Key(tc.victim)]
			if info == nil {
				t.Fatal("victim missing from the snapshot")
			}
			target := &Target{WorkloadInfo: info, WorkloadCq: pCtx.preemptorCQ}

			floor, _ := workload.PreemptionMinRetainedCount(tc.victim, victimPodSet)
			pCtx.snapshot.RemoveWorkload(info)
			if !tc.wholeEvicted {
				pCtx.snapshot.AddWorkload(reducedInfoForPartialPreemption(info, victimPodSet, floor))
				target.PartialPreemption = &policy.PartialPreemptionTarget{
					PodSet:        victimPodSet,
					RetainedCount: floor,
					ReducedInfo:   reducedInfoForPartialPreemption(info, victimPodSet, floor),
				}
			}

			dropped := fillbackPartialPreemption(ctx, pCtx, target, victimPodSet, false)
			if dropped != tc.wantDropped {
				t.Errorf("fillbackPartialPreemption() dropped = %t, want %t", dropped, tc.wantDropped)
			}

			gotCount, present := snapshotExecutorCount(pCtx, tc.victim)
			switch {
			case tc.wantSnapshot == nil:
				if present {
					t.Errorf("victim present in the snapshot with count %d, want absent", gotCount)
				}
			case !present:
				t.Errorf("victim absent from the snapshot, want count %d", *tc.wantSnapshot)
			case gotCount != *tc.wantSnapshot:
				t.Errorf("victim snapshot count = %d, want %d", gotCount, *tc.wantSnapshot)
			}

			if tc.wantDropped {
				return
			}
			if tc.wantRetained == nil {
				if target.PartialPreemption != nil {
					t.Errorf("target.PartialPreemption = %+v, want a whole eviction (nil)", target.PartialPreemption)
				}
				return
			}
			if target.PartialPreemption == nil {
				t.Fatal("target.PartialPreemption = nil, want a shrink")
			}
			if got := target.PartialPreemption.RetainedCount; got != *tc.wantRetained {
				t.Errorf("RetainedCount = %d, want %d", got, *tc.wantRetained)
			}
			if got := podSetRequestCount(target.PartialPreemption.ReducedInfo, victimPodSet); got != *tc.wantRetained {
				t.Errorf("ReducedInfo executor count = %d, want %d", got, *tc.wantRetained)
			}
		})
	}
}

func TestMergeStagedTargets(t *testing.T) {
	_, log := utiltesting.ContextWithLog(t)
	mk := func(name string) *Target {
		return &Target{WorkloadInfo: workload.NewInfo(log, utiltestingapi.MakeWorkload(name, "").Obj())}
	}
	partial := mk("v")
	partial.PartialPreemption = &policy.PartialPreemptionTarget{PodSet: victimPodSet, RetainedCount: 1}
	whole := mk("v")
	w := mk("w")

	cases := map[string]struct {
		targets []*Target
		want    []*Target
	}{
		"keeps the partial stage when it is yielded first": {
			targets: []*Target{partial, whole, w},
			want:    []*Target{partial, w},
		},
		"keeps the first stage regardless of the yield order": {
			targets: []*Target{w, whole, partial},
			want:    []*Target{whole, w},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := mergeStagedTargets(tc.targets, byName)
			if len(got) != len(tc.want) {
				t.Fatalf("mergeStagedTargets() returned %d targets, want %d", len(got), len(tc.want))
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Errorf("merged[%d] = %q, want the %q stage",
						i, got[i].WorkloadInfo.Obj.Name, tc.want[i].WorkloadInfo.Obj.Name)
				}
			}
		})
	}
}

func TestFillBackPartial(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.PartialPreemption, true)

	v := partialPreemptionVictim("v", 0, 5, 1, nil)
	n := partialPreemptionVictim("n", 0, 5, 5, nil)

	cases := map[string]struct {
		quota        string
		incomingCPU  string
		victims      []*kueue.Workload
		makeTargets  func(origByName map[string]*workload.Info, cq *schdcache.ClusterQueueSnapshot) []*Target
		wantRetained map[string]*int32
		wantSnapshot map[string]*int32
	}{
		"merges a victim's stages and backfills it over the partial range": {
			quota: "9", incomingCPU: "5",
			victims: []*kueue.Workload{v, n},
			makeTargets: func(origByName map[string]*workload.Info, cq *schdcache.ClusterQueueSnapshot) []*Target {
				vInfo := origByName["v"]
				floor, _ := workload.PreemptionMinRetainedCount(v, victimPodSet)
				return []*Target{
					{
						WorkloadInfo: vInfo, Reason: "shrink", WorkloadCq: cq,
						PartialPreemption: &policy.PartialPreemptionTarget{
							PodSet: victimPodSet, RetainedCount: floor,
							ReducedInfo: reducedInfoForPartialPreemption(vInfo, victimPodSet, floor),
						},
					},
					{WorkloadInfo: vInfo, Reason: "whole", WorkloadCq: cq},
					{WorkloadInfo: origByName["n"], Reason: "whole", WorkloadCq: cq},
				}
			},
			wantRetained: map[string]*int32{"v": new(int32(4)), "n": nil},
			wantSnapshot: map[string]*int32{"v": new(int32(4)), "n": nil},
		},
		"drops every target when the preemptor no longer needs the quota": {
			quota: "20", incomingCPU: "5",
			victims: []*kueue.Workload{v, n},
			makeTargets: func(origByName map[string]*workload.Info, cq *schdcache.ClusterQueueSnapshot) []*Target {
				vInfo := origByName["v"]
				floor, _ := workload.PreemptionMinRetainedCount(v, victimPodSet)
				return []*Target{
					{
						WorkloadInfo: vInfo, Reason: "shrink", WorkloadCq: cq,
						PartialPreemption: &policy.PartialPreemptionTarget{
							PodSet: victimPodSet, RetainedCount: floor,
							ReducedInfo: reducedInfoForPartialPreemption(vInfo, victimPodSet, floor),
						},
					},
					{WorkloadInfo: vInfo, Reason: "whole", WorkloadCq: cq},
					{WorkloadInfo: origByName["n"], Reason: "whole", WorkloadCq: cq},
				}
			},
			wantRetained: map[string]*int32{},
			wantSnapshot: map[string]*int32{"v": new(int32(5)), "n": new(int32(5))},
		},
		"routes a whole-eviction stage of a shrinkable victim through the partial search": {
			quota: "9", incomingCPU: "5",
			victims: []*kueue.Workload{v},
			makeTargets: func(origByName map[string]*workload.Info, cq *schdcache.ClusterQueueSnapshot) []*Target {
				return []*Target{
					{WorkloadInfo: origByName["v"], Reason: "whole", WorkloadCq: cq},
				}
			},
			wantRetained: map[string]*int32{"v": new(int32(4))},
			wantSnapshot: map[string]*int32{"v": new(int32(4))},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, pCtx := newPlanPreemptionCtx(t, tc.quota, tc.victims, tc.incomingCPU)
			cq := pCtx.preemptorCQ

			origByName := make(map[string]*workload.Info, len(tc.victims))
			for _, wl := range tc.victims {
				info := cq.Workloads[workload.Key(wl)]
				if info == nil {
					t.Fatalf("workload %q missing from the snapshot", wl.Name)
				}
				origByName[wl.Name] = info
			}
			for _, info := range origByName {
				pCtx.snapshot.RemoveWorkload(info)
			}

			targets := tc.makeTargets(origByName, cq)

			got := fillBackPartialWorkloads(ctx, pCtx, targets, false, byName)

			if len(got) != len(tc.wantRetained) {
				t.Fatalf("fillBackPartialWorkloads() returned %d targets, want %d: %+v", len(got), len(tc.wantRetained), got)
			}
			gotByName := make(map[string]*Target, len(got))
			for _, target := range got {
				gotByName[target.WorkloadInfo.Obj.Name] = target
			}
			for name, wantRetained := range tc.wantRetained {
				target, ok := gotByName[name]
				if !ok {
					t.Errorf("victim %q missing from the fillBack result", name)
					continue
				}
				if wantRetained == nil {
					if target.PartialPreemption != nil {
						t.Errorf("victim %q: PartialPreemption = %+v, want a whole eviction (nil)", name, target.PartialPreemption)
					}
					continue
				}
				if target.PartialPreemption == nil {
					t.Errorf("victim %q: PartialPreemption = nil, want a shrink to %d", name, *wantRetained)
					continue
				}
				if got := target.PartialPreemption.RetainedCount; got != *wantRetained {
					t.Errorf("victim %q: RetainedCount = %d, want %d", name, got, *wantRetained)
				}
			}
			for name, wantCount := range tc.wantSnapshot {
				gotCount, present := snapshotExecutorCount(pCtx, origByName[name].Obj)
				if wantCount == nil {
					if present {
						t.Errorf("victim %q present in the snapshot with count %d, want absent", name, gotCount)
					}
					continue
				}
				if !present {
					t.Errorf("victim %q absent from the snapshot, want count %d", name, *wantCount)
				} else if gotCount != *wantCount {
					t.Errorf("victim %q snapshot count = %d, want %d", name, gotCount, *wantCount)
				}
			}
		})
	}
}

func TestPartialPreemptionIssuePreemptions(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.PartialPreemption, true)

	type victimWant struct {
		partial     bool
		targetCount int32
	}
	cases := map[string]struct {
		quota         string
		victims       []*kueue.Workload
		incoming      *kueue.Workload
		wantPreempted int
		wantFailed    int
		want          map[string]victimWant
	}{
		"partial scale-down keeps the victim running": {
			quota:         "6",
			victims:       []*kueue.Workload{partialPreemptionVictim("victim", 0, 5, 1, nil)},
			incoming:      utiltestingapi.MakeWorkload("in", "").UID("wl-in").Priority(1).Request(corev1.ResourceCPU, "4").Obj(),
			wantPreempted: 1,
			want:          map[string]victimWant{"victim": {partial: true, targetCount: 2}},
		},
		"non-shrinkable victim is wholly evicted": {
			quota:         "6",
			victims:       []*kueue.Workload{partialPreemptionVictim("victim", 0, 5, 5, nil)},
			incoming:      utiltestingapi.MakeWorkload("in", "").UID("wl-in").Priority(1).Request(corev1.ResourceCPU, "4").Obj(),
			wantPreempted: 1,
			want:          map[string]victimWant{"victim": {partial: false}},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			now := time.Now().Truncate(time.Second)
			items := make([]kueue.Workload, len(tc.victims))
			for i := range tc.victims {
				items[i] = *tc.victims[i]
			}
			ctx, log := utiltesting.ContextWithLog(t)
			cl := utiltesting.NewClientBuilder().
				WithLists(&kueue.WorkloadList{Items: items}).
				WithStatusSubresource(&kueue.Workload{}).
				WithInterceptorFuncs(interceptor.Funcs{SubResourceApply: utiltesting.TreatSSAAsStrategicMergeForApplyConfiguration}).
				Build()
			cqCache := schdcache.New(cl)
			cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
			if err := cqCache.AddClusterQueue(ctx, partialPreemptionClusterQueue(tc.quota)); err != nil {
				t.Fatalf("Couldn't add ClusterQueue to cache: %v", err)
			}
			recorder := &utiltesting.EventRecorder{}
			preemptor := New(cl, workload.Ordering{}, recorder, nil, false, clocktesting.NewFakeClock(now), nil, preemptexpectations.New(), nil)
			snapshot, err := cqCache.Snapshot(ctx)
			if err != nil {
				t.Fatalf("unexpected error while building snapshot: %v", err)
			}
			wlInfo := workload.NewInfo(log, tc.incoming)
			wlInfo.ClusterQueue = "standalone"
			assignment := singlePodSetAssignment(flavorassigner.ResourceAssignment{
				corev1.ResourceCPU: &flavorassigner.FlavorAssignment{Name: "default", Mode: flavorassigner.Preempt},
			})
			strategies := preemptor.GetPreemptionStrategyIterator(ctx, *wlInfo, snapshot, assignment)
			targets := preemptor.GetTargetsWithStrategy(ctx, strategies)

			preempted, failed, err := preemptor.IssuePreemptions(ctx, cqCache, wlInfo, targets, snapshot.ClusterQueue(wlInfo.ClusterQueue))
			if err != nil {
				t.Fatalf("IssuePreemptions failed: %v", err)
			}
			if preempted != tc.wantPreempted || failed != tc.wantFailed {
				t.Errorf("IssuePreemptions = (preempted=%d, failed=%d), want (%d, %d)", preempted, failed, tc.wantPreempted, tc.wantFailed)
			}

			for victimName, want := range tc.want {
				got := &kueue.Workload{}
				if err := cl.Get(ctx, types.NamespacedName{Name: victimName}, got); err != nil {
					t.Fatalf("failed to get victim %q: %v", victimName, err)
				}
				if !want.partial {
					if !workloadevict.IsEvicted(got) {
						t.Errorf("victim %q was not evicted, want whole eviction", victimName)
					}
					continue
				}
				gotCount, ok := admittedTargetCount(got, "executor")
				if !ok || gotCount != want.targetCount {
					t.Errorf("victim %q executor targetCount = (%d, %t), want (%d, true)", victimName, gotCount, ok, want.targetCount)
				}
				if c := admittedCount(got, "executor"); c != 5 {
					t.Errorf("victim %q executor admitted count = %d, want 5 (unchanged; released as pods exit)", victimName, c)
				}
				if workloadevict.IsEvicted(got) {
					t.Errorf("victim %q was evicted, want it running (partial preemption must not evict)", victimName)
				}
			}
		})
	}
}
