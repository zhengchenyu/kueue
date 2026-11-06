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

package elasticjobs

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	autoscaling "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	configapi "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/controller/core"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/metrics"
	"sigs.k8s.io/kueue/pkg/resources"
	utilclient "sigs.k8s.io/kueue/pkg/util/client"
	"sigs.k8s.io/kueue/pkg/util/expectations"
	utilmaps "sigs.k8s.io/kueue/pkg/util/maps"
	"sigs.k8s.io/kueue/pkg/util/parallelize"
	utilpod "sigs.k8s.io/kueue/pkg/util/pod"
	"sigs.k8s.io/kueue/pkg/util/roletracker"
	utilslices "sigs.k8s.io/kueue/pkg/util/slices"
	utiltasranks "sigs.k8s.io/kueue/pkg/util/tas/ranks"
	"sigs.k8s.io/kueue/pkg/workload"
	workloadevict "sigs.k8s.io/kueue/pkg/workload/evict"
	workloadfinish "sigs.k8s.io/kueue/pkg/workload/finish"
	"sigs.k8s.io/kueue/pkg/workload/patching"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

const ControllerName = "ElasticJobUngater"

var errPendingUngateOps = errors.New("pending elastic ungate operations")

type elasticJobUngater struct {
	client            client.Client
	clock             clock.Clock
	expectationsStore *expectations.Store
	roleTracker       *roletracker.RoleTracker
	customLabels      *metrics.CustomLabels
}

var _ reconcile.Reconciler = (*elasticJobUngater)(nil)
var _ predicate.TypedPredicate[*kueue.Workload] = (*elasticJobUngater)(nil)

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads,verbs=get;list;watch

func SetupWithManager(mgr ctrl.Manager, cfg *configapi.Configuration, roleTracker *roletracker.RoleTracker, customLabels *metrics.CustomLabels) (string, error) {
	r := &elasticJobUngater{
		client:            mgr.GetClient(),
		clock:             clock.RealClock{},
		expectationsStore: expectations.NewStore(ControllerName),
		roleTracker:       roleTracker,
		customLabels:      customLabels,
	}
	podHandler := elasticPodHandler{
		client:            r.client,
		expectationsStore: r.expectationsStore,
	}
	// Enqueue the active slice; Reconcile resolves it again in case of a rollover.
	sliceKeyHandler := handler.TypedEnqueueRequestsFromMapFunc(
		func(ctx context.Context, wl *kueue.Workload) []reconcile.Request {
			active, err := workloadslicing.FindLatestAdmittedWorkload(ctx, r.client, wl, false)
			if err != nil || active == nil {
				return nil
			}
			return []reconcile.Request{{
				Namespace: active.Namespace,
				Name:      active.Name}}
		},
	)
	return ControllerName, builder.TypedControllerManagedBy[reconcile.Request](mgr).
		Named("elastic_job_ungater").
		WatchesRawSource(source.TypedKind(
			mgr.GetCache(),
			&kueue.Workload{},
			sliceKeyHandler,
			r,
		)).
		Watches(&corev1.Pod{}, &podHandler).
		WithOptions(controller.Options{
			NeedLeaderElection:      new(false),
			MaxConcurrentReconciles: mgr.GetControllerOptions().GroupKindConcurrency[kueue.SchemeGroupVersion.WithKind("Workload").GroupKind().String()],
		}).
		WithLogConstructor(roletracker.NewLogConstructor(r.roleTracker, ControllerName)).
		Complete(core.WithLeadingManager(mgr, r, &kueue.Workload{}, cfg))
}

func (r *elasticJobUngater) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.V(2).Info("Reconcile ElasticJobUngater")

	active, err := workloadslicing.FindActiveWorkload(ctx, r.client, req.NamespacedName, false)
	if err != nil || active == nil {
		return reconcile.Result{}, err
	}
	if !shouldUngate(active) || workloadevict.IsEvicted(active) {
		return reconcile.Result{}, nil
	}

	// Shrink any PodSet that a partial-preemption reclaim target has lowered below
	// its running count, evicting the excess Pods. This is independent of ungating,
	// so it runs before the pending-ungate-ops short-circuit below.
	if features.Enabled(features.PartialPreemption) && workload.HasPendingPreemptionTargetCount(active) {
		if err := r.shrinkToTargets(ctx, active); err != nil {
			return reconcile.Result{}, err
		}
	}

	// Expectations are keyed by the stable chain key (the origin slice name shared
	// by every slice and pod in the chain), not by the rolling active-slice name,
	// so in-flight ungate expectations survive a scale rollover.
	sliceKey := types.NamespacedName{Namespace: active.Namespace, Name: workloadslicing.SliceName(active)}
	if !r.expectationsStore.Satisfied(log, sliceKey) {
		return reconcile.Result{}, errPendingUngateOps
	}

	pods, err := r.podsToUngate(ctx, active)
	if err != nil {
		return reconcile.Result{}, err
	}
	if len(pods) == 0 {
		return reconcile.Result{}, nil
	}

	log.V(2).Info("identified elastic pods to ungate", "count", len(pods))
	uids := make([]types.UID, len(pods))
	for i := range pods {
		uids[i] = pods[i].UID
	}
	r.expectationsStore.ExpectUIDs(log, sliceKey, uids)

	err = parallelize.Until(ctx, len(pods), func(i int) error {
		pod := pods[i]
		var ungated bool
		e := utilclient.Patch(ctx, r.client, pod, func() (bool, error) {
			podSetName := kueue.PodSetReference(pod.Labels[constants.PodSetLabel])
			update, err := admissionUpdateForPodSet(active, podSetName)
			if err != nil {
				return false, err
			}
			changed, err := refreshPodAdmission(pod, update)
			if err != nil {
				return false, err
			}
			ungated = utilpod.Ungate(pod, kueue.ElasticJobSchedulingGate)
			if ungated {
				log.V(3).Info("ungating elastic pod", "pod", klog.KObj(pod))
			}
			return changed || ungated, nil
		})
		if e != nil {
			r.expectationsStore.ObservedUID(log, sliceKey, pod.UID)
			log.Error(e, "failed ungating elastic pod", "pod", klog.KObj(pod))
			return e
		}
		if !ungated {
			r.expectationsStore.ObservedUID(log, sliceKey, pod.UID)
		} else {
			utilpod.RecordPodSchedulingGateRemovalSeconds(r.clock, kueue.ElasticJobSchedulingGate, active, false, r.customLabels, r.roleTracker)
		}
		return nil
	})
	return reconcile.Result{}, err
}

type podAdmissionUpdate struct {
	annotations  map[string]string
	nodeSelector map[string]string
}

func admissionUpdateForPodSet(wl *kueue.Workload, podSetName kueue.PodSetReference) (podAdmissionUpdate, error) {
	update := podAdmissionUpdate{
		annotations:  make(map[string]string),
		nodeSelector: make(map[string]string),
	}
	if workloadslicing.IsEnabledForProvisioningRequests(wl) {
		update.annotations[kueue.WorkloadAnnotation] = wl.Name
		update.annotations[kueue.WorkloadSliceNameAnnotation] = workloadslicing.SliceName(wl)
	}
	for _, check := range wl.Status.AdmissionChecks {
		for _, psUpdate := range check.PodSetUpdates {
			if psUpdate.Name != podSetName {
				continue
			}
			// Stamp every admission annotation, not just consume-provisioning-request:
			// Pods created before the job template was stamped (the first
			// PRQ-backed slice in a chain) carry none of them, and a consume
			// annotation without its provisioning-class-name is ambiguous to the
			// autoscaler.
			for key, value := range psUpdate.Annotations {
				if old, exists := update.annotations[key]; exists && old != value {
					return podAdmissionUpdate{}, fmt.Errorf("conflicting %q annotation updates for PodSet %q", key, podSetName)
				}
				update.annotations[key] = value
			}
			for key, value := range psUpdate.NodeSelector {
				if old, exists := update.nodeSelector[key]; exists && old != value {
					return podAdmissionUpdate{}, fmt.Errorf("conflicting %q node selector updates for PodSet %q", key, podSetName)
				}
				update.nodeSelector[key] = value
			}
		}
	}
	return update, nil
}

// podAdmissionCompatible reports whether the Pod can still take update. Only the
// consume-provisioning-request annotation is checked among annotations: it is
// the Pod's immutable request identity, whereas the workload / slice-name
// annotations inherited from the template are meant to be overwritten with the
// active slice. A gated Pod may only gain nodeSelector keys (the API server
// rejects changing an existing one), so a conflicting value is incompatible.
func podAdmissionCompatible(pod *corev1.Pod, update podAdmissionUpdate) bool {
	if wanted, specified := update.annotations[autoscaling.ProvisioningRequestPodAnnotationKey]; specified {
		if existing, found := pod.Annotations[autoscaling.ProvisioningRequestPodAnnotationKey]; found && existing != wanted {
			return false
		}
	}
	for key, value := range update.nodeSelector {
		if existing, found := pod.Spec.NodeSelector[key]; found && existing != value {
			return false
		}
	}
	return true
}

func refreshPodAdmission(pod *corev1.Pod, update podAdmissionUpdate) (bool, error) {
	if !podAdmissionCompatible(pod, update) {
		return false, fmt.Errorf("pod %s/%s has immutable admission metadata from a different ProvisioningRequest", pod.Namespace, pod.Name)
	}
	changed := !utilmaps.Contains(pod.Annotations, update.annotations) ||
		!utilmaps.Contains(pod.Spec.NodeSelector, update.nodeSelector)
	if changed {
		utilmaps.Copy(&pod.Annotations, update.annotations)
		utilmaps.Copy(&pod.Spec.NodeSelector, update.nodeSelector)
	}
	return changed, nil
}

func (r *elasticJobUngater) podsToUngate(ctx context.Context, wl *kueue.Workload) ([]*corev1.Pod, error) {
	// All pods in the slice chain share the same WorkloadSliceNameAnnotation,
	// so the index lookup returns every pod created on behalf of this job.
	// Although those pods may still carry an older slice's name in their
	// WorkloadAnnotation (the template is stamped at the slice's admission),
	// wl is the chain's active slice (resolved in Reconcile), so its granted
	// PodSet counts are the right cap for ungating any of them.
	sliceName := workloadslicing.SliceName(wl)
	pods, err := workloadslicing.ListPodsForWorkloadSlice(ctx, r.client, wl.Namespace, sliceName)
	if err != nil {
		return nil, fmt.Errorf("listing pods for workload slice: %w", err)
	}

	granted := workload.ExtractGrantedPodSetCounts(wl)
	gatedPerPodSet := make(map[kueue.PodSetReference][]*corev1.Pod)
	ungatedPerPodSet := make(map[kueue.PodSetReference]int32)
	admissionUpdates := make(map[kueue.PodSetReference]podAdmissionUpdate)
	for _, p := range pods {
		if utilpod.IsTerminated(p) {
			continue
		}
		ps := kueue.PodSetReference(p.Labels[constants.PodSetLabel])
		if utilpod.HasGate(p, kueue.ElasticJobSchedulingGate) {
			update, found := admissionUpdates[ps]
			if !found {
				var err error
				update, err = admissionUpdateForPodSet(wl, ps)
				if err != nil {
					return nil, err
				}
				admissionUpdates[ps] = update
			}
			if !podAdmissionCompatible(p, update) {
				ctrl.LoggerFrom(ctx).Info("leaving elastic pod gated because immutable admission metadata is stale; recycle the pod after its template refreshes",
					"pod", klog.KObj(p), "podSet", ps)
				continue
			}
			gatedPerPodSet[ps] = append(gatedPerPodSet[ps], p)
		} else {
			// Already-ungated pods consume quota too.
			ungatedPerPodSet[ps]++
		}
	}

	log := ctrl.LoggerFrom(ctx)
	var gated []*corev1.Pod
	for ps, candidates := range gatedPerPodSet {
		// While a partial-preemption reclaim target is in flight, cap ungating at
		// the target instead of the granted count. Pods the job's controller
		// recreates above the target stay gated, so the PodSet effectively shrinks
		// to the target until its quota is released.
		ungateCap := granted[ps]
		if target, ok := workload.PreemptionTargetCount(wl, ps); ok && target < ungateCap {
			ungateCap = target
		}
		room := ungateCap - ungatedPerPodSet[ps]
		var toUngate []*corev1.Pod
		if room > 0 {
			toUngate = sortCandidates(log, candidates, wl, ps)
			if int32(len(candidates)) > room {
				toUngate = toUngate[:room]
			}
		}
		log.V(4).Info("elastic ungating quota accounting for PodSet",
			"podSet", ps,
			"grantedCount", granted[ps],
			"ungateCap", ungateCap,
			"alreadyUngatedCount", ungatedPerPodSet[ps],
			"gatedCount", len(candidates),
			"ungatingCount", len(toUngate),
		)
		gated = append(gated, toUngate...)
	}
	return gated, nil
}

// shrinkToTargets evicts the running Pods that a partial-preemption reclaim
// target has shrunk a PodSet below. The job's own controller recreates the
// evicted Pods, but podsToUngate caps ungating at the target, so each PodSet
// settles at its target until the quota is released. Eviction goes through the
// Eviction API so PodDisruptionBudgets are honored; a refused eviction is
// retried on the next reconcile rather than force-deleted. The pass is
// level-based: it recomputes the excess from the live Pod set every time, so it
// is safe to repeat.
func (r *elasticJobUngater) shrinkToTargets(ctx context.Context, wl *kueue.Workload) error {
	sliceName := workloadslicing.SliceName(wl)
	pods, err := workloadslicing.ListPodsForWorkloadSlice(ctx, r.client, wl.Namespace, sliceName)
	if err != nil {
		return fmt.Errorf("listing pods for workload slice: %w", err)
	}

	// Per PodSet, collect two views of the ungated, non-terminal Pods (gated Pods
	// hold no quota, terminal Pods none either):
	//   - evictable: those not already terminating, the Pods we may evict to reach
	//     the target (a Pod on its way out must not be re-evicted).
	//   - ungatedNonTerminal: the count including terminating Pods, which still hold
	//     quota until they are gone. The quota is only reported released once this
	//     count drops to the target (see releaseDrainedTargets).
	evictablePerPodSet := make(map[kueue.PodSetReference][]*corev1.Pod)
	ungatedNonTerminalPerPodSet := make(map[kueue.PodSetReference]int32)
	for _, p := range pods {
		if utilpod.IsTerminated(p) {
			continue
		}
		if utilpod.HasGate(p, kueue.ElasticJobSchedulingGate) {
			continue
		}
		ps := kueue.PodSetReference(p.Labels[constants.PodSetLabel])
		ungatedNonTerminalPerPodSet[ps]++
		if p.DeletionTimestamp == nil {
			evictablePerPodSet[ps] = append(evictablePerPodSet[ps], p)
		}
	}

	log := ctrl.LoggerFrom(ctx)
	var toEvict []*corev1.Pod
	for ps, evictable := range evictablePerPodSet {
		target, ok := workload.PreemptionTargetCount(wl, ps)
		if !ok {
			continue
		}
		// Keep `target` Pods, evict the rest. Pods already terminating counted
		// toward neither side, so the target is measured against the Pods that will
		// remain, keeping the pass idempotent across reconciles.
		excess := int32(len(evictable)) - target
		if excess <= 0 {
			continue
		}
		ordered := sortEvictionCandidates(evictable)[:excess]
		log.V(3).Info("partial-preemption shrinking PodSet",
			"podSet", ps,
			"evictableCount", len(evictable),
			"targetCount", target,
			"evictingCount", len(ordered))
		toEvict = append(toEvict, ordered...)
	}
	if len(toEvict) > 0 {
		if err := parallelize.Until(ctx, len(toEvict), func(i int) error {
			return r.evictPod(ctx, log, toEvict[i])
		}); err != nil {
			return err
		}
	}
	return r.releaseDrainedTargets(ctx, wl, ungatedNonTerminalPerPodSet)
}

// releaseDrainedTargets releases the quota of a drained partial-preemption target.
// Once every PodSet carrying a target has drained down to it (the drain count
// includes terminating Pods, which still hold quota until they are actually gone),
// Kueue lowers the PodSet's admitted count and resourceUsage to the target, clears
// the target, and flips the PartiallyPreempted condition to False (reason
// QuotaReleased), all in one status update. Lowering the admitted count - rather
// than reporting reclaimablePods relative to the spec count - keeps the grant equal
// to the real size and never releases quota before the Pods are gone.
func (r *elasticJobUngater) releaseDrainedTargets(ctx context.Context, wl *kueue.Workload, ungatedNonTerminal map[kueue.PodSetReference]int32) error {
	if !apimeta.IsStatusConditionTrue(wl.Status.Conditions, kueue.WorkloadPartiallyPreempted) {
		return nil
	}
	for i := range wl.Status.Admission.PodSetAssignments {
		psa := &wl.Status.Admission.PodSetAssignments[i]
		if psa.PreemptionTargetCount == nil {
			continue
		}
		if ungatedNonTerminal[psa.Name] > *psa.PreemptionTargetCount {
			// Still draining: an over-target Pod (possibly terminating) holds quota.
			return nil
		}
	}
	log := ctrl.LoggerFrom(ctx)
	wlCopy := wl.DeepCopy()
	if err := patching.PatchAdmissionStatus(ctx, r.client, wlCopy, r.clock, func(wl *kueue.Workload) (bool, error) {
		// Recompute from scratch: the closure may run again on a patch conflict.
		updated := false
		for i := range wl.Status.Admission.PodSetAssignments {
			psa := &wl.Status.Admission.PodSetAssignments[i]
			if psa.PreemptionTargetCount == nil {
				continue
			}
			target := *psa.PreemptionTargetCount
			if oldCount := ptr.Deref(psa.Count, target); oldCount > target {
				// Scale the recorded resource usage down in proportion to the count,
				// matching how the quota accounting scales it (totalRequestsFromAdmission).
				req := resources.NewRequestsFromResourceList(psa.ResourceUsage)
				req.Divide(int64(oldCount))
				req.Mul(int64(target))
				psa.ResourceUsage = req.ToResourceList(resources.NewResourceFormatter())
				psa.Count = ptr.To(target)
			}
			psa.PreemptionTargetCount = nil
			updated = true
		}
		changed := workload.SetPartiallyPreemptedCondition(
			wl,
			r.clock.Now(),
			false,
			kueue.WorkloadPartiallyPreemptedReasonQuotaReleased,
			"Partial-preemption reclaim target satisfied; quota released",
		)
		return updated || changed, nil
	}, patching.WithLooseOnApply()); err != nil {
		return fmt.Errorf("releasing partial-preemption quota: %w", err)
	}
	log.V(3).Info("partial-preemption quota released", "workload", klog.KObj(wl))
	return nil
}

// sortEvictionCandidates orders Pods least-valuable first, matching the KEP:
// unscheduled Pods first, then the lowest pod-deletion-cost, then the newest.
func sortEvictionCandidates(pods []*corev1.Pod) []*corev1.Pod {
	ordered := slices.Clone(pods)
	slices.SortFunc(ordered, func(a, b *corev1.Pod) int {
		if aScheduled, bScheduled := utilpod.IsScheduled(a), utilpod.IsScheduled(b); aScheduled != bScheduled {
			if !aScheduled {
				return -1
			}
			return 1
		}
		if c := cmp.Compare(podDeletionCost(a), podDeletionCost(b)); c != 0 {
			return c
		}
		return b.CreationTimestamp.Time.Compare(a.CreationTimestamp.Time)
	})
	return ordered
}

// podDeletionCost reads the controller.kubernetes.io/pod-deletion-cost annotation,
// defaulting to 0 when it is absent or malformed (matching the Job controller).
func podDeletionCost(pod *corev1.Pod) int64 {
	if v, ok := pod.Annotations[corev1.PodDeletionCost]; ok {
		if cost, err := strconv.ParseInt(v, 10, 64); err == nil {
			return cost
		}
	}
	return 0
}

// evictPod requests eviction of a single Pod through the Eviction API. A refused
// eviction (a PodDisruptionBudget would be violated) is surfaced as an error so
// the reconcile is retried with backoff; an already-gone Pod is not an error.
func (r *elasticJobUngater) evictPod(ctx context.Context, log logr.Logger, pod *corev1.Pod) error {
	eviction := &policyv1.Eviction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
		},
	}
	if err := r.client.SubResource("eviction").Create(ctx, pod, eviction); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		if apierrors.IsTooManyRequests(err) {
			log.V(3).Info("partial-preemption eviction blocked by PodDisruptionBudget, will retry", "pod", klog.KObj(pod))
			return err
		}
		return fmt.Errorf("evicting pod %s: %w", klog.KObj(pod), err)
	}
	log.V(3).Info("evicted elastic pod above partial-preemption target", "pod", klog.KObj(pod))
	return nil
}

// sortCandidates orders by completion index, or by name if any Pod lacks a valid index.
func sortCandidates(log logr.Logger, candidates []*corev1.Pod, wl *kueue.Workload, ps kueue.PodSetReference) []*corev1.Pod {
	if rankToPod, ok := readRanksFromCandidatesIfAvailable(log, candidates, wl, ps); ok {
		return sortCandidatesByRanks(rankToPod)
	}
	return sortCandidatesByName(candidates)
}

func sortCandidatesByRanks(rankToPod map[int]*corev1.Pod) []*corev1.Pod {
	ranks := slices.Sorted(maps.Keys(rankToPod))
	return utilslices.Map(ranks, func(rank *int) *corev1.Pod { return rankToPod[*rank] })
}

func sortCandidatesByName(candidates []*corev1.Pod) []*corev1.Pod {
	slices.SortFunc(candidates, func(a, b *corev1.Pod) int { return strings.Compare(a.Name, b.Name) })
	return candidates
}

func readRanksFromCandidatesIfAvailable(log logr.Logger, candidates []*corev1.Pod, wl *kueue.Workload, ps kueue.PodSetReference) (map[int]*corev1.Pod, bool) {
	psNameToTopologyRequest := workload.PodSetNameToTopologyRequest(wl)
	psReq := psNameToTopologyRequest[ps]
	if psReq == nil || psReq.PodIndexLabel == nil {
		return nil, false
	}

	psas := podSetAssignmentsWithSpecCounts(wl)
	psaIdx := slices.IndexFunc(psas, func(psa kueue.PodSetAssignment) bool { return psa.Name == ps })
	if psaIdx < 0 {
		return nil, false
	}

	rankOffsets := utiltasranks.PodSetRankOffsets(psas, psNameToTopologyRequest)
	rankToPod, err := utiltasranks.ReadRanksForLabels(&psas[psaIdx], candidates, psReq, rankOffsets[ps])
	if err != nil {
		// A missing index label is expected, e.g. for a NonIndexed Job, whose PodSet
		// carries a PodIndexLabel but whose pods never have the label set.
		if errors.Is(err, utilpod.ErrLabelNotFound) {
			log.V(5).Info("pods missing index label for rank ordering", "error", err)
		} else {
			log.Error(err, "failed to read rank information from pods")
		}
		return nil, false
	}
	return rankToPod, true
}

// podSetAssignmentsWithSpecCounts returns copies of the assignments with Count set to the
// PodSet's spec count: pod indexes span the whole Job, not just the admitted part.
// It returns nil when the Workload has no admission.
func podSetAssignmentsWithSpecCounts(wl *kueue.Workload) []kueue.PodSetAssignment {
	if wl.Status.Admission == nil {
		return nil
	}
	specCounts := workload.ExtractPodSetCounts(wl.Spec.PodSets)
	psas := make([]kueue.PodSetAssignment, len(wl.Status.Admission.PodSetAssignments))
	for i, psa := range wl.Status.Admission.PodSetAssignments {
		psa.Count = new(specCounts[psa.Name])
		psas[i] = psa
	}
	return psas
}

// Workload predicates

func (r *elasticJobUngater) Create(e event.TypedCreateEvent[*kueue.Workload]) bool {
	return shouldUngate(e.Object)
}

func (r *elasticJobUngater) Update(e event.TypedUpdateEvent[*kueue.Workload]) bool {
	return shouldUngate(e.ObjectNew)
}

func shouldUngate(wl *kueue.Workload) bool {
	return workloadslicing.IsElasticWorkload(wl) &&
		!workloadfinish.IsFinished(wl) &&
		workload.IsAdmitted(wl)
}

func (r *elasticJobUngater) Delete(event.TypedDeleteEvent[*kueue.Workload]) bool {
	return false
}

func (r *elasticJobUngater) Generic(event.TypedGenericEvent[*kueue.Workload]) bool {
	return false
}

// Pod event handler

var _ handler.EventHandler = (*elasticPodHandler)(nil)

type elasticPodHandler struct {
	client            client.Client
	expectationsStore *expectations.Store
}

func (h *elasticPodHandler) Create(ctx context.Context, e event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	h.queueReconcileForPod(ctx, e.Object, false, q)
}

func (h *elasticPodHandler) Update(ctx context.Context, e event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	h.queueReconcileForPod(ctx, e.ObjectNew, false, q)
}

func (h *elasticPodHandler) Delete(ctx context.Context, e event.DeleteEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	h.queueReconcileForPod(ctx, e.Object, true, q)
}

func (h *elasticPodHandler) Generic(context.Context, event.GenericEvent, workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (h *elasticPodHandler) queueReconcileForPod(ctx context.Context, object client.Object, deleted bool, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	pod, isPod := object.(*corev1.Pod)
	if !isPod {
		return
	}
	// Expectations are keyed by the stable chain key (the origin slice name the
	// pod carries), so observations survive scale rollovers.
	sliceKey := workloadslicing.KeyForPod(pod)
	if sliceKey == nil {
		return
	}
	// Mark expectation as observed when the gate has been removed or the pod is deleted.
	if !utilpod.HasGate(pod, kueue.ElasticJobSchedulingGate) || deleted {
		log := ctrl.LoggerFrom(ctx).WithValues("pod", klog.KObj(pod), "workloadSlice", sliceKey.String())
		h.expectationsStore.ObservedUID(log, *sliceKey, pod.UID)
	}
	active, err := workloadslicing.FindLatestAdmittedWorkloadForSlice(ctx, h.client, sliceKey.Namespace, sliceKey.Name)
	if err != nil || active == nil {
		return
	}
	q.AddAfter(reconcile.Request{
		Namespace: active.Namespace,
		Name:      active.Name}, constants.UpdatesBatchPeriod)
}
