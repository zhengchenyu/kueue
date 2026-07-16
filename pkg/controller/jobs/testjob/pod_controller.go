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

package testjob

import (
	"context"
	"fmt"
	"maps"
	"slices"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	clientutil "sigs.k8s.io/kueue/pkg/util/client"
	utilpod "sigs.k8s.io/kueue/pkg/util/pod"
	"sigs.k8s.io/kueue/pkg/util/roletracker"
)

const podControllerName = "testjob_pod"

type PodReconciler struct {
	client      client.Client
	roleTracker *roletracker.RoleTracker
}

var _ jobframework.JobReconcilerInterface = (*PodReconciler)(nil)

func NewPodReconciler(_ context.Context, client client.Client, _ client.FieldIndexer, _ events.EventRecorder, opts ...jobframework.Option) (jobframework.JobReconcilerInterface, error) {
	options := jobframework.ProcessOptions(opts...)
	return &PodReconciler{
		client:      client,
		roleTracker: options.RoleTracker,
	}, nil
}

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=testjobs,verbs=get;list;watch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=testjobs/status,verbs=get;update;patch

func (r *PodReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kueuealpha.TestJob{}).
		Owns(&corev1.Pod{}).
		Named(podControllerName).
		WithOptions(controller.Options{
			LogConstructor: roletracker.NewLogConstructor(r.roleTracker, podControllerName),
		}).
		Complete(r)
}

func (r *PodReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	job := &kueuealpha.TestJob{}
	if err := r.client.Get(ctx, req.NamespacedName, job); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if err := r.reconcilePods(ctx, job); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.reconcileStatus(ctx, job)
}

func (r *PodReconciler) reconcilePods(ctx context.Context, job *kueuealpha.TestJob) error {
	pods, err := r.listPods(ctx, job)
	if err != nil {
		return err
	}

	activePods := activePods(pods)
	desiredActive := desiredActivePods(job, succeededPods(pods))
	if ptr.Deref(job.Spec.Suspend, false) || testJobComplete(job) {
		desiredActive = 0
	}

	if int32(len(activePods)) > desiredActive {
		return r.deleteActivePods(ctx, activePods[int(desiredActive):])
	}
	for i := int32(len(activePods)); i < desiredActive; i++ {
		if err := r.createPod(ctx, job); err != nil {
			return err
		}
	}
	return nil
}

func (r *PodReconciler) reconcileStatus(ctx context.Context, job *kueuealpha.TestJob) error {
	pods, err := r.listPods(ctx, job)
	if err != nil {
		return err
	}
	status := calculateStatus(job, pods)

	return clientutil.PatchStatus(ctx, r.client, job, func() (bool, error) {
		changed := !testJobStatusEqual(job.Status, status)
		job.Status = status
		return changed, nil
	})
}

func (r *PodReconciler) listPods(ctx context.Context, job *kueuealpha.TestJob) ([]corev1.Pod, error) {
	var podList corev1.PodList
	if err := r.client.List(ctx, &podList,
		client.InNamespace(job.Namespace),
		client.MatchingLabels{
			TestJobUIDLabel: string(job.UID),
		},
	); err != nil {
		return nil, err
	}
	return podList.Items, nil
}

func (r *PodReconciler) createPod(ctx context.Context, job *kueuealpha.TestJob) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        r.nextPodName(ctx, job),
			Namespace:   job.Namespace,
			Labels:      maps.Clone(job.Spec.Template.Labels),
			Annotations: maps.Clone(job.Spec.Template.Annotations),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         gvk.GroupVersion().String(),
				Kind:               gvk.Kind,
				Name:               job.Name,
				UID:                job.UID,
				Controller:         ptr.To(true),
				BlockOwnerDeletion: ptr.To(true),
			}},
		},
		Spec: *job.Spec.Template.Spec.DeepCopy(),
	}
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	pod.Labels[TestJobNameLabel] = job.Name
	pod.Labels[TestJobUIDLabel] = string(job.UID)
	if pod.Spec.RestartPolicy == "" {
		pod.Spec.RestartPolicy = corev1.RestartPolicyNever
	}
	return r.client.Create(ctx, pod)
}

func (r *PodReconciler) nextPodName(ctx context.Context, job *kueuealpha.TestJob) string {
	pods, err := r.listPods(ctx, job)
	if err != nil {
		return fmt.Sprintf("%s-0", job.Name)
	}
	used := make(map[string]struct{}, len(pods))
	for _, pod := range pods {
		used[pod.Name] = struct{}{}
	}
	for i := 0; ; i++ {
		name := fmt.Sprintf("%s-%d", job.Name, i)
		if _, found := used[name]; !found {
			return name
		}
	}
}

func (r *PodReconciler) deleteActivePods(ctx context.Context, pods []corev1.Pod) error {
	for i := range pods {
		if err := r.client.Delete(ctx, &pods[i]); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
}

func activePods(pods []corev1.Pod) []corev1.Pod {
	active := make([]corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		if isActivePod(&pod) {
			active = append(active, pod)
		}
	}
	slices.SortFunc(active, func(a, b corev1.Pod) int {
		if cmp := a.CreationTimestamp.Compare(b.CreationTimestamp.Time); cmp != 0 {
			return cmp
		}
		return slices.Compare([]byte(a.Name), []byte(b.Name))
	})
	return active
}

func succeededPods(pods []corev1.Pod) int32 {
	var succeeded int32
	for i := range pods {
		if pods[i].Status.Phase == corev1.PodSucceeded {
			succeeded++
		}
	}
	return succeeded
}

func isActivePod(pod *corev1.Pod) bool {
	return pod.DeletionTimestamp == nil && !utilpod.IsTerminated(pod)
}

func desiredActivePods(job *kueuealpha.TestJob, succeeded int32) int32 {
	parallelism := ptr.Deref(job.Spec.Parallelism, int32(1))
	if parallelism < 0 {
		return 0
	}
	if !hasCompletionTarget(job) {
		return parallelism
	}
	remaining := *job.Spec.Completions - succeeded
	if remaining < 0 {
		return 0
	}
	if remaining < parallelism {
		return remaining
	}
	return parallelism
}

func calculateStatus(job *kueuealpha.TestJob, pods []corev1.Pod) kueuealpha.TestJobStatus {
	status := job.Status.DeepCopy()
	status.Active = 0
	status.Succeeded = 0
	status.Failed = 0
	ready := int32(0)

	for i := range pods {
		pod := &pods[i]
		switch pod.Status.Phase {
		case corev1.PodSucceeded:
			status.Succeeded++
		case corev1.PodFailed:
			status.Failed++
		default:
			if isActivePod(pod) {
				status.Active++
				if podReady(pod) {
					ready++
				}
			}
		}
	}

	status.Ready = ptr.To(ready)
	if status.Active > 0 && status.StartTime == nil {
		now := metav1.Now()
		status.StartTime = &now
	}
	if hasCompletionTarget(job) && status.Succeeded >= *job.Spec.Completions {
		now := metav1.Now()
		if status.CompletionTime == nil {
			status.CompletionTime = &now
		}
		setJobCondition(status, batchv1.JobComplete, corev1.ConditionTrue, "Completed", "TestJob completed")
	} else {
		removeJobCondition(status, batchv1.JobComplete)
		status.CompletionTime = nil
	}
	return *status
}

func testJobComplete(job *kueuealpha.TestJob) bool {
	if !hasCompletionTarget(job) {
		return false
	}
	return job.Status.Succeeded >= *job.Spec.Completions
}

func hasCompletionTarget(job *kueuealpha.TestJob) bool {
	return job.Spec.Completions != nil && *job.Spec.Completions != noCompletions
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func setJobCondition(status *kueuealpha.TestJobStatus, conditionType batchv1.JobConditionType, conditionStatus corev1.ConditionStatus, reason, message string) {
	now := metav1.Now()
	for i := range status.Conditions {
		if status.Conditions[i].Type == conditionType {
			status.Conditions[i].Status = conditionStatus
			status.Conditions[i].Reason = reason
			status.Conditions[i].Message = message
			status.Conditions[i].LastProbeTime = now
			status.Conditions[i].LastTransitionTime = now
			return
		}
	}
	status.Conditions = append(status.Conditions, batchv1.JobCondition{
		Type:               conditionType,
		Status:             conditionStatus,
		LastProbeTime:      now,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	})
}

func removeJobCondition(status *kueuealpha.TestJobStatus, conditionType batchv1.JobConditionType) {
	status.Conditions = slices.DeleteFunc(status.Conditions, func(condition batchv1.JobCondition) bool {
		return condition.Type == conditionType
	})
}

func testJobStatusEqual(a, b kueuealpha.TestJobStatus) bool {
	if a.Active != b.Active || a.Succeeded != b.Succeeded || a.Failed != b.Failed {
		return false
	}
	if ptr.Deref(a.Ready, -1) != ptr.Deref(b.Ready, -1) {
		return false
	}
	if (a.StartTime == nil) != (b.StartTime == nil) || (a.CompletionTime == nil) != (b.CompletionTime == nil) {
		return false
	}
	return conditionsEqual(a.Conditions, b.Conditions)
}

func conditionsEqual(a, b []batchv1.JobCondition) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Type != b[i].Type ||
			a[i].Status != b[i].Status ||
			a[i].Reason != b[i].Reason ||
			a[i].Message != b[i].Message {
			return false
		}
	}
	return true
}
