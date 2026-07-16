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
	"strconv"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apivalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/controller/core/indexer"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/podset"
	utilpod "sigs.k8s.io/kueue/pkg/util/pod"
	"sigs.k8s.io/kueue/pkg/util/webhook"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

var gvk = kueuealpha.GroupVersion.WithKind("TestJob")

const (
	FrameworkName = "kueue.x-k8s.io/testjob"

	TestJobNameLabel = "kueue.x-k8s.io/testjob-name"
	TestJobUIDLabel  = "kueue.x-k8s.io/testjob-uid"

	TestJobMinParallelismAnnotation = "kueue.x-k8s.io/job-min-parallelism"

	noCompletions int32 = -1
)

func init() {
	utilruntime.Must(jobframework.RegisterIntegration(FrameworkName, jobframework.IntegrationCallbacks{
		SetupIndexes:  SetupIndexes,
		NewJob:        NewJob,
		NewReconciler: NewReconciler,
		NewAdditionalReconcilers: []jobframework.ReconcilerFactory{
			NewPodReconciler,
		},
		SetupWebhook: SetupWebhook,
		JobType:      &kueuealpha.TestJob{},
		AddToScheme:  kueuealpha.AddToScheme,
	}))
}

// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;watch;update;patch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=testjobs,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=testjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=testjobs/finalizers,verbs=get;update;patch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads/finalizers,verbs=update
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=resourceflavors,verbs=get;list;watch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloadpriorityclasses,verbs=get;list;watch

func NewJob() jobframework.GenericJob {
	return &TestJob{}
}

var NewReconciler = jobframework.NewGenericReconcilerFactory(NewJob, func(b *builder.Builder, c client.Client) *builder.Builder {
	return b.Watches(&kueue.Workload{}, &parentWorkloadHandler{client: c})
})

type TestJob kueuealpha.TestJob

var _ jobframework.GenericJob = (*TestJob)(nil)
var _ jobframework.JobWithReclaimablePods = (*TestJob)(nil)
var _ jobframework.JobWithCustomValidation = (*TestJob)(nil)

func (j *TestJob) Object() client.Object {
	return (*kueuealpha.TestJob)(j)
}

func fromObject(o runtime.Object) *TestJob {
	return (*TestJob)(o.(*kueuealpha.TestJob))
}

func (j *TestJob) IsSuspended() bool {
	return ptr.Deref(j.Spec.Suspend, false)
}

func (j *TestJob) IsActive() bool {
	return j.Status.Active != 0
}

func (j *TestJob) Suspend() {
	j.Spec.Suspend = ptr.To(true)
}

func (j *TestJob) GVK() schema.GroupVersionKind {
	return gvk
}

func (j *TestJob) PodLabelSelector() string {
	return fmt.Sprintf("%s=%s", TestJobNameLabel, j.Name)
}

func (j *TestJob) PodSets(ctx context.Context, _ client.Client) ([]kueue.PodSet, error) {
	podSet := kueue.PodSet{
		Name:     kueue.DefaultPodSetName,
		Template: *j.Spec.Template.DeepCopy(),
		Count:    j.podsCount(),
		MinCount: j.minPodsCount(),
	}
	if features.Enabled(features.TopologyAwareScheduling) {
		topologyRequest, err := jobframework.NewPodSetTopologyRequest(&j.Spec.Template.ObjectMeta).Build()
		if err != nil {
			return nil, err
		}
		podSet.TopologyRequest = topologyRequest
	}
	return []kueue.PodSet{podSet}, nil
}

func (j *TestJob) RunWithPodSetsInfo(ctx context.Context, _ client.Client, podSetsInfo []podset.PodSetInfo) error {
	if len(podSetsInfo) != 1 {
		return podset.BadPodSetsInfoLenError(1, len(podSetsInfo))
	}
	j.Spec.Suspend = ptr.To(false)

	info := podSetsInfo[0]
	if j.minPodsCount() != nil {
		j.Spec.Parallelism = ptr.To(info.Count)
	}
	return podset.Merge(&j.Spec.Template.ObjectMeta, &j.Spec.Template.Spec, info)
}

func (j *TestJob) RestorePodSetsInfo(podSetsInfo []podset.PodSetInfo) bool {
	if len(podSetsInfo) == 0 {
		return false
	}
	changed := false
	if j.minPodsCount() != nil && ptr.Deref(j.Spec.Parallelism, int32(1)) != podSetsInfo[0].Count {
		j.Spec.Parallelism = ptr.To(podSetsInfo[0].Count)
		changed = true
	}
	return podset.RestorePodSpec(&j.Spec.Template.ObjectMeta, &j.Spec.Template.Spec, podSetsInfo[0]) || changed
}

func (j *TestJob) Finished(ctx context.Context) (message string, success, finished bool) {
	for _, c := range j.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return c.Message, c.Type != batchv1.JobFailed, true
		}
	}
	return "", true, false
}

func (j *TestJob) PodsReady(ctx context.Context, _ client.Client) bool {
	if j.hasCompletionTarget() {
		// comp = 100, par = 10, succeed = 10, ready = 10
		return j.Status.Succeeded+ptr.Deref(j.Status.Ready, int32(0)) >= j.podsCount()
	}
	return ptr.Deref(j.Status.Ready, int32(0)) >= j.podsCount()
}

func (j *TestJob) ReclaimablePods(ctx context.Context, _ client.Client) ([]kueue.ReclaimablePod, error) {
	if !j.hasCompletionTarget() {
		return nil, nil
	}
	parallelism := ptr.Deref(j.Spec.Parallelism, int32(1))
	if parallelism == 1 || j.Status.Succeeded == 0 {
		return nil, nil
	}
	remaining := *j.Spec.Completions - j.Status.Succeeded
	if remaining >= parallelism {
		return nil, nil
	}
	return []kueue.ReclaimablePod{{
		Name:  kueue.DefaultPodSetName,
		Count: parallelism - remaining,
	}}, nil
}

func (j *TestJob) ValidateOnCreate(ctx context.Context) (field.ErrorList, error) {
	return j.validatePartialAdmission(), nil
}

func (j *TestJob) ValidateOnUpdate(ctx context.Context, oldJob jobframework.GenericJob) (field.ErrorList, error) {
	oldTestJob, ok := oldJob.(*TestJob)
	if !ok {
		return nil, nil
	}
	var allErrs field.ErrorList
	if j.GetAnnotations()[TestJobMinParallelismAnnotation] != oldTestJob.GetAnnotations()[TestJobMinParallelismAnnotation] {
		allErrs = append(allErrs, j.validatePartialAdmission()...)
	}
	if _, found := oldTestJob.GetAnnotations()[TestJobMinParallelismAnnotation]; found {
		if !ptr.Deref(oldTestJob.Spec.Suspend, false) && ptr.Deref(oldTestJob.Spec.Parallelism, int32(1)) != ptr.Deref(j.Spec.Parallelism, int32(1)) {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("spec", "parallelism"), "cannot change when partial admission is enabled and the job is not suspended"))
		}
	}
	return allErrs, nil
}

func (j *TestJob) validatePartialAdmission() field.ErrorList {
	var allErrs field.ErrorList
	annotations := j.GetAnnotations()
	if strVal, found := annotations[TestJobMinParallelismAnnotation]; found {
		v, err := strconv.Atoi(strVal)
		if err != nil {
			allErrs = append(allErrs, field.Invalid(field.NewPath("metadata", "annotations").Key(TestJobMinParallelismAnnotation), strVal, err.Error()))
		} else if int32(v) >= j.podsCount() || v <= 0 {
			allErrs = append(allErrs, field.Invalid(field.NewPath("metadata", "annotations").Key(TestJobMinParallelismAnnotation), v, fmt.Sprintf("should be between 0 and %d", j.podsCount()-1)))
		}
	}
	return allErrs
}

func (j *TestJob) podsCount() int32 {
	podsCount := ptr.Deref(j.Spec.Parallelism, int32(1))
	if j.hasCompletionTarget() && *j.Spec.Completions < podsCount {
		podsCount = *j.Spec.Completions
	}
	return podsCount
}

func (j *TestJob) hasCompletionTarget() bool {
	return j.Spec.Completions != nil && *j.Spec.Completions != noCompletions
}

func (j *TestJob) minPodsCount() *int32 {
	strVal, found := j.GetAnnotations()[TestJobMinParallelismAnnotation]
	if !found {
		return nil
	}
	iVal, err := strconv.Atoi(strVal)
	if err != nil {
		return nil
	}
	return ptr.To(int32(iVal))
}

func SetupIndexes(ctx context.Context, fieldIndexer client.FieldIndexer) error {
	if err := fieldIndexer.IndexField(ctx, &kueuealpha.TestJob{}, indexer.OwnerReferenceUID, indexer.IndexOwnerUID); err != nil {
		return err
	}
	return jobframework.SetupWorkloadOwnerIndex(ctx, fieldIndexer, gvk)
}

type parentWorkloadHandler struct {
	client client.Client
}

func (h *parentWorkloadHandler) Create(ctx context.Context, e event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	h.queueReconcileForChildJob(ctx, e.Object, q)
}

func (h *parentWorkloadHandler) Update(ctx context.Context, e event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	h.queueReconcileForChildJob(ctx, e.ObjectNew, q)
}

func (h *parentWorkloadHandler) Delete(context.Context, event.DeleteEvent, workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (h *parentWorkloadHandler) Generic(context.Context, event.GenericEvent, workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (h *parentWorkloadHandler) queueReconcileForChildJob(ctx context.Context, object client.Object, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	w, ok := object.(*kueue.Workload)
	if !ok {
		return
	}
	owner := metav1.GetControllerOf(w)
	if owner == nil {
		return
	}
	var childJobs kueuealpha.TestJobList
	if err := h.client.List(ctx, &childJobs, client.InNamespace(w.Namespace), client.MatchingFields{indexer.OwnerReferenceUID: string(owner.UID)}); err != nil {
		return
	}
	for _, childJob := range childJobs.Items {
		q.Add(reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&childJob)})
	}
}

type TestJobWebhook struct {
	client                       client.Client
	manageJobsWithoutQueueName   bool
	managedJobsNamespaceSelector labels.Selector
	queues                       *qcache.Manager
	cache                        *schdcache.Cache
}

func SetupWebhook(mgr ctrl.Manager, opts ...jobframework.Option) error {
	options := jobframework.ProcessOptions(opts...)
	wh := &TestJobWebhook{
		client:                       mgr.GetClient(),
		manageJobsWithoutQueueName:   options.ManageJobsWithoutQueueName,
		managedJobsNamespaceSelector: options.ManagedJobsNamespaceSelector,
		queues:                       options.Queues,
		cache:                        options.Cache,
	}
	obj := &kueuealpha.TestJob{}
	if options.NoopWebhook {
		return webhook.SetupNoopWebhook(mgr, obj)
	}
	return ctrl.NewWebhookManagedBy(mgr, obj).
		WithDefaulter(wh).
		WithValidator(wh).
		WithLogConstructor(jobframework.WebhookLogConstructor(gvk, options.RoleTracker)).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-kueue-x-k8s-io-v1alpha1-testjob,mutating=true,failurePolicy=fail,sideEffects=None,groups=kueue.x-k8s.io,resources=testjobs,verbs=create,versions=v1alpha1,name=mtestjob.kb.io,admissionReviewVersions=v1

var _ admission.Defaulter[*kueuealpha.TestJob] = &TestJobWebhook{}

func (w *TestJobWebhook) Default(ctx context.Context, obj *kueuealpha.TestJob) error {
	job := fromObject(obj)
	jobframework.ApplyDefaultLocalQueue(job.Object(), w.queues.DefaultLocalQueueExist)
	jobframework.ApplyDefaultWorkloadPriorityClass(ctx, w.client, job.Object())
	if err := jobframework.ApplyDefaultForSuspend(ctx, job, w.client, w.manageJobsWithoutQueueName, w.managedJobsNamespaceSelector); err != nil {
		return err
	}
	if job.Spec.Parallelism == nil {
		job.Spec.Parallelism = ptr.To[int32](1)
	}
	if job.Spec.Template.Spec.RestartPolicy == "" {
		job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
	}
	applyWorkloadSliceSchedulingGate(job)
	return nil
}

func applyWorkloadSliceSchedulingGate(job *TestJob) {
	if !features.Enabled(features.ElasticJobsViaWorkloadSlices) || !workloadslicing.Enabled(job.Object()) {
		return
	}
	utilpod.GateTemplate(&job.Spec.Template, kueue.ElasticJobSchedulingGate)
}

// +kubebuilder:webhook:path=/validate-kueue-x-k8s-io-v1alpha1-testjob,mutating=false,failurePolicy=fail,sideEffects=None,groups=kueue.x-k8s.io,resources=testjobs,verbs=create;update,versions=v1alpha1,name=vtestjob.kb.io,admissionReviewVersions=v1

var _ admission.Validator[*kueuealpha.TestJob] = &TestJobWebhook{}

func (w *TestJobWebhook) ValidateCreate(ctx context.Context, obj *kueuealpha.TestJob) (admission.Warnings, error) {
	job := fromObject(obj)
	allErrs := w.validateCommon(job)
	validationErrs, err := job.ValidateOnCreate(ctx)
	if err != nil {
		return nil, err
	}
	allErrs = append(allErrs, validationErrs...)
	return nil, allErrs.ToAggregate()
}

func (w *TestJobWebhook) ValidateUpdate(ctx context.Context, oldObj, newObj *kueuealpha.TestJob) (admission.Warnings, error) {
	oldJob := fromObject(oldObj)
	newJob := fromObject(newObj)
	allErrs := w.validateCommon(newJob)
	allErrs = append(allErrs, validateQueueNameUpdate(oldJob, newJob, w.queues.DefaultLocalQueueExist)...)
	validationErrs, err := newJob.ValidateOnUpdate(ctx, oldJob)
	if err != nil {
		return nil, err
	}
	allErrs = append(allErrs, validationErrs...)
	return nil, allErrs.ToAggregate()
}

func (w *TestJobWebhook) ValidateDelete(context.Context, *kueuealpha.TestJob) (admission.Warnings, error) {
	return nil, nil
}

func (w *TestJobWebhook) validateCommon(job *TestJob) field.ErrorList {
	var allErrs field.ErrorList
	allErrs = append(allErrs, jobframework.ValidateQueueName(job.Object())...)
	if ptr.Deref(job.Spec.Parallelism, int32(1)) < 0 {
		allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "parallelism"), job.Spec.Parallelism, "must be greater than or equal to 0"))
	}
	if job.Spec.Completions != nil && *job.Spec.Completions < noCompletions {
		allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "completions"), job.Spec.Completions, "must be greater than or equal to -1"))
	}
	for i, container := range job.Spec.Template.Spec.Containers {
		for resourceName, request := range container.Resources.Requests {
			if request.Cmp(resource.Quantity{}) < 0 {
				allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "template", "spec", "containers").Index(i).Child("resources", "requests").Key(string(resourceName)), request.String(), "must be greater than or equal to 0"))
			}
		}
	}
	return allErrs
}

func validateQueueNameUpdate(oldJob, newJob *TestJob, defaultQueueExist func(string) bool) field.ErrorList {
	var allErrs field.ErrorList
	if !newJob.IsSuspended() {
		allErrs = append(allErrs, apivalidation.ValidateImmutableField(jobframework.QueueName(newJob), jobframework.QueueName(oldJob), field.NewPath("metadata", "labels").Key("kueue.x-k8s.io/queue-name"))...)
	}
	if jobframework.QueueName(newJob) == "" && jobframework.QueueName(oldJob) != "" && defaultQueueExist(oldJob.Object().GetNamespace()) {
		allErrs = append(allErrs, field.Invalid(field.NewPath("metadata", "labels").Key("kueue.x-k8s.io/queue-name"), "", "queue-name must not be empty in namespace with default queue"))
	}
	return allErrs
}
