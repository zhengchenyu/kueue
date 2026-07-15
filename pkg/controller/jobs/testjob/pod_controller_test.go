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
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/podset"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
)

const testNamespace = "ns"

func TestPodReconcilerCreatesPods(t *testing.T) {
	ctx := context.Background()
	job := makeTestJob("job", false, 2, nil)
	job.Spec.Template.Labels = map[string]string{"app": "demo"}
	job.Spec.Template.Annotations = map[string]string{
		kueue.WorkloadAnnotation:          "wl",
		kueue.WorkloadSliceNameAnnotation: "slice",
	}
	job.Spec.Template.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: kueue.ElasticJobSchedulingGate}}

	kClient := utiltesting.NewClientBuilder(kueuealpha.AddToScheme).
		WithObjects(job).
		WithStatusSubresource(job).
		Build()
	reconciler := &PodReconciler{client: kClient}

	if _, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(job)}); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	pods := listTestPods(t, ctx, kClient, job)
	if len(pods) != 2 {
		t.Fatalf("pods count = %d, want 2", len(pods))
	}
	for _, pod := range pods {
		if pod.Labels[TestJobNameLabel] != job.Name {
			t.Errorf("pod %s missing TestJobNameLabel", pod.Name)
		}
		if pod.Labels[TestJobUIDLabel] != string(job.UID) {
			t.Errorf("pod %s missing TestJobUIDLabel", pod.Name)
		}
		if pod.Labels["app"] != "demo" {
			t.Errorf("pod %s did not inherit template labels", pod.Name)
		}
		if pod.Annotations[kueue.WorkloadAnnotation] != "wl" || pod.Annotations[kueue.WorkloadSliceNameAnnotation] != "slice" {
			t.Errorf("pod %s did not inherit Kueue annotations", pod.Name)
		}
		if len(pod.Spec.SchedulingGates) != 1 || pod.Spec.SchedulingGates[0].Name != kueue.ElasticJobSchedulingGate {
			t.Errorf("pod %s did not inherit scheduling gate", pod.Name)
		}
	}

	var gotJob kueuealpha.TestJob
	if err := kClient.Get(ctx, client.ObjectKeyFromObject(job), &gotJob); err != nil {
		t.Fatalf("Get(TestJob) error = %v", err)
	}
	if gotJob.Status.Active != 2 {
		t.Fatalf("status.active = %d, want 2", gotJob.Status.Active)
	}
}

func TestPodReconcilerDeletesPodsWhenSuspended(t *testing.T) {
	ctx := context.Background()
	job := makeTestJob("job", true, 2, nil)
	job.Status.Active = 1
	pod := makeTestPod(job, "job-0", corev1.PodRunning)

	kClient := utiltesting.NewClientBuilder(kueuealpha.AddToScheme).
		WithObjects(job, pod).
		WithStatusSubresource(job).
		Build()
	reconciler := &PodReconciler{client: kClient}

	if _, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(job)}); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	pods := listTestPods(t, ctx, kClient, job)
	if len(pods) != 0 {
		t.Fatalf("pods count = %d, want 0", len(pods))
	}
	var gotJob kueuealpha.TestJob
	if err := kClient.Get(ctx, client.ObjectKeyFromObject(job), &gotJob); err != nil {
		t.Fatalf("Get(TestJob) error = %v", err)
	}
	if gotJob.Status.Active != 0 {
		t.Fatalf("status.active = %d, want 0", gotJob.Status.Active)
	}
}

func TestPodReconcilerCompletesFromObservedPods(t *testing.T) {
	ctx := context.Background()
	completions := int32(1)
	job := makeTestJob("job", false, 3, &completions)
	pod := makeTestPod(job, "job-0", corev1.PodSucceeded)

	kClient := utiltesting.NewClientBuilder(kueuealpha.AddToScheme).
		WithObjects(job, pod).
		WithStatusSubresource(job).
		Build()
	reconciler := &PodReconciler{client: kClient}

	if _, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(job)}); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	pods := listTestPods(t, ctx, kClient, job)
	if len(pods) != 1 {
		t.Fatalf("pods count = %d, want only the succeeded pod", len(pods))
	}
	var gotJob kueuealpha.TestJob
	if err := kClient.Get(ctx, client.ObjectKeyFromObject(job), &gotJob); err != nil {
		t.Fatalf("Get(TestJob) error = %v", err)
	}
	if gotJob.Status.Succeeded != 1 || gotJob.Status.Active != 0 {
		t.Fatalf("status = %+v, want succeeded=1 active=0", gotJob.Status)
	}
	if !hasJobCompleteCondition(gotJob.Status.Conditions) {
		t.Fatalf("missing JobComplete condition: %+v", gotJob.Status.Conditions)
	}
}

func TestRunWithPodSetsInfoUpdatesParallelismForPartialAdmission(t *testing.T) {
	job := (*TestJob)(makeTestJob("job", true, 10, nil))
	job.Annotations = map[string]string{TestJobMinParallelismAnnotation: "2"}

	err := job.RunWithPodSetsInfo(context.Background(), nil, []podset.PodSetInfo{{
		Count: 4,
		Annotations: map[string]string{
			kueue.WorkloadAnnotation: "wl",
		},
	}})
	if err != nil {
		t.Fatalf("RunWithPodSetsInfo() error = %v", err)
	}
	if ptr.Deref(job.Spec.Suspend, true) {
		t.Fatalf("job should be unsuspended")
	}
	if got := ptr.Deref(job.Spec.Parallelism, int32(0)); got != 4 {
		t.Fatalf("parallelism = %d, want 4", got)
	}
	if got := job.Spec.Template.Annotations[kueue.WorkloadAnnotation]; got != "wl" {
		t.Fatalf("template workload annotation = %q, want wl", got)
	}
}

func makeTestJob(name string, suspended bool, parallelism int32, completions *int32) *kueuealpha.TestJob {
	return &kueuealpha.TestJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
			UID:       types.UID(name + "-uid"),
		},
		Spec: kueuealpha.TestJobSpec{
			Suspend:     ptr.To(suspended),
			Parallelism: ptr.To(parallelism),
			Completions: completions,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:  "main",
						Image: "busybox",
					}},
				},
			},
		},
	}
}

func makeTestPod(job *kueuealpha.TestJob, name string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: job.Namespace,
			Labels: map[string]string{
				TestJobNameLabel: job.Name,
				TestJobUIDLabel:  string(job.UID),
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         gvk.GroupVersion().String(),
				Kind:               gvk.Kind,
				Name:               job.Name,
				UID:                job.UID,
				Controller:         ptr.To(true),
				BlockOwnerDeletion: ptr.To(true),
			}},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:  "main",
				Image: "busybox",
			}},
		},
		Status: corev1.PodStatus{Phase: phase},
	}
}

func listTestPods(t *testing.T, ctx context.Context, kClient client.Client, job *kueuealpha.TestJob) []corev1.Pod {
	t.Helper()
	var podList corev1.PodList
	if err := kClient.List(ctx, &podList, client.InNamespace(job.Namespace), client.MatchingLabels{TestJobUIDLabel: string(job.UID)}); err != nil {
		t.Fatalf("List(Pods) error = %v", err)
	}
	return podList.Items
}

func hasJobCompleteCondition(conditions []batchv1.JobCondition) bool {
	for _, condition := range conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
