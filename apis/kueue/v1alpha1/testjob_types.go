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

package v1alpha1

import (
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestJobSpec defines the desired state of TestJob.
type TestJobSpec struct {
	// suspend controls whether the TestJob pod controller should create pods.
	// +optional
	Suspend *bool `json:"suspend,omitempty"`

	// parallelism is the maximum desired number of active pods.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=1
	// +optional
	Parallelism *int32 `json:"parallelism,omitempty"`

	// completions is the desired number of successfully finished pods.
	// If unset, the TestJob keeps parallelism pods active.
	// +kubebuilder:validation:Minimum=0
	// +optional
	Completions *int32 `json:"completions,omitempty"`

	// template describes the pods that will be created by this TestJob.
	// +required
	Template corev1.PodTemplateSpec `json:"template"`
}

// TestJobStatus defines the observed state of TestJob.
type TestJobStatus struct {
	// active is the number of active pods.
	// +optional
	Active int32 `json:"active,omitempty"`

	// ready is the number of active pods with a Ready condition.
	// +optional
	Ready *int32 `json:"ready,omitempty"`

	// succeeded is the number of pods which reached phase Succeeded.
	// +optional
	Succeeded int32 `json:"succeeded,omitempty"`

	// failed is the number of pods which reached phase Failed.
	// +optional
	Failed int32 `json:"failed,omitempty"`

	// startTime represents time when the first pod was created.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// completionTime represents time when the TestJob completed successfully.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// conditions represent the latest available observations of the TestJob state.
	// The controller uses batch/v1 Job condition types intentionally, so Kueue tests
	// can exercise the same completion and failure paths as native Jobs.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []batchv1.JobCondition `json:"conditions,omitempty"`
}

// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// TestJob is a minimal Job-like CRD for testing Kueue job-framework behavior
// without modifying an external job framework controller.
type TestJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TestJobSpec   `json:"spec,omitempty"`
	Status TestJobStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TestJobList contains a list of TestJob.
type TestJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TestJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TestJob{}, &TestJobList{})
}
