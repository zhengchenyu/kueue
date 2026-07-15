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
	"k8s.io/apimachinery/pkg/runtime"
)

func (in *TestJobSpec) DeepCopyInto(out *TestJobSpec) {
	*out = *in
	if in.Suspend != nil {
		in, out := &in.Suspend, &out.Suspend
		*out = new(bool)
		**out = **in
	}
	if in.Parallelism != nil {
		in, out := &in.Parallelism, &out.Parallelism
		*out = new(int32)
		**out = **in
	}
	if in.Completions != nil {
		in, out := &in.Completions, &out.Completions
		*out = new(int32)
		**out = **in
	}
	in.Template.DeepCopyInto(&out.Template)
}

func (in *TestJobSpec) DeepCopy() *TestJobSpec {
	if in == nil {
		return nil
	}
	out := new(TestJobSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *TestJobStatus) DeepCopyInto(out *TestJobStatus) {
	*out = *in
	if in.Ready != nil {
		in, out := &in.Ready, &out.Ready
		*out = new(int32)
		**out = **in
	}
	if in.StartTime != nil {
		in, out := &in.StartTime, &out.StartTime
		*out = (*in).DeepCopy()
	}
	if in.CompletionTime != nil {
		in, out := &in.CompletionTime, &out.CompletionTime
		*out = (*in).DeepCopy()
	}
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]batchv1.JobCondition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

func (in *TestJobStatus) DeepCopy() *TestJobStatus {
	if in == nil {
		return nil
	}
	out := new(TestJobStatus)
	in.DeepCopyInto(out)
	return out
}

func (in *TestJob) DeepCopyInto(out *TestJob) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

func (in *TestJob) DeepCopy() *TestJob {
	if in == nil {
		return nil
	}
	out := new(TestJob)
	in.DeepCopyInto(out)
	return out
}

func (in *TestJob) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *TestJobList) DeepCopyInto(out *TestJobList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]TestJob, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

func (in *TestJobList) DeepCopy() *TestJobList {
	if in == nil {
		return nil
	}
	out := new(TestJobList)
	in.DeepCopyInto(out)
	return out
}

func (in *TestJobList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}
