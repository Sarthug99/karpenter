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

package disruption

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/node/health"
)

func TestEvaluateNodeUsesMaximumEligiblePolicyScore(t *testing.T) {
	now := time.Date(2026, time.September, 22, 12, 0, 0, 0, time.UTC)
	policies := []cloudprovider.RepairPolicy{
		{ConditionType: "LowPriority", ConditionStatus: corev1.ConditionFalse, ReasonRegex: ".*", TolerationDuration: 30 * time.Minute, Priority: 10, Action: cloudprovider.ReplaceNode},
		{ConditionType: "HighPriority", ConditionStatus: corev1.ConditionFalse, ReasonRegex: ".*", TolerationDuration: 30 * time.Minute, Priority: 90, Action: cloudprovider.ReplaceNode},
		{ConditionType: "Fallback", ConditionStatus: corev1.ConditionFalse, Action: cloudprovider.ReplaceNode},
	}
	matcher, err := health.NewRepairPolicyMatcher(policies, sets.New(cloudprovider.ReplaceNode))
	if err != nil {
		t.Fatalf("creating repair policy matcher, %v", err)
	}
	repair := &Repair{
		policyMatcher: matcher,
		ranks:         denseRanks(policies),
	}
	node := &corev1.Node{Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
		{
			Type:               "LowPriority",
			Status:             corev1.ConditionFalse,
			LastTransitionTime: metav1.NewTime(now.Add(-180 * time.Minute)),
		},
		{
			Type:               "HighPriority",
			Status:             corev1.ConditionFalse,
			LastTransitionTime: metav1.NewTime(now.Add(-45 * time.Minute)),
		},
	}}}

	evaluation := repair.evaluateNode(node, now)
	if evaluation.score != 6 {
		t.Fatalf("expected maximum eligible score 6, got %v", evaluation.score)
	}
}
