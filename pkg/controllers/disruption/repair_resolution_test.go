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
	"k8s.io/apimachinery/pkg/types"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

func TestResolveRepairCandidate(t *testing.T) {
	oneMinute := time.Minute
	twoMinutes := 2 * time.Minute
	results := []RepairResult{
		repairResultForTest("AcceleratorReady", "XID48", cloudprovider.RebootNode, time.Unix(1, 0), &oneMinute),
		repairResultForTest("StorageReady", "DiskFailure", cloudprovider.ReplaceNode, time.Unix(2, 0), nil),
		repairResultForTest("NetworkingReady", "LinkDown", cloudprovider.ReplaceNode, time.Unix(3, 0), &twoMinutes),
	}
	expectedCondition := RepairEvidence{Type: "StorageReady", Status: corev1.ConditionFalse, Reason: "DiskFailure"}
	expectedDrainCondition := RepairEvidence{Type: "AcceleratorReady", Status: corev1.ConditionFalse, Reason: "XID48"}

	for _, order := range [][]int{
		{0, 1, 2},
		{0, 2, 1},
		{1, 0, 2},
		{1, 2, 0},
		{2, 0, 1},
		{2, 1, 0},
	} {
		candidate := repairCandidateForTest("nodeclaim-uid")
		permuted := []RepairResult{results[order[0]], results[order[1]], results[order[2]]}
		if !resolveRepairCandidate(candidate, permuted) {
			t.Fatalf("expected a candidate for order %v", order)
		}
		if candidate.Action != cloudprovider.ReplaceNode ||
			!candidate.RepairEligibleAt.Equal(time.Unix(2, 0)) ||
			candidate.RepairCondition != expectedCondition {
			t.Fatalf("unexpected action decision for order %v: %#v", order, candidate)
		}
		if candidate.TerminationGracePeriod == nil || *candidate.TerminationGracePeriod != time.Minute {
			t.Fatalf("unexpected drain bound for order %v: %v", order, candidate.TerminationGracePeriod)
		}
		if candidate.TerminationGracePeriodCondition == nil || *candidate.TerminationGracePeriodCondition != expectedDrainCondition {
			t.Fatalf("unexpected drain-bound condition for order %v: %#v", order, candidate.TerminationGracePeriodCondition)
		}
	}

	candidate := repairCandidateForTest("nodeclaim-uid")
	resolveRepairCandidate(candidate, results)
	oneMinute = 10 * time.Minute
	if *candidate.TerminationGracePeriod != time.Minute {
		t.Fatal("expected the resolved drain bound to be cloned")
	}
}

func TestResolveRepairCandidateClearsStaleDecision(t *testing.T) {
	candidate := repairCandidateForTest("nodeclaim-uid")
	candidate.Action = cloudprovider.ReplaceNode
	candidate.RepairCondition = RepairEvidence{Type: "BadNode"}
	candidate.TerminationGracePeriod = new(time.Duration)

	if resolveRepairCandidate(candidate, nil) {
		t.Fatal("expected no candidate without eligible results")
	}
	if candidate.Action != "" || candidate.RepairCondition.Type != "" || candidate.TerminationGracePeriod != nil {
		t.Fatalf("expected stale repair decision to be cleared: %#v", candidate)
	}
}

func TestResolveRepairCandidatePreservesForcefulDrain(t *testing.T) {
	zero := time.Duration(0)
	oneMinute := time.Minute
	candidate := repairCandidateForTest("nodeclaim-uid")
	results := []RepairResult{
		repairResultForTest("AcceleratorReady", "XID48", cloudprovider.RebootNode, time.Unix(1, 0), &oneMinute),
		repairResultForTest("StorageReady", "Transient", cloudprovider.RebootNode, time.Unix(2, 0), &zero),
	}

	if !resolveRepairCandidate(candidate, results) {
		t.Fatal("expected a repair candidate")
	}
	if candidate.TerminationGracePeriod == nil || *candidate.TerminationGracePeriod != 0 {
		t.Fatalf("expected a zero drain bound, got %v", candidate.TerminationGracePeriod)
	}
	if candidate.TerminationGracePeriodCondition == nil || candidate.TerminationGracePeriodCondition.Type != "StorageReady" {
		t.Fatalf("unexpected drain-bound condition: %#v", candidate.TerminationGracePeriodCondition)
	}
}

func TestSameRepairResolution(t *testing.T) {
	drainBound := 10 * time.Minute
	resolved := repairCandidateForTest("nodeclaim-uid")
	resolved.Action = cloudprovider.ReplaceNode
	resolved.RepairEligibleAt = time.Unix(1, 0)
	resolved.RepairCondition = RepairEvidence{Type: "BadNode", Status: corev1.ConditionFalse, Reason: "Persistent"}
	resolved.TerminationGracePeriod = &drainBound

	unchanged := *resolved
	unchangedDrainBound := drainBound
	unchanged.TerminationGracePeriod = &unchangedDrainBound
	if !sameRepairResolution(resolved, &unchanged) {
		t.Fatal("expected equivalent repair resolutions to match")
	}

	changedCondition := unchanged
	changedCondition.RepairCondition = RepairEvidence{Type: "WorseNode", Status: corev1.ConditionFalse, Reason: "Urgent"}
	if sameRepairResolution(resolved, &changedCondition) {
		t.Fatal("expected a changed driving condition to invalidate the scheduling result")
	}

	changedDrainBound := unchanged
	shorterDrainBound := 2 * time.Minute
	changedDrainBound.TerminationGracePeriod = &shorterDrainBound
	if sameRepairResolution(resolved, &changedDrainBound) {
		t.Fatal("expected a changed drain bound to invalidate the scheduling result")
	}
}

func repairCandidateForTest(nodeClaimUID types.UID) *Candidate {
	return &Candidate{
		StateNode: &state.StateNode{
			Node:      &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: "node-uid"}},
			NodeClaim: &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "nodeclaim", UID: nodeClaimUID}},
		},
	}
}

func repairResultForTest(
	conditionType corev1.NodeConditionType,
	reason string,
	action cloudprovider.RepairAction,
	eligibleAt time.Time,
	terminationGracePeriod *time.Duration,
) RepairResult {
	return RepairResult{
		ConditionType: conditionType, ConditionStatus: corev1.ConditionFalse, Reason: reason, Action: action, EligibleAt: eligibleAt,
		TerminationGracePeriod: terminationGracePeriod,
	}
}
