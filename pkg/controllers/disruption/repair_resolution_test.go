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
	drainBound := time.Minute
	drainBoundCondition := RepairEvidence{Type: "DrainBound", Status: corev1.ConditionFalse, Reason: "Forceful"}
	candidate.Action = cloudprovider.ReplaceNode
	candidate.RepairEligibleAt = time.Unix(1, 0)
	candidate.RepairCondition = RepairEvidence{Type: "BadNode", Status: corev1.ConditionFalse, Reason: "Persistent"}
	candidate.RebootEscalated = true
	candidate.TerminationGracePeriod = &drainBound
	candidate.TerminationGracePeriodCondition = &drainBoundCondition

	if resolveRepairCandidate(candidate, nil) {
		t.Fatal("expected no candidate without eligible results")
	}
	if candidate.Action != "" ||
		!candidate.RepairEligibleAt.IsZero() ||
		candidate.RepairCondition != (RepairEvidence{}) ||
		candidate.RebootEscalated ||
		candidate.TerminationGracePeriod != nil ||
		candidate.TerminationGracePeriodCondition != nil {
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
	drainBoundCondition := RepairEvidence{Type: "DrainBound", Status: corev1.ConditionFalse, Reason: "Forceful"}
	resolved := repairCandidateForTest("nodeclaim-uid")
	resolved.Action = cloudprovider.ReplaceNode
	resolved.RepairEligibleAt = time.Unix(1, 0)
	resolved.RepairCondition = RepairEvidence{Type: "BadNode", Status: corev1.ConditionFalse, Reason: "Persistent"}
	resolved.RebootEscalated = true
	resolved.TerminationGracePeriod = &drainBound
	resolved.TerminationGracePeriodCondition = &drainBoundCondition

	if !sameRepairResolution(resolved, cloneRepairCandidateForTest(resolved)) {
		t.Fatal("expected equivalent repair resolutions to match")
	}

	tests := []struct {
		name   string
		mutate func(*Candidate)
	}{
		{
			name:   "node UID",
			mutate: func(candidate *Candidate) { candidate.Node.UID = "changed-node-uid" },
		},
		{
			name:   "NodeClaim UID",
			mutate: func(candidate *Candidate) { candidate.NodeClaim.UID = "changed-nodeclaim-uid" },
		},
		{
			name:   "action",
			mutate: func(candidate *Candidate) { candidate.Action = cloudprovider.RebootNode },
		},
		{
			name:   "eligibility",
			mutate: func(candidate *Candidate) { candidate.RepairEligibleAt = candidate.RepairEligibleAt.Add(time.Second) },
		},
		{
			name: "driving condition",
			mutate: func(candidate *Candidate) {
				candidate.RepairCondition = RepairEvidence{Type: "WorseNode", Status: corev1.ConditionFalse, Reason: "Urgent"}
			},
		},
		{
			name:   "reboot escalation",
			mutate: func(candidate *Candidate) { candidate.RebootEscalated = false },
		},
		{
			name:   "drain bound",
			mutate: func(candidate *Candidate) { candidate.TerminationGracePeriod = nil },
		},
		{
			name:   "drain-bound condition",
			mutate: func(candidate *Candidate) { candidate.TerminationGracePeriodCondition = nil },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := cloneRepairCandidateForTest(resolved)
			test.mutate(changed)
			if sameRepairResolution(resolved, changed) {
				t.Fatalf("expected changed %s to invalidate the scheduling result", test.name)
			}
		})
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

func cloneRepairCandidateForTest(candidate *Candidate) *Candidate {
	cloned := *candidate
	stateNode := *candidate.StateNode
	stateNode.Node = candidate.Node.DeepCopy()
	stateNode.NodeClaim = candidate.NodeClaim.DeepCopy()
	cloned.StateNode = &stateNode
	if candidate.TerminationGracePeriod != nil {
		drainBound := *candidate.TerminationGracePeriod
		cloned.TerminationGracePeriod = &drainBound
	}
	if candidate.TerminationGracePeriodCondition != nil {
		condition := *candidate.TerminationGracePeriodCondition
		cloned.TerminationGracePeriodCondition = &condition
	}
	return &cloned
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
