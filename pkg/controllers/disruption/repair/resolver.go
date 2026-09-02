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

package repair

import (
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/node/health"
)

// Result combines one eligible reason-matching result with its resolved drain bound.
type Result struct {
	health.RepairPolicyResult
	TerminationGracePeriod *time.Duration
}

// Condition identifies the current health evidence carried by a repair result.
type Condition struct {
	Type   corev1.NodeConditionType
	Status corev1.ConditionStatus
	Reason string
}

// Candidate is the single repair recommendation selected for one Node and NodeClaim.
type Candidate struct {
	NodeName      string
	NodeUID       types.UID
	NodeClaimName string
	NodeClaimUID  types.UID

	Action           cloudprovider.RepairAction
	EligibleAt       time.Time
	DrivingCondition Condition

	TerminationGracePeriod          *time.Duration
	TerminationGracePeriodCondition *Condition
}

// ResolveActions applies durable attempt history to current eligible results.
func ResolveActions(results []Result, attempt *v1.RepairAttemptStatus) []Result {
	if len(results) == 0 || (attempt != nil && attempt.ResolvedAt == nil) {
		return nil
	}
	if attempt == nil {
		return results
	}
	resolved := slices.Clone(results)
	for i := range resolved {
		if resolved[i].Action == cloudprovider.RebootNode {
			resolved[i].Action = cloudprovider.ReplaceNode
		}
	}
	return resolved
}

// ResolveCandidate deterministically combines action-resolved results.
func ResolveCandidate(node *corev1.Node, nodeClaim *v1.NodeClaim, results []Result) *Candidate {
	if len(results) == 0 {
		return nil
	}

	action := selectAction(results)
	driving := selectDrivingResult(results, action)
	drainBoundResult := selectDrainBoundResult(results)

	candidate := &Candidate{
		NodeName:      node.Name,
		NodeUID:       node.UID,
		NodeClaimName: nodeClaim.Name,
		NodeClaimUID:  nodeClaim.UID,
		Action:        action,
		EligibleAt:    driving.EligibleAt,
		DrivingCondition: Condition{
			Type:   driving.ConditionType,
			Status: driving.ConditionStatus,
			Reason: driving.Reason,
		},
	}
	if drainBoundResult != nil {
		duration := *drainBoundResult.TerminationGracePeriod
		candidate.TerminationGracePeriod = &duration
		candidate.TerminationGracePeriodCondition = &Condition{
			Type:   drainBoundResult.ConditionType,
			Status: drainBoundResult.ConditionStatus,
			Reason: drainBoundResult.Reason,
		}
	}
	return candidate
}

func selectAction(results []Result) cloudprovider.RepairAction {
	action := results[0].Action
	for _, result := range results[1:] {
		if actionRank(result.Action) > actionRank(action) {
			action = result.Action
		}
	}
	return action
}

func selectDrivingResult(results []Result, action cloudprovider.RepairAction) *Result {
	var driving *Result
	for i := range results {
		if results[i].Action == action && (driving == nil || resultLess(results[i], *driving)) {
			driving = &results[i]
		}
	}
	return driving
}

func selectDrainBoundResult(results []Result) *Result {
	var drainBound *Result
	for i := range results {
		if results[i].TerminationGracePeriod == nil {
			continue
		}
		if drainBound == nil ||
			*results[i].TerminationGracePeriod < *drainBound.TerminationGracePeriod ||
			*results[i].TerminationGracePeriod == *drainBound.TerminationGracePeriod && resultLess(results[i], *drainBound) {
			drainBound = &results[i]
		}
	}
	return drainBound
}

// NewRepairAttempt creates the durable repair attempt for an admitted reboot candidate.
// It returns nil for absent and replacement candidates.
func NewRepairAttempt(candidate *Candidate, operationID string, committedAt time.Time) *v1.RepairAttemptStatus {
	if candidate == nil || candidate.Action != cloudprovider.RebootNode {
		return nil
	}
	attempt := &v1.RepairAttemptStatus{
		Action:                 v1.RepairAttemptActionRebootNode,
		OperationID:            operationID,
		NodeUID:                candidate.NodeUID,
		CommittedAt:            metav1.NewTime(committedAt),
		DrivingConditionType:   candidate.DrivingCondition.Type,
		DrivingConditionStatus: candidate.DrivingCondition.Status,
		DrivingReason:          candidate.DrivingCondition.Reason,
	}
	if candidate.TerminationGracePeriod != nil {
		attempt.TerminationGracePeriod = &metav1.Duration{Duration: *candidate.TerminationGracePeriod}
	}
	return attempt
}

func resultLess(lhs, rhs Result) bool {
	if !lhs.EligibleAt.Equal(rhs.EligibleAt) {
		return lhs.EligibleAt.Before(rhs.EligibleAt)
	}
	if lhs.ConditionType != rhs.ConditionType {
		return lhs.ConditionType < rhs.ConditionType
	}
	if lhs.ConditionStatus != rhs.ConditionStatus {
		return lhs.ConditionStatus < rhs.ConditionStatus
	}
	return lhs.Reason < rhs.Reason
}

func actionRank(action cloudprovider.RepairAction) int {
	switch action {
	case cloudprovider.ReplaceNode:
		return 1
	case cloudprovider.RebootNode:
		return 0
	default:
		return -1
	}
}
