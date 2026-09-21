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
	"time"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

// RepairResult is the eligible reason-matching result for one current NodeCondition.
type RepairResult struct {
	ConditionType          corev1.NodeConditionType
	ConditionStatus        corev1.ConditionStatus
	Reason                 string
	Action                 cloudprovider.RepairAction
	EligibleAt             time.Time
	TerminationGracePeriod *time.Duration
}

// resolveRepairCandidate combines all eligible results and stores the decision on the existing disruption candidate.
func resolveRepairCandidate(candidate *Candidate, results []RepairResult) bool {
	clearRepairResolution(candidate)
	if len(results) == 0 {
		return false
	}

	action := selectRepairAction(results)
	driving := selectDrivingRepairResult(results, action)
	drainBound := selectDrainBoundRepairResult(results)

	candidate.Action = action
	candidate.RepairEligibleAt = driving.EligibleAt
	candidate.RepairCondition = repairConditionFor(*driving)
	if drainBound != nil {
		candidate.TerminationGracePeriod = cloneRepairDuration(drainBound.TerminationGracePeriod)
		condition := repairConditionFor(*drainBound)
		candidate.TerminationGracePeriodCondition = &condition
	}
	return true
}

func clearRepairResolution(candidate *Candidate) {
	candidate.Action = ""
	candidate.RepairEligibleAt = time.Time{}
	candidate.RepairCondition = RepairEvidence{}
	candidate.RebootEscalated = false
	candidate.TerminationGracePeriod = nil
	candidate.TerminationGracePeriodCondition = nil
}

func selectRepairAction(results []RepairResult) cloudprovider.RepairAction {
	action := results[0].Action
	for _, result := range results[1:] {
		if result.Action.IsMoreDisruptiveThan(action) {
			action = result.Action
		}
	}
	return action
}

func selectDrivingRepairResult(results []RepairResult, action cloudprovider.RepairAction) *RepairResult {
	var driving *RepairResult
	for i := range results {
		if results[i].Action == action && (driving == nil || repairResultLess(results[i], *driving)) {
			driving = &results[i]
		}
	}
	return driving
}

func selectDrainBoundRepairResult(results []RepairResult) *RepairResult {
	var drainBound *RepairResult
	for i := range results {
		if results[i].TerminationGracePeriod == nil {
			continue
		}
		if drainBound == nil ||
			*results[i].TerminationGracePeriod < *drainBound.TerminationGracePeriod ||
			*results[i].TerminationGracePeriod == *drainBound.TerminationGracePeriod && repairResultLess(results[i], *drainBound) {
			drainBound = &results[i]
		}
	}
	return drainBound
}

func repairResultLess(lhs, rhs RepairResult) bool {
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

func repairConditionFor(result RepairResult) RepairEvidence {
	return RepairEvidence{
		Type:   result.ConditionType,
		Status: result.ConditionStatus,
		Reason: result.Reason,
	}
}

func cloneRepairDuration(duration *time.Duration) *time.Duration {
	if duration == nil {
		return nil
	}
	cloned := *duration
	return &cloned
}

func repairLogValues(candidate *Candidate) []any {
	values := []any{
		"action", candidate.Action,
		"eligible-at", candidate.RepairEligibleAt,
		"condition", candidate.RepairCondition.Type,
		"status", candidate.RepairCondition.Status,
		"reason", candidate.RepairCondition.Reason,
		"reboot-escalated", candidate.RebootEscalated,
	}
	if candidate.TerminationGracePeriod != nil {
		values = append(values, "termination-grace-period", *candidate.TerminationGracePeriod)
	}
	if candidate.TerminationGracePeriodCondition != nil && *candidate.TerminationGracePeriodCondition != candidate.RepairCondition {
		values = append(values,
			"termination-grace-period-condition", candidate.TerminationGracePeriodCondition.Type,
			"termination-grace-period-status", candidate.TerminationGracePeriodCondition.Status,
			"termination-grace-period-reason", candidate.TerminationGracePeriodCondition.Reason,
		)
	}
	return values
}

func sameRepairResolution(previous, current *Candidate) bool {
	if previous == nil || current == nil {
		return previous == current
	}
	return sameRepairTarget(previous, current) &&
		previous.Action == current.Action &&
		previous.RepairEligibleAt.Equal(current.RepairEligibleAt) &&
		previous.RepairCondition == current.RepairCondition &&
		previous.RebootEscalated == current.RebootEscalated &&
		equalPointers(previous.TerminationGracePeriod, current.TerminationGracePeriod) &&
		equalPointers(previous.TerminationGracePeriodCondition, current.TerminationGracePeriodCondition)
}

func sameRepairTarget(previous, current *Candidate) bool {
	return previous.Node.UID == current.Node.UID &&
		previous.NodeClaim.UID == current.NodeClaim.UID
}

func equalPointers[T comparable](left, right *T) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}
