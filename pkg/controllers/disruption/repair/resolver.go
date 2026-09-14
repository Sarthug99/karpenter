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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

// Result combines one eligible reason-matching result with its resolved drain bound.
type Result struct {
	ConditionType   corev1.NodeConditionType
	ConditionStatus corev1.ConditionStatus
	Reason          string
	Action          cloudprovider.RepairAction
	EligibleAt      time.Time

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
	RebootEscalated  bool

	TerminationGracePeriod          *time.Duration
	TerminationGracePeriodCondition *Condition
}

// LogValues returns structured values describing the resolved candidate.
func (c *Candidate) LogValues() []any {
	values := []any{
		"action", c.Action,
		"eligible-at", c.EligibleAt,
		"condition", c.DrivingCondition.Type,
		"status", c.DrivingCondition.Status,
		"reason", c.DrivingCondition.Reason,
		"reboot-escalated", c.RebootEscalated,
	}
	if c.TerminationGracePeriod != nil {
		values = append(values, "termination-grace-period", *c.TerminationGracePeriod)
	}
	if c.TerminationGracePeriodCondition != nil && *c.TerminationGracePeriodCondition != c.DrivingCondition {
		values = append(values,
			"termination-grace-period-condition", c.TerminationGracePeriodCondition.Type,
			"termination-grace-period-status", c.TerminationGracePeriodCondition.Status,
			"termination-grace-period-reason", c.TerminationGracePeriodCondition.Reason,
		)
	}
	return values
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
		if result.Action.IsMoreDisruptiveThan(action) {
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
