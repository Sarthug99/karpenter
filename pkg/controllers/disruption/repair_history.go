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
	"slices"
	"time"

	"github.com/patrickmn/go-cache"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

const (
	rebootHistoryTTL             = 24 * time.Hour
	rebootHistoryCleanupInterval = time.Hour
)

// RebootHistory records recently committed reboots by NodeClaim UID. Entries expire automatically so a healthy
// NodeClaim can become eligible for another reboot after the cooldown.
type RebootHistory struct {
	recent *cache.Cache
}

func NewRebootHistory() *RebootHistory {
	return &RebootHistory{
		recent: cache.New(rebootHistoryTTL, rebootHistoryCleanupInterval),
	}
}

// RecordCommittedReboot starts the reboot cooldown after the lifecycle handoff commits.
func (h *RebootHistory) RecordCommittedReboot(nodeClaimUID types.UID) {
	h.recent.SetDefault(string(nodeClaimUID), struct{}{})
}

// Resolve applies active lifecycle state and recent reboot history to current eligible results, storing the resolved
// decision on the existing disruption candidate.
func (h *RebootHistory) Resolve(candidate *Candidate, activeRebootLifecycle bool, results []RepairResult) bool {
	if activeRebootLifecycle {
		clearRepairResolution(candidate)
		return false
	}
	_, recentlyRebooted := h.recent.Get(string(candidate.NodeClaim.UID))
	resolvedResults, rebootEscalated := resolveRepairActions(recentlyRebooted, results)
	if !resolveRepairCandidate(candidate, resolvedResults) {
		return false
	}
	candidate.RebootEscalated = rebootEscalated
	return true
}

func resolveRepairActions(recentlyRebooted bool, results []RepairResult) ([]RepairResult, bool) {
	if len(results) == 0 || !recentlyRebooted {
		return results, false
	}

	resolved := slices.Clone(results)
	rebootEscalated := false
	for i := range resolved {
		if resolved[i].Action == cloudprovider.RebootNode {
			resolved[i].Action = cloudprovider.ReplaceNode
			rebootEscalated = true
		}
	}
	return resolved, rebootEscalated
}
