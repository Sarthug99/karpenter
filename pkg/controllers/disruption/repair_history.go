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
	"sync"
	"time"

	"github.com/patrickmn/go-cache"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

const (
	rebootHistoryWindow          = 24 * time.Hour
	rebootHistoryCleanupInterval = time.Hour
	rebootsBeforeReplacement     = 2
)

type rebootHistoryEntry struct {
	committedAt [rebootsBeforeReplacement]time.Time
	count       int
}

// RebootHistory counts up to rebootsBeforeReplacement committed reboots in a sliding window by NodeClaim UID.
type RebootHistory struct {
	mu     sync.Mutex
	clock  clock.Clock
	recent *cache.Cache
}

func NewRebootHistory() *RebootHistory {
	return newRebootHistory(clock.RealClock{})
}

func newRebootHistory(clk clock.Clock) *RebootHistory {
	return &RebootHistory{
		clock:  clk,
		recent: cache.New(rebootHistoryWindow, rebootHistoryCleanupInterval),
	}
}

// RecordCommittedReboot consumes one reboot attempt after a new lifecycle handoff commits.
// Callers must record each committed handoff exactly once; retries that observe an active lifecycle must not record it
// again.
func (h *RebootHistory) RecordCommittedReboot(nodeClaimUID types.UID) {
	h.mu.Lock()
	defer h.mu.Unlock()

	key := string(nodeClaimUID)
	now := h.clock.Now()
	entry := h.recentReboots(nodeClaimUID, now)
	if entry.count >= rebootsBeforeReplacement {
		return
	}
	entry.committedAt[entry.count] = now
	entry.count++
	h.recent.SetDefault(key, entry)
}

// Resolve applies active lifecycle state and recent reboot history to current eligible results, storing the resolved
// decision on the existing disruption candidate.
func (h *RebootHistory) Resolve(candidate *Candidate, activeRebootLifecycle bool, results []RepairResult) bool {
	if activeRebootLifecycle {
		clearRepairResolution(candidate)
		return false
	}
	resolvedResults, rebootEscalated := resolveRepairActions(h.committedReboots(candidate.NodeClaim.UID), results)
	if !resolveRepairCandidate(candidate, resolvedResults) {
		return false
	}
	candidate.RebootEscalated = rebootEscalated
	return true
}

func (h *RebootHistory) committedReboots(nodeClaimUID types.UID) int {
	return h.recentReboots(nodeClaimUID, h.clock.Now()).count
}

func (h *RebootHistory) recentReboots(nodeClaimUID types.UID, now time.Time) rebootHistoryEntry {
	value, ok := h.recent.Get(string(nodeClaimUID))
	if !ok {
		return rebootHistoryEntry{}
	}
	entry, _ := value.(rebootHistoryEntry)
	cutoff := now.Add(-rebootHistoryWindow)
	recent := rebootHistoryEntry{}
	for i := range entry.count {
		if entry.committedAt[i].After(cutoff) {
			recent.committedAt[recent.count] = entry.committedAt[i]
			recent.count++
		}
	}
	return recent
}

func resolveRepairActions(committedReboots int, results []RepairResult) ([]RepairResult, bool) {
	if len(results) == 0 || committedReboots < rebootsBeforeReplacement {
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
