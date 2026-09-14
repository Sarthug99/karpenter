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
	"fmt"
	"slices"
	"sync"

	"github.com/awslabs/operatorpkg/serrors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

type rebootHistoryState uint8

const (
	rebootHistoryAbsent rebootHistoryState = iota
	rebootHistoryActive
	rebootHistoryResolved
)

// RebootHistory stores process-local reboot history by NodeClaim UID.
type RebootHistory struct {
	mu      sync.Mutex
	entries map[types.UID]rebootHistoryState
	locks   map[types.UID]*rebootHistoryLock
}

type rebootHistoryLock struct {
	mu         sync.Mutex
	references int
}

// NewRebootHistory constructs empty process-local reboot history.
func NewRebootHistory() *RebootHistory {
	return &RebootHistory{
		entries: make(map[types.UID]rebootHistoryState),
		locks:   make(map[types.UID]*rebootHistoryLock),
	}
}

// MarkResolved marks a recorded reboot as resolved after observing a terminal lifecycle.
func (h *RebootHistory) MarkResolved(nodeClaimUID types.UID) {
	unlock := h.lock(nodeClaimUID)
	defer unlock()

	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.entries[nodeClaimUID]; ok {
		h.entries[nodeClaimUID] = rebootHistoryResolved
	}
}

// Delete removes history after the NodeClaim no longer exists.
func (h *RebootHistory) Delete(nodeClaimUID types.UID) {
	unlock := h.lock(nodeClaimUID)
	defer unlock()

	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.entries, nodeClaimUID)
}

// Admit applies reboot history to current eligible results and serializes final
// admission for one NodeClaim with history creation and resolution. The callback
// runs while the NodeClaim lock is held and returns true only after the candidate
// crosses its commitment boundary. It must not call Admit, MarkResolved, or Delete
// for the same NodeClaim UID.
func (h *RebootHistory) Admit(
	node *corev1.Node,
	nodeClaim *v1.NodeClaim,
	activeRebootLifecycle bool,
	results []Result,
	commit func(*Candidate) (bool, error),
) error {
	nodeClaimUID := nodeClaim.UID
	unlock := h.lock(nodeClaimUID)
	defer unlock()

	h.mu.Lock()
	state := h.entries[nodeClaimUID]
	h.mu.Unlock()

	resolvedResults, rebootEscalated := resolveActions(state, activeRebootLifecycle, results)
	candidate := ResolveCandidate(node, nodeClaim, resolvedResults)
	if candidate == nil {
		return nil
	}
	candidate.RebootEscalated = rebootEscalated
	if commit == nil {
		return serrors.Wrap(fmt.Errorf("committing repair action, callback is nil"), "NodeClaim", klog.KObj(nodeClaim))
	}
	action := candidate.Action
	committed, err := commit(candidate)
	if committed && action == cloudprovider.RebootNode {
		h.mu.Lock()
		defer h.mu.Unlock()
		if _, ok := h.entries[nodeClaimUID]; !ok {
			h.entries[nodeClaimUID] = rebootHistoryActive
		}
	}
	return err
}

func (h *RebootHistory) lock(nodeClaimUID types.UID) func() {
	h.mu.Lock()
	lock, ok := h.locks[nodeClaimUID]
	if !ok {
		lock = &rebootHistoryLock{}
		h.locks[nodeClaimUID] = lock
	}
	lock.references++
	h.mu.Unlock()

	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()

		h.mu.Lock()
		defer h.mu.Unlock()
		lock.references--
		if lock.references == 0 {
			delete(h.locks, nodeClaimUID)
		}
	}
}

func resolveActions(state rebootHistoryState, activeRebootLifecycle bool, results []Result) ([]Result, bool) {
	if len(results) == 0 {
		return nil, false
	}
	if activeRebootLifecycle || state == rebootHistoryActive {
		return nil, false
	}
	if state != rebootHistoryResolved {
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
