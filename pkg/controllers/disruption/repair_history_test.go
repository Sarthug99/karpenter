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
	"sync"
	"testing"
	"time"

	"github.com/patrickmn/go-cache"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

func TestRebootHistoryEscalatesAfterTwoCommittedReboots(t *testing.T) {
	history := NewRebootHistory()
	candidate := repairCandidateForTest("nodeclaim-uid")
	oneMinute := time.Minute
	results := []RepairResult{
		repairResultForTest("AcceleratorReady", "XID48", cloudprovider.RebootNode, time.Unix(1, 0), &oneMinute),
	}

	if !history.Resolve(candidate, false, results) || candidate.Action != cloudprovider.RebootNode {
		t.Fatalf("expected the first repair to remain a reboot: %#v", candidate)
	}

	history.RecordCommittedReboot(candidate.NodeClaim.UID)
	if !history.Resolve(candidate, false, results) || candidate.Action != cloudprovider.RebootNode {
		t.Fatalf("expected the second repair to remain a reboot: %#v", candidate)
	}
	if candidate.RebootEscalated {
		t.Fatal("expected one committed reboot not to trigger escalation")
	}

	history.RecordCommittedReboot(candidate.NodeClaim.UID)
	if !history.Resolve(candidate, false, results) || candidate.Action != cloudprovider.ReplaceNode {
		t.Fatalf("expected two committed reboots to escalate to replacement: %#v", candidate)
	}
	if !candidate.RebootEscalated {
		t.Fatal("expected escalation to be recorded")
	}
	if results[0].Action != cloudprovider.RebootNode {
		t.Fatal("expected history resolution to leave matching results unchanged")
	}
}

func TestRebootHistoryResolveDoesNotRecordReboots(t *testing.T) {
	history := NewRebootHistory()
	candidate := repairCandidateForTest("nodeclaim-uid")
	results := []RepairResult{
		repairResultForTest("AcceleratorReady", "XID48", cloudprovider.RebootNode, time.Unix(1, 0), nil),
	}

	for range 3 {
		if !history.Resolve(candidate, false, results) || candidate.Action != cloudprovider.RebootNode {
			t.Fatalf("expected uncommitted resolution not to consume a reboot: %#v", candidate)
		}
	}
}

func TestRebootHistoryUsesNodeClaimUID(t *testing.T) {
	history := NewRebootHistory()
	results := []RepairResult{
		repairResultForTest("AcceleratorReady", "XID48", cloudprovider.RebootNode, time.Unix(1, 0), nil),
	}
	original := repairCandidateForTest("original-uid")
	history.RecordCommittedReboot(original.NodeClaim.UID)
	history.RecordCommittedReboot(original.NodeClaim.UID)

	successor := repairCandidateForTest("successor-uid")
	if !history.Resolve(successor, false, results) || successor.Action != cloudprovider.RebootNode {
		t.Fatalf("expected a successor NodeClaim to have independent history: %#v", successor)
	}
}

func TestRebootHistorySuppressesActiveLifecycle(t *testing.T) {
	history := NewRebootHistory()
	candidate := repairCandidateForTest("nodeclaim-uid")
	history.RecordCommittedReboot(candidate.NodeClaim.UID)
	history.RecordCommittedReboot(candidate.NodeClaim.UID)
	results := []RepairResult{
		repairResultForTest("StorageReady", "DiskFailure", cloudprovider.ReplaceNode, time.Unix(1, 0), nil),
	}

	if history.Resolve(candidate, true, results) {
		t.Fatal("expected an active reboot lifecycle to suppress repair")
	}
	if candidate.Action != "" {
		t.Fatalf("expected an active lifecycle to clear the repair decision: %#v", candidate)
	}
}

func TestRebootHistoryKeepsReplacementAndRequiresCurrentEvidence(t *testing.T) {
	history := NewRebootHistory()
	candidate := repairCandidateForTest("nodeclaim-uid")
	history.RecordCommittedReboot(candidate.NodeClaim.UID)
	history.RecordCommittedReboot(candidate.NodeClaim.UID)

	replacement := []RepairResult{
		repairResultForTest("StorageReady", "DiskFailure", cloudprovider.ReplaceNode, time.Unix(1, 0), nil),
	}
	if !history.Resolve(candidate, false, replacement) || candidate.Action != cloudprovider.ReplaceNode || candidate.RebootEscalated {
		t.Fatalf("expected replacement evidence to remain unchanged: %#v", candidate)
	}
	if history.Resolve(candidate, false, nil) {
		t.Fatal("expected history alone not to create repair work")
	}
}

func TestRebootHistoryExpires(t *testing.T) {
	candidate := repairCandidateForTest("nodeclaim-uid")
	history := &RebootHistory{
		recent: cache.NewFrom(rebootHistoryTTL, 0, map[string]cache.Item{
			string(candidate.NodeClaim.UID): {
				Object:     rebootsBeforeReplacement,
				Expiration: time.Now().Add(-time.Hour).UnixNano(),
			},
		}),
	}
	results := []RepairResult{
		repairResultForTest("AcceleratorReady", "XID48", cloudprovider.RebootNode, time.Unix(1, 0), nil),
	}

	if !history.Resolve(candidate, false, results) || candidate.Action != cloudprovider.RebootNode {
		t.Fatalf("expected reboot eligibility after the history entry expires: %#v", candidate)
	}
}

func TestRebootHistoryRefreshesExpirationAfterCommit(t *testing.T) {
	candidate := repairCandidateForTest("nodeclaim-uid")
	key := string(candidate.NodeClaim.UID)
	initialExpiration := time.Now().Add(time.Hour).UnixNano()
	history := &RebootHistory{
		recent: cache.NewFrom(rebootHistoryTTL, 0, map[string]cache.Item{
			key: {
				Object:     1,
				Expiration: initialExpiration,
			},
		}),
	}

	history.RecordCommittedReboot(candidate.NodeClaim.UID)

	item := history.recent.Items()[key]
	if item.Object != rebootsBeforeReplacement {
		t.Fatalf("expected %d committed reboots, got %v", rebootsBeforeReplacement, item.Object)
	}
	if item.Expiration <= initialExpiration {
		t.Fatalf("expected the second commit to refresh expiration beyond %v, got %v", initialExpiration, item.Expiration)
	}
}

func TestRebootHistoryBoundsConcurrentCommitsAtEscalationThreshold(t *testing.T) {
	history := NewRebootHistory()
	candidate := repairCandidateForTest("nodeclaim-uid")
	const commits = 32

	var wg sync.WaitGroup
	start := make(chan struct{})
	for range commits {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			history.RecordCommittedReboot(candidate.NodeClaim.UID)
		}()
	}
	close(start)
	wg.Wait()

	if got := history.committedReboots(candidate.NodeClaim.UID); got != rebootsBeforeReplacement {
		t.Fatalf("expected committed reboots to stop at %d, got %d", rebootsBeforeReplacement, got)
	}
}
