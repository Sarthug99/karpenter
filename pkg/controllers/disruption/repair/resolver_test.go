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
	"encoding/json"
	"slices"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/node/health"
)

var _ = Describe("Resolver", func() {
	Describe("ResolveActions", func() {
		var results []Result

		BeforeEach(func() {
			results = []Result{
				repairResult("AcceleratorReady", corev1.ConditionFalse, "XID48", cloudprovider.RebootNode, time.Unix(1, 0), nil),
				repairResult("StorageReady", corev1.ConditionFalse, "DiskFailure", cloudprovider.ReplaceNode, time.Unix(2, 0), nil),
			}
		})

		It("keeps current actions when no attempt exists", func() {
			Expect(ResolveActions(results, nil)).To(Equal(results))
		})

		It("suppresses results while an attempt is unresolved", func() {
			Expect(ResolveActions(results, &v1.RepairAttemptStatus{})).To(BeNil())
		})

		It("escalates reboot results after an attempt resolves without mutating the input", func() {
			resolvedAt := metav1.NewTime(time.Unix(3, 0))
			resolved := ResolveActions(results, &v1.RepairAttemptStatus{ResolvedAt: &resolvedAt})
			expected := slices.Clone(results)
			expected[0].Action = cloudprovider.ReplaceNode

			Expect(resolved).To(Equal(expected))
			Expect(results[0].Action).To(Equal(cloudprovider.RebootNode))
		})

		It("returns no results when there is no current eligible evidence", func() {
			resolvedAt := metav1.NewTime(time.Unix(3, 0))
			Expect(ResolveActions(nil, &v1.RepairAttemptStatus{ResolvedAt: &resolvedAt})).To(BeNil())
		})
	})

	Describe("ResolveCandidate", func() {
		It("combines all results deterministically", func() {
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: types.UID("node-uid")}}
			nodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "nodeclaim", UID: types.UID("nodeclaim-uid")}}
			terminationGracePeriod := time.Minute
			twoMinutes := 2 * time.Minute
			results := []Result{
				repairResult("AcceleratorReady", corev1.ConditionFalse, "XID48", cloudprovider.RebootNode, time.Unix(1, 0), &terminationGracePeriod),
				repairResult("StorageReady", corev1.ConditionFalse, "DiskFailure", cloudprovider.ReplaceNode, time.Unix(2, 0), nil),
				repairResult("NetworkingReady", corev1.ConditionFalse, "LinkDown", cloudprovider.ReplaceNode, time.Unix(3, 0), &twoMinutes),
			}

			candidate := ResolveCandidate(node, nodeClaim, results)
			Expect(candidate.NodeName).To(Equal("node"))
			Expect(candidate.NodeUID).To(Equal(types.UID("node-uid")))
			Expect(candidate.NodeClaimName).To(Equal("nodeclaim"))
			Expect(candidate.NodeClaimUID).To(Equal(types.UID("nodeclaim-uid")))
			Expect(candidate.Action).To(Equal(cloudprovider.ReplaceNode))
			Expect(candidate.EligibleAt).To(Equal(time.Unix(2, 0)))
			Expect(candidate.DrivingCondition).To(Equal(Condition{
				Type: corev1.NodeConditionType("StorageReady"), Status: corev1.ConditionFalse, Reason: "DiskFailure",
			}))
			Expect(candidate.TerminationGracePeriod).NotTo(BeNil())
			Expect(*candidate.TerminationGracePeriod).To(Equal(time.Minute))
			Expect(candidate.TerminationGracePeriodCondition).To(Equal(&Condition{
				Type: corev1.NodeConditionType("AcceleratorReady"), Status: corev1.ConditionFalse, Reason: "XID48",
			}))

			for _, order := range [][]int{
				{0, 2, 1},
				{1, 0, 2},
				{1, 2, 0},
				{2, 0, 1},
				{2, 1, 0},
			} {
				permuted := []Result{results[order[0]], results[order[1]], results[order[2]]}
				Expect(ResolveCandidate(node, nodeClaim, permuted)).To(Equal(candidate), "order %v", order)
			}

			terminationGracePeriod = 10 * time.Minute
			Expect(*candidate.TerminationGracePeriod).To(Equal(time.Minute))
		})

		It("returns no candidate when there are no results", func() {
			Expect(ResolveCandidate(&corev1.Node{}, &v1.NodeClaim{}, nil)).To(BeNil())
		})

		DescribeTable("breaks driving-result ties deterministically",
			func(results []Result, expected Condition) {
				candidate := ResolveCandidate(&corev1.Node{}, &v1.NodeClaim{}, results)
				Expect(candidate.DrivingCondition).To(Equal(expected))
				Expect(candidate.TerminationGracePeriod).To(BeNil())
				Expect(candidate.TerminationGracePeriodCondition).To(BeNil())
			},
			Entry("by condition type",
				[]Result{
					repairResult("StorageReady", corev1.ConditionFalse, "A", cloudprovider.ReplaceNode, time.Unix(1, 0), nil),
					repairResult("AcceleratorReady", corev1.ConditionFalse, "Z", cloudprovider.ReplaceNode, time.Unix(1, 0), nil),
				},
				Condition{Type: "AcceleratorReady", Status: corev1.ConditionFalse, Reason: "Z"},
			),
			Entry("by condition status",
				[]Result{
					repairResult("StorageReady", corev1.ConditionTrue, "A", cloudprovider.ReplaceNode, time.Unix(1, 0), nil),
					repairResult("StorageReady", corev1.ConditionFalse, "Z", cloudprovider.ReplaceNode, time.Unix(1, 0), nil),
				},
				Condition{Type: "StorageReady", Status: corev1.ConditionFalse, Reason: "Z"},
			),
			Entry("by condition reason",
				[]Result{
					repairResult("StorageReady", corev1.ConditionFalse, "B", cloudprovider.ReplaceNode, time.Unix(1, 0), nil),
					repairResult("StorageReady", corev1.ConditionFalse, "A", cloudprovider.ReplaceNode, time.Unix(1, 0), nil),
				},
				Condition{Type: "StorageReady", Status: corev1.ConditionFalse, Reason: "A"},
			),
		)

		It("preserves a zero termination grace period", func() {
			zero := time.Duration(0)
			oneMinute := time.Minute
			results := []Result{
				repairResult("AcceleratorReady", corev1.ConditionFalse, "XID48", cloudprovider.RebootNode, time.Unix(1, 0), &oneMinute),
				repairResult("StorageReady", corev1.ConditionFalse, "Transient", cloudprovider.RebootNode, time.Unix(2, 0), &zero),
			}

			candidate := ResolveCandidate(&corev1.Node{}, &v1.NodeClaim{}, results)
			Expect(candidate.Action).To(Equal(cloudprovider.RebootNode))
			Expect(candidate.DrivingCondition.Type).To(Equal(corev1.NodeConditionType("AcceleratorReady")))
			Expect(candidate.TerminationGracePeriod).NotTo(BeNil())
			Expect(*candidate.TerminationGracePeriod).To(BeZero())
			Expect(candidate.TerminationGracePeriodCondition.Type).To(Equal(corev1.NodeConditionType("StorageReady")))

			attempt := NewRepairAttempt(candidate, "operation-id", time.Unix(3, 0))
			Expect(attempt.TerminationGracePeriod).NotTo(BeNil())
			Expect(attempt.TerminationGracePeriod.Duration).To(BeZero())
		})

		It("selects the drain-bound contributor deterministically", func() {
			eligibleAt := time.Unix(1, 0)
			firstGracePeriod := time.Minute
			secondGracePeriod := time.Minute
			results := []Result{
				repairResult("StorageReady", corev1.ConditionFalse, "DiskFailure", cloudprovider.ReplaceNode, eligibleAt, &firstGracePeriod),
				repairResult("AcceleratorReady", corev1.ConditionFalse, "XID48", cloudprovider.RebootNode, eligibleAt, &secondGracePeriod),
			}

			candidate := ResolveCandidate(&corev1.Node{}, &v1.NodeClaim{}, results)
			Expect(candidate.TerminationGracePeriodCondition).To(Equal(&Condition{
				Type: corev1.NodeConditionType("AcceleratorReady"), Status: corev1.ConditionFalse, Reason: "XID48",
			}))

			reversed := slices.Clone(results)
			slices.Reverse(reversed)
			Expect(ResolveCandidate(&corev1.Node{}, &v1.NodeClaim{}, reversed)).To(Equal(candidate))
		})
	})

	Describe("NewRepairAttempt", func() {
		var candidate *Candidate
		var committedAt time.Time
		var terminationGracePeriod time.Duration

		BeforeEach(func() {
			committedAt = time.Unix(10, 0)
			terminationGracePeriod = 2 * time.Minute
			candidate = &Candidate{
				NodeUID: types.UID("node-uid"),
				Action:  cloudprovider.RebootNode,
				DrivingCondition: Condition{
					Type: corev1.NodeConditionType("AcceleratorReady"), Status: corev1.ConditionFalse, Reason: "XID48",
				},
				TerminationGracePeriod: &terminationGracePeriod,
			}
		})

		It("snapshots an admitted reboot candidate", func() {
			attempt := NewRepairAttempt(candidate, "operation-id", committedAt)
			Expect(attempt).To(Equal(&v1.RepairAttemptStatus{
				Action:                 v1.RepairAttemptActionRebootNode,
				OperationID:            "operation-id",
				NodeUID:                types.UID("node-uid"),
				CommittedAt:            metav1.NewTime(committedAt),
				DrivingConditionType:   corev1.NodeConditionType("AcceleratorReady"),
				DrivingConditionStatus: corev1.ConditionFalse,
				DrivingReason:          "XID48",
				TerminationGracePeriod: &metav1.Duration{Duration: 2 * time.Minute},
			}))

			terminationGracePeriod = 3 * time.Minute
			Expect(attempt.TerminationGracePeriod.Duration).To(Equal(2 * time.Minute))
		})

		It("does not create attempts for absent or replacement candidates", func() {
			Expect(NewRepairAttempt(nil, "operation-id", committedAt)).To(BeNil())
			candidate.Action = cloudprovider.ReplaceNode
			Expect(NewRepairAttempt(candidate, "operation-id", committedAt)).To(BeNil())
		})

		It("preserves an unbounded drain window", func() {
			candidate.TerminationGracePeriod = nil
			Expect(NewRepairAttempt(candidate, "operation-id", committedAt).TerminationGracePeriod).To(BeNil())
		})

		It("serializes an empty driving reason", func() {
			candidate.DrivingCondition.Reason = ""
			attempt := NewRepairAttempt(candidate, "operation-id", committedAt)

			data, err := json.Marshal(attempt)
			Expect(err).NotTo(HaveOccurred())
			fields := map[string]json.RawMessage{}
			Expect(json.Unmarshal(data, &fields)).To(Succeed())
			Expect(fields).To(HaveKeyWithValue("drivingReason", json.RawMessage(`""`)))
		})
	})
})

func repairResult(
	conditionType corev1.NodeConditionType,
	conditionStatus corev1.ConditionStatus,
	reason string,
	action cloudprovider.RepairAction,
	eligibleAt time.Time,
	terminationGracePeriod *time.Duration,
) Result {
	return Result{
		RepairPolicyResult: health.RepairPolicyResult{
			ConditionType: conditionType, ConditionStatus: conditionStatus, Reason: reason, Action: action, EligibleAt: eligibleAt,
		},
		TerminationGracePeriod: terminationGracePeriod,
	}
}
