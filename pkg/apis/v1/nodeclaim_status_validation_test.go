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

package v1_test

import (
	"github.com/awslabs/operatorpkg/object"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	. "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/test"
)

var _ = Describe("NodeClaim Status Validation", func() {
	var nodeClaim *NodeClaim

	BeforeEach(func() {
		nodeClaim = &NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: test.RandomName()},
			Spec: NodeClaimSpec{
				NodeClassRef: &NodeClassReference{
					Group: "karpenter.test.sh",
					Kind:  "TestNodeClaim",
					Name:  "default",
				},
				Requirements: []NodeSelectorRequirementWithMinValues{
					{
						Key:      CapacityTypeLabelKey,
						Operator: v1.NodeSelectorOpExists,
					},
				},
			},
		}
		nodeClaim.SetGroupVersionKind(object.GVK(nodeClaim))
		Expect(env.Client.Create(ctx, nodeClaim)).To(Succeed())
	})

	It("should allow reboot repair attempts", func() {
		nodeClaim.Status.RepairAttempt = repairAttemptStatus(RepairAttemptActionRebootNode)
		Expect(env.Client.Status().Update(ctx, nodeClaim)).To(Succeed())
	})

	It("should reject replacement repair attempts", func() {
		nodeClaim.Status.RepairAttempt = repairAttemptStatus(RepairAttemptAction(cloudprovider.ReplaceNode))
		Expect(env.Client.Status().Update(ctx, nodeClaim)).ToNot(Succeed())
	})
})

func repairAttemptStatus(action RepairAttemptAction) *RepairAttemptStatus {
	return &RepairAttemptStatus{
		Action:                 action,
		OperationID:            "operation-id",
		NodeUID:                types.UID("node-uid"),
		CommittedAt:            metav1.Now(),
		DrivingConditionType:   v1.NodeReady,
		DrivingConditionStatus: v1.ConditionFalse,
		DrivingReason:          "Unhealthy",
	}
}
