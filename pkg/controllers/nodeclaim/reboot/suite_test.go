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

package reboot_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination/terminator"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/reboot"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var ctx context.Context
var rebootController *reboot.Controller
var env *test.Environment
var cloudProvider *fake.CloudProvider
var recorder *test.EventRecorder
var queue *terminator.Queue

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Reboot")
}

var _ = BeforeSuite(func() {
	env = test.NewEnvironment(
		test.WithCRDs(apis.CRDs...),
		test.WithCRDs(v1alpha1.CRDs...),
		test.WithFieldIndexers(test.NodeClaimProviderIDFieldIndexer(ctx), test.NodeProviderIDFieldIndexer(ctx), test.VolumeAttachmentFieldIndexer(ctx)),
	)
	cloudProvider = fake.NewCloudProvider()
	recorder = test.NewEventRecorder()
	queue = terminator.NewQueue(env.Clock, env.Client, recorder)
	rebootController = reboot.NewController(env.Clock, env.Client, cloudProvider, terminator.NewTerminator(env.Clock, env.Client, queue, recorder), recorder)
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = Describe("Reboot Lifecycle", func() {
	var nodePool *v1.NodePool
	var nodeClaim *v1.NodeClaim
	var node *corev1.Node

	BeforeEach(func() {
		env.Clock.SetTime(time.Now())
		cloudProvider.Reset()
		recorder.Reset()
		reboot.RebootsTotal.Reset()
		reboot.RebootDurationSeconds.Reset()
		reboot.RebootRecoveryDurationSeconds.Reset()

		nodePool = test.NodePool()
		nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name},
		}})
		node.Labels[v1.NodePoolLabelKey] = nodePool.Name
		node.Labels[v1.NodeInitializedLabelKey] = "true"
		node.Status.NodeInfo.BootID = "boot-1"
		nodeClaim.StatusConditions().SetTrueWithReason(v1.ConditionTypeRebooting, v1.RebootReasonRequested, "reboot requested")
	})

	AfterEach(func() {
		ExpectCleanedUp(ctx, env.Client)
	})

	hasRebootTaint := func(n *corev1.Node) bool {
		return lo.ContainsBy(n.Spec.Taints, func(t corev1.Taint) bool { return t.MatchTaint(&v1.RebootingNoScheduleTaint) })
	}

	Context("RebootRequested", func() {
		It("issues the reboot and transitions to RebootIssued", func() {
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
			ExpectObjectReconciled(ctx, env.Client, rebootController, nodeClaim)

			Expect(cloudProvider.RebootCalls).To(HaveLen(1))

			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			cond := nodeClaim.StatusConditions().Get(v1.ConditionTypeRebooting)
			Expect(cond.IsTrue()).To(BeTrue())
			Expect(cond.Reason).To(Equal(v1.RebootReasonIssued))
			Expect(nodeClaim.Annotations).To(HaveKeyWithValue(v1.RebootPreBootIDAnnotationKey, "boot-1"))
			Expect(cloudProvider.RebootOperationIDs[0]).ToNot(BeEmpty())
			// Initialization is invalidated by a committed reboot until the node re-initializes.
			Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeInitialized).Status).To(Equal(metav1.ConditionUnknown))

			node = ExpectExists(ctx, env.Client, node)
			Expect(hasRebootTaint(node)).To(BeTrue())
			Expect(node.Labels).ToNot(HaveKey(v1.NodeInitializedLabelKey))

			Expect(recorder.Calls(events.RebootRequested)).To(BeNumerically(">=", 1))
			Expect(recorder.Calls(events.RebootIssued)).To(Equal(1))
		})

		It("re-issues on a subsequent reboot of the same NodeClaim (no stale-state false success)", func() {
			// Episode 1: request -> issue -> succeed.
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
			ExpectObjectReconciled(ctx, env.Client, rebootController, nodeClaim)
			Expect(cloudProvider.RebootCalls).To(HaveLen(1))
			firstID := cloudProvider.RebootOperationIDs[0]

			node = ExpectExists(ctx, env.Client, node)
			node.Status.NodeInfo.BootID = "boot-2"
			node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
			ExpectApplied(ctx, env.Client, node)
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			ExpectObjectReconciled(ctx, env.Client, rebootController, nodeClaim)
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeRebooting).Reason).To(Equal(v1.RebootReasonSucceeded))
			// Episode-scoped state is cleared at terminal, so it can't leak into the next reboot.
			Expect(nodeClaim.Annotations).ToNot(HaveKey(v1.RebootPreBootIDAnnotationKey))

			// Episode 2: the node has re-initialized and the consumer re-requests a reboot on the same NodeClaim.
			node = ExpectExists(ctx, env.Client, node)
			node.Status.NodeInfo.BootID = "boot-2" // current boot; a stale pre-boot-id would falsely "prove" a reboot
			ExpectApplied(ctx, env.Client, node)
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeInitialized)
			nodeClaim.StatusConditions().SetTrueWithReason(v1.ConditionTypeRebooting, v1.RebootReasonRequested, "reboot requested again")
			ExpectApplied(ctx, env.Client, nodeClaim)
			ExpectObjectReconciled(ctx, env.Client, rebootController, nodeClaim)

			// The second episode must actually issue — not short-circuit to success on stale state.
			Expect(cloudProvider.RebootCalls).To(HaveLen(2))
			Expect(cloudProvider.RebootOperationIDs[1]).ToNot(Equal(firstID)) // distinct per episode
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeRebooting).Reason).To(Equal(v1.RebootReasonIssued))
			Expect(nodeClaim.Annotations).To(HaveKeyWithValue(v1.RebootPreBootIDAnnotationKey, "boot-2"))
		})

		It("fails with provider_error when the provider does not implement reboot", func() {
			cloudProvider.NextRebootErr = cloudprovider.NewNodeRebootNotImplementedError()
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
			ExpectObjectReconciled(ctx, env.Client, rebootController, nodeClaim)

			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			cond := nodeClaim.StatusConditions().Get(v1.ConditionTypeRebooting)
			Expect(cond.IsFalse()).To(BeTrue())
			Expect(cond.Reason).To(Equal(v1.RebootReasonFailed))

			node = ExpectExists(ctx, env.Client, node)
			Expect(hasRebootTaint(node)).To(BeFalse())

			Expect(recorder.Calls(events.RebootFailed)).To(Equal(1))
			ExpectMetricCounterValue(reboot.RebootsTotal, 1, map[string]string{"result": "provider_error"})
		})

		It("retries on a transient provider error, staying in RebootRequested", func() {
			cloudProvider.NextRebootErr = fmt.Errorf("throttled")
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
			_ = ExpectObjectReconcileFailed(ctx, env.Client, rebootController, nodeClaim)

			Expect(cloudProvider.RebootCalls).To(BeEmpty())
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			cond := nodeClaim.StatusConditions().Get(v1.ConditionTypeRebooting)
			Expect(cond.IsTrue()).To(BeTrue())
			Expect(cond.Reason).To(Equal(v1.RebootReasonRequested))
			Expect(recorder.Calls(events.RebootIssued)).To(Equal(0))
		})

		It("skips issuing when the boot already changed after recording issuing state (restart safety)", func() {
			nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, map[string]string{
				v1.RebootPreBootIDAnnotationKey: "boot-1",
			})
			node.Status.NodeInfo.BootID = "boot-2"
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
			ExpectObjectReconciled(ctx, env.Client, rebootController, nodeClaim)

			Expect(cloudProvider.RebootCalls).To(BeEmpty())
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeRebooting).Reason).To(Equal(v1.RebootReasonIssued))
		})

		It("waits to issue while pods still need draining", func() {
			nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, map[string]string{v1.RebootDrainGracePeriodAnnotationKey: "10m"})
			pod := test.Pod(test.PodOptions{NodeName: node.Name})
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node, pod)
			result := ExpectObjectReconciled(ctx, env.Client, rebootController, nodeClaim)

			Expect(cloudProvider.RebootCalls).To(BeEmpty())
			Expect(result.RequeueAfter).To(BeNumerically(">", 0))
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeRebooting).Reason).To(Equal(v1.RebootReasonRequested))
		})

		It("fails with provider_error when issuance does not succeed within the issuance timeout", func() {
			// Simulate a reboot stuck in the post-drain provider-accept loop.
			nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, map[string]string{
				v1.RebootPreBootIDAnnotationKey:         "boot-1",
				v1.RebootIssuanceStartedAtAnnotationKey: env.Clock.Now().Format(time.RFC3339),
			})
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
			env.Clock.Step(6 * time.Minute) // past the 5m issuance timeout
			ExpectObjectReconciled(ctx, env.Client, rebootController, nodeClaim)

			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			cond := nodeClaim.StatusConditions().Get(v1.ConditionTypeRebooting)
			Expect(cond.IsFalse()).To(BeTrue())
			Expect(cond.Reason).To(Equal(v1.RebootReasonFailed))
			Expect(cloudProvider.RebootCalls).To(BeEmpty()) // failed at the deadline, never re-issued
			Expect(recorder.Calls(events.RebootFailed)).To(Equal(1))
			ExpectMetricCounterValue(reboot.RebootsTotal, 1, map[string]string{"result": "provider_error"})
		})
	})

	Context("RebootIssued", func() {
		BeforeEach(func() {
			nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, map[string]string{
				v1.RebootPreBootIDAnnotationKey: "boot-1",
			})
			nodeClaim.StatusConditions().SetTrueWithReason(v1.ConditionTypeRebooting, v1.RebootReasonIssued, "reboot issued")
			// issuedAt is derived from the Initialized->Unknown transition, set at issue time.
			nodeClaim.StatusConditions().SetUnknownWithReason(v1.ConditionTypeInitialized, v1.RebootReasonRequested, "node is rebooting")
			node.Spec.Taints = append(node.Spec.Taints, v1.RebootingNoScheduleTaint)
		})

		It("stays issued while the node has not rebooted", func() {
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
			result := ExpectObjectReconciled(ctx, env.Client, rebootController, nodeClaim)

			Expect(result.RequeueAfter).To(BeNumerically(">", 0))
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeRebooting).Reason).To(Equal(v1.RebootReasonIssued))
			node = ExpectExists(ctx, env.Client, node)
			Expect(hasRebootTaint(node)).To(BeTrue())
		})

		It("removes the fence and emits Observed when the boot changes but the node is not yet Ready", func() {
			node.Status.NodeInfo.BootID = "boot-2"
			node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
			ExpectObjectReconciled(ctx, env.Client, rebootController, nodeClaim)

			node = ExpectExists(ctx, env.Client, node)
			Expect(hasRebootTaint(node)).To(BeFalse())
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeRebooting).IsTrue()).To(BeTrue())
			Expect(recorder.Calls(events.RebootObserved)).To(Equal(1))
		})

		It("succeeds when the boot changed and the node is Ready", func() {
			node.Status.NodeInfo.BootID = "boot-2"
			node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
			ExpectObjectReconciled(ctx, env.Client, rebootController, nodeClaim)

			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			cond := nodeClaim.StatusConditions().Get(v1.ConditionTypeRebooting)
			Expect(cond.IsFalse()).To(BeTrue())
			Expect(cond.Reason).To(Equal(v1.RebootReasonSucceeded))
			node = ExpectExists(ctx, env.Client, node)
			Expect(hasRebootTaint(node)).To(BeFalse())

			Expect(recorder.Calls(events.RebootSucceeded)).To(Equal(1))
			ExpectMetricCounterValue(reboot.RebootsTotal, 1, map[string]string{"result": "succeeded"})
			ExpectMetricHistogramSampleCountValue("karpenter_nodes_reboot_duration_seconds", 1, map[string]string{"result": "succeeded"})
			ExpectMetricHistogramSampleCountValue("karpenter_nodes_reboot_recovery_duration_seconds", 1, map[string]string{})
		})

		It("fails with recovery_timeout when the observation window elapses without recovery", func() {
			env.Clock.Step(21 * time.Minute)
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
			ExpectObjectReconciled(ctx, env.Client, rebootController, nodeClaim)

			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			cond := nodeClaim.StatusConditions().Get(v1.ConditionTypeRebooting)
			Expect(cond.IsFalse()).To(BeTrue())
			Expect(cond.Reason).To(Equal(v1.RebootReasonFailed))
			node = ExpectExists(ctx, env.Client, node)
			Expect(hasRebootTaint(node)).To(BeFalse())

			Expect(recorder.Calls(events.RebootFailed)).To(Equal(1))
			ExpectMetricCounterValue(reboot.RebootsTotal, 1, map[string]string{"result": "recovery_timeout"})
		})

		It("fails when the node is gone and the deadline has elapsed", func() {
			env.Clock.Step(21 * time.Minute)
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim) // node intentionally not applied
			ExpectObjectReconciled(ctx, env.Client, rebootController, nodeClaim)

			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			cond := nodeClaim.StatusConditions().Get(v1.ConditionTypeRebooting)
			Expect(cond.IsFalse()).To(BeTrue())
			Expect(cond.Reason).To(Equal(v1.RebootReasonFailed))
			ExpectMetricCounterValue(reboot.RebootsTotal, 1, map[string]string{"result": "recovery_timeout"})
		})

		It("keeps polling when the node is gone but the deadline has not elapsed", func() {
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim) // node intentionally not applied
			result := ExpectObjectReconciled(ctx, env.Client, rebootController, nodeClaim)

			Expect(result.RequeueAfter).To(BeNumerically(">", 0))
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeRebooting).Reason).To(Equal(v1.RebootReasonIssued))
		})
	})
})
