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

package lifecycle_test

import (
	"time"

	"github.com/awslabs/operatorpkg/object"
	"github.com/awslabs/operatorpkg/status"
	"github.com/samber/lo"

	operatorpkg "github.com/awslabs/operatorpkg/test/expectations"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	nodeclaimlifecycle "sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/lifecycle"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Liveness", func() {
	var nodePool *v1.NodePool

	BeforeEach(func() {
		nodePool = test.NodePool()
	})
	DescribeTable(
		"Liveness",
		func(isManagedNodeClaim bool) {
			nodeClaimOpts := []v1.NodeClaim{{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey: nodePool.Name,
					},
				},
				Spec: v1.NodeClaimSpec{
					Resources: v1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:      resource.MustParse("2"),
							corev1.ResourceMemory:   resource.MustParse("50Mi"),
							corev1.ResourcePods:     resource.MustParse("5"),
							fake.ResourceGPUVendorA: resource.MustParse("1"),
						},
					},
				},
			}}
			if !isManagedNodeClaim {
				nodeClaimOpts = append(nodeClaimOpts, v1.NodeClaim{
					Spec: v1.NodeClaimSpec{
						NodeClassRef: &v1.NodeClassReference{
							Group: "karpenter.test.sh",
							Kind:  "UnmanagedNodeClass",
							Name:  "default",
						},
					},
				})
			}
			nodeClaim := test.NodeClaim(nodeClaimOpts...)
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim)
			nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeLaunched)
			ExpectApplied(ctx, env.Client, nodeClaim)
			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)

			// If the node hasn't registered in the registration timeframe, then we deprovision the NodeClaim
			env.Clock.Step(time.Minute * 20)
			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
			ExpectFinalizersRemoved(ctx, env.Client, nodeClaim)
			if isManagedNodeClaim {
				ExpectNotFound(ctx, env.Client, nodeClaim)
				operatorpkg.ExpectStatusConditions(ctx, env.Client, 1*time.Minute, nodePool, status.Condition{
					Type:   v1.ConditionTypeNodeRegistrationHealthy,
					Status: metav1.ConditionUnknown,
				})
			} else {
				ExpectExists(ctx, env.Client, nodeClaim)
			}
		},
		Entry("should delete the nodeClaim when the Node hasn't registered past the registration timeout", true),
		Entry("should ignore NodeClaims not managed by this Karpenter instance", false),
	)
	It("shouldn't delete the nodeClaim when the node has registered past the registration timeout", func() {
		nodeClaim := test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey: nodePool.Name,
				},
			},
			Spec: v1.NodeClaimSpec{
				Resources: v1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:      resource.MustParse("2"),
						corev1.ResourceMemory:   resource.MustParse("50Mi"),
						corev1.ResourcePods:     resource.MustParse("5"),
						fake.ResourceGPUVendorA: resource.MustParse("1"),
					},
				},
			},
		})
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim)
		ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		node := test.NodeClaimLinkedNode(nodeClaim)
		ExpectApplied(ctx, env.Client, node)

		// Node and nodeClaim should still exist
		env.Clock.Step(time.Minute * 20)
		ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
		ExpectExists(ctx, env.Client, nodeClaim)
		ExpectExists(ctx, env.Client, node)
	})
	It("should delete the NodeClaim when the NodeClaim hasn't launched past the launch timeout", func() {
		nodeClaim := test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey: nodePool.Name,
				},
			},
			Spec: v1.NodeClaimSpec{
				Resources: v1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:      resource.MustParse("2"),
						corev1.ResourceMemory:   resource.MustParse("50Mi"),
						corev1.ResourcePods:     resource.MustParse("5"),
						fake.ResourceGPUVendorA: resource.MustParse("1"),
					},
				},
			},
		})
		cloudProvider.AllowedCreateCalls = 0 // Don't allow Create() calls to succeed
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim)
		_ = ExpectObjectReconcileFailed(ctx, env.Client, nodeClaimController, nodeClaim)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)

		// If the node hasn't launched in the launch timeout timeframe, then we deprovision the nodeClaim
		env.Clock.Step(time.Minute * 6)
		_ = ExpectObjectReconcileFailed(ctx, env.Client, nodeClaimController, nodeClaim)
		ExpectFinalizersRemoved(ctx, env.Client, nodeClaim)
		ExpectNotFound(ctx, env.Client, nodeClaim)
	})
	It("should not delete the NodeClaim when the NodeClaim hasn't launched before the launch timeout", func() {
		nodeClaim := test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey: nodePool.Name,
				},
			},
			Spec: v1.NodeClaimSpec{
				Resources: v1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:      resource.MustParse("2"),
						corev1.ResourceMemory:   resource.MustParse("50Mi"),
						corev1.ResourcePods:     resource.MustParse("5"),
						fake.ResourceGPUVendorA: resource.MustParse("1"),
					},
				},
			},
		})
		cloudProvider.AllowedCreateCalls = 0 // Don't allow Create() calls to succeed
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim)
		_ = ExpectObjectReconcileFailed(ctx, env.Client, nodeClaimController, nodeClaim)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)

		// try again a minute later but before the launch timeout
		env.Clock.Step(time.Minute * 1)
		_ = operatorpkg.ExpectObjectReconcileFailed(ctx, env.Client, nodeClaimController, nodeClaim)
		// expect that the nodeclaim was not deleted
		ExpectExists(ctx, env.Client, nodeClaim)
	})
	It("should respect a custom launch timeout configured via the LaunchTimeout var", func() {
		nodeclaimlifecycle.LaunchTimeout = 45 * time.Second
		DeferCleanup(func() { nodeclaimlifecycle.LaunchTimeout = 5 * time.Minute })
		nodeClaim := test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey: nodePool.Name,
				},
			},
			Spec: v1.NodeClaimSpec{
				Resources: v1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:      resource.MustParse("2"),
						corev1.ResourceMemory:   resource.MustParse("50Mi"),
						corev1.ResourcePods:     resource.MustParse("5"),
						fake.ResourceGPUVendorA: resource.MustParse("1"),
					},
				},
			},
		})
		cloudProvider.AllowedCreateCalls = 0 // Don't allow Create() calls to succeed
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim)
		_ = ExpectObjectReconcileFailed(ctx, env.Client, nodeClaimController, nodeClaim)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)

		// Before the custom 45s timeout, the NodeClaim should still exist
		env.Clock.Step(time.Second * 30)
		_ = ExpectObjectReconcileFailed(ctx, env.Client, nodeClaimController, nodeClaim)
		ExpectExists(ctx, env.Client, nodeClaim)

		// Past the custom 45s timeout but well before the default 5m timeout, it should be deleted
		env.Clock.Step(time.Second * 30)
		_ = ExpectObjectReconcileFailed(ctx, env.Client, nodeClaimController, nodeClaim)
		ExpectFinalizersRemoved(ctx, env.Client, nodeClaim)
		ExpectNotFound(ctx, env.Client, nodeClaim)
	})
	It("should use the status condition transition time for launch timeout, not the creation timestamp", func() {
		nodeClaim := test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey: nodePool.Name,
				},
			},
			Spec: v1.NodeClaimSpec{
				Resources: v1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:      resource.MustParse("2"),
						corev1.ResourceMemory:   resource.MustParse("50Mi"),
						corev1.ResourcePods:     resource.MustParse("5"),
						fake.ResourceGPUVendorA: resource.MustParse("1"),
					},
				},
			},
		})
		// the result cannot be tested with launch because if the launch fails the error is returned instead of requeue after
		cloudProvider.AllowedCreateCalls = 0 // Don't allow Create() calls to succeed
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim)
		_ = ExpectObjectReconcileFailed(ctx, env.Client, nodeClaimController, nodeClaim)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)

		conditions := nodeClaim.Status.Conditions
		newConditions := make([]status.Condition, len(conditions))
		for i, condition := range conditions {
			condition.LastTransitionTime = metav1.NewTime(env.Clock.Now().Add(10 * time.Minute))
			newConditions[i] = condition
		}
		nodeClaim.Status.Conditions = newConditions
		ExpectApplied(ctx, env.Client, nodeClaim)
		// advance the clock to show that the timeout is not based on creation timestamp when considering launch timeout
		env.Clock.Step(12 * time.Minute)
		_ = ExpectObjectReconcileFailed(ctx, env.Client, nodeClaimController, nodeClaim)

		// expect that the nodeclaim was not deleted after the timeout
		ExpectExists(ctx, env.Client, nodeClaim)
	})

	It("should use the status condition transition time for registration timeout, not the creation timestamp", func() {
		nodeClaim := test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey: nodePool.Name,
				},
			},
			Spec: v1.NodeClaimSpec{
				Resources: v1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:      resource.MustParse("2"),
						corev1.ResourceMemory:   resource.MustParse("50Mi"),
						corev1.ResourcePods:     resource.MustParse("5"),
						fake.ResourceGPUVendorA: resource.MustParse("1"),
					},
				},
			},
		})
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim)
		ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)

		conditions := nodeClaim.Status.Conditions
		newConditions := make([]status.Condition, len(conditions))
		for i, condition := range conditions {
			condition.LastTransitionTime = metav1.NewTime(env.Clock.Now().Add(10 * time.Minute))
			newConditions[i] = condition
		}
		nodeClaim.Status.Conditions = newConditions
		ExpectApplied(ctx, env.Client, nodeClaim)
		// advance the clock to show that the timeout is not based on creation timestamp when considering registration timeout
		env.Clock.Step(16 * time.Minute)
		result := ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
		Expect(result.RequeueAfter).To(Not(Equal(0 * time.Second)))
		Expect(result.RequeueAfter > 0*time.Second && result.RequeueAfter < 15*time.Minute).To(BeTrue())

		// expect that the nodeclaim was not deleted after the timeout
		ExpectExists(ctx, env.Client, nodeClaim)
	})

	It("should update NodeRegistrationHealthy status condition to False if it was previously set to True and there are >=2 registration failures", func() {
		ExpectApplied(ctx, env.Client, nodePool)
		nodePool.StatusConditions().SetTrue(v1.ConditionTypeNodeRegistrationHealthy)
		nodeClaim1 := test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey: nodePool.Name,
				},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion:         object.GVK(nodePool).GroupVersion().String(),
						Kind:               object.GVK(nodePool).Kind,
						Name:               nodePool.Name,
						UID:                nodePool.UID,
						BlockOwnerDeletion: new(true),
					},
				},
			},
		})
		nodeClaim2 := test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey: nodePool.Name,
				},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion:         object.GVK(nodePool).GroupVersion().String(),
						Kind:               object.GVK(nodePool).Kind,
						Name:               nodePool.Name,
						UID:                nodePool.UID,
						BlockOwnerDeletion: new(true),
					},
				},
			},
		})
		cloudProvider.AllowedCreateCalls = 0 // Don't allow Create() calls to succeed
		ExpectApplied(ctx, env.Client, nodeClaim1, nodeClaim2)
		_ = ExpectObjectReconcileFailed(ctx, env.Client, nodeClaimController, nodeClaim1)
		_ = ExpectObjectReconcileFailed(ctx, env.Client, nodeClaimController, nodeClaim2)

		// If the node hasn't registered in the registration timeframe, then we deprovision the nodeClaim
		env.Clock.Step(time.Minute * 6)
		_ = ExpectObjectReconcileFailed(ctx, env.Client, nodeClaimController, nodeClaim1)
		_ = ExpectObjectReconcileFailed(ctx, env.Client, nodeClaimController, nodeClaim2)

		// NodeClaim registration failed twice which is greater than our threshold so update the NodeRegistrationHealthy status condition
		operatorpkg.ExpectStatusConditions(ctx, env.Client, 1*time.Minute, nodePool, status.Condition{Type: v1.ConditionTypeNodeRegistrationHealthy, Status: metav1.ConditionFalse})
		ExpectFinalizersRemoved(ctx, env.Client, nodeClaim1, nodeClaim2)
		ExpectNotFound(ctx, env.Client, nodeClaim1, nodeClaim2)
	})
	It("should update NodeRegistrationHealthy status condition to False if it was previously set to Unknown and there are >=2 registration failures", func() {
		ExpectApplied(ctx, env.Client, nodePool)
		nodeClaim1 := test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey: nodePool.Name,
				},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion:         object.GVK(nodePool).GroupVersion().String(),
						Kind:               object.GVK(nodePool).Kind,
						Name:               nodePool.Name,
						UID:                nodePool.UID,
						BlockOwnerDeletion: new(true),
					},
				},
			},
		})
		nodeClaim2 := test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey: nodePool.Name,
				},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion:         object.GVK(nodePool).GroupVersion().String(),
						Kind:               object.GVK(nodePool).Kind,
						Name:               nodePool.Name,
						UID:                nodePool.UID,
						BlockOwnerDeletion: new(true),
					},
				},
			},
		})
		cloudProvider.AllowedCreateCalls = 0 // Don't allow Create() calls to succeed
		ExpectApplied(ctx, env.Client, nodeClaim1, nodeClaim2)
		_ = ExpectObjectReconcileFailed(ctx, env.Client, nodeClaimController, nodeClaim1)
		_ = ExpectObjectReconcileFailed(ctx, env.Client, nodeClaimController, nodeClaim2)

		// If the node hasn't registered in the registration timeframe, then we deprovision the nodeClaim
		env.Clock.Step(time.Minute * 6)
		_ = ExpectObjectReconcileFailed(ctx, env.Client, nodeClaimController, nodeClaim1)
		_ = ExpectObjectReconcileFailed(ctx, env.Client, nodeClaimController, nodeClaim2)

		// NodeClaim registration failed twice which is greater than our threshold so update the NodeRegistrationHealthy status condition
		operatorpkg.ExpectStatusConditions(ctx, env.Client, 1*time.Minute, nodePool, status.Condition{Type: v1.ConditionTypeNodeRegistrationHealthy, Status: metav1.ConditionFalse})
		ExpectFinalizersRemoved(ctx, env.Client, nodeClaim1, nodeClaim2)
		ExpectNotFound(ctx, env.Client, nodeClaim1, nodeClaim2)
	})
	It("should not update NodeRegistrationHealthy status condition if nodePool owning the nodeClaim is deleted", func() {
		nodePool = test.NodePool(
			v1.NodePool{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-nodepool",
				},
			},
		)
		ExpectApplied(ctx, env.Client, nodePool)
		nodeClaim := test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey: nodePool.Name,
				},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion:         object.GVK(nodePool).GroupVersion().String(),
						Kind:               object.GVK(nodePool).Kind,
						Name:               nodePool.Name,
						UID:                nodePool.UID,
						BlockOwnerDeletion: new(true),
					},
				},
			},
		})
		cloudProvider.AllowedCreateCalls = 0 // Don't allow Create() calls to succeed
		ExpectApplied(ctx, env.Client, nodeClaim)
		_ = ExpectObjectReconcileFailed(ctx, env.Client, nodeClaimController, nodeClaim)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		// Delete the nodePool referenced on the nodeClaim
		ExpectDeleted(ctx, env.Client, nodePool)
		//Recreate the nodePool so that it will have a different UID
		nodePool = test.NodePool(
			v1.NodePool{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-nodepool",
				},
			},
		)
		ExpectApplied(ctx, env.Client, nodePool)
		operatorpkg.ExpectStatusConditions(ctx, env.Client, 1*time.Minute, nodePool, status.Condition{Type: v1.ConditionTypeNodeRegistrationHealthy, Status: metav1.ConditionUnknown})

		// If the node hasn't registered in the registration timeframe, then we deprovision the nodeClaim
		env.Clock.Step(time.Minute * 20)
		_ = ExpectObjectReconcileFailed(ctx, env.Client, nodeClaimController, nodeClaim)
		operatorpkg.ExpectStatusConditions(ctx, env.Client, 1*time.Minute, nodePool, status.Condition{Type: v1.ConditionTypeNodeRegistrationHealthy, Status: metav1.ConditionUnknown})
		ExpectFinalizersRemoved(ctx, env.Client, nodeClaim)
		ExpectNotFound(ctx, env.Client, nodeClaim)
	})
	Context("Initialization Timeout", func() {
		var nodeClaim *v1.NodeClaim
		var node *corev1.Node

		BeforeEach(func() {
			recorder.Reset()
			metrics.NodeClaimsDisruptedTotal.Reset()
		})

		registerUninitialized := func(timeout string) {
			nodeClaim = test.NodeClaim(v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name},
				},
				Spec: v1.NodeClaimSpec{
					StartupTaints:         []corev1.Taint{{Key: "custom-startup-taint", Effect: corev1.TaintEffectNoSchedule}},
					InitializationTimeout: v1.MustParseNillableDuration(timeout),
				},
			})
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim)
			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			node = test.NodeClaimLinkedNode(nodeClaim)
			ExpectApplied(ctx, env.Client, node)
			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeRegistered).IsTrue()).To(BeTrue())
			Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeInitialized).Reason).To(Equal("StartupTaintsExist"))
		}

		It("should delete the NodeClaim when the node hasn't initialized past the initialization timeout", func() {
			registerUninitialized("30m")
			env.Clock.Step(31 * time.Minute)
			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
			ExpectFinalizersRemoved(ctx, env.Client, nodeClaim)
			ExpectNotFound(ctx, env.Client, nodeClaim)
			Expect(recorder.Calls(events.InitializationTimeout)).To(Equal(1))
			ExpectMetricCounterValue(metrics.NodeClaimsDisruptedTotal, 1, map[string]string{
				metrics.ReasonLabel:   metrics.InitializationTimeoutReason,
				metrics.NodePoolLabel: nodePool.Name,
			})
		})
		It("should requeue and not delete the NodeClaim before the initialization timeout", func() {
			registerUninitialized("30m")
			env.Clock.Step(20 * time.Minute)
			result := ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
			Expect(result.RequeueAfter).To(BeNumerically("~", 10*time.Minute, time.Minute))
			ExpectExists(ctx, env.Client, nodeClaim)
		})
		It("should not delete an uninitialized NodeClaim when the initialization timeout is Never", func() {
			registerUninitialized("Never")
			env.Clock.Step(24 * time.Hour)
			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
			ExpectExists(ctx, env.Client, nodeClaim)
			Expect(recorder.Calls(events.InitializationTimeout)).To(Equal(0))
		})
		It("should not delete an initialized NodeClaim past the initialization timeout", func() {
			registerUninitialized("30m")
			node = ExpectExists(ctx, env.Client, node)
			node.Spec.Taints = nil
			ExpectApplied(ctx, env.Client, node)
			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeInitialized).IsTrue()).To(BeTrue())
			env.Clock.Step(time.Hour)
			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
			ExpectExists(ctx, env.Client, nodeClaim)
		})
		It("should not delete a NodeClaim while it is rebooting", func() {
			registerUninitialized("30m")
			nodeClaim.StatusConditions().SetTrueWithReason(v1.ConditionTypeRebooting, v1.RebootReasonIssued, "")
			ExpectApplied(ctx, env.Client, nodeClaim)
			env.Clock.Step(time.Hour)
			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
			ExpectExists(ctx, env.Client, nodeClaim)
		})
		It("should measure the initialization timeout from the end of a reboot", func() {
			registerUninitialized("30m")
			env.Clock.Step(time.Hour)
			nodeClaim.StatusConditions(status.WithClock(env.Clock)).SetFalse(v1.ConditionTypeRebooting, v1.RebootReasonSucceeded, "")
			ExpectApplied(ctx, env.Client, nodeClaim)
			env.Clock.Step(20 * time.Minute)
			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
			ExpectExists(ctx, env.Client, nodeClaim)
			env.Clock.Step(11 * time.Minute)
			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
			ExpectFinalizersRemoved(ctx, env.Client, nodeClaim)
			ExpectNotFound(ctx, env.Client, nodeClaim)
		})
		It("should requeue for the time remaining", func() {
			registerUninitialized("30m")
			env.Clock.Step(12 * time.Minute)
			result := ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
			Expect(result.RequeueAfter).To(BeNumerically("~", 18*time.Minute, 5*time.Second))
		})
		It("should report the last Initialized reason in the event", func() {
			registerUninitialized("30m")
			env.Clock.Step(31 * time.Minute)
			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
			Expect(recorder.Events()).To(ContainElement(HaveField("Message", And(
				ContainSubstring("Node did not initialize within 30m0s"),
				ContainSubstring("StartupTaintsExist"),
			))))
		})
		It("should delete a never-initialized NodeClaim whose reboot is still draining", func() {
			registerUninitialized("30m")
			nodeClaim.StatusConditions(status.WithClock(env.Clock)).SetTrueWithReason(v1.ConditionTypeRebooting, v1.RebootReasonRequested, "")
			ExpectApplied(ctx, env.Client, nodeClaim)
			env.Clock.Step(31 * time.Minute)
			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
			ExpectFinalizersRemoved(ctx, env.Client, nodeClaim)
			ExpectNotFound(ctx, env.Client, nodeClaim)
			ExpectMetricCounterValue(metrics.NodeClaimsDisruptedTotal, 1, map[string]string{
				metrics.ReasonLabel:   metrics.InitializationTimeoutReason,
				metrics.NodePoolLabel: nodePool.Name,
			})
		})
		It("should not restart the clock when a reboot request fails without rebooting", func() {
			registerUninitialized("30m")
			env.Clock.Step(20 * time.Minute)
			nodeClaim.StatusConditions(status.WithClock(env.Clock)).SetFalse(v1.ConditionTypeRebooting, v1.RebootReasonFailed, "invalid reboot request")
			ExpectApplied(ctx, env.Client, nodeClaim)
			env.Clock.Step(11 * time.Minute)
			ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
			ExpectFinalizersRemoved(ctx, env.Client, nodeClaim)
			ExpectNotFound(ctx, env.Client, nodeClaim)
		})
		Context("Reboot", func() {
			rebootInitializedNode := func() {
				registerUninitialized("30m")
				node = ExpectExists(ctx, env.Client, node)
				node.Spec.Taints = nil
				ExpectApplied(ctx, env.Client, node)
				ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
				nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
				Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeInitialized).IsTrue()).To(BeTrue())
				env.Clock.Step(48 * time.Hour)
				nodeClaim.StatusConditions(status.WithClock(env.Clock)).SetTrueWithReason(v1.ConditionTypeRebooting, v1.RebootReasonIssued, "")
				nodeClaim.StatusConditions(status.WithClock(env.Clock)).SetUnknownWithReason(v1.ConditionTypeInitialized, "Rebooting", "")
				ExpectApplied(ctx, env.Client, nodeClaim)
				node.Spec.Taints = []corev1.Taint{{Key: "custom-startup-taint", Effect: corev1.TaintEffectNoSchedule}}
				ExpectApplied(ctx, env.Client, node)
			}
			succeed := func() {
				nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
				nodeClaim.StatusConditions(status.WithClock(env.Clock)).SetFalse(v1.ConditionTypeRebooting, v1.RebootReasonSucceeded, "")
				ExpectApplied(ctx, env.Client, nodeClaim)
			}
			It("should report a re-initialization timeout when the reboot succeeds in the second the node registered", func() {
				registerUninitialized("30m")
				nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
				nodeClaim.StatusConditions().SetFalse(v1.ConditionTypeRebooting, v1.RebootReasonSucceeded, "")
				conditions := nodeClaim.GetConditions()
				for i := range conditions {
					if conditions[i].Type == v1.ConditionTypeRebooting {
						conditions[i].LastTransitionTime = nodeClaim.StatusConditions().Get(v1.ConditionTypeRegistered).LastTransitionTime
					}
				}
				nodeClaim.SetConditions(conditions)
				ExpectApplied(ctx, env.Client, nodeClaim)
				env.Clock.Step(31 * time.Minute)
				ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
				ExpectFinalizersRemoved(ctx, env.Client, nodeClaim)
				ExpectNotFound(ctx, env.Client, nodeClaim)
				ExpectMetricCounterValue(metrics.NodeClaimsDisruptedTotal, 1, map[string]string{
					metrics.ReasonLabel:   metrics.ReinitializationTimeoutReason,
					metrics.NodePoolLabel: nodePool.Name,
				})
			})
			It("should not delete while the reboot is issued", func() {
				rebootInitializedNode()
				env.Clock.Step(time.Hour)
				ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
				ExpectExists(ctx, env.Client, nodeClaim)
			})
			It("should delete a NodeClaim that doesn't re-initialize within the timeout of the reboot succeeding", func() {
				rebootInitializedNode()
				env.Clock.Step(15 * time.Minute)
				succeed()
				env.Clock.Step(29 * time.Minute)
				ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
				ExpectExists(ctx, env.Client, nodeClaim)
				env.Clock.Step(2 * time.Minute)
				ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
				ExpectFinalizersRemoved(ctx, env.Client, nodeClaim)
				ExpectNotFound(ctx, env.Client, nodeClaim)
				ExpectMetricCounterValue(metrics.NodeClaimsDisruptedTotal, 1, map[string]string{
					metrics.ReasonLabel:   metrics.ReinitializationTimeoutReason,
					metrics.NodePoolLabel: nodePool.Name,
				})
				Expect(recorder.Events()).To(ContainElement(HaveField("Message", ContainSubstring("Node did not re-initialize after reboot within 30m0s"))))
			})
			It("should not delete a NodeClaim that re-initializes after the reboot", func() {
				rebootInitializedNode()
				succeed()
				node = ExpectExists(ctx, env.Client, node)
				node.Spec.Taints = nil
				ExpectApplied(ctx, env.Client, node)
				ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
				nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
				Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeInitialized).IsTrue()).To(BeTrue())
				env.Clock.Step(time.Hour)
				ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
				ExpectExists(ctx, env.Client, nodeClaim)
			})
			It("should bound the drain by the reboot's termination grace period", func() {
				rebootInitializedNode()
				nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
				nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, map[string]string{v1.RebootTerminationGracePeriodAnnotationKey: "10m"})
				nodeClaim.Finalizers = append(nodeClaim.Finalizers, "test.karpenter.sh/hold")
				ExpectApplied(ctx, env.Client, nodeClaim)
				succeed()
				env.Clock.Step(31 * time.Minute)
				ExpectObjectReconciled(ctx, env.Client, nodeClaimController, nodeClaim)
				nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
				Expect(nodeClaim.DeletionTimestamp.IsZero()).To(BeFalse())
				terminationTime, err := time.Parse(time.RFC3339, nodeClaim.Annotations[v1.NodeClaimTerminationTimestampAnnotationKey])
				Expect(err).ToNot(HaveOccurred())
				Expect(terminationTime).To(BeTemporally("~", env.Clock.Now().Add(10*time.Minute), 5*time.Second))
				ExpectFinalizersRemoved(ctx, env.Client, nodeClaim)
			})
		})
		It("should propagate the initialization timeout from the NodePool template", func() {
			nodePool.Spec.Template.Spec.InitializationTimeout = v1.MustParseNillableDuration("45m")
			Expect(nodePool.Spec.Template.ToNodeClaim().Spec.InitializationTimeout.Duration).To(Equal(lo.ToPtr(45 * time.Minute)))
		})
		It("should not drift NodeClaims when the initialization timeout changes", func() {
			hash := nodePool.Hash()
			nodePool.Spec.Template.Spec.InitializationTimeout = v1.MustParseNillableDuration("45m")
			Expect(nodePool.Hash()).To(Equal(hash))
		})
	})
})
