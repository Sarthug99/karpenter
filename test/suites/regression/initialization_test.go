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

package integration_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/test"
)

var _ = Describe("Initialization Timeout", func() {
	BeforeEach(func() {
		nodePool.Spec.Template.Spec.StartupTaints = []corev1.Taint{{Key: "example.com/never-removed", Effect: corev1.TaintEffectNoSchedule}}
	})
	expectStuckInitializing := func(nodeClaim *v1.NodeClaim) {
		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(env.Context, client.ObjectKeyFromObject(nodeClaim), nodeClaim)).To(Succeed())
			g.Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeRegistered).IsTrue()).To(BeTrue())
			g.Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeInitialized).IsUnknown()).To(BeTrue())
			g.Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeInitialized).Reason).To(Equal("StartupTaintsExist"))
		}).Should(Succeed())
	}
	It("should replace a NodeClaim whose startup taint is never removed", func() {
		nodePool.Spec.Template.Spec.InitializationTimeout = v1.MustParseNillableDuration("2m")
		pod := test.Pod()
		env.ExpectCreated(nodeClass, nodePool, pod)
		nodeClaim := env.EventuallyExpectCreatedNodeClaimCount("==", 1)[0]
		expectStuckInitializing(nodeClaim)

		Eventually(func(g Gomega) {
			g.Expect(errors.IsNotFound(env.Client.Get(env.Context, client.ObjectKeyFromObject(nodeClaim), &v1.NodeClaim{}))).To(BeTrue())
		}).WithTimeout(5 * time.Minute).Should(Succeed())

		replacement := env.EventuallyExpectCreatedNodeClaimCount("==", 1)[0]
		Expect(replacement.Name).ToNot(Equal(nodeClaim.Name))
		Expect(replacement.Spec.InitializationTimeout.Duration).To(Equal(nodePool.Spec.Template.Spec.InitializationTimeout.Duration))
		expectStuckInitializing(replacement)
	})
	It("should not replace an uninitialized NodeClaim when the initialization timeout is unset", func() {
		pod := test.Pod()
		env.ExpectCreated(nodeClass, nodePool, pod)
		nodeClaim := env.EventuallyExpectCreatedNodeClaimCount("==", 1)[0]
		expectStuckInitializing(nodeClaim)

		Consistently(func(g Gomega) {
			g.Expect(env.Client.Get(env.Context, client.ObjectKeyFromObject(nodeClaim), nodeClaim)).To(Succeed())
			g.Expect(nodeClaim.DeletionTimestamp.IsZero()).To(BeTrue())
		}).WithTimeout(3 * time.Minute).Should(Succeed())
	})
})
