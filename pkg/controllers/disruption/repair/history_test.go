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
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

var _ = Describe("RebootHistory", func() {
	var (
		history        *RebootHistory
		node           *corev1.Node
		nodeClaim      *v1.NodeClaim
		rebootResults  []Result
		replaceResults []Result
	)

	BeforeEach(func() {
		history = NewRebootHistory()
		node = &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: types.UID("node-uid")}}
		nodeClaim = &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "nodeclaim", UID: types.UID("nodeclaim-uid")}}
		terminationGracePeriod := time.Minute
		rebootResults = []Result{
			repairResult("AcceleratorReady", corev1.ConditionFalse, "XID48", cloudprovider.RebootNode, time.Unix(1, 0), &terminationGracePeriod),
		}
		replaceResults = []Result{
			repairResult("StorageReady", corev1.ConditionFalse, "DiskFailure", cloudprovider.ReplaceNode, time.Unix(2, 0), nil),
		}
	})

	It("resolves the candidate before commitment", func() {
		candidate := admit(history, node, nodeClaim, false, append(rebootResults, replaceResults...))
		Expect(candidate.Action).To(Equal(cloudprovider.ReplaceNode))
		Expect(candidate.DrivingCondition.Type).To(Equal(corev1.NodeConditionType("StorageReady")))
		Expect(candidate.RebootEscalated).To(BeFalse())

		candidate = admit(history, node, nodeClaim, false, rebootResults)
		Expect(candidate.Action).To(Equal(cloudprovider.RebootNode))
	})

	It("resolves current results without committing or recording history", func() {
		candidate := history.Resolve(node, nodeClaim, false, rebootResults)
		Expect(candidate.Action).To(Equal(cloudprovider.RebootNode))

		candidate = history.Resolve(node, nodeClaim, false, rebootResults)
		Expect(candidate.Action).To(Equal(cloudprovider.RebootNode))
	})

	It("suppresses every action from active history before the lifecycle is observed", func() {
		admit(history, node, nodeClaim, false, rebootResults)

		Expect(admit(history, node, nodeClaim, false, append(rebootResults, replaceResults...))).To(BeNil())
	})

	It("suppresses every action while the durable reboot lifecycle is active", func() {
		Expect(admit(history, node, nodeClaim, true, append(rebootResults, replaceResults...))).To(BeNil())
	})

	It("keeps active lifecycle suppression after local history resolves", func() {
		admit(history, node, nodeClaim, false, rebootResults)
		history.MarkResolved(nodeClaim.UID)

		Expect(admit(history, node, nodeClaim, true, rebootResults)).To(BeNil())
	})

	It("escalates reboot results after local history resolves without mutating the input", func() {
		admit(history, node, nodeClaim, false, rebootResults)
		history.MarkResolved(nodeClaim.UID)

		candidate := admit(history, node, nodeClaim, false, rebootResults)
		Expect(candidate.Action).To(Equal(cloudprovider.ReplaceNode))
		Expect(candidate.EligibleAt).To(Equal(rebootResults[0].EligibleAt))
		Expect(candidate.DrivingCondition.Type).To(Equal(rebootResults[0].ConditionType))
		Expect(candidate.TerminationGracePeriod).NotTo(BeNil())
		Expect(*candidate.TerminationGracePeriod).To(Equal(time.Minute))
		Expect(candidate.RebootEscalated).To(BeTrue())
		Expect(rebootResults[0].Action).To(Equal(cloudprovider.RebootNode))
	})

	It("keeps replacement results after local history resolves", func() {
		admit(history, node, nodeClaim, false, rebootResults)
		history.MarkResolved(nodeClaim.UID)

		candidate := admit(history, node, nodeClaim, false, replaceResults)
		Expect(candidate.Action).To(Equal(cloudprovider.ReplaceNode))
		Expect(candidate.DrivingCondition.Type).To(Equal(corev1.NodeConditionType("StorageReady")))
		Expect(candidate.RebootEscalated).To(BeFalse())
	})

	It("does not reconstruct history from an unrecorded terminal lifecycle", func() {
		history.MarkResolved(nodeClaim.UID)
		candidate := admit(history, node, nodeClaim, false, rebootResults)
		Expect(candidate.Action).To(Equal(cloudprovider.RebootNode))
	})

	It("removes history when the NodeClaim no longer exists", func() {
		admit(history, node, nodeClaim, false, rebootResults)
		history.MarkResolved(nodeClaim.UID)

		candidate := admit(history, node, nodeClaim, false, rebootResults)
		Expect(candidate.Action).To(Equal(cloudprovider.ReplaceNode))

		history.Delete(nodeClaim.UID)

		candidate = admit(history, node, nodeClaim, false, rebootResults)
		Expect(candidate.Action).To(Equal(cloudprovider.RebootNode))
	})

	It("keys history by NodeClaim UID", func() {
		admit(history, node, nodeClaim, false, rebootResults)
		history.MarkResolved(nodeClaim.UID)

		successor := nodeClaim.DeepCopy()
		successor.UID = types.UID("successor-uid")
		candidate := admit(history, node, successor, false, rebootResults)
		Expect(candidate.Action).To(Equal(cloudprovider.RebootNode))
	})

	It("returns no results without current eligible evidence", func() {
		admit(history, node, nodeClaim, false, rebootResults)
		history.MarkResolved(nodeClaim.UID)

		Expect(admit(history, node, nodeClaim, false, nil)).To(BeNil())
	})

	It("does not record a reboot when commitment fails", func() {
		expectedErr := errors.New("committing reboot")
		Expect(history.Admit(node, nodeClaim, false, rebootResults, func(*Candidate) (bool, error) {
			return false, expectedErr
		})).To(MatchError(expectedErr))

		candidate := admit(history, node, nodeClaim, false, rebootResults)
		Expect(candidate.Action).To(Equal(cloudprovider.RebootNode))
	})

	It("records a committed reboot before returning a later error", func() {
		expectedErr := errors.New("after committing reboot")
		Expect(history.Admit(node, nodeClaim, false, rebootResults, func(*Candidate) (bool, error) {
			return true, expectedErr
		})).To(MatchError(expectedErr))

		Expect(admit(history, node, nodeClaim, false, rebootResults)).To(BeNil())
	})

	It("serializes final admission and reboot recording for one NodeClaim", func() {
		firstStarted := make(chan struct{})
		releaseFirst := make(chan struct{})
		firstDone := make(chan error, 1)
		go func() {
			firstDone <- history.Admit(node, nodeClaim, false, rebootResults, func(*Candidate) (bool, error) {
				close(firstStarted)
				<-releaseFirst
				return true, nil
			})
		}()
		<-firstStarted

		secondCommitted := false
		secondDone := make(chan error, 1)
		go func() {
			secondDone <- history.Admit(node, nodeClaim, false, rebootResults, func(*Candidate) (bool, error) {
				secondCommitted = true
				return true, nil
			})
		}()
		Eventually(func() int { return history.lockReferences(nodeClaim.UID) }).Should(Equal(2))

		close(releaseFirst)
		Expect(<-firstDone).NotTo(HaveOccurred())
		Expect(<-secondDone).NotTo(HaveOccurred())
		Expect(secondCommitted).To(BeFalse())
	})

	It("does not block admission for another NodeClaim", func() {
		firstStarted := make(chan struct{})
		releaseFirst := make(chan struct{})
		firstDone := make(chan error, 1)
		go func() {
			firstDone <- history.Admit(node, nodeClaim, false, rebootResults, func(*Candidate) (bool, error) {
				close(firstStarted)
				<-releaseFirst
				return true, nil
			})
		}()
		<-firstStarted

		successor := nodeClaim.DeepCopy()
		successor.UID = types.UID("successor-uid")
		secondCommitted := make(chan struct{})
		secondDone := make(chan error, 1)
		go func() {
			secondDone <- history.Admit(node, successor, false, rebootResults, func(*Candidate) (bool, error) {
				close(secondCommitted)
				return true, nil
			})
		}()

		Eventually(secondCommitted).Should(BeClosed())
		Expect(<-secondDone).NotTo(HaveOccurred())
		close(releaseFirst)
		Expect(<-firstDone).NotTo(HaveOccurred())
	})

	It("serializes terminal resolution with reboot recording", func() {
		commitStarted := make(chan struct{})
		releaseCommit := make(chan struct{})
		commitDone := make(chan error, 1)
		go func() {
			commitDone <- history.Admit(node, nodeClaim, false, rebootResults, func(*Candidate) (bool, error) {
				close(commitStarted)
				<-releaseCommit
				return true, nil
			})
		}()
		<-commitStarted

		resolveDone := make(chan struct{})
		go func() {
			history.MarkResolved(nodeClaim.UID)
			close(resolveDone)
		}()
		Eventually(func() int { return history.lockReferences(nodeClaim.UID) }).Should(Equal(2))

		close(releaseCommit)
		Expect(<-commitDone).NotTo(HaveOccurred())
		Eventually(resolveDone).Should(BeClosed())

		candidate := admit(history, node, nodeClaim, false, rebootResults)
		Expect(candidate.Action).To(Equal(cloudprovider.ReplaceNode))
	})
})

func admit(
	history *RebootHistory,
	node *corev1.Node,
	nodeClaim *v1.NodeClaim,
	activeRebootLifecycle bool,
	results []Result,
) *Candidate {
	GinkgoHelper()
	var selected *Candidate
	Expect(history.Admit(node, nodeClaim, activeRebootLifecycle, results, func(candidate *Candidate) (bool, error) {
		selected = candidate
		return true, nil
	})).To(Succeed())
	return selected
}

func (h *RebootHistory) lockReferences(nodeClaimUID types.UID) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	if lock, ok := h.locks[nodeClaimUID]; ok {
		return lock.references
	}
	return 0
}
