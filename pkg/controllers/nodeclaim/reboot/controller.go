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

// Package reboot implements the reboot node action: it drives a committed reboot (a NodeClaim carrying
// a Rebooting=True/RebootRequested condition) through drain -> issue -> observe to a terminal
// RebootSucceeded/RebootFailed outcome. The consumer (the disruption pipeline) commits the reboot; this
// controller carries it out and hands back a terminal outcome. See designs/reboot-node-action.md.
package reboot

import (
	"context"
	"fmt"
	"time"

	"github.com/awslabs/operatorpkg/reasonable"
	"github.com/google/uuid"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination/terminator"
	rebootevents "sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/reboot/events"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
)

const (
	// observationWindow bounds how long we wait for a rebooted node to prove a new boot and rejoin
	// after issuance before declaring RebootFailed. Beta uses a fixed value sized for slow instances.
	observationWindow = 20 * time.Minute
	// pollInterval is how often we re-check for boot/readiness while observing recovery.
	pollInterval = 15 * time.Second
)

// Controller drives the reboot lifecycle for NodeClaims carrying an active Rebooting condition.
type Controller struct {
	clock         clock.Clock
	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
	terminator    *terminator.Terminator
	recorder      events.Recorder
}

func NewController(clk clock.Clock, kubeClient client.Client, cloudProvider cloudprovider.CloudProvider, t *terminator.Terminator, recorder events.Recorder) *Controller {
	return &Controller{
		clock:         clk,
		kubeClient:    kubeClient,
		cloudProvider: cloudProvider,
		terminator:    t,
		recorder:      recorder,
	}
}

func (c *Controller) Name() string {
	return "nodeclaim.reboot"
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		For(&v1.NodeClaim{}, builder.WithPredicates(predicate.NewPredicateFuncs(func(o client.Object) bool {
			nc, ok := o.(*v1.NodeClaim)
			return ok && nc.StatusConditions().Get(v1.ConditionTypeRebooting) != nil
		}))).
		WithOptions(controller.Options{RateLimiter: reasonable.RateLimiter(), MaxConcurrentReconciles: 10}).
		Complete(reconcile.AsReconciler(m.GetClient(), c))
}

func (c *Controller) Reconcile(ctx context.Context, nodeClaim *v1.NodeClaim) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())

	cond := nodeClaim.StatusConditions().Get(v1.ConditionTypeRebooting)
	// Only active (True) reboots are driven here. Absent or terminal (False) => nothing to do.
	if cond == nil || !cond.IsTrue() {
		return reconcile.Result{}, nil
	}

	node, err := nodeclaimutils.NodeForNodeClaim(ctx, c.kubeClient, nodeClaim)
	if err != nil {
		// A missing node is expected transiently; requeue and retry.
		return reconcile.Result{}, nodeclaimutils.IgnoreNodeNotFoundError(err)
	}

	switch cond.Reason {
	case v1.RebootReasonRequested:
		return c.reconcileRequested(ctx, nodeClaim, node)
	case v1.RebootReasonIssued:
		return c.reconcileIssued(ctx, nodeClaim, node)
	default:
		return reconcile.Result{}, nil
	}
}

// reconcileRequested applies the scheduling fence, drains (bounded), then issues the provider reboot.
func (c *Controller) reconcileRequested(ctx context.Context, nodeClaim *v1.NodeClaim, node *corev1.Node) (reconcile.Result, error) {
	c.recorder.Publish(rebootevents.Requested(nodeClaim))
	// Stamp the request time (the total-duration metric's start) before draining.
	if err := c.ensureRequestedAt(ctx, nodeClaim); err != nil {
		return reconcile.Result{}, err
	}
	// Scheduling fence: reboot-owned taint that keeps evicted pods from rescheduling onto the pre-reboot boot.
	if err := c.ensureRebootTaint(ctx, node); err != nil {
		return reconcile.Result{}, err
	}

	// Restart-safety: once we've begun issuing (pre-boot bootID recorded), a changed bootID proves the
	// reboot already happened — advance to observe without re-issuing.
	preBootID, issuing := nodeClaim.Annotations[v1.RebootPreBootIDAnnotationKey]
	if issuing && node.Status.NodeInfo.BootID != preBootID {
		return c.transitionToIssued(ctx, nodeClaim, node)
	}

	// Drain before issuing (only before we've recorded pre-boot state; on resume after that, skip drain).
	if !issuing {
		if done, res, err := c.drain(ctx, nodeClaim, node); err != nil || !done {
			return res, err
		}
		// Record pre-boot state before the first provider call, and mint a stable operationID.
		if err := c.recordIssuingState(ctx, nodeClaim, node); err != nil {
			return reconcile.Result{}, err
		}
	}

	// Issue the reboot with the persisted, stable operationID.
	operationID := nodeClaim.Annotations[v1.RebootOperationIDAnnotationKey]
	if err := c.cloudProvider.Reboot(ctx, nodeClaim, operationID); err != nil {
		if cloudprovider.IsNodeRebootNotImplementedError(err) {
			return c.transitionToFailed(ctx, nodeClaim, node, resultProviderError, "reboot not implemented by the cloud provider")
		}
		// Transient error: stay in RebootRequested and retry with backoff using the same operationID.
		return reconcile.Result{}, fmt.Errorf("issuing reboot, %w", err)
	}
	return c.transitionToIssued(ctx, nodeClaim, node)
}

// reconcileIssued observes recovery: remove the fence once the boot changes, succeed on a fresh boot +
// Ready, fail if the observation window elapses first.
func (c *Controller) reconcileIssued(ctx context.Context, nodeClaim *v1.NodeClaim, node *corev1.Node) (reconcile.Result, error) {
	preBootID := nodeClaim.Annotations[v1.RebootPreBootIDAnnotationKey]
	bootChanged := node.Status.NodeInfo.BootID != preBootID

	// The fence exists only while the pre-reboot boot may still be active. A changed bootID proves the
	// new boot has begun, so remove it immediately (independent of readiness).
	if bootChanged {
		if err := c.removeRebootTaint(ctx, node); err != nil {
			return reconcile.Result{}, err
		}
		c.recorder.Publish(rebootevents.Observed(nodeClaim))
	}

	ready := nodeutils.GetCondition(node, corev1.NodeReady).Status == corev1.ConditionTrue
	if bootChanged && ready {
		return c.transitionToSucceeded(ctx, nodeClaim, node)
	}

	if issuedAt, ok := c.issuedAt(nodeClaim); ok && c.clock.Since(issuedAt) > observationWindow {
		return c.transitionToFailed(ctx, nodeClaim, node, resultRecoveryTimeout, "node did not recover within observation window")
	}
	return reconcile.Result{RequeueAfter: pollInterval}, nil
}

// drain runs a bounded graceful drain (eviction only, no cordon). Returns done=true when the drain
// completes or the drainGracePeriod deadline elapses (residual pods ride the reboot). drainGracePeriod=0
// skips the drain entirely (forceful).
func (c *Controller) drain(ctx context.Context, nodeClaim *v1.NodeClaim, node *corev1.Node) (done bool, res reconcile.Result, err error) {
	dgp := c.drainGracePeriod(nodeClaim)
	if dgp <= 0 {
		return true, reconcile.Result{}, nil
	}
	// Deadline is measured from when the reboot was requested (the Rebooting condition's transition).
	deadline := nodeClaim.StatusConditions().Get(v1.ConditionTypeRebooting).LastTransitionTime.Add(dgp)
	if err := c.terminator.Drain(ctx, node, &deadline); err != nil {
		if !terminator.IsNodeDrainError(err) {
			return false, reconcile.Result{}, fmt.Errorf("draining node, %w", err)
		}
		// Pods still draining: keep trying until the deadline, then proceed with residual pods riding.
		if c.clock.Now().Before(deadline) {
			return false, reconcile.Result{RequeueAfter: deadline.Sub(c.clock.Now())}, nil
		}
	}
	return true, reconcile.Result{}, nil
}

// recordIssuingState mints the operationID and records the pre-reboot bootID before the first provider
// call, so retries and restarts reuse the same operationID and the reboot is detectable after a restart.
func (c *Controller) recordIssuingState(ctx context.Context, nodeClaim *v1.NodeClaim, node *corev1.Node) error {
	stored := nodeClaim.DeepCopy()
	if nodeClaim.Annotations[v1.RebootOperationIDAnnotationKey] == "" {
		nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, map[string]string{v1.RebootOperationIDAnnotationKey: uuid.NewString()})
	}
	nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, map[string]string{v1.RebootPreBootIDAnnotationKey: node.Status.NodeInfo.BootID})
	if equality.Semantic.DeepEqual(stored, nodeClaim) {
		return nil
	}
	return c.kubeClient.Patch(ctx, nodeClaim, client.MergeFrom(stored))
}

func (c *Controller) transitionToIssued(ctx context.Context, nodeClaim *v1.NodeClaim, node *corev1.Node) (reconcile.Result, error) {
	stored := nodeClaim.DeepCopy()
	nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, map[string]string{v1.RebootIssuedAtAnnotationKey: c.clock.Now().Format(time.RFC3339)})
	nodeClaim.StatusConditions().SetTrueWithReason(v1.ConditionTypeRebooting, v1.RebootReasonIssued, "reboot issued to the provider")
	// Initialization is scoped to a boot; a committed reboot invalidates it until the node re-initializes.
	nodeClaim.StatusConditions().SetUnknownWithReason(v1.ConditionTypeInitialized, v1.RebootReasonRequested, "node is rebooting")
	if !equality.Semantic.DeepEqual(stored, nodeClaim) {
		if err := c.kubeClient.Status().Patch(ctx, nodeClaim, client.MergeFrom(stored)); err != nil {
			return reconcile.Result{}, err
		}
		if err := c.kubeClient.Patch(ctx, nodeClaim, client.MergeFrom(stored)); err != nil {
			return reconcile.Result{}, err
		}
	}
	// Remove the initialized label so uninitialized-node accounting treats capacity as returning.
	if err := c.removeInitializedLabel(ctx, node); err != nil {
		return reconcile.Result{}, err
	}
	c.recorder.Publish(rebootevents.Issued(nodeClaim))
	return reconcile.Result{RequeueAfter: pollInterval}, nil
}

func (c *Controller) transitionToSucceeded(ctx context.Context, nodeClaim *v1.NodeClaim, node *corev1.Node) (reconcile.Result, error) {
	if err := c.removeRebootTaint(ctx, node); err != nil {
		return reconcile.Result{}, err
	}
	c.recorder.Publish(rebootevents.Succeeded(nodeClaim))
	c.recordTerminalMetrics(nodeClaim, resultSucceeded)
	// Recovery duration (drain-independent): issuance -> new boot rejoined. Success only.
	if issuedAt, ok := c.issuedAt(nodeClaim); ok {
		RebootRecoveryDurationSeconds.Observe(c.clock.Since(issuedAt).Seconds(), map[string]string{})
	}
	return c.setTerminal(ctx, nodeClaim, v1.RebootReasonSucceeded, "node rebooted and rejoined the cluster")
}

func (c *Controller) transitionToFailed(ctx context.Context, nodeClaim *v1.NodeClaim, node *corev1.Node, result, msg string) (reconcile.Result, error) {
	// Terminal cleanup: ensure the reboot-owned fence is removed even if the boot never changed.
	if err := c.removeRebootTaint(ctx, node); err != nil {
		return reconcile.Result{}, err
	}
	c.recorder.Publish(rebootevents.Failed(nodeClaim, msg))
	c.recordTerminalMetrics(nodeClaim, result)
	return c.setTerminal(ctx, nodeClaim, v1.RebootReasonFailed, msg)
}

func (c *Controller) setTerminal(ctx context.Context, nodeClaim *v1.NodeClaim, reason, msg string) (reconcile.Result, error) {
	stored := nodeClaim.DeepCopy()
	nodeClaim.StatusConditions().SetFalse(v1.ConditionTypeRebooting, reason, msg)
	if !equality.Semantic.DeepEqual(stored, nodeClaim) {
		if err := c.kubeClient.Status().Patch(ctx, nodeClaim, client.MergeFrom(stored)); err != nil {
			return reconcile.Result{}, err
		}
	}
	log.FromContext(ctx).WithValues("reason", reason).Info("reboot reached terminal outcome")
	return reconcile.Result{}, nil
}

func (c *Controller) ensureRebootTaint(ctx context.Context, node *corev1.Node) error {
	if _, found := lo.Find(node.Spec.Taints, func(t corev1.Taint) bool { return t.MatchTaint(&v1.RebootingNoScheduleTaint) }); found {
		return nil
	}
	stored := node.DeepCopy()
	node.Spec.Taints = append(node.Spec.Taints, v1.RebootingNoScheduleTaint)
	return c.kubeClient.Patch(ctx, node, client.MergeFrom(stored))
}

func (c *Controller) removeRebootTaint(ctx context.Context, node *corev1.Node) error {
	stored := node.DeepCopy()
	node.Spec.Taints = lo.Reject(node.Spec.Taints, func(t corev1.Taint, _ int) bool { return t.MatchTaint(&v1.RebootingNoScheduleTaint) })
	if equality.Semantic.DeepEqual(stored, node) {
		return nil
	}
	return c.kubeClient.Patch(ctx, node, client.MergeFrom(stored))
}

func (c *Controller) removeInitializedLabel(ctx context.Context, node *corev1.Node) error {
	if _, ok := node.Labels[v1.NodeInitializedLabelKey]; !ok {
		return nil
	}
	stored := node.DeepCopy()
	delete(node.Labels, v1.NodeInitializedLabelKey)
	return c.kubeClient.Patch(ctx, node, client.MergeFrom(stored))
}

func (c *Controller) drainGracePeriod(nodeClaim *v1.NodeClaim) time.Duration {
	d, err := time.ParseDuration(nodeClaim.Annotations[v1.RebootDrainGracePeriodAnnotationKey])
	if err != nil {
		return 0
	}
	return d
}

func (c *Controller) issuedAt(nodeClaim *v1.NodeClaim) (time.Time, bool) {
	t, err := time.Parse(time.RFC3339, nodeClaim.Annotations[v1.RebootIssuedAtAnnotationKey])
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// ensureRequestedAt stamps the reboot request time (the Rebooting condition's transition) once, so the
// total-duration metric can measure request -> terminal across drain, issue, and observe.
func (c *Controller) ensureRequestedAt(ctx context.Context, nodeClaim *v1.NodeClaim) error {
	if _, ok := nodeClaim.Annotations[v1.RebootRequestedAtAnnotationKey]; ok {
		return nil
	}
	requestedAt := nodeClaim.StatusConditions().Get(v1.ConditionTypeRebooting).LastTransitionTime.Time
	stored := nodeClaim.DeepCopy()
	nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, map[string]string{v1.RebootRequestedAtAnnotationKey: requestedAt.Format(time.RFC3339)})
	return c.kubeClient.Patch(ctx, nodeClaim, client.MergeFrom(stored))
}

// recordTerminalMetrics counts the reboot by result and observes the full-action duration (request ->
// terminal), on every terminal outcome.
func (c *Controller) recordTerminalMetrics(nodeClaim *v1.NodeClaim, result string) {
	RebootsTotal.Inc(map[string]string{resultLabel: result})
	if requestedAt, err := time.Parse(time.RFC3339, nodeClaim.Annotations[v1.RebootRequestedAtAnnotationKey]); err == nil {
		RebootDurationSeconds.Observe(c.clock.Since(requestedAt).Seconds(), map[string]string{resultLabel: result})
	}
}
