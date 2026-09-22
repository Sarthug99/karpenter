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
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
)

const (
	// observationWindow bounds how long we wait for a rebooted node to prove a new boot and rejoin
	// after issuance before declaring RebootFailed. Beta uses a fixed value sized for slow instances.
	observationWindow = 20 * time.Minute
	// pollInterval is how often we re-check for boot/readiness while observing recovery.
	pollInterval = 15 * time.Second
	// issuanceTimeout bounds the post-drain provider-accept retry loop before declaring RebootFailed.
	// Mirrors nodeclaim lifecycle's LaunchTimeout: a provider control-plane call should complete within it.
	issuanceTimeout = 5 * time.Minute
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
		if !nodeclaimutils.IsNodeNotFoundError(err) {
			return reconcile.Result{}, err
		}
		// The Node is gone mid-reboot: we can neither observe recovery nor clean the fence (it went with
		// the Node). Fail if the deadline has elapsed, otherwise keep polling until it does — never a
		// silent no-requeue stop, which would wedge the NodeClaim at Rebooting=True forever.
		if c.pastRebootDeadline(nodeClaim) {
			result, msg := deadlineResult(nodeClaim)
			return c.transitionToFailed(ctx, nodeClaim, nil, result, msg)
		}
		return reconcile.Result{RequeueAfter: pollInterval}, nil
	}

	// Bound every phase: a reboot that never issues (request phase) or never recovers (observe phase) is
	// failed here rather than retrying forever, since Rebooting=True excludes the node from other
	// disruption and advertises returning capacity to the scheduler.
	if c.pastRebootDeadline(nodeClaim) {
		result, msg := deadlineResult(nodeClaim)
		return c.transitionToFailed(ctx, nodeClaim, node, result, msg)
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
		// Record pre-boot state before the first provider call.
		if err := c.recordIssuingState(ctx, nodeClaim, node); err != nil {
			return reconcile.Result{}, err
		}
	}

	// Issue the reboot with a deterministic, per-episode operationID (stable across retries/restarts).
	if err := c.cloudProvider.Reboot(ctx, nodeClaim, rebootOperationID(nodeClaim)); err != nil {
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
	// The observation-window timeout is enforced by the phase-agnostic deadline check in Reconcile.
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
	deadline := rebootRequestedAt(nodeClaim).Add(dgp)
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

// recordIssuingState records the pre-reboot bootID before the first provider call, so a changed bootID
// afterward proves the reboot happened (restart-safety) and terminal cleanup can scope to this episode.
func (c *Controller) recordIssuingState(ctx context.Context, nodeClaim *v1.NodeClaim, node *corev1.Node) error {
	stored := nodeClaim.DeepCopy()
	// Record the pre-reboot bootID (restart-safety / episode scoping) and the drain-completion time, which
	// anchors the post-drain issuance timeout independently of how much of the drain budget was used.
	nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, map[string]string{
		v1.RebootPreBootIDAnnotationKey:         node.Status.NodeInfo.BootID,
		v1.RebootIssuanceStartedAtAnnotationKey: c.clock.Now().Format(time.RFC3339),
	})
	if equality.Semantic.DeepEqual(stored, nodeClaim) {
		return nil
	}
	return c.kubeClient.Patch(ctx, nodeClaim, client.MergeFrom(stored))
}

func (c *Controller) transitionToIssued(ctx context.Context, nodeClaim *v1.NodeClaim, node *corev1.Node) (reconcile.Result, error) {
	stored := nodeClaim.DeepCopy()
	nodeClaim.StatusConditions().SetTrueWithReason(v1.ConditionTypeRebooting, v1.RebootReasonIssued, "reboot issued to the provider")
	// Initialization is scoped to a boot; a committed reboot invalidates it until the node re-initializes.
	// The Initialized->Unknown transition time also serves as the issuance timestamp (see issuedAt).
	nodeClaim.StatusConditions().SetUnknownWithReason(v1.ConditionTypeInitialized, v1.RebootReasonRequested, "node is rebooting")
	if !equality.Semantic.DeepEqual(stored, nodeClaim) {
		if err := c.kubeClient.Status().Patch(ctx, nodeClaim, client.MergeFrom(stored)); err != nil {
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
	// Terminal cleanup: ensure the reboot-owned fence is removed even if the boot never changed. node may
	// be nil when it was deleted mid-reboot, in which case the fence went with it — nothing to clean.
	if node != nil {
		if err := c.removeRebootTaint(ctx, node); err != nil {
			return reconcile.Result{}, err
		}
	}
	c.recorder.Publish(rebootevents.Failed(nodeClaim, msg))
	c.recordTerminalMetrics(nodeClaim, result)
	return c.setTerminal(ctx, nodeClaim, v1.RebootReasonFailed, msg)
}

func (c *Controller) setTerminal(ctx context.Context, nodeClaim *v1.NodeClaim, reason, msg string) (reconcile.Result, error) {
	// Clear episode-scoped reboot state (metadata) first, so a later reboot on this NodeClaim starts clean
	// and the restart-safety check can't misfire on a prior episode's pre-boot bootID.
	stored := nodeClaim.DeepCopy()
	_, hadPreBoot := nodeClaim.Annotations[v1.RebootPreBootIDAnnotationKey]
	_, hadStarted := nodeClaim.Annotations[v1.RebootIssuanceStartedAtAnnotationKey]
	if hadPreBoot || hadStarted {
		delete(nodeClaim.Annotations, v1.RebootPreBootIDAnnotationKey)
		delete(nodeClaim.Annotations, v1.RebootIssuanceStartedAtAnnotationKey)
		if err := c.kubeClient.Patch(ctx, nodeClaim, client.MergeFrom(stored)); err != nil {
			return reconcile.Result{}, err
		}
	}
	stored = nodeClaim.DeepCopy()
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

// issuedAt derives the issuance time from the Initialized condition, which transitions to Unknown exactly
// when the reboot is issued and is held there (by the initialization guard) until the reboot is terminal.
func (c *Controller) issuedAt(nodeClaim *v1.NodeClaim) (time.Time, bool) {
	cond := nodeClaim.StatusConditions().Get(v1.ConditionTypeInitialized)
	if cond == nil || cond.Status != metav1.ConditionUnknown {
		return time.Time{}, false
	}
	return cond.LastTransitionTime.Time, true
}

// issuanceStartedAt is when the drain completed and the provider-accept retry loop began.
func (c *Controller) issuanceStartedAt(nodeClaim *v1.NodeClaim) (time.Time, bool) {
	t, err := time.Parse(time.RFC3339, nodeClaim.Annotations[v1.RebootIssuanceStartedAtAnnotationKey])
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// rebootDeadline is the wall-clock bound for the current phase, after which the reboot is failed. It is
// derived only from the NodeClaim (never the Node), so it fires even when the Node has been deleted.
func (c *Controller) rebootDeadline(nodeClaim *v1.NodeClaim) time.Time {
	if nodeClaim.StatusConditions().Get(v1.ConditionTypeRebooting).Reason == v1.RebootReasonIssued {
		if issuedAt, ok := c.issuedAt(nodeClaim); ok {
			return issuedAt.Add(observationWindow)
		}
		// Fallback if issuedAt isn't derivable: the full request budget plus the observation window.
		return rebootRequestedAt(nodeClaim).Add(c.drainGracePeriod(nodeClaim) + issuanceTimeout + observationWindow)
	}
	// RebootRequested: once issuing, bound the provider-accept loop from drain-completion. Before drain
	// completes, fall back to the full request budget (drain + issuance) so a wedge during drain — e.g. a
	// missing Node — still fails rather than polling forever.
	if startedAt, ok := c.issuanceStartedAt(nodeClaim); ok {
		return startedAt.Add(issuanceTimeout)
	}
	return rebootRequestedAt(nodeClaim).Add(c.drainGracePeriod(nodeClaim) + issuanceTimeout)
}

func (c *Controller) pastRebootDeadline(nodeClaim *v1.NodeClaim) bool {
	return c.clock.Now().After(c.rebootDeadline(nodeClaim))
}

// deadlineResult maps the current phase to the terminal result label and message used when the deadline
// elapses: the request phase failed to issue (provider_error); the observe phase failed to recover.
func deadlineResult(nodeClaim *v1.NodeClaim) (result, msg string) {
	if nodeClaim.StatusConditions().Get(v1.ConditionTypeRebooting).Reason == v1.RebootReasonIssued {
		return resultRecoveryTimeout, "node did not recover within the observation window"
	}
	return resultProviderError, "reboot was not issued within the issuance timeout"
}

// rebootRequestedAt is when the current reboot episode was committed: the Rebooting condition's transition
// to True. operatorpkg preserves LastTransitionTime across the RebootRequested->RebootIssued reason change
// (status stays True), so this is stable for the whole episode until the terminal SetFalse.
func rebootRequestedAt(nodeClaim *v1.NodeClaim) time.Time {
	return nodeClaim.StatusConditions().Get(v1.ConditionTypeRebooting).LastTransitionTime.Time
}

// rebootOperationID is a deterministic, per-episode idempotency key passed to CloudProvider.Reboot: stable
// across retries and controller restarts within an episode, and distinct across episodes. Derived from the
// request time and the pre-reboot bootID (recorded before issuing) rather than stored, so no annotation is
// needed and stale keys can't leak. The bootID disambiguates episodes within the same second, since
// metav1.Time (the request time's source) only round-trips at second precision.
func rebootOperationID(nodeClaim *v1.NodeClaim) string {
	return fmt.Sprintf("%s-%d-%s", nodeClaim.UID, rebootRequestedAt(nodeClaim).UnixNano(), nodeClaim.Annotations[v1.RebootPreBootIDAnnotationKey])
}

// recordTerminalMetrics counts the reboot by result and observes the full-action duration (request ->
// terminal). Must be called while the Rebooting condition is still True (before setTerminal resets its
// transition time).
func (c *Controller) recordTerminalMetrics(nodeClaim *v1.NodeClaim, result string) {
	RebootsTotal.Inc(map[string]string{resultLabel: result})
	RebootDurationSeconds.Observe(c.clock.Since(rebootRequestedAt(nodeClaim)).Seconds(), map[string]string{resultLabel: result})
}
