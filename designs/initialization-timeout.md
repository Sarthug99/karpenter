# NodeClaim Initialization Timeout

## Motivation

Karpenter bounds every phase of a NodeClaim's startup except the last one. Liveness deletes a NodeClaim that doesn't launch within 5 minutes or register within 15 minutes, and the reboot controller fails a reboot that doesn't recover within its observation window. Nothing bounds initialization, and nothing bounds re-initialization after a reboot. A node that registers but never becomes `Initialized` stays that way until `expireAfter` fires, which defaults to 30 days and is often `Never`.

A stuck node does more harm than sitting idle:

- **It attracts pending pods.** The provisioner simulates onto every node that isn't marked for deletion. An uninitialized node's missing capacity is filled in from the NodeClaim's expected capacity, so pods that don't fit elsewhere are nominated to it and no new NodeClaim is created for them. This repeats every scheduling loop. On EKS we measured a GPU pod nominated 6 times over about 22 minutes to a node whose device plugin never registered, with no new NodeClaim, and GPU pods pending for about 3 hours on never-initialized nodes.
- **Disruption mostly can't free it.** Drift, consolidation and expiration-driven replacement skip uninitialized candidates. Repair admits them, but only when a health policy matches a node condition, and a missing startup-taint removal or DRA pool doesn't produce one.
- **Pre-spin disruption leaves one behind.** When a replace-first command's replacement never initializes, the command fails after 10 minutes and the candidate goes back into service. The replacement is not deleted. It stays uninitialized, outside budgets, and attracts pods as above.
- **Reboot makes it worse.** A rebooted node is uninitialized from issuance until it re-initializes. [#3497](https://github.com/kubernetes-sigs/karpenter/pull/3497) counts that window against disruption budgets, and [DerekFrank/karpenter#98](https://github.com/DerekFrank/karpenter/pull/98) adds a re-initialization gate that waits, without a timeout, for pods deleted before the reboot to finish terminating. Together they hold a budget slot for as long as a node fails to come back.

Initialization usually blocks on something Karpenter doesn't own: a DaemonSet that removes a startup taint, a device plugin, or a DRA driver. The right fix is in that component, but the person who gets paged is rarely the one who can ship it, and the mitigation is always the same: delete the NodeClaim and let Karpenter try again. Some of these failures are transient or specific to a host, and a retry clears them. The ones that aren't should be loud, not silent.

This RFC proposes an opt-in, per-NodePool `initializationTimeout` that bounds initialization and re-initialization the same way launch and registration are already bounded. See [#3242](https://github.com/kubernetes-sigs/karpenter/issues/3242).

### Use Cases

1. **Startup taint never removed.** A DaemonSet sizes hugepages from pod requests, restarts kubelet, then removes a startup taint. If it fails, the taint stays (from #3242).
2. **DRA driver never publishes.** The node goes Ready, but the driver never publishes a complete ResourceSlice pool, so the NodeClaim sits at `Initialized=Unknown, Reason=DRADriverPoolsNotPublished`.
3. **Device plugin never registers.** A GPU node's device plugin crashes before it advertises `nvidia.com/gpu`, leaving `Reason=ResourceNotRegistered`.
4. **Node never re-initializes after a reboot.** Repair reboots a node. It comes back with a new boot ID and the reboot succeeds, but a driver or agent doesn't come back, so `Initialized` stays `Unknown`.
5. **Departing pod outlives the reboot.** With #98, a pod deleted before the reboot that is stuck terminating (a finalizer, a dead kubelet) holds re-initialization open indefinitely.
6. **Pre-spin replacement never initializes.** Drift or consolidation creates a replacement on a broken image or driver. The command fails at 10 minutes and the replacement is orphaned.

### Non-Goals

- **Finding the root cause.** Karpenter surfaces the last `Initialized` reason and message. It doesn't diagnose them.
- **Timeouts on individual startup taints.** A per-taint deadline belongs upstream in Kubernetes and wouldn't cover use cases 2 through 6.
- **Scheduling around a broken DRA driver.** Letting pods that don't need a driver use the node shrinks the blast radius but doesn't free it for the pods that do.
- **Provisioning while a node reboots.** How long Karpenter waits before launching capacity alongside a rebooting node ([#3464](https://github.com/kubernetes-sigs/karpenter/issues/3464)) is a different action (launch, not delete). See Alternative 1.

## Proposal

### Proposed Spec

```yaml
apiVersion: karpenter.sh/v1
kind: NodePool
spec:
  template:
    spec:
      expireAfter: Never
      # Delete and replace a NodeClaim whose node registers, or comes back from a
      # reboot, and doesn't initialize within this duration. Unset or Never keeps
      # today's behavior.
      initializationTimeout: 1h
```

```go
type NodeClaimTemplateSpec struct {
	// ...
	// +kubebuilder:validation:Pattern=`^(([0-9]+(s|m|h))+|Never)$`
	// +kubebuilder:validation:XValidation:rule="self == 'Never' || duration(self) >= duration('1m')",message="initializationTimeout must be at least 1m"
	// +kubebuilder:validation:Type="string"
	// +kubebuilder:validation:Schemaless
	// +optional
	InitializationTimeout NillableDuration `json:"initializationTimeout,omitempty" hash:"ignore"`
}
```

The field is copied onto `NodeClaimSpec` like `expireAfter` and `terminationGracePeriod`, so standalone NodeClaims can set it too. It has no default. Values under 1 minute are rejected: `0s` would delete every node at registration.

### How It Works

Liveness already reconciles every NodeClaim and stops once `Registered=True`. With this change a registered NodeClaim keeps going through liveness until it initializes:

1. If `initializationTimeout` is unset or `Never`, or `Initialized=True`, do nothing.
2. If `Rebooting=True` with reason `RebootIssued`, do nothing. The reboot controller bounds that phase with its observation window and fails the reboot itself.
3. Start the clock at registration. If `Rebooting=False` with reason `RebootSucceeded`, start it at that transition instead. No other `Rebooting` transition moves the clock.
4. If the timeout hasn't passed, requeue for the remainder. Otherwise delete the NodeClaim.

Every timestamp is in status conditions, so a controller restart doesn't reset the clock. Deletion goes through normal termination: cordon, drain honoring PDBs and `karpenter.sh/do-not-disrupt` up to `terminationGracePeriod`, then terminate the instance. Pending pods go back through normal provisioning, which launches a replacement.

| NodeClaim state | Clock starts at | At expiry |
|---|---|---|
| Registered, never initialized | `Registered` transition | Delete, reason `initialization_timeout` |
| `Rebooting=True/RebootRequested` (draining), never initialized | `Registered` transition | Delete, reason `initialization_timeout` |
| `Rebooting=True/RebootIssued` | Paused | Reboot controller owns it |
| `Rebooting=False/RebootSucceeded`, not re-initialized | `RebootSucceeded` transition | Delete, reason `reinitialization_timeout`, drain bounded by the reboot's grace period |
| `Rebooting=False/RebootFailed` (invalid request, node not rebooted) | `Registered` transition | Delete, reason `initialization_timeout` |
| `Initialized=True` | n/a | Nothing |

### Behavior per Disruption Path

**Fresh registration.** The clock starts at registration. On expiry the NodeClaim is deleted, it leaves `nodes.Active()`, and the pods that were nominated to it get a new NodeClaim on the next provisioning loop.

**Reboot.** A reboot only invalidates initialization once it's issued (`Initialized` goes `Unknown` at `RebootIssued`).
- *Requested:* the node is draining and hasn't rebooted. An initialized node isn't affected because `Initialized` is still `True`. A never-initialized node keeps its registration clock. This matters because a reboot drain with no grace period is unbounded; without this, a never-initialized node being repaired by reboot would be exempt forever.
- *Issued:* paused. The reboot controller fails the reboot at the end of its observation window and replaces the node.
- *Succeeded:* the node gets a full `initializationTimeout` to re-initialize, measured from success. This anchors on the reboot's own terminal transition, not `Initialized`'s last transition, so it doesn't inherit [#3535](https://github.com/kubernetes-sigs/karpenter/issues/3535).
- *Failed:* every failure except an invalid request already deletes the NodeClaim. An invalid request fails before rebooting, so the clock doesn't restart.
- *Expiry action:* delete, with the same drain bound the reboot controller applies when it fails a reboot (the `karpenter.sh/reboot-termination-grace-period` annotation tightens the termination deadline). The metric reason is `reinitialization_timeout`, so operators can tell a node that never came up from one that didn't come back. See Alternative 6 for why this isn't "fail the reboot".
- *With #3497 and #98:* once deleted the node counts against budgets until `InstanceTerminating=True`, so the budget slot is held for at most the timeout plus the drain. The drain skips pods stuck terminating, so #98's gate can't block the deletion.

**Pre-spin (replace-first): drift, consolidation replace, repair replace, static drift.** The replacement inherits the field from the NodePool template.
- *Timeout under 10 minutes:* liveness deletes the replacement and the disruption queue fails the command: the candidate's `karpenter.sh/disrupted` taint is removed, it's un-marked for deletion, and it keeps serving. Today the queue only fails a command once a deleted replacement is fully gone, so the candidate stays marked for deletion while the replacement drains. On EKS that window was about 33 seconds, long enough for the provisioner to launch an extra NodeClaim for the candidate's pods. This proposal also fails the command as soon as a replacement has a deletion timestamp. A NodeClaim that's being deleted will never initialize, so waiting gains nothing.
- *Timeout over 10 minutes:* the queue fails the command at 10 minutes and returns the candidate to service. The orphaned replacement is deleted when its own timeout expires.
- Either way nothing is left behind. Alternative 7 covers deleting orphans even when the field is unset.

**Delete-first: emptiness, consolidation delete, terminate-first drift and repair, expiration, interruption.** The old node is gone before the new one exists. The new node comes from normal provisioning, so it's a fresh registration in whichever NodePool the scheduler picks. The timeout applies if that NodePool sets it.

**Static NodePools.** Deleting a timed-out node drops the count below `replicas`, and static provisioning launches another.

**Restart.** The disruption queue is in memory, but the timeout reads only status conditions, so orphans created before a restart are still covered.

### Interaction with Existing Features

- **`expireAfter`** is unchanged and independent. Whichever fires first wins.
- **Launch and registration timeouts** are unchanged. The initialization clock starts at registration, so the three don't overlap.
- **Reboot** is never interrupted while issued. See the per-path section.
- **Disruption budgets** don't apply, matching the launch and registration timeouts. Uninitialized nodes don't count against budgets on mainline (#3497 changes that for rebooting nodes), and drift and consolidation skip them. Repair does admit uninitialized nodes and spends no budget on them, but only acts when a health policy matches a node condition. A budget here would block the only path that frees the node.
- **Drift.** The field is `hash:"ignore"`. Changing it doesn't drift nodes, and the new value applies to NodeClaims created after the change.
- **`NodeRegistrationHealthy`** is unchanged. An initialization timeout isn't a registration failure.

### Observability

- **Metric.** `karpenter_nodeclaims_disrupted_total{reason="initialization_timeout"|"reinitialization_timeout"}`, labeled by NodePool and capacity type, next to `launch_timeout` and `registration_timeout`.
- **Event.** A Warning `InitializationTimeout` event on the NodeClaim with the last `Initialized` reason and message, e.g. `Node did not re-initialize after reboot within 30m0s, replacing NodeClaim (last reason StartupTaintsExist: StartupTaint "example.com/agent-not-ready:NoSchedule" still exists)`.
- **Log.** The existing `terminating due to timeout` line with the reason.

### Edge Cases

- **Systematic failure.** If every node in a pool hits the same bug, each NodeClaim waits the full timeout before it's replaced: one replacement per stuck NodeClaim per timeout. The metric and event make this visible per NodePool. Open question 3.
- **Timeout shorter than normal startup.** GPU driver installs and kubelet restarts can take many minutes. A timeout below that churns healthy nodes. Docs should recommend a value well above the pool's normal initialization time.
- **Pods already on the node.** Nodes stuck on `ResourceNotRegistered` or `DRADriverPoolsNotPublished` are Ready and untainted, so pods that don't need the missing resource can land there. They are drained like any other deletion.
- **Already-stuck nodes.** NodeClaim spec is immutable, so setting the field doesn't rescue NodeClaims that already exist. Open question 4.
- **Startup taints after a reboot.** Karpenter applies startup taints at registration, not after a reboot, and re-initializes a rebooted node as soon as it's Ready. An agent that needs to hold a rebooted node uninitialized has to re-add its taint before the node comes back. This is existing behavior; the timeout only bounds how long the agent can hold it.
- **Pods evicted by a reboot.** They're nominated back to the rebooting node and no new NodeClaim is created for them (#3464). The timeout bounds this when the node fails to re-initialize; it doesn't change the successful case.
- **Repeated reboots.** Each successful reboot restarts the clock. Repair caps reboots at 2 per 24 hours, but that history is in memory, so a restart between reboots could allow more.

## Alternatives Considered

### Alternative 1: A general `giveUpOnNodeClaimAfter`

One knob for how long a NodeClaim may be unusable for any reason. Launch and registration are already bounded, and a reboot that doesn't recover is already failed. The open reboot gap in #3464 is how long to suppress provisioning while a node reboots; the action there is to launch alongside the node, not give up on it. The one reboot case that does leave a stuck NodeClaim, a reboot that succeeds and never re-initializes, is covered here. If a general knob is wanted later, `initializationTimeout` can be its first member.

### Alternative 2: Timeouts on startup taints

Covers use case 1 only and needs an upstream Kubernetes change.

### Alternative 3: A hardcoded constant like `registrationTimeout`

Normal initialization time varies by pool far more than registration does. One global value would be too long to help or short enough to churn slow pools, and would change behavior for every user.

### Alternative 4: Make uninitialized nodes disruption candidates

Let drift and consolidation act on uninitialized nodes so budgets apply. Repair already admits them, so the gap is the other methods. Their simulations assume a candidate's capacity is real and schedulable, which an uninitialized node's isn't, and they'd still need a per-pool notion of how long is too long.

### Alternative 5: Read the timeout from the NodePool at reconcile time

Setting the field would immediately rescue existing stuck NodeClaims. No other lifecycle field works this way, and standalone NodeClaims couldn't use it. Open question 4.

### Alternative 6: Fail the reboot instead of deleting

On a rebooted node, expiry could mark the reboot failed and let the reboot controller's escalation replace the node, keeping reboot telemetry. But once a reboot succeeds there's nothing in flight to fail. Keeping `Rebooting=True` until re-initialization changes what `RebootSucceeded` means for #3497, #98 and repair's result tracking. Writing `RebootFailed` from liveness and deleting is just delete with a different label. The proposal keeps delete, reuses the reboot's drain bound, and uses a distinct metric reason.

### Alternative 7: Delete orphaned replacements when a disruption command fails

On command failure the queue could delete replacements that aren't `Initialized`, with no opt-in. We prototyped it (+33 lines in `queue.go`, 7 specs, 5 of which fail without it). It fixes use case 6 for everyone, but it turns the queue's 10 minute retry window into a hard, unconfigurable initialization deadline for every replacement, churns a pool that can never initialize, and can't see orphans from before a restart. It's worth proposing on its own; this RFC doesn't depend on it.

## Backward Compatibility

The field is optional with no default, so existing NodePools behave exactly as today. It's excluded from the drift hash, so upgrading Karpenter or setting it doesn't drift nodes. Both CRDs gain a field, and providers vendoring them need to regenerate.

## Graduation Criteria

No feature gate. The field is opt-in and does nothing until set.

## Open Questions

1. **Default.** Unset keeps today's behavior and is proposed for now. Every stuck node we measured was in a pool that didn't set anything, so a large finite default (for example `4h`) would help users who never learn the field exists. Revisit once the metric shows real initialization times.
2. **Budgets.** Should initialization timeouts count against disruption budgets? Proposed no, matching launch and registration.
3. **Repeated failures.** Should repeated timeouts in a NodePool set a status condition or back off launches, so a systematic failure doesn't become steady churn?
4. **Existing NodeClaims.** Should liveness fall back to the NodePool's current value (Alternative 5) so the field can rescue NodeClaims that are already stuck?
5. **Minimum.** 1 minute is proposed. Should it be higher?
6. **Name.** `initializationTimeout` matches the issue and the internal launch and registration timeouts. `initializeWithin` would read closer to `expireAfter` and `consolidateAfter`.
